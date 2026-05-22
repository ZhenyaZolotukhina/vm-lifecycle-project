"""
Resume/repair final join for Google ClusterData 2019 cluster A.

Put this file into the same folder as build_google_a_sample.py, e.g.:
D:\google_cluster_a_pipeline\join_google_a_final_resume.py

It DOES NOT download or parse Google shards again.
It uses files already created in out/:
- google_a_usage_sample.parquet
- google_a_events_requested.parquet
- google_a_requested_by_vm.parquet

Modes:
1) MODE = "fast_by_vm"       very fast join by vm_id using latest requested CPU/RAM per VM.
2) MODE = "asof_bucketed"    exact time-aware ASOF join, resumable by buckets.
"""

from pathlib import Path
import shutil
import duckdb

# ============================================================
# SETTINGS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "out"

USAGE_OUT = OUT_DIR / "google_a_usage_sample.parquet"
EVENTS_OUT = OUT_DIR / "google_a_events_requested.parquet"
REQUESTED_BY_VM_OUT = OUT_DIR / "google_a_requested_by_vm.parquet"

# Output names
FAST_FINAL_OUT = OUT_DIR / "google_a_final_joined_by_vm.parquet"
ASOF_FINAL_PARTS_DIR = OUT_DIR / "final_asof_parts"
ASOF_FINAL_OUT = OUT_DIR / "google_a_final_joined_asof.parquet"

# Change mode here if needed:
MODE = "asof_bucketed"   # "fast_by_vm" or "asof_bucketed"

# ASOF bucket settings. 16 is usually a good balance for laptop/external disk.
N_BUCKETS = 16
MAKE_SINGLE_ASOF_FILE = True   # True: also merge bucket files into one parquet at the end.

DUCKDB_THREADS = 4
DUCKDB_MEMORY_LIMIT = "4GB"    # increase to "8GB" if laptop has enough RAM.
CLEAN_OLD_DUCKDB_TMP = True


# ============================================================
# HELPERS
# ============================================================

def qpath(p: Path) -> str:
    """Safe path for DuckDB SQL on Windows/macOS/Linux."""
    return str(p.resolve()).replace("\\", "/").replace("'", "''")


def check_input_files():
    required = [USAGE_OUT, EVENTS_OUT, REQUESTED_BY_VM_OUT]
    missing = [str(p) for p in required if not p.exists() or p.stat().st_size == 0]
    if missing:
        raise FileNotFoundError("Missing or empty input files:\n" + "\n".join(missing))


def prepare_duckdb():
    tmp_dir = OUT_DIR / "duckdb_tmp_resume"
    db_file = OUT_DIR / "duckdb_resume.db"

    if CLEAN_OLD_DUCKDB_TMP and tmp_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=str(db_file))
    con.execute(f"PRAGMA threads={DUCKDB_THREADS};")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    con.execute("PRAGMA temp_directory='" + qpath(tmp_dir) + "';")
    return con


def fast_join_by_vm():
    """
    Keeps ALL usage rows. Adds the latest known requested_cpu/requested_ram per vm_id.
    This is not time-aware, but is much faster and usually enough for baseline modeling.
    """
    print("[FAST] Building final table by vm_id...")
    if FAST_FINAL_OUT.exists() and FAST_FINAL_OUT.stat().st_size > 0:
        print(f"[SKIP] Already exists: {FAST_FINAL_OUT}")
        return

    con = prepare_duckdb()
    con.execute(f"""
        COPY (
            SELECT
                u.vm_id,
                u.collection_id,
                u.instance_index,
                u.start_time,
                u.end_time,
                u.trace_minute,
                u.average_cpu,
                u.maximum_cpu,
                u.maximum_memory,
                r.requested_cpu,
                r.requested_ram,
                r.last_request_event_time AS event_time,
                CASE WHEN r.requested_cpu > 0 THEN u.average_cpu / r.requested_cpu ELSE NULL END
                    AS avg_cpu_ratio_to_requested,
                CASE WHEN r.requested_cpu > 0 THEN u.maximum_cpu / r.requested_cpu ELSE NULL END
                    AS max_cpu_ratio_to_requested,
                CASE WHEN r.requested_ram > 0 THEN u.maximum_memory / r.requested_ram ELSE NULL END
                    AS max_memory_ratio_to_requested
            FROM read_parquet('{qpath(USAGE_OUT)}') AS u
            LEFT JOIN read_parquet('{qpath(REQUESTED_BY_VM_OUT)}') AS r
            USING (vm_id)
        )
        TO '{qpath(FAST_FINAL_OUT)}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    con.close()
    print(f"[DONE] Saved: {FAST_FINAL_OUT}")


def asof_join_bucketed():
    """
    Exact time-aware join. Keeps ALL usage rows.
    For every usage row, adds the latest requested_cpu/requested_ram event for the same VM
    with event_time <= usage.start_time.

    Resumable: if the script stops, rerun it; existing non-empty bucket parquet files are skipped.
    """
    ASOF_FINAL_PARTS_DIR.mkdir(parents=True, exist_ok=True)

    # Remove broken 0-byte outputs if they exist.
    if ASOF_FINAL_OUT.exists() and ASOF_FINAL_OUT.stat().st_size == 0:
        ASOF_FINAL_OUT.unlink()

    con = prepare_duckdb()

    for bucket in range(N_BUCKETS):
        part_out = ASOF_FINAL_PARTS_DIR / f"google_a_final_joined_asof_bucket_{bucket:03d}.parquet"
        if part_out.exists() and part_out.stat().st_size > 0:
            print(f"[SKIP] bucket {bucket + 1}/{N_BUCKETS}: already exists")
            continue

        if part_out.exists() and part_out.stat().st_size == 0:
            part_out.unlink()

        print(f"[ASOF] bucket {bucket + 1}/{N_BUCKETS}: building...")

        con.execute(f"""
            COPY (
                WITH usage_data AS (
                    SELECT *
                    FROM read_parquet('{qpath(USAGE_OUT)}')
                    WHERE hash(vm_id) % {N_BUCKETS} = {bucket}
                    ORDER BY vm_id, start_time
                ),
                events_data AS (
                    SELECT *
                    FROM read_parquet('{qpath(EVENTS_OUT)}')
                    WHERE requested_cpu IS NOT NULL
                      AND requested_ram IS NOT NULL
                      AND hash(vm_id) % {N_BUCKETS} = {bucket}
                    ORDER BY vm_id, event_time
                )
                SELECT
                    u.vm_id,
                    u.collection_id,
                    u.instance_index,
                    u.start_time,
                    u.end_time,
                    u.trace_minute,
                    u.average_cpu,
                    u.maximum_cpu,
                    u.maximum_memory,
                    e.requested_cpu,
                    e.requested_ram,
                    e.event_time,
                    CASE WHEN e.requested_cpu > 0 THEN u.average_cpu / e.requested_cpu ELSE NULL END
                        AS avg_cpu_ratio_to_requested,
                    CASE WHEN e.requested_cpu > 0 THEN u.maximum_cpu / e.requested_cpu ELSE NULL END
                        AS max_cpu_ratio_to_requested,
                    CASE WHEN e.requested_ram > 0 THEN u.maximum_memory / e.requested_ram ELSE NULL END
                        AS max_memory_ratio_to_requested
                FROM usage_data u
                ASOF LEFT JOIN events_data e
                ON u.vm_id = e.vm_id
               AND u.start_time >= e.event_time
            )
            TO '{qpath(part_out)}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

        print(f"[DONE] bucket {bucket + 1}/{N_BUCKETS}: {part_out.name}")

    if MAKE_SINGLE_ASOF_FILE:
        parts_glob = qpath(ASOF_FINAL_PARTS_DIR / "*.parquet")
        print("[MERGE] Creating one final ASOF parquet file...")
        if ASOF_FINAL_OUT.exists():
            ASOF_FINAL_OUT.unlink()
        con.execute(f"""
            COPY (
                SELECT *
                FROM read_parquet('{parts_glob}')
            )
            TO '{qpath(ASOF_FINAL_OUT)}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        print(f"[DONE] Saved: {ASOF_FINAL_OUT}")

    con.close()


def main():
    print("=== RESUME FINAL JOIN FOR GOOGLE CLUSTERDATA 2019 A ===")
    print(f"Working folder: {BASE_DIR}")
    print(f"Mode: {MODE}")
    check_input_files()

    if MODE == "fast_by_vm":
        fast_join_by_vm()
    elif MODE == "asof_bucketed":
        asof_join_bucketed()
    else:
        raise ValueError('MODE must be "fast_by_vm" or "asof_bucketed"')


if __name__ == "__main__":
    main()
