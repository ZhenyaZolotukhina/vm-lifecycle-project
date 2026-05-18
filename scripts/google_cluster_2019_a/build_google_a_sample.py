import os
import json
import gzip
import hashlib
import subprocess
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb


# ============================================================
# НАСТРОЙКИ
# ============================================================

CLUSTER = "a"

# Скрипт кладем в D:\google_cluster_a_pipeline.
# Все папки будут создаваться рядом со скриптом.
BASE_DIR = Path(__file__).resolve().parent

RAW_USAGE_DIR = BASE_DIR / "raw_usage"
RAW_EVENTS_DIR = BASE_DIR / "raw_events"
OUT_DIR = BASE_DIR / "out"
USAGE_PARTS_DIR = OUT_DIR / "usage_parts"
EVENTS_PARTS_DIR = OUT_DIR / "events_parts"
PROCESSED_USAGE_DIR = BASE_DIR / "processed" / "usage"
PROCESSED_EVENTS_DIR = BASE_DIR / "processed" / "events"
FAILED_DIR = BASE_DIR / "failed"

USAGE_OUT = OUT_DIR / "google_a_usage_sample.parquet"
EVENTS_OUT = OUT_DIR / "google_a_events_requested.parquet"
REQUESTED_BY_VM_OUT = OUT_DIR / "google_a_requested_by_vm.parquet"
FINAL_OUT = OUT_DIR / "google_a_final_joined.parquet"
VM_LIST_OUT = OUT_DIR / "sampled_vm_ids.txt"
PROGRESS_OUT = OUT_DIR / "progress.txt"

# Хеш-сэмплирование: 1/16 VM.
# Важно: фильтр применяется одинаково к usage и events через один и тот же vm_id.
HASH_MOD = 16
HASH_KEEP = 0

# Для полной выгрузки должно быть False.
# Если хочешь сначала тест на нескольких шардах, поставь True.
TEST_MODE = False
MAX_USAGE_SHARDS = 3
MAX_EVENT_SHARDS = 3

# После обработки shard сырой .json.gz удаляется, чтобы не забивать диск.
DELETE_RAW_AFTER_PARSE = True

# Если True, в конце строится единая итоговая таблица через DuckDB.
# Для полного кластера это может занять долго.
BUILD_FINAL_JOIN = True




def find_gsutil_executable() -> str:
    """На Windows gsutil часто лежит как gsutil.cmd, поэтому ищем оба варианта."""
    exe = shutil.which("gsutil") or shutil.which("gsutil.cmd")
    if exe is None:
        raise FileNotFoundError(
            "gsutil не найден. Запускай скрипт из Google Cloud SDK Shell "
            "или добавь Google Cloud SDK\\google-cloud-sdk\\bin в PATH."
        )
    return exe

GSUTIL_CMD = find_gsutil_executable()

# ============================================================
# СОЗДАНИЕ ПАПОК
# ============================================================

for d in [
    RAW_USAGE_DIR,
    RAW_EVENTS_DIR,
    OUT_DIR,
    USAGE_PARTS_DIR,
    EVENTS_PARTS_DIR,
    PROCESSED_USAGE_DIR,
    PROCESSED_EVENTS_DIR,
    FAILED_DIR,
]:
    d.mkdir(parents=True, exist_ok=True)


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def log(msg: str):
    print(msg, flush=True)
    with open(PROGRESS_OUT, "a", encoding="utf-8") as f:
        f.write(str(msg) + "\n")


def stable_hash_vm(vm_id: str) -> int:
    return int(hashlib.md5(vm_id.encode("utf-8")).hexdigest(), 16)


def keep_vm(vm_id: str) -> bool:
    return stable_hash_vm(vm_id) % HASH_MOD == HASH_KEEP


def make_vm_id(collection_id, instance_index) -> str:
    return f"{CLUSTER}_{int(collection_id)}_{int(instance_index)}"


def gsutil_ls(pattern: str):
    result = subprocess.run(
        [GSUTIL_CMD, "ls", pattern],
        capture_output=True,
        text=True,
        check=True,
    )
    return [x.strip() for x in result.stdout.splitlines() if x.strip()]


def shard_name_from_gs_path(gs_path: str) -> str:
    return gs_path.rstrip("/").split("/")[-1]


def part_name_from_shard(shard_name: str) -> str:
    return shard_name.replace(".json.gz", "").replace(".", "_") + ".parquet"


def marker_path(kind: str, shard_name: str) -> Path:
    if kind == "usage":
        return PROCESSED_USAGE_DIR / (shard_name.replace(".json.gz", "") + ".done")
    if kind == "events":
        return PROCESSED_EVENTS_DIR / (shard_name.replace(".json.gz", "") + ".done")
    raise ValueError("kind must be usage or events")


def mark_processed(kind: str, shard_name: str, rows_written: int, bad_lines: int):
    marker = marker_path(kind, shard_name)
    marker.write_text(
        f"rows_written={rows_written}\nbad_lines={bad_lines}\n",
        encoding="utf-8",
    )


def is_processed(kind: str, shard_name: str, part_path: Path) -> bool:
    """
    Shard считается обработанным, если есть marker.
    Если marker говорит, что строк было > 0, то дополнительно проверяем наличие parquet-part.
    """
    marker = marker_path(kind, shard_name)
    if not marker.exists():
        return False

    text = marker.read_text(encoding="utf-8", errors="ignore")
    rows_written = 0
    for line in text.splitlines():
        if line.startswith("rows_written="):
            try:
                rows_written = int(line.split("=", 1)[1])
            except Exception:
                rows_written = 0

    if rows_written > 0 and not part_path.exists():
        return False

    return True


def write_failed(kind: str, shard_name: str, error_text: str):
    path = FAILED_DIR / f"{kind}_{shard_name}.txt"
    path.write_text(error_text, encoding="utf-8")


def download_shard(gs_path: str, dest_dir: Path) -> Path:
    """
    Скачивает только один shard. Старый сырой файл перезаписывается.
    После парсинга файл удаляется.
    """
    shard_name = shard_name_from_gs_path(gs_path)
    local_file = dest_dir / shard_name

    if local_file.exists():
        local_file.unlink()

    subprocess.run(
        [GSUTIL_CMD, "cp", gs_path, str(local_file)],
        check=True,
    )

    return local_file


def to_int(x):
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None


def to_float(x):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def deep_get(obj: dict, paths):
    """
    Достает значение из JSON по нескольким возможным вариантам пути.
    paths: список tuple, например [("average_cpu",), ("average_usage", "cpus")]
    """
    for path in paths:
        cur = obj
        ok = True
        for key in path:
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                ok = False
                break
        if ok:
            return cur
    return None


def decode_json_line(line: str):
    """
    Обычно Google shard — это JSON Lines: одна строка = один JSON.
    Но функция сделана устойчивой:
    - понимает dict;
    - понимает list из dict;
    - пытается прочитать несколько JSON-объектов в одной строке;
    - если строка битая, возвращает пустой список.
    """
    s = line.strip()
    if not s:
        return []

    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
        if isinstance(obj, dict):
            return [obj]
        return []
    except json.JSONDecodeError:
        pass

    # fallback для случая склеенных JSON-объектов
    decoder = json.JSONDecoder()
    out = []
    idx = 0
    n = len(s)

    while idx < n:
        while idx < n and s[idx].isspace():
            idx += 1
        if idx >= n:
            break
        try:
            obj, end = decoder.raw_decode(s, idx)
        except json.JSONDecodeError:
            return []
        if isinstance(obj, list):
            out.extend([x for x in obj if isinstance(x, dict)])
        elif isinstance(obj, dict):
            out.append(obj)
        idx = end

    return out


def save_parquet(df: pd.DataFrame, path: Path):
    if df.empty:
        return
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="zstd")


def load_sampled_vm_ids() -> set:
    if not VM_LIST_OUT.exists():
        return set()
    with open(VM_LIST_OUT, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def save_sampled_vm_ids(vm_ids: set):
    with open(VM_LIST_OUT, "w", encoding="utf-8") as f:
        for vm_id in sorted(vm_ids):
            f.write(vm_id + "\n")


# ============================================================
# ПАРСИНГ USAGE
# ============================================================

def parse_usage_file(local_file: Path):
    rows = []
    total_lines = 0
    bad_lines = 0

    with gzip.open(local_file, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            total_lines += 1
            records = decode_json_line(line)
            if not records:
                bad_lines += 1
                continue

            for obj in records:
                collection_id = to_int(deep_get(obj, [
                    ("collection_id",),
                    ("collectionId",),
                    ("collection", "id"),
                ]))
                instance_index = to_int(deep_get(obj, [
                    ("instance_index",),
                    ("instanceIndex",),
                    ("instance", "index"),
                ]))

                if collection_id is None or instance_index is None:
                    continue

                vm_id = make_vm_id(collection_id, instance_index)

                if not keep_vm(vm_id):
                    continue

                start_time = to_int(deep_get(obj, [
                    ("start_time",),
                    ("startTime",),
                ]))
                end_time = to_int(deep_get(obj, [
                    ("end_time",),
                    ("endTime",),
                ]))
                average_cpu = to_float(deep_get(obj, [
                    ("average_cpu",),
                    ("average_usage", "cpus"),
                    ("averageUsage", "cpus"),
                    ("average_usage", "cpu"),
                    ("averageUsage", "cpu"),
                ]))
                maximum_cpu = to_float(deep_get(obj, [
                    ("maximum_cpu",),
                    ("maximum_usage", "cpus"),
                    ("maximumUsage", "cpus"),
                    ("maximum_usage", "cpu"),
                    ("maximumUsage", "cpu"),
                ]))
                maximum_memory = to_float(deep_get(obj, [
                    ("maximum_memory",),
                    ("maximum_usage", "memory"),
                    ("maximumUsage", "memory"),
                ]))

                if (
                    start_time is None
                    or end_time is None
                    or average_cpu is None
                    or maximum_cpu is None
                    or maximum_memory is None
                ):
                    continue

                rows.append({
                    "vm_id": vm_id,
                    "collection_id": collection_id,
                    "instance_index": instance_index,
                    "start_time": start_time,
                    "end_time": end_time,
                    "trace_minute": start_time / 60_000_000,
                    "average_cpu": average_cpu,
                    "maximum_cpu": maximum_cpu,
                    "maximum_memory": maximum_memory,
                })

    df = pd.DataFrame(rows)
    return df, total_lines, bad_lines


# ============================================================
# ПАРСИНГ EVENTS
# ============================================================

def parse_events_file(local_file: Path, sampled_vm_ids: set):
    rows = []
    total_lines = 0
    bad_lines = 0

    with gzip.open(local_file, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            total_lines += 1
            records = decode_json_line(line)
            if not records:
                bad_lines += 1
                continue

            for obj in records:
                collection_id = to_int(deep_get(obj, [
                    ("collection_id",),
                    ("collectionId",),
                    ("collection", "id"),
                ]))
                instance_index = to_int(deep_get(obj, [
                    ("instance_index",),
                    ("instanceIndex",),
                    ("instance", "index"),
                ]))

                if collection_id is None or instance_index is None:
                    continue

                vm_id = make_vm_id(collection_id, instance_index)

                if vm_id not in sampled_vm_ids:
                    continue

                event_time = to_int(deep_get(obj, [
                    ("time",),
                    ("event_time",),
                    ("eventTime",),
                ]))
                requested_cpu = to_float(deep_get(obj, [
                    ("resource_request_cpus",),
                    ("resourceRequestCpus",),
                    ("resource_request", "cpus"),
                    ("resourceRequest", "cpus"),
                    ("resource_request", "cpu"),
                    ("resourceRequest", "cpu"),
                ]))
                requested_ram = to_float(deep_get(obj, [
                    ("resource_request_memory",),
                    ("resourceRequestMemory",),
                    ("resource_request", "memory"),
                    ("resourceRequest", "memory"),
                ]))

                if event_time is None:
                    continue

                # Для нормализации нужны именно request CPU/RAM.
                # События без request здесь не сохраняем.
                if requested_cpu is None or requested_ram is None:
                    continue

                rows.append({
                    "vm_id": vm_id,
                    "collection_id": collection_id,
                    "instance_index": instance_index,
                    "event_time": event_time,
                    "requested_cpu": requested_cpu,
                    "requested_ram": requested_ram,
                })

    df = pd.DataFrame(rows)
    return df, total_lines, bad_lines


# ============================================================
# ОБЪЕДИНЕНИЕ PART-ФАЙЛОВ И FINAL JOIN
# ============================================================

def parquet_files_count(folder: Path) -> int:
    return len(list(folder.glob("*.parquet")))


def build_combined_outputs():
    usage_glob = str(USAGE_PARTS_DIR / "*.parquet").replace("\\", "/")
    events_glob = str(EVENTS_PARTS_DIR / "*.parquet").replace("\\", "/")
    usage_out = str(USAGE_OUT).replace("\\", "/")
    events_out = str(EVENTS_OUT).replace("\\", "/")
    requested_by_vm_out = str(REQUESTED_BY_VM_OUT).replace("\\", "/")
    final_out = str(FINAL_OUT).replace("\\", "/")
    temp_db = str(OUT_DIR / "duckdb_temp.db")
    temp_dir = str(OUT_DIR / "duckdb_tmp").replace("\\", "/")
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    if parquet_files_count(USAGE_PARTS_DIR) == 0:
        log("[ERROR] Нет usage part-файлов. Итог собрать невозможно.")
        return

    if parquet_files_count(EVENTS_PARTS_DIR) == 0:
        log("[ERROR] Нет events part-файлов. Итог собрать невозможно.")
        return

    log("\n[COMBINE] Собираем единые parquet-файлы и итоговую таблицу через DuckDB...")
    log("Это может занять заметное время на полном кластере.")

    con = duckdb.connect(database=temp_db)
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA temp_directory='" + temp_dir + "';")

    # Единый usage sample.
    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{usage_glob}')
        )
        TO '{usage_out}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Единый events requested.
    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{events_glob}')
        )
        TO '{events_out}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Отдельная таблица: vm_id, requested_cpu, requested_ram.
    # Берем последнее известное значение request для каждой VM.
    con.execute(f"""
        COPY (
            SELECT
                vm_id,
                arg_max(requested_cpu, event_time) AS requested_cpu,
                arg_max(requested_ram, event_time) AS requested_ram,
                max(event_time) AS last_request_event_time
            FROM read_parquet('{events_glob}')
            WHERE requested_cpu IS NOT NULL
              AND requested_ram IS NOT NULL
            GROUP BY vm_id
        )
        TO '{requested_by_vm_out}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    if BUILD_FINAL_JOIN:
        # Для каждой usage-строки подтягиваем последнее известное request-событие
        # той же VM, которое произошло не позже start_time.
        con.execute(f"""
            COPY (
                WITH usage_data AS (
                    SELECT *
                    FROM read_parquet('{usage_glob}')
                    ORDER BY vm_id, start_time
                ),
                events_data AS (
                    SELECT *
                    FROM read_parquet('{events_glob}')
                    WHERE requested_cpu IS NOT NULL
                      AND requested_ram IS NOT NULL
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
                    CASE
                        WHEN e.requested_cpu > 0
                        THEN u.average_cpu / e.requested_cpu
                        ELSE NULL
                    END AS avg_cpu_ratio_to_requested,
                    CASE
                        WHEN e.requested_cpu > 0
                        THEN u.maximum_cpu / e.requested_cpu
                        ELSE NULL
                    END AS max_cpu_ratio_to_requested,
                    CASE
                        WHEN e.requested_ram > 0
                        THEN u.maximum_memory / e.requested_ram
                        ELSE NULL
                    END AS max_memory_ratio_to_requested
                FROM usage_data u
                ASOF LEFT JOIN events_data e
                ON u.vm_id = e.vm_id
               AND u.start_time >= e.event_time
            )
            TO '{final_out}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

    con.close()

    log("\n[DONE] Готовые файлы:")
    log(str(USAGE_OUT))
    log(str(EVENTS_OUT))
    log(str(REQUESTED_BY_VM_OUT))
    if BUILD_FINAL_JOIN:
        log(str(FINAL_OUT))
    log(str(VM_LIST_OUT))


# ============================================================
# MAIN
# ============================================================

def main():
    if PROGRESS_OUT.exists():
        PROGRESS_OUT.unlink()

    log("=== GOOGLE CLUSTERDATA 2019 A PIPELINE ===")
    log(f"Рабочая папка: {BASE_DIR}")
    log(f"Hash sample: hash(vm_id) % {HASH_MOD} == {HASH_KEEP}")

    log("\nПолучаем список usage-shards...")
    usage_files = sorted(gsutil_ls("gs://clusterdata_2019_a/instance_usage-*.json.gz"))
    if TEST_MODE:
        usage_files = usage_files[:MAX_USAGE_SHARDS]
    log(f"Найдено usage shard-файлов: {len(usage_files)}")

    sampled_vm_ids = load_sampled_vm_ids()
    log(f"Sampled VM IDs перед обработкой usage: {len(sampled_vm_ids)}")

    failed_usage = []

    for i, gs_path in enumerate(usage_files, 1):
        shard_name = shard_name_from_gs_path(gs_path)
        part_path = USAGE_PARTS_DIR / part_name_from_shard(shard_name)

        if is_processed("usage", shard_name, part_path):
            log(f"[USAGE] shard {i}/{len(usage_files)} уже обработан, пропускаем: {shard_name}")
            continue

        log(f"\n[USAGE] shard {i}/{len(usage_files)}: {gs_path}")

        try:
            local_file = download_shard(gs_path, RAW_USAGE_DIR)
            df, total_lines, bad_lines = parse_usage_file(local_file)

            rows_written = 0
            if not df.empty:
                save_parquet(df, part_path)
                rows_written = len(df)
                sampled_vm_ids.update(df["vm_id"].unique().tolist())
                save_sampled_vm_ids(sampled_vm_ids)

            mark_processed("usage", shard_name, rows_written, bad_lines)

            log(
                f"[INFO] usage {shard_name}: "
                f"всего строк {total_lines}, оставлено {rows_written}, проблемных {bad_lines}"
            )

            if DELETE_RAW_AFTER_PARSE and local_file.exists():
                local_file.unlink()

        except Exception as e:
            failed_usage.append(shard_name)
            write_failed("usage", shard_name, repr(e))
            log(f"[ERROR] Не удалось обработать usage shard {shard_name}: {repr(e)}")
            log("Переходим к следующему shard.")

    save_sampled_vm_ids(sampled_vm_ids)
    log(f"\nUsage done. Sampled VM count: {len(sampled_vm_ids)}")
    log(f"Ошибок usage: {len(failed_usage)}")

    if failed_usage:
        log("[STOP] Есть ошибки на usage-shards. Перезапусти скрипт позже: он продолжит с необработанных shard.")
        return

    log("\nПолучаем список events-shards...")
    event_files = sorted(gsutil_ls("gs://clusterdata_2019_a/instance_events-*.json.gz"))
    if TEST_MODE:
        event_files = event_files[:MAX_EVENT_SHARDS]
    log(f"Найдено events shard-файлов: {len(event_files)}")

    failed_events = []

    for i, gs_path in enumerate(event_files, 1):
        shard_name = shard_name_from_gs_path(gs_path)
        part_path = EVENTS_PARTS_DIR / part_name_from_shard(shard_name)

        if is_processed("events", shard_name, part_path):
            log(f"[EVENTS] shard {i}/{len(event_files)} уже обработан, пропускаем: {shard_name}")
            continue

        log(f"\n[EVENTS] shard {i}/{len(event_files)}: {gs_path}")

        try:
            local_file = download_shard(gs_path, RAW_EVENTS_DIR)
            df, total_lines, bad_lines = parse_events_file(local_file, sampled_vm_ids)

            rows_written = 0
            if not df.empty:
                save_parquet(df, part_path)
                rows_written = len(df)

            mark_processed("events", shard_name, rows_written, bad_lines)

            log(
                f"[INFO] events {shard_name}: "
                f"всего строк {total_lines}, оставлено {rows_written}, проблемных {bad_lines}"
            )

            if DELETE_RAW_AFTER_PARSE and local_file.exists():
                local_file.unlink()

        except Exception as e:
            failed_events.append(shard_name)
            write_failed("events", shard_name, repr(e))
            log(f"[ERROR] Не удалось обработать events shard {shard_name}: {repr(e)}")
            log("Переходим к следующему shard.")

    log(f"\nEvents done. Ошибок events: {len(failed_events)}")

    if failed_events:
        log("[STOP] Есть ошибки на events-shards. Перезапусти скрипт позже: он продолжит с необработанных shard.")
        return

    build_combined_outputs()


if __name__ == "__main__":
    main()
