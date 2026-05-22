from pathlib import Path
import duckdb
import pyarrow.parquet as pq

BASE_DIR = Path("/home/user/predict/google_cluster_a_pipeline")
OUT_DIR = BASE_DIR / "out"

SOURCE = OUT_DIR / "google_a_final_joined_asof.parquet"

FINAL_USAGE = OUT_DIR / "google_a_usage_normalized.parquet"
VM_RESOURCES = OUT_DIR / "google_a_vm_resources.parquet"

TMP_DIR = OUT_DIR / "duckdb_tmp_final_slim"
TMP_DIR.mkdir(parents=True, exist_ok=True)

print("=== GOOGLE CLUSTERDATA 2019 A: FINAL DATASETS ===")
print("Source:", SOURCE)
print("Usage output:", FINAL_USAGE)
print("VM resources output:", VM_RESOURCES)

if not SOURCE.exists():
    raise FileNotFoundError(f"Не найден исходный файл: {SOURCE}")

pf = pq.ParquetFile(SOURCE)
print("Исходный файл найден.")
print("Строк:", pf.metadata.num_rows)
print("Размер GB:", round(SOURCE.stat().st_size / 1024**3, 2))

con = duckdb.connect(database=str(OUT_DIR / "duckdb_make_final_slim.db"))

con.execute("PRAGMA threads=8;")
con.execute(f"PRAGMA temp_directory='{TMP_DIR.as_posix()}';")
con.execute("PRAGMA memory_limit='45GB';")

print("\n[1/2] Создаем основную таблицу с нормализованной нагрузкой...")

con.execute(f"""
COPY (
    SELECT
        vm_id,

        CAST(trace_minute AS FLOAT) AS trace_minute,

        CAST(avg_cpu_ratio_to_requested AS FLOAT) AS average_cpu,
        CAST(max_cpu_ratio_to_requested AS FLOAT) AS maximum_cpu,
        CAST(max_memory_ratio_to_requested AS FLOAT) AS maximum_memory,

        CAST(event_time AS BIGINT) AS event_time

    FROM read_parquet('{SOURCE.as_posix()}')

    WHERE
        vm_id IS NOT NULL
        AND trace_minute IS NOT NULL
        AND avg_cpu_ratio_to_requested IS NOT NULL
        AND max_cpu_ratio_to_requested IS NOT NULL
        AND max_memory_ratio_to_requested IS NOT NULL
)
TO '{FINAL_USAGE.as_posix()}'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD,
    ROW_GROUP_SIZE 1000000
);
""")

print("[OK] Основная таблица сохранена:", FINAL_USAGE)

print("\n[2/2] Создаем справочник VM -> requested_cpu/requested_ram...")

con.execute(f"""
COPY (
    SELECT
        vm_id,
        CAST(MAX(requested_cpu) AS FLOAT) AS requested_cpu,
        CAST(MAX(requested_ram) AS FLOAT) AS requested_ram

    FROM read_parquet('{SOURCE.as_posix()}')

    WHERE
        vm_id IS NOT NULL
        AND requested_cpu IS NOT NULL
        AND requested_ram IS NOT NULL

    GROUP BY vm_id
)
TO '{VM_RESOURCES.as_posix()}'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD,
    ROW_GROUP_SIZE 1000000
);
""")

print("[OK] Справочник ресурсов сохранен:", VM_RESOURCES)

con.close()

print("\n=== Проверка итоговых файлов ===")

for path in [FINAL_USAGE, VM_RESOURCES]:
    pf = pq.ParquetFile(path)
    print("\nФайл:", path.name)
    print("Размер GB:", round(path.stat().st_size / 1024**3, 2))
    print("Строк:", pf.metadata.num_rows)
    print("Row groups:", pf.metadata.num_row_groups)
    print("Колонки:", pf.schema.names)

print("\nГотово.")
