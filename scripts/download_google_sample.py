from pathlib import Path
import requests
import sys

# =========================
# НАСТРОЙКИ
# =========================

CELL = "a"

TABLES = {
    "instance_usage": 5,
    "instance_events": 5,
    # Если потом захочешь докачать, можно добавить:
    # "machine_events": 2,
    # "machine_attributes": 2,
    # "collection_events": 2,
}

BASE_DIR = Path("/Volumes/Smartbuy/google_cluster_2019/data")

# =========================
# ФУНКЦИИ
# =========================

def trace_file_url(cell: str, table: str, index: int) -> str:
    return f"https://storage.googleapis.com/clusterdata_2019_{cell}/{table}-{index:012d}.json.gz"
from pathlib import Path
import requests
import sys
import time

# =========================
# НАСТРОЙКИ
# =========================

CELL = "a"

TABLES = {
    "instance_usage": 5,
    "instance_events": 5,
    # позже можно добавить:
    # "machine_events": 2,
    # "machine_attributes": 2,
    # "collection_events": 2,
}

BASE_DIR = Path("/Volumes/Smartbuy/google_cluster_2019/data")

CONNECT_TIMEOUT = 30
READ_TIMEOUT = 600
MAX_RETRIES = 5
CHUNK_SIZE = 1024 * 1024  # 1 MB

# =========================
# ФУНКЦИИ
# =========================

def trace_file_url(cell: str, table: str, index: int) -> str:
    return f"https://storage.googleapis.com/clusterdata_2019_{cell}/{table}-{index:012d}.json.gz"

def download_file(url: str, out_path: Path):
    temp_path = out_path.with_suffix(out_path.suffix + ".part")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with requests.get(url, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)) as response:
                if response.status_code != 200:
                    print(f"[STOP] Не удалось скачать {url} (status={response.status_code})")
                    return False

                with open(temp_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)

            temp_path.replace(out_path)
            return True

        except requests.exceptions.RequestException as e:
            print(f"[RETRY {attempt}/{MAX_RETRIES}] Ошибка загрузки {url}: {e}")
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass

            if attempt < MAX_RETRIES:
                wait_seconds = 5 * attempt
                print(f"[WAIT] Жду {wait_seconds} сек. и пробую снова...")
                time.sleep(wait_seconds)
            else:
                print(f"[FAIL] Не удалось скачать {url} после {MAX_RETRIES} попыток")
                return False

def main():
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    for table, max_shards in TABLES.items():
        out_dir = BASE_DIR / f"clusterdata_2019_{CELL}" / table
        out_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n=== Таблица: {table} ===")
        for idx in range(max_shards):
            url = trace_file_url(CELL, table, idx)
            filename = f"{table}-{idx:012d}.json.gz"
            out_file = out_dir / filename

            if out_file.exists() and out_file.stat().st_size > 0:
                size_mb = out_file.stat().st_size / (1024 * 1024)
                print(f"[SKIP] Уже скачано: {out_file} ({size_mb:.2f} MB)")
                continue

            print(f"[DOWNLOAD] {url}")
            ok = download_file(url, out_file)
            if not ok:
                print(f"[BREAK] Останавливаюсь на shard {idx} для таблицы {table}")
                break

            size_mb = out_file.stat().st_size / (1024 * 1024)
            print(f"[OK] Сохранено: {out_file} ({size_mb:.2f} MB)")

    print("\nГотово.")
    print(f"Данные лежат здесь: {BASE_DIR}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
        sys.exit(1)
            
