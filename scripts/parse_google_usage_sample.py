import gzip
import json
from pathlib import Path
import csv

INPUT_FILE = Path("/Volumes/Smartbuy/google_cluster_2019/data/clusterdata_2019_a/instance_usage/instance_usage-000000000000.json.gz")
OUTPUT_FILE = Path("/Volumes/Smartbuy/google_cluster_2019/data/clusterdata_2019_a/instance_usage_sample_parsed.csv")

FIELDS = [
    "start_time",
    "end_time",
    "collection_id",
    "instance_index",
    "machine_id",
    "average_cpu",
    "average_memory",
    "maximum_cpu",
    "maximum_memory",
    "assigned_memory",
    "page_cache_memory",
    "sample_rate"
]

with gzip.open(INPUT_FILE, "rt", encoding="utf-8") as f_in, open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f_out:
    writer = csv.DictWriter(f_out, fieldnames=FIELDS)
    writer.writeheader()

    for i, line in enumerate(f_in):
        obj = json.loads(line)

        row = {
            "start_time": obj.get("start_time"),
            "end_time": obj.get("end_time"),
            "collection_id": obj.get("collection_id"),
            "instance_index": obj.get("instance_index"),
            "machine_id": obj.get("machine_id"),
            "average_cpu": obj.get("average_usage", {}).get("cpus"),
            "average_memory": obj.get("average_usage", {}).get("memory"),
            "maximum_cpu": obj.get("maximum_usage", {}).get("cpus"),
            "maximum_memory": obj.get("maximum_usage", {}).get("memory"),
            "assigned_memory": obj.get("assigned_memory"),
            "page_cache_memory": obj.get("page_cache_memory"),
            "sample_rate": obj.get("sample_rate"),
        }

        writer.writerow(row)

        if i >= 9999:
            break

print(f"Saved parsed sample to: {OUTPUT_FILE}")
