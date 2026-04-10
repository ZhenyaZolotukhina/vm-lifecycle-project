import gzip
import json
from pathlib import Path
import csv

INPUT_FILE = Path("/Volumes/Smartbuy/google_cluster_2019/data/clusterdata_2019_a/instance_events/instance_events-000000000000.json.gz")
OUTPUT_FILE = Path("/Volumes/Smartbuy/google_cluster_2019/data/clusterdata_2019_a/instance_events_sample_parsed.csv")

FIELDS = [
    "time",
    "type",
    "collection_id",
    "instance_index",
    "machine_id",
    "priority",
    "resource_request_cpus",
    "resource_request_memory",
    "alloc_collection_id",
    "alloc_instance_index"
]

with gzip.open(INPUT_FILE, "rt", encoding="utf-8") as f_in, open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f_out:
    writer = csv.DictWriter(f_out, fieldnames=FIELDS)
    writer.writeheader()

    for i, line in enumerate(f_in):
        obj = json.loads(line)

        row = {
            "time": obj.get("time"),
            "type": obj.get("type"),
            "collection_id": obj.get("collection_id"),
            "instance_index": obj.get("instance_index"),
            "machine_id": obj.get("machine_id"),
            "priority": obj.get("priority"),
            "resource_request_cpus": obj.get("resource_request", {}).get("cpus"),
            "resource_request_memory": obj.get("resource_request", {}).get("memory"),
            "alloc_collection_id": obj.get("alloc_collection_id"),
            "alloc_instance_index": obj.get("alloc_instance_index"),
        }

        writer.writerow(row)

        if i >= 9999:
            break

print(f"Saved parsed sample to: {OUTPUT_FILE}")
