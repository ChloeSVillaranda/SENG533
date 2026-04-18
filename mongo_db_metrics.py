import argparse
import json
import random
import statistics
import time
from collections import defaultdict

from pymongo import MongoClient

try:
    import psutil
except ImportError:
    psutil = None

DB_NAME = "performance_test"
COLLECTION_NAME = "measurements"


def _parse_host_port(entry: str, default_port: int) -> tuple:
    """Allow host:port per entry (e.g. 127.0.0.1:27018) or host with shared default_port."""
    if entry.count(":") == 1 and entry.rsplit(":", 1)[1].isdigit():
        host, p = entry.rsplit(":", 1)
        return host, int(p)
    return entry, default_port


def build_mongo_uri(hosts, port, replica_set=None):
    if not hosts:
        raise ValueError("At least one --contact-point is required")
    host_ports = ",".join(f"{h}:{p}" for h, p in (_parse_host_port(e, port) for e in hosts))
    uri = f"mongodb://{host_ports}/"
    if replica_set:
        sep = "?" if "?" not in uri else "&"
        uri = f"{uri}{sep}replicaSet={replica_set}"
    return uri


def get_collection(client):
    return client[DB_NAME][COLLECTION_NAME]


def setup_schema(collection):
    collection.create_index("group_name")


def read_data(data_file):
    with open(data_file, "r") as f:
        return json.load(f)


def doc_from_item(item):
    return {
        "group_name": item["group"],
        "id": str(item["id"]),
        "value": int(item["value"]),
    }


def insert_group_data(collection, data, group, batch_size=10_000):
    data_group = [item for item in data if item["group"] == group]
    if not data_group:
        raise ValueError(f"No data found for group '{group}'")

    docs = [doc_from_item(item) for item in data_group]
    n = len(docs)
    start = time.perf_counter()
    milestone = 100_000
    for i in range(0, n, batch_size):
        chunk = docs[i : i + batch_size]
        if not chunk:
            continue
        collection.insert_many(chunk, ordered=False)
        done = min(i + len(chunk), n)
        if n >= 50_000 and done >= milestone:
            print(f"  Group {group}: {done}/{n} rows inserted (bulk)...", flush=True)
            while milestone <= done:
                milestone += 100_000
    elapsed = time.perf_counter() - start
    return elapsed, n


def aggregation_query(collection, group):
    start = time.perf_counter()
    total_count = collection.count_documents({"group_name": group})
    pipeline = [
        {"$match": {"group_name": group}},
        {"$group": {"_id": "$group_name", "total_value": {"$sum": "$value"}}},
    ]
    rows = list(collection.aggregate(pipeline))
    elapsed = time.perf_counter() - start
    total_value = rows[0]["total_value"] if rows else None
    return elapsed, total_value, total_count


def truncate_collection(collection):
    collection.delete_many({})


def measure_workload(collection, groups, workload_type, sample_size):
    latencies = []
    ops = 0
    per_group_latencies = defaultdict(list)
    per_group_ops = defaultdict(int)

    for i in range(sample_size):
        if workload_type == "read-heavy":
            action = "read" if random.random() < 0.95 else "write"
        elif workload_type == "write-heavy":
            action = "write" if random.random() < 0.95 else "read"
        elif workload_type == "balanced":
            action = "read" if random.random() < 0.5 else "write"
        elif workload_type == "aggregation":
            action = "aggregation"
        elif workload_type == "aggregation-count":
            action = "aggregation-count"
        elif workload_type == "aggregation-sum":
            action = "aggregation-sum"
        else:
            raise ValueError("Invalid workload type")

        group = random.choice(groups)

        if action == "read":
            start = time.perf_counter()
            _ = collection.find_one({"group_name": group})
            elapsed = time.perf_counter() - start
        elif action == "write":
            payload = {
                "group_name": group,
                "id": str(int(time.time() * 1000) + i),
                "value": random.randint(1, 1000),
            }
            start = time.perf_counter()
            collection.insert_one(payload)
            elapsed = time.perf_counter() - start
        elif action == "aggregation-count":
            start = time.perf_counter()
            _ = collection.count_documents({"group_name": group})
            elapsed = time.perf_counter() - start
        elif action == "aggregation-sum":
            start = time.perf_counter()
            _ = list(
                collection.aggregate(
                    [
                        {"$match": {"group_name": group}},
                        {"$group": {"_id": "$group_name", "total_value": {"$sum": "$value"}}},
                    ]
                )
            )
            elapsed = time.perf_counter() - start
        else:
            start = time.perf_counter()
            _ = collection.count_documents({"group_name": group})
            _ = list(
                collection.aggregate(
                    [
                        {"$match": {"group_name": group}},
                        {"$group": {"_id": "$group_name", "total_value": {"$sum": "$value"}}},
                    ]
                )
            )
            elapsed = time.perf_counter() - start

        latencies.append(elapsed)
        per_group_latencies[group].append(elapsed)
        per_group_ops[group] += 1
        ops += 1

    total_latency_s = sum(latencies)
    per_group_metrics = {}
    for group in groups:
        group_latencies = per_group_latencies[group]
        group_ops = per_group_ops[group]
        if not group_latencies:
            per_group_metrics[group] = {
                "ops": 0,
                "avg_latency_ms": 0.0,
                "p95_latency_ms": 0.0,
                "p99_latency_ms": 0.0,
                "throughput_ops_s": 0.0,
                "ops_utilization_pct": 0.0,
                "time_utilization_pct": 0.0,
            }
            continue

        group_total_latency_s = sum(group_latencies)
        group_avg = statistics.mean(group_latencies)
        group_p95 = (
            statistics.quantiles(group_latencies, n=100)[94]
            if len(group_latencies) >= 100
            else max(group_latencies)
        )
        group_p99 = (
            statistics.quantiles(group_latencies, n=100)[98]
            if len(group_latencies) >= 100
            else max(group_latencies)
        )

        per_group_metrics[group] = {
            "ops": group_ops,
            "avg_latency_ms": human_ms(group_avg),
            "p95_latency_ms": human_ms(group_p95),
            "p99_latency_ms": human_ms(group_p99),
            "throughput_ops_s": (group_ops / group_total_latency_s) if group_total_latency_s > 0 else 0.0,
            "ops_utilization_pct": (group_ops / ops * 100) if ops > 0 else 0.0,
            "time_utilization_pct": (group_total_latency_s / total_latency_s * 100) if total_latency_s > 0 else 0.0,
        }

    return latencies, ops, per_group_metrics


def resource_snapshot():
    if not psutil:
        return None
    disk = psutil.disk_io_counters()
    return {
        "cpu_percent": psutil.cpu_percent(interval=None),
        "mem_percent": psutil.virtual_memory().percent,
        "disk_read_bytes": disk.read_bytes,
        "disk_write_bytes": disk.write_bytes,
    }


def human_ms(seconds):
    return seconds * 1000


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MongoDB performance measurement (Cassandra.py-style)")
    parser.add_argument("--data-file", default="data.json", help="JSON file with data")
    parser.add_argument(
        "--workload",
        choices=[
            "read-heavy",
            "write-heavy",
            "balanced",
            "aggregation",
            "aggregation-count",
            "aggregation-sum",
        ],
        default="balanced",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10000,
        help="Number of operations to simulate per workload",
    )
    parser.add_argument("--clear-table", action="store_true", help="Delete all documents before running")
    parser.add_argument("--no-populate", action="store_true", help="Skip initial base data population")
    parser.add_argument(
        "--populate-batch-size",
        type=int,
        default=10_000,
        metavar="N",
        help="Documents per insert_many during population (default 10000; larger can be faster)",
    )
    parser.add_argument(
        "--cold-start",
        action="store_true",
        help="Clear collection before run (Mongo has no nodetool; same as clear for local cold data)",
    )
    parser.add_argument(
        "--contact-points",
        nargs="+",
        default=["127.0.0.1"],
        help="Hosts or host:port (e.g. 127.0.0.1:27018). Omit :port to use --port for that host.",
    )
    parser.add_argument("--port", type=int, default=27017, help="Default port when a contact point has no :port")
    parser.add_argument(
        "--replica-set",
        default=None,
        help="Replica set name when using a replica set (required for multi-host RS URIs)",
    )
    parser.add_argument(
        "--uri",
        default=None,
        help="Full MongoDB URI (overrides --contact-points / --port / --replica-set)",
    )
    args = parser.parse_args()

    uri = args.uri or build_mongo_uri(args.contact_points, args.port, args.replica_set)
    client = MongoClient(uri, serverSelectionTimeoutMS=60_000)
    collection = get_collection(client)

    setup_schema(collection)

    if args.clear_table or args.cold_start:
        print("Clearing collection before run ...")
        truncate_collection(collection)

    groups = ["A", "B", "C"]
    group_stats = {}

    if args.no_populate:
        print("Skipping base population (--no-populate enabled) ...")
    else:
        data = read_data(args.data_file)
        print("Populating base data for groups A/B/C ...")
        for group in groups:
            elapsed, count = insert_group_data(
                collection, data, group, batch_size=max(1, args.populate_batch_size)
            )
            group_stats[group] = {"insert_time_s": elapsed, "row_count": count}
            print(f"Group {group}: inserted {count} rows in {elapsed:.4f}s")

    print("Running workload measurement...")
    before_res = resource_snapshot()
    latencies, total_ops, per_group_metrics = measure_workload(
        collection, groups, args.workload, args.sample_size
    )
    after_res = resource_snapshot()

    avg_lat = statistics.mean(latencies)
    p95_lat = statistics.quantiles(latencies, n=100)[94] if len(latencies) >= 100 else max(latencies)
    p99_lat = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies)
    throughput_ops = total_ops / sum(latencies) if sum(latencies) > 0 else 0.0

    print("\n=== PRIMARY METRICS ===")
    print(f"Workload type: {args.workload}")
    print(f"Total operations: {total_ops}")
    print(f"Throughput (ops/s): {throughput_ops:.2f}")
    print(f"Average latency: {human_ms(avg_lat):.3f} ms")
    print(f"95th latency: {human_ms(p95_lat):.3f} ms")
    print(f"99th latency: {human_ms(p99_lat):.3f} ms")

    if before_res and after_res:
        print("\n=== RESOURCE UTILIZATION CHANGES ===")
        print(f"CPU percent (snapshot after): {after_res['cpu_percent']}%")
        print(f"Memory percent (after): {after_res['mem_percent']}%")
        dr = after_res["disk_read_bytes"] - before_res["disk_read_bytes"]
        dw = after_res["disk_write_bytes"] - before_res["disk_write_bytes"]
        print(f"Disk read bytes + write bytes diff: {dr + dw}")

    print("\n=== WORKLOAD BY GROUP ===")
    for g in groups:
        m = per_group_metrics[g]
        print(
            f"Group {g}: ops={m['ops']} "
            f"avg={m['avg_latency_ms']:.3f}ms "
            f"p95={m['p95_latency_ms']:.3f}ms "
            f"p99={m['p99_latency_ms']:.3f}ms "
            f"throughput={m['throughput_ops_s']:.2f} ops/s "
            f"ops_util={m['ops_utilization_pct']:.2f}% "
            f"time_util={m['time_utilization_pct']:.2f}%"
        )

    print("\n=== GROUP SUMMARY ===")
    for g, stats in group_stats.items():
        print(f"Group {g}: {stats['row_count']} rows inserted in {stats['insert_time_s']:.4f}s")

    for group in groups:
        agg_time, total_val, total_count = aggregation_query(collection, group)
        print(
            f"Aggregation group {group}: "
            f"time={agg_time:.4f}s total_count={total_count} total_value={total_val}"
        )

    client.close()
