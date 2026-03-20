import time
import json
import argparse
import random
import statistics
from cassandra.cluster import Cluster

try:
    import psutil
except ImportError:
    psutil = None

KEYSPACE = "performance_test"
TABLE = "measurements"


def setup_schema(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)

    session.set_keyspace(KEYSPACE)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            group_name text,
            id text,
            value int,
            PRIMARY KEY (group_name, id)
        )
    """)


def read_data(data_file):
    with open(data_file, "r") as f:
        return json.load(f)


def insert_group_data(session, data, group, insert_stmt):
    data_group = [item for item in data if item["group"] == group]
    if not data_group:
        raise ValueError(f"No data found for group '{group}'")

    start = time.perf_counter()
    for item in data_group:
        session.execute(insert_stmt, (item["group"], str(item["id"]), int(item["value"])))
    elapsed = time.perf_counter() - start

    return elapsed, len(data_group)


def aggregation_query(session, group):
    start = time.perf_counter()
    result = session.execute(f"""
        SELECT SUM(value) AS total_value
        FROM {TABLE}
        WHERE group_name = %s
    """, (group,))
    elapsed = time.perf_counter() - start
    row = result.one()
    return elapsed, row.total_value if row else None


def measure_workload(session, groups, workload_type, sample_size):
    latencies = []
    ops = 0
    insert_stmt = session.prepare(f"""
        INSERT INTO {TABLE} (group_name, id, value)
        VALUES (?, ?, ?)
    """)

    # Pre-warm by ensuring data exists for each group
    for group in groups:
        _, _ = insert_group_data(session, data, group, insert_stmt)

    for i in range(sample_size):
        if workload_type == "read-heavy":
            action = "read" if random.random() < 0.95 else "write"
        elif workload_type == "write-heavy":
            action = "write" if random.random() < 0.95 else "read"
        elif workload_type == "balanced":
            action = "read" if random.random() < 0.5 else "write"
        elif workload_type == "aggregation":
            action = "aggregation"
        else:
            raise ValueError("Invalid workload type")

        group = random.choice(groups)

        if action == "read":
            start = time.perf_counter()
            _ = session.execute(f"SELECT * FROM {TABLE} WHERE group_name = %s LIMIT 1", (group,)).one()
            elapsed = time.perf_counter() - start
        elif action == "write":
            payload = {
                "group_name": group,
                "id": str(int(time.time() * 1000) + i),
                "value": random.randint(1, 1000)
            }
            start = time.perf_counter()
            session.execute(insert_stmt, (payload["group_name"], payload["id"], payload["value"]))
            elapsed = time.perf_counter() - start
        else:  # aggregation
            start = time.perf_counter()
            _ = session.execute(f"SELECT SUM(value) AS total_value FROM {TABLE} WHERE group_name = %s", (group,)).one()
            elapsed = time.perf_counter() - start

        latencies.append(elapsed)
        ops += 1

    return latencies, ops


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
    parser = argparse.ArgumentParser(description="Cassandra performance measurement")
    parser.add_argument("--data-file", default="data.json", help="JSON file with data")
    parser.add_argument("--workload", choices=["read-heavy", "write-heavy", "balanced", "aggregation"], default="balanced")
    parser.add_argument("--sample-size", type=int, default=1000, help="Number of operations to simulate per workload")
    args = parser.parse_args()

    # Connect
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    setup_schema(session)
    data = read_data(args.data_file)

    groups = ["A", "B", "C"]

    print("Populating base data for groups A/B/C ...")
    insert_stmt = session.prepare(f"""
        INSERT INTO {TABLE} (group_name, id, value)
        VALUES (?, ?, ?)
    """)

    group_stats = {}
    for group in groups:
        elapsed, count = insert_group_data(session, data, group, insert_stmt)
        group_stats[group] = {"insert_time_s": elapsed, "row_count": count}
        print(f"Group {group}: inserted {count} rows in {elapsed:.4f}s")

    print("Running workload measurement...")
    before_res = resource_snapshot()
    latencies, total_ops = measure_workload(session, groups, args.workload, args.sample_size)
    after_res = resource_snapshot()

    avg_lat = statistics.mean(latencies)
    p95_lat = statistics.quantiles(latencies, n=100)[94] if len(latencies) >= 100 else max(latencies)
    p99_lat = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies)
    throughput_ops = total_ops / sum(latencies)

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
        print(f"Disk read bytes + write bytes diff: {(after_res['disk_read_bytes'] - before_res['disk_read_bytes']) + (after_res['disk_write_bytes'] - before_res['disk_write_bytes'])}")

    print("\n=== GROUP SUMMARY ===")
    for g, stats in group_stats.items():
        print(f"Group {g}: {stats['row_count']} rows inserted in {stats['insert_time_s']:.4f}s")

    # run one final aggregation per group
    for group in groups:
        agg_time, total_val = aggregation_query(session, group)
        print(f"Aggregation group {group}: time={agg_time:.4f}s total_value={total_val}")

    cluster.shutdown()