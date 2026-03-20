import time
import json
import argparse
import random
import statistics
import subprocess
from collections import defaultdict
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
    count_result = session.execute(
        f"SELECT COUNT(*) AS total_count FROM {TABLE} WHERE group_name = %s",
        (group,)
    )
    sum_result = session.execute(
        f"SELECT SUM(value) AS total_value FROM {TABLE} WHERE group_name = %s",
        (group,)
    )
    elapsed = time.perf_counter() - start
    count_row = count_result.one()
    sum_row = sum_result.one()
    if not count_row and not sum_row:
        return elapsed, None, 0
    total_count = count_row.total_count if count_row else 0
    total_value = sum_row.total_value if sum_row else None
    return elapsed, total_value, total_count


def truncate_table(session):
    session.execute(f"TRUNCATE {TABLE}")


def invalidate_cassandra_caches():
    """Best-effort invalidation of Cassandra key/row caches via nodetool."""
    for cmd in (("nodetool", "invalidatekeycache"), ("nodetool", "invalidaterowcache")):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                stderr = result.stderr.strip() or "unknown error"
                print(f"Warning: {' '.join(cmd)} failed: {stderr}")
        except FileNotFoundError:
            print("Warning: nodetool not found; Cassandra caches were not invalidated")
            return


def measure_workload(session, groups, workload_type, sample_size):
    latencies = []
    ops = 0
    per_group_latencies = defaultdict(list)
    per_group_ops = defaultdict(int)
    insert_stmt = session.prepare(f"""
        INSERT INTO {TABLE} (group_name, id, value)
        VALUES (?, ?, ?)
    """)

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
        elif action == "aggregation-count":
            start = time.perf_counter()
            _ = session.execute(
                f"SELECT COUNT(*) AS total_count FROM {TABLE} WHERE group_name = %s",
                (group,)
            ).one()
            elapsed = time.perf_counter() - start
        elif action == "aggregation-sum":
            start = time.perf_counter()
            _ = session.execute(
                f"SELECT SUM(value) AS total_value FROM {TABLE} WHERE group_name = %s",
                (group,)
            ).one()
            elapsed = time.perf_counter() - start
        else:  # aggregation (count then sum)
            start = time.perf_counter()
            _ = session.execute(
                f"SELECT COUNT(*) AS total_count FROM {TABLE} WHERE group_name = %s",
                (group,)
            ).one()
            _ = session.execute(
                f"SELECT SUM(value) AS total_value FROM {TABLE} WHERE group_name = %s",
                (group,)
            ).one()
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
        group_p95 = statistics.quantiles(group_latencies, n=100)[94] if len(group_latencies) >= 100 else max(group_latencies)
        group_p99 = statistics.quantiles(group_latencies, n=100)[98] if len(group_latencies) >= 100 else max(group_latencies)

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
    parser = argparse.ArgumentParser(description="Cassandra performance measurement")
    parser.add_argument("--data-file", default="data.json", help="JSON file with data")
    parser.add_argument(
        "--workload",
        choices=["read-heavy", "write-heavy", "balanced", "aggregation", "aggregation-count", "aggregation-sum"],
        default="balanced"
    )
    parser.add_argument("--sample-size", type=int, default=1000, help="Number of operations to simulate per workload")
    parser.add_argument("--clear-table", action="store_true", help="Truncate the table before running")
    parser.add_argument("--no-populate", action="store_true", help="Skip initial base data population")
    parser.add_argument(
        "--cold-start",
        action="store_true",
        help="Force cold-start style run: truncate table and invalidate Cassandra key/row caches before workload",
    )
    args = parser.parse_args()

    # Connect
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    setup_schema(session)

    if args.clear_table or args.cold_start:
        print("Clearing table before run ...")
        truncate_table(session)

    groups = ["A", "B", "C"]
    group_stats = {}

    if args.no_populate:
        print("Skipping base population (--no-populate enabled) ...")
    else:
        data = read_data(args.data_file)
        print("Populating base data for groups A/B/C ...")
        insert_stmt = session.prepare(f"""
            INSERT INTO {TABLE} (group_name, id, value)
            VALUES (?, ?, ?)
        """)

        for group in groups:
            elapsed, count = insert_group_data(session, data, group, insert_stmt)
            group_stats[group] = {"insert_time_s": elapsed, "row_count": count}
            print(f"Group {group}: inserted {count} rows in {elapsed:.4f}s")

    if args.cold_start:
        print("Invalidating Cassandra key/row caches before workload ...")
        invalidate_cassandra_caches()

    print("Running workload measurement...")
    before_res = resource_snapshot()
    latencies, total_ops, per_group_metrics = measure_workload(session, groups, args.workload, args.sample_size)
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

    # run one final aggregation per group
    for group in groups:
        agg_time, total_val, total_count = aggregation_query(session, group)
        print(
            f"Aggregation group {group}: "
            f"time={agg_time:.4f}s total_count={total_count} total_value={total_val}"
        )

    cluster.shutdown()