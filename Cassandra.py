import time
import json
from cassandra.cluster import Cluster

# Setup connection
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

KEYSPACE = "performance_test"
TABLE = "measurements"

# Create keyspace and table
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

# Load JSON data
data_file = "data.json"
groups = ["A", "B", "C"]

with open(data_file, "r") as f:
    data = json.load(f)

# Insert data group-by-group

insert_durations = []

insert_stmt = session.prepare(f"""
    INSERT INTO {TABLE} (group_name, id, value)
    VALUES (?, ?, ?)
""")

for group in groups:
    data_group = [item for item in data if item["group"] == group]

    start_time = time.time()
    for item in data_group:
        session.execute(insert_stmt, (
            item["group"],
            str(item["id"]),
            int(item["value"])
        ))
    duration = time.time() - start_time
    insert_durations.append(duration)

    print(f"Cassandra insertion time for group {group}: {duration:.4f} seconds")

# Aggregation queries: SUM value per group

agg_durations = []

for group in groups:
    start_time = time.time()
    result = session.execute(f"""
        SELECT SUM(value) AS total_value
        FROM {TABLE}
        WHERE group_name = %s
    """, (group,))
    duration = time.time() - start_time
    agg_durations.append(duration)

    row = result.one()
    total_value = row.total_value if row else None

    print(
        f"Cassandra aggregation time for group {group}: "
        f"{duration:.4f} seconds — Result: total_value={total_value}"
    )

print(f"Insertion durations: {insert_durations}")
print(f"Aggregation durations: {agg_durations}")

cluster.shutdown()