# SENG533

repository for our 533

## Testing Cassandra Cluster Sizes and Workloads

You can test the Cassandra performance script with different cluster sizes, workloads, and data sizes using the `Cassandra.py` script.

### Usage

```
python Cassandra.py --contact-points <node1> [<node2> ...] --workload <workload_type> --sample-size <N>
```

- `--contact-points`: List the IPs or hostnames of your Cassandra nodes (space-separated).
- `--workload`: Type of workload to test. Options: `read-heavy`, `write-heavy`, `balanced`, `aggregation`, `aggregation-count`, `aggregation-sum`.
- `--sample-size`: Number of operations to simulate per workload.
- `--data-file`: (Optional) Path to your data file (default: `data.json`).
- `--clear-table`: (Optional) Truncate the table before running.
- `--no-populate`: (Optional) Skip initial base data population.
- `--cold-start`: (Optional) Truncate table and invalidate Cassandra caches before workload.

### Examples

**1-node cluster:**
```
python Cassandra.py --contact-points 127.0.0.1 --workload balanced --sample-size 1000
```

**3-node cluster:**
```
python Cassandra.py --contact-points 127.0.0.1 127.0.0.2 127.0.0.3 --workload read-heavy --sample-size 2000
```

**9-node cluster:**
```
python Cassandra.py --contact-points 127.0.0.1 127.0.0.2 127.0.0.3 127.0.0.4 127.0.0.5 127.0.0.6 127.0.0.7 127.0.0.8 127.0.0.9 --workload write-heavy --sample-size 5000
```

Review the output metrics to compare performance across different cluster sizes and workloads.
