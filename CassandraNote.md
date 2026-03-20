## Cassandra Benchmark Summary

The following results were collected from `Cassandra.py` using cold-start mode, which truncates the table and invalidates Cassandra key/row caches before each workload run.

### Commands Used

```bash
python Cassandra.py --cold-start --workload read-heavy --sample-size 10000
python Cassandra.py --cold-start --workload balanced --sample-size 10000
python Cassandra.py --cold-start --workload write-heavy --sample-size 10000
```

### Primary Metrics (sample-size = 10,000)

| Workload    | Throughput (ops/s) | Avg Latency (ms) | P95 (ms) | P99 (ms) |
| ----------- | -----------------: | ---------------: | -------: | -------: |
| read-heavy  |            4134.14 |            0.242 |    0.340 |    0.448 |
| balanced    |            4863.49 |            0.206 |    0.278 |    0.347 |
| write-heavy |            5643.66 |            0.177 |    0.235 |    0.296 |

### Workload-by-Group Metrics

#### Read-heavy

| Group |  Ops | Avg (ms) | P95 (ms) | P99 (ms) | Throughput (ops/s) | Ops Util (%) | Time Util (%) |
| ----- | ---: | -------: | -------: | -------: | -----------------: | -----------: | ------------: |
| A     | 3266 |    0.244 |    0.338 |    0.445 |            4090.46 |        32.66 |         33.01 |
| B     | 3353 |    0.240 |    0.339 |    0.450 |            4163.19 |        33.53 |         33.30 |
| C     | 3381 |    0.241 |    0.341 |    0.447 |            4148.21 |        33.81 |         33.70 |

#### Balanced

| Group |  Ops | Avg (ms) | P95 (ms) | P99 (ms) | Throughput (ops/s) | Ops Util (%) | Time Util (%) |
| ----- | ---: | -------: | -------: | -------: | -----------------: | -----------: | ------------: |
| A     | 3369 |    0.204 |    0.277 |    0.353 |            4891.27 |        33.69 |         33.50 |
| B     | 3296 |    0.204 |    0.277 |    0.340 |            4896.72 |        32.96 |         32.74 |
| C     | 3335 |    0.208 |    0.280 |    0.348 |            4803.71 |        33.35 |         33.77 |

#### Write-heavy

| Group |  Ops | Avg (ms) | P95 (ms) | P99 (ms) | Throughput (ops/s) | Ops Util (%) | Time Util (%) |
| ----- | ---: | -------: | -------: | -------: | -----------------: | -----------: | ------------: |
| A     | 3352 |    0.177 |    0.234 |    0.292 |            5653.36 |        33.52 |         33.46 |
| B     | 3322 |    0.177 |    0.235 |    0.305 |            5635.83 |        33.22 |         33.27 |
| C     | 3326 |    0.177 |    0.236 |    0.295 |            5641.74 |        33.26 |         33.27 |

### Key Observations

- Throughput increased from read-heavy to write-heavy in these runs (4134.14 -> 5643.66 ops/s).
- Latency decreased from read-heavy to write-heavy (0.242 ms -> 0.177 ms average).
- Group utilization stayed balanced for all workloads, with each group near one-third of total operations and time utilization.
- Initial base population cost is significant due to dataset size differences:
  - Group A: 10,000 rows
  - Group B: 100,000 rows
  - Group C: 1,000,000 rows
