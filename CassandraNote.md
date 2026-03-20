## Cassandra Benchmark Summary

The following results were collected from `Cassandra.py` using cold-start mode, which truncates the table and invalidates Cassandra key/row caches before each workload run.

### Commands Used

```bash
python Cassandra.py --cold-start --workload aggregation-count --sample-size 10000
python Cassandra.py --cold-start --workload aggregation-sum --sample-size 10000
python Cassandra.py --cold-start --workload balanced --sample-size 10000
python Cassandra.py --cold-start --workload write-heavy --sample-size 10000
python Cassandra.py --cold-start --workload read-heavy --sample-size 10000
```

### Primary Metrics (sample-size = 10,000)

| Workload          | Throughput (ops/s) | Avg Latency (ms) | P95 (ms) | P99 (ms) |
| ----------------- | -----------------: | ---------------: | -------: | -------: |
| aggregation-count |               7.52 |          133.035 |  357.985 |  368.061 |
| aggregation-sum   |               5.08 |          196.743 |  621.757 |  710.352 |
| balanced          |            4896.68 |            0.204 |    0.273 |    0.330 |
| write-heavy       |            5658.95 |            0.177 |    0.238 |    0.299 |
| read-heavy        |            4007.97 |            0.250 |    0.350 |    0.458 |

### Aggregation Workload by Group

#### Aggregation-count

| Group |  Ops | Avg (ms) | P95 (ms) | P99 (ms) | Throughput (ops/s) | Ops Util (%) | Time Util (%) |
| ----- | ---: | -------: | -------: | -------: | -----------------: | -----------: | ------------: |
| A     | 3347 |    4.306 |    4.720 |    5.037 |             232.25 |        33.47 |          1.08 |
| B     | 3291 |   37.066 |   37.999 |   41.113 |              26.98 |        32.91 |          9.17 |
| C     | 3362 |  355.132 |  364.980 |  383.666 |               2.82 |        33.62 |         89.75 |

#### Aggregation-sum

| Group |  Ops | Avg (ms) | P95 (ms) | P99 (ms) | Throughput (ops/s) | Ops Util (%) | Time Util (%) |
| ----- | ---: | -------: | -------: | -------: | -----------------: | -----------: | ------------: |
| A     | 3285 |    6.217 |    8.191 |   12.229 |             160.84 |        32.85 |          1.04 |
| B     | 3352 |   54.679 |   71.393 |   99.931 |              18.29 |        33.52 |          9.32 |
| C     | 3363 |  524.448 |  679.444 |  792.280 |               1.91 |        33.63 |         89.65 |

### Workload-by-Group Metrics

#### Balanced

| Group |  Ops | Avg (ms) | P95 (ms) | P99 (ms) | Throughput (ops/s) | Ops Util (%) | Time Util (%) |
| ----- | ---: | -------: | -------: | -------: | -----------------: | -----------: | ------------: |
| A     | 3341 |    0.202 |    0.275 |    0.327 |            4938.97 |        33.41 |         33.12 |
| B     | 3315 |    0.204 |    0.277 |    0.334 |            4902.90 |        33.15 |         33.11 |
| C     | 3344 |    0.206 |    0.269 |    0.326 |            4849.10 |        33.44 |         33.77 |

#### Write-heavy

| Group |  Ops | Avg (ms) | P95 (ms) | P99 (ms) | Throughput (ops/s) | Ops Util (%) | Time Util (%) |
| ----- | ---: | -------: | -------: | -------: | -----------------: | -----------: | ------------: |
| A     | 3285 |    0.177 |    0.240 |    0.307 |            5646.11 |        32.85 |         32.92 |
| B     | 3358 |    0.177 |    0.240 |    0.297 |            5656.18 |        33.58 |         33.60 |
| C     | 3357 |    0.176 |    0.233 |    0.294 |            5674.35 |        33.57 |         33.48 |

#### Read-heavy

| Group |  Ops | Avg (ms) | P95 (ms) | P99 (ms) | Throughput (ops/s) | Ops Util (%) | Time Util (%) |
| ----- | ---: | -------: | -------: | -------: | -----------------: | -----------: | ------------: |
| A     | 3279 |    0.248 |    0.348 |    0.460 |            4036.94 |        32.79 |         32.55 |
| B     | 3430 |    0.251 |    0.351 |    0.439 |            3976.76 |        34.30 |         34.57 |
| C     | 3291 |    0.249 |    0.351 |    0.477 |            4012.10 |        32.91 |         32.88 |

### Final Aggregation Snapshot (after each run)

#### Aggregation-count run

- Group A: `total_count=10000`, `total_value=10000`
- Group B: `total_count=100000`, `total_value=100000`
- Group C: `total_count=1000000`, `total_value=1000000`

#### Aggregation-sum run

- Group A: `total_count=10000`, `total_value=10000`
- Group B: `total_count=100000`, `total_value=100000`
- Group C: `total_count=1000000`, `total_value=1000000`

#### Balanced run

- Group A: `total_count=11712`, `total_value=858952`
- Group B: `total_count=101655`, `total_value=935368`
- Group C: `total_count=1001643`, `total_value=1820000`

#### Write-heavy run

- Group A: `total_count=13114`, `total_value=1574630`
- Group B: `total_count=103179`, `total_value=1675806`
- Group C: `total_count=1003194`, `total_value=2610975`

#### Read-heavy run

- Group A: `total_count=10165`, `total_value=90729`
- Group B: `total_count=100169`, `total_value=179597`
- Group C: `total_count=1000162`, `total_value=1081653`

### Key Observations

- `aggregation-count` and especially `aggregation-sum` are orders of magnitude slower than read/write workloads at this data scale.
- In both aggregation workloads, Group C dominates runtime (`~89.7%` time utilization) because it has the largest partition (1,000,000 rows).
- Read/write workloads remain fast and balanced across groups (`~33%` ops/time utilization per group).
- Throughput ranking in this experiment: `write-heavy` > `balanced` > `read-heavy` >> `aggregation-count` > `aggregation-sum`.
- Initial base population cost is significant due to dataset size differences:
  - Group A: 10,000 rows
  - Group B: 100,000 rows
  - Group C: 1,000,000 rows
