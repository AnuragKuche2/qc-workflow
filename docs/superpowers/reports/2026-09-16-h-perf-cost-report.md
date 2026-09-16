# Sub-project H: Layout & Cost Comparison Report

Generated: 2026-09-16T14:21:28.810645+00:00
Benchmark scale: 73,723,047 rows (target was 500x baseline; see Task 1's actual result)

## Results by layout (lower bytes-scanned = better query efficiency)

| Layout | Total duration (ms) | Total bytes scanned | Missing bytes-scanned rows |
|---|---|---|---|
| orders_bench_liquid | 7523 | 420,340,485 | 0 |
| orders_bench_zorder | 8454 | 421,585,617 | 0 |
| orders_bench_partitioned | 6899 | 552,189,357 | 0 |
| orders_bench_baseline | 7879 | 616,350,363 | 0 |

## Recommendation

**orders_bench_liquid** scanned the fewest total bytes across the benchmark query set (420,340,485 bytes), making it the recommended layout for the real `fct_orders` table.
