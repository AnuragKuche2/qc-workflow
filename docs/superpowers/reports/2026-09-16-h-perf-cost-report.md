# Sub-project H: Layout & Cost Comparison Report

Generated: 2026-09-16T14:21:28.810645+00:00
Benchmark scale: 73,723,047 rows (~51.7x baseline; the 500x target was not reached - see README)

## Results by layout (lower bytes-scanned = better query efficiency)

| Layout | Total duration (ms) | Total bytes scanned | Missing bytes-scanned rows |
|---|---|---|---|
| orders_bench_liquid | 7523 | 420,340,485 | 0 |
| orders_bench_zorder | 8454 | 421,585,617 | 0 |
| orders_bench_partitioned | 6899 | 552,189,357 | 0 |
| orders_bench_baseline | 7879 | 616,350,363 | 0 |

## Recommendation

**orders_bench_liquid** scanned the fewest total bytes across the benchmark query set (420,340,485 bytes), making it the recommended layout for the real `fct_orders` table.

**Note on duration:** `orders_bench_partitioned` had the lowest total wall-clock duration (6899ms) among the layouts compared, faster than the recommended `orders_bench_liquid` (7523ms). Duration is not used as the decision signal here: each query ran once (no repeated sampling) on a shared, contended 2X-Small warehouse, so wall-clock time is subject to queuing/contention noise. Bytes-scanned is deterministic given the query and physical layout, which is why it - not duration - drives the recommendation above.

## Cost

Aggregate SQL warehouse usage for the two-hour window covering this benchmark's original run and report generation (2026-09-16, 13:00-15:00 UTC - verified via a live `system.billing.usage` query after the fact, not literal output of a single script invocation at the `Generated:` timestamp above): **7.7676 DBU** (~$5.44 at $0.70/DBU, `PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO`).

This is a warehouse-hour aggregate, not a per-layout or per-query cost: `system.billing.usage` buckets consumption by warehouse and hour, with no per-statement or per-query cost column, so this total cannot be split across the 4 layouts compared above - it covers everything the warehouse did in that window (the benchmark queries, the `system.query.history` backfill polling, and this report's own generation queries), not any single layout's cost alone.

Bytes-scanned, not this billing total, is the per-layout decision signal used above: it is captured per query via `system.query.history` (see `run_benchmark_queries.py`), and DBU consumption for serverless SQL scales with compute-time/bytes-processed, so a lower-bytes-scanned layout is the one that would cost less at scale, even though this billing table's granularity can't prove that arithmetically at the level available here.
