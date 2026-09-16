# QC Lakehouse: Data Engineering Best Practices Evaluation

Evaluated against 17 practices spanning code-level discipline, architecture/system design,
and efficiency techniques. Covers all 6 completed sub-projects (A, B1/W1a, C, E1, H) as of
2026-09-16, after the same day's independent review + fix pass and the `no-mistakes` gate run
(PR: https://github.com/AnuragKuche2/qc-workflow/pull/1, all local gates clean).

## The 17 practices

### Code-level practices

1. **Idempotency by default** - every write should be safely re-runnable (`overwrite`/
   `CREATE OR REPLACE`/deterministic upserts on a natural key), never `append`-only unless
   there's a real dedup mechanism.
2. **Explicit schemas, never inferred** - declare and enforce column types at write time;
   catch precision/type drift before it lands, not after.
3. **Assert invariants close to where data is produced**, not just downstream - a check
   before a write beats a dashboard alert three hops later.
4. **Verify invariants twice, by two independent mechanisms, at two layers** - if both agree,
   something's actually been proven; if only one exists, it's only been asserted.
5. **No `SELECT *`, ever** - explicit column lists make schema changes visible in diffs and
   stop silent column leakage across layers.
6. **Tests that prove a check can fail, not just that it can pass** - pair every invariant
   check with a test that deliberately breaks it.

### Architecture / system design principles

7. **Medallion (or equivalent staged) architecture with mechanically enforced boundaries** -
   enforced by schema-routing or catalog boundaries, not just naming convention.
8. **Data contracts at every producer/consumer boundary** - a schema a downstream consumer
   can rely on, ideally enforced, not just documented in a README.
9. **Separation of orchestration from computation** - the scheduler triggers and sequences,
   never transforms.
10. **Design for exactly the failure mode your platform actually has** - retry/checkpoint
    logic matched to the real, observed failure signature, not a generic pattern.
11. **Isolate experimental/benchmark work from production code paths** - and if any of it
    becomes production, move it out of the "lab" directory.
12. **Every layout/architecture decision should be reversible or loudly announced** - owned
    by the system of record (a model config, IaC state), never remembered by only one script.

### Efficiency techniques

13. **Predicate pushdown via physical layout, matched to real query patterns** - chosen by
    measuring bytes-scanned, not guessing.
14. **Compaction (`OPTIMIZE`) as a first-class scheduled operation**, separate from
    clustering - treated as routine maintenance, not an afterthought.
15. **Measure cost with real signals, not proxies asserted as if they were measured** - and
    when the real signal can't be attributed at the granularity wanted, say so explicitly.
16. **Route each operation to the cheapest capable compute tier** - don't pay
    scalable-cluster prices for a row count.
17. **Retention and cleanup as a designed lifecycle, not a manual chore** - VACUUM schedules,
    TTLs on scratch schemas, explicit drop-after-use boundaries.

## Evaluation

| # | Practice | Status | Evidence |
|---|---|---|---|
| 1 | Idempotency by default | Done | Generator: fixed-seed `overwrite`. dbt: `CREATE OR REPLACE TABLE`. Live-verified via induced-rerun tests. |
| 2 | Explicit schemas at write time | Partial | `fact_writer._write()` asserts schema before every Python write - strong. dbt marts have no `contract: enforced`/`data_type:` - `fct_orders` (what G's text-to-SQL will bind against) has an unpinned schema. |
| 3 | Invariants asserted close to production | Done | 5 Spark-native checks run before any fact-table write, not after. |
| 4 | Invariants verified twice, two layers | Partial | Money-chain: yes, both Python (bronze) and dbt singular tests (gold), including `refund_amount <= order_total` added this session. PII boundary ("full_name/lat/lon stay unmasked") is documented in 3 places but tested in zero. |
| 5 | No `SELECT *` | Done | Verified via grep across all 24 dbt models. |
| 6 | Tests that prove a failure is catchable | Done | All 12 `fact_writer` tests are matched pairs (passes-on-good / catches-a-break). |
| 7 | Mechanically enforced medallion boundaries | Partial | Schema routing (`generate_schema_name` macro) makes bronze/silver/gold real, not just naming convention. But `perf_lab/run_maintenance.py` + `apply_winning_layout.py` are now production code (real deployed Jobs touching real `fct_orders`) sitting in a directory named and documented as a throwaway lab. |
| 8 | Data contracts at every boundary | Gap | Zero `not_null`/`unique` tests on any of the 14 tables in `_staging__sources.yml`. No `contract: enforced` anywhere. |
| 9 | Orchestration separated from computation | Done | Both Airflow DAGs contain zero transform logic - pure job-name triggers. |
| 10 | Design for your platform's actual failure mode | Done | H's checkpoint bailout was redesigned around the real observed failure (hard session crash), not a generic retry. Airflow retries tuned and live-tested, not assumed. |
| 11 | Isolate experimental work from production paths | Gap | Same as #7 - the sharpest unresolved architectural seam. |
| 12 | Layout/architecture decisions owned by the system of record | Done (fixed) | Was a real bug (one-shot `ALTER TABLE` would've been silently reverted by dbt's next rebuild) - now `liquid_clustered_by` lives in the dbt model config itself, live-verified surviving a rebuild. |
| 13 | Predicate pushdown matched to real query patterns | Done | `zone_id` chosen and validated via a real 4-way benchmark against representative queries, not guessed. |
| 14 | `OPTIMIZE` as scheduled maintenance | Partial | Maintenance DAG exists and runs. But `ANALYZE TABLE` doesn't collect column-level stats (`FOR ALL COLUMNS`) - the exact stats the optimizer would use for `zone_id` pruning. |
| 15 | Cost measured with real signals | Partial (fixed, still fragile) | `cost_report.py` now genuinely queries `system.billing.usage`, honestly scoped as a warehouse-hour aggregate. No timestamp column on `benchmark_results`, so a future re-run's cost window is approximate (`now - 2h`), not tied to the actual query timestamps. |
| 16 | Route to cheapest capable compute | Done (1 disclosed exception) | Serverless Spark for bulk ops, SQL warehouse only for comparison queries - deliberate, documented. Exception: `apply_winning_layout.py`'s one-time small `OPTIMIZE` uses the warehouse instead of Spark (defensible, but the README doesn't say so). |
| 17 | Retention/cleanup as designed lifecycle | Partial | `qc_dev.perf_bench` genuinely dropped after use, `VACUUM` correctly defaults to 7 days. Zero offline dbt unit tests - all ~110 dbt tests need a live warehouse, real recurring cost/quota exposure every dev cycle. |

## What's worth fixing, ranked

1. **`perf_lab` production/lab seam (#7, #11)** - move `run_maintenance.py` and
   `apply_winning_layout.py` into `scripts/` where the rest of the real, deployed entrypoints
   live. Closes two rows at once.
2. **Data contracts on `fct_orders` + source tests (#2, #8)** - matters most before D and G
   start binding against these tables. `contract: enforced` on the marts, `not_null`/`unique`
   on the 14 bronze source primary keys.
3. **PII boundary test (#4)** - one dbt test (or a Python assertion) that `dim_customer` has
   no raw `email`/`phone` column, turning a documented claim into an enforced one.
4. **Offline dbt unit tests (#17)** - `dbt`'s `unit_tests:` blocks (supported by the installed
   dbt-databricks version) for a handful of the trickiest transforms (`stg_refunds`'s
   accounting-parens parsing, `stg_customers`'s phone masking) would cut live-warehouse
   dependency for routine dev iteration.
5. **`ANALYZE ... FOR ALL COLUMNS` (#14)** and **timestamp the cost window properly (#15)** -
   both small, mechanical fixes.
