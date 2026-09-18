# Project Info: Known Gaps & Open Decisions

A living tracker for QC Lakehouse's open issues and pending decisions - things that are
true right now, not aspirational. See [`README.md`](README.md) for the project pitch and
architecture, and `docs/superpowers/reports/2026-09-16-best-practices-evaluation.md` for the
full 17-practice scorecard this list is drawn from.

## Open gaps, ranked

### 1. `perf_lab` production/lab seam (not fixed)

`perf_lab/` was designed as a throwaway "lab" directory for one-off benchmark/analysis
scripts, isolated from the real, deployed pipeline. Two of its scripts stopped being
throwaway and became real, permanently-deployed Databricks Jobs that run against production
data on a schedule:

- `perf_lab/run_maintenance.py` - runs `OPTIMIZE`/`ANALYZE`/`VACUUM` on the real `fct_orders`
  table, triggered by the real `qc_lakehouse_maintenance` Airflow DAG.
- `perf_lab/apply_winning_layout.py` - applied the Liquid Clustering decision to the real
  `fct_orders` table.

Both are wired into `databricks.yml` as real jobs (`optimize_fct_orders`, `analyze_fct_orders`,
`vacuum_fct_orders`). The mismatch: the directory name and its original design intent still
say "throwaway lab work, safe to ignore or delete," but two of its files are load-bearing
infrastructure. Someone skimming the repo would reasonably (and wrongly) assume nothing in
`perf_lab/` matters in production.

**Fix (mechanical, not yet done):** move `run_maintenance.py` and `apply_winning_layout.py`
into `scripts/` (alongside the other real, deployed entry points -
`generate_reference_data.py`, `generate_fact_data.py`, etc.), update `databricks.yml`'s
`python_file:` paths to match. Leave the genuinely-experimental files
(`generate_benchmark_orders.py`, `run_benchmark_queries.py`, `cost_report.py`, `_shared.py`,
`apply_layouts.py`) in `perf_lab/`, where the name is still accurate.

### 2. ~800M rows of unresolved benchmark scratch data (decision pending)

This project's own design philosophy is quota-conscious (Databricks Free Edition has real
compute/storage limits, no cheap way to know you're close to them until a hard failure).
After the *original* Sub-project H benchmark, `qc_dev.perf_bench` was deliberately dropped
once the Liquid Clustering decision was made - the README said so explicitly.

On 2026-09-17, that same schema was re-created and grown to reach the 500x benchmark target:
`qc_dev.perf_bench.orders_bench` now holds **799,389,745 rows (~561x baseline)**. Correction to
an earlier version of this note: the original 4 layout-comparison tables
(`orders_bench_baseline`/`orders_bench_zorder`/`orders_bench_partitioned`/`orders_bench_liquid`)
were *not* still around at 73.7M rows as first assumed here - they'd already been dropped
during Sub-project H's original cleanup, only `orders_bench` itself survived. On 2026-09-18 all
4 were rebuilt from the full 799M-row table and the layout benchmark was re-run for real (see
"Already resolved" below) - so `qc_dev.perf_bench` now holds `orders_bench` plus all 4 rebuilt
layout tables, all at 561x scale. Nobody has decided what happens to any of it now.

**Two honest options, no default assumed:**
1. **Drop everything in `qc_dev.perf_bench`** - matches the project's own stated design, frees
   up quota/storage. The generator's own docstring still calls this "a one-time analysis," and
   the layout re-verification (the main reason to keep it around) is now done.
2. **Keep it** - if there's a plan to use it for another stress test (relevant to Sub-projects
   D/G/F, not yet built). The README's "schema was dropped" claim has already been corrected
   to state the schema currently exists, pending this decision.

**Status: unresolved as of this writing.**

## Already resolved (for continuity - don't re-litigate)

- Root-level `README.md` + Mermaid architecture diagram: done (2026-09-17).
- Data contracts (`contract: enforced` on all 7 marts) + `not_null`/`unique` on all 14 bronze
  source keys: done, live-validated (2026-09-17).
- 500x benchmark scale: done, 799,389,745 rows, zero ID collisions, reached via 9
  independently-cancellable Databricks Job runs (2026-09-17) - see
  `qc-lakehouse/perf_lab/generate_benchmark_orders.py`'s module docstring for the full
  incident history (a live data-corruption incident from an unkillable remote session,
  diagnosed and fixed) and `docs/superpowers/reports/2026-09-16-h-perf-cost-report.md`'s
  addendum for the scale-extension details.
- Layout benchmark re-verified at 561x scale (2026-09-18): all 4 layouts rebuilt from the full
  799M-row table and the comparison queries re-run for real (not just reasoned about). Liquid
  Clustering still wins, same ranking as the original 51.7x run - see the report's "Addendum
  2" for the full numbers, including the one honest nuance (its margin over baseline narrowed
  from 47% to 25% at the larger scale, though the decision direction didn't change).
  `apply_layouts.py` also hit and was fixed for the same session-duration failure mode as the
  generator - now one layout per Databricks Job invocation, same pattern.

## Still open, lower priority

- PII boundary test: `dim_customer` having no raw `email`/`phone` column is documented in 3
  places, tested in zero.
- Offline dbt unit tests: all ~110 dbt tests still require a live warehouse.
- `ANALYZE ... FOR ALL COLUMNS` on `fct_orders` (currently doesn't collect column-level stats).
- Cost report's window is still an approximate `now - 2h`, not tied to actual query
  timestamps.
