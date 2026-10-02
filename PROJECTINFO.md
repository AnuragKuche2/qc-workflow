# Project Info: Known Gaps & Open Decisions

A living tracker for QC Lakehouse's open issues and pending decisions - things that are
true right now, not aspirational. See [`README.md`](README.md) for the project pitch and
architecture, and `docs/superpowers/reports/2026-09-16-best-practices-evaluation.md` for the
full 17-practice scorecard this list is drawn from.

## Roadmap to completion (NOT done - project is not "complete" without these)

Explicitly recorded per the user's own instruction (2026-09-18): this project is not to be
considered complete at any stage until all of the below land, on top of the "Open gaps" list
further down. Most of this is already in the design spec
(`docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md`) as deferred/
not-yet-built sub-projects, not new scope - cross-referenced below so this doesn't drift from
that spec.

1. **Streaming (Sub-projects B2/W1b + E2)** - spec section 4 explicitly tags these
   `**streaming**` and marks them "deferred - not yet designed": Auto Loader ingestion of
   `order_events`/`courier_shifts`/`gps_pings` (B2/W1b), plus an Airflow addition to trigger
   and monitor that streaming job (E2). The current pipeline is batch-only; this is the
   single biggest gap against "Databricks/Spark" as headline portfolio skills, since
   Structured Streaming is commonly expected even for otherwise-batch-first DE roles.
2. **Orchestration hardening (beyond E2's narrow scope)** - the spec's E2 is scoped only to
   "trigger/monitor the streaming job," not general production-orchestration maturity. Adding
   this as new scope, not yet speced anywhere: at least one scheduled (not manual-trigger-only)
   DAG, failure alerting (even a simple Slack/email callback), and ideally a documented SLA on
   at least one task. Update (2026-09-18): the two DAGs this produced (`qc_lakehouse_pipeline`,
   `qc_lakehouse_maintenance`) were subsequently merged into one medallion-shaped DAG -
   having two DAGs meant maintenance ran on its own schedule with no dependency on whether
   the pipeline that produces the gold tables it maintains had ever succeeded, and the
   pipeline DAG's `dbt_run`/`dbt_test` pair built the entire dbt project before testing any
   of it. See `docs/superpowers/specs/2026-09-18-qc-lakehouse-medallion-dag-redesign.md`. Flagged in the 2026-09-18 portfolio review as the cheapest
   high-leverage fix available given "Airflow" is a headline skill but both current DAGs are
   manual-trigger-only with no scheduling or alerting.
3. **AI/RAG layer (Sub-project D)** - spec section 4: Claude-powered review-issue
   classification (late delivery, food temperature/quality, wrong order, packaging, courier
   behavior), aggregable by restaurant/city/cuisine; embeddings + Databricks Vector Search/AI
   Functions for retrieval. Depends on C (done) and needs synthetic review/comment data that
   doesn't exist yet (noted as an open item in spec section 9).
4. **Text-to-SQL analytics agent (Sub-project G)** - spec section 4: a custom Claude-based
   agent (natural-language question -> generated SQL -> executed against gold marts -> answer),
   with its own prompting/schema-context/validation - explicitly not Databricks Genie. Depends
   on C (done); D is optional enrichment for it.
5. **Polish (Sub-project F)** - spec section 4: CI via GitHub Actions, Free Edition
   credit/cost guardrails, README/narrative, and an **optional Streamlit dashboard**. Per the
   2026-09-18 review, treat the Streamlit dashboard as required, not optional, for this
   project specifically - it's the most direct fix for the "the 500x benchmark tooling has no
   real downstream consumer" criticism, and gives the AI/text-to-SQL layer (D, G) an actual
   interface once those exist.
6. **A self-defensibility pass on the AI-agent-built work** - not a code artifact, a process
   gap: per the 2026-09-18 portfolio review, be able to explain the core design decisions in
   this repo (the ID-block collision scheme, the Liquid Clustering benchmark methodology, the
   PII salting choice, the checkpoint/bailout design) from memory, unaided, before treating
   any of this as interview-ready. This is the highest-risk item in the whole roadmap and the
   only one that isn't fixable by writing more code.

Build order for the above, per the existing spec's fixed order (A->B->C->E->H->D->G->F) plus
the two new additions: **streaming (B2/W1b, E2) and orchestration hardening should land before
D, G, F**, since D/G's own Airflow triggers get folded into E's DAG per spec section 4, and
that DAG should already be mature (scheduled, alerting) before adding more triggers to it.

## Open gaps, ranked

### 1. `perf_lab` production/lab seam (not fixed)

`perf_lab/` was designed as a throwaway "lab" directory for one-off benchmark/analysis
scripts, isolated from the real, deployed pipeline. Two of its scripts stopped being
throwaway and became real, permanently-deployed Databricks Jobs that run against production
data on a schedule:

- `perf_lab/run_maintenance.py` - runs `OPTIMIZE`/`ANALYZE`/`VACUUM` on all 7 real gold
  tables (one table per job run), triggered by the real `qc_lakehouse_pipeline` Airflow DAG (part of the merged medallion DAG).
- `perf_lab/apply_winning_layout.py` - applied the Liquid Clustering decision to the real
  `fct_orders` table.

Both are wired into `databricks.yml` as real jobs (`optimize_gold_table`, `analyze_gold_table`,
`vacuum_gold_table`). The mismatch: the directory name and its original design intent still
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
