# QC Lakehouse: Medallion-Shaped Orchestration Redesign

## Problem

`qc_lakehouse_pipeline` and `qc_lakehouse_maintenance` (built in the 2026-09-18
orchestration-hardening plan) are two separate DAGs. Neither actually orchestrates the
medallion pipeline it sits on top of:

- `qc_lakehouse_pipeline` triggers `generate_reference_data` and `generate_fact_data`
  (bronze ingestion - these are fine), then two opaque Databricks Job tasks: `dbt_run`
  (runs the *entire* dbt project - staging, intermediate, and marts - in one shot) and
  `dbt_test` (tests *everything*, only after all three layers are already built). A bad
  row in staging is not caught until after intermediate and marts have already built on
  top of it. Airflow's task graph shows two blobs, not the medallion layers that actually
  exist in `dbt/qc_lakehouse/models/`.
- `qc_lakehouse_maintenance` runs OPTIMIZE/ANALYZE/VACUUM against the gold tables on its
  own weekly schedule, with no dependency on whether the pipeline that produces those
  tables ever ran, or ever succeeded. It is 7 hand-unrolled, identically-shaped 3-task
  Python-loop chains - a static approximation of "apply this operation to every table"
  rather than a use of Airflow's actual tool for that (dynamic task mapping).

Neither problem is a bug (both DAGs run and complete correctly) - the problem is that the
task graph does not represent real data-engineering control flow: no per-layer gating,
no fail-fast, and maintenance is decoupled from the data it maintains.

## Goals

- One DAG whose task graph visibly matches the medallion architecture: bronze ingest ->
  build+test silver (staging) -> build+test silver (intermediate) -> build+test gold
  (marts) -> maintain gold -> report.
- A staging-layer test failure must block intermediate and marts from building on bad
  data (fail-closed, using dbt's own build+test interleaving inside each layer's job, plus
  Airflow's default `trigger_rule="all_success"` between layer stages).
- Gold-table maintenance runs only after marts has built and passed its tests, using
  Airflow's dynamic task mapping (`.expand()`) instead of a hand-unrolled per-table loop.
- Keep the "at least one scheduled DAG" signal from the 2026-09-18 orchestration-hardening
  work - the merged DAG keeps a real `@weekly` schedule, not `schedule=None`.
- Preserve the Databricks Free Edition quota constraint already designed around:
  `max_concurrent_runs: 1` stays on `optimize_gold_table`/`analyze_gold_table`/
  `vacuum_gold_table` (max 5 concurrent job tasks account-wide; a quota overrun shuts down
  all workspace compute for the day - see `datapipelineplan.md` ADR-001/C16).

## Non-goals

- No change to the dbt SQL itself. Staging/intermediate/marts models already do real
  cleaning (defect-flagging, PII salting) and are already correctly organized by schema
  (`+schema: silver`/`gold` in `dbt_project.yml`). This is an orchestration-visibility
  problem, not a missing-transformation-logic problem.
- No genuinely new external data sources. Bronze ingestion stays the two existing
  synthetic generator jobs (`generate_reference_data`, `generate_fact_data`).
- No Dataset/Asset-based decoupling between build and maintenance (considered and
  rejected - see "Alternative considered" below). One DAG, one schedule.
- No per-table independent maintenance chains (i.e., no attempt to make table B's
  maintenance chain proceed independently of table A's failure via correlated
  `expand(x=upstream.output)` mapping). Barrier semantics between operations (all
  OPTIMIZE done -> all ANALYZE done -> all VACUUM done) is simpler, matches how batch
  maintenance is commonly run in practice, and is what this spec implements. Revisit only
  if a real need for per-table failure isolation emerges.
- No change to `run_maintenance.py`, `optimize_gold_table`/`analyze_gold_table`/
  `vacuum_gold_table`, or their `max_concurrent_runs: 1` settings - those are correct
  as-is; only how Airflow invokes them changes.

## Architecture

```
generate_reference_data
        |
generate_fact_data
        |
dbt_build_test_staging        (dbt build --select staging)
        |
dbt_build_test_intermediate   (dbt build --select intermediate)
        |
dbt_build_test_marts          (dbt build --select marts)
        |
   [maintenance TaskGroup, dynamic task mapping]
   optimize.expand(table=GOLD_TABLES)
        |
   analyze.expand(table=GOLD_TABLES)
        |
   vacuum.expand(table=GOLD_TABLES)
        |
     report                    (trigger_rule="all_done")
```

Single DAG, `dag_id="qc_lakehouse_pipeline"`, `schedule="@weekly"`. The previous
`qc_lakehouse_maintenance` DAG is deleted, not deprecated - it is fully absorbed.

## Components

### `orchestration/dags/qc_lakehouse_pipeline.py` (rewritten)

The single DAG definition. Sequential `DatabricksRunNowOperator` tasks for the two
generation jobs and the three new per-layer dbt jobs, then calls into
`maintenance.build_maintenance_tasks(dag)` for the maintenance stage, then the `report`
`PythonOperator`. Keeps the existing `DeadlineAlert` pattern from the current file
(`DeadlineReference.DAGRUN_QUEUED_AT`, not `DAGRUN_LOGICAL_DATE` - already live-verified
this session to be the only reference that actually creates a Deadline row for this DAG's
trigger pattern) but the budget must be recalculated: the DAG now does 5 sequential
Databricks Job stages plus 21 mapped maintenance job runs (barriered in 3 groups of 7),
not 4 sequential jobs. Exact minutes are a plan-time decision informed by real per-job
timings already visible in this project's Databricks job-run history, not a guess.

### `orchestration/dags/maintenance.py` (new)

Moved from the deleted `qc_lakehouse_maintenance.py`: `GOLD_TABLES` (duplicated from
`perf_lab/run_maintenance.py` - same reasoning as before, `.venv-airflow` has no import
path to `perf_lab`), `OPERATIONS`, `JOB_NAMES`, and `check_maintenance_results`. New:
`build_maintenance_tasks(dag)`, which builds the three mapped OPTIMIZE/ANALYZE/VACUUM
stages using `DatabricksRunNowOperator.partial(...).expand(python_params=...)` - confirmed
live against this project's installed `apache-airflow==3.3.1` that
`DatabricksRunNowOperator` supports `.partial()`/`.expand()` (it is a normal
`BaseOperator` subclass; `python_params` is a template field) - and returns the final
(VACUUM) mapped task handle, which the DAG file wires `>> report`. The `report`
`PythonOperator` itself is instantiated in `qc_lakehouse_pipeline.py`, calling
`check_maintenance_results` (imported from `maintenance.py`), matching the current file's
structure where the report task lives in the DAG file, not the module it inspects.

`check_maintenance_results` needs one adjustment for mapped task instances: each mapped
`DatabricksRunNowOperator` instance carries a `map_index` (0-6, one per table). The
existing `ti.task_id != "maintenance_report"` exclusion still works unchanged since the
report task now sits outside the mapped group, but the failed-task list should include
`map_index` so a failure message names the actual table, not just "optimize_gold_table"
seven times over.

### `orchestration/dags/alerting.py`

No changes. `alert_on_failure` in `DEFAULT_ARGS` already applies to every task in a DAG
automatically, including mapped task instances - nothing about dynamic mapping requires
touching this file.

### `orchestration/dags/qc_lakehouse_maintenance.py`

Deleted.

### `qc-lakehouse/databricks.yml`

Remove the `dbt_run` and `dbt_test` jobs. Add three jobs following the exact same
`resolve_secrets` -> `dbt_task` two-task pattern already used by `dbt_run`/`dbt_test`
(including the documented CAVEAT about the resolved PII salt echoing into job logs -
unchanged, still accepted for this single-user Free Edition workspace):

- `dbt_build_test_staging`: `dbt build --select staging --vars '...'`
- `dbt_build_test_intermediate`: `dbt build --select intermediate --vars '...'`
- `dbt_build_test_marts`: `dbt build --select marts --vars '...'`

Each is a separate, hardcoded job - not one job parameterized three ways. `dbt build`
(not `dbt run` + separate `dbt test`) builds and tests each model in dbt's own DAG order,
so a test failure on a staging model stops before intermediate models are attempted,
without any additional Airflow-level branching logic.

`optimize_gold_table`/`analyze_gold_table`/`vacuum_gold_table` are unchanged.

### Tests

- `orchestration/tests/test_dag.py`: rewritten for the new single-DAG sequence (task ids,
  the five sequential dependencies, `DeadlineAlert` presence, alert callback on every
  non-mapped task).
- `orchestration/tests/test_maintenance.py` (new, split out to mirror `maintenance.py`):
  asserts the three maintenance tasks are mapped operators (`isinstance` /
  `MappedOperator` or equivalent public attribute - confirmed at plan/implementation time
  against the installed Airflow version) with `GOLD_TABLES`-shaped expand input, asserts
  `job_name`/`python_params` values match `JOB_NAMES` and `GOLD_TABLES`, and keeps the
  existing direct unit tests of `check_maintenance_results` (adapted for `map_index`).
- `orchestration/tests/test_alerting.py`: unchanged.

### Documentation

- `README.md`'s Mermaid diagram and "Airflow orchestration" section need updating to
  describe one medallion-shaped DAG instead of two.
- `PROJECTINFO.md` roadmap item 2 ("Orchestration hardening") gets a note that the
  maintenance DAG was subsequently merged into the pipeline DAG on 2026-09-18, and why.

## Error handling

- Default `trigger_rule="all_success"` between `generate_reference_data` ->
  `generate_fact_data` -> `dbt_build_test_staging` -> `dbt_build_test_intermediate` ->
  `dbt_build_test_marts` -> maintenance stage: a failure anywhere in this chain stops
  everything downstream. This is intentional and is the actual fix for the "not real
  orchestration" complaint - previously a `dbt_test` failure was only discovered after
  every layer had already built.
- `dbt build`'s own model-then-test-per-node interleaving means a test failure inside, say,
  `dbt_build_test_staging` causes that Databricks Job to fail outright (non-zero exit),
  which `DatabricksRunNowOperator` surfaces as an Airflow task failure - no custom
  Airflow-side logic needed to detect "did the tests pass," dbt's own semantics already do
  it.
- `report` keeps `trigger_rule="all_done"` and raises `AirflowException` listing every
  failed task (now including `map_index` for the mapped maintenance tasks), so a partial
  maintenance failure is visible by table, not just "something in optimize_gold_table
  failed."
- `alert_on_failure` stays wired at `DEFAULT_ARGS` - covers every task, mapped or not,
  with zero extra code.

## Scheduling

`schedule="@weekly"` on the single merged DAG (confirmed with the user - this preserves
the "at least one scheduled DAG" signal that was the specific point of the 2026-09-18
orchestration-hardening work, which would otherwise regress back to manual-trigger-only if
the merged DAG dropped its schedule). `start_date` stays a fixed, timezone-aware date per
this repo's existing pattern (`datetime(2026, 1, 1, tzinfo=UTC)`), `catchup=False`.

## Alternative considered and rejected

Two DAGs (build DAG + maintenance DAG) linked via Airflow Datasets/Assets, so maintenance
auto-fires on a gold-table update event rather than being hard-sequenced in one DAG. This
is a legitimate pattern for decoupling teams with independent release cadences, but it
directly contradicts the stated requirement ("click one, order runs top to bottom") and
adds indirection (two DAG files, an Asset/Dataset definition, two schedules to reason
about) that this single-team, single-schedule project does not need. Rejected under YAGNI.

## Open questions resolved during brainstorming (recorded for the plan)

- Dynamic task mapping semantics: barrier-style (all `optimize` mapped instances complete
  before any `analyze` instance starts), not per-table correlated chains. Simpler, and
  matches how batch maintenance commonly runs in practice (see Non-goals).
- `TaskGroup` import path and `DatabricksRunNowOperator.partial()/.expand()` support:
  confirmed live against this project's installed `apache-airflow==3.3.1` /
  `apache-airflow-providers-databricks` before this spec was written. Import
  `TaskGroup` from `airflow.sdk` (the `airflow.utils.task_group` path still works but
  raises `DeprecatedImportWarning` under this version). `DatabricksRunNowOperator`
  supports `.partial()`/`.expand()` since it is a standard `BaseOperator` subclass and
  `python_params` is a declared template field.
- DeadlineAlert budget: not fixed in this spec - the plan should set it from this
  project's actual observed per-job run times (already available from this session's live
  runs), not a guess.
