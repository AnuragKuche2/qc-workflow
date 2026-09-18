# QC Lakehouse: 500x Benchmark Scale (Sub-project H follow-up) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reach the original ~500x/675M-row target for `qc_dev.perf_bench.orders_bench`
(Sub-project H's layout benchmark table) via several short, independently-cancellable
Databricks Job runs, instead of the one long Databricks-Connect session that crashed after
2 of 18 chunks on 2026-09-15.

**Architecture:** `perf_lab/generate_benchmark_orders.py` already has (added 2026-09-17,
uncommitted) a resumable design: `resolve_window()` computes a `(start_date, days)` slice
`day_offset` days into the overall 90-day target, and `id_offset_for_day_offset()` reserves a
non-overlapping `order_id` block per offset, so independent invocations covering different
date windows can `append` into the same table without colliding. That design is currently
driven by environment variables (`BENCH_DAY_OFFSET`/`BENCH_WINDOW_DAYS`), fine for an
interactive Databricks-Connect run but not for a Databricks Job, whose per-run parameters
are CLI args (see `databricks.yml`'s `optimize_fct_orders` job and its
`spark_python_task.parameters` - the existing, working pattern `run_maintenance.py` uses).
This plan converts the day-offset/window-days interface to CLI args to match that pattern,
adds a `generate_benchmark_orders` Databricks Job, then executes 8 short job runs
(`day_offset` = 10, 20, ..., 80) to cover the full 90-day window - each run independently
cancellable via `databricks jobs cancel-run`, unlike a local Python client `kill`, which
2026-09-17's live session proved does **not** reliably stop an already-dispatched remote
Databricks Connect write (that gap caused a real data corruption incident this plan's Task 1
cleans up).

**Tech Stack:** PySpark, Databricks Asset Bundles (`databricks.yml`), Databricks CLI
(`databricks bundle`, `databricks jobs`), pytest.

**Spec:** `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md` (Sub-project
H's parent design) and `docs/superpowers/plans/2026-09-16-qc-lakehouse-h-perf-cost-lab.md`
(the original benchmark plan this extends). No separate design spec exists for this follow-up;
its requirements are captured directly in this plan, driven by the 2026-09-17 live session's
findings (see Global Constraints).

## Global Constraints

- `id_offset_for_day_offset(day_offset) = day_offset * 10_000_000` (`ID_BLOCK_SIZE` in
  `generate_benchmark_orders.py`) - never change `ID_BLOCK_SIZE` without re-verifying it stays
  above the realistic max orders/day at `SCALE_MULTIPLIER=500` (~7.95M/day with the ~6% event
  bump) - a smaller block size reintroduces the exact ID-collision bug this plan fixes.
- `day_offset=0` is the only value allowed to use `write_mode="overwrite"` - every other
  `day_offset` must `append`. Never invoke the script/job with a `day_offset` you have not
  first confirmed (via a live row-count/id-range query) is not already covered by existing data.
- Never rely on killing a local Python process to stop live Databricks Connect work - a
  dispatched remote write can keep running and committing after the local client dies (root
  cause of the 2026-09-17 corruption). Use `databricks jobs cancel-run <run-id>` for any run
  that needs to be stopped.
- Before any new invocation, verify the table's current row count is *stable* (query it twice,
  ~30s apart, and confirm no change) - this is the cheapest available proof no other writer is
  still active.
- `PII_HASH_SALT` and all other live-Databricks secrets stay in `.env`/Databricks Secrets -
  never hardcoded, never printed, never committed.

---

### Task 1: Clean up the corrupted date range (manual, requires explicit permission)

**Files:** none (live data operation only, run from `qc-lakehouse/`)

**Interfaces:**
- Consumes: a working `qc_dev` Databricks Connect session (`.venv-databricks`, `.env` populated
  per this session's earlier setup).
- Produces: `qc_dev.perf_bench.orders_bench` restored to exactly 73,723,047 rows, covering only
  `date_day` 2026-06-01 through 2026-06-10 (the verified-clean `day_offset=0` baseline).

This is a live `DELETE` against cloud data - the harness's own auto-mode classifier blocks it
by default ("Cloud Storage Mass Delete"). A human must run this directly, or grant the
permission explicitly when prompted.

- [ ] **Step 1: Confirm the corruption is still exactly what was diagnosed**

```bash
cd qc-lakehouse && set -a && . ./.env && set +a && cat > /tmp/check_corruption.py << 'EOF'
from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session, is_running_on_databricks
from pyspark.sql import functions as F

settings = None if is_running_on_databricks() else load_settings()
spark = build_databricks_session(settings)
df = spark.table("qc_dev.perf_bench.orders_bench")
bad = df.filter(F.col("date_day") >= "2026-06-11")
good = df.filter(F.col("date_day") < "2026-06-11")
print(f"rows before 2026-06-11 (expected 73723047): {good.count():,}")
print(f"rows on/after 2026-06-11 (to be deleted): {bad.count():,}")
EOF
.venv-databricks/bin/python /tmp/check_corruption.py
rm /tmp/check_corruption.py
```

Expected: `rows before 2026-06-11 (expected 73723047): 73,723,047`. If this number differs,
STOP - the corrupted region may have grown or shrunk since diagnosis, and Step 2's `DELETE`
boundary needs re-checking before proceeding.

- [ ] **Step 2: Delete the corrupted rows**

```bash
cd qc-lakehouse && set -a && . ./.env && set +a && cat > /tmp/cleanup_bench.py << 'EOF'
from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session, is_running_on_databricks

settings = None if is_running_on_databricks() else load_settings()
spark = build_databricks_session(settings)
before = spark.table("qc_dev.perf_bench.orders_bench").count()
spark.sql("DELETE FROM qc_dev.perf_bench.orders_bench WHERE date_day >= '2026-06-11'")
after = spark.table("qc_dev.perf_bench.orders_bench").count()
print(f"before={before:,} after={after:,}")
EOF
.venv-databricks/bin/python /tmp/cleanup_bench.py
rm /tmp/cleanup_bench.py
```

Expected: `before=261,737,180 after=73,723,047` (or whatever Step 1 actually reported as the
"before 2026-06-11" count, if it differed).

- [ ] **Step 3: Verify no other writer touches the table while idle**

```bash
cd qc-lakehouse && set -a && . ./.env && set +a && cat > /tmp/verify_stable.py << 'EOF'
from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session, is_running_on_databricks
settings = None if is_running_on_databricks() else load_settings()
spark = build_databricks_session(settings)
print("count:", spark.table("qc_dev.perf_bench.orders_bench").count())
EOF
.venv-databricks/bin/python /tmp/verify_stable.py
sleep 30
.venv-databricks/bin/python /tmp/verify_stable.py
rm /tmp/verify_stable.py
```

Expected: both counts print `73,723,047` - identical, proving the table is idle before Task 2
starts.

---

### Task 2: Convert day-offset/window-days from env vars to CLI args

**Files:**
- Modify: `qc-lakehouse/perf_lab/generate_benchmark_orders.py`
- Test: `qc-lakehouse/tests/test_generate_benchmark_orders.py`

**Interfaces:**
- Consumes: nothing new from other tasks.
- Produces: `parse_args(argv: list[str]) -> tuple[int, int]` returning `(day_offset,
  window_days)`, used by `main()` in place of the current `os.environ.get(...)` reads. Task 3
  passes these as `spark_python_task.parameters` in `databricks.yml`.

- [ ] **Step 1: Write the failing tests**

Add to `qc-lakehouse/tests/test_generate_benchmark_orders.py`, alongside the existing
`resolve_window`/`id_offset_for_day_offset` tests:

```python
def test_parse_args_defaults_to_zero_offset_and_ten_day_window():
    assert parse_args([]) == (0, 10)


def test_parse_args_reads_both_flags():
    assert parse_args(["--day-offset", "20", "--window-days", "15"]) == (20, 15)


def test_parse_args_reads_flags_in_either_order():
    assert parse_args(["--window-days", "15", "--day-offset", "20"]) == (20, 15)
```

And update the import at the top of the file:

```python
from perf_lab.generate_benchmark_orders import (
    _run_chunks,
    chunk_date_ranges,
    id_offset_for_day_offset,
    parse_args,
    resolve_window,
    should_bail_out,
)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd qc-lakehouse && .venv/bin/python -m pytest tests/test_generate_benchmark_orders.py -k parse_args -v`
Expected: `ImportError: cannot import name 'parse_args'`

- [ ] **Step 3: Implement `parse_args`**

In `qc-lakehouse/perf_lab/generate_benchmark_orders.py`, add near `resolve_window`:

```python
def parse_args(argv: list[str]) -> tuple[int, int]:
    """(day_offset, window_days) from `--day-offset N --window-days N` CLI flags (either
    order), matching the CLI-args convention `databricks.yml`'s spark_python_task.parameters
    already uses for run_maintenance.py - Databricks Jobs pass per-run overrides as argv, not
    environment variables."""
    day_offset, window_days = 0, 10
    i = 0
    while i < len(argv):
        if argv[i] == "--day-offset":
            day_offset = int(argv[i + 1])
            i += 2
        elif argv[i] == "--window-days":
            window_days = int(argv[i + 1])
            i += 2
        else:
            i += 1
    return day_offset, window_days
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd qc-lakehouse && .venv/bin/python -m pytest tests/test_generate_benchmark_orders.py -k parse_args -v`
Expected: 3 passed.

- [ ] **Step 5: Wire `parse_args` into `main()`, replacing the env-var reads**

In `qc-lakehouse/perf_lab/generate_benchmark_orders.py`, in `main()`, replace:

```python
    day_offset = int(os.environ.get("BENCH_DAY_OFFSET", "0"))
    window_days = int(os.environ.get("BENCH_WINDOW_DAYS", "10"))
```

with:

```python
    day_offset, window_days = parse_args(sys.argv[1:])
```

`import os` (added earlier today solely for the two lines just replaced, confirmed via
`grep -n "os\." qc-lakehouse/perf_lab/generate_benchmark_orders.py` to have no other use in
the file) is now dead and must be deleted, or `ruff check` fails on the unused import. Replace:

```python
import os
import time
```

with:

```python
import sys
import time
```

- [ ] **Step 6: Run the full test file and lint**

Run: `cd qc-lakehouse && .venv/bin/python -m pytest tests/test_generate_benchmark_orders.py -v && .venv/bin/python -m ruff check perf_lab/generate_benchmark_orders.py tests/test_generate_benchmark_orders.py`
Expected: all tests pass, `All checks passed!` from ruff.

- [ ] **Step 7: Commit**

```bash
git add qc-lakehouse/perf_lab/generate_benchmark_orders.py qc-lakehouse/tests/test_generate_benchmark_orders.py
git commit -m "Make generate_benchmark_orders resumable via CLI args, not env vars

Databricks Jobs pass per-run overrides as spark_python_task.parameters (CLI argv),
not environment variables - matching the existing run_maintenance.py convention.
Includes the id_offset/resolve_window resumable-window design added while diagnosing
2026-09-17's cross-invocation order_id collision (see the corrupted-data cleanup in
docs/superpowers/plans/2026-09-17-qc-lakehouse-h-500x-benchmark-scale.md Task 1)."
```

---

### Task 3: Add the `generate_benchmark_orders` Databricks Job

**Files:**
- Modify: `qc-lakehouse/databricks.yml`

**Interfaces:**
- Consumes: `parse_args`'s `--day-offset`/`--window-days` CLI flags from Task 2.
- Produces: a deployable `generate_benchmark_orders` job, invocable per-day-offset via
  `databricks bundle run generate_benchmark_orders --python-params '["--day-offset","N","--window-days","10"]'`.

- [ ] **Step 1: Add the job definition**

In `qc-lakehouse/databricks.yml`, add after the `generate_fact_data` job (before `dbt_run`):

```yaml
    generate_benchmark_orders:
      name: generate_benchmark_orders
      tasks:
        - task_key: main
          spark_python_task:
            python_file: ./perf_lab/generate_benchmark_orders.py
            parameters: ["--day-offset", "10", "--window-days", "10"]
          environment_key: qc_lakehouse_env
      environments:
        - environment_key: qc_lakehouse_env
          spec:
            environment_version: "3"
            dependencies:
              - ./dist/*.whl
              - python-dotenv
```

The `parameters` default (`day-offset=10`) is a placeholder for `databricks bundle validate`
to check schema against - Task 5 overrides it per-invocation via `--python-params`, never
relies on this default (which would silently re-run the same 10-day window every time if
someone forgot to override it).

- [ ] **Step 2: Validate the bundle**

Run: `cd qc-lakehouse && databricks bundle validate`
Expected: no errors. If `warehouse_id` or another variable prompts, that's pre-existing
behavior unrelated to this change - confirm the diff is additive only
(`git diff databricks.yml` should show only the new `generate_benchmark_orders` block added).

- [ ] **Step 3: Deploy the bundle**

Run: `cd qc-lakehouse && databricks bundle deploy`
Expected: deploy succeeds, output lists `generate_benchmark_orders` among the deployed jobs.

- [ ] **Step 4: Commit**

```bash
git add qc-lakehouse/databricks.yml
git commit -m "Add generate_benchmark_orders as a real Databricks Job

Running this as a Job (not a local Databricks-Connect client) means a run can be
properly cancelled via 'databricks jobs cancel-run' - a local process kill does not
reliably stop already-dispatched remote work, which caused 2026-09-17's data
corruption incident."
```

---

### Task 4: Run the 8-invocation generation sequence and verify after each

**Files:** none (live operational task, run from `qc-lakehouse/`)

**Interfaces:**
- Consumes: the deployed `generate_benchmark_orders` job from Task 3.
- Produces: `qc_dev.perf_bench.orders_bench` covering the full 90-day/500x target window
  (2026-06-01 through 2026-08-30), built from 9 total invocations (the existing `day_offset=0`
  baseline plus 8 new ones from this task).

Repeat this step for `day_offset` in `10, 20, 30, 40, 50, 60, 70, 80`, in order, waiting for
each run to fully finish before starting the next:

- [ ] **Step 1: Run one invocation**

```bash
cd qc-lakehouse
DAY_OFFSET=10   # replace with the current value in the sequence
databricks bundle run generate_benchmark_orders \
  --python-params "[\"--day-offset\",\"${DAY_OFFSET}\",\"--window-days\",\"10\"]"
```

Watch the run's output (the CLI streams it, or check `databricks jobs list-runs
--job-id <id>` and `databricks jobs get-run <run-id>` for status). Expected: the job's own
final print line shows `generate-benchmark-orders: OK - this invocation wrote N rows`. If it
instead prints `STOPPED EARLY` (either checkpoint fired), stop the sequence here - do not
proceed to the next `day_offset` - and report the real total reached so far as this
benchmark's accepted scale, matching the project's existing "accept the real number, don't
force it" precedent from the original 2026-09-15 run.

If a run needs to be stopped for any other reason, use
`databricks jobs cancel-run <run-id>` - never assume closing a terminal or killing a local
process actually stops it.

- [ ] **Step 2: Verify after this invocation**

```bash
cd qc-lakehouse && set -a && . ./.env && set +a && cat > /tmp/verify_bench.py << 'EOF'
from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session, is_running_on_databricks
settings = None if is_running_on_databricks() else load_settings()
spark = build_databricks_session(settings)
df = spark.table("qc_dev.perf_bench.orders_bench")
total = df.count()
distinct_ids = df.select("order_id").distinct().count()
print(f"total_rows={total:,} distinct_order_ids={distinct_ids:,} no_collisions={total == distinct_ids}")
EOF
.venv-databricks/bin/python /tmp/verify_bench.py
rm /tmp/verify_bench.py
```

Expected: `no_collisions=True`. If `False`, STOP the sequence immediately and re-diagnose
before running any further invocations - do not assume the next invocation will self-correct.

- [ ] **Step 3: Move to the next `day_offset` in the sequence, or stop**

If Steps 1-2 both succeeded and there are remaining values in `10, 20, 30, 40, 50, 60, 70, 80`,
repeat Steps 1-2 with the next one. Once `day_offset=80` completes successfully (covering
through day 89, the last day of the 90-day window), the sequence is done.

---

### Task 5: Record the final accepted scale

**Files:**
- Modify: `qc-lakehouse/perf_lab/generate_benchmark_orders.py` (docstring)
- Modify: `docs/superpowers/reports/2026-09-16-h-perf-cost-report.md`

**Interfaces:**
- Consumes: the final `total_rows`/`distinct_order_ids` from Task 4's last successful Step 2.
- Produces: documentation reflecting the real, live-verified final scale - never the aspirational
  500x/675M target if the actual result differs.

- [ ] **Step 1: Update the generator's module docstring**

Replace the `HISTORY (live run of 2026-09-15): ...` paragraph in
`qc-lakehouse/perf_lab/generate_benchmark_orders.py` with the final outcome: which
`day_offset` values succeeded, the final `total_rows`/date range covered, and whether the
full 90-day window was reached or the sequence stopped early (and why, if so) - using Task 4's
actual final numbers, not the ones in this plan (which are illustrative estimates only).

- [ ] **Step 2: Add a short addendum to the H perf/cost report**

In `docs/superpowers/reports/2026-09-16-h-perf-cost-report.md`, add a dated addendum section
noting the benchmark scale was later extended (with the date, the new total row count, and a
one-line note on whether this changes the layout recommendation - it should not, since Liquid
Clustering already won by a wide margin at 51.7x, but say so explicitly rather than leaving it
implied).

- [ ] **Step 3: Commit**

```bash
git add qc-lakehouse/perf_lab/generate_benchmark_orders.py docs/superpowers/reports/2026-09-16-h-perf-cost-report.md
git commit -m "Record final accepted benchmark scale after the resumable-generation run"
```
