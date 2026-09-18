import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))
BUNDLE_PATH = Path(__file__).resolve().parents[2] / "databricks.yml"


def _dagbag():
    from airflow.models import DagBag

    # NOTE: adapted from the brief's draft. Airflow 3.3.1's DagBag.__init__ no longer accepts
    # `include_examples` (verified via inspect.signature(DagBag.__init__) against the
    # installed version) - it isn't needed here anyway since dag_folder is scoped to this
    # project's own single-DAG directory, so example DAGs are never in scope.
    return DagBag(dag_folder=str(DAGS_DIR))


def test_dag_imports_without_errors():
    dagbag = _dagbag()
    assert dagbag.import_errors == {}


def test_pipeline_dag_has_exactly_four_tasks():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag is not None
    assert set(dag.task_ids) == {
        "generate_reference_data",
        "generate_fact_data",
        "dbt_run",
        "dbt_test",
    }


def test_pipeline_dag_dependency_chain_is_sequential():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    ref = dag.get_task("generate_reference_data")
    fact = dag.get_task("generate_fact_data")
    run = dag.get_task("dbt_run")
    test = dag.get_task("dbt_test")
    assert ref.downstream_task_ids == {"generate_fact_data"}
    assert fact.downstream_task_ids == {"dbt_run"}
    assert run.downstream_task_ids == {"dbt_test"}
    assert test.downstream_task_ids == set()


def test_pipeline_dag_has_no_schedule():
    # NOTE: adapted from the brief's draft. Airflow 3.3.1 removed `DAG.schedule_interval`
    # and `Timetable.summary` (present in the 2.x-era draft) in favor of `DAG.schedule`
    # and `Timetable.can_be_scheduled` - verified directly against the installed
    # airflow.sdk.definitions.dag.DAG and airflow.sdk.definitions.timetables.simple
    # (NullTimetable has no `summary` attribute; DAG has no `schedule_interval` attribute).
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag.schedule is None
    assert dag.timetable.can_be_scheduled is False


@pytest.mark.parametrize("dag_id", ["qc_lakehouse_pipeline", "qc_lakehouse_maintenance"])
def test_all_tasks_have_retries_configured(dag_id):
    # Parametrized over both DAGs (Minor finding from task-5 review): originally only checked
    # qc_lakehouse_pipeline, silently leaving qc_lakehouse_maintenance's retry config
    # unverified.
    dagbag = _dagbag()
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    for task_id in dag.task_ids:
        task = dag.get_task(task_id)
        assert task.retries == 2


def test_dag_has_deadline_alert_configured():
    # Airflow 3.0 removed the SLA feature (`sla_miss_callback` on DAG, `sla` per task) in
    # favor of Deadline Alerts (stable as of >=3.1; confirmed installed as 3.3.1). Both
    # legacy parameters are still accepted but are no-ops that only emit a
    # DeprecationWarning (see airflow.sdk.definitions.dag.DAG._validate_sla_miss_callback
    # and airflow.sdk.bases.operator.BaseOperator.__init__), so this project uses the
    # DAG-level `deadline` parameter (a DeadlineAlert) instead, with a callback that logs
    # a warning - preserving the brief's intent of a logged SLA-miss signal.
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag.deadline is not None
    assert len(dag.deadline) == 1


def test_deadline_alert_uses_queued_at_not_logical_date():
    # Regression test for a real bug live-verified against this project's own Airflow 3.3.1
    # instance: DeadlineReference.DAGRUN_LOGICAL_DATE never creates a Deadline row for a
    # manually-triggered run (this DAG's only trigger path - it has no schedule), because a
    # manual trigger leaves `logical_date` NULL and the deadline evaluator silently skips a
    # null-resolving reference - the callback then can never fire, with no error surfaced
    # anywhere but a buried "Could not find DagRun" warning. DAGRUN_QUEUED_AT is always
    # populated regardless of trigger type, so it's the only reference that actually works
    # here. `test_dag_has_deadline_alert_configured` above gave a false green on the broken
    # DAGRUN_LOGICAL_DATE version (a DeadlineAlert object existed - it just could never fire),
    # so this test pins the reference type itself, not just its presence.
    from airflow.sdk.definitions.deadline import DagRunQueuedAtDeadline

    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert isinstance(dag.deadline[0].reference, DagRunQueuedAtDeadline)


def test_maintenance_dag_imports_without_errors():
    # NOTE: fixed per task-5 review (Minor finding) - this was previously an exact duplicate
    # of test_dag_imports_without_errors above (just re-checking dagbag.import_errors == {}
    # with no scoping to this DAG specifically). Scoped here to actually assert something
    # maintenance-DAG-specific: that it was found and parsed into the bag at all.
    dagbag = _dagbag()
    assert dagbag.import_errors == {}
    assert dagbag.get_dag("qc_lakehouse_maintenance") is not None


GOLD_TABLES = (
    "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
    "dim_rider", "dim_zone", "dim_date",
)
OPERATIONS = ("optimize", "analyze", "vacuum")


def test_maintenance_dag_has_one_chain_per_table_plus_a_report_task():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    assert dag is not None
    expected_task_ids = {
        f"{operation}_{table}" for table in GOLD_TABLES for operation in OPERATIONS
    }
    expected_task_ids.add("maintenance_report")
    assert set(dag.task_ids) == expected_task_ids


def test_maintenance_dag_each_table_chain_is_sequential_and_independent():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    for table in GOLD_TABLES:
        optimize = dag.get_task(f"optimize_{table}")
        analyze = dag.get_task(f"analyze_{table}")
        vacuum = dag.get_task(f"vacuum_{table}")
        assert optimize.downstream_task_ids == {f"analyze_{table}"}
        assert analyze.downstream_task_ids == {f"vacuum_{table}"}
        assert vacuum.downstream_task_ids == {"maintenance_report"}
        # Independent of every other table's chain - optimize_fct_orders must not depend on
        # or block dim_customer's chain, proving these run in parallel, not sequentially.
        assert optimize.upstream_task_ids == set()


def test_maintenance_dag_report_task_fans_in_from_every_chain_with_all_done():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    report = dag.get_task("maintenance_report")
    expected_upstream = {f"vacuum_{table}" for table in GOLD_TABLES}
    assert report.upstream_task_ids == expected_upstream
    assert report.trigger_rule == "all_done"


def test_maintenance_dag_has_a_daily_schedule():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    assert dag.schedule == "@daily"
    assert dag.timetable.can_be_scheduled is True


def test_maintenance_dag_every_task_has_the_alert_callback():
    # NOTE: adapted from the brief's draft, same reasoning as the other Airflow-3.3.1
    # adaptations in this file. Verified directly against
    # airflow.sdk.bases.operator.BaseOperator.__init__: it always runs on_failure_callback
    # through `_collect_from_input(...)` into a list (the class attribute is typed
    # `Sequence[TaskStateChangeCallback] = ()`), even when a single bare callable is passed
    # in (as it is here, via DEFAULT_ARGS). So the accessor is a one-item list, not the bare
    # function, for every task in this DAG - not just some of them.
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    from alerting import alert_on_failure

    for task_id in dag.task_ids:
        task = dag.get_task(task_id)
        assert task.on_failure_callback == [alert_on_failure]


def test_maintenance_dag_job_names_and_table_params_match_databricks_yml():
    # Extends the pre-existing job-name/databricks.yml cross-check (task-5 review, Important
    # finding #3) to the renamed jobs, and adds a new assertion this redesign needs: every
    # task's python_params must pass the correct --table for its own chain, not silently
    # reuse the job's fct_orders default every time.
    import yaml

    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")

    with BUNDLE_PATH.open() as f:
        bundle = yaml.safe_load(f)
    jobs = bundle["resources"]["jobs"]

    expected_job_names = {"optimize_gold_table", "analyze_gold_table", "vacuum_gold_table"}
    for job_name in expected_job_names:
        assert job_name in jobs, f"{job_name} is missing from databricks.yml's resources.jobs"
        assert jobs[job_name]["name"] == job_name

    for table in GOLD_TABLES:
        for operation in OPERATIONS:
            task = dag.get_task(f"{operation}_{table}")
            assert task.job_name == f"{operation}_gold_table"
            assert task.job_name in jobs
            assert task.python_params == ["--operation", operation, "--table", table]
