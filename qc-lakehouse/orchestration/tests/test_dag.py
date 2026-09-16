from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
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


def test_maintenance_dag_has_exactly_three_tasks():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    assert dag is not None
    assert set(dag.task_ids) == {"optimize_fct_orders", "analyze_fct_orders", "vacuum_fct_orders"}


def test_maintenance_dag_dependency_chain_is_sequential():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    optimize = dag.get_task("optimize_fct_orders")
    analyze = dag.get_task("analyze_fct_orders")
    vacuum = dag.get_task("vacuum_fct_orders")
    assert optimize.downstream_task_ids == {"analyze_fct_orders"}
    assert analyze.downstream_task_ids == {"vacuum_fct_orders"}
    assert vacuum.downstream_task_ids == set()


def test_maintenance_dag_has_no_schedule():
    # NOTE: adapted from the brief's draft, same reasoning as test_pipeline_dag_has_no_schedule
    # above - Airflow 3.3.1 removed `DAG.schedule_interval` and `Timetable.summary` (present in
    # the 2.x-era draft) in favor of `DAG.schedule` and `Timetable.can_be_scheduled`.
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    assert dag.schedule is None
    assert dag.timetable.can_be_scheduled is False


def test_maintenance_dag_job_names_match_databricks_yml():
    # Added per task-5 review (Important finding #3): the DAG's `job_name=` strings and
    # databricks.yml's job `name:` keys are duplicated literals with no assertion tying them
    # together - a rename in one file would only surface as an Airflow runtime failure
    # (job-not-found), not a test failure. Parses databricks.yml directly (pyyaml is already
    # a dev dependency) and cross-checks both the job_name values themselves and the mapping
    # from DAG task_id -> job_name against the actual job resource keys/names declared there.
    import yaml

    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")

    with BUNDLE_PATH.open() as f:
        bundle = yaml.safe_load(f)
    jobs = bundle["resources"]["jobs"]

    expected_job_names = {"optimize_fct_orders", "analyze_fct_orders", "vacuum_fct_orders"}
    # Every expected job must exist in databricks.yml, as its own resource key, with a
    # matching `name:` field (the DatabricksRunNowOperator resolves job_name against the
    # job's `name:`, not the bundle resource key - they happen to be identical by convention
    # in this file, but this test pins that convention rather than assuming it).
    for job_name in expected_job_names:
        assert job_name in jobs, f"{job_name} is missing from databricks.yml's resources.jobs"
        assert jobs[job_name]["name"] == job_name, (
            f"databricks.yml job resource {job_name!r} has name: {jobs[job_name]['name']!r}, "
            f"expected {job_name!r}"
        )

    # Every DAG task's job_name must point at one of those same, real job names.
    for task_id in dag.task_ids:
        task = dag.get_task(task_id)
        assert task.job_name in expected_job_names, (
            f"DAG task {task_id!r} has job_name={task.job_name!r}, not one of the 3 "
            f"maintenance jobs declared in databricks.yml ({expected_job_names})"
        )
        assert task.job_name in jobs, (
            f"DAG task {task_id!r} references job_name={task.job_name!r}, which does not "
            "exist as a resource key in databricks.yml"
        )
