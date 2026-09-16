from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


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


def test_all_tasks_have_retries_configured():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
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
