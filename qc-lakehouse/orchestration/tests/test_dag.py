import sys
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))
BUNDLE_PATH = Path(__file__).resolve().parents[2] / "databricks.yml"

GOLD_TABLES = (
    "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
    "dim_rider", "dim_zone", "dim_date",
)
OPERATIONS = ("optimize", "analyze", "vacuum")


def _dagbag():
    from airflow.models import DagBag

    # Airflow 3.3.1's DagBag.__init__ no longer accepts `include_examples` (verified via
    # inspect.signature(DagBag.__init__) against the installed version) - it isn't needed
    # here anyway since dag_folder is scoped to this project's own single-DAG directory.
    return DagBag(dag_folder=str(DAGS_DIR))


def test_dag_imports_without_errors():
    dagbag = _dagbag()
    assert dagbag.import_errors == {}


def test_maintenance_dag_no_longer_exists():
    dagbag = _dagbag()
    assert dagbag.get_dag("qc_lakehouse_maintenance") is None


def test_pipeline_dag_has_the_expected_medallion_shaped_tasks():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag is not None
    expected_task_ids = {
        "generate_reference_data",
        "generate_fact_data",
        "dbt_build_test_staging",
        "dbt_build_test_intermediate",
        "dbt_build_test_marts",
        "report",
    }
    for operation in OPERATIONS:
        expected_task_ids.add(f"maintenance.{operation}")
    assert set(dag.task_ids) == expected_task_ids


def test_pipeline_dag_dependency_chain_is_sequential_through_each_medallion_layer():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    ref = dag.get_task("generate_reference_data")
    fact = dag.get_task("generate_fact_data")
    staging = dag.get_task("dbt_build_test_staging")
    intermediate = dag.get_task("dbt_build_test_intermediate")
    marts = dag.get_task("dbt_build_test_marts")
    optimize = dag.get_task("maintenance.optimize")
    vacuum = dag.get_task("maintenance.vacuum")
    report = dag.get_task("report")

    assert ref.downstream_task_ids == {"generate_fact_data"}
    assert fact.downstream_task_ids == {"dbt_build_test_staging"}
    assert staging.downstream_task_ids == {"dbt_build_test_intermediate"}
    assert intermediate.downstream_task_ids == {"dbt_build_test_marts"}
    assert marts.downstream_task_ids == {"maintenance.optimize"}
    assert optimize.downstream_task_ids == {"maintenance.analyze"}
    assert vacuum.downstream_task_ids == {"report"}
    assert report.downstream_task_ids == set()


def test_pipeline_dag_has_a_weekly_schedule():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag.schedule == "@weekly"
    assert dag.timetable.can_be_scheduled is True


def test_pipeline_dag_every_task_has_the_alert_callback():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    from alerting import alert_on_failure

    for task_id in dag.task_ids:
        task = dag.get_task(task_id)
        assert task.on_failure_callback == [alert_on_failure]


def test_all_tasks_have_retries_configured():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    for task_id in dag.task_ids:
        task = dag.get_task(task_id)
        assert task.retries == 2


def test_dag_has_deadline_alert_configured():
    # Airflow 3.0 removed the SLA feature in favor of Deadline Alerts - see the
    # DAGRUN_QUEUED_AT test below for the full reasoning, carried over unchanged from the
    # pre-redesign version of this file.
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag.deadline is not None
    assert len(dag.deadline) == 1


def test_deadline_alert_uses_queued_at_not_logical_date():
    from airflow.sdk.definitions.deadline import DagRunQueuedAtDeadline

    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert isinstance(dag.deadline[0].reference, DagRunQueuedAtDeadline)


def test_report_task_fans_in_from_vacuum_with_all_done():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    report = dag.get_task("report")
    assert report.upstream_task_ids == {"maintenance.vacuum"}
    assert report.trigger_rule == "all_done"
    # NOTE: DagBag loads DAG files under a generated module name, distinct from a plain
    # `import maintenance` - so the callable object it attaches to the task is never `is`
    # a separately-imported one, even though it's the same source function. Comparing by
    # name is therefore the correct check here, not object identity.
    assert report.python_callable.__name__ == "check_maintenance_results"


def test_pipeline_dag_job_names_and_layer_selectors_match_databricks_yml():
    import yaml

    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")

    with BUNDLE_PATH.open() as f:
        bundle = yaml.safe_load(f)
    jobs = bundle["resources"]["jobs"]

    for job_name in (
        "generate_reference_data", "generate_fact_data",
        "dbt_build_test_staging", "dbt_build_test_intermediate", "dbt_build_test_marts",
    ):
        assert job_name in jobs, f"{job_name} is missing from databricks.yml's resources.jobs"

    dag_job_names = {
        dag.get_task("generate_reference_data").job_name,
        dag.get_task("generate_fact_data").job_name,
        dag.get_task("dbt_build_test_staging").job_name,
        dag.get_task("dbt_build_test_intermediate").job_name,
        dag.get_task("dbt_build_test_marts").job_name,
    }
    assert dag_job_names == {
        "generate_reference_data", "generate_fact_data",
        "dbt_build_test_staging", "dbt_build_test_intermediate", "dbt_build_test_marts",
    }

    for job_name in ("optimize_gold_table", "analyze_gold_table", "vacuum_gold_table"):
        assert jobs[job_name].get("max_concurrent_runs") == 1


def test_check_maintenance_results_raises_when_a_table_task_failed():
    # context["dag_run"] in Airflow's Task SDK (3.x) has no .get_task_instances() -
    # confirmed live against a real run of this DAG (AttributeError: 'DagRun' object has no
    # attribute 'get_task_instances'). The supported replacement is
    # context["ti"].get_task_states(...), returning {run_id: {task_key: state}} with mapped
    # tasks keyed as f"{task_id}_{map_index}".
    from maintenance import check_maintenance_results

    class _FakeDagRun:
        dag_id = "qc_lakehouse_pipeline"
        run_id = "manual__test"

    class _FakeTI:
        def get_task_states(self, dag_id, run_ids):
            return {
                "manual__test": {
                    "maintenance.optimize_0": "success",
                    "maintenance.analyze_2": "failed",
                    "report": "running",
                },
            }

    with pytest.raises(AirflowException, match=r"maintenance\.analyze_2"):
        check_maintenance_results(ti=_FakeTI(), dag_run=_FakeDagRun())
