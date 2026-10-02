import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from airflow import DAG
from airflow.exceptions import AirflowException

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))

from maintenance import (
    GOLD_TABLES,
    JOB_NAMES,
    OPERATIONS,
    build_maintenance_tasks,
    check_maintenance_results,
)


def _build_test_dag():
    with DAG(
        dag_id="test_maintenance_only",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
    ) as dag:
        final_task = build_maintenance_tasks(dag)
    return dag, final_task


def test_gold_tables_and_operations_are_the_expected_seven_tables_three_ops():
    assert GOLD_TABLES == (
        "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
        "dim_rider", "dim_zone", "dim_date",
    )
    assert OPERATIONS == ("optimize", "analyze", "vacuum")
    assert JOB_NAMES == {
        "optimize": "optimize_gold_table",
        "analyze": "analyze_gold_table",
        "vacuum": "vacuum_gold_table",
    }


def test_build_maintenance_tasks_creates_one_mapped_task_per_operation():
    dag, _ = _build_test_dag()
    for operation in OPERATIONS:
        task = dag.get_task(f"maintenance.{operation}")
        # MappedOperator stores job_name in partial_kwargs, not as a direct attribute
        assert task.partial_kwargs["job_name"] == JOB_NAMES[operation]
        # A mapped operator's expand input carries one python_params list per table.
        expanded = task.expand_input.value["python_params"]
        assert expanded == [
            ["--operation", operation, "--table", table] for table in GOLD_TABLES
        ]


def test_build_maintenance_tasks_chains_optimize_then_analyze_then_vacuum():
    dag, final_task = _build_test_dag()
    optimize = dag.get_task("maintenance.optimize")
    analyze = dag.get_task("maintenance.analyze")
    vacuum = dag.get_task("maintenance.vacuum")
    assert optimize.downstream_task_ids == {"maintenance.analyze"}
    assert analyze.downstream_task_ids == {"maintenance.vacuum"}
    assert vacuum.downstream_task_ids == set()
    assert final_task.task_id == "maintenance.vacuum"


def test_check_maintenance_results_raises_with_the_real_task_states_api_shape_when_a_mapped_task_failed():
    # context["dag_run"] in Airflow's Task SDK (3.x) is a lightweight data object with no
    # .get_task_instances() - confirmed live against a real run of this DAG. The supported
    # replacement is context["ti"].get_task_states(...), which returns
    # {run_id: {task_key: state}}, where a mapped task's key is f"{task_id}_{map_index}"
    # (see airflow.api_fastapi.execution_api.routes.task_instances.get_task_instance_states).
    class _FakeDagRun:
        dag_id = "qc_lakehouse_pipeline"
        run_id = "manual__test"

    class _FakeTI:
        def get_task_states(self, dag_id, run_ids):
            assert dag_id == "qc_lakehouse_pipeline"
            assert run_ids == ["manual__test"]
            return {
                "manual__test": {
                    "generate_reference_data": "success",
                    "maintenance.optimize_0": "success",
                    "maintenance.optimize_1": "failed",
                    "report": "running",
                },
            }

    with pytest.raises(AirflowException, match=r"maintenance\.optimize_1"):
        check_maintenance_results(ti=_FakeTI(), dag_run=_FakeDagRun())


def test_check_maintenance_results_passes_when_everything_succeeded():
    class _FakeDagRun:
        dag_id = "qc_lakehouse_pipeline"
        run_id = "manual__test"

    class _FakeTI:
        def get_task_states(self, dag_id, run_ids):
            return {
                "manual__test": {
                    "generate_reference_data": "success",
                    "maintenance.vacuum_6": "success",
                    "report": "running",
                },
            }

    check_maintenance_results(ti=_FakeTI(), dag_run=_FakeDagRun())  # must not raise


def test_check_maintenance_results_fails_closed_when_run_id_is_missing_from_the_response():
    # Guards against a future Airflow version changing get_task_states' top-level keying:
    # an unguarded .get(run_id, {}) would silently see zero failed tasks and report success
    # on a response shape it no longer understands - the exact class of undocumented-
    # contract drift that caused the bug this function was fixed for.
    class _FakeDagRun:
        dag_id = "qc_lakehouse_pipeline"
        run_id = "manual__test"

    class _FakeTI:
        def get_task_states(self, dag_id, run_ids):
            return {"some_other_run_id": {"report": "running"}}

    with pytest.raises(AirflowException, match=r"no entry for this run"):
        check_maintenance_results(ti=_FakeTI(), dag_run=_FakeDagRun())
