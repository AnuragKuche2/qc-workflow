import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from airflow import DAG
from airflow.exceptions import AirflowException

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))

from maintenance import (  # noqa: E402
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
    assert final_task.task_id == "maintenance.vacuum"


def test_check_maintenance_results_raises_with_map_index_when_a_mapped_task_failed():
    class _FakeTI:
        def __init__(self, task_id, state, map_index=-1):
            self.task_id = task_id
            self.state = state
            self.map_index = map_index

    class _FakeDagRun:
        def get_task_instances(self):
            return [
                _FakeTI("generate_reference_data", "success"),
                _FakeTI("maintenance.optimize", "success", map_index=0),
                _FakeTI("maintenance.optimize", "failed", map_index=1),
                _FakeTI("report", "running"),
            ]

    with pytest.raises(AirflowException, match=r"maintenance\.optimize\[1\]"):
        check_maintenance_results(dag_run=_FakeDagRun())


def test_check_maintenance_results_passes_when_everything_succeeded():
    class _FakeTI:
        def __init__(self, task_id, state, map_index=-1):
            self.task_id = task_id
            self.state = state
            self.map_index = map_index

    class _FakeDagRun:
        def get_task_instances(self):
            return [
                _FakeTI("generate_reference_data", "success"),
                _FakeTI("maintenance.vacuum", "success", map_index=6),
                _FakeTI("report", "running"),
            ]

    check_maintenance_results(dag_run=_FakeDagRun())  # must not raise
