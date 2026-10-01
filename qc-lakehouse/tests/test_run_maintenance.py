# qc-lakehouse/tests/test_run_maintenance.py
import pytest

from perf_lab.run_maintenance import GOLD_TABLES, parse_args


def test_parse_args_requires_a_table():
    with pytest.raises(ValueError, match="--table"):
        parse_args(["--operation", "optimize"])


def test_parse_args_rejects_an_unknown_flag():
    with pytest.raises(ValueError, match="unrecognized argument '--tabel'"):
        parse_args(["--operation", "optimize", "--tabel", "dim_zone"])


def test_parse_args_reads_an_explicit_table():
    assert parse_args(["--operation", "vacuum", "--table", "dim_customer"]) == (
        "vacuum",
        "dim_customer",
    )


def test_parse_args_reads_flags_in_either_order():
    assert parse_args(["--table", "dim_zone", "--operation", "analyze"]) == (
        "analyze",
        "dim_zone",
    )


def test_parse_args_requires_a_valid_operation():
    with pytest.raises(ValueError, match="--operation"):
        parse_args(["--table", "fct_orders"])
    with pytest.raises(ValueError, match="--operation"):
        parse_args(["--operation", "not-a-real-operation", "--table", "fct_orders"])


def test_parse_args_rejects_an_unknown_table():
    with pytest.raises(ValueError, match="--table"):
        parse_args(["--operation", "optimize", "--table", "not_a_real_table"])


def test_gold_tables_has_all_seven_gold_layer_tables():
    assert GOLD_TABLES == (
        "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
        "dim_rider", "dim_zone", "dim_date",
    )
