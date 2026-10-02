# qc-lakehouse/tests/test_apply_layouts.py
import pytest

from perf_lab.apply_layouts import parse_args


def test_parse_args_reads_the_layout_flag():
    assert parse_args(["--layout", "baseline"]) == "baseline"
    assert parse_args(["--layout", "zorder"]) == "zorder"


def test_parse_args_requires_the_layout_flag():
    with pytest.raises(ValueError, match="layout"):
        parse_args([])


def test_parse_args_rejects_an_unknown_layout():
    with pytest.raises(ValueError, match="unknown layout"):
        parse_args(["--layout", "not-a-real-layout"])


def test_parse_args_rejects_an_unknown_flag():
    with pytest.raises(ValueError, match="unrecognized argument '--layuot'"):
        parse_args(["--layuot", "baseline"])
