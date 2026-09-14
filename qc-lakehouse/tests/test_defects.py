from decimal import Decimal

import pytest

from qc_lakehouse.generator.defects import (
    format_refund_amount,
    mangle_text_casing_and_whitespace,
)


def test_format_refund_amount_plain():
    assert format_refund_amount(Decimal("20.47"), use_accounting_format=False) == "20.47"


def test_format_refund_amount_accounting_negative():
    assert format_refund_amount(Decimal("20.47"), use_accounting_format=True) == "(20.47)"


def test_format_refund_amount_pads_to_two_decimal_places():
    assert format_refund_amount(Decimal(5), use_accounting_format=False) == "5.00"


def test_mangle_text_casing_and_whitespace_upper():
    assert mangle_text_casing_and_whitespace("Late again", "UPPER") == "LATE AGAIN"


def test_mangle_text_casing_and_whitespace_lower():
    assert mangle_text_casing_and_whitespace("Late again", "LOWER") == "late again"


def test_mangle_text_casing_and_whitespace_leading_space():
    assert mangle_text_casing_and_whitespace("Late again", "LEADING_SPACE") == "   Late again"


def test_mangle_text_casing_and_whitespace_trailing_space():
    assert mangle_text_casing_and_whitespace("Late again", "TRAILING_SPACE") == "Late again   "


def test_mangle_text_casing_and_whitespace_rejects_unknown_mode():
    with pytest.raises(ValueError):
        mangle_text_casing_and_whitespace("x", "NOT_A_MODE")
