from __future__ import annotations

from decimal import Decimal


def format_refund_amount(amount: Decimal, use_accounting_format: bool) -> str:
    """Defect #18-flavored (from the superseded old plan's defect catalog): money stored
    as text, with a fraction using accounting-negative notation for a credit/reduction -
    parens instead of a minus sign, e.g. "(20.47)" - which breaks a naive CAST under ANSI
    mode. `amount` is always the positive refund magnitude; `use_accounting_format`
    controls only the STRING representation, never the underlying value."""
    plain = f"{amount:.2f}"
    return f"({plain})" if use_accounting_format else plain


def mangle_text_casing_and_whitespace(text: str, mode: str) -> str:
    """Defect #17-flavored noise: casing/whitespace damage on free-text fields, which
    inflates naive GROUP BY cardinality. The caller decides which mode via a per-row hash
    draw; this function only applies one deterministically."""
    if mode == "UPPER":
        return text.upper()
    if mode == "LOWER":
        return text.lower()
    if mode == "LEADING_SPACE":
        return f"   {text}"
    if mode == "TRAILING_SPACE":
        return f"{text}   "
    raise ValueError(f"unknown casing mode: {mode}")
