# constellation_2/phaseB/lib/decimal_determinism_v1.py
#
# Deterministic decimal parsing + formatting for Constellation 2.0 Phase B.
#
# Governance alignment:
# - C2_DETERMINISM_STANDARD.md requires:
#   * fixed precision
#   * no floats
#   * no scientific notation
#   * deterministic rounding
#
# Phase B policy:
# - All monetary/price decimals emitted by Phase B are quantized to 2 decimal places.
# - Rounding: ROUND_HALF_UP (explicit).
#
# Rationale:
# - Matches provided acceptance sample conventions (2 decimal places).
# - Produces stable output even if upstream raw inputs vary in precision.

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Optional


class DecimalDeterminismError(Exception):
    pass


Q_2DP = Decimal("0.01")


def parse_decimal_strict_v1(x: Any, field_name: str) -> Decimal:
    """
    Parse a decimal input deterministically.

    Allowed input types:
    - str: "123", "123.45"
    - int: 123
    Forbidden:
    - float (non-deterministic)
    - None
    """
    if x is None:
        raise DecimalDeterminismError(f"DECIMAL_MISSING: {field_name}")
    if isinstance(x, float):
        raise DecimalDeterminismError(f"FLOAT_FORBIDDEN: {field_name}")
    if isinstance(x, int):
        return Decimal(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            raise DecimalDeterminismError(f"DECIMAL_EMPTY: {field_name}")
        # Decimal will reject scientific notation only if we disallow it explicitly by inspection
        # because Decimal('1e-3') is valid but violates our standard.
        if "e" in s.lower():
            raise DecimalDeterminismError(f"SCIENTIFIC_NOTATION_FORBIDDEN: {field_name}")
        try:
            return Decimal(s)
        except InvalidOperation as e:
            raise DecimalDeterminismError(f"DECIMAL_INVALID: {field_name}") from e
    raise DecimalDeterminismError(f"DECIMAL_TYPE_FORBIDDEN({type(x).__name__}): {field_name}")


def quantize_2dp_v1(d: Decimal, field_name: str) -> Decimal:
    try:
        return d.quantize(Q_2DP, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError) as e:
        raise DecimalDeterminismError(f"DECIMAL_QUANTIZE_FAILED: {field_name}") from e


def decimal_to_str_2dp_v1(d: Decimal, field_name: str) -> str:
    """
    Convert Decimal to fixed 2dp string, forbidding exponent form.
    """
    q = quantize_2dp_v1(d, field_name)
    s = format(q, "f")
    if "e" in s.lower():
        raise DecimalDeterminismError(f"DECIMAL_EXPONENT_FORBIDDEN: {field_name}")
    # Ensure exactly 2dp are present (quantize should guarantee this)
    if "." not in s:
        s = s + ".00"
    else:
        whole, frac = s.split(".", 1)
        if len(frac) < 2:
            s = s + ("0" * (2 - len(frac)))
        elif len(frac) > 2:
            # Should never happen after quantize, but fail closed if it does.
            raise DecimalDeterminismError(f"DECIMAL_NOT_2DP: {field_name}")
    return s


def add_2dp_str_v1(a_str: Any, b_str: Any, field_name: str) -> str:
    a = parse_decimal_strict_v1(a_str, field_name + ".a")
    b = parse_decimal_strict_v1(b_str, field_name + ".b")
    return decimal_to_str_2dp_v1(a + b, field_name)


def sub_2dp_str_v1(a_str: Any, b_str: Any, field_name: str) -> str:
    a = parse_decimal_strict_v1(a_str, field_name + ".a")
    b = parse_decimal_strict_v1(b_str, field_name + ".b")
    r = a - b
    if r < 0:
        raise DecimalDeterminismError(f"DECIMAL_NEGATIVE_FORBIDDEN: {field_name}")
    return decimal_to_str_2dp_v1(r, field_name)


def mid_2dp_str_v1(bid_str: Any, ask_str: Any, field_name: str) -> str:
    bid = parse_decimal_strict_v1(bid_str, field_name + ".bid")
    ask = parse_decimal_strict_v1(ask_str, field_name + ".ask")
    if ask < bid:
        raise DecimalDeterminismError(f"ASK_LT_BID_FORBIDDEN: {field_name}")
    mid = (bid + ask) / Decimal(2)
    return decimal_to_str_2dp_v1(mid, field_name + ".mid")
