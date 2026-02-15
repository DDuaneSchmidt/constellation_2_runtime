from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from random import Random
from typing import Any, Dict, List, Tuple, Optional


RET_Q = Decimal("0.00000001")   # 8dp returns
DD_Q = Decimal("0.000001")      # 6dp drawdown pct
TRADING_DAYS = Decimal("252")


class KStructError(Exception):
    pass


def dec(x: Any, what: str) -> Decimal:
    try:
        if isinstance(x, Decimal):
            return x
        if isinstance(x, int):
            return Decimal(x)
        if isinstance(x, float):
            return Decimal(str(x))
        if isinstance(x, str):
            return Decimal(x)
    except (InvalidOperation, ValueError) as e:
        raise KStructError(f"DEC_PARSE_ERROR({what}): {x}") from e
    raise KStructError(f"DEC_TYPE_ERROR({what}): {type(x).__name__}")


def read_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise KStructError(f"JSON_READ_ERROR: {p}: {e}") from e


def write_json_deterministic(p: Path, obj: Any) -> None:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    p.write_text(s + "\n", encoding="utf-8")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def seeded_rng(seed_material: str) -> Random:
    # Deterministic RNG seed derived from sha256 of seed_material
    h = hashlib.sha256(seed_material.encode("utf-8")).hexdigest()
    seed_int = int(h[:16], 16)
    return Random(seed_int)


def mean(xs: List[Decimal]) -> Decimal:
    if not xs:
        raise KStructError("MEAN_EMPTY")
    return sum(xs) / Decimal(len(xs))


def std_sample(xs: List[Decimal]) -> Decimal:
    n = len(xs)
    if n < 2:
        raise KStructError("STD_NEEDS_N_GE_2")
    m = mean(xs)
    var = sum((x - m) * (x - m) for x in xs) / Decimal(n - 1)
    if var < 0:
        raise KStructError("NEG_VARIANCE_IMPOSSIBLE")
    return var.sqrt()


def annualized_vol(daily: List[Decimal]) -> Optional[Decimal]:
    if len(daily) < 2:
        return None
    return (std_sample(daily) * TRADING_DAYS.sqrt()).quantize(RET_Q, rounding=ROUND_HALF_UP)


def sharpe_annualized(daily: List[Decimal]) -> Optional[Decimal]:
    if len(daily) < 2:
        return None
    sd = std_sample(daily)
    if sd == 0:
        return None
    s = (mean(daily) / sd) * TRADING_DAYS.sqrt()
    return s.quantize(RET_Q, rounding=ROUND_HALF_UP)


def compound_nav_path(daily: List[Decimal]) -> List[Decimal]:
    # Start at NAV=1.0, apply NAV *= (1+ret)
    nav = Decimal("1")
    out = [nav]
    for r in daily:
        nav = nav * (Decimal("1") + r)
        out.append(nav)
    return out


def max_drawdown(nav_path: List[Decimal]) -> Tuple[Optional[Decimal], Optional[int]]:
    if len(nav_path) < 2:
        return (None, None)
    peak = nav_path[0]
    dd_min = Decimal("0")
    longest = 0
    cur = 0
    for nav in nav_path:
        if nav > peak:
            peak = nav
        if peak <= 0:
            return (None, None)
        dd = (nav - peak) / peak  # <=0 underwater
        if dd < dd_min:
            dd_min = dd
        if dd < 0:
            cur += 1
            if cur > longest:
                longest = cur
        else:
            cur = 0
    return (dd_min.quantize(DD_Q, rounding=ROUND_HALF_UP), longest)


def cagr_from_nav_path(nav_path: List[Decimal]) -> Optional[Decimal]:
    # CAGR = (end/start)^(1/years)-1, years = (n-1)/252
    if len(nav_path) < 2:
        return None
    start = nav_path[0]
    end = nav_path[-1]
    if start <= 0 or end <= 0:
        return None
    years = Decimal(len(nav_path) - 1) / TRADING_DAYS
    if years <= 0:
        return None
    # Deterministic enough for audit: pure function of inputs
    ratio = float(end / start)
    exp = float(Decimal("1") / years)
    return Decimal(str(ratio ** exp - 1)).quantize(RET_Q, rounding=ROUND_HALF_UP)


def empirical_quantile(xs: List[Decimal], q: Decimal) -> Optional[Decimal]:
    if not xs:
        return None
    if q < 0 or q > 1:
        raise KStructError("Q_OUT_OF_RANGE")
    s = sorted(xs)
    n = len(s)
    idx = int((q * Decimal(n - 1)).to_integral_value(rounding=ROUND_HALF_UP))
    idx = max(0, min(n - 1, idx))
    return s[idx].quantize(RET_Q, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class BasicStats:
    n: int
    cagr: Optional[Decimal]
    vol_ann: Optional[Decimal]
    sharpe: Optional[Decimal]
    max_dd: Optional[Decimal]
    max_dd_duration_days: Optional[int]
    tail_95: Optional[Decimal]
    tail_99: Optional[Decimal]
