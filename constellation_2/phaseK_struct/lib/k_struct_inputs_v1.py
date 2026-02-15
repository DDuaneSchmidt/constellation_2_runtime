from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseK_struct.lib.k_struct_common_v1 import KStructError, dec, read_json


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

NAV_SERIES_ROOT = (TRUTH / "monitoring_v1/nav_series").resolve()
ENGINE_METRICS_ROOT = (TRUTH / "monitoring_v1/engine_metrics").resolve()
ENGINE_CORR_ROOT = (TRUTH / "monitoring_v1/engine_correlation_matrix").resolve()


@dataclass(frozen=True)
class Inputs:
    asof_day_utc: str
    nav_series_path: Path
    engine_metrics_path: Optional[Path]
    engine_corr_path: Optional[Path]
    nav_points: List[Dict[str, Any]]
    daily_returns: List[Decimal]
    flags: List[str]


def load_inputs_or_fail(asof_day_utc: str) -> Inputs:
    p = (NAV_SERIES_ROOT / asof_day_utc / "portfolio_nav_series.v1.json").resolve()
    if not p.exists():
        raise KStructError(f"NAV_SERIES_MISSING: {p}")
    obj = read_json(p)
    series = obj.get("series", {})
    pts = series.get("points", None)
    if not isinstance(pts, list) or not pts:
        raise KStructError("NAV_SERIES_POINTS_MISSING_OR_INVALID")

    daily: List[Decimal] = []
    for i, pt in enumerate(pts):
        if not isinstance(pt, dict):
            raise KStructError("NAV_SERIES_POINT_NOT_OBJECT")
        if "daily_return" not in pt:
            raise KStructError("NAV_SERIES_POINT_DAILY_RETURN_MISSING")
        r = dec(pt["daily_return"], f"daily_return[{i}]")
        daily.append(r)

    # Note: PhaseJ includes day 0 with daily_return 0; we keep full series
    flags: List[str] = []
    st = obj.get("status")
    if isinstance(st, str):
        flags.append(f"nav_series_status={st}")
    rcs = obj.get("reason_codes")
    if isinstance(rcs, list):
        for x in rcs:
            if isinstance(x, str):
                flags.append(f"nav_series_reason={x}")

    em = (ENGINE_METRICS_ROOT / asof_day_utc / "engine_metrics.v1.json").resolve()
    ec = (ENGINE_CORR_ROOT / asof_day_utc / "engine_correlation_matrix.v1.json").resolve()

    em_p = em if em.exists() else None
    ec_p = ec if ec.exists() else None

    return Inputs(
        asof_day_utc=asof_day_utc,
        nav_series_path=p,
        engine_metrics_path=em_p,
        engine_corr_path=ec_p,
        nav_points=pts,
        daily_returns=daily,
        flags=flags,
    )
