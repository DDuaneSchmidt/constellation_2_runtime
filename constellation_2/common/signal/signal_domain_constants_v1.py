from __future__ import annotations

from pathlib import Path

DOMAIN_NAME = "signal"
DOMAIN_SCHEMA_DIR = "SIGNAL"
AUTHORITY_STATUS_CANDIDATE = "candidate"
AUTHORITY_STATUS_AUTHORITATIVE = "authoritative"

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH_HY_SPREAD_SIGNAL_V1 = (
    "governance/04_DATA/SCHEMAS/C2/SIGNAL/hy_spread_signal.v1.schema.json"
)
SCHEMA_RELPATH_SPREAD_DIRECTION_SIGNAL_V1 = (
    "governance/04_DATA/SCHEMAS/C2/SIGNAL/spread_direction_signal.v1.schema.json"
)
SCHEMA_RELPATH_HY_VOLATILITY_SIGNAL_V1 = (
    "governance/04_DATA/SCHEMAS/C2/SIGNAL/hy_volatility_signal.v1.schema.json"
)
SCHEMA_RELPATH_DURATION_TREND_SIGNAL_V1 = (
    "governance/04_DATA/SCHEMAS/C2/SIGNAL/duration_trend_signal.v1.schema.json"
)
