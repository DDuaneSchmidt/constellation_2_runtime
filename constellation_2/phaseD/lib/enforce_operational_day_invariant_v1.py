"""
C2 Operational Day Key Invariant v1

Enforces future-day quarantine per:
- C2_TEST_DAY_QUARANTINE_POLICY_V1

This helper MUST be used by all operational writers that accept --day_utc
and produce immutable runtime truth artifacts.

Invariant:
- day_utc MUST NOT be greater than today_utc (UTC).
- Comparison is lexicographic (YYYY-MM-DD safe).
- Fail-closed.

PAPER-only governed exception:
- day_utc may be greater than today_utc ONLY when:
  - environment variable C2_MODE == "PAPER", AND
  - an explicit operator override artifact exists and is valid at:
    constellation_2/runtime/truth/reports/operator_future_day_override_v1/<DAY>/operator_future_day_override.v1.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
OVERRIDE_ROOT = (TRUTH / "reports" / "operator_future_day_override_v1").resolve()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: FUTURE_DAY_OVERRIDE_UNREADABLE path={path} err={e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: FUTURE_DAY_OVERRIDE_NOT_OBJECT path={path}")
    return obj


def _paper_override_allows(day_utc: str) -> bool:
    mode = (os.environ.get("C2_MODE") or "").strip().upper()
    if mode != "PAPER":
        return False

    p = (OVERRIDE_ROOT / day_utc / "operator_future_day_override.v1.json").resolve()
    if not p.exists() or not p.is_file():
        return False

    obj = _read_json_obj(p)
    if str(obj.get("schema_id") or "") != "C2_OPERATOR_FUTURE_DAY_OVERRIDE_V1":
        raise SystemExit(f"FAIL: FUTURE_DAY_OVERRIDE_SCHEMA_ID_MISMATCH path={p}")
    if int(obj.get("schema_version") or 0) != 1:
        raise SystemExit(f"FAIL: FUTURE_DAY_OVERRIDE_SCHEMA_VERSION_MISMATCH path={p}")
    if str(obj.get("override_day_utc") or "") != day_utc:
        raise SystemExit(f"FAIL: FUTURE_DAY_OVERRIDE_DAY_MISMATCH path={p}")
    if str(obj.get("mode") or "").strip().upper() != "PAPER":
        raise SystemExit(f"FAIL: FUTURE_DAY_OVERRIDE_MODE_NOT_PAPER path={p}")
    return True


def enforce_operational_day_key_invariant_v1(day_utc: str) -> None:
    if not isinstance(day_utc, str):
        raise SystemExit("FAIL: DAY_UTC_NOT_STRING")

    if len(day_utc) != 10 or day_utc[4] != "-" or day_utc[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {day_utc!r}")

    today_utc = datetime.now(timezone.utc).date().isoformat()

    # Lexicographic compare valid for YYYY-MM-DD
    if day_utc > today_utc:
        if _paper_override_allows(day_utc):
            return
        raise SystemExit(
            f"FAIL: FUTURE_DAY_UTC_DISALLOWED day_utc={day_utc} today_utc={today_utc}"
        )
