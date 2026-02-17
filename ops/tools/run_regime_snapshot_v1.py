#!/usr/bin/env python3
"""
run_regime_snapshot_v1.py

Regime Classification Spine v1 (immutable truth artifact).

Deterministic, fail-closed, audit-grade.

Run:
  python3 ops/tools/run_regime_snapshot_v1.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

# --- Import bootstrap (match existing Bundle A tools) ---
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]

if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1


REPO_ROOT = _REPO_ROOT_FROM_FILE
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/regime_snapshot.v1.schema.json"

PATH_ACCOUNTING_NAV = (TRUTH / "accounting_v1/nav").resolve()
PATH_ECON_DD_SNAP = (TRUTH / "monitoring_v1/economic_nav_drawdown_v1/nav_snapshot").resolve()
PATH_RISK_LEDGER = (TRUTH / "risk_v1/engine_budget").resolve()

PATH_CAP_ENVELOPE = (TRUTH / "reports/capital_risk_envelope_v1").resolve()
PATH_SUBMISSIONS = (TRUTH / "execution_evidence_v1/submissions").resolve()
PATH_BROKER_MANIFEST = (TRUTH / "execution_evidence_v1/broker_events").resolve()

OUT_ROOT = (TRUTH / "monitoring_v1/regime_snapshot_v1").resolve()


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _produced_utc_for_day(day: str) -> str:
    return f"{day}T23:59:59Z"


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return o


def _dec6_to_int_micro(s: str) -> int:
    t = (s or "").strip()
    neg = t.startswith("-")
    if neg:
        t = t[1:]
    if "." not in t:
        raise ValueError(f"DECIMAL_MISSING_DOT: {s!r}")
    a, b = t.split(".", 1)
    if len(b) != 6:
        raise ValueError(f"DECIMAL_NOT_6DP: {s!r}")
    v = int(a) * 1_000_000 + int(b)
    return -v if neg else v


def _compute_self_sha(obj: Dict[str, Any], field: str) -> str:
    tmp = dict(obj)
    tmp[field] = None
    return _sha256_bytes(_canonical_bytes(tmp))


def _submissions_present(day: str) -> bool:
    d = (PATH_SUBMISSIONS / day).resolve()
    if not d.exists() or not d.is_dir():
        return False
    for p in d.iterdir():
        if p.is_dir():
            return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_regime_snapshot_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)

    nav_path = (PATH_ACCOUNTING_NAV / day / "nav.json").resolve()
    if not nav_path.exists():
        raise SystemExit(f"FAIL: MISSING_REQUIRED_NAV: {nav_path}")

    dd_path = (PATH_ECON_DD_SNAP / day / "nav_snapshot.v1.json").resolve()
    if not dd_path.exists():
        raise SystemExit(f"FAIL: MISSING_REQUIRED_DRAWDOWN_SNAPSHOT: {dd_path}")

    risk_path = (PATH_RISK_LEDGER / day / "engine_risk_budget_ledger.v1.json").resolve()
    if not risk_path.exists():
        raise SystemExit(f"FAIL: MISSING_REQUIRED_ENGINE_RISK_BUDGET_LEDGER: {risk_path}")

    dd_obj = _read_json_obj(dd_path)
    drawdown_pct = str(dd_obj.get("drawdown_pct") or "").strip()
    if drawdown_pct == "":
        raise SystemExit("FAIL: MISSING_REQUIRED_DRAWDOWN_PCT")

    dd_micro = _dec6_to_int_micro(drawdown_pct)

    # Determine regime
    regime_label = "NORMAL"
    risk_multiplier = "1.00"
    blocking = False
    reason_codes: List[str] = []

    if dd_micro <= _dec6_to_int_micro("-0.150000"):
        regime_label = "CRASH"
        risk_multiplier = "0.25"
        blocking = True
        reason_codes.append("REGIME_CRASH_DRAWDOWN_LEQ_-0_150000")
    elif dd_micro <= _dec6_to_int_micro("-0.100000"):
        regime_label = "STRESS"
        risk_multiplier = "0.50"
        blocking = True
        reason_codes.append("REGIME_STRESS_DRAWDOWN_LEQ_-0_100000")
    elif dd_micro <= _dec6_to_int_micro("-0.050000"):
        regime_label = "HIGH_RISK"
        risk_multiplier = "0.75"
        reason_codes.append("REGIME_HIGH_RISK_DRAWDOWN_LEQ_-0_050000")
    else:
        reason_codes.append("REGIME_NORMAL_NO_TRIGGERS")

    input_manifest = [
        {"type": "accounting_nav", "path": str(nav_path), "sha256": _sha256_file(nav_path)},
        {"type": "economic_drawdown_nav_snapshot", "path": str(dd_path), "sha256": _sha256_file(dd_path)},
        {"type": "engine_risk_budget_ledger_v1", "path": str(risk_path), "sha256": _sha256_file(risk_path)},
    ]

    out_obj: Dict[str, Any] = {
        "schema_id": "regime_snapshot",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": _produced_utc_for_day(day),
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_regime_snapshot_v1.py",
            "git_sha": _git_sha(),
        },
        "status": "OK",
        "regime_label": regime_label,
        "risk_multiplier": risk_multiplier,
        "blocking": blocking,
        "reason_codes": sorted(reason_codes),
        "input_manifest": input_manifest,
        "evidence": {
            "drawdown_pct": drawdown_pct,
            "engine_risk_budget_ledger_status": "UNKNOWN",
            "capital_risk_envelope_present": False,
            "capital_risk_envelope_status": "MISSING",
            "capital_risk_envelope_severe_failure": False,
            "submissions_present": False,
            "broker_manifest_required": False,
            "broker_manifest_present": False,
            "broker_manifest_status": "MISSING"
        },
        "snapshot_sha256": None,
    }

    out_obj["snapshot_sha256"] = _compute_self_sha(out_obj, "snapshot_sha256")

    validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

    out_path = (OUT_ROOT / day / "regime_snapshot.v1.json").resolve()
    payload = _canonical_bytes(out_obj)

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(
        f"OK: REGIME_SNAPSHOT_WRITTEN day_utc={day} label={regime_label} "
        f"blocking={blocking} path={wr.path} sha256={wr.sha256} action={wr.action}"
    )

    return 2 if blocking else 0


if __name__ == "__main__":
    raise SystemExit(main())
