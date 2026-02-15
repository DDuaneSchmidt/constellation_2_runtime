#!/usr/bin/env python3
"""
run_degradation_sentinel_day_v1.py

Phase J — Degradation Sentinel v1 (non-invasive)
Deterministic, fail-closed, single-writer, canonical JSON, schema-validated.

Inputs (immutable truth):
- monitoring_v1/nav_series/<DAY>/portfolio_nav_series.v1.json
- monitoring_v1/engine_metrics/<DAY>/engine_metrics.v1.json
- monitoring_v1/engine_correlation_matrix/<DAY>/engine_correlation_matrix.v1.json

Output (immutable truth):
- monitoring_v1/sentinel/<DAY>/degradation_sentinel.v1.json

Policy:
- If insufficient history for 180d / 90d / confidence bands, emit NULL values and status DEGRADED_INSUFFICIENT_HISTORY.
- This is flag-only and MUST NOT influence execution.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

NAV_SERIES_PATH = TRUTH / "monitoring_v1/nav_series"
ENGINE_METRICS_PATH = TRUTH / "monitoring_v1/engine_metrics"
CORR_PATH = TRUTH / "monitoring_v1/engine_correlation_matrix"
OUT_PATH = TRUTH / "monitoring_v1/sentinel"

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/degradation_sentinel.v1.schema.json"


class CliError(Exception):
    pass


def _utc_now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception as e:  # noqa: BLE001
        raise CliError(f"FAIL_GIT_SHA: {e}") from e


def _sha256_file(p: Path) -> str:
    import hashlib  # local import

    h = hashlib.sha256()
    try:
        h.update(p.read_bytes())
    except Exception as e:  # noqa: BLE001
        raise CliError(f"READ_FAILED: {p}: {e}") from e
    return h.hexdigest()


def _must_file(p: Path) -> Path:
    if not p.exists() or not p.is_file():
        raise CliError(f"MISSING_FILE: {p}")
    return p


def _read_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise CliError(f"JSON_READ_FAILED: {p}: {e}") from e


def _write_failclosed_new(path: Path, obj: Dict[str, Any]) -> None:
    if path.exists():
        raise CliError(f"REFUSE_OVERWRITE: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(obj))


def build(day_utc: str) -> Dict[str, Any]:
    nav_p = _must_file(NAV_SERIES_PATH / day_utc / "portfolio_nav_series.v1.json")
    eng_p = _must_file(ENGINE_METRICS_PATH / day_utc / "engine_metrics.v1.json")
    cor_p = _must_file(CORR_PATH / day_utc / "engine_correlation_matrix.v1.json")

    # We load to ensure JSON validity and to allow future enhancements; current bootstrap emits nulls.
    _ = _read_json(nav_p)
    _ = _read_json(eng_p)
    _ = _read_json(cor_p)

    # Thresholds (deterministic constants)
    # - Sharpe threshold: < 0 over 180d
    # - Expectancy drift threshold: < 0
    # - Win rate collapse: confidence 0.95 (requires trade truth; absent => null)
    # - Correlation clustering shift: placeholder metric "avg_abs_corr" with threshold 0.100000
    sharpe_thr = "-0.00000000"
    exp_thr = "-0.00000000"
    conf = "0.95"
    corr_metric = "avg_abs_corr"
    corr_thr = "0.100000"

    # We do not have enough history nor trade-level truth => null values, flags false.
    status = "DEGRADED_INSUFFICIENT_HISTORY"
    reason_codes = ["J_SENTINEL_INSUFFICIENT_HISTORY", "J_SENTINEL_NO_TRADE_LEVEL_TRUTH"]

    obj: Dict[str, Any] = {
        "schema_id": "C2_DEGRADATION_SENTINEL_V1",
        "schema_version": 1,
        "status": status,
        "day_utc": day_utc,
        "signals": {
            "rolling_sharpe_180d": {"threshold": sharpe_thr, "value": None, "flag": False},
            "expectancy_drift": {"threshold": exp_thr, "value": None, "flag": False},
            "win_rate_collapse": {"confidence_level": conf, "value": None, "lower_band": None, "flag": False},
            "correlation_clustering_shift": {"metric": corr_metric, "threshold": corr_thr, "value": None, "flag": False},
            "flags": [
                {
                    "code": "SENTINEL_BOOTSTRAP_INSUFFICIENT_HISTORY",
                    "severity": "WARN",
                    "flag": True,
                    "details": {
                        "note": "Insufficient history for 180d/90d computations; trade-level truth absent; sentinel is non-invasive."
                    },
                }
            ],
        },
        "recommendations": [
            {
                "recommendation_id": "SENTINEL_NO_ACTION_BOOTSTRAP",
                "text": "Bootstrap state: insufficient history. Sentinel emits flags only; no execution coupling.",
                "severity": "INFO",
            }
        ],
        "input_manifest": [
            {"type": "portfolio_nav_series", "path": str(nav_p), "sha256": _sha256_file(nav_p), "producer": "phaseJ_nav_series_v1"},
            {"type": "engine_metrics", "path": str(eng_p), "sha256": _sha256_file(eng_p), "producer": "phaseJ_engine_metrics_v1"},
            {"type": "engine_correlation_matrix", "path": str(cor_p), "sha256": _sha256_file(cor_p), "producer": "phaseJ_engine_correlation_matrix_v1"},
        ],
        "produced_utc": _utc_now_iso_z(),
        "producer": {
            "repo": "constellation_2_runtime",
            "git_sha": _git_sha(),
            "module": "constellation_2/phaseJ/monitoring/run/run_degradation_sentinel_day_v1.py",
        },
        "reason_codes": reason_codes,
    }

    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = (args.day_utc or "").strip()
    if not day:
        raise CliError("MISSING_DAY_UTC")

    obj = build(day)
    out_file = (OUT_PATH / day / "degradation_sentinel.v1.json").resolve()
    _write_failclosed_new(out_file, obj)

    print(f"OK: DEGRADATION_SENTINEL_V1_WRITTEN day={day} out={out_file}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CliError as e:
        print(f"FAIL: {e}", file=os.sys.stderr)
        raise SystemExit(2)
