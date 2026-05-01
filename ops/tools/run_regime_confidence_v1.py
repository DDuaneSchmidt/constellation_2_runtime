#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, read_json_object_v1, resolve_fact_plane_truth_root_v1
from ops.tools.run_portfolio_state_v1 import portfolio_state_path

PAPER_MODE = "PAPER"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def regime_confidence_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "regime_confidence_v1" / day_utc / "regime_confidence.v1.json"


def _confidence_level(score: float, regime: str) -> str:
    if regime == "UNKNOWN":
        return "UNKNOWN"
    if score >= 0.75:
        return "HIGH"
    if score >= 0.45:
        return "MEDIUM"
    return "LOW"


def _duration_days(truth_root: Path, day_utc: str, regime: str) -> int:
    if not regime or regime == "UNKNOWN":
        return 0
    root = Path(truth_root).resolve() / "reports" / "portfolio_state_v1"
    days = sorted(path.parent.name for path in root.glob("*/portfolio_state.v1.json") if path.is_file() and path.parent.name <= day_utc)
    duration = 0
    for day in reversed(days):
        payload = _read_json(portfolio_state_path(truth_root=truth_root, day_utc=day))
        if str(payload.get("regime") or "").strip().upper() != regime:
            break
        duration += 1
    return duration


def _transition_risk(confidence_score: float, duration_days: int, volatility_state: str) -> str:
    if confidence_score <= 0:
        return "UNKNOWN"
    if volatility_state in {"SHOCK", "HIGH"} or confidence_score < 0.45:
        return "HIGH"
    if duration_days <= 2 or confidence_score < 0.75:
        return "MEDIUM"
    return "LOW"


def build_regime_confidence_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    state_path = portfolio_state_path(truth_root=truth_root, day_utc=day_utc)
    state = _read_json(state_path)
    regime = str(state.get("regime") or "UNKNOWN").strip().upper()
    volatility_state = str(state.get("volatility_regime") or "UNKNOWN").strip().upper()
    correlation_state = str(state.get("correlation_regime") or "UNKNOWN").strip().upper()
    dispersion_state = str(state.get("dispersion_regime") or "UNKNOWN").strip().upper()
    trend_strength = str(state.get("trend_strength") or "UNKNOWN").strip().upper()
    missing_inputs = [str(item) for item in state.get("missing_inputs", [])] if isinstance(state.get("missing_inputs"), list) else []
    degraded_inputs = [str(item) for item in state.get("degraded_inputs", [])] if isinstance(state.get("degraded_inputs"), list) else []
    bootstrap_inputs = [str(item) for item in state.get("bootstrap_inputs", [])] if isinstance(state.get("bootstrap_inputs"), list) else []
    base = 0.85 if regime != "UNKNOWN" else 0.25
    base -= min(0.30, 0.10 * len(missing_inputs))
    base -= min(0.25, 0.08 * len(degraded_inputs))
    base -= min(0.15, 0.05 * len(bootstrap_inputs))
    if any(value == "UNKNOWN" for value in (volatility_state, correlation_state, dispersion_state, trend_strength)):
        base -= 0.15
    confidence_score = round(max(0.0, min(1.0, base)), 6)
    confidence_level = _confidence_level(confidence_score, regime)
    duration = _duration_days(truth_root, day_utc, regime)
    transition_risk = _transition_risk(confidence_score, duration, volatility_state)
    inputs_used = state.get("inputs_used") if isinstance(state.get("inputs_used"), list) else []
    evidence_paths = [str(state_path)] if state_path.exists() else []
    for row in inputs_used:
        if isinstance(row, dict) and str(row.get("path") or "").strip():
            evidence_paths.append(str(row.get("path")))
    out_path = regime_confidence_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "regime_confidence",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": "PASS" if confidence_level in {"HIGH", "MEDIUM"} else "DEGRADED",
        "regime": regime,
        "confidence_score": confidence_score,
        "confidence_level": confidence_level,
        "regime_duration_days": duration,
        "transition_risk": transition_risk,
        "volatility_state": volatility_state,
        "correlation_state": correlation_state,
        "dispersion_state": dispersion_state,
        "trend_strength": trend_strength,
        "inputs_used": inputs_used,
        "degraded_inputs": sorted(set([*missing_inputs, *degraded_inputs, *bootstrap_inputs])),
        "evidence_paths": sorted(set(evidence_paths)),
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_regime_confidence_v1.py",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_regime_confidence_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_regime_confidence_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "confidence_level": payload["confidence_level"], "path": payload["artifact_path"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
