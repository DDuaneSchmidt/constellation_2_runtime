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
from ops.tools.run_portfolio_state_v1 import build_portfolio_state_v1, portfolio_state_path
from ops.tools.run_sleeve_evaluation_kernel_v1 import sleeve_evaluation_rollup_path

PAPER_MODE = "PAPER"
PRIORITY = [
    "C2_DEFENSIVE_TAIL_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
]
MACRO_DIVERSIFIERS = {"DBC", "GLD", "HYG", "IEF", "LQD", "TLT", "UUP"}
EQUITY_BETA = {"SPY", "QQQ", "IWM"}


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


def portfolio_activation_gate_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "portfolio_activation_gate_v1" / day_utc / "portfolio_activation_gate.v1.json"


def _latest_scan_rollup_path(truth_root: Path, day_utc: str) -> Path:
    pointer = _read_json(Path(truth_root).resolve() / "pointers" / "latest_scan_cycle_pointer.v1.json")
    if str(pointer.get("day_utc") or "") == day_utc:
        artifact_root = str(pointer.get("artifact_root") or "").strip()
        if artifact_root:
            path = Path(artifact_root) / "scan_rollup.v1.json"
            if path.is_file():
                return path.resolve()
    return sleeve_evaluation_rollup_path(truth_root=truth_root, day_utc=day_utc)


def _intent_from_outcome(outcome: dict[str, Any]) -> dict[str, str]:
    output_intents = outcome.get("output_intents") if isinstance(outcome.get("output_intents"), list) else []
    if output_intents and isinstance(output_intents[0], dict):
        row = output_intents[0]
        return {
            "raw_intent_id": str(row.get("intent_id") or ""),
            "raw_intent_path": str(row.get("intent_path") or ""),
            "raw_intent_hash": str(row.get("intent_hash") or ""),
            "raw_intent_symbol": str(row.get("symbol") or "").upper(),
        }
    signature = outcome.get("intent_signature") if isinstance(outcome.get("intent_signature"), list) else []
    if signature and isinstance(signature[0], dict):
        row = signature[0]
        return {
            "raw_intent_id": str(row.get("intent_id") or ""),
            "raw_intent_path": str(outcome.get("intent_artifact_path") or ""),
            "raw_intent_hash": str(row.get("intent_hash") or ""),
            "raw_intent_symbol": str(row.get("symbol") or outcome.get("intent_symbol") or "").upper(),
        }
    return {"raw_intent_id": "", "raw_intent_path": "", "raw_intent_hash": "", "raw_intent_symbol": ""}


def _raw_status(outcome: dict[str, Any], intent: dict[str, str]) -> str:
    status = str(outcome.get("status") or "").strip().upper()
    signal = outcome.get("signal_state") if isinstance(outcome.get("signal_state"), dict) else {}
    if intent.get("raw_intent_id") or signal.get("state") == "ACTIVE":
        return "ACTIVE"
    if status in {"BLOCKED", "DISABLED"}:
        return status
    return "INACTIVE"


def _classify(engine_id: str) -> tuple[str, str]:
    if engine_id in {"C2_TREND_EQ_PRIMARY_V1", "C2_CROSS_ASSET_TREND_V1"}:
        return "TREND", "trend"
    if engine_id in {"C2_MEAN_REVERSION_EQ_V1", "C2_MARKET_NEUTRAL_SPREAD_V1"}:
        return "MEAN_REVERSION", "mean_reversion"
    if engine_id == "C2_VOL_INCOME_DEFINED_RISK_V1":
        return "VOL_CARRY", "vol_income"
    if engine_id == "C2_EVENT_DISLOCATION_V1":
        return "EVENT", "event_dislocation"
    if engine_id == "C2_DEFENSIVE_TAIL_V1":
        return "CRISIS", "defensive_tail"
    return "UNKNOWN", "unknown"


def _initial_decision(*, engine_id: str, raw_status: str, symbol: str, state: dict[str, Any]) -> tuple[str, list[str]]:
    if raw_status == "BLOCKED":
        return "DEGRADED", ["RAW_SLEEVE_BLOCKED"]
    if raw_status == "DISABLED":
        return "SUPPRESS", ["RAW_SLEEVE_DISABLED"]
    if raw_status != "ACTIVE":
        return "SUPPRESS", ["NO_RAW_SIGNAL"]

    regime = str(state.get("regime") or "UNKNOWN").upper()
    trend = str(state.get("trend_strength") or "UNKNOWN").upper()
    vol = str(state.get("volatility_regime") or "UNKNOWN").upper()
    equity_beta = str(state.get("equity_beta_state") or "UNKNOWN").upper()
    dispersion = str(state.get("dispersion_regime") or "UNKNOWN").upper()

    if engine_id == "C2_MARKET_NEUTRAL_SPREAD_V1":
        return "SIGNAL_ONLY", ["PAIRED_EXECUTION_NOT_SUPPORTED"]

    if str(state.get("status") or "").strip().upper() in {"DEGRADED", "BLOCKED"} or regime == "UNKNOWN":
        return "ALLOW", ["PORTFOLIO_STATE_DEGRADED_NO_SUPPRESSION_APPLIED"]

    if regime == "CRISIS":
        if engine_id == "C2_DEFENSIVE_TAIL_V1":
            return "ALLOW", ["CRISIS_OVERRIDE_DEFENSIVE_ALLOWED"]
        if engine_id == "C2_VOL_INCOME_DEFINED_RISK_V1":
            return "SUPPRESS", ["CRISIS_OVERRIDE_SUPPRESSES_VOL_INCOME"]
        return "SUPPRESS", ["CRISIS_OVERRIDE_SUPPRESSES_RISK_ON"]

    if engine_id == "C2_DEFENSIVE_TAIL_V1":
        return ("ALLOW", ["DEFENSIVE_SIGNAL_ALLOWED"]) if regime in {"VOL_SHOCK", "DISPERSION"} or vol == "SHOCK" else ("SUPPRESS", ["DEFENSIVE_NOT_REQUIRED"])

    if engine_id == "C2_VOL_INCOME_DEFINED_RISK_V1":
        if vol in {"SHOCK", "UNKNOWN"} or regime in {"VOL_SHOCK", "DISPERSION"}:
            return "SUPPRESS", ["VOL_CARRY_GATE_UNFAVORABLE_VOLATILITY"]
        return "ALLOW", ["VOL_CARRY_GATE_ALLOWED"]

    if engine_id == "C2_MEAN_REVERSION_EQ_V1":
        if trend == "HIGH":
            return "SUPPRESS", ["MEAN_REVERSION_SUPPRESSED_BY_STRONG_TREND"]
        return "ALLOW", ["MEAN_REVERSION_ALLOWED_WHEN_NOT_STRONG_TREND"]

    if engine_id == "C2_TREND_EQ_PRIMARY_V1":
        if trend in {"MEDIUM", "HIGH"}:
            return "ALLOW", ["TREND_SPLIT_PRIMARY_ALLOWED"]
        return "SUPPRESS", ["TREND_SPLIT_NO_TREND"]

    if engine_id == "C2_CROSS_ASSET_TREND_V1":
        if symbol in MACRO_DIVERSIFIERS and symbol not in EQUITY_BETA:
            return "ALLOW", ["CROSS_ASSET_MACRO_DIVERSIFIER_LEADERSHIP"]
        if symbol in EQUITY_BETA and equity_beta == "HIGH":
            return "SUPPRESS", ["CROSS_ASSET_SUPPRESSED_EQUITY_BETA_REDUNDANT"]
        if dispersion == "HIGH":
            return "ALLOW", ["CROSS_ASSET_DISPERSION_LEADERSHIP"]
        return "ALLOW", ["CROSS_ASSET_DIVERSIFICATION_ALLOWED"]

    if engine_id == "C2_EVENT_DISLOCATION_V1":
        return "ALLOW", ["EVENT_DISLOCATION_ALLOWED"]

    return "DEGRADED", ["UNKNOWN_SLEEVE_PORTFOLIO_POLICY"]


def _priority(engine_id: str) -> tuple[int, str]:
    try:
        return (PRIORITY.index(engine_id), engine_id)
    except ValueError:
        return (len(PRIORITY), engine_id)


def _apply_one_primary_per_bucket(rows: list[dict[str, Any]]) -> None:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("portfolio_gate_decision") == "ALLOW":
            buckets.setdefault(str(row.get("regime_bucket") or "UNKNOWN"), []).append(row)
    for bucket_rows in buckets.values():
        if len(bucket_rows) <= 1:
            continue
        bucket_rows.sort(key=lambda row: _priority(str(row.get("sleeve_id") or "")))
        for row in bucket_rows[1:]:
            row["portfolio_gate_decision"] = "SUPPRESS"
            row["allowed_by_portfolio_gate"] = False
            row["reason_codes"] = sorted(set([*row.get("reason_codes", []), "ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"]))


def build_portfolio_activation_gate_v1(
    *,
    day_utc: str,
    truth_root: Path,
    environment: str = PAPER_MODE,
    source_rollup_path: Path | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    state_path = portfolio_state_path(truth_root=truth_root, day_utc=day_utc)
    state = _read_json(state_path)
    if not state:
        state = build_portfolio_state_v1(day_utc=day_utc, truth_root=truth_root, environment=environment)
    rollup_path = Path(source_rollup_path).resolve() if source_rollup_path is not None else _latest_scan_rollup_path(truth_root, day_utc)
    rollup = _read_json(rollup_path)
    outcomes = rollup.get("outcomes") if isinstance(rollup.get("outcomes"), list) else rollup.get("sleeve_outcomes", [])
    decisions: list[dict[str, Any]] = []
    raw_signals: list[dict[str, Any]] = []
    for outcome in outcomes if isinstance(outcomes, list) else []:
        if not isinstance(outcome, dict):
            continue
        sleeve_id = str(outcome.get("sleeve_id") or outcome.get("engine_id") or "")
        intent = _intent_from_outcome(outcome)
        raw_status = _raw_status(outcome, intent)
        regime_bucket, overlap_group = _classify(sleeve_id)
        decision, reasons = _initial_decision(
            engine_id=sleeve_id,
            raw_status=raw_status,
            symbol=str(intent.get("raw_intent_symbol") or ""),
            state=state,
        )
        row = {
            "sleeve_id": sleeve_id,
            "raw_signal_status": raw_status,
            "raw_intent_id": intent["raw_intent_id"],
            "raw_intent_path": intent["raw_intent_path"],
            "raw_intent_hash": intent["raw_intent_hash"],
            "raw_intent_symbol": intent["raw_intent_symbol"],
            "allowed_by_portfolio_gate": decision == "ALLOW",
            "portfolio_gate_decision": decision,
            "reason_codes": reasons,
            "overlap_group": overlap_group,
            "regime_bucket": regime_bucket,
            "portfolio_state_snapshot_path": str(state.get("artifact_path") or state_path),
        }
        decisions.append(row)
        raw_signals.append(
            {
                "sleeve_id": sleeve_id,
                "raw_signal_status": raw_status,
                **intent,
                "source_sleeve_outcome_path": str(outcome.get("artifact_path") or ""),
            }
        )

    _apply_one_primary_per_bucket(decisions)
    for row in decisions:
        row["allowed_by_portfolio_gate"] = row.get("portfolio_gate_decision") == "ALLOW"
    out_path = portfolio_activation_gate_path(truth_root=truth_root, day_utc=day_utc)
    allowed = [row for row in decisions if row.get("portfolio_gate_decision") == "ALLOW"]
    degraded = [row for row in decisions if row.get("portfolio_gate_decision") == "DEGRADED"]
    payload = {
        "schema_id": "portfolio_activation_gate",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": "DEGRADED" if degraded or str(state.get("status") or "") == "DEGRADED" else "PASS",
        "canonical_blocker": "",
        "portfolio_state_snapshot_path": str(state.get("artifact_path") or state_path),
        "source_rollup_path": str(rollup_path),
        "raw_sleeve_signals": raw_signals,
        "decisions": decisions,
        "approved_executable_intents": [row for row in allowed if str(row.get("raw_intent_id") or "")],
        "suppressed_or_signal_only_intents": [row for row in decisions if row.get("portfolio_gate_decision") in {"SUPPRESS", "SIGNAL_ONLY"}],
        "policy": {
            "policy_id": "portfolio_activation_gate_v1",
            "rules": [
                "CRISIS_OVERRIDE",
                "TREND_SPLIT",
                "VOL_INCOME_CARRY_GATE",
                "MEAN_REVERSION_VS_TREND_GATE",
                "MARKET_NEUTRAL_SIGNAL_ONLY_WITHOUT_PAIRED_EXECUTION",
                "CROSS_ASSET_DOMINANCE_CONTROL",
                "ONE_PRIMARY_PER_REGIME_BUCKET",
            ],
        },
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_portfolio_activation_gate_v1.py",
        "operator_next_action": "" if rollup else "Run raw sleeve scan before portfolio activation gate.",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_portfolio_activation_gate_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--source_rollup_path", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    rollup = Path(args.source_rollup_path).resolve() if str(args.source_rollup_path or "").strip() else None
    payload = build_portfolio_activation_gate_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper(), source_rollup_path=rollup)
    print(json.dumps({"status": payload["status"], "path": payload["artifact_path"], "decision_count": len(payload["decisions"])}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "DEGRADED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
