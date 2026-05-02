#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_fact_plane_truth_root_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1, git_commit_v1, git_dirty_status_v1
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, write_json_v1


SCHEMA = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_performance_control.v1.schema.json"
PRODUCER = "ops/tools/run_sleeve_performance_control_v1.py"
KNOWN_SLEEVES = (
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_DEFENSIVE_TAIL_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
)
SUBMITTED_CLASSIFICATIONS = {
    "INTENT_SUBMITTED_NO_ORDER",
    "ORDER_ACCEPTED_NO_FILL",
    "PARTIAL_FILL",
    "FILLED",
    "ORDER_REJECTED",
    "ORDER_CANCELLED",
}


def sleeve_performance_control_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "sleeve_performance_control_v1" / day_utc / "sleeve_performance_control.v1.json"


def _report_path(truth_root: Path, family: str, day_utc: str, filename: str) -> Path:
    return Path(truth_root).resolve() / "reports" / family / day_utc / filename


def _artifact_ref(artifact_type: str, path: Path) -> dict[str, Any]:
    return {"artifact_type": artifact_type, "path": str(path.resolve()), "exists": path.exists()}


def _status(payload: dict[str, Any]) -> str:
    return str(payload.get("status") or payload.get("performance_status") or payload.get("outcome_status") or "MISSING").strip().upper()


def _float_or_none(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _canonical_sleeve_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    aliases = {
        "C2_TREND_EQ_PRIMARY": "C2_TREND_EQ_PRIMARY_V1",
        "C2_MEAN_REVERSION_EQ": "C2_MEAN_REVERSION_EQ_V1",
        "C2_MARKET_NEUTRAL_SPREAD": "C2_MARKET_NEUTRAL_SPREAD_V1",
        "C2_CROSS_ASSET_TREND": "C2_CROSS_ASSET_TREND_V1",
        "C2_EVENT_DISLOCATION": "C2_EVENT_DISLOCATION_V1",
        "C2_DEFENSIVE_TAIL": "C2_DEFENSIVE_TAIL_V1",
        "C2_VOL_INCOME_DEFINED_RISK": "C2_VOL_INCOME_DEFINED_RISK_V1",
    }
    return aliases.get(text, text)


def _load_inputs(truth_root: Path, day_utc: str) -> tuple[dict[str, Path], dict[str, dict[str, Any]]]:
    paths = {
        "sleeve_intent_trade_attribution_v1": _report_path(truth_root, "sleeve_intent_trade_attribution_v1", day_utc, "sleeve_intent_trade_attribution.v1.json"),
        "trade_outcome_v1": _report_path(truth_root, "trade_outcome_v1", day_utc, "trade_outcome.v1.json"),
        "edge_attribution_v1": _report_path(truth_root, "edge_attribution_v1", day_utc, "edge_attribution.v1.json"),
        "weekly_scorecard_view_v1": _report_path(truth_root, "weekly_scorecard_view_v1", day_utc, "weekly_scorecard_view.v1.json"),
        "selection_quality_v1": _report_path(truth_root, "selection_quality_v1", day_utc, "selection_quality.v1.json"),
        "decision_consistency_v1": _report_path(truth_root, "decision_consistency_v1", day_utc, "decision_consistency.v1.json"),
        "regime_confidence_v1": _report_path(truth_root, "regime_confidence_v1", day_utc, "regime_confidence.v1.json"),
        "missed_opportunity_v1": _report_path(truth_root, "missed_opportunity_v1", day_utc, "missed_opportunity.v1.json"),
        "risk_sizing_authority_v1": _report_path(truth_root, "risk_sizing_authority_v1", day_utc, "risk_sizing_authority.v1.json"),
    }
    payloads = {name: read_json_v1(path) for name, path in paths.items()}
    return paths, payloads


def _sleeve_ids(payloads: dict[str, dict[str, Any]]) -> list[str]:
    sleeves = set(KNOWN_SLEEVES)
    attribution = payloads.get("sleeve_intent_trade_attribution_v1", {})
    for row in attribution.get("sleeves", []) if isinstance(attribution.get("sleeves"), list) else []:
        if isinstance(row, dict):
            sleeves.add(_canonical_sleeve_id(row.get("sleeve_id")))
    for row in attribution.get("opportunities", []) if isinstance(attribution.get("opportunities"), list) else []:
        if isinstance(row, dict):
            sleeves.add(_canonical_sleeve_id(row.get("sleeve_id")))
    edge = payloads.get("edge_attribution_v1", {})
    for row in edge.get("sleeves", []) if isinstance(edge.get("sleeves"), list) else []:
        if isinstance(row, dict) and str(row.get("sleeve_id") or "").upper() != "ALL":
            sleeves.add(_canonical_sleeve_id(row.get("sleeve_id")))
    scorecard = payloads.get("weekly_scorecard_view_v1", {})
    for row in scorecard.get("sleeve_rows", []) if isinstance(scorecard.get("sleeve_rows"), list) else []:
        if isinstance(row, dict):
            sleeves.add(_canonical_sleeve_id(row.get("sleeve_id")))
    selected = _canonical_sleeve_id(payloads.get("selection_quality_v1", {}).get("selected_sleeve_id"))
    if selected:
        sleeves.add(selected)
    outcome_sleeve = _canonical_sleeve_id(payloads.get("trade_outcome_v1", {}).get("sleeve_id"))
    if outcome_sleeve:
        sleeves.add(outcome_sleeve)
    return sorted(item for item in sleeves if item)


def _attribution_counts(attribution: dict[str, Any], sleeve_id: str) -> tuple[int, int, int]:
    if not attribution:
        return 0, 0, 0
    intent_count = submitted = completed = 0
    for row in attribution.get("opportunities", []) if isinstance(attribution.get("opportunities"), list) else []:
        if not isinstance(row, dict) or _canonical_sleeve_id(row.get("sleeve_id")) != sleeve_id:
            continue
        intent_count += 1
        classification = str(row.get("final_classification") or "").strip().upper()
        if classification in SUBMITTED_CLASSIFICATIONS or str(row.get("submit_decision") or "").strip().upper() == "ATTEMPTED":
            submitted += 1
        if classification == "FILLED" and str(row.get("completeness_status") or "").strip().upper() == "COMPLETE":
            completed += 1
    return intent_count, submitted, completed


def _edge_for_sleeve(edge: dict[str, Any], sleeve_id: str) -> str:
    if not edge:
        return "MISSING"
    rows = edge.get("sleeves") if isinstance(edge.get("sleeves"), list) else []
    match = next((row for row in rows if isinstance(row, dict) and _canonical_sleeve_id(row.get("sleeve_id")) == sleeve_id), None)
    if match is None:
        match = next((row for row in rows if isinstance(row, dict) and str(row.get("sleeve_id") or "").upper() == "ALL"), None)
    return str((match or {}).get("edge_health") or edge.get("status") or "UNKNOWN").strip().upper()


def _scorecard_for_sleeve(scorecard: dict[str, Any], sleeve_id: str) -> str:
    if not scorecard:
        return "MISSING"
    rows = scorecard.get("sleeve_rows") if isinstance(scorecard.get("sleeve_rows"), list) else []
    match = next((row for row in rows if isinstance(row, dict) and _canonical_sleeve_id(row.get("sleeve_id")) == sleeve_id), None)
    if not match:
        return "MISSING"
    return str(match.get("performance_status") or match.get("validity_state") or "UNKNOWN").strip().upper()


def _outcome_for_sleeve(outcome: dict[str, Any], sleeve_id: str) -> str:
    if not outcome:
        return "MISSING"
    if _canonical_sleeve_id(outcome.get("sleeve_id")) != sleeve_id:
        return "NO_MATCHING_OUTCOME"
    return str(outcome.get("outcome_status") or outcome.get("status") or "UNKNOWN").strip().upper()


def _outcome_has_real_evidence(outcome: dict[str, Any], sleeve_id: str) -> bool:
    if not outcome or _canonical_sleeve_id(outcome.get("sleeve_id")) != sleeve_id:
        return False
    for key in (
        "fill_refs",
        "fills",
        "submission_refs",
        "execution_refs",
        "evidence_refs",
        "lifecycle_refs",
        "position_lifecycle_refs",
    ):
        value = outcome.get(key)
        if isinstance(value, list) and any(isinstance(row, dict) or str(row).strip() for row in value):
            return True
    for key in ("submission_id", "order_id", "perm_id", "fill_id", "position_lifecycle_id", "outcome_ref"):
        if str(outcome.get(key) or "").strip():
            return True
    return False


def _risk_sizing(payload: dict[str, Any]) -> tuple[str, str, float | None, bool, str]:
    status = _status(payload)
    envelope = payload.get("risk_envelope") if isinstance(payload.get("risk_envelope"), dict) else {}
    nav = _float_or_none(envelope.get("nav_total") if "nav_total" in envelope else envelope.get("nav_total_cents"))
    nav_source = str(payload.get("artifact_path") or "").strip() or "risk_sizing_authority_v1"
    blocker = ""
    if not payload:
        blocker = "RISK_SIZING_AUTHORITY_MISSING"
    elif status != "PASS":
        blocker = str(payload.get("first_blocker") or "RISK_SIZING_NOT_PASS").strip().upper()
    elif nav is None or nav <= 0:
        blocker = "NAV_ZERO_OR_UNAVAILABLE"
    return status, nav_source, nav, bool(blocker), blocker


def _global_blockers(paths: dict[str, Path], payloads: dict[str, dict[str, Any]], truth_root: Path) -> list[str]:
    blockers: list[str] = []
    if truth_root.name != "production_truth":
        blockers.append("NON_PRODUCTION_TRUTH_ROOT")
    if not payloads.get("sleeve_intent_trade_attribution_v1"):
        blockers.append("SLEEVE_INTENT_TRADE_ATTRIBUTION_MISSING")
    for name, payload in payloads.items():
        if name == "sleeve_intent_trade_attribution_v1":
            continue
        if not payload:
            blockers.append(f"{name.upper()}_MISSING")
    return blockers


def build_sleeve_performance_control_v1(*, day_utc: str, truth_root: Path, runtime_root: Path | None = None) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    runtime = Path(runtime_root or truth_root).resolve()
    paths, payloads = _load_inputs(root, day_utc)
    risk_status, nav_source, nav_value, risk_blocks, risk_blocker = _risk_sizing(payloads["risk_sizing_authority_v1"])
    global_blockers = _global_blockers(paths, payloads, root)
    if risk_blocker:
        global_blockers.append(risk_blocker)

    attribution = payloads["sleeve_intent_trade_attribution_v1"]
    outcome = payloads["trade_outcome_v1"]
    edge = payloads["edge_attribution_v1"]
    scorecard = payloads["weekly_scorecard_view_v1"]
    selection = payloads["selection_quality_v1"]
    decision = payloads["decision_consistency_v1"]
    regime = payloads["regime_confidence_v1"]
    missed = payloads["missed_opportunity_v1"]

    evidence_refs = [_artifact_ref(name, path) for name, path in paths.items()]
    rows: list[dict[str, Any]] = []
    for sleeve_id in _sleeve_ids(payloads):
        intent_count, submitted_count, completed_count = _attribution_counts(attribution, sleeve_id)
        outcome_status = _outcome_for_sleeve(outcome, sleeve_id)
        outcome_has_real_evidence = _outcome_has_real_evidence(outcome, sleeve_id)
        edge_status = _edge_for_sleeve(edge, sleeve_id)
        scorecard_status = _scorecard_for_sleeve(scorecard, sleeve_id)
        selection_confidence = (
            str(selection.get("confidence_level") or "UNKNOWN").strip().upper()
            if _canonical_sleeve_id(selection.get("selected_sleeve_id")) == sleeve_id
            else "UNKNOWN"
        )
        regime_confidence = str(regime.get("confidence_level") or regime.get("status") or "UNKNOWN").strip().upper()
        decision_consistency = str(decision.get("ranking_stability") or decision.get("status") or "UNKNOWN").strip().upper()
        missed_status = _status(missed)

        blockers = list(global_blockers)
        if not attribution:
            blockers.append("SLEEVE_INTENT_TRADE_ATTRIBUTION_MISSING")
        if completed_count <= 0:
            blockers.append("NO_COMPLETED_TRADES")
        if outcome_status not in {"CLOSED"}:
            blockers.append(f"TRADE_OUTCOME_{outcome_status}")
        elif not outcome_has_real_evidence:
            blockers.append("TRADE_OUTCOME_PROOF_MISSING")
        if edge_status in {"MISSING", "UNKNOWN", "UNPROVEN", "DEGRADED", "NOT_ENOUGH_EVIDENCE"}:
            blockers.append(f"EDGE_{edge_status}")
        if scorecard_status in {"MISSING", "UNKNOWN", "NOT_ENOUGH_EVIDENCE", "INSUFFICIENT_SAMPLE", "DEGRADED"}:
            blockers.append(f"SCORECARD_{scorecard_status}")
        if regime_confidence in {"MISSING", "UNKNOWN", "DEGRADED"}:
            blockers.append(f"REGIME_{regime_confidence}")
        if decision_consistency in {"MISSING", "UNKNOWN", "DEGRADED"}:
            blockers.append(f"DECISION_CONSISTENCY_{decision_consistency}")
        if missed_status in {"MISSING", "UNKNOWN", "DEGRADED"}:
            blockers.append(f"MISSED_OPPORTUNITY_{missed_status}")
        if risk_blocks:
            blockers.append("RISK_SIZING_BLOCKS_ALLOCATION")

        unique_blockers = sorted(set(item for item in blockers if item))
        proven = not unique_blockers
        evidence_quality = "PROVEN" if proven else ("PARTIAL" if intent_count or submitted_count or outcome_status not in {"MISSING", "NO_MATCHING_OUTCOME"} else "NOT_ENOUGH_EVIDENCE")
        rows.append(
            {
                "sleeve_id": sleeve_id,
                "intent_count": int(intent_count),
                "submitted_trade_count": int(submitted_count),
                "completed_trade_count": int(completed_count),
                "outcome_status": outcome_status,
                "edge_status": edge_status,
                "scorecard_status": scorecard_status,
                "selection_confidence": selection_confidence,
                "regime_confidence": regime_confidence,
                "decision_consistency": decision_consistency,
                "missed_opportunity_status": missed_status,
                "evidence_quality": evidence_quality,
                "allocation_eligible": proven,
                "blockers": unique_blockers,
                "evidence_refs": list(evidence_refs),
            }
        )

    payload = {
        "schema_id": "sleeve_performance_control",
        "schema_version": "sleeve_performance_control.v1",
        "day_utc": day_utc,
        "generated_at": now_iso_v1(),
        "git_commit": git_commit_v1(),
        "git_dirty_status": git_dirty_status_v1(),
        "truth_root": str(root),
        "runtime_root": str(runtime),
        "producer": PRODUCER,
        "authority": "SLEEVE_PERFORMANCE_CONTROL",
        "readiness_authority": "aegis_control_plane_v1",
        "allocation_authority": "strategy_allocation_plan_v1",
        "status": "PASS" if rows and all(row["allocation_eligible"] is True for row in rows) else ("EMPTY" if not rows else "BLOCKED"),
        "risk_sizing_status": risk_status,
        "nav_source": nav_source,
        "nav_value": nav_value,
        "risk_sizing_blocks_allocation": risk_blocks,
        "sleeve_results": rows,
    }
    return payload


def _validate(payload: dict[str, Any]) -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError("SLEEVE_PERFORMANCE_CONTROL_SCHEMA_INVALID:" + ";".join(str(err.message) for err in errors[:3]))


def write_sleeve_performance_control_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any], command: str) -> Path:
    path = sleeve_performance_control_path(truth_root=truth_root, day_utc=day_utc)
    payload["artifact_path"] = str(path)
    input_paths = [
        _report_path(truth_root, "sleeve_intent_trade_attribution_v1", day_utc, "sleeve_intent_trade_attribution.v1.json"),
        _report_path(truth_root, "trade_outcome_v1", day_utc, "trade_outcome.v1.json"),
        _report_path(truth_root, "edge_attribution_v1", day_utc, "edge_attribution.v1.json"),
        _report_path(truth_root, "weekly_scorecard_view_v1", day_utc, "weekly_scorecard_view.v1.json"),
        _report_path(truth_root, "selection_quality_v1", day_utc, "selection_quality.v1.json"),
        _report_path(truth_root, "decision_consistency_v1", day_utc, "decision_consistency.v1.json"),
        _report_path(truth_root, "regime_confidence_v1", day_utc, "regime_confidence.v1.json"),
        _report_path(truth_root, "missed_opportunity_v1", day_utc, "missed_opportunity.v1.json"),
        _report_path(truth_root, "risk_sizing_authority_v1", day_utc, "risk_sizing_authority.v1.json"),
    ]
    attach_producer_contract_v1(
        payload,
        producer_name=PRODUCER,
        producer_command=command,
        input_artifacts=input_paths,
        output_artifacts=[path],
        schema_versions={"sleeve_performance_control_v1": "sleeve_performance_control.v1"},
    )
    _validate(payload)
    write_json_v1(path, payload)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_sleeve_performance_control_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--runtime_root", default="")
    args = parser.parse_args(argv)
    day = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    runtime_root = Path(args.runtime_root).expanduser().resolve() if str(args.runtime_root or "").strip() else truth_root
    payload = build_sleeve_performance_control_v1(day_utc=day, truth_root=truth_root, runtime_root=runtime_root)
    command = f"PYTHONPATH=\"$PWD\" python3 {PRODUCER} --day_utc {day} --truth_root {truth_root}"
    path = write_sleeve_performance_control_v1(truth_root=truth_root, day_utc=day, payload=payload, command=command)
    eligible = [row["sleeve_id"] for row in payload["sleeve_results"] if row["allocation_eligible"]]
    print(json.dumps({"status": payload["status"], "path": str(path), "allocation_eligible": eligible}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
