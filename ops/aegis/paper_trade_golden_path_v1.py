from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_lite_eod_v1 import artifact_ref_v1
from constellation_2.common.aegis_lite_manual_feedback_v1 import (
    build_edge_cluster_v1,
    build_operator_execution_queue_v1,
    build_trade_outcome_attribution_v1,
    validate_manual_feedback_artifact_v1,
    write_manual_feedback_artifact_v1,
)
from constellation_2.common.aegis_lite_promoted_candidates_v1 import (
    build_promoted_candidate_set_v1,
    validate_promoted_candidate_set_v1,
    write_promoted_candidate_set_v1,
)
from constellation_2.common.aegis_research_lab_v1 import (
    build_manual_execution_receipt_v1,
    build_manual_trade_packet_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.aegis.candidate_lifecycle_v1 import (
    append_candidate_decision_v1,
    update_candidate_outcomes_v1,
    write_candidate_lifecycle_reports_v1,
)
from ops.tools.run_aegis_lite_eod_pipeline_v1 import write_manual_trade_packet_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT_FAMILY = "aegis_paper_trade_golden_path_v1"
DEFAULT_SLEEVE_ID = "C2_EVENT_DISLOCATION_V1"
DEFAULT_SYMBOL = "SPY"


def run_paper_trade_golden_path_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    sleeve_id: str = DEFAULT_SLEEVE_ID,
    symbol: str = DEFAULT_SYMBOL,
    run_id: str = "",
    generated_at_utc: str = "",
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or _now()
    safe_run_id = run_id or f"paper_rehearsal_golden_path_v1_{day_utc}"
    raw_signal = _raw_signal(day_utc=day_utc, run_id=safe_run_id, generated_at_utc=generated_at, sleeve_id=sleeve_id, symbol=symbol)
    candidate = _candidate_from_raw(raw_signal)

    promoted_set = build_promoted_candidate_set_v1(day_utc=day_utc, run_id=safe_run_id, generated_at_utc=generated_at, candidates=[candidate])
    validate_promoted_candidate_set_v1(promoted_set)
    promoted_path = write_promoted_candidate_set_v1(truth_root=root, payload=promoted_set)

    edge_cluster = build_edge_cluster_v1(day_utc=day_utc, run_id=safe_run_id, candidates=[candidate], source_artifact_lineage=[artifact_ref_v1(promoted_path, artifact_type="promoted_candidate_set_v1")])
    validate_manual_feedback_artifact_v1(edge_cluster)
    edge_cluster_path = write_manual_feedback_artifact_v1(truth_root=root, payload=edge_cluster)

    operator_queue = build_operator_execution_queue_v1(
        day_utc=day_utc,
        run_id=safe_run_id,
        generated_at_utc=generated_at,
        candidates=[candidate],
        edge_clusters=edge_cluster,
        source_artifact_lineage=[
            artifact_ref_v1(promoted_path, artifact_type="promoted_candidate_set_v1"),
            artifact_ref_v1(edge_cluster_path, artifact_type="edge_cluster_v1"),
        ],
        input_contract_status="PASS",
    )
    validate_manual_feedback_artifact_v1(operator_queue)
    queue_path = write_manual_feedback_artifact_v1(truth_root=root, payload=operator_queue)

    manual_packet = build_manual_trade_packet_v1(
        packet_id=f"manual_trade_packet:{safe_run_id}",
        run_id=safe_run_id,
        date=day_utc,
        generated_at_utc=generated_at,
        regime_state="PAPER_REHEARSAL",
        trade_candidates=[candidate],
        promoted_sleeve_library=None,
    )
    manual_packet["runtime_truth_classification"] = "DRY_RUN_ONLY"
    for row in manual_packet.get("trade_candidates", []):
        if isinstance(row, dict):
            row["runtime_truth_classification"] = "DRY_RUN_ONLY"
            row["demo_mode"] = True
            row["dry_run_only"] = True
            row["governance_notes"] = "PAPER_REHEARSAL / SIMULATED fixture. Not real candidate generation or trade advice."
            row["manual_execution_checklist"] = [
                "Confirm this is PAPER_REHEARSAL / SIMULATED evidence.",
                "Do not submit, transmit, or route any order from this packet.",
                "Record only simulated/manual paper receipt evidence.",
            ]
    manual_packet["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(manual_packet)
    validate_against_repo_schema_v1(manual_packet, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_trade_packet.v1.schema.json")
    packet_path = write_manual_trade_packet_v1(truth_root=root, payload=manual_packet)

    receipt = build_manual_execution_receipt_v1(
        receipt_id=f"simulated-paper:{safe_run_id}",
        recommended_trade_id=candidate["candidate_id"],
        actual_symbol=symbol,
        actual_side="BUY",
        actual_quantity=1,
        suggested_quantity=1,
        expected_risk=candidate["risk_per_trade"],
        actual_risk=candidate["risk_per_trade"],
        order_type="LIMIT",
        fill_price=candidate["entry_reference_price"],
        fill_timestamp=generated_at,
        fill_timestamp_utc=generated_at,
        stop_order_entered=True,
        stop_price=candidate["stop_price"],
        operator_notes="PAPER_REHEARSAL / SIMULATED receipt generated by deterministic golden path. No broker submit/transmit.",
        deviations_from_recommendation=[],
        source_packet_type="EOD_MANUAL_PACKET",
        source_packet_id=str(manual_packet["packet_id"]),
        fill_before_valid_until=True,
        max_entry_slippage_respected=True,
        deviation_from_entry_reference_price="0.00",
        sizing_quality="MATCHED",
    )
    receipt["day_utc"] = day_utc
    receipt["generated_at_utc"] = generated_at
    receipt["generated_at"] = generated_at
    receipt["receipt_type"] = "SIMULATED_PAPER"
    receipt["manual_fill_present"] = False
    receipt["fill_details_present"] = True
    receipt["result"] = "SIMULATED_PAPER_RECEIPT_VALID"
    receipt["manual_trade_execution_proven"] = False
    receipt["broker_submission_by_aegis"] = False
    receipt["autonomous_execution"] = False
    receipt["trade_advice_allowed"] = False
    receipt["autonomous_execution_allowed"] = False
    receipt["evidence_paths"] = [str(packet_path)]
    receipt["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(receipt)
    validate_against_repo_schema_v1(receipt, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_execution_receipt.v1.schema.json")
    receipt_path = _manual_execution_receipt_path(root=root, day_utc=day_utc, run_id=safe_run_id)
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_bytes(canonical_json_bytes_v1(receipt) + b"\n")

    append_candidate_decision_v1(
        truth_root=root,
        day_utc=day_utc,
        candidate_id=candidate["candidate_id"],
        decision="TRADED_MANUALLY",
        reason="PAPER_REHEARSAL_SIMULATED_RECEIPT_RECORDED",
        operator="aegis_paper_trade_golden_path_v1",
        manual_trade_receipt_id=str(receipt["receipt_id"]),
        intended_shares=1,
        risk_bucket="PAPER_REHEARSAL",
        operator_note="Simulated/manual paper rehearsal only; no broker or autonomous execution.",
        executed_confirmed=False,
    )
    update_candidate_outcomes_v1(
        truth_root=root,
        day_utc=day_utc,
        candidate_id=candidate["candidate_id"],
        outcome_status="OUTCOME_FLAT",
        outcome_window="PAPER_REHEARSAL_SIMULATED",
        outcome_metrics={"realized_pnl": "0.00", "receipt_type": "SIMULATED_PAPER", "simulated": True},
    )
    lifecycle_paths = write_candidate_lifecycle_reports_v1(truth_root=root, day_utc=day_utc)

    attribution = build_trade_outcome_attribution_v1(
        day_utc=day_utc,
        run_id=safe_run_id,
        candidates=[candidate],
        market_outcomes={
            candidate["candidate_id"]: {
                "actual_entry_price": candidate["entry_reference_price"],
                "actual_exit_price": candidate["entry_reference_price"],
                "realized_pnl": "0.00",
                "operator_execution_quality": "SIMULATED_PAPER",
                "implementation_quality": "PAPER_REHEARSAL",
                "sleeve_signal_quality": "FIXTURE_VALID",
            }
        },
        source_artifact_lineage=[
            artifact_ref_v1(packet_path, artifact_type="manual_trade_packet_v1"),
            artifact_ref_v1(receipt_path, artifact_type="manual_execution_receipt_v1"),
        ],
    )
    validate_manual_feedback_artifact_v1(attribution)
    attribution_path = write_manual_feedback_artifact_v1(truth_root=root, payload=attribution)

    report = _report(
        day_utc=day_utc,
        run_id=safe_run_id,
        generated_at_utc=generated_at,
        sleeve_id=sleeve_id,
        symbol=symbol,
        raw_signal=raw_signal,
        candidate=candidate,
        paths={
            "promoted_candidate_set": str(promoted_path),
            "candidate_lifecycle": lifecycle_paths.get("lifecycle", ""),
            "operator_execution_queue": str(queue_path),
            "manual_trade_packet": str(packet_path),
            "manual_execution_receipt": str(receipt_path),
            "trade_outcome_attribution": str(attribution_path),
        },
    )
    report_path = _golden_path_report_path(root=root, day_utc=day_utc, run_id=safe_run_id)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_bytes(canonical_json_bytes_v1(report) + b"\n")
    return {**report, "artifact_path": str(report_path)}


def latest_paper_trade_golden_path_v1(*, truth_root: Path | str, day_utc: str) -> tuple[Path | None, dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    if not root.exists():
        return None, {}
    paths = sorted(path for path in root.rglob("paper_trade_golden_path.v1.json") if path.is_file())
    if not paths:
        return None, {}
    path = paths[-1]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return path, {}
    return path, payload if isinstance(payload, dict) else {}


def _raw_signal(*, day_utc: str, run_id: str, generated_at_utc: str, sleeve_id: str, symbol: str) -> dict[str, Any]:
    return {
        "raw_signal_id": f"raw_signal:{run_id}:{symbol.upper()}",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "sleeve_id": sleeve_id,
        "symbol": symbol.upper(),
        "mode": "PAPER_REHEARSAL",
        "signal_type": "DETERMINISTIC_FIXTURE",
        "direction": "LONG",
        "entry_reference_price": "520.10",
        "stop_price": "514.90",
        "risk_per_trade": "5.20",
        "evidence_path": "fixture://aegis_paper_trade_golden_path_v1",
    }


def _candidate_from_raw(raw_signal: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": f"paper_rehearsal_candidate:{raw_signal['run_id']}:{raw_signal['symbol']}",
        "raw_signal_id": raw_signal["raw_signal_id"],
        "intent_id": raw_signal["raw_signal_id"],
        "sleeve_id": raw_signal["sleeve_id"],
        "source_hypothesis_id": "PAPER_REHEARSAL_FIXTURE_HYPOTHESIS",
        "research_hypothesis_id": "PAPER_REHEARSAL_FIXTURE_HYPOTHESIS",
        "symbol": raw_signal["symbol"],
        "direction": "LONG",
        "instrument_type": "LONG_EQUITY",
        "entry_reference_price": raw_signal["entry_reference_price"],
        "suggested_quantity": 1,
        "sizing_guidance": "PAPER_REHEARSAL fixture: 1 simulated share.",
        "stop_price": raw_signal["stop_price"],
        "stop_logic": "PAPER_REHEARSAL_FIXED_STOP",
        "risk_per_trade": raw_signal["risk_per_trade"],
        "sleeve_ownership": raw_signal["sleeve_id"],
        "confidence": "FIXTURE",
        "conviction": "FIXTURE",
        "reason_codes": ["PAPER_REHEARSAL", "SIMULATED", "CONTRACT_VALIDATION_FIXTURE"],
        "edge_family": "EVENT_DISLOCATION_PAPER_REHEARSAL",
        "thesis_id": "PAPER_REHEARSAL_EVENT_DISLOCATION",
        "shared_risk_tags": ["PAPER_REHEARSAL_US_EQUITY_BETA"],
        "correlated_symbols": ["QQQ"],
        "regime_dependency": "PAPER_REHEARSAL",
        "macro_sensitivity": "SIMULATED",
        "volatility_liquidity_dependency": "SIMULATED",
        "promotion_status": "promoted",
        "executable_status": "EXECUTABLE",
        "governance_status": "PASS",
        "review_only": True,
        "human_review_required": True,
        "operator_review_status": "PAPER_REHEARSAL_SIMULATED",
        "automatic_approval_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "demo_mode": True,
        "dry_run_only": True,
        "runtime_truth_classification": "DRY_RUN_ONLY",
        "promotion_contract": {
            "contract_status": "PASS",
            "mode": "PAPER_REHEARSAL",
            "simulated": True,
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
        },
        "source_artifact_refs": [{"artifact_type": "paper_rehearsal_fixture", "path": raw_signal["evidence_path"]}],
        "source_promotion_refs": [{"artifact_type": "paper_rehearsal_fixture", "path": raw_signal["evidence_path"]}],
    }


def _report(*, day_utc: str, run_id: str, generated_at_utc: str, sleeve_id: str, symbol: str, raw_signal: dict[str, Any], candidate: dict[str, Any], paths: dict[str, str]) -> dict[str, Any]:
    chain = [
        {"stage": "raw_signal", "status": "PASS", "id": raw_signal["raw_signal_id"], "evidence_path": "fixture://aegis_paper_trade_golden_path_v1"},
        {"stage": "candidate", "status": "PASS", "id": candidate["candidate_id"], "evidence_path": paths["promoted_candidate_set"]},
        {"stage": "operator_execution_queue", "status": "PASS", "id": candidate["candidate_id"], "evidence_path": paths["operator_execution_queue"]},
        {"stage": "manual_trade_packet", "status": "PASS", "id": f"manual_trade_packet:{run_id}", "evidence_path": paths["manual_trade_packet"]},
        {"stage": "manual_execution_receipt", "status": "PASS", "id": f"simulated-paper:{run_id}", "receipt_type": "SIMULATED_PAPER", "evidence_path": paths["manual_execution_receipt"]},
        {"stage": "outcome", "status": "PASS", "id": candidate["candidate_id"], "outcome_status": "OUTCOME_FLAT", "evidence_path": paths["trade_outcome_attribution"]},
    ]
    payload = {
        "schema_id": "aegis_paper_trade_golden_path",
        "schema_version": "v1",
        "artifact_id": "aegis_paper_trade_golden_path_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "mode": "PAPER_REHEARSAL",
        "execution": "SIMULATED_MANUAL_PAPER_RECEIPT_ONLY",
        "sleeve_id": sleeve_id,
        "symbol": symbol.upper(),
        "raw_signal_count": 1,
        "candidate_count": 1,
        "receipt_type": "SIMULATED_PAPER",
        "paper_rehearsal_lifecycle_proven": True,
        "contract_validation": {
            "promoted_candidate_set": "PASS",
            "operator_execution_queue": "PASS",
            "manual_trade_packet": "PASS",
            "manual_execution_receipt": "PASS",
            "trade_outcome_attribution": "PASS",
        },
        "chain": chain,
        "artifact_paths": paths,
        "safety": {
            "trade_advice_allowed": False,
            "broker_submit_transmit_allowed": False,
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
            "live_trading_allowed": False,
            "mutates_sleeve_governance": False,
            "real_candidate_generation": False,
        },
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _golden_path_report_path(*, root: Path, day_utc: str, run_id: str) -> Path:
    return root / "reports" / REPORT_FAMILY / day_utc / _safe_run_id(run_id) / "paper_trade_golden_path.v1.json"


def _manual_execution_receipt_path(*, root: Path, day_utc: str, run_id: str) -> Path:
    return root / "reports" / "manual_execution_receipt_v1" / day_utc / _safe_run_id(run_id) / "manual_execution_receipt.v1.json"


def _safe_run_id(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip()) or "paper_rehearsal_golden_path"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
