from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_chatgpt_control_packet_v1 import (
    build_aegis_chatgpt_control_packet_v1,
    render_aegis_chatgpt_control_packet_summary_v1,
    write_aegis_chatgpt_control_packet_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import build_runtime_truth_kernel_v1, write_runtime_truth_kernel_reports_v1
from ops.tools import build_aegis_chatgpt_control_packet_v1 as cli


DAY = "2026-05-15"
NOW = "2026-05-15T20:55:00Z"
VALID_UNTIL = "2099-01-01T20:55:00Z"


def test_complete_packet_builds_with_all_sections(tmp_path: Path) -> None:
    _write_complete_sources(tmp_path)
    kernel = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=kernel)

    packet = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert packet["schema_id"] == "aegis_chatgpt_control_packet"
    assert packet["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert packet["runtime_evaluation_hash"]
    assert packet["runtime_evaluation_path"].endswith("runtime_evaluation.v1.json")
    assert packet["packet_generated_at_utc"] == NOW
    assert packet["packet_day_utc"] == DAY
    assert packet["packet_freshness_status"] == "CURRENT"
    assert packet["readiness_state"]["target_operating_mode"] == "HUMAN_APPROVED_ADVISORY_RUNTIME"
    assert packet["readiness_state"]["live_broker_trading_policy"] == "DISABLED_BY_DESIGN"
    assert packet["trade_advice_allowed"] is False
    assert packet["manual_trade_capture_allowed"] is False
    assert packet["current_actionable_items"][0]["symbol"] == "SPY"
    assert packet["safety_assertions"]["manual_execution_only"] is True
    for section in [
        "aegis_lite_status",
        "event_monitoring_status",
        "market_context_status",
        "research_lab_status",
        "operator_inbox_status",
        "sleeve_performance_status",
        "ai_feedback_status",
        "dataset_gaps",
        "readiness_state",
        "do_not_claim",
    ]:
        assert section in packet


def test_missing_event_monitoring_marks_partial_context(tmp_path: Path) -> None:
    _write_complete_sources(tmp_path, omit={"event_monitoring_status"})

    packet = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert packet["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert packet["manual_trade_capture_allowed"] is False
    assert any(row["source"] == "event_monitoring_status" for row in packet["stale_or_missing_sources"])


def test_demo_and_dry_run_packets_block_manual_trade_advice(tmp_path: Path) -> None:
    _write_complete_sources(tmp_path, runtime_truth_classification="DEMO_ONLY")
    demo = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    assert demo["runtime_truth_classification"] == "DEMO_ONLY"
    assert demo["manual_trade_capture_allowed"] is False
    assert "DEMO_ONLY_NOT_ACTIONABLE" in demo["blocked_items"][0]["blockers"]

    other = tmp_path / "dry"
    _write_complete_sources(other, runtime_truth_classification="DRY_RUN_ONLY")
    dry = build_aegis_chatgpt_control_packet_v1(truth_root=other, day_utc=DAY, generated_at_utc=NOW)
    assert dry["runtime_truth_classification"] == "DRY_RUN_ONLY"
    assert dry["manual_trade_capture_allowed"] is False
    assert "DRY_RUN_ONLY_NOT_ACTIONABLE" in dry["blocked_items"][0]["blockers"]


def test_stale_required_source_blocks_manual_capture(tmp_path: Path) -> None:
    _write_complete_sources(tmp_path, generated_at="2026-05-10T20:55:00Z")

    packet = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert packet["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert packet["manual_trade_capture_allowed"] is False
    assert any(row["staleness_status"] == "STALE" for row in packet["stale_or_missing_sources"])


def test_do_not_claim_includes_deferred_unproven_items(tmp_path: Path) -> None:
    _write_complete_sources(tmp_path, dataset_missing=True, ai_review=False, omit={"alert_transport_proof"})

    packet = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    joined = "\n".join(packet["do_not_claim"])
    assert "Live email/SMS transport is not proven" in joined
    assert "Do not claim broker submit/transmit" in joined
    assert "Research dataset binding must be current" in joined
    assert "AI feedback is deterministic fallback only" in joined


def test_source_artifact_refs_are_preserved_and_output_is_deterministic(tmp_path: Path) -> None:
    _write_complete_sources(tmp_path)
    kernel = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=kernel)

    first = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    second = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert first["canonical_json_hash"] == second["canonical_json_hash"]
    assert any(row["artifact_type"] == "manual_trade_packet" for row in first["source_artifacts_used"])
    assert first["source_artifact_timestamps"]["manual_trade_packet"] == NOW
    assert first["market_context_status"]["regime_label"] == "TRENDING_UP"


def test_packet_cli_writes_artifact_and_has_no_broker_dependency(tmp_path: Path, capsys) -> None:
    _write_complete_sources(tmp_path)
    kernel = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_runtime_truth_kernel_reports_v1(truth_root=tmp_path, payload=kernel)

    assert cli.main(["--truth_root", str(tmp_path), "--day", DAY, "--generated_at_utc", NOW, "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    payload = json.loads(Path(out["path"]).read_text(encoding="utf-8"))

    assert payload["broker_submit_required"] is False
    assert payload["ib_automation_required"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert "AEGIS CHATGPT CONTROL PACKET" in render_aegis_chatgpt_control_packet_summary_v1(payload)
    assert "target_operating_mode: HUMAN_APPROVED_ADVISORY_RUNTIME" in render_aegis_chatgpt_control_packet_summary_v1(payload)
    assert "live_broker_trading_policy: DISABLED_BY_DESIGN" in render_aegis_chatgpt_control_packet_summary_v1(payload)
    assert write_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, payload=payload).exists()


def _write_complete_sources(
    root: Path,
    *,
    omit: set[str] | None = None,
    runtime_truth_classification: str = "REAL_RUNTIME",
    generated_at: str = NOW,
    dataset_missing: bool = False,
    ai_review: bool = True,
) -> None:
    omit = omit or set()
    sources = {
        "aegis_lite_operating_status": (
            "reports/aegis_lite_operating_status_v1/2026-05-15/aegis_lite_operating_status.v1.json",
            {
                "schema_id": "aegis_lite_operating_status",
                "artifact_id": "aegis_lite_operating_status_v1",
                "day_utc": DAY,
                "generated_at_utc": generated_at,
                "broker_mode": "MANUAL_ONLY",
                "ib_automation_status": "DEFERRED",
                "canonical_eod_timer": "09:50 UTC and 14:50 UTC",
                "manual_execution_only": True,
                "broker_submit_required": False,
                "runtime_truth_classification": runtime_truth_classification,
            },
        ),
        "aegis_lite_eod_report": (
            "reports/aegis_lite_eod_report_v1/2026-05-15/eod-1/aegis_lite_eod_report.v1.json",
            {
                "schema_id": "aegis_lite_eod_report",
                "artifact_id": "aegis_lite_eod_report_v1",
                "day_utc": DAY,
                "run_id": "eod-1",
                "generated_at_utc": generated_at,
                "broker_mode": "MANUAL_ONLY",
                "manual_execution_only": True,
                "broker_submit_required": False,
                "runtime_truth_classification": runtime_truth_classification,
            },
        ),
        "operator_execution_queue": (
            "reports/operator_execution_queue_v1/2026-05-15/eod-1/operator_execution_queue.v1.json",
            {
                "schema_id": "operator_execution_queue",
                "artifact_id": "operator_execution_queue_v1",
                "day_utc": DAY,
                "run_id": "eod-1",
                "generated_at_utc": generated_at,
                "execution_queue": [{"candidate_id": "trade-1", "queue_status": "READY_FOR_MANUAL_ENTRY"}],
                "manual_execution_only": True,
                "broker_submit_required": False,
            },
        ),
        "manual_trade_packet": (
            "reports/manual_trade_packet_v1/2026-05-15/eod-1/manual_trade_packet.v1.json",
            _manual_packet(runtime_truth_classification=runtime_truth_classification, generated_at=generated_at),
        ),
        "event_monitoring_status": (
            "reports/event_monitoring_status_v1/2026-05-15/run/event_monitoring_status.v1.json",
            {
                "schema_id": "event_monitoring_status",
                "artifact_id": "event_monitoring_status_v1",
                "day_utc": DAY,
                "monitor_run_id": "event-run-1",
                "timestamp_utc": generated_at,
                "triggered_events": [],
                "blocked_events": [],
                "market_context": {
                    "status": "PRESENT",
                    "regime_label": "TRENDING_UP",
                    "volatility_classification": "NORMAL_VOL",
                    "breadth_classification": "STRONG_BREADTH",
                    "macro_event_risk_level": "NONE",
                    "stale_data_status": "FRESH",
                },
                "market_snapshot_freshness_status": "FRESH",
                "canonical_eod_state_mutated": False,
            },
        ),
        "event_market_snapshot": (
            "reports/event_market_snapshot_v1/2026-05-15/event_market_snapshot.v1.json",
            {
                "schema_id": "event_market_snapshot",
                "artifact_id": "event_market_snapshot_v1",
                "snapshot_id": "snapshot-1",
                "day_utc": DAY,
                "generated_at_utc": generated_at,
                "market_open_status": "OPEN",
                "trading_day_type": "TRADING_DAY",
                "regime_label": "TRENDING_UP",
                "volatility_classification": "NORMAL_VOL",
                "breadth_classification": "STRONG_BREADTH",
                "macro_event_today": False,
                "macro_event_type": "NONE",
                "macro_event_risk_level": "NONE",
                "stale_data_status": "FRESH",
                "reason_codes": [],
            },
        ),
        "event_rules_registry": (
            "reports/event_rules_registry_v1/2026-05-15/event_rules_registry.v1.json",
            {
                "schema_id": "event_rules_registry",
                "artifact_id": "event_rules_registry_v1",
                "day_utc": DAY,
                "generated_at_utc": generated_at,
                "registry_version": "v1",
                "event_rules": [{"event_rule_id": "panic", "event_type": "PANIC_EXHAUSTION"}],
            },
        ),
        "promoted_sleeve_library": (
            "reports/promoted_sleeve_library_v1/current/promoted_sleeve_library.v1.json",
            {
                "schema_id": "promoted_sleeve_library",
                "artifact_id": "promoted_sleeve_library_v1",
                "generated_at_utc": generated_at,
                "promoted_sleeves": [{"sleeve_id": "sleeve-1"}],
                "sleeves": [],
                "broker_submit_required": False,
            },
        ),
        "research_task_queue": (
            "research_lab/research_task_queue_v1/2026-05-15/index/research_task_queue.v1.json",
            {"schema_id": "research_task_queue", "artifact_id": "research_task_queue_v1", "day_utc": DAY, "generated_at_utc": generated_at, "tasks": []},
        ),
        "operator_inbox_review_report": (
            "operator_inbox_review_report_v1/latest/operator_inbox_review_report.v1.json",
            {"schema_id": "operator_inbox_review_report", "artifact_id": "operator_inbox_review_report_v1", "generated_at_utc": generated_at, "open_count": 0},
        ),
        "sleeve_performance_report": (
            "reports/sleeve_performance_report_v1/2026-05-15/sleeve_performance_report.v1.json",
            {
                "schema_id": "sleeve_performance_report",
                "artifact_id": "sleeve_performance_report_v1",
                "day_utc": DAY,
                "generated_at_utc": generated_at,
                "portfolio_summary": {"total_recommended_trades": 1, "total_executed_trades": 0, "missing_receipt_count": 0, "missing_outcome_count": 0},
                "sleeve_summary": [],
            },
        ),
        "research_dataset_gap": (
            "reports/research_dataset_gap_v1/2026-05-15/research_dataset_gap.v1.json",
            {
                "schema_id": "research_dataset_gap",
                "artifact_id": "research_dataset_gap_v1",
                "day_utc": DAY,
                "generated_at_utc": generated_at,
                "dataset_gaps": [
                    {"dataset_name": "price_data", "current_status": "MISSING" if dataset_missing else "BOUND", "blocker": "PRICE_DATA_NOT_BOUND" if dataset_missing else "", "next_action": ""}
                ],
            },
        ),
        "manual_execution_receipt": (
            "reports/manual_execution_receipt_v1/2026-05-15/run/manual_execution_receipt.v1.json",
            {
                "schema_id": "manual_execution_receipt",
                "artifact_id": "manual_execution_receipt_v1",
                "day_utc": DAY,
                "generated_at_utc": generated_at,
                "receipt_type": "MANUAL_FILL_RECORDED",
                "operator_declared_no_manual_execution": False,
                "manual_fill_present": True,
                "fill_details_present": True,
                "source": "manual_entry",
                "trade_ids": ["trade-1"],
                "evidence_paths": [],
                "evidence_hash": "hash",
                "result": "MANUAL_FILL_RECEIPT_VALID",
                "receipt_id": "receipt-1",
                "broker_submission_by_aegis": False,
                "autonomous_execution": False,
            },
        ),
        "event_validity_gate": (
            "reports/event_validity_gate_v1/2026-05-15/run/event_validity_gate.v1.json",
            {
                "schema_id": "event_validity_gate",
                "artifact_id": "event_validity_gate_v1",
                "day_utc": DAY,
                "generated_at_utc": generated_at,
                "evaluated": True,
                "event_packet_present": True,
                "event_packet_path": "/tmp/event_packet.v1.json",
                "source_event_snapshot_path": "",
                "validity_status": "VALID",
                "reason": "Event packet exists.",
                "evidence_hash": "hash",
                "generated_by_command": "test",
                "validation_command": "test",
            },
        ),
    }
    if ai_review:
        sources["ai_feedback_review"] = (
            "reports/ai_feedback_review_v1/EOD/2026-05-15/ai_feedback_review.v1.json",
            {
                "schema_id": "ai_feedback_review",
                "artifact_id": "ai_feedback_review_v1",
                "review_id": "ai-eod-1",
                "generated_at_utc": generated_at,
                "evidence_gate_status": "PASS",
                "research_tasks_created": [],
                "ai_used": True,
                "deterministic_fallback_used": False,
                "human_review_required": True,
                "production_mutation": False,
            },
        )
    sources["broker_lifecycle_proof"] = (
        "reports/broker_lifecycle_proof_v1/2026-05-15/run/broker_lifecycle_proof.v1.json",
        {
            "schema_id": "broker_lifecycle_proof",
            "schema_version": "v1",
            "artifact_id": "broker_lifecycle_proof_v1",
            "day_utc": DAY,
            "generated_at_utc": generated_at,
            "lifecycle_mode": "PAPER",
            "evaluated": True,
            "broker_connected": True,
            "order_created": True,
            "order_submitted": True,
            "order_acknowledged": True,
            "order_filled": False,
            "order_cancelled": True,
            "account_type": "paper",
            "broker": "IBKR",
            "evidence_paths": [],
            "external_ids_redacted": [],
            "result": "PAPER_LIFECYCLE_CONFIRMED",
            "evidence_hash": "hash",
        },
    )
    sources["alert_transport_proof"] = (
        "reports/alert_transport_proof_v1/2026-05-15/run/alert_transport_proof.v1.json",
        {
            "schema_id": "alert_transport_proof",
            "artifact_id": "alert_transport_proof_v1",
            "day_utc": DAY,
            "generated_at_utc": generated_at,
            "transport_mode": "LIVE",
            "evaluated": True,
            "delivery_attempted": True,
            "delivery_confirmed": True,
            "channel": "email",
            "dry_run": False,
            "provider_message_id": "redacted",
            "destination_redacted": "operator@example.invalid",
            "result": "LIVE_CONFIRMED",
            "evidence_hash": "hash",
        },
    )
    for key, (relpath, payload) in sources.items():
        if key not in omit:
            _write_json(root / relpath, payload)


def _manual_packet(*, runtime_truth_classification: str, generated_at: str) -> dict[str, object]:
    return {
        "schema_id": "manual_trade_packet",
        "artifact_id": "manual_trade_packet_v1",
        "packet_id": "packet-1",
        "run_id": "eod-1",
        "date": DAY,
        "day_utc": DAY,
        "generated_at_utc": generated_at,
        "runtime_truth_classification": runtime_truth_classification,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_status": "DEFERRED",
        "all_candidates_traceable_to_promoted_sleeves": True,
        "trade_candidates": [
            {
                "recommended_trade_id": "trade-1",
                "sleeve_id": "sleeve-1",
                "source_hypothesis_id": "hypothesis-1",
                "symbol": "SPY",
                "side": "BUY",
                "quantity_or_sizing_guidance": "1 share",
                "entry_reference_price": "500.00",
                "stop_price": "490.00",
                "stop_logic": "protective stop below setup invalidation",
                "risk_per_trade": "10.00",
                "valid_until": VALID_UNTIL,
                "runtime_truth_classification": runtime_truth_classification,
                "demo_mode": runtime_truth_classification == "DEMO_ONLY",
                "dry_run_only": runtime_truth_classification == "DRY_RUN_ONLY",
                "actionable": True,
                "do_not_trade_blockers": [],
            }
        ],
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
