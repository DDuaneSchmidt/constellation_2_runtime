from __future__ import annotations

import copy
import json
from pathlib import Path

from ops.aegis.candidate_to_paper_lifecycle_v1 import build_candidate_to_paper_lifecycle_v1, normalized_candidate_to_paper_lifecycle_v1, write_candidate_to_paper_lifecycle_v1
from ops.aegis.outcome_registry_v1 import build_outcome_registry_v1, write_outcome_registry_v1
from ops.aegis.validation_sample_generator_v1 import build_validation_samples_v1, write_validation_samples_v1
from ops.aegis.candidate_to_paper_self_check_v1 import build_candidate_to_paper_self_check_v1, write_candidate_to_paper_self_check_v1

DAY = "2026-06-01"
CANDIDATE_ID = "candidate-C2_TREND_EQ_PRIMARY_V1-AAPL-20260601"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _base_candidate(candidate_id: str = CANDIDATE_ID) -> dict:
    return {
        "candidate_id": candidate_id,
        "raw_signal_id": "signal-aapl",
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
        "hypothesis_id": "HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1",
        "thesis_id": "THESIS_TREND_PERSISTENCE_V1",
        "symbol": "AAPL",
        "contract_validation_status": "VALID",
        "direction": "LONG",
        "instrument_type": "LONG_EQUITY",
        "governance_status": "GOVERNED",
        "entry_reference_price": "100",
        "entry_reference_price_certification_status": "CERTIFIED",
    }


def _seed(root: Path, *, queue_status: str = "AWAITING_REVIEW", include_session: bool = True, include_construction: bool = True, positions: list[dict] | None = None, outcomes: list[dict] | None = None, candidate: dict | None = None) -> None:
    candidate_row = candidate or _base_candidate()
    session_id = "PAPER-2026-06-01-0950"
    _write(root / "reports" / "aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json", {
        "day_utc": DAY,
        "candidate_contracts": [candidate_row],
    })
    _write(root / "reports" / "aegis_candidate_review_packet_v1" / DAY / "candidate_review_packet.v1.json", {
        "day_utc": DAY,
        "paper_session_id": session_id,
        "review_candidates": [{
            **candidate_row,
            "paper_session_id": session_id,
            "review_status": queue_status,
            "promotion_status": "BLOCKED_PENDING_OPERATOR_REVIEW" if queue_status == "AWAITING_REVIEW" else "PROMOTION_ELIGIBLE",
            "paper_session_status": "CANDIDATES_GENERATED",
            "blocker_stage": "PROMOTION_GATE_BLOCKED" if queue_status == "AWAITING_REVIEW" else "NONE",
            "blocker_reason_codes": ["HUMAN_REVIEW_REQUIRED"] if queue_status == "AWAITING_REVIEW" else [],
        }],
    })
    _write(root / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json", {
        "day_utc": DAY,
        "rows": [{
            **candidate_row,
            "status": queue_status,
            "paper_session_id": session_id,
            "decision_reason": "AWAITING_HUMAN_REVIEW" if queue_status == "AWAITING_REVIEW" else "APPROVED_BY_OPERATOR",
            "blocker_reason_codes": ["HUMAN_REVIEW_REQUIRED"] if queue_status == "AWAITING_REVIEW" else [],
        }],
    })
    _write(root / "reports" / "paper_trade_construction_v1" / DAY / "paper_trade_construction.v1.json", {
        "day_utc": DAY,
        "constructed_paper_trades": ([{
            **candidate_row,
            "paper_session_id": session_id,
            "paper_trade_id": "paper-trade-aapl",
            "construction_status": "CONSTRUCTED",
            "missing_fields": [],
        }] if include_construction else []),
    })
    _write(root / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json", {
        "day_utc": DAY,
        "positions": positions or [],
    })
    _write(root / "reports" / "aegis_outcome_registry_v1" / DAY / "outcome_registry.v1.json", {
        "day_utc": DAY,
        "outcomes": outcomes or [],
    })
    if include_session:
        _write(root / "reports" / "aegis_paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json", {
            "day_utc": DAY,
            "sessions": [{"paper_session_id": session_id, "status": "CANDIDATES_GENERATED"}],
        })


def test_valid_candidate_auto_promotes_to_paper_tracking_without_human_approval(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary"]["valid_candidate_contract_count"] == 1
    assert payload["summary"]["review_eligible_count"] == 1
    assert payload["summary"]["promotion_eligible_count"] == 0
    assert payload["summary"]["constructed_paper_trade_count"] == 1
    assert payload["summary"]["paper_positions_created_count"] == 1
    assert payload["summary"]["blocked_from_paper_count"] == 0
    assert payload["summary"]["auto_promoted_to_paper_tracking_count"] == 1
    assert payload["summary"]["human_approved_for_paper_count"] == 0
    row = payload["rows"][0]
    assert row["review_status"] == "AWAITING_REVIEW"
    assert row["promotion_status"] == "AUTO_PROMOTED_TO_PAPER_TRACKING"
    assert row["auto_promotion_status"] == "AUTO_PROMOTED_TO_PAPER_TRACKING"
    assert row["human_approval_status"] == "NOT_HUMAN_APPROVED"
    assert row["paper_tracking_mode"] == "AUTO_PROMOTED_RESEARCH_OBSERVATION"
    assert row["entry_reference_price"] == "100"
    assert row["entry_reference_price_certification_status"] == "CERTIFIED"
    assert row["blocker_classification"] == "NONE"
    assert "AUTO_PROMOTION_ALLOWED" in row["auto_promotion_reason_codes"]
    assert row["repairable_system_issue"] is False


def test_paper_session_missing_is_detected_as_repairable_system_issue(tmp_path: Path) -> None:
    _seed(tmp_path, include_session=False)
    row = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)["rows"][0]

    assert row["paper_session_status"] == "MISSING"
    assert row["blocker_classification"] == "PROMOTION_GATE_BLOCKED"
    assert "PAPER_SESSION_MISSING" in row["blocker_reason_codes"]
    assert row["repairable_system_issue"] is False


def test_paper_construction_success_path_links_ledger_and_outcome(tmp_path: Path) -> None:
    position = {**_base_candidate(), "position_id": "paper-position-aapl", "paper_session_id": "PAPER-2026-06-01-0950"}
    outcome = {**_base_candidate(), "position_id": "paper-position-aapl", "outcome_id": "outcome-aapl", "outcome_state": "OPEN"}
    _seed(tmp_path, queue_status="APPROVED_FOR_PAPER", positions=[position], outcomes=[outcome])
    payload = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["rows"][0]

    assert payload["summary"]["promotion_eligible_count"] == 0
    assert payload["summary"]["paper_positions_created_count"] == 1
    assert payload["summary"]["blocked_from_paper_count"] == 0
    assert row["paper_position_id"] == "paper-position-aapl"
    assert row["outcome_id"] == "outcome-aapl"
    assert row["blocker_classification"] == "NONE"


def test_approved_candidate_without_ledger_entry_is_flagged_as_write_gap(tmp_path: Path) -> None:
    _seed(tmp_path, queue_status="APPROVED_FOR_PAPER")
    row = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)["rows"][0]

    assert row["promotion_status"] == "PROMOTION_ELIGIBLE"
    assert row["human_approval_status"] == "HUMAN_APPROVED"
    assert row["blocker_classification"] == "PAPER_LEDGER_WRITE_FAILED"
    assert "PAPER_LEDGER_WRITE_FAILED" in row["blocker_reason_codes"]
    assert row["repairable_system_issue"] is True


def test_duplicate_candidate_positions_are_reported(tmp_path: Path) -> None:
    first = {**_base_candidate(), "position_id": "paper-position-a"}
    second = {**_base_candidate(), "position_id": "paper-position-b"}
    _seed(tmp_path, queue_status="APPROVED_FOR_PAPER", positions=[first, second])
    row = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)["rows"][0]

    assert row["blocker_classification"] == "DUPLICATE_POSITION_BLOCKED"
    assert "DUPLICATE_POSITION_BLOCKED" in row["blocker_reason_codes"]


def test_uncertified_candidate_is_not_eligible_for_auto_promotion(tmp_path: Path) -> None:
    candidate = {**_base_candidate(), "entry_reference_price_certification_status": "UNCERTIFIED_STALE_PRICE"}
    _seed(tmp_path, candidate=candidate)
    row = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)["rows"][0]

    assert row["promotion_status"] == "AUTO_PROMOTION_NOT_ELIGIBLE"
    assert "ENTRY_REFERENCE_PRICE_UNCERTIFIED" in row["auto_promotion_reason_codes"]


def test_ungoverned_instrument_is_not_eligible_for_auto_promotion(tmp_path: Path) -> None:
    candidate = {**_base_candidate(), "governance_status": "UNGOVERNED"}
    _seed(tmp_path, candidate=candidate)
    row = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)["rows"][0]

    assert row["promotion_status"] == "AUTO_PROMOTION_NOT_ELIGIBLE"
    assert "INSTRUMENT_TYPE_NOT_GOVERNED" in row["auto_promotion_reason_codes"]


def test_missing_lineage_is_not_eligible_for_auto_promotion(tmp_path: Path) -> None:
    candidate = {**_base_candidate(), "hypothesis_id": ""}
    _seed(tmp_path, candidate=candidate)
    row = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)["rows"][0]

    assert row["promotion_status"] == "AUTO_PROMOTION_NOT_ELIGIBLE"
    assert "MISSING_HYPOTHESIS_LINKAGE" in row["auto_promotion_reason_codes"]


def test_auto_promotion_materializes_position_outcome_and_validation_sample_without_live_side_effects(tmp_path: Path) -> None:
    _seed(tmp_path)
    lifecycle = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    write_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY, payload=lifecycle)
    outcomes = build_outcome_registry_v1(truth_root=tmp_path, day_utc=DAY)
    write_outcome_registry_v1(truth_root=tmp_path, day_utc=DAY, payload=outcomes)
    samples = build_validation_samples_v1(truth_root=tmp_path, day_utc=DAY, outcome_registry=outcomes)
    write_validation_samples_v1(truth_root=tmp_path, day_utc=DAY, payload=samples)
    check = build_candidate_to_paper_self_check_v1(truth_root=tmp_path, day_utc=DAY)

    assert outcomes["summary"]["paper_position_count"] == 1
    assert len(samples["samples"]) == 1
    assert samples["samples"][0]["position_id"]
    assert check["ok"] is True
    position = json.loads((tmp_path / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json").read_text())["positions"][0]
    assert position["current_state"] == "AUTO_PROMOTED_TO_PAPER_TRACKING"
    assert position["entry_price"] == "100"
    assert position["source_receipt"]["paper_entry_price"] == "100"
    assert position["trade_advice_allowed"] is False
    assert position["manual_capture_allowed"] is False
    assert position["broker_execution_allowed"] is False
    assert position["autonomous_execution_allowed"] is False


def test_self_check_success_path_requires_lifecycle_and_position_outcome_linkage(tmp_path: Path) -> None:
    position = {**_base_candidate(), "position_id": "paper-position-aapl"}
    outcome = {**_base_candidate(), "position_id": "paper-position-aapl", "outcome_id": "outcome-aapl"}
    _seed(tmp_path, queue_status="APPROVED_FOR_PAPER", positions=[position], outcomes=[outcome])
    lifecycle = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    write_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY, payload=lifecycle)

    check = build_candidate_to_paper_self_check_v1(truth_root=tmp_path, day_utc=DAY)

    assert check["ok"] is True
    assert check["failures"] == []


def test_self_check_failure_path_detects_missing_blocker_reason_codes(tmp_path: Path) -> None:
    candidate = {**_base_candidate(), "entry_reference_price_certification_status": "UNCERTIFIED_MISSING_PRICE"}
    _seed(tmp_path, candidate=candidate)
    lifecycle = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    lifecycle["rows"][0]["blocker_reason_codes"] = []
    write_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY, payload=lifecycle)

    check = build_candidate_to_paper_self_check_v1(truth_root=tmp_path, day_utc=DAY)

    assert check["ok"] is False
    assert any(f["failure_code"] == "CANDIDATE_BLOCKED_FROM_PAPER_MISSING_REASON_CODES" for f in check["failures"])


def test_self_check_failure_path_detects_paper_position_missing_outcome(tmp_path: Path) -> None:
    position = {**_base_candidate(), "position_id": "paper-position-aapl"}
    _seed(tmp_path, queue_status="APPROVED_FOR_PAPER", positions=[position], outcomes=[])
    lifecycle = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    write_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY, payload=lifecycle)

    check = build_candidate_to_paper_self_check_v1(truth_root=tmp_path, day_utc=DAY)

    assert check["ok"] is False
    assert any(f["failure_code"] == "PAPER_POSITION_MISSING_OUTCOME_REGISTRY_ROW" for f in check["failures"])


def test_deterministic_rerun_stability(tmp_path: Path) -> None:
    _seed(tmp_path)
    first = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)

    assert normalized_candidate_to_paper_lifecycle_v1(first) == normalized_candidate_to_paper_lifecycle_v1(second)


def test_2026_06_01_regression_shape_with_nineteen_valid_candidates(tmp_path: Path) -> None:
    candidates = []
    for i in range(19):
        candidates.append({**_base_candidate(f"candidate-{i:02d}"), "symbol": f"SYM{i:02d}"})
    for idx, candidate in enumerate(candidates):
        if idx == 0:
            _seed(tmp_path, candidate=candidate)
        else:
            # Append rows into the existing fixture artifacts to mirror the June 1 review queue shape.
            for family, filename, key in [
                ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json", "candidate_contracts"),
                ("aegis_candidate_review_packet_v1", "candidate_review_packet.v1.json", "review_candidates"),
                ("aegis_paper_review_queue_v1", "paper_review_queue.v1.json", "rows"),
                ("paper_trade_construction_v1", "paper_trade_construction.v1.json", "constructed_paper_trades"),
            ]:
                path = tmp_path / "reports" / family / DAY / filename
                payload = json.loads(path.read_text(encoding="utf-8"))
                row = {**payload[key][0], **candidate}
                if key == "constructed_paper_trades":
                    row["paper_trade_id"] = f"paper-trade-{idx:02d}"
                payload[key].append(row)
                _write(path, payload)
    payload = build_candidate_to_paper_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary"]["valid_candidate_contract_count"] == 19
    assert payload["summary"]["review_eligible_count"] == 19
    assert payload["summary"]["constructed_paper_trade_count"] == 19
    assert payload["summary"]["paper_positions_created_count"] == 19
    assert payload["summary"]["blocked_from_paper_count"] == 0
    assert payload["summary"]["auto_promoted_to_paper_tracking_count"] == 19
    assert all(row["promotion_status"] == "AUTO_PROMOTED_TO_PAPER_TRACKING" for row in payload["rows"])
