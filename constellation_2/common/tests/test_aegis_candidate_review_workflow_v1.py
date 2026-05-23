from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.candidate_lifecycle_v1 import (
    append_candidate_review_action_v1,
    build_candidate_lifecycle_v1,
    candidate_reviews_path_v1,
    write_candidate_lifecycle_reports_v1,
)
from ops.aegis.candidate_manual_capture_v1 import append_manual_external_capture_v1, manual_external_capture_path_v1
from ops.aegis.canonical_operator_state_v1 import build_canonical_operator_state_v1
from ops.aegis.candidate_review_ledger_v1 import build_candidate_review_ledger_v1, filter_candidate_review_ledger_v1, write_candidate_review_ledger_v1


DAY = "2026-05-18"
CID = "review_ffbfca12f6897953a560"


def test_watchlist_persists_with_audit_metadata(tmp_path: Path) -> None:
    _write_promoted_candidate(tmp_path)

    event = append_candidate_review_action_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=CID,
        action="watchlist",
        operator="operator",
        operator_note="Monitor setup.",
        source="TEST",
    )
    lifecycle = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    row = lifecycle["candidates"][0]

    assert event["prior_state"] == "REVIEW_REQUIRED"
    assert event["new_state"] == "WATCHLISTED"
    assert row["review_state"] == "WATCHLISTED"
    assert row["latest_operator_note"] == "Monitor setup."
    assert row["review_action_history_count"] == 1
    assert row["safety"]["broker_execution_allowed"] is False
    assert event["automatic_approval_allowed"] is False


def test_dismiss_and_needs_more_evidence_persist(tmp_path: Path) -> None:
    _write_promoted_candidate(tmp_path)

    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID, action="needs-more-evidence", operator="operator", operator_note="Need source packet.")
    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID, action="dismiss", operator="operator", operator_note="Dismiss after review.")
    row = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)["candidates"][0]

    assert row["review_state"] == "DISMISSED"
    assert row["latest_operator_note"] == "Dismiss after review."
    assert row["review_action_history_count"] == 2
    assert [event["new_state"] for event in row["review_action_history"]] == ["NEEDS_MORE_EVIDENCE", "DISMISSED"]


def test_add_note_persists_without_state_change(tmp_path: Path) -> None:
    _write_promoted_candidate(tmp_path)

    event = append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID, action="add-note", operator="operator", operator_note="Keep review open.")
    row = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)["candidates"][0]

    assert event["prior_state"] == "REVIEW_REQUIRED"
    assert event["new_state"] == "REVIEW_REQUIRED"
    assert row["review_state"] == "REVIEW_REQUIRED"
    assert row["latest_operator_note"] == "Keep review open."


def test_expired_candidates_are_not_active_opportunities(tmp_path: Path) -> None:
    _write_promoted_candidate(tmp_path, generated_at="2020-01-01T00:00:00Z")

    lifecycle = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    write_candidate_lifecycle_reports_v1(truth_root=tmp_path, day_utc=DAY, payload=lifecycle)
    canonical = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert lifecycle["candidates"][0]["review_state"] == "EXPIRED"
    assert canonical["candidates"]["expired"][0]["candidate_id"] == CID
    assert canonical["opportunities"]["open"] == []
    assert canonical["opportunities"]["expired"][0]["candidate_id"] == CID


def test_review_actions_reject_execution_or_approval_actions(tmp_path: Path) -> None:
    _write_promoted_candidate(tmp_path)

    for action in ["approve", "execute", "route", "allocate"]:
        try:
            append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID, action=action, operator="operator")
        except ValueError as exc:
            assert "action must be one of" in str(exc)
        else:
            raise AssertionError(f"unsafe action accepted: {action}")


def test_review_audit_records_every_action(tmp_path: Path) -> None:
    _write_promoted_candidate(tmp_path)

    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID, action="watchlist", operator="operator", operator_note="one")
    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID, action="add-note", operator="operator", operator_note="two")

    path = candidate_reviews_path_v1(truth_root=tmp_path, day_utc=DAY)
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

    assert len(rows) == 2
    assert rows[0]["prior_state"] == "REVIEW_REQUIRED"
    assert rows[0]["new_state"] == "WATCHLISTED"
    assert rows[1]["prior_state"] == "WATCHLISTED"
    assert rows[1]["new_state"] == "WATCHLISTED"
    assert all(row["broker_execution_allowed"] is False for row in rows)
    assert all(row["autonomous_execution_allowed"] is False for row in rows)
    assert all(row["automatic_approval_allowed"] is False for row in rows)


def test_manual_external_capture_is_append_only_and_not_execution(tmp_path: Path) -> None:
    _write_promoted_candidate(tmp_path)

    event = append_manual_external_capture_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=CID,
        manually_captured=True,
        quantity=12,
        capture_timestamp="2026-05-18T15:55:00Z",
        external_execution_venue="external-paper-ledger",
        operator_notes="Operator declared an external paper capture.",
        confidence_override="medium",
        paper_trade_only=True,
        review_decision="MANUAL_CAPTURE_RECORDED",
        operator="operator",
    )
    path = manual_external_capture_path_v1(truth_root=tmp_path, day_utc=DAY)
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

    assert event["event_type"] == "MANUAL_EXTERNAL_CAPTURE_RECORDED"
    assert event["operator_statement"] == "Aegis did not execute this trade."
    assert event["broker_execution_allowed"] is False
    assert event["order_routing_allowed"] is False
    assert event["live_trading_allowed"] is False
    assert event["autonomous_execution_allowed"] is False
    assert event["automatic_approval_allowed"] is False
    assert len(rows) == 1
    assert rows[0]["candidate_id"] == CID


def test_manual_external_capture_rejects_execution_like_payloads(tmp_path: Path) -> None:
    _write_promoted_candidate(tmp_path)

    try:
        append_manual_external_capture_v1(
            truth_root=tmp_path,
            day_utc=DAY,
            candidate_id=CID,
            manually_captured=True,
            quantity="",
            capture_timestamp="2026-05-18T15:55:00Z",
            review_decision="APPROVE",
            operator="operator",
        )
    except ValueError as exc:
        assert "review_decision" in str(exc) or "quantity" in str(exc)
    else:
        raise AssertionError("unsafe manual capture payload was accepted")


def test_review_ledger_includes_watchlisted_candidate(tmp_path: Path) -> None:
    _write_promoted_candidate(tmp_path)
    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID, action="watchlist", operator="operator", operator_note="watch")

    ledger = build_candidate_review_ledger_v1(truth_root=tmp_path, day_utc=DAY)
    row = ledger["candidates"][0]

    assert row["candidate_id"] == CID
    assert row["current_review_state"] == "WATCHLISTED"
    assert row["active"] is True
    assert row["latest_operator_note"] == "watch"


def test_review_ledger_includes_dismissed_and_needs_more_evidence_candidates(tmp_path: Path) -> None:
    dismissed_id = "review-dismissed"
    evidence_id = "review-evidence"
    _write_promoted_candidate(tmp_path, candidate_id=dismissed_id)
    _write_promoted_candidate(tmp_path, candidate_id=evidence_id, append=True)

    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=dismissed_id, action="dismiss", operator="operator", operator_note="dismissed")
    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=evidence_id, action="needs-more-evidence", operator="operator", operator_note="needs packet")
    ledger = build_candidate_review_ledger_v1(truth_root=tmp_path, day_utc=DAY)
    by_id = {row["candidate_id"]: row for row in ledger["candidates"]}

    assert by_id[dismissed_id]["current_review_state"] == "DISMISSED"
    assert by_id[dismissed_id]["active"] is False
    assert by_id[evidence_id]["current_review_state"] == "NEEDS_MORE_EVIDENCE"
    assert by_id[evidence_id]["active"] is True


def test_review_ledger_filters_active_and_all_history(tmp_path: Path) -> None:
    active_id = "review-active"
    dismissed_id = "review-dismissed"
    expired_id = "review-expired"
    _write_promoted_candidate(tmp_path, candidate_id=active_id)
    _write_promoted_candidate(tmp_path, candidate_id=dismissed_id, append=True)
    _write_promoted_candidate(tmp_path, candidate_id=expired_id, generated_at="2020-01-01T00:00:00Z", append=True)
    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=active_id, action="watchlist", operator="operator")
    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=dismissed_id, action="dismiss", operator="operator")

    ledger = build_candidate_review_ledger_v1(truth_root=tmp_path, day_utc=DAY)
    active = filter_candidate_review_ledger_v1(ledger, "active")
    all_rows = filter_candidate_review_ledger_v1(ledger, "all")
    expired = filter_candidate_review_ledger_v1(ledger, "expired")

    assert {row["candidate_id"] for row in active["filtered_candidates"]} == {active_id}
    assert {row["candidate_id"] for row in all_rows["filtered_candidates"]} == {active_id, dismissed_id, expired_id}
    assert expired["filtered_candidates"][0]["candidate_id"] == expired_id
    assert expired["filtered_candidates"][0]["active"] is False


def test_expired_watchlisted_candidate_is_historical_with_audit_preserved(tmp_path: Path) -> None:
    expired_watchlisted_id = "review-expired-watchlisted"
    _write_promoted_candidate(tmp_path, candidate_id=expired_watchlisted_id, generated_at="2020-01-01T00:00:00Z")
    append_candidate_review_action_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=expired_watchlisted_id,
        action="watchlist",
        operator="operator",
        operator_note="watch before expiry",
    )

    lifecycle = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    write_candidate_lifecycle_reports_v1(truth_root=tmp_path, day_utc=DAY, payload=lifecycle)
    ledger = build_candidate_review_ledger_v1(truth_root=tmp_path, day_utc=DAY)
    write_candidate_review_ledger_v1(truth_root=tmp_path, day_utc=DAY, payload=ledger)
    canonical = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    expired = filter_candidate_review_ledger_v1(ledger, "expired")
    active = filter_candidate_review_ledger_v1(ledger, "active")
    watchlisted = filter_candidate_review_ledger_v1(ledger, "watchlisted")
    all_rows = filter_candidate_review_ledger_v1(ledger, "all")
    row = expired["filtered_candidates"][0]

    assert row["candidate_id"] == expired_watchlisted_id
    assert row["current_review_state"] == "EXPIRED"
    assert row["expired_from_review_state"] == "WATCHLISTED"
    assert row["active"] is False
    assert row["audit_action_count"] == 1
    assert row["audit_action_history"][0]["action"] == "WATCHLIST"
    assert row["audit_action_history"][0]["operator_note"] == "watch before expiry"
    assert active["filtered_candidates"] == []
    assert watchlisted["filtered_candidates"] == []
    assert all_rows["filtered_candidates"][0]["candidate_id"] == expired_watchlisted_id
    assert canonical["opportunities"]["open"] == []
    assert canonical["opportunities"]["expired"][0]["candidate_id"] == expired_watchlisted_id
    assert canonical["opportunities"]["candidate_review_historical_rows"][0]["candidate_id"] == expired_watchlisted_id
    assert row["no_execution_status"]["executable_status"] == "NON_EXECUTABLE"
    assert row["no_execution_status"]["broker_execution_allowed"] is False
    assert row["no_execution_status"]["automatic_approval_allowed"] is False


def test_review_ledger_audit_history_is_append_only_and_has_no_execution_actions(tmp_path: Path) -> None:
    _write_promoted_candidate(tmp_path)
    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID, action="watchlist", operator="operator", operator_note="one")
    append_candidate_review_action_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID, action="add-note", operator="operator", operator_note="two")

    ledger = build_candidate_review_ledger_v1(truth_root=tmp_path, day_utc=DAY)
    row = ledger["candidates"][0]

    assert [event["operator_note"] for event in row["audit_action_history"]] == ["one", "two"]
    assert row["audit_action_count"] == 2
    assert not {"approve", "execute", "route", "order", "allocate", "broker", "autonomous"} & set(ledger["allowed_review_actions"])
    assert ledger["safety"]["broker_execution_allowed"] is False
    assert ledger["safety"]["autonomous_execution_allowed"] is False
    assert row["no_execution_status"]["broker_execution_allowed"] is False
    assert row["no_execution_status"]["order_routing_allowed"] is False


def _write_promoted_candidate(root: Path, *, candidate_id: str = CID, generated_at: str = "2099-01-01T00:00:00Z", append: bool = False) -> None:
    path = root / "reports" / "promoted_candidate_set_v1" / DAY / "selected_intent_promotion_v1_2026-05-18" / "promoted_candidate_set.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if append and path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
    else:
        payload = {
            "schema_id": "promoted_candidate_set",
            "schema_version": "v1",
            "artifact_id": "promoted_candidate_set_v1",
            "day_utc": DAY,
            "generated_at": generated_at,
            "candidates": [],
        }
    payload["candidates"].append(
        {
            "candidate_id": candidate_id,
            "generated_at": generated_at,
            "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
            "symbol": "QQQ",
            "direction": "LONG",
            "promotion_status": "promoted",
            "executable_status": "NON_EXECUTABLE",
            "review_only": True,
            "human_review_required": True,
            "operator_review_status": "AWAITING_OPERATOR_REVIEW",
            "promotion_contract": {
                "status": "PASS",
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
                "automatic_approval_allowed": False,
            },
        }
    )
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
