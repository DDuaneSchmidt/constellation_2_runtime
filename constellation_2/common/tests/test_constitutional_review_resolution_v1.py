from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.constitutional_authorization_v1 import evaluate_constitutional_enforcement_v1  # noqa: E402
from constellation_2.common.constitutional_review_resolution_v1 import (  # noqa: E402
    build_constitutional_operator_decision_v1,
    build_constitutional_review_packet_v1,
    iter_constitutional_operator_decision_paths_v1,
    write_constitutional_operator_decision_v1,
)


def _review_packet() -> dict:
    return build_constitutional_review_packet_v1(
        proposal_hash="1" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        created_at="2026-04-16T12:00:00Z",
        action_type="CLOSE_TRADE",
        action_class="PROTECTIVE",
        target_entities=["PRIMARY", "DU1234567"],
        expected_economic_effect={"effect": "reduce loss"},
        expected_tax_effect={"effect": "possible realization"},
        expected_risk_effect={"effect": "de-risk"},
        admissibility_summary={
            "general_admissibility": "VERIFIED_PARTIAL",
            "tax_admissibility": "INCOMPLETE",
            "dependency_health": "DEGRADED_NON_BLOCKING",
            "state_coherence": "PARTIAL",
        },
        missing_facts=["lot_basis_fact"],
        dependency_issues=["execution_capability_fact"],
        decision_enum="REQUIRE_HUMAN_REVIEW",
        blocker_rules=["POLICY_REQUIRES_HUMAN_REVIEW"],
        negative_evidence=[
            {
                "type": "MISSING_FACT",
                "fact": "lot_basis_fact",
                "severity": "BLOCKING",
                "detail": "required fact type missing: lot_basis_fact",
            }
        ],
        consequence_of_no_action="Protective action remains blocked until an operator decides.",
        effective_scope={
            "global": "PAPER",
            "domain": "POST_ENTRY",
            "account": "DU1234567",
            "sleeve": "PRIMARY",
            "action_class": "PROTECTIVE",
            "effective_authority": "REQUIRE_HUMAN_REVIEW",
        },
        authorization_expires_at="2026-04-16T23:59:59Z",
        visible_fact_summary={
            "required_fact_types": ["execution_capability_fact", "policy_binding_fact", "position_state_fact"],
            "fact_types_present": ["execution_capability_fact", "policy_binding_fact", "position_state_fact"],
        },
        visible_facts=[
            {
                "fact_id": "fact-1",
                "fact_type": "position_state_fact",
                "logical_name": "core2_trade_state",
                "observed_at": "2026-04-16T12:00:00Z",
                "captured_at": "2026-04-16T12:00:01Z",
                "freshness_class": "CURRENT",
                "provenance_class": "AUTHORITATIVE_FILE",
                "content_hash": "3" * 64,
                "general_admissibility": "VERIFIED_COMPLETE",
                "tax_admissibility": "ESTIMATED_POSITION_LEVEL",
                "dependency_health": "HEALTHY",
                "state_coherence": "COHERENT",
                "artifact_path": "/tmp/core2.json",
            }
        ],
    )


def test_review_packet_is_deterministic_and_complete() -> None:
    packet_a = _review_packet()
    packet_b = _review_packet()

    assert packet_a["packet_hash"] == packet_b["packet_hash"]
    assert packet_a["suggested_options"] == ["APPROVE", "REJECT", "DEFER"]
    assert packet_a["visible_facts"][0]["fact_type"] == "position_state_fact"


def test_operator_approve_records_human_override_and_becomes_executable() -> None:
    packet = _review_packet()
    decision_record = build_constitutional_operator_decision_v1(
        review_packet=packet,
        operator_action="APPROVE",
        operator_id="ops-1",
        decided_at="2026-04-16T12:30:00Z",
        source_artifact_type="authorization_v1",
        source_artifact_path="/tmp/source.authorization.v1.json",
        source_artifact_hash="4" * 64,
        operator_note="Approve protective unwind.",
    )

    resolved_authorization = decision_record["resolved_constitutional_authorization"]
    assert decision_record["authorization_source"] == "HUMAN_OVERRIDE"
    assert decision_record["final_decision_applied"] == "AUTO_EXECUTE_PROTECTIVE"
    assert resolved_authorization["authorization_source"] == "HUMAN_OVERRIDE"

    enforcement = evaluate_constitutional_enforcement_v1(
        action_class="PROTECTIVE",
        legacy_status="AUTHORIZED",
        legacy_decision="AUTHORIZED",
        legacy_authorized_quantity=1,
        constitutional_decision_enum=decision_record["final_decision_applied"],
        constitutional_authorization_issuable=True,
        constitutional_authorization=resolved_authorization,
        proposal_hash=packet["proposal_hash"],
        fact_bundle_hash=packet["fact_bundle_hash"],
        policy_version=packet["policy_version"],
        scope_authority="AUTO_EXECUTE_PROTECTIVE",
        evaluated_at="2026-04-16T12:30:00Z",
    )

    assert enforcement["enforcement_result"] == "ALLOWED"


def test_operator_reject_remains_blocked() -> None:
    packet = _review_packet()
    decision_record = build_constitutional_operator_decision_v1(
        review_packet=packet,
        operator_action="REJECT",
        operator_id="ops-2",
        decided_at="2026-04-16T12:31:00Z",
        source_artifact_type="post_entry_submit_boundary_v1",
        source_artifact_path="/tmp/post_entry_submit_boundary.v1.json",
        source_artifact_hash="5" * 64,
    )

    assert decision_record["final_decision_applied"] == "BLOCK"
    assert decision_record["resolved_constitutional_authorization"] is None


def test_operator_decision_write_is_auditable() -> None:
    packet = _review_packet()
    decision_record = build_constitutional_operator_decision_v1(
        review_packet=packet,
        operator_action="DEFER",
        operator_id="ops-3",
        decided_at="2026-04-16T12:32:00Z",
        source_artifact_type="authorization_v1",
        source_artifact_path="/tmp/source.authorization.v1.json",
        source_artifact_hash="6" * 64,
        operator_note="Wait for updated basis.",
    )

    with tempfile.TemporaryDirectory(dir=str(SOURCE_ROOT / "tmp")) as td:
        truth_root = Path(td)
        written_path = write_constitutional_operator_decision_v1(
            truth_root=truth_root,
            day_utc="2026-04-16",
            decision_record=decision_record,
        )
        stored_payload = json.loads(written_path.read_text(encoding="utf-8"))
        matched_paths = iter_constitutional_operator_decision_paths_v1(
            truth_root=truth_root,
            day_utc="2026-04-16",
            proposal_hash=packet["proposal_hash"],
        )

    assert stored_payload["review_packet_hash"] == packet["packet_hash"]
    assert stored_payload["options_shown"] == ["APPROVE", "REJECT", "DEFER"]
    assert stored_payload["source_artifact_ref"]["artifact_path"] == "/tmp/source.authorization.v1.json"
    assert stored_payload["operator_id"] == "ops-3"
    assert stored_payload["decided_at"] == "2026-04-16T12:32:00Z"
    assert matched_paths == [written_path]
