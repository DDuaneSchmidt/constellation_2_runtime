from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_improvement_control_v1 import (
    approve_proposal_v1,
    build_improvement_control_report_v1,
    create_policy_from_approved_proposal_v1,
    create_rollback_record_v1,
    make_action_proposal_v1,
    make_evidence_record_v1,
    make_finding_record_v1,
    measure_policy_impact_v1,
    policy_state_is_runtime_eligible_v1,
    reject_proposal_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


TS = "2026-04-28T12:00:00Z"
SHA = "a" * 64

EVIDENCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_improvement_evidence.v1.schema.json"
FINDING_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_improvement_finding.v1.schema.json"
PROPOSAL_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_action_proposal.v1.schema.json"
APPROVAL_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_policy_approval.v1.schema.json"
POLICY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_versioned_policy.v1.schema.json"
MEASUREMENT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_policy_measurement.v1.schema.json"
ROLLBACK_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_policy_rollback.v1.schema.json"
REPORT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_improvement_control_report.v1.schema.json"


def _evidence() -> dict:
    return make_evidence_record_v1(
        timestamp=TS,
        evidence_type="trade_outcome",
        source_path="truth/reports/example.json",
        source_sha256=SHA,
        summary="Closed trade missed expected reward.",
        raw_payload={"closed_trades": 1},
        created_at=TS,
    )


def _finding() -> dict:
    evidence = _evidence()
    return make_finding_record_v1(
        timestamp=TS,
        title="Sleeve underperformed in chop",
        category="trade_review",
        affected_scope="sleeve",
        affected_ids=["CORE_TEST"],
        severity="medium",
        confidence="high",
        observation="Sleeve had repeated weak exits in observed chop.",
        evidence_ids=[evidence["evidence_id"]],
        suggested_action_summary="Review threshold bounds before proposing any change.",
        created_at=TS,
    )


def _proposal() -> dict:
    finding = _finding()
    return make_action_proposal_v1(
        finding_id=finding["finding_id"],
        proposal_type="adjust_threshold_bounded",
        title="Bound threshold review",
        rationale="Evidence supports a bounded test proposal only.",
        affected_scope="risk_config",
        affected_ids=["CORE_TEST"],
        current_value={"threshold_bps": 20},
        proposed_value={"threshold_bps": 25},
        bounds={"min_threshold_bps": 15, "max_threshold_bps": 30},
        expected_impact="Reduce weak entries without changing runtime behavior in V1.",
        risk_notes="Advisory only until later approval phase.",
        success_criteria=["drawdown_not_worse", "trade_quality_improves"],
        rollback_criteria=["drawdown_worse"],
        proposed_duration="5 trading days",
        created_at=TS,
    )


def _approved_policy(status: str = "draft") -> dict:
    proposal = _proposal()
    approval = approve_proposal_v1(proposal=proposal, approver="operator", timestamp=TS, notes="Approved for policy record only.")
    policy = create_policy_from_approved_proposal_v1(
        proposal=proposal,
        approval=approval,
        policy_version=1,
        policy_payload={"advisory_policy": {"threshold_bps": 25}},
        status=status,
        created_at=TS,
    )
    return dict(policy)


def test_evidence_record_has_deterministic_id_and_valid_schema() -> None:
    first = _evidence()
    second = _evidence()
    assert first["evidence_id"] == second["evidence_id"]
    validate_against_repo_schema_v1(first, REPO_ROOT, EVIDENCE_SCHEMA)


def test_finding_requires_evidence_and_valid_schema() -> None:
    with pytest.raises(ValueError, match="evidence_ids"):
        make_finding_record_v1(
            timestamp=TS,
            title="No evidence",
            category="trade_review",
            affected_scope="sleeve",
            affected_ids=["CORE_TEST"],
            severity="medium",
            confidence="low",
            observation="Missing evidence should fail.",
            evidence_ids=[],
            suggested_action_summary="Do nothing.",
            created_at=TS,
        )
    validate_against_repo_schema_v1(_finding(), REPO_ROOT, FINDING_SCHEMA)


def test_proposal_requires_finding_and_valid_schema() -> None:
    with pytest.raises(ValueError, match="finding_id"):
        make_action_proposal_v1(
            finding_id="",
            proposal_type="no_action_monitor",
            title="Monitor",
            rationale="No action.",
            affected_scope="portfolio",
            affected_ids=[],
            current_value={},
            proposed_value={},
            bounds={},
            expected_impact="None.",
            risk_notes="None.",
            created_at=TS,
        )
    validate_against_repo_schema_v1(_proposal(), REPO_ROOT, PROPOSAL_SCHEMA)


def test_proposal_type_must_be_allowed() -> None:
    finding = _finding()
    with pytest.raises(ValueError, match="proposal_type"):
        make_action_proposal_v1(
            finding_id=finding["finding_id"],
            proposal_type="auto_tune_strategy",
            title="Invalid",
            rationale="Invalid.",
            affected_scope="portfolio",
            affected_ids=[],
            current_value={},
            proposed_value={},
            bounds={},
            expected_impact="Invalid.",
            risk_notes="Invalid.",
            success_criteria=["ok"],
            rollback_criteria=["bad"],
            created_at=TS,
        )


def test_behavior_changing_proposal_requires_success_criteria() -> None:
    finding = _finding()
    with pytest.raises(ValueError, match="success_criteria"):
        make_action_proposal_v1(
            finding_id=finding["finding_id"],
            proposal_type="adjust_sleeve_allocation",
            title="Missing success",
            rationale="Invalid.",
            affected_scope="allocation",
            affected_ids=["CORE_TEST"],
            current_value={"weight": 1},
            proposed_value={"weight": 2},
            bounds={"max": 2},
            expected_impact="Invalid.",
            risk_notes="Invalid.",
            rollback_criteria=["bad"],
            created_at=TS,
        )


def test_behavior_changing_proposal_requires_rollback_criteria() -> None:
    finding = _finding()
    with pytest.raises(ValueError, match="rollback_criteria"):
        make_action_proposal_v1(
            finding_id=finding["finding_id"],
            proposal_type="adjust_sleeve_allocation",
            title="Missing rollback",
            rationale="Invalid.",
            affected_scope="allocation",
            affected_ids=["CORE_TEST"],
            current_value={"weight": 1},
            proposed_value={"weight": 2},
            bounds={"max": 2},
            expected_impact="Invalid.",
            risk_notes="Invalid.",
            success_criteria=["ok"],
            created_at=TS,
        )


def test_approval_creates_record_without_runtime_behavior() -> None:
    proposal = _proposal()
    approval = approve_proposal_v1(proposal=proposal, approver="operator", timestamp=TS, notes="Approved.")
    assert approval["decision"] == "approve"
    assert proposal["status"] == "proposed"
    assert approval["resulting_policy_id"] is None
    validate_against_repo_schema_v1(approval, REPO_ROOT, APPROVAL_SCHEMA)


def test_rejected_proposal_cannot_create_active_policy() -> None:
    proposal = _proposal()
    rejection = reject_proposal_v1(proposal=proposal, approver="operator", timestamp=TS, notes="Rejected.")
    with pytest.raises(ValueError, match="approved proposal"):
        create_policy_from_approved_proposal_v1(
            proposal=proposal,
            approval=rejection,
            policy_version=1,
            policy_payload={"advisory_policy": {"threshold_bps": 25}},
            status="active",
            created_at=TS,
        )


def test_approved_proposal_can_create_draft_and_inactive_policy() -> None:
    proposal = _proposal()
    approval = approve_proposal_v1(proposal=proposal, approver="operator", timestamp=TS, notes="Approved.")
    draft = create_policy_from_approved_proposal_v1(
        proposal=proposal,
        approval=approval,
        policy_version=1,
        policy_payload={"advisory_policy": {"threshold_bps": 25}},
        status="draft",
        created_at=TS,
    )
    inactive = create_policy_from_approved_proposal_v1(
        proposal=proposal,
        approval=approval,
        policy_version=2,
        policy_payload={"advisory_policy": {"threshold_bps": 25}},
        status="inactive",
        created_at=TS,
    )
    assert draft["status"] == "draft"
    assert inactive["status"] == "inactive"
    validate_against_repo_schema_v1(draft, REPO_ROOT, POLICY_SCHEMA)
    validate_against_repo_schema_v1(inactive, REPO_ROOT, POLICY_SCHEMA)


def test_active_policy_is_immutable() -> None:
    proposal = _proposal()
    approval = approve_proposal_v1(proposal=proposal, approver="operator", timestamp=TS, notes="Approved.")
    active = create_policy_from_approved_proposal_v1(
        proposal=proposal,
        approval=approval,
        policy_version=1,
        policy_payload={"advisory_policy": {"threshold_bps": 25}},
        status="active",
        created_at=TS,
    )
    with pytest.raises(TypeError):
        active["status"] = "inactive"  # type: ignore[index]
    with pytest.raises(TypeError):
        active["policy_payload"]["advisory_policy"]["threshold_bps"] = 30  # type: ignore[index]


def test_unknown_policy_state_fails_closed() -> None:
    policy = _approved_policy(status="draft")
    policy["status"] = "mystery"
    report = build_improvement_control_report_v1(policies=[policy], generated_at=TS)
    assert policy_state_is_runtime_eligible_v1(policy) is False
    assert report["unknown_policy_state_fail_closed"] is True
    assert report["active_policies"] == []


@pytest.mark.parametrize("conclusion", ["improved", "degraded", "inconclusive"])
def test_measurement_can_mark_improved_degraded_or_inconclusive(conclusion: str) -> None:
    policy = _approved_policy(status="draft")
    measurement = measure_policy_impact_v1(
        policy_id=policy["policy_id"],
        measurement_window_start="2026-04-01",
        measurement_window_end="2026-04-28",
        before_metrics={"trades": 5},
        after_metrics={"trades": 5},
        success_criteria_met=conclusion == "improved",
        rollback_criteria_met=conclusion == "degraded",
        conclusion=conclusion,
        summary=f"Marked {conclusion}.",
        created_at=TS,
    )
    assert measurement["conclusion"] == conclusion
    validate_against_repo_schema_v1(measurement, REPO_ROOT, MEASUREMENT_SCHEMA)


def test_rollback_record_references_prior_policy_and_valid_schema() -> None:
    policy = _approved_policy(status="draft")
    rollback = create_rollback_record_v1(
        policy_id=policy["policy_id"],
        trigger_reason="Rollback criteria met.",
        triggered_by="rollback_criteria",
        timestamp=TS,
        restored_policy_id="policy_" + "b" * 64,
        notes="Restore prior policy record.",
    )
    assert rollback["policy_id"] == policy["policy_id"]
    assert rollback["restored_policy_id"] == "policy_" + "b" * 64
    validate_against_repo_schema_v1(rollback, REPO_ROOT, ROLLBACK_SCHEMA)


def test_control_report_includes_advisory_only_flags_and_valid_schema() -> None:
    finding = _finding()
    proposal = _proposal()
    approval = approve_proposal_v1(proposal=proposal, approver="operator", timestamp=TS, notes="Approved.")
    policy = create_policy_from_approved_proposal_v1(
        proposal=proposal,
        approval=approval,
        policy_version=1,
        policy_payload={"advisory_policy": {"threshold_bps": 25}},
        status="inactive",
        created_at=TS,
    )
    report = build_improvement_control_report_v1(
        findings=[finding],
        proposals=[proposal],
        approvals=[approval],
        policies=[policy],
        generated_at=TS,
    )
    assert report["advisory_only"] is True
    assert report["controls_runtime_behavior"] is False
    assert report["controls_broker_execution"] is False
    assert report["controls_phasec_materialization"] is False
    validate_against_repo_schema_v1(report, REPO_ROOT, REPORT_SCHEMA)


def test_no_broker_phasec_or_sleeve_logic_files_are_changed() -> None:
    result = subprocess.run(
        ["git", "status", "--short"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    changed = {line[3:].strip() for line in result.stdout.splitlines() if line.strip()}
    forbidden_fragments = (
        "/broker/",
        "broker_execution",
        "broker_submit",
        "phaseC",
        "phase_c",
        "phasec_materialization",
        "/sleeves/",
        "sleeve_logic",
        "signal_logic",
    )
    offenders = sorted(path for path in changed if any(fragment in path for fragment in forbidden_fragments))
    assert offenders == []
