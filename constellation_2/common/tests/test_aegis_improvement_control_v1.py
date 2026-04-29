from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_improvement_control_v1 import (
    approve_proposal_v1,
    build_improvement_control_review_v1,
    build_improvement_control_report_v1,
    create_policy_from_approved_proposal_v1,
    create_rollback_record_v1,
    ingest_paper_session_divergence_evidence_v1,
    ingest_preflight_output_evidence_v1,
    make_action_proposal_v1,
    make_evidence_record_v1,
    make_finding_record_v1,
    make_measurement_placeholder_from_replay_or_backtest_v1,
    measure_policy_impact_v1,
    policy_state_is_runtime_eligible_v1,
    render_improvement_control_review_markdown_v1,
    reject_proposal_v1,
    test_first_proposal_v1 as make_test_first_decision_v1,
    write_improvement_control_review_artifacts_v1,
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
REVIEW_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_improvement_control_review.v1.schema.json"
PREFLIGHT_ARTIFACT = Path("constellation_2/runtime/truth/reports/preopen_preflight_v1/2026-04-08/preopen_preflight.v1.json")
DIVERGENCE_ARTIFACT = Path("constellation_2/runtime/truth/reports/paper_session_divergence_v1/2026-04-08/paper_session_divergence.v1.json")


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


def test_real_preflight_and_divergence_artifacts_ingest_as_evidence() -> None:
    if not (REPO_ROOT / PREFLIGHT_ARTIFACT).exists() or not (REPO_ROOT / DIVERGENCE_ARTIFACT).exists():
        pytest.skip("local Aegis validation artifacts are not present")
    preflight = ingest_preflight_output_evidence_v1(
        artifact_path=PREFLIGHT_ARTIFACT,
        repo_root=REPO_ROOT,
        created_at=TS,
    )
    divergence = ingest_paper_session_divergence_evidence_v1(
        artifact_path=DIVERGENCE_ARTIFACT,
        repo_root=REPO_ROOT,
        created_at=TS,
    )
    assert preflight["evidence_type"] == "preflight_output"
    assert divergence["evidence_type"] == "paper_session_divergence"
    assert preflight["source_path"] == PREFLIGHT_ARTIFACT.as_posix()
    assert divergence["source_path"] == DIVERGENCE_ARTIFACT.as_posix()
    assert preflight["source_sha256"] == "1797d697462753582595f0b2da8c1c2d86c61c03fcecf169e896a9059b66afb0"
    assert divergence["source_sha256"] == "7a718799303ba39a2023063238b6cf971c2180c322936f517685d6e3b9e00e38"
    assert preflight["raw_payload"]["first_true_blocker_code"] == "INTENTS_DAY_COMPLETENESS_MISSING_DAY_DIR"
    assert divergence["raw_payload"]["reason_code"] == "UNDECLARED_DEPENDENCY_ACCESS"
    assert ingest_preflight_output_evidence_v1(artifact_path=PREFLIGHT_ARTIFACT, repo_root=REPO_ROOT, created_at="different")["evidence_id"] == preflight["evidence_id"]
    validate_against_repo_schema_v1(preflight, REPO_ROOT, EVIDENCE_SCHEMA)
    validate_against_repo_schema_v1(divergence, REPO_ROOT, EVIDENCE_SCHEMA)


def test_test_first_decision_does_not_create_policy() -> None:
    proposal = _proposal()
    decision = make_test_first_decision_v1(proposal=proposal, approver="operator", timestamp=TS, notes="Run replay before approval.")
    assert decision["decision"] == "test_first"
    assert decision["resulting_policy_id"] is None
    with pytest.raises(ValueError, match="approved proposal"):
        create_policy_from_approved_proposal_v1(
            proposal=proposal,
            approval=decision,
            policy_version=1,
            policy_payload={"advisory_policy": {"threshold_bps": 25}},
            status="inactive",
            created_at=TS,
        )
    validate_against_repo_schema_v1(decision, REPO_ROOT, APPROVAL_SCHEMA)


def test_measurement_placeholder_can_reference_available_replay_or_backtest_output() -> None:
    if not (REPO_ROOT / DIVERGENCE_ARTIFACT).exists():
        pytest.skip("local replay-like validation artifact is not present")
    policy = _approved_policy(status="inactive")
    measurement = make_measurement_placeholder_from_replay_or_backtest_v1(
        policy_id=policy["policy_id"],
        measurement_window_start="2026-04-08",
        measurement_window_end="2026-04-08",
        artifact_path=DIVERGENCE_ARTIFACT,
        repo_root=REPO_ROOT,
        selected_summary_fields=("schema_id", "day_utc", "status", "reason_code"),
        created_at=TS,
    )
    assert measurement["conclusion"] == "inconclusive"
    assert measurement["before_metrics"]["artifact_available"] is True
    assert measurement["before_metrics"]["source_path"] == DIVERGENCE_ARTIFACT.as_posix()
    assert measurement["before_metrics"]["source_sha256"] == "7a718799303ba39a2023063238b6cf971c2180c322936f517685d6e3b9e00e38"
    assert measurement["before_metrics"]["selected_summary_fields"]["reason_code"] == "UNDECLARED_DEPENDENCY_ACCESS"
    validate_against_repo_schema_v1(measurement, REPO_ROOT, MEASUREMENT_SCHEMA)


def _review_fixture() -> dict:
    evidence = _evidence()
    finding = _finding()
    proposal = _proposal()
    test_first = make_test_first_decision_v1(proposal=proposal, approver="operator", timestamp=TS, notes="Test before approval.")
    approval = approve_proposal_v1(proposal=proposal, approver="operator", timestamp=TS, notes="Approved for inactive policy record.")
    inactive_policy_due = create_policy_from_approved_proposal_v1(
        proposal=proposal,
        approval=approval,
        policy_version=1,
        policy_payload={"advisory_policy": {"threshold_bps": 25}},
        status="inactive",
        created_at=TS,
    )
    inactive_policy_degraded = create_policy_from_approved_proposal_v1(
        proposal=proposal,
        approval=approval,
        policy_version=2,
        policy_payload={"advisory_policy": {"threshold_bps": 26}},
        status="inactive",
        created_at=TS,
    )
    degraded_measurement = measure_policy_impact_v1(
        policy_id=inactive_policy_degraded["policy_id"],
        measurement_window_start="2026-04-01",
        measurement_window_end="2026-04-28",
        before_metrics={"blocked_days": 1},
        after_metrics={"blocked_days": 2},
        success_criteria_met=False,
        rollback_criteria_met=True,
        conclusion="degraded",
        summary="Offline measurement indicates degradation.",
        created_at=TS,
    )
    return {
        "evidence": [evidence],
        "findings": [finding],
        "proposals": [proposal],
        "approvals": [test_first, approval],
        "policies": [inactive_policy_due, inactive_policy_degraded],
        "measurements": [degraded_measurement],
        "rollbacks": [],
    }


def test_review_report_includes_required_sections_and_flags() -> None:
    fixture = _review_fixture()
    review = build_improvement_control_review_v1(day_utc="2026-04-28", generated_at=TS, **fixture)
    assert review["summary"]["advisory_only"] is True
    assert review["summary"]["controls_runtime_behavior"] is False
    assert review["summary"]["controls_broker_execution"] is False
    assert review["summary"]["controls_phasec_materialization"] is False
    assert review["advisory_only"] is True
    assert review["controls_runtime_behavior"] is False
    validate_against_repo_schema_v1(review, REPO_ROOT, REVIEW_SCHEMA)


def test_review_report_includes_findings_with_linked_evidence() -> None:
    fixture = _review_fixture()
    review = build_improvement_control_review_v1(day_utc="2026-04-28", generated_at=TS, **fixture)
    assert review["findings"][0]["finding_id"] == fixture["findings"][0]["finding_id"]
    assert review["findings"][0]["evidence_ids"] == fixture["findings"][0]["evidence_ids"]
    assert review["evidence"][0]["evidence_id"] == fixture["evidence"][0]["evidence_id"]
    assert review["evidence"][0]["source_sha256"] == SHA


def test_review_report_groups_proposals_and_surfaces_test_first() -> None:
    fixture = _review_fixture()
    review = build_improvement_control_review_v1(day_utc="2026-04-28", generated_at=TS, **fixture)
    assert review["summary"]["proposals_by_status"]["proposed"] == 1
    assert len(review["proposals_by_status"]["proposed"]) == 1
    assert review["summary"]["test_first_items_count"] == 1
    assert review["approval_test_queue"]["test_first"][0]["proposal_id"] == fixture["proposals"][0]["proposal_id"]


def test_review_report_shows_inactive_policies_without_active_policy() -> None:
    fixture = _review_fixture()
    review = build_improvement_control_review_v1(day_utc="2026-04-28", generated_at=TS, **fixture)
    assert review["summary"]["inactive_policies_count"] == 2
    assert review["summary"]["active_policies_count"] == 0
    assert {row["status"] for row in review["inactive_policies"]} == {"inactive"}
    assert review["active_policies"] == []


def test_review_report_surfaces_measurements_due_and_rollback_candidates() -> None:
    fixture = _review_fixture()
    review = build_improvement_control_review_v1(day_utc="2026-04-28", generated_at=TS, **fixture)
    assert review["summary"]["measurements_due_count"] == 1
    assert review["measurements_due"][0]["policy_status"] == "inactive"
    assert review["summary"]["rollback_candidates_count"] == 1
    assert review["rollback_candidates"][0]["reason"] == "rollback_criteria_met"
    assert review["rollback_candidates"][0]["conclusion"] == "degraded"


def test_review_report_generation_does_not_mutate_inputs() -> None:
    fixture = _review_fixture()
    before = copy.deepcopy(fixture)
    build_improvement_control_review_v1(day_utc="2026-04-28", generated_at=TS, **fixture)
    assert fixture == before


def test_review_markdown_and_artifact_writer_are_read_only_outputs(tmp_path: Path) -> None:
    fixture = _review_fixture()
    review = build_improvement_control_review_v1(day_utc="2026-04-28", generated_at=TS, **fixture)
    markdown = render_improvement_control_review_markdown_v1(review)
    assert "Aegis Improvement Control Review V1" in markdown
    paths = write_improvement_control_review_artifacts_v1(truth_root=tmp_path, review=review)
    json_path = Path(str(paths["json_path"]))
    markdown_path = Path(str(paths["markdown_path"]))
    assert json_path.exists()
    assert markdown_path.exists()
    assert json.loads(json_path.read_text(encoding="utf-8"))["schema_id"] == "C2_AEGIS_IMPROVEMENT_CONTROL_REVIEW_V1"


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
