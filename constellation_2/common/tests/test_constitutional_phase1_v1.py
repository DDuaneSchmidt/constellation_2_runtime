from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.constitutional_authorization_v1 import (  # noqa: E402
    build_constitutional_authorization_v1,
    compare_legacy_authorization_to_constitutional_v1,
    evaluate_constitutional_enforcement_v1,
    validate_constitutional_authorization_match_v1,
)
from constellation_2.common.constitutional_decision_v1 import (  # noqa: E402
    DECISION_ENUMS_V1,
    DEPENDENCY_HEALTH_VALUES_V1,
    GENERAL_ADMISSIBILITY_VALUES_V1,
    STATE_COHERENCE_VALUES_V1,
    TAX_ADMISSIBILITY_VALUES_V1,
    evaluate_constitutional_decision_v1,
    resolve_effective_scope_v1,
)
from constellation_2.common.constitutional_proposal_v1 import (  # noqa: E402
    build_constitutional_proposal_v1,
    build_post_entry_request_proposal_v1,
    proposal_hash_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (  # noqa: E402
    build_constitutional_fact_bundle_v1,
    build_constitutional_fact_record_v1,
)


def _proposal(*, action_class: str, required_fact_types: list[str] | None = None) -> dict:
    return build_constitutional_proposal_v1(
        proposal_id=f"proposal:{action_class.lower()}",
        proposal_version="v1",
        created_at="2026-04-16T00:00:00Z",
        source_subsystem="test",
        action_type="TEST_ACTION",
        action_class=action_class,
        target_scope={
            "global": "PAPER",
            "domain": "TRADING",
            "account": "DU1234567",
            "sleeve": "PRIMARY",
            "action_class": action_class,
        },
        target_entities=["DU1234567", "PRIMARY"],
        requested_effect={"status": "TEST"},
        expected_economic_effect={"status": "TEST"},
        expected_tax_effect={"status": "TEST"},
        expected_risk_effect={"status": "TEST"},
        reversibility_class="REVERSIBLE",
        urgency_class="DAY",
        expiration_at="2026-04-16T23:59:59Z",
        required_fact_types=required_fact_types or ["account_state_fact", "execution_capability_fact"],
        required_dependency_checks=["test_dependency"],
        source_reasoning_reference="tests",
        source_policy_bindings=["governance/test-policy"],
        source_artifact_hashes=[{"artifact_ref": "tests/input.json", "sha256": "a" * 64}],
    )


def _fact_record(
    *,
    fact_type: str,
    logical_name: str,
    content_hash: str,
    general_admissibility: str = "VERIFIED_COMPLETE",
    tax_admissibility: str = "ESTIMATED_POSITION_LEVEL",
    dependency_health: str = "HEALTHY",
    state_coherence: str = "COHERENT",
) -> dict:
    return build_constitutional_fact_record_v1(
        fact_type=fact_type,
        source_system="test",
        source_version="v1",
        observed_at="2026-04-16T00:00:00Z",
        captured_at="2026-04-16T00:00:00Z",
        freshness_class="CURRENT",
        provenance_class="AUTHORITATIVE_FILE",
        payload={"logical_name": logical_name},
        scope_keys={"day_utc": "2026-04-16"},
        content_hash=content_hash,
        general_admissibility=general_admissibility,
        tax_admissibility=tax_admissibility,
        dependency_health=dependency_health,
        state_coherence=state_coherence,
        logical_name=logical_name,
        artifact_path=f"/tmp/{logical_name}.json",
    )


def _fact_bundle(*, proposal: dict, fact_records: list[dict]) -> dict:
    return build_constitutional_fact_bundle_v1(
        day_utc="2026-04-16",
        session_id="session:test",
        policy_version="constitutional_shadow_v1",
        required_fact_types=list(proposal["required_fact_types"]),
        fact_records=fact_records,
    )


def _evaluate(
    *,
    proposal: dict,
    fact_bundle: dict,
    scope_authorities: dict[str, str],
    hard_envelope_ok: bool = True,
    policy_blockers: list[str] | None = None,
    persistence_ok: bool = True,
) -> dict:
    return evaluate_constitutional_decision_v1(
        proposal=proposal,
        proposal_hash=proposal_hash_v1(proposal),
        fact_bundle=fact_bundle,
        fact_bundle_hash=str(fact_bundle["fact_bundle_hash"]),
        policy_version="constitutional_shadow_v1",
        scope_authorities=scope_authorities,
        hard_envelope_ok=hard_envelope_ok,
        policy_blockers=list(policy_blockers or []),
        persistence_ok=persistence_ok,
        evaluated_at="2026-04-16T00:00:00Z",
    )


def test_constitutional_proposal_hash_is_deterministic() -> None:
    proposal_a = build_constitutional_proposal_v1(
        proposal_id="proposal:test",
        proposal_version="v1",
        created_at="2026-04-16T00:00:00Z",
        source_subsystem="test",
        action_type="TEST_ACTION",
        action_class="CONSTRUCTIVE",
        target_scope={
            "sleeve": "PRIMARY",
            "account": "DU1234567",
            "domain": "TRADING",
            "action_class": "CONSTRUCTIVE",
            "global": "PAPER",
        },
        target_entities=["PRIMARY", "DU1234567", "PRIMARY"],
        requested_effect={"b": 2, "a": 1},
        expected_economic_effect={"status": "TEST"},
        expected_tax_effect={"status": "TEST"},
        expected_risk_effect={"status": "TEST"},
        reversibility_class="REVERSIBLE",
        urgency_class="DAY",
        expiration_at="2026-04-16T23:59:59Z",
        required_fact_types=["execution_capability_fact", "account_state_fact"],
        required_dependency_checks=["beta", "alpha"],
        source_reasoning_reference="tests",
        source_policy_bindings=["policy-b", "policy-a"],
        source_artifact_hashes=[
            {"artifact_ref": "b", "sha256": "b" * 64},
            {"artifact_ref": "a", "sha256": "a" * 64},
        ],
    )
    proposal_b = build_constitutional_proposal_v1(
        proposal_id="proposal:test",
        proposal_version="v1",
        created_at="2026-04-16T00:00:00Z",
        source_subsystem="test",
        action_type="TEST_ACTION",
        action_class="CONSTRUCTIVE",
        target_scope={
            "global": "PAPER",
            "domain": "TRADING",
            "account": "DU1234567",
            "sleeve": "PRIMARY",
            "action_class": "CONSTRUCTIVE",
        },
        target_entities=["DU1234567", "PRIMARY"],
        requested_effect={"a": 1, "b": 2},
        expected_economic_effect={"status": "TEST"},
        expected_tax_effect={"status": "TEST"},
        expected_risk_effect={"status": "TEST"},
        reversibility_class="REVERSIBLE",
        urgency_class="DAY",
        expiration_at="2026-04-16T23:59:59Z",
        required_fact_types=["account_state_fact", "execution_capability_fact"],
        required_dependency_checks=["alpha", "beta"],
        source_reasoning_reference="tests",
        source_policy_bindings=["policy-a", "policy-b"],
        source_artifact_hashes=[
            {"artifact_ref": "a", "sha256": "a" * 64},
            {"artifact_ref": "b", "sha256": "b" * 64},
        ],
    )
    assert proposal_hash_v1(proposal_a) == proposal_hash_v1(proposal_b)


def test_constitutional_fact_bundle_hash_is_deterministic() -> None:
    proposal = _proposal(action_class="CONSTRUCTIVE")
    record_a = _fact_record(fact_type="account_state_fact", logical_name="account", content_hash="1" * 64)
    record_b = _fact_record(fact_type="execution_capability_fact", logical_name="execution", content_hash="2" * 64)
    bundle_a = _fact_bundle(proposal=proposal, fact_records=[record_a, record_b])
    bundle_b = _fact_bundle(proposal=proposal, fact_records=[record_b, record_a])
    assert bundle_a["fact_bundle_hash"] == bundle_b["fact_bundle_hash"]


def test_post_entry_request_proposal_normalization_is_covered() -> None:
    proposal = build_post_entry_request_proposal_v1(
        request={
            "request_id": "req-1",
            "action_class": "CLOSE_TRADE",
            "trade_identity_id": "abc123",
            "requested_quantity": "10",
            "order_parameters": {"limit_price": "500.00"},
            "execution_identity": {
                "environment": "PAPER",
                "sleeve_id": "PRIMARY",
                "account_id": "DU1234567",
                "client_id_orders": 7,
                "execution_root_ref": "sleeve_execution_root_v1::PRIMARY::PAPER",
            },
            "created_at_utc": "2026-04-16T00:00:00Z",
        },
        core2_snapshot_ref={"artifact_path": "/tmp/core2.json", "artifact_sha256": "1" * 64},
        core3_snapshot_ref={"artifact_path": "/tmp/core3.json", "artifact_sha256": "2" * 64},
        identity_snapshot_ref={"artifact_path": "/tmp/identity.json", "snapshot_sha256": "3" * 64},
    )
    assert proposal["proposal_id"] == "req-1"
    assert proposal["action_class"] == "PROTECTIVE"
    assert proposal["target_scope"]["domain"] == "POST_ENTRY"
    assert proposal["required_fact_types"] == [
        "execution_capability_fact",
        "policy_binding_fact",
        "position_state_fact",
    ]


def test_constitutional_enum_sets_are_stable() -> None:
    assert GENERAL_ADMISSIBILITY_VALUES_V1 == (
        "VERIFIED_COMPLETE",
        "VERIFIED_PARTIAL",
        "ESTIMATED",
        "STALE",
        "CONFLICTED",
        "UNAVAILABLE",
        "UNKNOWN",
    )
    assert TAX_ADMISSIBILITY_VALUES_V1 == (
        "EXACT_LOT_LEVEL",
        "ESTIMATED_LOT_LEVEL",
        "ESTIMATED_POSITION_LEVEL",
        "INCOMPLETE",
        "UNKNOWN",
    )
    assert DEPENDENCY_HEALTH_VALUES_V1 == (
        "HEALTHY",
        "DEGRADED_NON_BLOCKING",
        "DEGRADED_BLOCKING",
        "UNAVAILABLE",
    )
    assert STATE_COHERENCE_VALUES_V1 == (
        "COHERENT",
        "PARTIAL",
        "CONFLICTED",
        "STALE",
        "UNKNOWN",
    )
    assert DECISION_ENUMS_V1 == (
        "AUTO_EXECUTE",
        "AUTO_EXECUTE_PROTECTIVE",
        "REQUIRE_HUMAN_REVIEW",
        "ADVISORY_ONLY",
        "DEFER",
        "BLOCK",
        "FREEZE_SCOPE",
    )


def test_admissible_protective_action_is_allowed() -> None:
    proposal = _proposal(action_class="PROTECTIVE")
    bundle = _fact_bundle(
        proposal=proposal,
        fact_records=[
            _fact_record(
                fact_type="account_state_fact",
                logical_name="account",
                content_hash="1" * 64,
                dependency_health="DEGRADED_NON_BLOCKING",
            ),
            _fact_record(
                fact_type="execution_capability_fact",
                logical_name="execution",
                content_hash="2" * 64,
                dependency_health="DEGRADED_NON_BLOCKING",
            ),
        ],
    )
    decision = _evaluate(
        proposal=proposal,
        fact_bundle=bundle,
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "AUTO_EXECUTE",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE",
        },
    )
    assert decision["decision_enum"] == "AUTO_EXECUTE_PROTECTIVE"
    assert decision["authorization_issuable"] is True


def test_inadmissible_constructive_action_is_blocked() -> None:
    proposal = _proposal(action_class="CONSTRUCTIVE")
    bundle = _fact_bundle(
        proposal=proposal,
        fact_records=[
            _fact_record(
                fact_type="account_state_fact",
                logical_name="account",
                content_hash="1" * 64,
                general_admissibility="ESTIMATED",
            ),
            _fact_record(
                fact_type="execution_capability_fact",
                logical_name="execution",
                content_hash="2" * 64,
                general_admissibility="ESTIMATED",
            ),
        ],
    )
    decision = _evaluate(
        proposal=proposal,
        fact_bundle=bundle,
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "AUTO_EXECUTE",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE",
        },
    )
    assert decision["decision_enum"] == "BLOCK"
    assert "CONSTITUTIONAL_ADMISSIBILITY_BLOCK" in decision["blocker_rules"]


def test_negative_evidence_is_structured() -> None:
    proposal = _proposal(action_class="CONSTRUCTIVE", required_fact_types=["account_state_fact", "execution_capability_fact", "policy_binding_fact"])
    bundle = _fact_bundle(
        proposal=proposal,
        fact_records=[
            _fact_record(fact_type="account_state_fact", logical_name="account", content_hash="1" * 64),
            _fact_record(fact_type="execution_capability_fact", logical_name="execution", content_hash="2" * 64),
        ],
    )
    decision = _evaluate(
        proposal=proposal,
        fact_bundle=bundle,
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "AUTO_EXECUTE",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE",
        },
    )
    assert any(
        isinstance(row, dict)
        and row["type"] == "MISSING_FACT"
        and row["fact"] == "policy_binding_fact"
        and row["severity"] == "BLOCKING"
        for row in decision["negative_evidence"]
    )


def test_stale_dependency_causes_scoped_degradation() -> None:
    effective_scope = resolve_effective_scope_v1(
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "AUTO_EXECUTE",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE",
        },
        proposal_scope=_proposal(action_class="PROTECTIVE")["target_scope"],
        action_class="PROTECTIVE",
        dependency_health="DEGRADED_NON_BLOCKING",
    )
    assert effective_scope["effective_authority"] == "AUTO_EXECUTE_PROTECTIVE"


def test_multi_scope_conflict_uses_most_restrictive_resolution() -> None:
    effective_scope = resolve_effective_scope_v1(
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "BLOCK",
            "sleeve": "AUTO_EXECUTE_PROTECTIVE",
            "action_class": "AUTO_EXECUTE",
        },
        proposal_scope=_proposal(action_class="PROTECTIVE")["target_scope"],
        action_class="PROTECTIVE",
        dependency_health="HEALTHY",
    )
    assert effective_scope["effective_authority"] == "BLOCK"


def test_require_review_precedence_over_auto() -> None:
    effective_scope = resolve_effective_scope_v1(
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "REQUIRE_HUMAN_REVIEW",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE",
        },
        proposal_scope=_proposal(action_class="CONSTRUCTIVE")["target_scope"],
        action_class="CONSTRUCTIVE",
        dependency_health="HEALTHY",
    )
    assert effective_scope["effective_authority"] == "REQUIRE_HUMAN_REVIEW"


def test_constructive_action_is_blocked_by_single_blocking_scope() -> None:
    proposal = _proposal(action_class="CONSTRUCTIVE")
    bundle = _fact_bundle(
        proposal=proposal,
        fact_records=[
            _fact_record(fact_type="account_state_fact", logical_name="account", content_hash="1" * 64),
            _fact_record(fact_type="execution_capability_fact", logical_name="execution", content_hash="2" * 64),
        ],
    )
    decision = _evaluate(
        proposal=proposal,
        fact_bundle=bundle,
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "BLOCK",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE",
        },
    )
    assert decision["decision_enum"] == "BLOCK"
    assert "CONSTITUTIONAL_SCOPE_BLOCK" in decision["blocker_rules"]


def test_incomplete_tax_admissibility_forces_constructive_block() -> None:
    proposal = _proposal(action_class="CONSTRUCTIVE")
    bundle = _fact_bundle(
        proposal=proposal,
        fact_records=[
            _fact_record(
                fact_type="account_state_fact",
                logical_name="account",
                content_hash="1" * 64,
                tax_admissibility="INCOMPLETE",
            ),
            _fact_record(
                fact_type="execution_capability_fact",
                logical_name="execution",
                content_hash="2" * 64,
                tax_admissibility="INCOMPLETE",
            ),
        ],
    )
    decision = _evaluate(
        proposal=proposal,
        fact_bundle=bundle,
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "AUTO_EXECUTE",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE",
        },
    )
    assert decision["decision_enum"] == "BLOCK"
    assert "CONSTITUTIONAL_ADMISSIBILITY_BLOCK" in decision["blocker_rules"]


def test_authorization_mismatch_is_rejected() -> None:
    authorization = build_constitutional_authorization_v1(
        proposal_hash="1" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        effective_scope={
            "global": "PAPER",
            "domain": "TRADING",
            "account": "DU1234567",
            "sleeve": "PRIMARY",
            "action_class": "CONSTRUCTIVE",
            "effective_authority": "AUTO_EXECUTE",
        },
        decision_enum="AUTO_EXECUTE",
        issued_at="2026-04-16T00:00:00Z",
        expires_at="2026-04-16T23:59:59Z",
        issuer_identity={
            "issuer": "test",
            "producer_module": "tests",
            "git_sha": "a" * 40,
        },
    )
    with pytest.raises(ValueError, match="FACT_BUNDLE_HASH_MISMATCH"):
        validate_constitutional_authorization_match_v1(
            authorization,
            proposal_hash="1" * 64,
            fact_bundle_hash="3" * 64,
            policy_version="constitutional_shadow_v1",
            scope_authority="AUTO_EXECUTE",
        )


def test_scope_intersection_is_deterministic() -> None:
    effective_scope = resolve_effective_scope_v1(
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "REQUIRE_HUMAN_REVIEW",
            "account": "AUTO_EXECUTE",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE",
        },
        proposal_scope=_proposal(action_class="CONSTRUCTIVE")["target_scope"],
        action_class="CONSTRUCTIVE",
        dependency_health="HEALTHY",
    )
    assert effective_scope["effective_authority"] == "REQUIRE_HUMAN_REVIEW"


def test_persistence_failure_prevents_authorization() -> None:
    proposal = _proposal(action_class="PROTECTIVE")
    bundle = _fact_bundle(
        proposal=proposal,
        fact_records=[
            _fact_record(fact_type="account_state_fact", logical_name="account", content_hash="1" * 64),
            _fact_record(fact_type="execution_capability_fact", logical_name="execution", content_hash="2" * 64),
        ],
    )
    decision = _evaluate(
        proposal=proposal,
        fact_bundle=bundle,
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "AUTO_EXECUTE",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE_PROTECTIVE",
        },
        persistence_ok=False,
    )
    assert decision["authorization_issuable"] is False
    assert decision["decision_enum"] == "BLOCK"
    assert "CONSTITUTIONAL_PERSISTENCE_BLOCK" in decision["blocker_rules"]


def test_decision_is_deterministic_for_same_inputs() -> None:
    proposal = _proposal(action_class="PROTECTIVE")
    bundle = _fact_bundle(
        proposal=proposal,
        fact_records=[
            _fact_record(fact_type="account_state_fact", logical_name="account", content_hash="1" * 64),
            _fact_record(fact_type="execution_capability_fact", logical_name="execution", content_hash="2" * 64),
        ],
    )
    decision_a = _evaluate(
        proposal=proposal,
        fact_bundle=bundle,
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "AUTO_EXECUTE",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE_PROTECTIVE",
        },
    )
    decision_b = _evaluate(
        proposal=proposal,
        fact_bundle=bundle,
        scope_authorities={
            "global": "AUTO_EXECUTE",
            "domain": "AUTO_EXECUTE",
            "account": "AUTO_EXECUTE",
            "sleeve": "AUTO_EXECUTE",
            "action_class": "AUTO_EXECUTE_PROTECTIVE",
        },
    )
    assert decision_a == decision_b


def test_legacy_constitutional_mismatch_detection() -> None:
    comparison = compare_legacy_authorization_to_constitutional_v1(
        legacy_status="AUTHORIZED",
        legacy_decision="AUTHORIZED",
        legacy_authorized_quantity=1,
        constitutional_decision_enum="REQUIRE_HUMAN_REVIEW",
        constitutional_authorization_issuable=False,
    )
    assert comparison["comparison_status"] == "MISMATCH"
    assert comparison["reason_codes"] == ["LEGACY_AUTHORIZED_BUT_CONSTITUTIONAL_NOT_AUTHORIZABLE"]


def test_phase3_non_protective_actions_are_not_enforced() -> None:
    enforcement = evaluate_constitutional_enforcement_v1(
        action_class="CONSTRUCTIVE",
        legacy_status="AUTHORIZED",
        legacy_decision="AUTHORIZED",
        legacy_authorized_quantity=1,
        constitutional_decision_enum="REQUIRE_HUMAN_REVIEW",
        constitutional_authorization_issuable=False,
        constitutional_authorization=None,
        proposal_hash="1" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        scope_authority="REQUIRE_HUMAN_REVIEW",
        evaluated_at="2026-04-16T00:00:00Z",
    )
    assert enforcement["enforcement_applied"] is False
    assert enforcement["enforcement_result"] == "NOT_APPLIED"


def test_phase3_protective_enforcement_allows_when_both_systems_agree() -> None:
    authorization = build_constitutional_authorization_v1(
        proposal_hash="1" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        effective_scope={
            "global": "PAPER",
            "domain": "POST_ENTRY",
            "account": "DU1234567",
            "sleeve": "PRIMARY",
            "action_class": "PROTECTIVE",
            "effective_authority": "AUTO_EXECUTE_PROTECTIVE",
        },
        decision_enum="AUTO_EXECUTE_PROTECTIVE",
        issued_at="2026-04-16T00:00:00Z",
        expires_at="2026-04-16T23:59:59Z",
        issuer_identity={
            "issuer": "test",
            "producer_module": "tests",
            "git_sha": "a" * 40,
        },
    )
    enforcement = evaluate_constitutional_enforcement_v1(
        action_class="PROTECTIVE",
        legacy_status="AUTHORIZED",
        legacy_decision="AUTHORIZED",
        legacy_authorized_quantity=1,
        constitutional_decision_enum="AUTO_EXECUTE_PROTECTIVE",
        constitutional_authorization_issuable=True,
        constitutional_authorization=authorization,
        proposal_hash="1" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        scope_authority="AUTO_EXECUTE_PROTECTIVE",
        evaluated_at="2026-04-16T00:00:00Z",
    )
    assert enforcement["enforcement_applied"] is True
    assert enforcement["enforcement_result"] == "ALLOWED"
    assert enforcement["metrics"]["allowed_count"] == 1


def test_phase3_mismatch_blocks_protective_execution() -> None:
    authorization = build_constitutional_authorization_v1(
        proposal_hash="1" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        effective_scope={
            "global": "PAPER",
            "domain": "POST_ENTRY",
            "account": "DU1234567",
            "sleeve": "PRIMARY",
            "action_class": "PROTECTIVE",
            "effective_authority": "AUTO_EXECUTE_PROTECTIVE",
        },
        decision_enum="AUTO_EXECUTE_PROTECTIVE",
        issued_at="2026-04-16T00:00:00Z",
        expires_at="2026-04-16T23:59:59Z",
        issuer_identity={
            "issuer": "test",
            "producer_module": "tests",
            "git_sha": "a" * 40,
        },
    )
    enforcement = evaluate_constitutional_enforcement_v1(
        action_class="PROTECTIVE",
        legacy_status="AUTHORIZED",
        legacy_decision="AUTHORIZED",
        legacy_authorized_quantity=1,
        constitutional_decision_enum="AUTO_EXECUTE_PROTECTIVE",
        constitutional_authorization_issuable=False,
        constitutional_authorization=authorization,
        proposal_hash="1" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        scope_authority="AUTO_EXECUTE_PROTECTIVE",
        evaluated_at="2026-04-16T00:00:00Z",
    )
    assert enforcement["enforcement_result"] == "BLOCKED"
    assert "LEGACY_CONSTITUTIONAL_MISMATCH" in enforcement["reason_codes"]


def test_phase3_hash_mismatch_blocks_protective_execution() -> None:
    authorization = build_constitutional_authorization_v1(
        proposal_hash="1" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        effective_scope={
            "global": "PAPER",
            "domain": "POST_ENTRY",
            "account": "DU1234567",
            "sleeve": "PRIMARY",
            "action_class": "PROTECTIVE",
            "effective_authority": "AUTO_EXECUTE_PROTECTIVE",
        },
        decision_enum="AUTO_EXECUTE_PROTECTIVE",
        issued_at="2026-04-16T00:00:00Z",
        expires_at="2026-04-16T23:59:59Z",
        issuer_identity={
            "issuer": "test",
            "producer_module": "tests",
            "git_sha": "a" * 40,
        },
    )
    enforcement = evaluate_constitutional_enforcement_v1(
        action_class="PROTECTIVE",
        legacy_status="AUTHORIZED",
        legacy_decision="AUTHORIZED",
        legacy_authorized_quantity=1,
        constitutional_decision_enum="AUTO_EXECUTE_PROTECTIVE",
        constitutional_authorization_issuable=True,
        constitutional_authorization=authorization,
        proposal_hash="f" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        scope_authority="AUTO_EXECUTE_PROTECTIVE",
        evaluated_at="2026-04-16T00:00:00Z",
    )
    assert enforcement["enforcement_result"] == "BLOCKED"
    assert enforcement["enforcement_reason"] == "CONSTITUTIONAL_AUTHORIZATION_PROPOSAL_HASH_MISMATCH"


def test_phase3_expired_authorization_blocks_protective_execution() -> None:
    authorization = build_constitutional_authorization_v1(
        proposal_hash="1" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        effective_scope={
            "global": "PAPER",
            "domain": "POST_ENTRY",
            "account": "DU1234567",
            "sleeve": "PRIMARY",
            "action_class": "PROTECTIVE",
            "effective_authority": "AUTO_EXECUTE_PROTECTIVE",
        },
        decision_enum="AUTO_EXECUTE_PROTECTIVE",
        issued_at="2026-04-15T00:00:00Z",
        expires_at="2026-04-15T23:59:59Z",
        issuer_identity={
            "issuer": "test",
            "producer_module": "tests",
            "git_sha": "a" * 40,
        },
    )
    enforcement = evaluate_constitutional_enforcement_v1(
        action_class="PROTECTIVE",
        legacy_status="AUTHORIZED",
        legacy_decision="AUTHORIZED",
        legacy_authorized_quantity=1,
        constitutional_decision_enum="AUTO_EXECUTE_PROTECTIVE",
        constitutional_authorization_issuable=True,
        constitutional_authorization=authorization,
        proposal_hash="1" * 64,
        fact_bundle_hash="2" * 64,
        policy_version="constitutional_shadow_v1",
        scope_authority="AUTO_EXECUTE_PROTECTIVE",
        evaluated_at="2026-04-16T00:00:00Z",
    )
    assert enforcement["enforcement_result"] == "BLOCKED"
    assert enforcement["enforcement_reason"] == "CONSTITUTIONAL_AUTHORIZATION_EXPIRED"
