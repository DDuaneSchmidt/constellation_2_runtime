from __future__ import annotations

import json
import tempfile
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

import constellation_2.common.post_entry_submit_boundary_v1 as post_entry_boundary_module
from constellation_2.common.post_entry_action_request_v1 import seal_post_entry_action_request_v1
from constellation_2.common.post_entry_boundary_operator_surface_v1 import derive_post_entry_operator_surface_v1
from constellation_2.common.post_entry_boundary_snapshot_binding_v1 import build_post_entry_boundary_snapshot_binding_v1
from constellation_2.common.post_entry_core4_shared_v1 import canonical_hash_v1
from constellation_2.common.constitutional_review_resolution_v1 import (
    build_constitutional_operator_decision_v1,
    write_constitutional_operator_decision_v1,
)
from constellation_2.common.post_entry_submit_boundary_v1 import (
    RC_CANCEL_TARGET_UNRESOLVED,
    RC_FLAT_OPEN_CONTRADICTION,
    RC_OPERATOR_DECISION_REJECTED,
    RC_PROTECTION_LINKAGE_MISSING,
    RC_QUANTITY_EXCEEDS_RECONCILED,
    RC_STALE_TRUTH_THRESHOLD,
    STATUS_AUTHORIZED,
    STATUS_BLOCKED,
    STATUS_REVIEW_REQUIRED,
    evaluate_post_entry_submit_boundary_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_ROOT = REPO_ROOT / "ops" / "fixtures" / "core4_post_entry"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _identity_snapshot(*, account_id: str = "DUO847203", client_id_orders: int = 7, snapshot_status: str = "CURRENT") -> dict:
    base = {
        "authority_owner": "execution_identity_binding_v1",
        "snapshot_status": snapshot_status,
        "environment": "PAPER",
        "sleeve_id": "PRIMARY",
        "account_id": account_id,
        "client_id_orders": client_id_orders,
        "execution_root_ref": "sleeve_execution_root_v1::PRIMARY::PAPER",
        "artifact_path": None,
    }
    base["snapshot_id"] = canonical_hash_v1(base)
    base["snapshot_sha256"] = canonical_hash_v1(base)
    return base


def _request_payload(**updates) -> dict:
    payload = _fixture("request.close_trade.v1.json")
    payload.update(updates)
    if "execution_identity" in updates and isinstance(payload["execution_identity"], dict):
        merged = _fixture("request.close_trade.v1.json")["execution_identity"].copy()
        merged.update(updates["execution_identity"])
        payload["execution_identity"] = merged
    return payload


def _core2_snapshot(**updates) -> dict:
    payload = _fixture("core2.incorporated_broker_trade_state.open_long.v1.json")
    for key, value in updates.items():
        if key in {"freshness_basis", "trade_identity_ref"} and isinstance(value, dict):
            merged = dict(payload[key])
            merged.update(value)
            payload[key] = merged
        else:
            payload[key] = value
    return payload


def _core3_projection(**updates) -> dict:
    payload = _fixture("core3.close_authorized_projection.v1.json")
    for key, value in updates.items():
        if key in {"upstream_snapshot_ref", "action_projection"} and isinstance(value, dict):
            merged = dict(payload[key])
            merged.update(value)
            payload[key] = merged
        else:
            payload[key] = value
    return payload


def _bundle(*, request_payload: dict | None = None, core2_snapshot: dict | None = None, core3_projection: dict | None = None, identity_snapshot: dict | None = None):
    request = seal_post_entry_action_request_v1(request_payload or _request_payload())
    binding, bound_core2, bound_core3, bound_identity = build_post_entry_boundary_snapshot_binding_v1(
        request=request,
        core2_snapshot=core2_snapshot or _core2_snapshot(),
        core2_artifact_path=str(FIXTURE_ROOT / "core2.incorporated_broker_trade_state.open_long.v1.json"),
        core3_projection=core3_projection or _core3_projection(),
        execution_identity_snapshot=identity_snapshot or _identity_snapshot(),
        evaluated_at_utc="2026-04-11T12:00:10Z",
    )
    bundle = evaluate_post_entry_submit_boundary_v1(
        request=request,
        binding=binding,
        core2_snapshot=bound_core2,
        core3_projection=bound_core3,
        execution_identity_snapshot=bound_identity,
        evaluated_at_utc="2026-04-11T12:00:10Z",
    )
    return bundle


def _review_required_bundle(*, truth_root: Path, operator_action: str | None = None):
    request = seal_post_entry_action_request_v1(_request_payload())
    binding, bound_core2, bound_core3, bound_identity = build_post_entry_boundary_snapshot_binding_v1(
        request=request,
        core2_snapshot=_core2_snapshot(),
        core2_artifact_path=str(FIXTURE_ROOT / "core2.incorporated_broker_trade_state.open_long.v1.json"),
        core3_projection=_core3_projection(action_projection={"authorization_status": "REVIEW_REQUIRED", "reason_codes": ["CORE3_REVIEW_ONLY"]}),
        execution_identity_snapshot=_identity_snapshot(),
        evaluated_at_utc="2026-04-11T12:00:10Z",
    )
    preview_bundle = evaluate_post_entry_submit_boundary_v1(
        request=request,
        binding=binding,
        core2_snapshot=bound_core2,
        core3_projection=bound_core3,
        execution_identity_snapshot=bound_identity,
        evaluated_at_utc="2026-04-11T12:00:10Z",
    )
    if operator_action is not None:
        review_packet = dict(preview_bundle.boundary.to_dict()["constitutional_shadow"]["review_packet"])
        decision_record = build_constitutional_operator_decision_v1(
            review_packet=review_packet,
            operator_action=operator_action,
            operator_id="ops-reviewer",
            decided_at="2026-04-11T12:10:00Z",
            source_artifact_type="post_entry_submit_boundary_v1",
            source_artifact_path=str((truth_root / "reports" / "post_entry_submit_boundary_v1" / "2026-04-11" / "post_entry_submit_boundary.v1.json").resolve()),
            source_artifact_hash="9" * 64,
        )
        write_constitutional_operator_decision_v1(
            truth_root=truth_root,
            day_utc="2026-04-11",
            decision_record=decision_record,
        )
    return evaluate_post_entry_submit_boundary_v1(
        request=request,
        binding=binding,
        core2_snapshot=bound_core2,
        core3_projection=bound_core3,
        execution_identity_snapshot=bound_identity,
        evaluated_at_utc="2026-04-11T12:00:10Z",
        truth_root=truth_root,
    )


def test_sealed_request_is_immutable_and_schema_valid() -> None:
    request = seal_post_entry_action_request_v1(_request_payload())

    assert request.to_dict()["schema_id"] == "post_entry_action_request"
    assert request.request_seal_id == request.to_dict()["request_seal_id"]
    with pytest.raises(FrozenInstanceError):
        request.request_id = "mutated"  # type: ignore[misc]


def test_request_missing_required_fields_blocks_admission() -> None:
    payload = _request_payload()
    payload.pop("order_parameters")

    with pytest.raises(ValueError, match="REQUEST_MISSING_REQUIRED_FIELD:order_parameters"):
        seal_post_entry_action_request_v1(payload)


def test_unsupported_request_class_blocks_admission() -> None:
    with pytest.raises(ValueError, match="REQUEST_ACTION_CLASS_UNSUPPORTED"):
        seal_post_entry_action_request_v1(_request_payload(action_class="STOP_AND_REVERSE"))


def test_snapshot_binding_is_exact_and_deterministic() -> None:
    request = seal_post_entry_action_request_v1(_request_payload())
    identity = _identity_snapshot()
    binding_a, _, _, _ = build_post_entry_boundary_snapshot_binding_v1(
        request=request,
        core2_snapshot=_core2_snapshot(),
        core2_artifact_path=str(FIXTURE_ROOT / "core2.incorporated_broker_trade_state.open_long.v1.json"),
        core3_projection=_core3_projection(),
        execution_identity_snapshot=identity,
        evaluated_at_utc="2026-04-11T12:00:10Z",
    )
    binding_b, _, _, _ = build_post_entry_boundary_snapshot_binding_v1(
        request=request,
        core2_snapshot=_core2_snapshot(),
        core2_artifact_path=str(FIXTURE_ROOT / "core2.incorporated_broker_trade_state.open_long.v1.json"),
        core3_projection=_core3_projection(),
        execution_identity_snapshot=identity,
        evaluated_at_utc="2026-04-11T12:00:10Z",
    )

    assert binding_a.binding_id == binding_b.binding_id
    assert binding_a.binding_status == "BOUND"
    assert binding_a.core2_snapshot_ref()["artifact_path"].endswith("core2.incorporated_broker_trade_state.open_long.v1.json")
    assert binding_a.core3_snapshot_ref()["snapshot_id"] == "core3-close-authorized-001"
    assert binding_a.execution_identity_snapshot_ref()["account_id"] == "DUO847203"


def test_snapshot_binding_invalidates_stale_and_superseded_snapshots() -> None:
    request = seal_post_entry_action_request_v1(_request_payload())
    stale_binding, _, _, _ = build_post_entry_boundary_snapshot_binding_v1(
        request=request,
        core2_snapshot=_core2_snapshot(),
        core2_artifact_path=str(FIXTURE_ROOT / "core2.incorporated_broker_trade_state.open_long.v1.json"),
        core3_projection=_core3_projection(upstream_snapshot_ref={"snapshot_status": "STALE"}),
        execution_identity_snapshot=_identity_snapshot(),
        evaluated_at_utc="2026-04-11T12:00:10Z",
    )
    superseded_binding, _, _, _ = build_post_entry_boundary_snapshot_binding_v1(
        request=request,
        core2_snapshot=_core2_snapshot(),
        core2_artifact_path=str(FIXTURE_ROOT / "core2.incorporated_broker_trade_state.open_long.v1.json"),
        core3_projection=_core3_projection(upstream_snapshot_ref={"snapshot_status": "SUPERSEDED", "superseded_by_snapshot_id": "newer-core3"}),
        execution_identity_snapshot=_identity_snapshot(),
        evaluated_at_utc="2026-04-11T12:00:10Z",
    )

    assert stale_binding.binding_status == "INVALIDATED"
    assert stale_binding.invalidation_status == "STALE_SNAPSHOT"
    assert superseded_binding.binding_status == "INVALIDATED"
    assert superseded_binding.invalidation_status == "SUPERSEDED_SNAPSHOT"


def test_valid_close_request_is_authorized() -> None:
    bundle = _bundle()
    boundary = bundle.boundary.to_dict()

    assert bundle.boundary.transmission_authorization_status == STATUS_AUTHORIZED
    assert bundle.authorized_payload is not None
    assert bundle.authorized_payload.approved_payload()["requested_quantity"] == "10"
    assert boundary["constitutional_shadow"]["proposal"]["proposal_id"] == "req-close-001"
    assert boundary["constitutional_shadow"]["decision"]["decision_enum"] == "AUTO_EXECUTE_PROTECTIVE"
    assert boundary["constitutional_shadow"]["legacy_constitutional_comparison"]["comparison_status"] == "CONSISTENT"
    assert boundary["enforcement_applied"] is True
    assert boundary["enforcement_scope"] == "action_class"
    assert boundary["enforcement_result"] == "ALLOWED"
    assert boundary["constitutional_enforcement"]["metrics"]["enforced_proposal_count"] == 1
    assert boundary["constitutional_enforcement"]["metrics"]["allowed_count"] == 1


def test_excessive_quantity_is_blocked_without_silent_repair() -> None:
    bundle = _bundle(request_payload=_request_payload(requested_quantity="11"))

    assert bundle.boundary.transmission_authorization_status == STATUS_BLOCKED
    assert RC_QUANTITY_EXCEEDS_RECONCILED in bundle.boundary.to_dict()["blocker_codes"]
    assert bundle.boundary.validated_quantity is None
    assert bundle.authorized_payload is None


def test_flat_position_close_is_blocked() -> None:
    bundle = _bundle(core2_snapshot=_core2_snapshot(current_quantity="0", side="FLAT"))

    assert bundle.boundary.transmission_authorization_status == STATUS_BLOCKED
    assert RC_FLAT_OPEN_CONTRADICTION in bundle.boundary.to_dict()["blocker_codes"]


def test_stale_core2_truth_is_blocked_at_boundary() -> None:
    bundle = _bundle(core2_snapshot=_core2_snapshot(freshness_basis={"age_seconds": 121}))

    assert bundle.binding.binding_status == "BOUND"
    assert bundle.boundary.transmission_authorization_status == STATUS_BLOCKED
    assert RC_STALE_TRUTH_THRESHOLD in bundle.boundary.to_dict()["blocker_codes"]


def test_core3_review_only_posture_blocks_autonomous_authorization() -> None:
    bundle = _bundle(core3_projection=_core3_projection(action_projection={"authorization_status": "REVIEW_REQUIRED", "reason_codes": ["CORE3_REVIEW_ONLY"]}))

    assert bundle.boundary.transmission_authorization_status == STATUS_REVIEW_REQUIRED
    assert bundle.authorized_payload is None
    assert bundle.boundary.to_dict()["enforcement_result"] == "BLOCKED"


def test_phase5_operator_approved_override_allows_execution() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        bundle = _review_required_bundle(
            truth_root=Path(td),
            operator_action="APPROVE",
        )
        boundary = bundle.boundary.to_dict()

    assert boundary["transmission_authorization_status"] == STATUS_AUTHORIZED
    assert bundle.authorized_payload is not None
    assert boundary["constitutional_enforcement"]["enforcement_result"] == "ALLOWED"
    assert boundary["constitutional_shadow"]["constitutional_authorization"]["authorization_source"] == "HUMAN_OVERRIDE"


def test_phase5_operator_rejected_override_blocks_execution() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        bundle = _review_required_bundle(
            truth_root=Path(td),
            operator_action="REJECT",
        )
        boundary = bundle.boundary.to_dict()

    assert boundary["transmission_authorization_status"] == STATUS_BLOCKED
    assert bundle.authorized_payload is None
    assert RC_OPERATOR_DECISION_REJECTED in boundary["blocker_codes"]


def test_phase5_mismatched_override_is_rejected() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        truth_root = Path(td)
        request = seal_post_entry_action_request_v1(_request_payload())
        binding, bound_core2, bound_core3, bound_identity = build_post_entry_boundary_snapshot_binding_v1(
            request=request,
            core2_snapshot=_core2_snapshot(),
            core2_artifact_path=str(FIXTURE_ROOT / "core2.incorporated_broker_trade_state.open_long.v1.json"),
            core3_projection=_core3_projection(action_projection={"authorization_status": "REVIEW_REQUIRED", "reason_codes": ["CORE3_REVIEW_ONLY"]}),
            execution_identity_snapshot=_identity_snapshot(),
            evaluated_at_utc="2026-04-11T12:00:10Z",
        )
        preview_bundle = evaluate_post_entry_submit_boundary_v1(
            request=request,
            binding=binding,
            core2_snapshot=bound_core2,
            core3_projection=bound_core3,
            execution_identity_snapshot=bound_identity,
            evaluated_at_utc="2026-04-11T12:00:10Z",
        )
        review_packet = dict(preview_bundle.boundary.to_dict()["constitutional_shadow"]["review_packet"])
        review_packet["fact_bundle_hash"] = "f" * 64
        decision_record = build_constitutional_operator_decision_v1(
            review_packet=review_packet,
            operator_action="APPROVE",
            operator_id="ops-reviewer",
            decided_at="2026-04-11T12:10:00Z",
            source_artifact_type="post_entry_submit_boundary_v1",
            source_artifact_path=str((truth_root / "reports" / "post_entry_submit_boundary_v1" / "2026-04-11" / "post_entry_submit_boundary.v1.json").resolve()),
            source_artifact_hash="8" * 64,
        )
        write_constitutional_operator_decision_v1(
            truth_root=truth_root,
            day_utc="2026-04-11",
            decision_record=decision_record,
        )
        bundle = evaluate_post_entry_submit_boundary_v1(
            request=request,
            binding=binding,
            core2_snapshot=bound_core2,
            core3_projection=bound_core3,
            execution_identity_snapshot=bound_identity,
            evaluated_at_utc="2026-04-11T12:00:10Z",
            truth_root=truth_root,
        )
        boundary = bundle.boundary.to_dict()

    assert boundary["transmission_authorization_status"] == STATUS_BLOCKED
    assert "CONSTITUTIONAL_AUTHORIZATION_FACT_BUNDLE_HASH_MISMATCH" in boundary["blocker_codes"]


def test_phase3_boundary_blocks_when_constitutional_enforcement_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    original = post_entry_boundary_module.evaluate_constitutional_enforcement_v1

    def _forced_block(**kwargs):
        result = original(**kwargs)
        result["enforcement_result"] = "BLOCKED"
        result["enforcement_reason"] = "LEGACY_CONSTITUTIONAL_MISMATCH"
        result["reason_codes"] = ["LEGACY_CONSTITUTIONAL_MISMATCH"]
        result["mismatch"] = True
        result["metrics"]["allowed_count"] = 0
        result["metrics"]["blocked_count"] = 1
        result["metrics"]["mismatch_count"] = 1
        result["metrics"]["block_reasons"] = {"LEGACY_CONSTITUTIONAL_MISMATCH": 1}
        result["metrics"]["blocked_ratio"] = "1.0000"
        result["metrics"]["warning_triggered"] = True
        result["metrics"]["warning_codes"] = ["CONSTITUTIONAL_ENFORCEMENT_BLOCK_RATE_THRESHOLD_EXCEEDED"]
        return result

    monkeypatch.setattr(post_entry_boundary_module, "evaluate_constitutional_enforcement_v1", _forced_block)

    bundle = _bundle()
    boundary = bundle.boundary.to_dict()

    assert boundary["transmission_authorization_status"] == STATUS_BLOCKED
    assert bundle.authorized_payload is None
    assert boundary["enforcement_result"] == "BLOCKED"
    assert "POST_ENTRY_CONSTITUTIONAL_ENFORCEMENT_BLOCKED" in boundary["blocker_codes"]
    assert "LEGACY_CONSTITUTIONAL_MISMATCH" in boundary["blocker_codes"]
    assert boundary["constitutional_enforcement"]["metrics"]["mismatch_count"] == 1


def test_identity_mismatch_blocks() -> None:
    bundle = _bundle(identity_snapshot=_identity_snapshot(account_id="DIFFERENT"))

    assert bundle.binding.binding_status == "INVALIDATED"
    assert bundle.boundary.transmission_authorization_status == STATUS_BLOCKED


def test_protection_amendment_missing_linkage_blocks() -> None:
    bundle = _bundle(
        request_payload={
            **_request_payload(
                action_class="AMEND_PROTECTION",
                requested_quantity="10",
                target_order_linkage={"order_id": "9999"},
                order_parameters={"stop_price": "495.00"},
            )
        },
        core3_projection=_core3_projection(action_projection={"action_class": "AMEND_PROTECTION"}),
    )

    assert bundle.boundary.transmission_authorization_status == STATUS_BLOCKED
    assert RC_PROTECTION_LINKAGE_MISSING in bundle.boundary.to_dict()["blocker_codes"]


def test_cancel_target_unresolved_blocks() -> None:
    cancel_request = {
        **_request_payload(
            action_class="CANCEL_ORDER",
            order_parameters=None,
            requested_quantity=None,
            target_order_linkage={"order_id": "9999"},
        )
    }
    cancel_request.pop("order_parameters", None)
    cancel_request.pop("requested_quantity", None)
    bundle = _bundle(
        request_payload=cancel_request,
        core3_projection=_core3_projection(action_projection={"action_class": "CANCEL_ORDER", "requires_target_order_linkage": True}),
    )

    assert bundle.boundary.transmission_authorization_status == STATUS_BLOCKED
    assert RC_CANCEL_TARGET_UNRESOLVED in bundle.boundary.to_dict()["blocker_codes"]


def test_authorized_payload_is_immutable_after_authorization() -> None:
    bundle = _bundle()

    assert bundle.authorized_payload is not None
    with pytest.raises(FrozenInstanceError):
        bundle.authorized_payload.payload_id = "mutated"  # type: ignore[misc]


def test_boundary_provenance_records_refs_checks_and_payload_shape() -> None:
    bundle = _bundle()
    provenance = bundle.provenance.to_dict()

    assert provenance["request_ref"]["request_id"] == "req-close-001"
    assert provenance["snapshot_binding_ref"]["binding_id"] == bundle.binding.binding_id
    assert provenance["checks_run"]
    assert provenance["authorized_payload_ref"]["payload_id"] == bundle.authorized_payload.payload_id


def test_core4_does_not_recreate_core2_or_core3_truth_inside_boundary() -> None:
    bundle = _bundle(request_payload=_request_payload(requested_quantity="11"))
    boundary = bundle.boundary.to_dict()
    fact_bundle = boundary["constitutional_shadow"]["fact_bundle"]

    assert "current_working_orders" not in json.dumps(boundary, sort_keys=True)
    assert "incorporated_fills" not in json.dumps(boundary, sort_keys=True)
    assert "action_projection" not in json.dumps(boundary, sort_keys=True)
    assert "fact_records" not in fact_bundle
    assert fact_bundle["fact_record_ids"]
    assert fact_bundle["fact_bundle_hash"]
    assert bundle.authorized_payload is None


def test_operator_surface_is_derived_only() -> None:
    authorized = _bundle().boundary.to_dict()
    blocked = _bundle(request_payload=_request_payload(requested_quantity="11")).boundary.to_dict()
    surface = derive_post_entry_operator_surface_v1([authorized, blocked])

    assert surface["boundary_summary_dossier"]["authorized_count"] == 1
    assert surface["boundary_summary_dossier"]["blocked_count"] == 1
    assert len(surface["authorized_transmit_index"]) == 1
    assert len(surface["blocked_transmit_index"]) == 1
