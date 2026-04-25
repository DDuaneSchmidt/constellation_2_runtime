from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from constellation_2.common.action_decision_provenance_v1 import (
    ACTION_DECISION_PROVENANCE_SCHEMA_RELPATH,
    build_action_decision_provenance_v1,
    compare_prior_authority_v1,
)
from constellation_2.common.candidate_action_generator_v1 import (
    ACTION_ADD_INITIAL_PROTECTION,
    ACTION_AMEND_PROTECTION,
    ACTION_BLOCK_ALL_ACTIONS,
    ACTION_CANCEL_ORPHAN_CHILD,
    ACTION_CLOSE_POSITION,
    ACTION_CODES,
    ACTION_HOLD,
    ACTION_OPERATOR_REVIEW_REQUIRED,
    ACTION_REDUCE_POSITION,
    CANDIDATE_ACTION_SET_SCHEMA_RELPATH,
    POST_ENTRY_ACTION_POLICY_CONTRACT_ID,
    POST_ENTRY_ACTION_POLICY_CONTRACT_RELPATH,
    POST_ENTRY_ACTION_POLICY_CONTRACT_VERSION,
    generate_candidate_action_set_v1,
)
from constellation_2.common.global_actionability_gate_v1 import (
    ACTIONABILITY_GATE_SCHEMA_RELPATH,
    GATE_CONTRACT_ID,
    GATE_CONTRACT_RELPATH,
    GATE_CONTRACT_VERSION,
    GATE_FIELDS_READ,
    GATE_ACTIONABLE,
    GATE_BLOCKED,
    GATE_DEGRADED,
    evaluate_global_actionability_gate_v1,
)
from constellation_2.common.lifecycle_action_operator_surface_v1 import (
    LIFECYCLE_ACTION_OPERATOR_SURFACE_SCHEMA_RELPATH,
    build_lifecycle_action_operator_surface_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    read_validated_surface_v1,
    sha256_file_v1,
)


LIFECYCLE_ACTION_AUTHORITY_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/lifecycle_action_authority.v1.schema.json"
)
LIFECYCLE_ACTION_AUTHORITY_CONTRACT_RELPATH = (
    "governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md"
)
LIFECYCLE_ACTION_AUTHORITY_CONTRACT_ID = "C2_LIFECYCLE_ACTION_AUTHORITY_CONTRACT_V1"
LIFECYCLE_ACTION_AUTHORITY_CONTRACT_VERSION = 1
ACTION_CONFLICT_CONTRACT_RELPATH = (
    "governance/05_CONTRACTS/C2/action_conflict_resolution_v1.contract.md"
)
ACTION_CONFLICT_CONTRACT_ID = "C2_ACTION_CONFLICT_RESOLUTION_CONTRACT_V1"
ACTION_CONFLICT_CONTRACT_VERSION = 1

TRADE_IDENTITY_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/trade_identity.v1.schema.json"
INCORPORATED_STATE_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/incorporated_broker_trade_state.v1.schema.json"
)
RECONCILED_DESCRIPTION_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciled_trade_description.v1.schema.json"
)
RECONCILIATION_HEALTH_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_health.v1.schema.json"
)
RECONCILIATION_PROVENANCE_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_provenance.v1.schema.json"
)


@dataclass(frozen=True)
class Core2TradeBundleV1:
    trade_dir: Path
    trade_identity: SurfaceRefV1
    incorporated_state: SurfaceRefV1
    reconciled_description: SurfaceRefV1
    reconciliation_health: SurfaceRefV1
    reconciliation_provenance: SurfaceRefV1


def _artifact_ref(path: Path) -> Dict[str, str]:
    return {
        "artifact_path": str(path.resolve()),
        "artifact_sha256": sha256_file_v1(path.resolve()) if path.exists() and path.is_file() else "",
    }


def _repo_artifact_ref(relpath: str) -> Dict[str, str]:
    repo_root = Path(__file__).resolve().parents[2]
    return _artifact_ref((repo_root / relpath).resolve())


def _surface_artifact_ref(ref: SurfaceRefV1) -> Dict[str, str]:
    return {"artifact_path": str(ref.path), "artifact_sha256": str(ref.sha256)}


def _stable_unique(values: Sequence[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        item = str(value or "").strip()
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _trade_identity_ref(bundle: Core2TradeBundleV1) -> Dict[str, str]:
    return {
        "trade_identity_id": str(bundle.trade_identity.payload.get("trade_identity_id") or ""),
        "artifact_path": str(bundle.trade_identity.path),
        "artifact_sha256": str(bundle.trade_identity.sha256),
    }


def _upstream_core2_refs(bundle: Core2TradeBundleV1) -> Dict[str, Dict[str, str]]:
    return {
        "trade_identity_ref": _surface_artifact_ref(bundle.trade_identity),
        "incorporated_state_ref": _surface_artifact_ref(bundle.incorporated_state),
        "reconciled_description_ref": _surface_artifact_ref(bundle.reconciled_description),
        "reconciliation_health_ref": _surface_artifact_ref(bundle.reconciliation_health),
        "reconciliation_provenance_ref": _surface_artifact_ref(bundle.reconciliation_provenance),
    }


def resolve_core3_trade_output_dir_v1(*, execution_root: Path, day_utc: str, materialization_set_id: str, trade_identity_id: str) -> Path:
    return (
        Path(execution_root).resolve()
        / "lifecycle_action_authority_v1"
        / "materializations"
        / str(day_utc)
        / str(materialization_set_id)
        / "trades"
        / str(trade_identity_id)
    ).resolve()


def resolve_lifecycle_action_operator_surface_path_v1(*, execution_root: Path, day_utc: str, materialization_set_id: str) -> Path:
    return (
        Path(execution_root).resolve()
        / "reports"
        / "lifecycle_action_operator_surface_v1"
        / str(day_utc)
        / str(materialization_set_id)
        / "lifecycle_action_operator_surface.v1.json"
    ).resolve()


def load_core2_trade_bundle_v1(trade_dir: Path) -> Core2TradeBundleV1:
    root = Path(trade_dir).resolve()
    return Core2TradeBundleV1(
        trade_dir=root,
        trade_identity=read_validated_surface_v1(path=root / "trade_identity.v1.json", schema_relpath=TRADE_IDENTITY_SCHEMA_RELPATH),
        incorporated_state=read_validated_surface_v1(
            path=root / "incorporated_broker_trade_state.v1.json",
            schema_relpath=INCORPORATED_STATE_SCHEMA_RELPATH,
        ),
        reconciled_description=read_validated_surface_v1(
            path=root / "reconciled_trade_description.v1.json",
            schema_relpath=RECONCILED_DESCRIPTION_SCHEMA_RELPATH,
        ),
        reconciliation_health=read_validated_surface_v1(
            path=root / "reconciliation_health.v1.json",
            schema_relpath=RECONCILIATION_HEALTH_SCHEMA_RELPATH,
        ),
        reconciliation_provenance=read_validated_surface_v1(
            path=root / "reconciliation_provenance.v1.json",
            schema_relpath=RECONCILIATION_PROVENANCE_SCHEMA_RELPATH,
        ),
    )


def _policy_basis_refs() -> List[Dict[str, str]]:
    return [
        _repo_artifact_ref(POST_ENTRY_ACTION_POLICY_CONTRACT_RELPATH),
        _repo_artifact_ref(ACTION_CONFLICT_CONTRACT_RELPATH),
        _repo_artifact_ref(GATE_CONTRACT_RELPATH),
    ]


def _nominated_actions(candidate_payload: Mapping[str, Any]) -> List[str]:
    return [
        str(row.get("action_code") or "")
        for row in candidate_payload.get("candidate_rows") or []
        if isinstance(row, Mapping) and str(row.get("nomination_status") or "") == "NOMINATED"
    ]


def _policy_forbidden_actions(bundle: Core2TradeBundleV1) -> List[str]:
    lifecycle_status = str(bundle.reconciled_description.payload.get("lifecycle_status") or "").strip()
    protection_status = str(bundle.reconciled_description.payload.get("protection_status") or "").strip()
    side = str(bundle.incorporated_state.payload.get("side") or "").strip().upper()
    quantity = str(bundle.incorporated_state.payload.get("current_quantity") or "").strip()
    forbidden: List[str] = []
    is_open = lifecycle_status in {
        "WORKING_ENTRY",
        "OPEN_LONG",
        "OPEN_SHORT",
        "OPEN_LONG_WITH_WORKING_EXIT",
        "OPEN_SHORT_WITH_WORKING_EXIT",
    }
    if not is_open or side not in {"LONG", "SHORT"} or quantity in {"", "0", "0.0", "0.00", "0E-8"}:
        forbidden.extend([ACTION_CLOSE_POSITION, ACTION_REDUCE_POSITION, ACTION_ADD_INITIAL_PROTECTION, ACTION_AMEND_PROTECTION])
    elif protection_status != "PROTECTION_NOT_OBSERVED":
        forbidden.append(ACTION_ADD_INITIAL_PROTECTION)
    if protection_status != "PROTECTION_WORKING_PRESENT":
        forbidden.append(ACTION_AMEND_PROTECTION)
    if not any(bundle.incorporated_state.payload.get("orphan_order_facts") or []):
        forbidden.append(ACTION_CANCEL_ORPHAN_CHILD)
    return _stable_unique(forbidden)


def _resolve_final_authority(
    *,
    bundle: Core2TradeBundleV1,
    gate_payload: Mapping[str, Any],
    candidate_payload: Mapping[str, Any],
    gate_ref: Mapping[str, Any],
    candidate_ref: Mapping[str, Any],
) -> tuple[Dict[str, Any], List[Dict[str, str]], List[str], Dict[str, str], str]:
    gate_verdict = str(gate_payload.get("gate_verdict") or "").strip()
    nominated = _nominated_actions(candidate_payload)
    nominated_set = set(nominated)
    forbidden_actions = _policy_forbidden_actions(bundle)
    blocked_actions: List[str] = []
    required_actions: List[str] = []
    allowed_actions: List[str] = []
    rejected_candidates: List[Dict[str, str]] = []
    blocker_rules_fired: List[str] = list(gate_payload.get("reason_codes") or [])
    first_blocker = str(gate_payload.get("first_reason_code") or "NONE")

    if gate_verdict == GATE_BLOCKED:
        blocked_actions = [action for action in ACTION_CODES if action not in {ACTION_HOLD, ACTION_OPERATOR_REVIEW_REQUIRED}]
        conflict_rule = {
            "rule_id": "GATE_BLOCK_PRECEDENCE",
            "summary": "Blocked gate suppresses all autonomous modifying actions.",
        }
        final_posture = "BLOCKED"
        rationale = "Global Actionability Gate is blocked, so Core 3 remains fail-closed and does not permit autonomous post-entry action evaluation."
    elif gate_verdict == GATE_DEGRADED:
        allowed_actions = [ACTION_HOLD] if ACTION_HOLD in nominated_set else []
        required_actions = [ACTION_OPERATOR_REVIEW_REQUIRED]
        blocked_actions = [action for action in ACTION_CODES if action not in {ACTION_HOLD, ACTION_OPERATOR_REVIEW_REQUIRED}]
        conflict_rule = {
            "rule_id": "REVIEW_REQUIRED_PRECEDENCE",
            "summary": "Degraded gate forces hold-safe posture plus operator review.",
        }
        final_posture = "REVIEW_REQUIRED"
        rationale = "Global Actionability Gate is degraded, so Core 3 surfaces hold-safe posture and requires operator review instead of autonomous modifying action."
    else:
        if ACTION_ADD_INITIAL_PROTECTION in nominated_set:
            required_actions.append(ACTION_ADD_INITIAL_PROTECTION)
        if ACTION_CANCEL_ORPHAN_CHILD in nominated_set:
            required_actions.append(ACTION_CANCEL_ORPHAN_CHILD)
        if required_actions:
            allowed_actions = [ACTION_HOLD] if ACTION_HOLD in nominated_set else []
            for action in nominated:
                if action in set(required_actions) or action == ACTION_HOLD:
                    continue
                rejected_candidates.append(
                    {"action_code": action, "reason": "REQUIRED_ACTION_PRECEDENCE"}
                )
            conflict_rule = {
                "rule_id": "REQUIRED_SAFETY_REPAIR_PRECEDENCE",
                "summary": "Required safety-repair actions outrank discretionary actions.",
            }
            blocker_rules_fired.append("REQUIRED_ACTION_PRECEDENCE")
        else:
            if ACTION_CLOSE_POSITION in nominated_set:
                allowed_actions = [ACTION_HOLD, ACTION_CLOSE_POSITION] if ACTION_HOLD in nominated_set else [ACTION_CLOSE_POSITION]
                if ACTION_REDUCE_POSITION in nominated_set:
                    rejected_candidates.append({"action_code": ACTION_REDUCE_POSITION, "reason": "CLOSE_POSITION_PRECEDENCE"})
                    blocker_rules_fired.append("REDUCE_CLOSE_CONFLICT")
                if ACTION_AMEND_PROTECTION in nominated_set:
                    rejected_candidates.append({"action_code": ACTION_AMEND_PROTECTION, "reason": "CLOSE_POSITION_PRECEDENCE"})
                    blocker_rules_fired.append("PROTECTION_CLOSE_CONFLICT")
                if ACTION_CANCEL_ORPHAN_CHILD in nominated_set:
                    rejected_candidates.append({"action_code": ACTION_CANCEL_ORPHAN_CHILD, "reason": "ORPHAN_CLEANUP_CONFLICT_CLOSE_PRECEDENCE"})
                    blocker_rules_fired.append("ORPHAN_CLEANUP_CONFLICT")
                conflict_rule = {
                    "rule_id": "CLOSE_OVER_REDUCE_AND_AMEND",
                    "summary": "Close position outranks reduce and amend-protection candidates.",
                }
            elif ACTION_REDUCE_POSITION in nominated_set:
                allowed_actions = [ACTION_HOLD, ACTION_REDUCE_POSITION] if ACTION_HOLD in nominated_set else [ACTION_REDUCE_POSITION]
                if ACTION_AMEND_PROTECTION in nominated_set:
                    rejected_candidates.append({"action_code": ACTION_AMEND_PROTECTION, "reason": "REDUCE_POSITION_PRECEDENCE"})
                if ACTION_CANCEL_ORPHAN_CHILD in nominated_set:
                    required_actions.append(ACTION_CANCEL_ORPHAN_CHILD)
                conflict_rule = {
                    "rule_id": "REDUCE_POSITION_ALLOWED",
                    "summary": "Reduction is the highest surviving discretionary action.",
                }
            elif ACTION_AMEND_PROTECTION in nominated_set:
                allowed_actions = [ACTION_HOLD, ACTION_AMEND_PROTECTION] if ACTION_HOLD in nominated_set else [ACTION_AMEND_PROTECTION]
                if ACTION_CANCEL_ORPHAN_CHILD in nominated_set:
                    required_actions.append(ACTION_CANCEL_ORPHAN_CHILD)
                conflict_rule = {
                    "rule_id": "AMEND_PROTECTION_ALLOWED",
                    "summary": "Amend protection is the highest surviving discretionary action.",
                }
            else:
                allowed_actions = [ACTION_HOLD] if ACTION_HOLD in nominated_set else []
                if ACTION_CANCEL_ORPHAN_CHILD in nominated_set:
                    required_actions.append(ACTION_CANCEL_ORPHAN_CHILD)
                    conflict_rule = {
                        "rule_id": "ORPHAN_CLEANUP_REQUIRED",
                        "summary": "Orphan cleanup is required when no stronger discretionary action survives.",
                    }
                else:
                    conflict_rule = {
                        "rule_id": "HOLD_ONLY_FALLBACK",
                        "summary": "No modifying action survived candidate generation and policy conflict resolution.",
                    }

        blocked_actions = _stable_unique(blocked_actions + [row["action_code"] for row in rejected_candidates])
        required_actions = _stable_unique(required_actions)
        allowed_actions = _stable_unique(allowed_actions)
        if required_actions:
            surviving_allowed: List[str] = []
            for action in allowed_actions:
                if action == ACTION_HOLD:
                    surviving_allowed.append(action)
                    continue
                rejected_candidates.append({"action_code": action, "reason": "REQUIRED_ACTION_PRECEDENCE"})
                blocked_actions.append(action)
                blocker_rules_fired.append("REQUIRED_ACTION_PRECEDENCE")
            allowed_actions = _stable_unique(surviving_allowed)
            blocked_actions = _stable_unique(blocked_actions)
        forbidden_actions = [
            action for action in _stable_unique(forbidden_actions)
            if action not in required_actions and action not in allowed_actions and action not in blocked_actions
        ]
        if required_actions:
            final_posture = "ACTION_REQUIRED"
            rationale = "Governed safety-repair actions were nominated and survived conflict resolution, so Core 3 surfaces explicit required actions."
        elif any(action != ACTION_HOLD for action in allowed_actions):
            final_posture = "ACTION_ALLOWED"
            rationale = "Truth is actionable and the highest-priority discretionary action survived conflict resolution without contradiction."
        else:
            final_posture = "HOLD_ONLY"
            rationale = "Truth is actionable but only hold posture survives governed policy and conflict resolution."
        if not blocker_rules_fired:
            blocker_rules_fired = ["NONE"]

    authority_payload = {
        "schema_id": "lifecycle_action_authority",
        "schema_version": "v1",
        "authority_owner": "lifecycle_action_authority_v1",
        "canonical_owner_status": "CANONICAL_ACTION_AUTHORITY_OWNER",
        "materialization_set_id": str(bundle.trade_identity.payload.get("materialization_set_id") or ""),
        "day_utc": str(bundle.trade_identity.payload.get("day_utc") or ""),
        "evaluated_at_utc": str(bundle.trade_identity.payload.get("evaluation_utc") or ""),
        "trade_identity_ref": _trade_identity_ref(bundle),
        "environment": str(bundle.trade_identity.payload.get("environment") or ""),
        "sleeve_id": str(bundle.trade_identity.payload.get("sleeve_id") or ""),
        "account_id": str(bundle.trade_identity.payload.get("account_id") or ""),
        "ownership_classification": str(bundle.trade_identity.payload.get("ownership_classification") or ""),
        "final_action_posture": final_posture,
        "allowed_actions": allowed_actions,
        "required_actions": required_actions,
        "forbidden_actions": forbidden_actions,
        "blocked_actions": blocked_actions,
        "action_safety_posture": gate_verdict,
        "first_blocker": first_blocker,
        "ambiguity_state": str(bundle.trade_identity.payload.get("ambiguity_state") or "NONE"),
        "policy_basis_refs": _policy_basis_refs(),
        "upstream_core2_refs": _upstream_core2_refs(bundle),
        "gate_ref": dict(gate_ref),
        "candidate_set_ref": dict(candidate_ref),
        "rule_version": {
            "policy_contract_id": POST_ENTRY_ACTION_POLICY_CONTRACT_ID,
            "policy_contract_version": POST_ENTRY_ACTION_POLICY_CONTRACT_VERSION,
            "conflict_contract_id": ACTION_CONFLICT_CONTRACT_ID,
            "conflict_contract_version": ACTION_CONFLICT_CONTRACT_VERSION,
        },
        "derived_only": False,
    }
    return authority_payload, rejected_candidates, blocker_rules_fired, conflict_rule, rationale


def materialize_lifecycle_action_authority_trade_v1(
    *,
    bundle: Core2TradeBundleV1,
    execution_root: Path,
    evaluated_at_utc: str,
) -> Dict[str, Any]:
    trade_identity_id = str(bundle.trade_identity.payload.get("trade_identity_id") or "")
    materialization_set_id = str(bundle.trade_identity.payload.get("materialization_set_id") or "")
    day_utc = str(bundle.trade_identity.payload.get("day_utc") or "")
    output_dir = resolve_core3_trade_output_dir_v1(
        execution_root=execution_root,
        day_utc=day_utc,
        materialization_set_id=materialization_set_id,
        trade_identity_id=trade_identity_id,
    )

    gate_payload = evaluate_global_actionability_gate_v1(
        trade_identity=bundle.trade_identity.payload,
        reconciled_description=bundle.reconciled_description.payload,
        reconciliation_health=bundle.reconciliation_health.payload,
        upstream_core2_refs=_upstream_core2_refs(bundle),
        trade_identity_ref=_trade_identity_ref(bundle),
        materialization_set_id=materialization_set_id,
        day_utc=day_utc,
        evaluated_at_utc=evaluated_at_utc,
    )
    gate_ref = atomic_write_validated_json_v1(
        path=output_dir / "global_actionability_gate.v1.json",
        payload=gate_payload,
        schema_relpath=ACTIONABILITY_GATE_SCHEMA_RELPATH,
    )

    candidate_payload = generate_candidate_action_set_v1(
        gate_payload=gate_payload,
        incorporated_state=bundle.incorporated_state.payload,
        reconciled_description=bundle.reconciled_description.payload,
        trade_identity_ref=_trade_identity_ref(bundle),
        gate_ref={
            "artifact_path": str(gate_ref.path),
            "artifact_sha256": str(gate_ref.sha256),
            "gate_verdict": str(gate_payload.get("gate_verdict") or ""),
        },
        policy_basis_refs=_policy_basis_refs(),
        materialization_set_id=materialization_set_id,
        day_utc=day_utc,
        evaluated_at_utc=evaluated_at_utc,
    )
    candidate_ref = atomic_write_validated_json_v1(
        path=output_dir / "candidate_action_set.v1.json",
        payload=candidate_payload,
        schema_relpath=CANDIDATE_ACTION_SET_SCHEMA_RELPATH,
    )

    authority_path = output_dir / "lifecycle_action_authority.v1.json"
    prior_ref = _artifact_ref(authority_path) if authority_path.exists() else {"artifact_path": str(authority_path), "artifact_sha256": ""}
    if authority_path.exists() and authority_path.is_file():
        try:
            prior_payload = read_validated_surface_v1(
                path=authority_path,
                schema_relpath=LIFECYCLE_ACTION_AUTHORITY_SCHEMA_RELPATH,
            ).payload
        except Exception:
            prior_payload = None
    else:
        prior_payload = None

    authority_payload, rejected_candidates, blocker_rules_fired, conflict_rule, rationale = _resolve_final_authority(
        bundle=bundle,
        gate_payload=gate_payload,
        candidate_payload=candidate_payload,
        gate_ref=_surface_artifact_ref(gate_ref),
        candidate_ref=_surface_artifact_ref(candidate_ref),
    )
    authority_payload["evaluated_at_utc"] = str(evaluated_at_utc)
    authority_ref = atomic_write_validated_json_v1(
        path=authority_path,
        payload=authority_payload,
        schema_relpath=LIFECYCLE_ACTION_AUTHORITY_SCHEMA_RELPATH,
    )

    comparison = compare_prior_authority_v1(
        prior_ref=prior_ref,
        prior_payload=prior_payload,
        current_payload=authority_payload,
    )
    provenance_payload = build_action_decision_provenance_v1(
        materialization_set_id=materialization_set_id,
        day_utc=day_utc,
        evaluated_at_utc=evaluated_at_utc,
        trade_identity_id=trade_identity_id,
        core2_input_refs_used=list(_upstream_core2_refs(bundle).values()),
        core2_fields_read=sorted(
            set(GATE_FIELDS_READ)
            | {
                "reconciled_trade_description.lifecycle_status",
                "reconciled_trade_description.protection_status",
                "incorporated_broker_trade_state.current_quantity",
                "incorporated_broker_trade_state.side",
                "incorporated_broker_trade_state.current_working_orders",
                "incorporated_broker_trade_state.orphan_order_facts",
            }
        ),
        candidate_actions_generated=[
            {
                "action_code": str(row.get("action_code") or ""),
                "reason": str(row.get("note") or "NOMINATED"),
            }
            for row in candidate_payload.get("candidate_rows") or []
            if str(row.get("nomination_status") or "") == "NOMINATED"
        ],
        rejected_candidates=rejected_candidates,
        blocker_rules_fired=blocker_rules_fired,
        conflict_rule_applied=conflict_rule,
        final_posture_rationale=rationale,
        policy_versions_used={
            "gate_contract_id": GATE_CONTRACT_ID,
            "gate_contract_version": GATE_CONTRACT_VERSION,
            "policy_contract_id": POST_ENTRY_ACTION_POLICY_CONTRACT_ID,
            "policy_contract_version": POST_ENTRY_ACTION_POLICY_CONTRACT_VERSION,
            "conflict_contract_id": ACTION_CONFLICT_CONTRACT_ID,
            "conflict_contract_version": ACTION_CONFLICT_CONTRACT_VERSION,
        },
        prior_state_comparison=comparison,
    )
    provenance_ref = atomic_write_validated_json_v1(
        path=output_dir / "action_decision_provenance.v1.json",
        payload=provenance_payload,
        schema_relpath=ACTION_DECISION_PROVENANCE_SCHEMA_RELPATH,
    )
    return {
        "trade_identity_id": trade_identity_id,
        "gate": gate_ref,
        "candidates": candidate_ref,
        "authority": authority_ref,
        "provenance": provenance_ref,
        "authority_payload": authority_payload,
        "materialization_set_id": materialization_set_id,
        "day_utc": day_utc,
    }


def materialize_lifecycle_action_authority_set_v1(
    *,
    core2_materialization_dir: Path,
    execution_root: Path,
    evaluated_at_utc: str,
) -> Dict[str, Any]:
    materialization_dir = Path(core2_materialization_dir).resolve()
    trades_dir = (materialization_dir / "trades").resolve()
    if not trades_dir.exists() or not trades_dir.is_dir():
        raise ValueError(f"CORE2_TRADES_DIR_MISSING:path={trades_dir}")
    trade_dirs = sorted(path for path in trades_dir.iterdir() if path.is_dir())
    if not trade_dirs:
        raise ValueError(f"CORE2_TRADES_DIR_EMPTY:path={trades_dir}")

    authority_rows: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    for trade_dir in trade_dirs:
        bundle = load_core2_trade_bundle_v1(trade_dir)
        result = materialize_lifecycle_action_authority_trade_v1(
            bundle=bundle,
            execution_root=execution_root,
            evaluated_at_utc=evaluated_at_utc,
        )
        results.append(result)
        authority_rows.append(
            {
                "payload": result["authority_payload"],
                "ref": {
                    "artifact_path": str(result["authority"].path),
                    "artifact_sha256": str(result["authority"].sha256),
                },
            }
        )

    day_utc = str(results[0]["day_utc"])
    materialization_set_id = str(results[0]["materialization_set_id"])
    surface_payload = build_lifecycle_action_operator_surface_v1(
        materialization_set_id=materialization_set_id,
        day_utc=day_utc,
        evaluated_at_utc=evaluated_at_utc,
        authority_rows=authority_rows,
    )
    surface_ref = atomic_write_validated_json_v1(
        path=resolve_lifecycle_action_operator_surface_path_v1(
            execution_root=execution_root,
            day_utc=day_utc,
            materialization_set_id=materialization_set_id,
        ),
        payload=surface_payload,
        schema_relpath=LIFECYCLE_ACTION_OPERATOR_SURFACE_SCHEMA_RELPATH,
    )
    return {
        "day_utc": day_utc,
        "materialization_set_id": materialization_set_id,
        "trades_processed": len(results),
        "trade_results": results,
        "operator_surface": surface_ref,
        "operator_surface_payload": surface_payload,
    }
