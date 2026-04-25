from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import ops.tools.run_submission_lifecycle_refresh_v1 as submission_lifecycle_refresh_tool
from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.execution_kernel.execution_kernel_runner_v1 import run_execution_kernel_v1
from constellation_2.common.execution_kernel.execution_lifecycle_runner_v1 import run_execution_lifecycle_v1
from constellation_2.common.execution_kernel.execution_submission_record_v1 import ExecutionSubmissionRecordV1
from constellation_2.common.runtime_control_kernel.runtime_control_runner_v1 import run_runtime_control_kernel_v1
from constellation_2.phaseL.ui_api.common import (
    ADVISORY_RUNTIME_ROOT,
    GLOBAL_TRUTH_ROOT,
    SLEEVE_TRUTH_ROOT,
    evidence_ref,
    utc_now_iso,
)


QUERY_CONTRACT_VERSION = "ui_kernel_workspace_query_contract_v1"
COMMAND_CONTRACT_VERSION = "ui_kernel_workspace_command_contract_v1"
RENDERING_CONTRACT_VERSION = "ui_authority_rendering_contract_v1"
KNOWN_WORKSPACE_IDS = ("control", "state", "advisory", "submission", "lifecycle")
SHELL_STATUS_ORDER = {"blocked": 0, "missing": 1, "stale": 2, "in_progress": 3, "ready": 4, "terminal": 5}


def _now_utc() -> str:
    return utc_now_iso()


def _safe_str(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _parse_iso(value: Any) -> datetime | None:
    raw = _safe_str(value)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _ts_sort_key(*values: Any) -> tuple[Any, ...]:
    parsed = [item for item in (_parse_iso(value) for value in values) if item is not None]
    if not parsed:
        return (datetime.min.replace(tzinfo=timezone.utc),)
    return tuple(parsed)


def _status(code: str, *, label: str | None = None, semantic: str | None = None, reason_codes: list[str] | tuple[str, ...] = ()) -> dict[str, Any]:
    semantics = {
        "ready": "healthy",
        "blocked": "blocked",
        "missing": "unknown",
        "stale": "warning",
        "in_progress": "degraded",
        "terminal": "warning",
    }
    return {
        "code": code,
        "label": label or code.replace("_", " ").title(),
        "semantic": semantic or semantics.get(code, "unknown"),
        "reason_codes": list(reason_codes),
    }


def _field(label: str, value: Any, *, detail: str | None = None) -> dict[str, Any]:
    return {"label": label, "value": "" if value is None else str(value), "detail": detail}


def _artifact_card(
    *,
    artifact_type: str,
    artifact_id: str,
    artifact_path: str | None,
    effective_time: str | None,
    status: str,
    semantic: str,
    fields: list[dict[str, Any]],
    authority: bool = True,
    terminal: bool = False,
    superseded: bool = False,
    reason_codes: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "render_contract_version": RENDERING_CONTRACT_VERSION,
        "surface_kind": "authority_artifact" if authority else "decision_gate",
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "artifact_path": artifact_path,
        "effective_time": effective_time,
        "status": status,
        "semantic": semantic,
        "terminal": terminal,
        "superseded": superseded,
        "reason_codes": list(reason_codes),
        "fields": fields,
    }


def _decision_card(
    *,
    artifact_type: str,
    artifact_id: str,
    artifact_path: str | None,
    effective_time: str | None,
    outcome: str,
    reason_codes: list[str] | tuple[str, ...] = (),
    fields: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    semantic = "healthy" if outcome in {"valid", "submit", "advance", "allow"} else ("warning" if outcome in {"duplicate", "no_action"} else "blocked")
    return {
        "render_contract_version": RENDERING_CONTRACT_VERSION,
        "surface_kind": "decision_gate",
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "artifact_path": artifact_path,
        "effective_time": effective_time,
        "status": outcome,
        "semantic": semantic,
        "reason_codes": list(reason_codes),
        "fields": fields or [],
    }


def _run_envelope_panel(
    *,
    envelope_type: str,
    envelope_id: str,
    envelope_path: str | None,
    produced_utc: str | None,
    outcome: str,
    reason_codes: list[str] | tuple[str, ...],
    artifact_refs: dict[str, Any],
) -> dict[str, Any]:
    return {
        "render_contract_version": RENDERING_CONTRACT_VERSION,
        "surface_kind": "run_envelope",
        "artifact_type": envelope_type,
        "artifact_id": envelope_id,
        "artifact_path": envelope_path,
        "effective_time": produced_utc,
        "status": outcome,
        "semantic": "healthy" if outcome in {"success", "allow", "promote", "submit", "advance"} else ("warning" if outcome in {"duplicate", "no_action"} else "blocked"),
        "reason_codes": list(reason_codes),
        "artifact_refs": dict(artifact_refs),
    }


def _derived_summary_card(*, title: str, fields: list[dict[str, Any]], status: str = "derived") -> dict[str, Any]:
    return {
        "render_contract_version": RENDERING_CONTRACT_VERSION,
        "surface_kind": "derived_summary",
        "title": title,
        "status": status,
        "semantic": "derived",
        "fields": fields,
    }


def _lineage_node(*, label: str, artifact_type: str, artifact_id: str | None, artifact_path: str | None, status: str | None = None) -> dict[str, Any]:
    return {
        "label": label,
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "artifact_path": artifact_path,
        "status": status or "unknown",
    }


def _command_contract(
    *,
    command_id: str,
    label: str,
    endpoint: str,
    artifact_binding: dict[str, Any] | None,
    supported: bool,
    reason_codes: list[str] | tuple[str, ...] = (),
    inputs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "command_contract_version": COMMAND_CONTRACT_VERSION,
        "command_id": command_id,
        "label": label,
        "endpoint": endpoint,
        "method": "POST",
        "supported": supported,
        "reason_codes": list(reason_codes),
        "artifact_binding": artifact_binding,
        "inputs": inputs or [],
    }


def _workspace_payload(
    *,
    workspace_id: str,
    title: str,
    description: str,
    status: dict[str, Any],
    authority_artifacts: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    run_envelopes: list[dict[str, Any]],
    lineage_nodes: list[dict[str, Any]],
    command: dict[str, Any] | None,
    derived_summaries: list[dict[str, Any]] | None = None,
    supersession_banner: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "ok": True,
        "query_contract_version": QUERY_CONTRACT_VERSION,
        "workspace_id": workspace_id,
        "title": title,
        "description": description,
        "as_of_utc": _now_utc(),
        "status": status,
        "authority_artifacts": authority_artifacts,
        "decisions": decisions,
        "run_envelopes": run_envelopes,
        "lineage_chain": {"nodes": lineage_nodes},
        "command": command,
        "derived_summaries": derived_summaries or [],
        "supersession_banner": supersession_banner or {"show": False, "message": "", "block_actions": False},
    }


def _command_response(
    *,
    command_id: str,
    outcome: str,
    reason_codes: list[str] | tuple[str, ...],
    envelope_ref: dict[str, Any] | None = None,
    authority_artifact_ref: dict[str, Any] | None = None,
    artifact_refs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "ok": True,
        "command_contract_version": COMMAND_CONTRACT_VERSION,
        "command_id": command_id,
        "outcome": outcome,
        "reason_codes": list(reason_codes),
        "envelope_ref": envelope_ref,
        "authority_artifact_ref": authority_artifact_ref,
        "artifact_refs": artifact_refs or {},
    }


def _artifact_ref_from_result(*, artifact_type: str, artifact_id: str | None, path: str | None) -> dict[str, Any] | None:
    if not artifact_id and not path:
        return None
    return {"artifact_type": artifact_type, "artifact_id": artifact_id, "path": path}


def build_control_workspace_view() -> dict[str, Any]:
    from constellation_2.phaseL.ui_api.kernel_operator_shell_control_plane_v1 import (
        load_runtime_control_workspace_bundle_v1,
    )

    control_bundle = load_runtime_control_workspace_bundle_v1(truth_root=GLOBAL_TRUTH_ROOT)
    record_path = control_bundle["record_path"]
    record = control_bundle["record"]
    decision_path = control_bundle["decision_path"]
    decision = control_bundle["decision"]
    envelope_path = control_bundle["envelope_path"]
    envelope = control_bundle["envelope"]
    scope = control_bundle["scope"]

    authority_artifacts: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    run_envelopes: list[dict[str, Any]] = []
    derived_summaries: list[dict[str, Any]] = []
    lineage_nodes: list[dict[str, Any]] = []

    if record_path is not None and record is not None:
        control_state = _safe_str(record.get("control_state")) or "UNKNOWN"
        authority_artifacts.append(
            _artifact_card(
                artifact_type="RuntimeControlRecord",
                artifact_id=_safe_str(record.get("runtime_control_record_id")),
                artifact_path=str(record_path),
                effective_time=_safe_str(record.get("effective_at_utc")),
                status=control_state,
                semantic="healthy" if control_state == "ALLOW" else "blocked",
                fields=[
                    _field("Kill Switch", record.get("kill_switch_state")),
                    _field("Allow Entries", record.get("allow_entries")),
                    _field("Readiness", record.get("readiness_state")),
                    _field("Capability Scope", record.get("capability_scope")),
                ],
                reason_codes=list(record.get("reason_codes") or []),
            )
        )
        status = _status("ready" if control_state == "ALLOW" else "blocked", reason_codes=list(record.get("reason_codes") or []))
        derived_summaries.append(
            _derived_summary_card(
                title="Controlled Scope",
                fields=[
                    _field("Environment", record.get("environment")),
                    _field("IB Account", record.get("ib_account")),
                    _field("Sleeve", record.get("sleeve_id")),
                ],
            )
        )
    elif decision_path is not None and decision is not None:
        status = _status("blocked", reason_codes=list(decision.get("reason_codes") or []))
    else:
        status = _status("missing", reason_codes=["RUNTIME_CONTROL_RECORD_MISSING"])

    if decision_path is not None and decision is not None:
        decisions.append(
            _decision_card(
                artifact_type="RuntimeControlDecision",
                artifact_id=_safe_str(decision.get("runtime_control_decision_id")),
                artifact_path=str(decision_path),
                effective_time=_safe_str(decision.get("effective_at_utc") or decision.get("produced_utc")),
                outcome=_safe_str(decision.get("outcome")) or "blocked",
                reason_codes=list(decision.get("reason_codes") or []),
                fields=[
                    _field("Candidate Control State", decision.get("candidate_control_state")),
                    _field("Capability Scope", decision.get("capability_scope")),
                ],
            )
        )
    if envelope_path is not None and envelope is not None:
        run_envelopes.append(
            _run_envelope_panel(
                envelope_type="RuntimeControlRunEnvelope",
                envelope_id=_safe_str(envelope.get("run_id") or envelope.get("runtime_control_run_id")),
                envelope_path=str(envelope_path),
                produced_utc=_safe_str(envelope.get("produced_utc")),
                outcome=_safe_str(envelope.get("run_outcome")) or "blocked",
                reason_codes=list(envelope.get("reason_codes") or []),
                artifact_refs=dict(envelope.get("artifact_refs") or {}),
            )
        )

    lineage_nodes.extend(
        [
            _lineage_node(
                label="Kill Switch Authority",
                artifact_type="kill_switch_authority",
                artifact_id=None,
                artifact_path=None if not record else next((str(ref.get("path")) for ref in record.get("source_artifact_refs", []) if "kill" in str(ref.get("path", "")).lower()), None),
                status="current",
            ),
            _lineage_node(
                label="Trade Submit Readiness",
                artifact_type="trade_submit_readiness",
                artifact_id=None,
                artifact_path=None if not record else next((str(ref.get("path")) for ref in record.get("source_artifact_refs", []) if "readiness" in str(ref.get("path", "")).lower()), None),
                status="current",
            ),
            _lineage_node(
                label="Runtime Control Record",
                artifact_type="RuntimeControlRecord",
                artifact_id=None if not record else _safe_str(record.get("runtime_control_record_id")),
                artifact_path=None if record_path is None else str(record_path),
                status=status["code"],
            ),
        ]
    )

    command = _command_contract(
        command_id="evaluate_runtime_control",
        label="Evaluate Runtime Control",
        endpoint="/api/commands/control/evaluate",
        artifact_binding=None if scope is None else {
            "capability_scope": scope["capability_scope"],
            "expected_runtime_control_record_id": scope.get("expected_runtime_control_record_id") or None,
        },
        supported=scope is not None,
        reason_codes=[] if scope is not None else ["RUNTIME_CONTROL_SCOPE_NOT_DISCOVERABLE"],
        inputs=[] if scope is None else [
            {"name": "day_utc", "label": "Day", "type": "hidden", "value": scope["day_utc"], "required": True},
            {"name": "environment", "label": "Environment", "type": "hidden", "value": scope["environment"], "required": True},
            {"name": "ib_account", "label": "IB Account", "type": "hidden", "value": scope["ib_account"], "required": True},
            {"name": "sleeve_id", "label": "Sleeve", "type": "hidden", "value": scope["sleeve_id"], "required": True},
            {"name": "capability_scope", "label": "Capability Scope", "type": "hidden", "value": scope["capability_scope"], "required": True},
            {"name": "expected_runtime_control_record_id", "label": "Expected Record", "type": "hidden", "value": scope.get("expected_runtime_control_record_id") or "", "required": False},
        ],
    )

    return _workspace_payload(
        workspace_id="control",
        title="Runtime Control",
        description="Current runtime/operator control truth and the one authoritative allow/block decision path.",
        status=status,
        authority_artifacts=authority_artifacts,
        decisions=decisions,
        run_envelopes=run_envelopes,
        lineage_nodes=lineage_nodes,
        command=command,
        derived_summaries=derived_summaries,
    )


def build_state_workspace_view() -> dict[str, Any]:
    from constellation_2.phaseL.ui_api.kernel_operator_shell_diagnostic_v1 import (
        load_current_household_id_v1,
        load_state_workspace_snapshot_bundle_v1,
    )

    household_id = load_current_household_id_v1(advisory_runtime_root=ADVISORY_RUNTIME_ROOT)
    authority_artifacts: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    run_envelopes: list[dict[str, Any]] = []
    derived_summaries: list[dict[str, Any]] = []
    lineage_nodes: list[dict[str, Any]] = []

    if household_id is None:
        return _workspace_payload(
            workspace_id="state",
            title="Snapshot State",
            description="Current HouseholdSnapshot authority and the latest snapshot run evidence.",
            status=_status("missing", reason_codes=["HOUSEHOLD_SNAPSHOT_MISSING"]),
            authority_artifacts=[],
            decisions=[],
            run_envelopes=[],
            lineage_nodes=[],
            command=_command_contract(
                command_id="run_snapshot_kernel",
                label="Run Snapshot Kernel",
                endpoint="/api/commands/state/run-snapshot",
                artifact_binding=None,
                supported=False,
                reason_codes=["SNAPSHOT_INPUT_ASSEMBLY_NOT_SAFE_FROM_UI_SURFACE"],
            ),
        )

    state_bundle = load_state_workspace_snapshot_bundle_v1(
        advisory_runtime_root=ADVISORY_RUNTIME_ROOT,
        household_id=household_id,
    )
    snapshot_path = state_bundle.get("snapshot_path")
    snapshot_doc = state_bundle.get("snapshot_doc")
    decision_path = state_bundle.get("decision_path")
    decision_doc = state_bundle.get("decision_doc")
    envelope_path = state_bundle.get("envelope_path")
    envelope_doc = state_bundle.get("envelope_doc")

    if snapshot_path is not None and snapshot_doc is not None:
        freshness = _safe_str(snapshot_doc.get("freshness_status")) or "UNKNOWN"
        validation = _safe_str(snapshot_doc.get("validation_status")) or "UNKNOWN"
        status_code = "ready"
        if freshness not in {"CURRENT", "HEALTHY"}:
            status_code = "stale"
        if validation != "VALID":
            status_code = "blocked"
        status = _status(status_code, reason_codes=list(snapshot_doc.get("reason_codes") or []))
        authority_artifacts.append(
            _artifact_card(
                artifact_type="HouseholdSnapshot",
                artifact_id=_safe_str(snapshot_doc.get("household_snapshot_id")),
                artifact_path=str(snapshot_path),
                effective_time=_safe_str(snapshot_doc.get("effective_at")),
                status=f"{validation}/{freshness}",
                semantic=status["semantic"],
                fields=[
                    _field("Household", snapshot_doc.get("household_id")),
                    _field("Completeness", snapshot_doc.get("completeness_status")),
                    _field("Freshness", snapshot_doc.get("freshness_status")),
                    _field("Reconciliation", snapshot_doc.get("reconciliation_status")),
                ],
                reason_codes=list(snapshot_doc.get("reason_codes") or []),
            )
        )
        investable = snapshot_doc.get("investable_asset_summary") if isinstance(snapshot_doc.get("investable_asset_summary"), dict) else {}
        cash = snapshot_doc.get("cash_liquidity_summary") if isinstance(snapshot_doc.get("cash_liquidity_summary"), dict) else {}
        derived_summaries.append(
            _derived_summary_card(
                title="Frozen Current State",
                fields=[
                    _field("Accounts In Scope", (snapshot_doc.get("coverage_scope") or {}).get("accounts_in_scope_count")),
                    _field("Positions", investable.get("position_count")),
                    _field("Cash", cash.get("total_cash")),
                ],
            )
        )
    elif decision_doc is not None:
        status = _status("blocked" if _safe_str(decision_doc.get("outcome")) == "blocked" else "missing", reason_codes=list(decision_doc.get("reason_codes") or []))
    else:
        status = _status("missing", reason_codes=["HOUSEHOLD_SNAPSHOT_MISSING"])

    if decision_doc is not None and decision_path is not None:
        decisions.append(
            _decision_card(
                artifact_type="SnapshotValidationDecision",
                artifact_id=_safe_str(decision_doc.get("snapshot_validation_decision_id")),
                artifact_path=str(decision_path),
                effective_time=_safe_str(decision_doc.get("effective_at")),
                outcome=_safe_str(decision_doc.get("outcome")),
                reason_codes=list(decision_doc.get("reason_codes") or []),
                fields=[
                    _field("Completeness", decision_doc.get("completeness_status")),
                    _field("Freshness", decision_doc.get("freshness_status")),
                    _field("Reconciliation", decision_doc.get("reconciliation_status")),
                ],
            )
        )
    if envelope_doc is not None and envelope_path is not None:
        run_envelopes.append(
            _run_envelope_panel(
                envelope_type="SnapshotRunEnvelope",
                envelope_id=_safe_str(envelope_doc.get("snapshot_run_id")),
                envelope_path=str(envelope_path),
                produced_utc=_safe_str(envelope_doc.get("produced_utc")),
                outcome=_safe_str(envelope_doc.get("run_outcome")),
                reason_codes=list(envelope_doc.get("reason_codes") or []),
                artifact_refs=dict(envelope_doc.get("artifact_refs") or {}),
            )
        )

    lineage_nodes.extend(
        [
            _lineage_node(label="Policy", artifact_type="Policy", artifact_id=None if snapshot_doc is None else _safe_str(snapshot_doc.get("parent_policy_id")), artifact_path=None, status="current"),
            _lineage_node(label="HouseholdSnapshot", artifact_type="HouseholdSnapshot", artifact_id=None if snapshot_doc is None else _safe_str(snapshot_doc.get("household_snapshot_id")), artifact_path=None if snapshot_path is None else str(snapshot_path), status=status["code"]),
        ]
    )

    command = _command_contract(
        command_id="run_snapshot_kernel",
        label="Run Snapshot Kernel",
        endpoint="/api/commands/state/run-snapshot",
        artifact_binding=None if snapshot_doc is None else {"household_snapshot_id": _safe_str(snapshot_doc.get("household_snapshot_id"))},
        supported=False,
        reason_codes=["SNAPSHOT_INPUT_ASSEMBLY_NOT_SAFE_FROM_UI_SURFACE"],
    )

    return _workspace_payload(
        workspace_id="state",
        title="Snapshot State",
        description="Current HouseholdSnapshot authority, its validation gate, and the latest snapshot run evidence.",
        status=status,
        authority_artifacts=authority_artifacts,
        decisions=decisions,
        run_envelopes=run_envelopes,
        lineage_nodes=lineage_nodes,
        command=command,
        derived_summaries=derived_summaries,
    )


def build_advisory_workspace_view() -> dict[str, Any]:
    from constellation_2.phaseL.ui_api.kernel_operator_shell_diagnostic_v1 import (
        load_current_household_id_v1,
        load_advisory_workspace_bundle_v1,
    )

    household_id = load_current_household_id_v1(advisory_runtime_root=ADVISORY_RUNTIME_ROOT)
    authority_artifacts: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    run_envelopes: list[dict[str, Any]] = []
    derived_summaries: list[dict[str, Any]] = []
    lineage_nodes: list[dict[str, Any]] = []

    if household_id is None:
        return _workspace_payload(
            workspace_id="advisory",
            title="Advisory Chain",
            description="InvestorIntent through PromotionRecordV2, rendered from authority artifacts only.",
            status=_status("missing", reason_codes=["ADVISORY_CHAIN_MISSING"]),
            authority_artifacts=[],
            decisions=[],
            run_envelopes=[],
            lineage_nodes=[],
            command=_command_contract(
                command_id="run_advisory_kernel",
                label="Run Advisory Kernel",
                endpoint="/api/commands/advisory/run",
                artifact_binding=None,
                supported=False,
                reason_codes=["ADVISORY_RUN_REQUIRES_EXPLICIT_SNAPSHOT_AND_EXECUTION_PROFILE"],
            ),
        )

    advisory_bundle = load_advisory_workspace_bundle_v1(
        advisory_runtime_root=ADVISORY_RUNTIME_ROOT,
        household_id=household_id,
    )
    envelope_path = advisory_bundle.get("envelope_path")
    envelope_doc = advisory_bundle.get("envelope_doc")
    investor_intent = advisory_bundle.get("investor_intent")
    policy = advisory_bundle.get("policy")
    snapshot = advisory_bundle.get("snapshot")
    portfolio_intent = advisory_bundle.get("portfolio_intent")
    promotion_decision = advisory_bundle.get("promotion_decision")
    promotion_record = advisory_bundle.get("promotion_record")
    execution_intent = advisory_bundle.get("execution_intent")

    if investor_intent is not None:
        authority_artifacts.append(
            _artifact_card(
                artifact_type="InvestorIntent",
                artifact_id=investor_intent.intent_id,
                artifact_path=None,
                effective_time=investor_intent.effective_at,
                status=investor_intent.completeness_status,
                semantic="healthy",
                fields=[
                    _field("Intent Version", investor_intent.intent_version),
                    _field("Completeness", investor_intent.completeness_status),
                ],
            )
        )
    if policy is not None:
        authority_artifacts.append(
            _artifact_card(
                artifact_type="Policy",
                artifact_id=policy.policy_id,
                artifact_path=None,
                effective_time=policy.effective_at,
                status=policy.validity_status,
                semantic="healthy" if policy.validity_status == "VALID" else "blocked",
                fields=[
                    _field("Risk Profile", policy.risk_profile),
                    _field("Approval Requirements", ",".join(policy.approval_requirements)),
                ],
            )
        )
    if snapshot is not None:
        authority_artifacts.append(
            _artifact_card(
                artifact_type="HouseholdSnapshot",
                artifact_id=snapshot.household_snapshot_id,
                artifact_path=None,
                effective_time=snapshot.effective_at,
                status=snapshot.validation_status,
                semantic="healthy" if snapshot.validation_status == "VALID" else "blocked",
                fields=[
                    _field("Freshness", snapshot.freshness_status),
                    _field("Completeness", snapshot.completeness_status),
                ],
            )
        )
    if portfolio_intent is not None:
        authority_artifacts.append(
            _artifact_card(
                artifact_type="PortfolioIntent",
                artifact_id=portfolio_intent.portfolio_intent_id,
                artifact_path=None,
                effective_time=portfolio_intent.effective_at,
                status=portfolio_intent.execution_eligibility,
                semantic="healthy" if portfolio_intent.execution_eligibility == "ELIGIBLE" else "warning",
                fields=[
                    _field("Action Needed", portfolio_intent.action_needed),
                    _field("Validity Tier", portfolio_intent.validity_tier),
                ],
                authority=False,
            )
        )
    if promotion_decision is not None:
        decisions.append(
            _decision_card(
                artifact_type="PromotionDecision",
                artifact_id=promotion_decision.promotion_decision_id,
                artifact_path=None,
                effective_time=promotion_decision.effective_at,
                outcome=promotion_decision.outcome,
                reason_codes=list(promotion_decision.reason_codes),
                fields=[
                    _field("Approved Changes", len(promotion_decision.approved_change_ids)),
                    _field("Blocked Changes", len(promotion_decision.blocked_change_ids)),
                ],
            )
        )
    if promotion_record is not None:
        authority_artifacts.append(
            _artifact_card(
                artifact_type="PromotionRecordV2",
                artifact_id=promotion_record.promotion_record_id,
                artifact_path=None,
                effective_time=promotion_record.timestamp_utc,
                status=promotion_record.status,
                semantic="healthy" if promotion_record.status == "AUTHORIZED" else ("warning" if promotion_record.status == "NO_ACTION" else "blocked"),
                fields=[
                    _field("Validity", promotion_record.validity_status),
                    _field("Approved Delta", len(promotion_record.approved_delta)),
                ],
                reason_codes=list(promotion_record.reason_codes),
            )
        )
    if execution_intent is not None:
        authority_artifacts.append(
            _artifact_card(
                artifact_type="ExecutionIntent",
                artifact_id=execution_intent.execution_intent_id,
                artifact_path=None,
                effective_time=execution_intent.effective_at_utc,
                status=execution_intent.side,
                semantic="healthy",
                fields=[
                    _field("Symbol", (execution_intent.instrument or {}).get("symbol")),
                    _field("Quantity", execution_intent.quantity_shares),
                    _field("Account", execution_intent.account_id),
                ],
            )
        )

    if envelope_doc is not None and envelope_path is not None:
        run_envelopes.append(
            _run_envelope_panel(
                envelope_type="AdvisoryKernelRunEnvelope",
                envelope_id=_safe_str(envelope_doc.get("kernel_run_id")),
                envelope_path=str(envelope_path),
                produced_utc=_safe_str(envelope_doc.get("produced_utc")),
                outcome=_safe_str(envelope_doc.get("run_outcome")),
                reason_codes=list(envelope_doc.get("reason_codes") or []),
                artifact_refs=dict(envelope_doc.get("artifact_refs") or {}),
            )
        )

    if promotion_record is not None:
        adv_status = "ready" if promotion_record.status == "AUTHORIZED" else ("terminal" if promotion_record.status == "NO_ACTION" else "blocked")
        reason_codes = list(promotion_record.reason_codes)
    elif envelope_doc is not None:
        adv_status = "ready" if _safe_str(envelope_doc.get("run_outcome")) in {"promote", "no_action"} else "blocked"
        reason_codes = list(envelope_doc.get("reason_codes") or [])
    else:
        adv_status = "missing"
        reason_codes = ["ADVISORY_CHAIN_MISSING"]

    lineage_nodes.extend(
        [
            _lineage_node(label="InvestorIntent", artifact_type="InvestorIntent", artifact_id=None if investor_intent is None else investor_intent.intent_id, artifact_path=None, status="current" if investor_intent else "missing"),
            _lineage_node(label="Policy", artifact_type="Policy", artifact_id=None if policy is None else policy.policy_id, artifact_path=None, status="current" if policy else "missing"),
            _lineage_node(label="HouseholdSnapshot", artifact_type="HouseholdSnapshot", artifact_id=None if snapshot is None else snapshot.household_snapshot_id, artifact_path=None, status="current" if snapshot else "missing"),
            _lineage_node(label="PortfolioIntent", artifact_type="PortfolioIntent", artifact_id=None if portfolio_intent is None else portfolio_intent.portfolio_intent_id, artifact_path=None, status="current" if portfolio_intent else "missing"),
            _lineage_node(label="PromotionDecision", artifact_type="PromotionDecision", artifact_id=None if promotion_decision is None else promotion_decision.promotion_decision_id, artifact_path=None, status="current" if promotion_decision else "missing"),
            _lineage_node(label="PromotionRecordV2", artifact_type="PromotionRecordV2", artifact_id=None if promotion_record is None else promotion_record.promotion_record_id, artifact_path=None, status=adv_status),
            _lineage_node(label="ExecutionIntent", artifact_type="ExecutionIntent", artifact_id=None if execution_intent is None else execution_intent.execution_intent_id, artifact_path=None, status="current" if execution_intent else "missing"),
        ]
    )

    if portfolio_intent is not None:
        derived_summaries.append(
            _derived_summary_card(
                title="Derived Advisory Summary",
                fields=[
                    _field("Required Directional Changes", len(portfolio_intent.required_directional_changes)),
                    _field("Blocked Conditions", len(portfolio_intent.blocked_conditions)),
                ],
            )
        )

    return _workspace_payload(
        workspace_id="advisory",
        title="Advisory Chain",
        description="InvestorIntent through PromotionRecordV2 and ExecutionIntent, rendered from the backend authority chain only.",
        status=_status(adv_status, reason_codes=reason_codes),
        authority_artifacts=authority_artifacts,
        decisions=decisions,
        run_envelopes=run_envelopes,
        lineage_nodes=lineage_nodes,
        command=_command_contract(
            command_id="run_advisory_kernel",
            label="Run Advisory Kernel",
            endpoint="/api/commands/advisory/run",
            artifact_binding=None if snapshot is None else {"household_snapshot_id": snapshot.household_snapshot_id},
            supported=False,
            reason_codes=["ADVISORY_RUN_REQUIRES_EXPLICIT_SNAPSHOT_AND_EXECUTION_PROFILE"],
        ),
        derived_summaries=derived_summaries,
    )


def build_submission_workspace_view() -> dict[str, Any]:
    from constellation_2.phaseL.ui_api.kernel_operator_shell_diagnostic_v1 import (
        load_submission_workspace_bundle_v1,
    )

    submission_bundle = load_submission_workspace_bundle_v1(
        advisory_runtime_root=ADVISORY_RUNTIME_ROOT,
        sleeve_truth_root=SLEEVE_TRUTH_ROOT,
    )
    submission_record_path = submission_bundle.get("submission_record_path")
    submission_record = submission_bundle.get("submission_record")
    authority_artifacts: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    run_envelopes: list[dict[str, Any]] = []
    lineage_nodes: list[dict[str, Any]] = []
    derived_summaries: list[dict[str, Any]] = []

    if submission_record is None or submission_record_path is None:
        return _workspace_payload(
            workspace_id="submission",
            title="Execution Submission",
            description="Authorized execution, submission gate, submission record, and the latest submission run envelope.",
            status=_status("missing", reason_codes=["SUBMISSION_RECORD_MISSING"]),
            authority_artifacts=[],
            decisions=[],
            run_envelopes=[],
            lineage_nodes=[],
            command=_command_contract(
                command_id="submit_authorized_execution",
                label="Submit Authorized Execution",
                endpoint="/api/commands/submission/submit",
                artifact_binding=None,
                supported=False,
                reason_codes=["EXECUTION_INTENT_MISSING"],
            ),
        )

    execution_intent = submission_bundle.get("execution_intent")
    promotion_record = submission_bundle.get("promotion_record")
    if submission_bundle.get("lineage_error") == "SUBMISSION_CHAIN_LINEAGE_UNREADABLE":
        return _workspace_payload(
            workspace_id="submission",
            title="Execution Submission",
            description="Authorized execution, submission gate, submission record, and the latest submission run envelope.",
            status=_status("blocked", reason_codes=["SUBMISSION_CHAIN_LINEAGE_UNREADABLE"]),
            authority_artifacts=[
                _artifact_card(
                    artifact_type="SubmissionRecord",
                    artifact_id=submission_record.submission_record_id,
                    artifact_path=str(submission_record_path),
                    effective_time=submission_record.produced_utc,
                    status=submission_record.status,
                    semantic="warning",
                    fields=[_field("Submission ID", submission_record.submission_id)],
                )
            ],
            decisions=[],
            run_envelopes=[],
            lineage_nodes=[_lineage_node(label="SubmissionRecord", artifact_type="SubmissionRecord", artifact_id=submission_record.submission_record_id, artifact_path=str(submission_record_path), status="blocked")],
            command=_command_contract(
                command_id="submit_authorized_execution",
                label="Submit Authorized Execution",
                endpoint="/api/commands/submission/submit",
                artifact_binding={"submission_record_id": submission_record.submission_record_id},
                supported=False,
                reason_codes=["SUBMISSION_CHAIN_LINEAGE_UNREADABLE"],
            ),
        )
    decision_path = submission_bundle.get("decision_path")
    decision_doc = submission_bundle.get("decision_doc")
    envelope_path = submission_bundle.get("envelope_path")
    envelope_doc = submission_bundle.get("envelope_doc")
    current_state = submission_bundle.get("current_state")

    authority_artifacts.append(
        _artifact_card(
            artifact_type="PromotionRecordV2",
            artifact_id=promotion_record.promotion_record_id,
            artifact_path=None,
            effective_time=promotion_record.timestamp_utc,
            status=promotion_record.status,
            semantic="healthy" if promotion_record.status == "AUTHORIZED" else "blocked",
            fields=[
                _field("Validity", promotion_record.validity_status),
                _field("Approved Delta", len(promotion_record.approved_delta)),
            ],
            reason_codes=list(promotion_record.reason_codes),
        )
    )
    authority_artifacts.append(
        _artifact_card(
            artifact_type="ExecutionIntent",
            artifact_id=execution_intent.execution_intent_id,
            artifact_path=None,
            effective_time=execution_intent.effective_at_utc,
            status=execution_intent.side,
            semantic="healthy",
            fields=[
                _field("Symbol", (execution_intent.instrument or {}).get("symbol")),
                _field("Quantity", execution_intent.quantity_shares),
                _field("Account", execution_intent.account_id),
            ],
        )
    )
    authority_artifacts.append(
        _artifact_card(
            artifact_type="SubmissionRecord",
            artifact_id=submission_record.submission_record_id,
            artifact_path=str(submission_record_path),
            effective_time=submission_record.produced_utc,
            status=submission_record.status,
            semantic="healthy",
            fields=[
                _field("Submission ID", submission_record.submission_id),
                _field("Package SHA", submission_record.execution_package_ref.get("sha256")),
                _field("Payload SHA", submission_record.downstream_payload_ref.get("sha256")),
            ],
        )
    )

    if decision_doc is not None and decision_path is not None:
        decisions.append(
            _decision_card(
                artifact_type="SubmissionDecision",
                artifact_id=_safe_str(decision_doc.get("submission_decision_id")),
                artifact_path=str(decision_path),
                effective_time=_safe_str(decision_doc.get("produced_utc")),
                outcome=_safe_str(decision_doc.get("outcome")),
                reason_codes=list(decision_doc.get("reason_codes") or []),
                fields=[
                    _field("Predicted Submission", decision_doc.get("predicted_submission_id")),
                    _field("Predicted Trade Instance", decision_doc.get("predicted_trade_instance_id")),
                ],
            )
        )

    if envelope_doc is not None and envelope_path is not None:
        run_envelopes.append(
            _run_envelope_panel(
                envelope_type="ExecutionRunEnvelope",
                envelope_id=_safe_str(envelope_doc.get("run_id")),
                envelope_path=str(envelope_path),
                produced_utc=_safe_str(envelope_doc.get("produced_utc")),
                outcome=_safe_str(envelope_doc.get("run_outcome")),
                reason_codes=list(envelope_doc.get("reason_codes") or []),
                artifact_refs=dict(envelope_doc.get("artifact_refs") or {}),
            )
        )

    if current_state is None:
        status_code = "ready"
    elif current_state.terminal_state:
        status_code = "terminal"
    else:
        status_code = "in_progress"

    derived_summaries.append(
        _derived_summary_card(
            title="Derived Submission Summary",
            fields=[
                _field("Execution Package", submission_record.execution_package_ref.get("path")),
                _field("Downstream Payload", submission_record.downstream_payload_ref.get("path")),
                _field("Current Lifecycle", None if current_state is None else current_state.lifecycle_status),
            ],
        )
    )

    lineage_nodes.extend(
        [
            _lineage_node(label="PromotionRecordV2", artifact_type="PromotionRecordV2", artifact_id=promotion_record.promotion_record_id, artifact_path=None, status="current"),
            _lineage_node(label="ExecutionIntent", artifact_type="ExecutionIntent", artifact_id=execution_intent.execution_intent_id, artifact_path=None, status="current"),
            _lineage_node(label="SubmissionDecision", artifact_type="SubmissionDecision", artifact_id=None if decision_doc is None else _safe_str(decision_doc.get("submission_decision_id")), artifact_path=None if decision_path is None else str(decision_path), status="current" if decision_doc else "missing"),
            _lineage_node(label="SubmissionRecord", artifact_type="SubmissionRecord", artifact_id=submission_record.submission_record_id, artifact_path=str(submission_record_path), status=status_code),
        ]
    )

    return _workspace_payload(
        workspace_id="submission",
        title="Execution Submission",
        description="Authorized execution, submission gate, submission record, and the latest submission run evidence.",
        status=_status(status_code),
        authority_artifacts=authority_artifacts,
        decisions=decisions,
        run_envelopes=run_envelopes,
        lineage_nodes=lineage_nodes,
        command=_command_contract(
            command_id="submit_authorized_execution",
            label="Submit Authorized Execution",
            endpoint="/api/commands/submission/submit",
            artifact_binding={
                "execution_intent_id": execution_intent.execution_intent_id,
                "submission_record_id": submission_record.submission_record_id,
            },
            supported=True,
            inputs=[
                {"name": "execution_intent_id", "label": "Execution Intent", "type": "hidden", "value": execution_intent.execution_intent_id, "required": True},
                {"name": "eval_time_utc", "label": "Eval Time UTC", "type": "text", "value": _now_utc(), "required": True},
                {"name": "risk_budget_path", "label": "Risk Budget Path", "type": "text", "value": "", "required": True},
                {"name": "ib_host", "label": "IB Host", "type": "text", "value": "127.0.0.1", "required": True},
                {"name": "ib_port", "label": "IB Port", "type": "text", "value": "7497", "required": True},
                {"name": "ib_client_id", "label": "IB Client ID", "type": "text", "value": "1", "required": True},
                {"name": "dry_run", "label": "Dry Run", "type": "checkbox", "value": True, "required": False},
            ],
        ),
        derived_summaries=derived_summaries,
    )


def build_lifecycle_workspace_view() -> dict[str, Any]:
    from constellation_2.phaseL.ui_api.kernel_operator_shell_diagnostic_v1 import (
        load_lifecycle_workspace_bundle_v1,
    )

    lifecycle_bundle = load_lifecycle_workspace_bundle_v1(
        sleeve_truth_root=SLEEVE_TRUTH_ROOT,
    )
    record_path = lifecycle_bundle.get("record_path")
    record_doc = lifecycle_bundle.get("record_doc")
    authority_artifacts: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    run_envelopes: list[dict[str, Any]] = []
    lineage_nodes: list[dict[str, Any]] = []
    derived_summaries: list[dict[str, Any]] = []

    if record_path is None or record_doc is None:
        return _workspace_payload(
            workspace_id="lifecycle",
            title="Execution Lifecycle",
            description="Long-lived execution truth from authoritative downstream evidence only.",
            status=_status("missing", reason_codes=["EXECUTION_STATE_RECORD_MISSING"]),
            authority_artifacts=[],
            decisions=[],
            run_envelopes=[],
            lineage_nodes=[],
            command=_command_contract(
                command_id="refresh_lifecycle_truth",
                label="Refresh Lifecycle Truth",
                endpoint="/api/commands/lifecycle/refresh",
                artifact_binding=None,
                supported=False,
                reason_codes=["SUBMISSION_RECORD_MISSING"],
            ),
        )

    submission_record = lifecycle_bundle.get("submission_record")
    if lifecycle_bundle.get("lineage_error") == "SUBMISSION_RECORD_FOR_LIFECYCLE_UNREADABLE":
        submission_id = _safe_str(record_doc.get("submission_id"))
        return _workspace_payload(
            workspace_id="lifecycle",
            title="Execution Lifecycle",
            description="Long-lived execution truth from authoritative downstream evidence only.",
            status=_status("blocked", reason_codes=["SUBMISSION_RECORD_FOR_LIFECYCLE_UNREADABLE"]),
            authority_artifacts=[
                _artifact_card(
                    artifact_type="ExecutionStateRecord",
                    artifact_id=_safe_str(record_doc.get("execution_state_record_id")),
                    artifact_path=str(record_path),
                    effective_time=_safe_str(record_doc.get("produced_utc")),
                    status=_safe_str(record_doc.get("lifecycle_status")),
                    semantic="warning",
                    fields=[_field("Submission ID", record_doc.get("submission_id"))],
                )
            ],
            decisions=[],
            run_envelopes=[],
            lineage_nodes=[_lineage_node(label="ExecutionStateRecord", artifact_type="ExecutionStateRecord", artifact_id=_safe_str(record_doc.get("execution_state_record_id")), artifact_path=str(record_path), status="blocked")],
            command=_command_contract(
                command_id="refresh_lifecycle_truth",
                label="Refresh Lifecycle Truth",
                endpoint="/api/commands/lifecycle/refresh",
                artifact_binding={"submission_id": submission_id},
                supported=False,
                reason_codes=["SUBMISSION_RECORD_FOR_LIFECYCLE_UNREADABLE"],
            ),
        )
    decision_path = lifecycle_bundle.get("decision_path")
    decision_doc = lifecycle_bundle.get("decision_doc")
    envelope_path = lifecycle_bundle.get("envelope_path")
    envelope_doc = lifecycle_bundle.get("envelope_doc")

    terminal_state = bool(record_doc.get("terminal_state"))
    status_code = "terminal" if terminal_state else "in_progress"
    authority_artifacts.append(
        _artifact_card(
            artifact_type="ExecutionStateRecord",
            artifact_id=_safe_str(record_doc.get("execution_state_record_id")),
            artifact_path=str(record_path),
            effective_time=_safe_str(record_doc.get("produced_utc")),
            status=_safe_str(record_doc.get("lifecycle_status")),
            semantic="warning" if terminal_state else "healthy",
            fields=[
                _field("Transition Index", record_doc.get("transition_index")),
                _field("Evidence Basis", record_doc.get("evidence_basis")),
                _field("Filled Qty", record_doc.get("filled_qty")),
                _field("Remaining Qty", record_doc.get("remaining_qty")),
            ],
            terminal=terminal_state,
            reason_codes=list(record_doc.get("reason_codes") or []),
        )
    )

    if decision_doc is not None and decision_path is not None:
        decisions.append(
            _decision_card(
                artifact_type="ExecutionLifecycleDecision",
                artifact_id=_safe_str(decision_doc.get("execution_lifecycle_decision_id")),
                artifact_path=str(decision_path),
                effective_time=_safe_str(decision_doc.get("produced_utc")),
                outcome=_safe_str(decision_doc.get("outcome")),
                reason_codes=list(decision_doc.get("reason_codes") or []),
                fields=[
                    _field("Candidate Lifecycle", decision_doc.get("candidate_lifecycle_status")),
                    _field("Evidence Basis", decision_doc.get("evidence_basis")),
                ],
            )
        )

    if envelope_doc is not None and envelope_path is not None:
        run_envelopes.append(
            _run_envelope_panel(
                envelope_type="ExecutionLifecycleRunEnvelope",
                envelope_id=_safe_str(envelope_doc.get("run_id")),
                envelope_path=str(envelope_path),
                produced_utc=_safe_str(envelope_doc.get("produced_utc")),
                outcome=_safe_str(envelope_doc.get("run_outcome")),
                reason_codes=list(envelope_doc.get("reason_codes") or []),
                artifact_refs=dict(envelope_doc.get("artifact_refs") or {}),
            )
        )

    derived_summaries.append(
        _derived_summary_card(
            title="Derived Lifecycle Summary",
            fields=[
                _field("Submission ID", record_doc.get("submission_id")),
                _field("Broker Order ID", record_doc.get("broker_order_id")),
                _field("Perm ID", record_doc.get("perm_id")),
            ],
        )
    )

    lineage_nodes.extend(
        [
            _lineage_node(label="SubmissionRecord", artifact_type="SubmissionRecord", artifact_id=submission_record.submission_record_id, artifact_path=None, status="current"),
            _lineage_node(label="ExecutionStateRecord", artifact_type="ExecutionStateRecord", artifact_id=_safe_str(record_doc.get("execution_state_record_id")), artifact_path=str(record_path), status=status_code),
        ]
    )

    return _workspace_payload(
        workspace_id="lifecycle",
        title="Execution Lifecycle",
        description="Long-lived execution truth from authoritative downstream evidence only.",
        status=_status(status_code, reason_codes=list(record_doc.get("reason_codes") or [])),
        authority_artifacts=authority_artifacts,
        decisions=decisions,
        run_envelopes=run_envelopes,
        lineage_nodes=lineage_nodes,
        command=_command_contract(
            command_id="refresh_lifecycle_truth",
            label="Refresh Lifecycle Truth",
            endpoint="/api/commands/lifecycle/refresh",
            artifact_binding={"submission_record_id": submission_record.submission_record_id},
            supported=True,
            inputs=[
                {"name": "submission_record_id", "label": "Submission Record", "type": "hidden", "value": submission_record.submission_record_id, "required": True},
            ],
        ),
        derived_summaries=derived_summaries,
    )


def build_kernel_status_rail_view() -> dict[str, Any]:
    workspaces = [
        build_control_workspace_view(),
        build_state_workspace_view(),
        build_advisory_workspace_view(),
        build_submission_workspace_view(),
        build_lifecycle_workspace_view(),
    ]
    labels = {
        "control": "Control",
        "state": "State",
        "advisory": "Advisory",
        "submission": "Submission",
        "lifecycle": "Lifecycle",
    }
    return {
        "ok": True,
        "query_contract_version": QUERY_CONTRACT_VERSION,
        "generated_utc": _now_utc(),
        "kernels": [
            {
                "kernel_id": view["workspace_id"],
                "label": labels[view["workspace_id"]],
                "status": dict(view["status"]),
                "href": "/" if view["workspace_id"] == "root" else f'/{view["workspace_id"]}',
            }
            for view in workspaces
        ],
    }


def build_operator_work_queue_view() -> dict[str, Any]:
    rail = build_kernel_status_rail_view()
    items = []
    for item in rail["kernels"]:
        code = _safe_str(item["status"].get("code"))
        if code == "ready":
            continue
        items.append(
            {
                "kernel_id": item["kernel_id"],
                "label": item["label"],
                "status": dict(item["status"]),
                "href": item["href"],
                "next_action_label": f'Open {item["label"]}',
            }
        )
    items.sort(key=lambda item: (SHELL_STATUS_ORDER.get(item["status"]["code"], 99), item["label"]))
    return {
        "ok": True,
        "query_contract_version": QUERY_CONTRACT_VERSION,
        "generated_utc": _now_utc(),
        "title": "Operator Work Queue",
        "description": "Authoritative kernel status only: what is blocked, missing, stale, or awaiting the next valid operator action.",
        "status_items": rail["kernels"],
        "work_items": items,
    }


def run_runtime_control_command(body: dict[str, Any]) -> dict[str, Any]:
    from constellation_2.phaseL.ui_api.kernel_operator_shell_diagnostic_v1 import (
        discover_runtime_scope_v1,
    )

    scope = discover_runtime_scope_v1(
        truth_root=GLOBAL_TRUTH_ROOT,
        sleeve_truth_root=SLEEVE_TRUTH_ROOT,
    )
    if scope is None:
        return _command_response(
            command_id="evaluate_runtime_control",
            outcome="blocked",
            reason_codes=["RUNTIME_CONTROL_SCOPE_NOT_DISCOVERABLE"],
        )
    expected_record_id = _safe_str(body.get("expected_runtime_control_record_id"))
    current_scope = discover_runtime_scope_v1(
        truth_root=GLOBAL_TRUTH_ROOT,
        sleeve_truth_root=SLEEVE_TRUTH_ROOT,
    ) or {}
    current_record_id = _safe_str(current_scope.get("expected_runtime_control_record_id"))
    if expected_record_id and current_record_id and expected_record_id != current_record_id:
        return _command_response(
            command_id="evaluate_runtime_control",
            outcome="blocked",
            reason_codes=["SUPERSEDED_RUNTIME_CONTROL_RECORD"],
        )

    day_utc = _safe_str(body.get("day_utc")) or current_scope["day_utc"]
    environment = _safe_str(body.get("environment")) or current_scope["environment"]
    ib_account = _safe_str(body.get("ib_account")) or current_scope["ib_account"]
    sleeve_id = _safe_str(body.get("sleeve_id")) or current_scope["sleeve_id"]
    capability_scope = _safe_str(body.get("capability_scope")) or current_scope["capability_scope"]

    result = run_runtime_control_kernel_v1(
        canonical_truth_root=GLOBAL_TRUTH_ROOT,
        execution_truth_root=SLEEVE_TRUTH_ROOT,
        day_utc=day_utc,
        produced_utc=_now_utc(),
        run_id=f"ui-control-{uuid4().hex}",
        environment=environment,
        ib_account=ib_account,
        sleeve_id=sleeve_id,
        capability_scope=capability_scope,
    )
    record = result["runtime_control_record"]
    envelope = result["runtime_control_run_envelope"]
    return _command_response(
        command_id="evaluate_runtime_control",
        outcome=_safe_str(envelope.run_outcome),
        reason_codes=list(envelope.reason_codes),
        envelope_ref=_artifact_ref_from_result(
            artifact_type="RuntimeControlRunEnvelope",
            artifact_id=getattr(envelope, "record_id", None),
            path=result.get("runtime_control_run_envelope_path"),
        ),
        authority_artifact_ref=_artifact_ref_from_result(
            artifact_type="RuntimeControlRecord",
            artifact_id=None if record is None else record.runtime_control_record_id,
            path=result.get("runtime_control_record_path"),
        ),
        artifact_refs={
            "runtime_control_decision_path": result.get("runtime_control_decision_path"),
            "runtime_control_record_path": result.get("runtime_control_record_path"),
        },
    )


def run_snapshot_command(body: dict[str, Any]) -> dict[str, Any]:
    del body
    return _command_response(
        command_id="run_snapshot_kernel",
        outcome="blocked",
        reason_codes=["SNAPSHOT_INPUT_ASSEMBLY_NOT_SAFE_FROM_UI_SURFACE"],
    )


def run_advisory_command(body: dict[str, Any]) -> dict[str, Any]:
    del body
    return _command_response(
        command_id="run_advisory_kernel",
        outcome="blocked",
        reason_codes=["ADVISORY_RUN_REQUIRES_EXPLICIT_SNAPSHOT_AND_EXECUTION_PROFILE"],
    )


def run_advisory_promote_command(body: dict[str, Any]) -> dict[str, Any]:
    del body
    return _command_response(
        command_id="promote_advisory_decision",
        outcome="blocked",
        reason_codes=["STANDALONE_PROMOTION_COMMAND_NOT_PROVEN_IN_REPO_REALITY"],
    )


def run_submission_command(body: dict[str, Any]) -> dict[str, Any]:
    from constellation_2.phaseL.ui_api.kernel_operator_shell_diagnostic_v1 import (
        load_current_execution_intent_for_submit_v1,
    )

    execution_intent_id = _safe_str(body.get("execution_intent_id"))
    if not execution_intent_id:
        return _command_response(
            command_id="submit_authorized_execution",
            outcome="blocked",
            reason_codes=["EXECUTION_INTENT_ID_REQUIRED"],
        )

    current_intent, current_submission_record = load_current_execution_intent_for_submit_v1(
        advisory_runtime_root=ADVISORY_RUNTIME_ROOT,
        sleeve_truth_root=SLEEVE_TRUTH_ROOT,
    )
    if current_intent is None or current_submission_record is None:
        return _command_response(
            command_id="submit_authorized_execution",
            outcome="blocked",
            reason_codes=["CURRENT_EXECUTION_INTENT_NOT_FOUND"],
        )
    if execution_intent_id != current_intent.execution_intent_id:
        return _command_response(
            command_id="submit_authorized_execution",
            outcome="blocked",
            reason_codes=["SUPERSEDED_OR_NONCURRENT_EXECUTION_INTENT"],
        )

    risk_budget_path = Path(_safe_str(body.get("risk_budget_path"))).expanduser()
    if not _safe_str(body.get("risk_budget_path")) or not risk_budget_path.exists():
        return _command_response(
            command_id="submit_authorized_execution",
            outcome="blocked",
            reason_codes=["RISK_BUDGET_PATH_REQUIRED"],
        )

    ib_host = _safe_str(body.get("ib_host"))
    ib_port = _safe_str(body.get("ib_port"))
    ib_client_id = _safe_str(body.get("ib_client_id"))
    eval_time_utc = _safe_str(body.get("eval_time_utc")) or _now_utc()
    if not ib_host or not ib_port or not ib_client_id:
        return _command_response(
            command_id="submit_authorized_execution",
            outcome="blocked",
            reason_codes=["IB_CONNECTION_FIELDS_REQUIRED"],
        )

    result = run_execution_kernel_v1(
        repo_root=Path(__file__).resolve().parents[3],
        truth_root=SLEEVE_TRUTH_ROOT,
        run_id=f"ui-submit-{uuid4().hex}",
        execution_intent=current_intent,
        produced_utc=_now_utc(),
        eval_time_utc=eval_time_utc,
        risk_budget_path=risk_budget_path.resolve(),
        ib_host=ib_host,
        ib_port=int(ib_port),
        ib_client_id=int(ib_client_id),
        dry_run=bool(body.get("dry_run", True)),
    )
    envelope = result["execution_run_envelope"]
    submission_record = result.get("submission_record")
    execution_state = result.get("execution_state_record")
    authority_ref = None
    if execution_state is not None:
        authority_ref = _artifact_ref_from_result(
            artifact_type="ExecutionStateRecord",
            artifact_id=execution_state.execution_state_record_id,
            path=result.get("execution_state_record_path"),
        )
    elif submission_record is not None:
        authority_ref = _artifact_ref_from_result(
            artifact_type="SubmissionRecord",
            artifact_id=submission_record.submission_record_id,
            path=str(execution_submission_record_path_v1(truth_root=SLEEVE_TRUTH_ROOT, day_utc=submission_record.day_utc, submission_id=submission_record.submission_id)),
        )

    return _command_response(
        command_id="submit_authorized_execution",
        outcome=_safe_str(envelope.run_outcome),
        reason_codes=list(envelope.reason_codes),
        envelope_ref=_artifact_ref_from_result(
            artifact_type="ExecutionRunEnvelope",
            artifact_id=getattr(envelope, "run_id", None),
            path=result.get("execution_run_envelope_path"),
        ),
        authority_artifact_ref=authority_ref,
        artifact_refs={
            "submission_decision_path": result.get("submission_decision_path"),
            "execution_run_envelope_path": result.get("execution_run_envelope_path"),
        },
    )


def run_lifecycle_refresh_command(body: dict[str, Any]) -> dict[str, Any]:
    submission_record_id = _safe_str(body.get("submission_record_id"))
    if not submission_record_id:
        return _command_response(
            command_id="refresh_lifecycle_truth",
            outcome="blocked",
            reason_codes=["SUBMISSION_RECORD_ID_REQUIRED"],
        )

    submission_record_path, submission_record = _latest_submission_record()
    if submission_record is None or submission_record_path is None:
        return _command_response(
            command_id="refresh_lifecycle_truth",
            outcome="blocked",
            reason_codes=["SUBMISSION_RECORD_MISSING"],
        )
    if submission_record_id != submission_record.submission_record_id:
        return _command_response(
            command_id="refresh_lifecycle_truth",
            outcome="blocked",
            reason_codes=["SUPERSEDED_OR_NONCURRENT_SUBMISSION_RECORD"],
        )

    try:
        submission_lifecycle_refresh_tool.main(
            [
                "--day_utc",
                submission_record.day_utc,
                "--truth_root",
                str(SLEEVE_TRUTH_ROOT),
                "--submission_id",
                submission_record.submission_id,
            ]
        )
        result = run_execution_lifecycle_v1(
            truth_root=SLEEVE_TRUTH_ROOT,
            run_id=f"ui-lifecycle-{uuid4().hex}",
            submission_record=submission_record,
            produced_utc=_now_utc(),
        )
    except Exception as exc:
        return _command_response(
            command_id="refresh_lifecycle_truth",
            outcome="blocked",
            reason_codes=[f"LIFECYCLE_REFRESH_FAILED:{type(exc).__name__}"],
        )

    envelope = result["execution_lifecycle_run_envelope"]
    record = result.get("execution_state_record")
    return _command_response(
        command_id="refresh_lifecycle_truth",
        outcome=_safe_str(envelope.run_outcome),
        reason_codes=list(envelope.reason_codes),
        envelope_ref=_artifact_ref_from_result(
            artifact_type="ExecutionLifecycleRunEnvelope",
            artifact_id=getattr(envelope, "run_id", None),
            path=result.get("execution_lifecycle_run_envelope_path"),
        ),
        authority_artifact_ref=_artifact_ref_from_result(
            artifact_type="ExecutionStateRecord",
            artifact_id=None if record is None else record.execution_state_record_id,
            path=result.get("execution_state_record_path"),
        ),
        artifact_refs={
            "execution_lifecycle_decision_path": result.get("execution_lifecycle_decision_path"),
        },
    )


def dispatch_kernel_command(command_path: str, body: dict[str, Any]) -> dict[str, Any]:
    routes = {
        "/api/commands/control/evaluate": run_runtime_control_command,
        "/api/commands/state/run-snapshot": run_snapshot_command,
        "/api/commands/advisory/run": run_advisory_command,
        "/api/commands/advisory/promote": run_advisory_promote_command,
        "/api/commands/submission/submit": run_submission_command,
        "/api/commands/lifecycle/refresh": run_lifecycle_refresh_command,
    }
    handler = routes.get(command_path)
    if handler is None:
        return _command_response(command_id="unknown", outcome="blocked", reason_codes=["ENDPOINT_NOT_FOUND"])
    return handler(body)


def build_workspace_view(workspace_id: str) -> dict[str, Any]:
    views = {
        "control": build_control_workspace_view,
        "state": build_state_workspace_view,
        "advisory": build_advisory_workspace_view,
        "submission": build_submission_workspace_view,
        "lifecycle": build_lifecycle_workspace_view,
    }
    builder = views.get(workspace_id)
    if builder is None:
        return {
            "ok": False,
            "query_contract_version": QUERY_CONTRACT_VERSION,
            "errors": ["WORKSPACE_NOT_FOUND"],
            "workspace_id": workspace_id,
        }
    return builder()
