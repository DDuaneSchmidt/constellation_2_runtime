from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1


MODULE_VERSION = "constellation_2.common.aegis_improvement_control_v1"
REPORT_SCHEMA_ID = "C2_AEGIS_IMPROVEMENT_CONTROL_REPORT_V1"
REVIEW_SCHEMA_ID = "C2_AEGIS_IMPROVEMENT_CONTROL_REVIEW_V1"

EVIDENCE_TYPES = frozenset(
    {
        "trade_outcome",
        "preflight_output",
        "paper_session_divergence",
        "decision_trace",
        "eod_advisory",
        "backtest_result",
        "config_snapshot",
        "regime_observation",
        "sleeve_performance",
    }
)

FINDING_CATEGORIES = frozenset(
    {
        "failure_analysis",
        "trade_review",
        "backtest_interpretation",
        "scenario_test",
        "trace_interpretation",
        "strategy_diagnostic",
        "configuration_review",
    }
)

AFFECTED_SCOPES = frozenset(
    {
        "sleeve",
        "regime",
        "structure_layer",
        "risk_config",
        "allocation",
        "portfolio",
        "execution",
    }
)

SEVERITIES = frozenset({"low", "medium", "high", "critical"})
CONFIDENCE_LEVELS = frozenset({"low", "medium", "high"})
FINDING_STATUSES = frozenset({"open", "proposal_created", "dismissed", "resolved"})

PROPOSAL_TYPES = frozenset(
    {
        "adjust_sleeve_allocation",
        "enable_sleeve_by_regime",
        "disable_sleeve_by_regime",
        "adjust_threshold_bounded",
        "add_temporary_filter",
        "flag_for_backtest",
        "rollback_policy",
        "no_action_monitor",
    }
)
PROPOSAL_STATUSES = frozenset({"proposed", "approved", "rejected", "test_first", "expired", "rolled_back"})
APPROVAL_DECISIONS = frozenset({"approve", "reject", "test_first"})
POLICY_STATUSES = frozenset({"draft", "active", "inactive", "superseded", "rolled_back"})
POLICY_CONCLUSIONS = frozenset({"improved", "degraded", "inconclusive"})
ROLLBACK_TRIGGERS = frozenset({"operator", "rollback_criteria", "system_review"})

_DIRECT_RUNTIME_CHANGE_KEYS = frozenset(
    {
        "runtime_config_path",
        "runtime_config_patch",
        "config_patch",
        "config_write",
        "apply_to_runtime",
        "broker_execution",
        "broker_transmit_enabled",
        "phasec_materialization",
        "sleeve_signal_logic",
    }
)

_DEFAULT_SUMMARY_FIELDS_BY_EVIDENCE_TYPE = {
    "preflight_output": ("schema_id", "day_utc", "status", "final_start_decision", "first_true_blocker_code"),
    "paper_session_divergence": ("schema_id", "day_utc", "status", "reason_code", "dependency_key"),
    "decision_trace": ("schema_id", "day_utc", "status", "decision", "reason_code"),
    "eod_advisory": ("schema_id", "day_utc", "status", "advisory_status", "summary"),
    "config_snapshot": ("schema_id", "day_utc", "config_version", "status"),
    "backtest_result": ("schema_id", "day_utc", "status", "conclusion", "run_id"),
}

_TIMESTAMP_FIELDS = ("timestamp", "generated_at_utc", "produced_utc", "detected_at", "recorded_at", "created_at", "day_utc")


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_non_empty(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _require_enum(value: Any, field: str, allowed: Iterable[str]) -> str:
    text = _require_non_empty(value, field)
    allowed_set = set(allowed)
    if text not in allowed_set:
        raise ValueError(f"{field} must be one of {sorted(allowed_set)}")
    return text


def _string_list(values: Iterable[Any] | None, field: str, *, require_non_empty: bool = False) -> list[str]:
    result = sorted({str(value).strip() for value in values or [] if str(value).strip()})
    if require_non_empty and not result:
        raise ValueError(f"{field} requires at least one value")
    return result


def _stable_id(prefix: str, seed: Mapping[str, Any]) -> str:
    return f"{prefix}_{canonical_sha256_hex_v1(dict(seed))}"


def _copy_jsonish(value: Any) -> Any:
    return copy.deepcopy(value)


def _read_json_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"artifact path does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"artifact path is not a file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"artifact is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"artifact JSON root must be an object: {path}")
    return payload


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _display_path(path: Path, repo_root: Path | None) -> str:
    resolved = path.resolve()
    if repo_root is not None:
        try:
            return resolved.relative_to(repo_root.resolve()).as_posix()
        except ValueError:
            pass
    return str(path)


def _get_path_value(payload: Mapping[str, Any], field_path: str) -> Any:
    current: Any = payload
    for part in field_path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _selected_summary_fields(payload: Mapping[str, Any], field_paths: Iterable[str]) -> dict[str, Any]:
    selected: dict[str, Any] = {}
    for field_path in field_paths:
        value = _get_path_value(payload, str(field_path))
        if value is not None:
            selected[str(field_path)] = _plain_jsonish(value)
    return selected


def _summary_from_fields(evidence_type: str, selected_fields: Mapping[str, Any]) -> str:
    if not selected_fields:
        return f"{evidence_type} artifact ingested with no configured summary fields present."
    fragments = [f"{key}={json.dumps(value, sort_keys=True, separators=(',', ':'))}" for key, value in sorted(selected_fields.items())]
    return f"{evidence_type} artifact: " + "; ".join(fragments)


def _timestamp_from_payload(payload: Mapping[str, Any], fallback: str | None = None) -> str:
    for field in _TIMESTAMP_FIELDS:
        value = payload.get(field)
        if value is not None and str(value).strip():
            return str(value).strip()
    return fallback or "unknown"


def _contains_direct_runtime_change(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key).strip()
            if key_text in _DIRECT_RUNTIME_CHANGE_KEYS:
                if key_text == "apply_to_runtime" and child is False:
                    continue
                return True
            if _contains_direct_runtime_change(child):
                return True
    if isinstance(value, list):
        return any(_contains_direct_runtime_change(item) for item in value)
    return False


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _deep_freeze(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _plain_jsonish(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain_jsonish(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [_plain_jsonish(item) for item in value]
    if isinstance(value, list):
        return [_plain_jsonish(item) for item in value]
    return copy.deepcopy(value)


def _known_policy_status(policy: Mapping[str, Any]) -> str:
    status = str(policy.get("status") or "").strip()
    if status not in POLICY_STATUSES:
        raise ValueError(f"unknown policy status fails closed: {status or '<missing>'}")
    return status


def policy_state_is_runtime_eligible_v1(policy: Mapping[str, Any]) -> bool:
    """Advisory fail-closed check only. V1 does not enforce runtime behavior."""
    try:
        return _known_policy_status(policy) == "active"
    except ValueError:
        return False


def _projection(row: Mapping[str, Any], fields: Iterable[str]) -> dict[str, Any]:
    return {field: _plain_jsonish(row.get(field)) for field in fields}


def _proposal_decisions_by_id(approvals: Iterable[Mapping[str, Any]]) -> dict[str, list[str]]:
    decisions: dict[str, list[str]] = {}
    for approval in approvals:
        proposal_id = str(approval.get("proposal_id") or "").strip()
        decision = str(approval.get("decision") or "").strip()
        if proposal_id and decision in APPROVAL_DECISIONS:
            decisions.setdefault(proposal_id, [])
            if decision not in decisions[proposal_id]:
                decisions[proposal_id].append(decision)
    return decisions


def _proposal_queue_groups(proposal: Mapping[str, Any], decisions_by_id: Mapping[str, list[str]]) -> list[str]:
    proposal_id = str(proposal.get("proposal_id") or "").strip()
    decisions = list(decisions_by_id.get(proposal_id) or [])
    groups: list[str] = []
    for decision in decisions:
        if decision == "approve":
            groups.append("approved")
        elif decision == "reject":
            groups.append("rejected")
        elif decision == "test_first":
            groups.append("test_first")
    if groups:
        return groups
    status = str(proposal.get("status") or "proposed").strip()
    if status in {"approved", "rejected", "test_first"}:
        return [status]
    return ["proposed"]


def _measurements_due_for_review(policies: Iterable[Mapping[str, Any]], measurements: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    measured_policy_ids = {str(row.get("policy_id") or "") for row in measurements}
    due: list[dict[str, Any]] = []
    for policy in policies:
        policy_id = str(policy.get("policy_id") or "").strip()
        if not policy_id or policy_id in measured_policy_ids:
            continue
        status = str(policy.get("status") or "").strip()
        if status == "active":
            reason = "active_policy_has_no_measurement_record"
        elif status == "inactive":
            reason = "inactive_policy_waiting_for_measurement_plan"
        else:
            continue
        due.append(
            {
                "policy_id": policy_id,
                "policy_status": status,
                "source_proposal_id": str(policy.get("source_proposal_id") or ""),
                "reason": reason,
            }
        )
    return due


def _rollback_candidates_from_measurements(measurements: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for measurement in measurements:
        rollback_met = measurement.get("rollback_criteria_met") is True
        degraded = measurement.get("conclusion") == "degraded"
        if rollback_met or degraded:
            candidates.append(
                {
                    "policy_id": str(measurement.get("policy_id") or ""),
                    "measurement_id": str(measurement.get("measurement_id") or ""),
                    "conclusion": str(measurement.get("conclusion") or ""),
                    "rollback_criteria_met": bool(measurement.get("rollback_criteria_met")),
                    "reason": "rollback_criteria_met" if rollback_met else "degraded_measurement",
                }
            )
    return candidates


def _operator_action_required_v1(
    *,
    approval_test_queue: Mapping[str, list[dict[str, Any]]],
    finding_rows: Iterable[Mapping[str, Any]],
    inactive_policy_rows: Iterable[Mapping[str, Any]],
    active_policy_rows: Iterable[Mapping[str, Any]],
    measurements_due: Iterable[Mapping[str, Any]],
    rollback_candidates: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    finding_by_id = {str(row.get("finding_id") or ""): row for row in finding_rows}
    actions: list[dict[str, Any]] = []
    for proposal in approval_test_queue.get("test_first", []):
        finding = finding_by_id.get(str(proposal.get("finding_id") or ""), {})
        actions.append(
            {
                "action_type": "TEST",
                "title": _short_action_title("Replay proposal", proposal.get("title") or proposal.get("proposal_type")),
                "confidence": str(finding.get("confidence") or "unknown").upper(),
                "impact": "execution readiness",
                "action": "Run replay validation",
                "related_ids": {
                    "proposal_id": proposal.get("proposal_id"),
                    "finding_id": proposal.get("finding_id"),
                },
            }
        )
    inactive_by_id = {str(row.get("policy_id") or ""): row for row in inactive_policy_rows}
    for due in measurements_due:
        policy = inactive_by_id.get(str(due.get("policy_id") or ""), {})
        actions.append(
            {
                "action_type": "MEASURE",
                "title": "Policy awaiting measurement",
                "confidence": "UNKNOWN",
                "impact": str(policy.get("policy_type") or "policy measurement"),
                "action": "Review results",
                "related_ids": {
                    "policy_id": due.get("policy_id"),
                    "source_proposal_id": due.get("source_proposal_id"),
                },
            }
        )
    for candidate in rollback_candidates:
        policy = inactive_by_id.get(str(candidate.get("policy_id") or ""), {})
        actions.append(
            {
                "action_type": "REVIEW",
                "title": "Rollback candidate",
                "confidence": "UNKNOWN",
                "impact": "policy degrading performance",
                "action": "Evaluate rollback",
                "related_ids": {
                    "policy_id": candidate.get("policy_id"),
                    "measurement_id": candidate.get("measurement_id"),
                    "policy_type": policy.get("policy_type"),
                },
            }
        )
    if not list(active_policy_rows):
        actions.append(
            {
                "action_type": "SAFE_STATE",
                "title": "No runtime changes currently active",
                "confidence": "HIGH",
                "impact": "runtime behavior unchanged",
                "action": "No runtime action required",
                "related_ids": {},
            }
        )
    return actions


def _short_action_title(prefix: str, value: Any) -> str:
    text = " ".join(str(value or "").replace("_", " ").split())
    if not text:
        return prefix
    if text.lower().startswith(prefix.lower()):
        return text[:96]
    return f"{prefix}: {text}"[:96]


def make_evidence_record_v1(
    *,
    timestamp: str,
    evidence_type: str,
    source_path: str,
    source_sha256: str,
    summary: str,
    raw_payload: Mapping[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    evidence_type_value = _require_enum(evidence_type, "evidence_type", EVIDENCE_TYPES)
    source_path_value = _require_non_empty(source_path, "source_path")
    source_sha256_value = _require_non_empty(source_sha256, "source_sha256").lower()
    summary_value = _require_non_empty(summary, "summary")
    raw_payload_value = _copy_jsonish(dict(raw_payload or {}))
    evidence_id = _stable_id(
        "evidence",
        {
            "evidence_type": evidence_type_value,
            "source_path": source_path_value,
            "source_sha256": source_sha256_value,
            "summary": summary_value,
            "raw_payload": raw_payload_value,
        },
    )
    return {
        "evidence_id": evidence_id,
        "timestamp": _require_non_empty(timestamp, "timestamp"),
        "evidence_type": evidence_type_value,
        "source_path": source_path_value,
        "source_sha256": source_sha256_value,
        "summary": summary_value,
        "raw_payload": raw_payload_value,
        "created_at": created_at or _now_utc(),
    }


def ingest_local_artifact_evidence_v1(
    *,
    artifact_path: str | Path,
    evidence_type: str,
    repo_root: str | Path | None = None,
    selected_summary_fields: Iterable[str] | None = None,
    summary: str | None = None,
    timestamp: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    evidence_type_value = _require_enum(evidence_type, "evidence_type", EVIDENCE_TYPES)
    repo_root_path = None if repo_root is None else Path(repo_root)
    read_path = Path(artifact_path)
    if not read_path.is_absolute() and repo_root_path is not None:
        read_path = repo_root_path / read_path
    payload = _read_json_artifact(read_path)
    fields = tuple(selected_summary_fields or _DEFAULT_SUMMARY_FIELDS_BY_EVIDENCE_TYPE.get(evidence_type_value, ("schema_id", "day_utc", "status")))
    selected_fields = _selected_summary_fields(payload, fields)
    return make_evidence_record_v1(
        timestamp=timestamp or _timestamp_from_payload(payload, created_at),
        evidence_type=evidence_type_value,
        source_path=_display_path(read_path, repo_root_path),
        source_sha256=_sha256_file(read_path),
        summary=summary or _summary_from_fields(evidence_type_value, selected_fields),
        raw_payload=payload,
        created_at=created_at,
    )


def ingest_preflight_output_evidence_v1(
    *,
    artifact_path: str | Path,
    repo_root: str | Path | None = None,
    selected_summary_fields: Iterable[str] | None = None,
    summary: str | None = None,
    timestamp: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    return ingest_local_artifact_evidence_v1(
        artifact_path=artifact_path,
        evidence_type="preflight_output",
        repo_root=repo_root,
        selected_summary_fields=selected_summary_fields,
        summary=summary,
        timestamp=timestamp,
        created_at=created_at,
    )


def ingest_paper_session_divergence_evidence_v1(
    *,
    artifact_path: str | Path,
    repo_root: str | Path | None = None,
    selected_summary_fields: Iterable[str] | None = None,
    summary: str | None = None,
    timestamp: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    return ingest_local_artifact_evidence_v1(
        artifact_path=artifact_path,
        evidence_type="paper_session_divergence",
        repo_root=repo_root,
        selected_summary_fields=selected_summary_fields,
        summary=summary,
        timestamp=timestamp,
        created_at=created_at,
    )


def ingest_decision_trace_evidence_v1(
    *,
    artifact_path: str | Path,
    repo_root: str | Path | None = None,
    selected_summary_fields: Iterable[str] | None = None,
    summary: str | None = None,
    timestamp: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    return ingest_local_artifact_evidence_v1(
        artifact_path=artifact_path,
        evidence_type="decision_trace",
        repo_root=repo_root,
        selected_summary_fields=selected_summary_fields,
        summary=summary,
        timestamp=timestamp,
        created_at=created_at,
    )


def ingest_eod_advisory_evidence_v1(
    *,
    artifact_path: str | Path,
    repo_root: str | Path | None = None,
    selected_summary_fields: Iterable[str] | None = None,
    summary: str | None = None,
    timestamp: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    return ingest_local_artifact_evidence_v1(
        artifact_path=artifact_path,
        evidence_type="eod_advisory",
        repo_root=repo_root,
        selected_summary_fields=selected_summary_fields,
        summary=summary,
        timestamp=timestamp,
        created_at=created_at,
    )


def ingest_config_snapshot_evidence_v1(
    *,
    artifact_path: str | Path,
    repo_root: str | Path | None = None,
    selected_summary_fields: Iterable[str] | None = None,
    summary: str | None = None,
    timestamp: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    return ingest_local_artifact_evidence_v1(
        artifact_path=artifact_path,
        evidence_type="config_snapshot",
        repo_root=repo_root,
        selected_summary_fields=selected_summary_fields,
        summary=summary,
        timestamp=timestamp,
        created_at=created_at,
    )


def ingest_backtest_result_evidence_v1(
    *,
    artifact_path: str | Path,
    repo_root: str | Path | None = None,
    selected_summary_fields: Iterable[str] | None = None,
    summary: str | None = None,
    timestamp: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    return ingest_local_artifact_evidence_v1(
        artifact_path=artifact_path,
        evidence_type="backtest_result",
        repo_root=repo_root,
        selected_summary_fields=selected_summary_fields,
        summary=summary,
        timestamp=timestamp,
        created_at=created_at,
    )


def make_finding_record_v1(
    *,
    timestamp: str,
    title: str,
    category: str,
    affected_scope: str,
    affected_ids: Iterable[Any],
    severity: str,
    confidence: str,
    observation: str,
    evidence_ids: Iterable[Any],
    suggested_action_summary: str,
    status: str = "open",
    created_at: str | None = None,
) -> dict[str, Any]:
    category_value = _require_enum(category, "category", FINDING_CATEGORIES)
    affected_scope_value = _require_enum(affected_scope, "affected_scope", AFFECTED_SCOPES)
    affected_id_values = _string_list(affected_ids, "affected_ids")
    evidence_id_values = _string_list(evidence_ids, "evidence_ids", require_non_empty=True)
    observation_value = _require_non_empty(observation, "observation")
    finding_id = _stable_id(
        "finding",
        {
            "category": category_value,
            "affected_scope": affected_scope_value,
            "affected_ids": affected_id_values,
            "evidence_ids": evidence_id_values,
            "observation": observation_value,
        },
    )
    return {
        "finding_id": finding_id,
        "timestamp": _require_non_empty(timestamp, "timestamp"),
        "title": _require_non_empty(title, "title"),
        "category": category_value,
        "affected_scope": affected_scope_value,
        "affected_ids": affected_id_values,
        "severity": _require_enum(severity, "severity", SEVERITIES),
        "confidence": _require_enum(confidence, "confidence", CONFIDENCE_LEVELS),
        "observation": observation_value,
        "evidence_ids": evidence_id_values,
        "suggested_action_summary": _require_non_empty(suggested_action_summary, "suggested_action_summary"),
        "status": _require_enum(status, "status", FINDING_STATUSES),
        "created_at": created_at or _now_utc(),
    }


def make_action_proposal_v1(
    *,
    finding_id: str,
    proposal_type: str,
    title: str,
    rationale: str,
    affected_scope: str,
    affected_ids: Iterable[Any],
    current_value: Any,
    proposed_value: Any,
    bounds: Mapping[str, Any] | None,
    expected_impact: str,
    risk_notes: str,
    success_criteria: Iterable[Any] | None = None,
    rollback_criteria: Iterable[Any] | None = None,
    proposed_duration: str | None = None,
    status: str = "proposed",
    created_at: str | None = None,
    reviewed_at: str | None = None,
    reviewer_notes: str | None = None,
) -> dict[str, Any]:
    finding_id_value = _require_non_empty(finding_id, "finding_id")
    proposal_type_value = _require_enum(proposal_type, "proposal_type", PROPOSAL_TYPES)
    affected_scope_value = _require_enum(affected_scope, "affected_scope", AFFECTED_SCOPES)
    affected_id_values = _string_list(affected_ids, "affected_ids")
    proposed_value_copy = _copy_jsonish(proposed_value)
    proposal_id = _stable_id(
        "proposal",
        {
            "finding_id": finding_id_value,
            "proposal_type": proposal_type_value,
            "proposed_value": proposed_value_copy,
        },
    )
    proposal = {
        "proposal_id": proposal_id,
        "finding_id": finding_id_value,
        "proposal_type": proposal_type_value,
        "title": _require_non_empty(title, "title"),
        "rationale": _require_non_empty(rationale, "rationale"),
        "affected_scope": affected_scope_value,
        "affected_ids": affected_id_values,
        "current_value": _copy_jsonish(current_value),
        "proposed_value": proposed_value_copy,
        "bounds": _copy_jsonish(dict(bounds or {})),
        "expected_impact": _require_non_empty(expected_impact, "expected_impact"),
        "risk_notes": _require_non_empty(risk_notes, "risk_notes"),
        "success_criteria": list(success_criteria or []),
        "rollback_criteria": list(rollback_criteria or []),
        "proposed_duration": proposed_duration,
        "status": _require_enum(status, "status", PROPOSAL_STATUSES),
        "created_at": created_at or _now_utc(),
        "reviewed_at": reviewed_at,
        "reviewer_notes": reviewer_notes,
    }
    validate_action_proposal_v1(proposal)
    return proposal


def validate_action_proposal_v1(proposal: Mapping[str, Any]) -> dict[str, Any]:
    proposal_type = _require_enum(proposal.get("proposal_type"), "proposal_type", PROPOSAL_TYPES)
    _require_non_empty(proposal.get("finding_id"), "finding_id")
    _require_non_empty(proposal.get("proposal_id"), "proposal_id")
    _require_enum(proposal.get("affected_scope"), "affected_scope", AFFECTED_SCOPES)
    _require_enum(proposal.get("status"), "status", PROPOSAL_STATUSES)
    if proposal_type != "no_action_monitor":
        if not list(proposal.get("success_criteria") or []):
            raise ValueError("success_criteria is required for behavior-changing proposals")
        if not list(proposal.get("rollback_criteria") or []):
            raise ValueError("rollback_criteria is required for behavior-changing proposals")
    if _contains_direct_runtime_change(proposal.get("proposed_value")):
        raise ValueError("proposal must not directly change runtime config or execution behavior")
    return dict(proposal)


def approve_proposal_v1(
    *,
    proposal: Mapping[str, Any],
    approver: str,
    timestamp: str,
    notes: str,
    resulting_policy_id: str | None = None,
) -> dict[str, Any]:
    validate_action_proposal_v1(proposal)
    approval_seed = {
        "proposal_id": proposal["proposal_id"],
        "decision": "approve",
        "approver": _require_non_empty(approver, "approver"),
        "timestamp": _require_non_empty(timestamp, "timestamp"),
        "notes": str(notes or ""),
        "resulting_policy_id": resulting_policy_id,
    }
    return {
        "approval_id": _stable_id("approval", approval_seed),
        "proposal_id": proposal["proposal_id"],
        "decision": "approve",
        "approver": approval_seed["approver"],
        "timestamp": approval_seed["timestamp"],
        "notes": approval_seed["notes"],
        "resulting_policy_id": resulting_policy_id,
    }


def reject_proposal_v1(
    *,
    proposal: Mapping[str, Any],
    approver: str,
    timestamp: str,
    notes: str,
) -> dict[str, Any]:
    validate_action_proposal_v1(proposal)
    approval_seed = {
        "proposal_id": proposal["proposal_id"],
        "decision": "reject",
        "approver": _require_non_empty(approver, "approver"),
        "timestamp": _require_non_empty(timestamp, "timestamp"),
        "notes": str(notes or ""),
    }
    return {
        "approval_id": _stable_id("approval", approval_seed),
        "proposal_id": proposal["proposal_id"],
        "decision": "reject",
        "approver": approval_seed["approver"],
        "timestamp": approval_seed["timestamp"],
        "notes": approval_seed["notes"],
        "resulting_policy_id": None,
    }


def test_first_proposal_v1(
    *,
    proposal: Mapping[str, Any],
    approver: str,
    timestamp: str,
    notes: str,
) -> dict[str, Any]:
    validate_action_proposal_v1(proposal)
    approval_seed = {
        "proposal_id": proposal["proposal_id"],
        "decision": "test_first",
        "approver": _require_non_empty(approver, "approver"),
        "timestamp": _require_non_empty(timestamp, "timestamp"),
        "notes": str(notes or ""),
    }
    return {
        "approval_id": _stable_id("approval", approval_seed),
        "proposal_id": proposal["proposal_id"],
        "decision": "test_first",
        "approver": approval_seed["approver"],
        "timestamp": approval_seed["timestamp"],
        "notes": approval_seed["notes"],
        "resulting_policy_id": None,
    }


def create_policy_from_approved_proposal_v1(
    *,
    proposal: Mapping[str, Any],
    approval: Mapping[str, Any],
    policy_version: int | str,
    policy_payload: Mapping[str, Any],
    policy_type: str | None = None,
    status: str = "draft",
    effective_start: str | None = None,
    effective_end: str | None = None,
    created_at: str | None = None,
    activated_at: str | None = None,
    superseded_by_policy_id: str | None = None,
) -> Mapping[str, Any]:
    validate_action_proposal_v1(proposal)
    if str(approval.get("decision") or "") != "approve":
        raise ValueError("policy must reference an approved proposal")
    if str(approval.get("proposal_id") or "") != str(proposal.get("proposal_id") or ""):
        raise ValueError("approval proposal_id must match proposal")
    status_value = _require_enum(status, "status", POLICY_STATUSES)
    payload = _copy_jsonish(dict(policy_payload))
    if _contains_direct_runtime_change(payload):
        raise ValueError("policy payload must not directly change runtime config or execution behavior in V1")
    policy_id = _stable_id(
        "policy",
        {
            "proposal_id": proposal["proposal_id"],
            "policy_version": policy_version,
            "policy_payload": payload,
        },
    )
    record: dict[str, Any] = {
        "policy_id": policy_id,
        "policy_version": policy_version,
        "source_proposal_id": proposal["proposal_id"],
        "policy_type": policy_type or proposal["proposal_type"],
        "policy_payload": payload,
        "status": status_value,
        "effective_start": effective_start,
        "effective_end": effective_end,
        "created_at": created_at or _now_utc(),
        "activated_at": activated_at,
        "superseded_by_policy_id": superseded_by_policy_id,
    }
    if status_value == "active":
        return _deep_freeze(record)
    return record


def measure_policy_impact_v1(
    *,
    policy_id: str,
    measurement_window_start: str,
    measurement_window_end: str,
    before_metrics: Mapping[str, Any],
    after_metrics: Mapping[str, Any],
    success_criteria_met: bool,
    rollback_criteria_met: bool,
    conclusion: str,
    summary: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    conclusion_value = _require_enum(conclusion, "conclusion", POLICY_CONCLUSIONS)
    seed = {
        "policy_id": _require_non_empty(policy_id, "policy_id"),
        "measurement_window_start": _require_non_empty(measurement_window_start, "measurement_window_start"),
        "measurement_window_end": _require_non_empty(measurement_window_end, "measurement_window_end"),
        "before_metrics": dict(before_metrics),
        "after_metrics": dict(after_metrics),
        "success_criteria_met": bool(success_criteria_met),
        "rollback_criteria_met": bool(rollback_criteria_met),
        "conclusion": conclusion_value,
        "summary": _require_non_empty(summary, "summary"),
    }
    return {
        "measurement_id": _stable_id("measurement", seed),
        **seed,
        "created_at": created_at or _now_utc(),
    }


def make_measurement_placeholder_from_replay_or_backtest_v1(
    *,
    policy_id: str,
    measurement_window_start: str,
    measurement_window_end: str,
    artifact_path: str | Path | None = None,
    repo_root: str | Path | None = None,
    selected_summary_fields: Iterable[str] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    repo_root_path = None if repo_root is None else Path(repo_root)
    artifact_ref: dict[str, Any] = {
        "artifact_available": False,
        "source_path": None,
        "source_sha256": None,
        "selected_summary_fields": {},
    }
    if artifact_path is not None:
        read_path = Path(artifact_path)
        if not read_path.is_absolute() and repo_root_path is not None:
            read_path = repo_root_path / read_path
        if read_path.exists():
            payload = _read_json_artifact(read_path)
            fields = tuple(selected_summary_fields or _DEFAULT_SUMMARY_FIELDS_BY_EVIDENCE_TYPE["backtest_result"])
            artifact_ref = {
                "artifact_available": True,
                "source_path": _display_path(read_path, repo_root_path),
                "source_sha256": _sha256_file(read_path),
                "selected_summary_fields": _selected_summary_fields(payload, fields),
            }
    summary = (
        "Measurement placeholder created from available replay/backtest output; impact remains inconclusive until reviewed."
        if artifact_ref["artifact_available"]
        else "Measurement placeholder created without replay/backtest output; impact remains inconclusive."
    )
    return measure_policy_impact_v1(
        policy_id=policy_id,
        measurement_window_start=measurement_window_start,
        measurement_window_end=measurement_window_end,
        before_metrics={
            "measurement_source": "replay_or_backtest_placeholder",
            **artifact_ref,
        },
        after_metrics={
            "measurement_status": "pending_operator_or_replay_review",
            "artifact_available": artifact_ref["artifact_available"],
        },
        success_criteria_met=False,
        rollback_criteria_met=False,
        conclusion="inconclusive",
        summary=summary,
        created_at=created_at,
    )


def create_rollback_record_v1(
    *,
    policy_id: str,
    trigger_reason: str,
    triggered_by: str,
    timestamp: str,
    restored_policy_id: str | None,
    notes: str,
) -> dict[str, Any]:
    seed = {
        "policy_id": _require_non_empty(policy_id, "policy_id"),
        "trigger_reason": _require_non_empty(trigger_reason, "trigger_reason"),
        "triggered_by": _require_enum(triggered_by, "triggered_by", ROLLBACK_TRIGGERS),
        "timestamp": _require_non_empty(timestamp, "timestamp"),
        "restored_policy_id": restored_policy_id,
        "notes": str(notes or ""),
    }
    return {"rollback_id": _stable_id("rollback", seed), **seed}


def build_improvement_control_report_v1(
    *,
    findings: Iterable[Mapping[str, Any]] | None = None,
    proposals: Iterable[Mapping[str, Any]] | None = None,
    approvals: Iterable[Mapping[str, Any]] | None = None,
    policies: Iterable[Mapping[str, Any]] | None = None,
    measurements: Iterable[Mapping[str, Any]] | None = None,
    rollbacks: Iterable[Mapping[str, Any]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    finding_rows = [_plain_jsonish(row) for row in findings or []]
    proposal_rows = [_plain_jsonish(row) for row in proposals or []]
    approval_rows = [_plain_jsonish(row) for row in approvals or []]
    policy_rows = [_plain_jsonish(row) for row in policies or []]
    measurement_rows = [_plain_jsonish(row) for row in measurements or []]
    rollback_rows = [_plain_jsonish(row) for row in rollbacks or []]

    findings_by_category = {category: 0 for category in sorted(FINDING_CATEGORIES)}
    for row in finding_rows:
        category = str(row.get("category") or "")
        if category in findings_by_category:
            findings_by_category[category] += 1

    proposals_by_status = {status: 0 for status in sorted(PROPOSAL_STATUSES)}
    for row in proposal_rows:
        status = str(row.get("status") or "")
        if status in proposals_by_status:
            proposals_by_status[status] += 1

    approved_proposal_ids = {str(row.get("proposal_id")) for row in approval_rows if row.get("decision") == "approve"}
    approved_policies = [row for row in policy_rows if str(row.get("source_proposal_id")) in approved_proposal_ids]

    active_policies: list[dict[str, Any]] = []
    fail_closed_policy_ids: list[str] = []
    for row in policy_rows:
        try:
            if _known_policy_status(row) == "active":
                active_policies.append(row)
        except ValueError:
            fail_closed_policy_ids.append(str(row.get("policy_id") or "<missing>"))

    measured_policy_ids = {str(row.get("policy_id")) for row in measurement_rows}
    measurements_due = [
        {
            "policy_id": row["policy_id"],
            "reason": "active_policy_has_no_measurement_record",
        }
        for row in active_policies
        if str(row.get("policy_id")) not in measured_policy_ids
    ]
    rollback_candidates = [
        {
            "policy_id": row["policy_id"],
            "measurement_id": row["measurement_id"],
            "reason": "rollback_criteria_met" if row.get("rollback_criteria_met") else "degraded_measurement",
        }
        for row in measurement_rows
        if row.get("rollback_criteria_met") is True or row.get("conclusion") == "degraded"
    ]

    return {
        "schema_id": REPORT_SCHEMA_ID,
        "schema_version": 1,
        "generated_at": generated_at or _now_utc(),
        "module_version": MODULE_VERSION,
        "open_findings": [row for row in finding_rows if row.get("status") == "open"],
        "findings_by_category": findings_by_category,
        "proposals_by_status": proposals_by_status,
        "approved_policies": approved_policies,
        "active_policies": active_policies,
        "measurements_due": measurements_due,
        "rollback_candidates": rollback_candidates,
        "rollback_records": rollback_rows,
        "unknown_policy_state_fail_closed": bool(fail_closed_policy_ids),
        "fail_closed_policy_ids": fail_closed_policy_ids,
        "advisory_only": True,
        "controls_runtime_behavior": False,
        "controls_broker_execution": False,
        "controls_phasec_materialization": False,
    }


def build_improvement_control_review_v1(
    *,
    day_utc: str,
    evidence: Iterable[Mapping[str, Any]] | None = None,
    findings: Iterable[Mapping[str, Any]] | None = None,
    proposals: Iterable[Mapping[str, Any]] | None = None,
    approvals: Iterable[Mapping[str, Any]] | None = None,
    policies: Iterable[Mapping[str, Any]] | None = None,
    measurements: Iterable[Mapping[str, Any]] | None = None,
    rollbacks: Iterable[Mapping[str, Any]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    evidence_rows = [_plain_jsonish(row) for row in evidence or []]
    finding_rows = [_plain_jsonish(row) for row in findings or []]
    proposal_rows = [_plain_jsonish(row) for row in proposals or []]
    approval_rows = [_plain_jsonish(row) for row in approvals or []]
    policy_rows = [_plain_jsonish(row) for row in policies or []]
    measurement_rows = [_plain_jsonish(row) for row in measurements or []]
    rollback_rows = [_plain_jsonish(row) for row in rollbacks or []]

    proposals_by_status = {status: 0 for status in sorted(PROPOSAL_STATUSES)}
    proposal_status_groups = {status: [] for status in sorted(PROPOSAL_STATUSES)}
    for proposal in proposal_rows:
        status = str(proposal.get("status") or "")
        if status in proposals_by_status:
            proposals_by_status[status] += 1
            proposal_status_groups[status].append(_proposal_review_row_v1(proposal))

    decisions_by_id = _proposal_decisions_by_id(approval_rows)
    approval_test_queue = {"proposed": [], "test_first": [], "rejected": [], "approved": []}
    for proposal in proposal_rows:
        proposal_row = _proposal_review_row_v1(proposal)
        for group in _proposal_queue_groups(proposal, decisions_by_id):
            approval_test_queue[group].append(proposal_row)

    inactive_policy_rows = [_policy_review_row_v1(policy) for policy in policy_rows if str(policy.get("status") or "") == "inactive"]
    active_policy_rows = [_policy_review_row_v1(policy) for policy in policy_rows if str(policy.get("status") or "") == "active"]
    measurements_due = _measurements_due_for_review(policy_rows, measurement_rows)
    rollback_candidates = _rollback_candidates_from_measurements(measurement_rows)
    operator_action_required = _operator_action_required_v1(
        approval_test_queue=approval_test_queue,
        finding_rows=finding_rows,
        inactive_policy_rows=inactive_policy_rows,
        active_policy_rows=active_policy_rows,
        measurements_due=measurements_due,
        rollback_candidates=rollback_candidates,
    )

    return {
        "schema_id": REVIEW_SCHEMA_ID,
        "schema_version": 1,
        "day_utc": _require_non_empty(day_utc, "day_utc"),
        "generated_at": generated_at or _now_utc(),
        "module_version": MODULE_VERSION,
        "operator_action_required": operator_action_required,
        "summary": {
            "open_findings_count": sum(1 for row in finding_rows if row.get("status") == "open"),
            "proposals_by_status": proposals_by_status,
            "test_first_items_count": len(approval_test_queue["test_first"]),
            "inactive_policies_count": len(inactive_policy_rows),
            "active_policies_count": len(active_policy_rows),
            "measurements_due_count": len(measurements_due),
            "rollback_candidates_count": len(rollback_candidates),
            "operator_action_required_count": len(operator_action_required),
            "advisory_only": True,
            "controls_runtime_behavior": False,
            "controls_broker_execution": False,
            "controls_phasec_materialization": False,
        },
        "findings": [_finding_review_row_v1(row) for row in finding_rows],
        "evidence": [_evidence_review_row_v1(row) for row in evidence_rows],
        "proposals": [_proposal_review_row_v1(row) for row in proposal_rows],
        "proposals_by_status": proposal_status_groups,
        "approval_test_queue": approval_test_queue,
        "approvals": [_approval_review_row_v1(row) for row in approval_rows],
        "policies": [_policy_review_row_v1(row) for row in policy_rows],
        "inactive_policies": inactive_policy_rows,
        "active_policies": active_policy_rows,
        "measurements": [_measurement_review_row_v1(row) for row in measurement_rows],
        "measurements_due": measurements_due,
        "rollback_candidates": rollback_candidates,
        "rollback_records": rollback_rows,
        "advisory_only": True,
        "controls_runtime_behavior": False,
        "controls_broker_execution": False,
        "controls_phasec_materialization": False,
    }


def _finding_review_row_v1(row: Mapping[str, Any]) -> dict[str, Any]:
    return _projection(
        row,
        (
            "finding_id",
            "category",
            "severity",
            "confidence",
            "affected_scope",
            "affected_ids",
            "observation",
            "evidence_ids",
            "status",
        ),
    )


def _evidence_review_row_v1(row: Mapping[str, Any]) -> dict[str, Any]:
    return _projection(row, ("evidence_id", "evidence_type", "source_path", "source_sha256", "summary"))


def _proposal_review_row_v1(row: Mapping[str, Any]) -> dict[str, Any]:
    return _projection(
        row,
        (
            "proposal_id",
            "finding_id",
            "proposal_type",
            "title",
            "rationale",
            "current_value",
            "proposed_value",
            "expected_impact",
            "risk_notes",
            "success_criteria",
            "rollback_criteria",
            "status",
        ),
    )


def _approval_review_row_v1(row: Mapping[str, Any]) -> dict[str, Any]:
    return _projection(row, ("approval_id", "proposal_id", "decision", "approver", "timestamp", "notes", "resulting_policy_id"))


def _policy_review_row_v1(row: Mapping[str, Any]) -> dict[str, Any]:
    return _projection(
        row,
        (
            "policy_id",
            "policy_version",
            "source_proposal_id",
            "policy_type",
            "status",
            "effective_start",
            "effective_end",
        ),
    )


def _measurement_review_row_v1(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "policy_id": _plain_jsonish(row.get("policy_id")),
        "measurement_id": _plain_jsonish(row.get("measurement_id")),
        "measurement_window_start": _plain_jsonish(row.get("measurement_window_start")),
        "measurement_window_end": _plain_jsonish(row.get("measurement_window_end")),
        "success_criteria_met": _plain_jsonish(row.get("success_criteria_met")),
        "rollback_criteria_met": _plain_jsonish(row.get("rollback_criteria_met")),
        "conclusion": _plain_jsonish(row.get("conclusion")),
        "summary": _plain_jsonish(row.get("summary")),
    }


def render_improvement_control_review_markdown_v1(review: Mapping[str, Any]) -> str:
    summary = review.get("summary") if isinstance(review.get("summary"), Mapping) else {}
    lines = [
        "# Aegis Improvement Control Review V1",
        "",
        f"Day: {review.get('day_utc')}",
        f"Generated: {review.get('generated_at')}",
        "",
        "## Summary",
        f"- Open findings: {summary.get('open_findings_count')}",
        f"- Test-first items: {summary.get('test_first_items_count')}",
        f"- Inactive policies: {summary.get('inactive_policies_count')}",
        f"- Active policies: {summary.get('active_policies_count')}",
        f"- Measurements due: {summary.get('measurements_due_count')}",
        f"- Rollback candidates: {summary.get('rollback_candidates_count')}",
        f"- Operator actions required: {summary.get('operator_action_required_count')}",
        f"- Advisory only: {str(summary.get('advisory_only')).lower()}",
        f"- Controls runtime behavior: {str(summary.get('controls_runtime_behavior')).lower()}",
        f"- Controls broker execution: {str(summary.get('controls_broker_execution')).lower()}",
        f"- Controls Phase C materialization: {str(summary.get('controls_phasec_materialization')).lower()}",
    ]
    _append_operator_action_required_markdown(lines, review.get("operator_action_required") or [])
    _append_markdown_table(lines, "Findings", review.get("findings") or [], ("finding_id", "category", "severity", "confidence", "affected_scope", "status"))
    _append_markdown_table(lines, "Evidence", review.get("evidence") or [], ("evidence_id", "evidence_type", "source_path", "source_sha256"))
    _append_markdown_table(lines, "Proposals", review.get("proposals") or [], ("proposal_id", "proposal_type", "finding_id", "status"))
    _append_markdown_table(lines, "Policies", review.get("policies") or [], ("policy_id", "policy_version", "policy_type", "status"))
    _append_markdown_table(lines, "Measurements", review.get("measurements") or [], ("measurement_id", "policy_id", "conclusion", "rollback_criteria_met"))
    _append_markdown_table(lines, "Rollback Candidates", review.get("rollback_candidates") or [], ("policy_id", "measurement_id", "reason", "conclusion"))
    return "\n".join(lines).rstrip() + "\n"


def _append_operator_action_required_markdown(lines: list[str], actions: Iterable[Mapping[str, Any]]) -> None:
    action_rows = [dict(row) for row in actions]
    lines.extend(["", "## Operator Action Required"])
    if not action_rows:
        lines.append("_None._")
        return
    for idx, action in enumerate(action_rows, start=1):
        action_type = str(action.get("action_type") or "REVIEW")
        title = str(action.get("title") or "Review item")
        if action_type == "SAFE_STATE":
            lines.append(f"{idx}. [{action_type}]")
            lines.append("   No runtime changes currently active")
            continue
        lines.append(f"{idx}. [{action_type}] {title}")
        lines.append(f"   - Confidence: {str(action.get('confidence') or 'UNKNOWN').upper()}")
        lines.append(f"   - Impact: {action.get('impact') or 'operator review'}")
        lines.append(f"   -> Action: {action.get('action') or 'Review'}")


def _append_markdown_table(lines: list[str], title: str, rows: Iterable[Mapping[str, Any]], fields: tuple[str, ...]) -> None:
    row_list = [dict(row) for row in rows]
    lines.extend(["", f"## {title}"])
    if not row_list:
        lines.append("_None._")
        return
    lines.append("| " + " | ".join(fields) + " |")
    lines.append("| " + " | ".join("---" for _ in fields) + " |")
    for row in row_list:
        values = [_markdown_cell(row.get(field)) for field in fields]
        lines.append("| " + " | ".join(values) + " |")


def _markdown_cell(value: Any) -> str:
    if isinstance(value, (dict, list)):
        text = json.dumps(value, sort_keys=True, separators=(",", ":"))
    else:
        text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")[:240]


def write_improvement_control_review_artifacts_v1(
    *,
    truth_root: str | Path,
    review: Mapping[str, Any],
    write_markdown: bool = True,
) -> dict[str, str | None]:
    day_utc = _require_non_empty(review.get("day_utc"), "day_utc")
    output_dir = Path(truth_root) / "reports" / "aegis_improvement_control_review_v1" / day_utc
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "aegis_improvement_control_review.v1.json"
    json_path.write_text(json.dumps(_plain_jsonish(review), sort_keys=True, indent=2) + "\n", encoding="utf-8")
    markdown_path: Path | None = None
    if write_markdown:
        markdown_path = output_dir / "aegis_improvement_control_review.v1.md"
        markdown_path.write_text(render_improvement_control_review_markdown_v1(review), encoding="utf-8")
    return {
        "json_path": str(json_path),
        "markdown_path": str(markdown_path) if markdown_path is not None else None,
    }
