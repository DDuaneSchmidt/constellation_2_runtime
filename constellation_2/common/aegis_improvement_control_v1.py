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
