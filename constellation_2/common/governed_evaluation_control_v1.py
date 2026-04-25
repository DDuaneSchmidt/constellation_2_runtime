from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from constellation_2.common.constitutional_runtime_v1 import validate_governed_artifact_payload_v1
from constellation_2.common.paper_session_fact_plane_v1 import read_json_object_v1, sha256_file_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_GOVERNED_EVALUATION_POLICY_V1.json").resolve()

ALLOWED_ACTION_STATES = ("continue", "watch", "reduce", "pause", "retire", "no_conclusion")


def _parse_day(day_utc: str) -> str:
    day = str(day_utc or "").strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise ValueError(f"BAD_DAY_UTC:{day!r}")
    return day


def _read_json(path: Path) -> Dict[str, Any]:
    return read_json_object_v1(path.resolve())


def _policy_ref() -> Dict[str, str]:
    return {
        "path": str(POLICY_PATH),
        "sha256": sha256_file_v1(POLICY_PATH),
    }


def _runtime_policy() -> Dict[str, Any]:
    payload = _read_json(POLICY_PATH)
    runtime = payload.get("runtime_control_policy")
    if not isinstance(runtime, dict):
        raise ValueError("GOVERNED_EVALUATION_RUNTIME_CONTROL_POLICY_MISSING")
    allocation_policy = runtime.get("capital_authority_allocation_v1")
    if not isinstance(allocation_policy, dict):
        raise ValueError("GOVERNED_EVALUATION_CAPITAL_AUTHORITY_CONTROL_POLICY_MISSING")
    return allocation_policy


def _runtime_data_root_from_truth_root(truth_root: Path) -> Path:
    resolved = truth_root.resolve()
    parts = resolved.parts
    if "truth_sleeves" in parts:
        index = parts.index("truth_sleeves")
        if index > 0:
            return Path(*parts[:index]).resolve()
    if resolved.name == "truth":
        return resolved.parent.resolve()
    return resolved.parent.resolve()


def _canonical_truth_root_from_input(truth_root: Path) -> Path:
    runtime_root = _runtime_data_root_from_truth_root(truth_root)
    return (runtime_root / "truth").resolve()


def _execution_truth_root_from_binding(
    *,
    truth_root: Path,
    execution_sleeve_id: str,
    mode: str,
) -> Path:
    runtime_root = _runtime_data_root_from_truth_root(truth_root)
    return (
        runtime_root
        / "truth_sleeves"
        / str(execution_sleeve_id).strip().upper()
        / str(mode).strip().upper()
    ).resolve()


def resolve_adopted_governed_sleeve_bindings_v1(
    *,
    execution_sleeve_id: str = "",
    mode: str = "",
) -> list[dict[str, str]]:
    runtime_policy = _runtime_policy()
    sleeve_scope = runtime_policy.get("sleeve_scope")
    if not isinstance(sleeve_scope, dict):
        raise ValueError("SLEEVE_SCOPE_POLICY_MISSING")
    adopted_bindings_raw = sleeve_scope.get("adopted_bindings") or []
    if not isinstance(adopted_bindings_raw, list):
        raise ValueError("SLEEVE_SCOPE_ADOPTED_BINDINGS_INVALID")

    wanted_execution_sleeve_id = str(execution_sleeve_id or "").strip().upper()
    wanted_mode = str(mode or "").strip().upper()
    out: list[dict[str, str]] = []
    for row in adopted_bindings_raw:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or "").strip()
        binding_execution_sleeve_id = str(row.get("execution_sleeve_id") or "").strip().upper()
        binding_mode = str(row.get("mode") or "").strip().upper()
        if not sleeve_id or not binding_execution_sleeve_id or not binding_mode:
            raise ValueError("SLEEVE_SCOPE_ADOPTED_BINDING_FIELDS_MISSING")
        if wanted_execution_sleeve_id and binding_execution_sleeve_id != wanted_execution_sleeve_id:
            continue
        if wanted_mode and binding_mode != wanted_mode:
            continue
        out.append(
            {
                "sleeve_id": sleeve_id,
                "execution_sleeve_id": binding_execution_sleeve_id,
                "mode": binding_mode,
            }
        )
    return out


def _normalized_reason_codes(values: Sequence[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        code = str(raw or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _require_multiplier_bp(raw: Any, *, label: str) -> int:
    if not isinstance(raw, int):
        raise ValueError(f"{label}_NOT_INT")
    value = int(raw)
    if value < 0 or value > 10000:
        raise ValueError(f"{label}_OUT_OF_RANGE")
    return value


def _action_map(scope_policy: Mapping[str, Any], *, label: str) -> Dict[str, Dict[str, Any]]:
    raw_map = scope_policy.get("action_map")
    if not isinstance(raw_map, dict):
        raise ValueError(f"{label}_ACTION_MAP_MISSING")
    result: Dict[str, Dict[str, Any]] = {}
    for action_state in ALLOWED_ACTION_STATES:
        row = raw_map.get(action_state)
        if not isinstance(row, dict):
            raise ValueError(f"{label}_ACTION_MAP_ENTRY_MISSING:{action_state}")
        result[action_state] = {
            "headroom_multiplier_bp": _require_multiplier_bp(
                row.get("headroom_multiplier_bp"),
                label=f"{label}_{action_state}_HEADROOM_MULTIPLIER_BP",
            ),
            "control_state": str(row.get("control_state") or "").strip(),
            "reason_codes": _normalized_reason_codes(row.get("reason_codes") or []),
        }
        if not result[action_state]["control_state"]:
            raise ValueError(f"{label}_CONTROL_STATE_MISSING:{action_state}")
    return result


def _fallback_policy(scope_policy: Mapping[str, Any], *, label: str, key: str) -> Dict[str, Any]:
    row = scope_policy.get(key)
    if not isinstance(row, dict):
        raise ValueError(f"{label}_{key.upper()}_MISSING")
    return {
        "headroom_multiplier_bp": _require_multiplier_bp(
            row.get("headroom_multiplier_bp"),
            label=f"{label}_{key.upper()}_HEADROOM_MULTIPLIER_BP",
        ),
        "control_state": str(row.get("control_state") or "").strip(),
        "reason_codes": _normalized_reason_codes(row.get("reason_codes") or []),
    }


def _artifact_ref(artifact_id: str, path: Path) -> Dict[str, str]:
    return {
        "artifact_id": artifact_id,
        "path": str(path.resolve()),
        "sha256": sha256_file_v1(path.resolve()),
    }


def _decision_row(
    *,
    scope_kind: str,
    scope_id: str,
    adoption_state: str,
    artifact_status: str,
    action_state: str,
    mandatory_or_advisory: str,
    headroom_multiplier_bp: int,
    control_state: str,
    reason_codes: Sequence[Any],
    action_reason_codes: Sequence[Any],
    action_ref: Dict[str, Any] | None,
    evaluation_ref: Dict[str, Any] | None,
    policy_snapshot_ref: Dict[str, Any] | None,
    diagnostic: str = "",
    execution_sleeve_id: str = "",
    mode: str = "",
) -> Dict[str, Any]:
    return {
        "scope_kind": scope_kind,
        "scope_id": scope_id,
        "execution_sleeve_id": execution_sleeve_id,
        "mode": mode,
        "adoption_state": adoption_state,
        "artifact_status": artifact_status,
        "action_state": action_state,
        "mandatory_or_advisory": mandatory_or_advisory,
        "headroom_multiplier_bp": int(headroom_multiplier_bp),
        "control_state": control_state,
        "reason_codes": _normalized_reason_codes(reason_codes),
        "action_reason_codes": _normalized_reason_codes(action_reason_codes),
        "action_ref": dict(action_ref or {}),
        "evaluation_ref": dict(evaluation_ref or {}),
        "policy_snapshot_ref": dict(policy_snapshot_ref or {}),
        "diagnostic": str(diagnostic or "").strip(),
    }


def _fallback_decision(
    *,
    scope_kind: str,
    scope_id: str,
    scope_policy: Mapping[str, Any],
    label: str,
    adoption_state: str,
    artifact_status: str,
    diagnostic: str,
    execution_sleeve_id: str = "",
    mode: str = "",
) -> Dict[str, Any]:
    fallback = _fallback_policy(scope_policy, label=label, key="missing_or_invalid_action_artifact")
    return _decision_row(
        scope_kind=scope_kind,
        scope_id=scope_id,
        adoption_state=adoption_state,
        artifact_status=artifact_status,
        action_state="",
        mandatory_or_advisory="MANDATORY",
        headroom_multiplier_bp=int(fallback["headroom_multiplier_bp"]),
        control_state=str(fallback["control_state"]),
        reason_codes=list(fallback["reason_codes"]),
        action_reason_codes=[],
        action_ref=None,
        evaluation_ref=None,
        policy_snapshot_ref=None,
        diagnostic=diagnostic,
        execution_sleeve_id=execution_sleeve_id,
        mode=mode,
    )


def _not_adopted_decision(
    *,
    scope_id: str,
    sleeve_scope_policy: Mapping[str, Any],
) -> Dict[str, Any]:
    fallback = _fallback_policy(
        sleeve_scope_policy,
        label="SLEEVE_SCOPE",
        key="not_yet_adopted_default",
    )
    return _decision_row(
        scope_kind="sleeve",
        scope_id=scope_id,
        adoption_state="NOT_ADOPTED",
        artifact_status="NOT_ADOPTED",
        action_state="",
        mandatory_or_advisory="ADVISORY",
        headroom_multiplier_bp=int(fallback["headroom_multiplier_bp"]),
        control_state=str(fallback["control_state"]),
        reason_codes=list(fallback["reason_codes"]),
        action_reason_codes=[],
        action_ref=None,
        evaluation_ref=None,
        policy_snapshot_ref=None,
    )


def _resolve_action_decision(
    *,
    artifact_id: str,
    scope_kind: str,
    scope_id: str,
    day_utc: str,
    path: Path,
    action_map: Mapping[str, Mapping[str, Any]],
    scope_policy: Mapping[str, Any],
    label: str,
    execution_sleeve_id: str = "",
    mode: str = "",
) -> Dict[str, Any]:
    resolved_path = path.resolve()
    if not resolved_path.exists() or not resolved_path.is_file():
        return _fallback_decision(
            scope_kind=scope_kind,
            scope_id=scope_id,
            scope_policy=scope_policy,
            label=label,
            adoption_state="ADOPTED",
            artifact_status="MISSING",
            diagnostic=f"MISSING_ACTION_ARTIFACT:{resolved_path}",
            execution_sleeve_id=execution_sleeve_id,
            mode=mode,
        )
    try:
        payload = _read_json(resolved_path)
        validate_governed_artifact_payload_v1(
            repo_root=REPO_ROOT,
            artifact_id=artifact_id,
            payload=payload,
        )
        if str(payload.get("day_utc") or "").strip() != _parse_day(day_utc):
            raise ValueError("ACTION_ARTIFACT_DAY_MISMATCH")
        if str(payload.get("scope_kind") or "").strip() != scope_kind:
            raise ValueError("ACTION_ARTIFACT_SCOPE_KIND_MISMATCH")
        if str(payload.get("scope_id") or "").strip() != str(scope_id).strip():
            raise ValueError("ACTION_ARTIFACT_SCOPE_ID_MISMATCH")
        action_state = str(payload.get("action_state") or "").strip()
        if action_state not in action_map:
            raise ValueError(f"ACTION_STATE_UNSUPPORTED:{action_state}")
        policy_row = action_map[action_state]
        return _decision_row(
            scope_kind=scope_kind,
            scope_id=scope_id,
            adoption_state="ADOPTED",
            artifact_status="OK",
            action_state=action_state,
            mandatory_or_advisory=str(payload.get("mandatory_or_advisory") or "").strip(),
            headroom_multiplier_bp=int(policy_row["headroom_multiplier_bp"]),
            control_state=str(policy_row["control_state"]),
            reason_codes=list(policy_row["reason_codes"]) + list(payload.get("action_reason_codes") or []),
            action_reason_codes=list(payload.get("action_reason_codes") or []),
            action_ref=_artifact_ref(artifact_id, resolved_path),
            evaluation_ref=dict(payload.get("evaluation_ref") or {}),
            policy_snapshot_ref=dict(payload.get("policy_snapshot_ref") or {}),
            execution_sleeve_id=execution_sleeve_id,
            mode=mode,
        )
    except Exception as exc:
        return _fallback_decision(
            scope_kind=scope_kind,
            scope_id=scope_id,
            scope_policy=scope_policy,
            label=label,
            adoption_state="ADOPTED",
            artifact_status="INVALID",
            diagnostic=f"{type(exc).__name__}:{exc}",
            execution_sleeve_id=execution_sleeve_id,
            mode=mode,
        )


def resolve_capital_authority_runtime_control_v1(
    *,
    truth_root: Path,
    day_utc: str,
    sleeve_ids: Sequence[str],
) -> Dict[str, Any]:
    day = _parse_day(day_utc)
    runtime_policy = _runtime_policy()
    portfolio_scope = runtime_policy.get("portfolio_scope")
    sleeve_scope = runtime_policy.get("sleeve_scope")
    if not isinstance(portfolio_scope, dict):
        raise ValueError("PORTFOLIO_SCOPE_POLICY_MISSING")
    if not isinstance(sleeve_scope, dict):
        raise ValueError("SLEEVE_SCOPE_POLICY_MISSING")

    portfolio_action_map = _action_map(portfolio_scope, label="PORTFOLIO_SCOPE")
    sleeve_action_map = _action_map(sleeve_scope, label="SLEEVE_SCOPE")

    canonical_truth_root = _canonical_truth_root_from_input(truth_root)
    portfolio_path = (
        canonical_truth_root
        / "reports"
        / "portfolio_governance_action_state_v1"
        / day
        / "portfolio_governance_action_state.v1.json"
    ).resolve()
    portfolio_control = _resolve_action_decision(
        artifact_id="portfolio_governance_action_state_v1",
        scope_kind="portfolio",
        scope_id="CONSTELLATION_PORTFOLIO",
        day_utc=day,
        path=portfolio_path,
        action_map=portfolio_action_map,
        scope_policy=portfolio_scope,
        label="PORTFOLIO_SCOPE",
    )

    adopted_bindings: Dict[str, Dict[str, str]] = {}
    for row in resolve_adopted_governed_sleeve_bindings_v1():
        sleeve_id = row["sleeve_id"]
        execution_sleeve_id = row["execution_sleeve_id"]
        mode = row["mode"]
        adopted_bindings[sleeve_id] = {
            "execution_sleeve_id": execution_sleeve_id,
            "mode": mode,
        }

    sleeve_controls: list[Dict[str, Any]] = []
    sleeve_multiplier_by_id: Dict[str, int] = {}
    for sleeve_id in sorted({str(item).strip() for item in sleeve_ids if str(item).strip()}):
        binding = adopted_bindings.get(sleeve_id)
        if binding is None:
            decision = _not_adopted_decision(scope_id=sleeve_id, sleeve_scope_policy=sleeve_scope)
        else:
            execution_sleeve_id = binding["execution_sleeve_id"]
            mode = binding["mode"]
            execution_truth_root = _execution_truth_root_from_binding(
                truth_root=truth_root,
                execution_sleeve_id=execution_sleeve_id,
                mode=mode,
            )
            action_path = (
                execution_truth_root
                / "reports"
                / "sleeve_governance_action_state_v1"
                / day
                / sleeve_id
                / "sleeve_governance_action_state.v1.json"
            ).resolve()
            decision = _resolve_action_decision(
                artifact_id="sleeve_governance_action_state_v1",
                scope_kind="sleeve",
                scope_id=sleeve_id,
                day_utc=day,
                path=action_path,
                action_map=sleeve_action_map,
                scope_policy=sleeve_scope,
                label="SLEEVE_SCOPE",
                execution_sleeve_id=execution_sleeve_id,
                mode=mode,
            )
        sleeve_controls.append(decision)
        sleeve_multiplier_by_id[sleeve_id] = int(decision["headroom_multiplier_bp"])

    return {
        "consumer_seam": "capital_authority_allocation_v1",
        "policy_registry_ref": _policy_ref(),
        "scorecard_control_input_forbidden": bool(runtime_policy.get("scorecard_control_input_forbidden") is True),
        "portfolio_control": portfolio_control,
        "sleeve_controls": sleeve_controls,
        "portfolio_headroom_multiplier_bp": int(portfolio_control["headroom_multiplier_bp"]),
        "sleeve_headroom_multiplier_bp_by_sleeve": sleeve_multiplier_by_id,
    }
