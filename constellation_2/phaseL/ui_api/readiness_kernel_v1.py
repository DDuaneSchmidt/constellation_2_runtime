from __future__ import annotations

import hashlib
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .common import GLOBAL_TRUTH_ROOT, SLEEVE_TRUTH_ROOT, iso_from_mtime, read_json_dict, resolve_ui_day
from ops.tools.aegis_runtime_mode_v1 import (
    CANDIDATE_TRUTH_ROOT,
    PRODUCTION_TRUTH_ROOT,
    normalize_runtime_mode_v1,
    production_version_path_v1,
    read_json_v1,
    read_production_version_v1,
    runtime_mode_from_truth_root_v1,
)
from ops.tools.aegis_submit_enforcement_v1 import evaluate_submit_enforcement_v1, packet_currentness_v1


SCHEMA_ID = "readiness_kernel"
SCHEMA_VERSION = "v1"
DEFAULT_ENVIRONMENT = "PAPER"

PASS_LIKE = {
    "ADMIT",
    "AUTHORIZED",
    "COMPLETE",
    "CURRENT",
    "GRANTED",
    "OPEN_READY",
    "PAPER_READY",
    "PAPER_TRANSMIT_ENABLED",
    "PASS",
    "READY",
    "SIZED",
}

BLOCK_LIKE = {
    "BLOCKED",
    "DENIED",
    "FAIL",
    "FAIL_CLOSED",
    "NO_GO",
    "NOT_READY",
    "REJECTED",
}

OUT_OF_SESSION_BLOCKERS = {"MARKET_CLOSED", "MARKET_NOT_OPEN", "OUT_OF_SESSION"}
EXTERNAL_PREFIXES = ("IB_", "MARKET_", "OPTIONS_", "BROKER_", "TWS_")


def _phase_controlled_truth_root(runtime_mode: Optional[str] = None) -> Path:
    mode = normalize_runtime_mode_v1(runtime_mode or os.environ.get("AEGIS_UI_RUNTIME_MODE") or os.environ.get("AEGIS_RUNTIME_MODE"))
    return PRODUCTION_TRUTH_ROOT if mode == "PRODUCTION" else CANDIDATE_TRUTH_ROOT


def _latest_control_plane_day(root: Path) -> str:
    family = (root / "reports" / "aegis_control_plane_v1").resolve()
    if not family.exists() or not family.is_dir():
        return ""
    days = sorted(path.name for path in family.iterdir() if path.is_dir())
    return days[-1] if days else ""


def _control_plane_path(root: Path, day: str) -> Path:
    return (root / "reports" / "aegis_control_plane_v1" / day / "control_plane.v1.json").resolve()


def _packet_status(root: Path, runtime_mode: str) -> Dict[str, Any]:
    return packet_currentness_v1(runtime_root=root, runtime_mode=runtime_mode)


def _production_version_status(root: Path, runtime_mode: str) -> Dict[str, Any]:
    path = production_version_path_v1(root)
    if runtime_mode != "PRODUCTION":
        return {
            "path": str(path),
            "present": False,
            "status": "NOT_REQUIRED_FOR_CANDIDATE",
            "promoted_commit": "",
            "promotion_id": "",
        }
    payload = read_production_version_v1(root)
    return {
        "path": str(path),
        "present": bool(payload),
        "status": str(payload.get("status") or "MISSING").strip().upper() if payload else "MISSING",
        "promoted_commit": str(payload.get("promoted_commit") or "").strip(),
        "promotion_id": str(payload.get("promotion_id") or "").strip(),
    }


def _supporting_path(root: Path, family: str, day: str, filename: str) -> str:
    return str((root / "reports" / family / day / filename).resolve())


def _submit_projection(root: Path, sleeve_root: Path, day: str, runtime_mode: str) -> Dict[str, Any]:
    try:
        result = evaluate_submit_enforcement_v1(
            truth_root=root,
            execution_root=sleeve_root,
            runtime_root=root,
            day_utc=day,
            action_id="submit_paper_order",
            runtime_mode=runtime_mode,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "BLOCKED",
            "source": "submit_firewall",
            "canonical_blocker": f"SUBMIT_FIREWALL_UNAVAILABLE:{type(exc).__name__}",
            "blockers": [{"code": f"SUBMIT_FIREWALL_UNAVAILABLE:{type(exc).__name__}", "detail": str(exc)}],
        }
    return {
        "status": "ALLOWED" if result.get("ok") is True else "BLOCKED",
        "source": "submit_firewall",
        "canonical_blocker": str(result.get("canonical_blocker") or ""),
        "blockers": result.get("blockers") if isinstance(result.get("blockers"), list) else [],
        "checked_paths": result.get("checked_paths") if isinstance(result.get("checked_paths"), dict) else {},
        "packet_currentness": result.get("packet_currentness") if isinstance(result.get("packet_currentness"), dict) else {},
    }


def _phase_rows(control: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = control.get("phase_results") if isinstance(control.get("phase_results"), list) else []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = _as_text(row.get("status") or "UNKNOWN")
        semantic = "blocked" if status == "BLOCKING_CURRENT_RUN" else ("warning" if status == "DEFERRED_BY_UPSTREAM_BLOCKER" else ("healthy" if status == "PASS" else "unknown"))
        out.append(
            {
                "layer_id": _as_text(row.get("phase_id")),
                "label": _as_text(row.get("phase_id")).replace("_", " ").title(),
                "status": status,
                "classification": status,
                "semantic": semantic,
                "canonical_blocker": (_as_text(row.get("blocker_codes")[0]) if isinstance(row.get("blocker_codes"), list) and row.get("blocker_codes") else ""),
                "source_path": "",
                "source_sha256": "",
                "next_action": _as_text(row.get("recovery_action")),
                "evidence_summary": {
                    "evidence_paths": row.get("evidence_paths") if isinstance(row.get("evidence_paths"), list) else [],
                    "recovery_commands": row.get("recovery_commands") if isinstance(row.get("recovery_commands"), list) else [],
                },
            }
        )
    return out


@dataclass(frozen=True)
class LayerSpec:
    layer_id: str
    label: str
    paths: Tuple[str, ...]
    status_keys: Tuple[str, ...]
    blocker_keys: Tuple[str, ...] = ("canonical_blocker", "first_blocker", "first_blocker_code")
    timestamp_keys: Tuple[str, ...] = (
        "produced_at_utc",
        "produced_utc",
        "updated_at_utc",
        "updated_utc",
        "completed_at_utc",
    )


def _layer_specs(day: str) -> List[LayerSpec]:
    return [
        LayerSpec(
            layer_id="BUILD",
            label="Build",
            paths=(f"target_day_build_v1/{day}.json",),
            status_keys=("status", "build_status", "source_reproducibility_status"),
        ),
        LayerSpec(
            layer_id="ADMISSION",
            label="Admission",
            paths=(
                f"target_day_admission_v1/{day}.json",
                f"reports/paper_session_authority_v1/{day}/paper_session_authority.v1.json",
            ),
            status_keys=("target_day_admission_status", "admission_status", "paper_session_status", "status"),
        ),
        LayerSpec(
            layer_id="MARKET_DATA",
            label="Market Data",
            paths=(f"reports/market_open_data_gate_v1/{day}/market_open_data_gate.v1.json",),
            status_keys=("status", "gate_status", "market_open_data_gate_status"),
        ),
        LayerSpec(
            layer_id="STRUCTURE",
            label="Structure",
            paths=(f"reports/structure_decision_supply_v1/{day}/structure_decision_supply.v1.json",),
            status_keys=("status", "structure_decision_status"),
        ),
        LayerSpec(
            layer_id="AUTHORIZATION",
            label="Authorization",
            paths=(f"reports/authorization_supply_v1/{day}/authorization_supply.v1.json",),
            status_keys=("status", "authorization_status"),
        ),
        LayerSpec(
            layer_id="RISK",
            label="Risk",
            paths=(f"reports/risk_sizing_authority_v1/{day}/risk_sizing_authority.v1.json",),
            status_keys=("status", "risk_sizing_state", "state"),
        ),
        LayerSpec(
            layer_id="SUBMIT_BOUNDARY",
            label="Submit Boundary",
            paths=(f"reports/submit_boundary_status_v1/{day}/submit_boundary_status.v1.json",),
            status_keys=("boundary_status", "status", "submit_boundary_status"),
        ),
        LayerSpec(
            layer_id="LEDGER",
            label="Day Ledger",
            paths=(f"reports/aegis_day_run_v1/{day}/day_run.v1.json",),
            status_keys=("final_status", "status"),
        ),
        LayerSpec(
            layer_id="CONTROL",
            label="Execution Control",
            paths=(f"reports/execution_mode_authority_v1/{day}/execution_mode_authority.v1.json",),
            status_keys=("mode_state", "execution_mode_state", "mode", "status"),
        ),
    ]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _first_value(doc: Dict[str, Any], keys: Sequence[str]) -> str:
    for key in keys:
        value = doc.get(key)
        if value is not None and _as_text(value):
            return _as_text(value)
    return ""


def _find_path(root: Path, spec: LayerSpec) -> Path:
    for relative in spec.paths:
        path = (root / relative).resolve()
        if path.exists():
            return path
    return (root / spec.paths[0]).resolve()


def _latest_phasec_identity(sleeve_truth_root: Path, day: str) -> Optional[Path]:
    candidates: List[Path] = []
    for root in (
        sleeve_truth_root / "phaseC_preflight_v1" / day,
        sleeve_truth_root / "execution_evidence_v1" / "submissions" / day,
    ):
        if root.exists():
            candidates.extend(root.rglob("execution_identity_record.v1.json"))
    if not candidates:
        return None
    candidates.sort(key=lambda path: (path.stat().st_mtime, str(path)))
    return candidates[-1]


def _artifact_is_stale(doc: Dict[str, Any], day: str) -> bool:
    doc_day = _as_text(doc.get("day_utc") or doc.get("target_day") or doc.get("business_day"))
    if doc_day and doc_day != day:
        return True
    for key in (
        "classification",
        "freshness_status",
        "freshness_verdict",
        "lineage_status",
        "stale_artifact_status",
    ):
        value = _as_text(doc.get(key)).upper()
        if value in {"STALE", "STALE_ARTIFACT"}:
            return True
    for key in ("canonical_blocker", "first_blocker", "first_blocker_code"):
        if _as_text(doc.get(key)).upper() == "STALE_ARTIFACT":
            return True
    reason_codes = doc.get("reason_codes")
    if isinstance(reason_codes, list) and any(_as_text(item).upper() == "STALE_ARTIFACT" for item in reason_codes):
        return True
    return False


def _truth_freshness_by_artifact_path(root: Path, day: str) -> Dict[str, Dict[str, Any]]:
    path = (root / "reports" / "truth_freshness_v1" / day / "truth_freshness.v1.json").resolve()
    doc, error = read_json_dict(path)
    if error:
        return {}
    records = doc.get("freshness_records") if isinstance(doc.get("freshness_records"), list) else []
    result: Dict[str, Dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        artifact_path = _as_text(record.get("artifact_path"))
        if artifact_path:
            result[artifact_path] = record
    return result


def _unified_truth_kernel(root: Path, day: str) -> Tuple[Dict[str, Any], Path, str]:
    path = (root / "reports" / "unified_truth_kernel_v1" / day / "unified_truth_kernel.v1.json").resolve()
    doc, error = read_json_dict(path)
    return (doc if isinstance(doc, dict) else {}, path, error)


def _classification(status: str, blocker: str, doc: Dict[str, Any], day: str, freshness_record: Optional[Dict[str, Any]] = None) -> str:
    status_u = status.upper()
    blocker_u = blocker.upper()
    freshness_status = _as_text((freshness_record or {}).get("freshness_status")).upper()
    if freshness_status in {"STALE", "EXPIRED"}:
        return "STALE_ARTIFACT"
    if not freshness_record and _artifact_is_stale(doc, day):
        return "STALE_ARTIFACT"
    if blocker_u in OUT_OF_SESSION_BLOCKERS or status_u in {"OUT_OF_SESSION", "MARKET_CLOSED"}:
        return "OUT_OF_SESSION"
    if blocker_u:
        return "EXTERNAL_FAIL" if blocker_u.startswith(EXTERNAL_PREFIXES) else "POLICY_FAIL"
    if status_u in PASS_LIKE:
        return "CURRENT"
    if status_u in BLOCK_LIKE:
        return "POLICY_FAIL"
    if status_u in {"PENDING", "SKIPPED"}:
        return "UNKNOWN"
    return "UNKNOWN"


def _semantic(classification: str, status: str) -> str:
    if classification == "CURRENT":
        return "healthy"
    if classification in {"OUT_OF_SESSION", "STALE_ARTIFACT"}:
        return "warning"
    if classification in {"POLICY_FAIL", "EXTERNAL_FAIL"}:
        return "blocked"
    if status.upper() in PASS_LIKE:
        return "healthy"
    return "unknown"


def _next_action(layer_id: str, status: str, blocker: str, classification: str) -> str:
    if classification == "CURRENT":
        return "No operator action required for this layer."
    if blocker == "MARKET_CLOSED":
        return "Rerun market-open data and readiness gates during the next eligible market session."
    if blocker == "MARKET_NOT_OPEN":
        return "Rerun market-open data gate after 09:30 ET."
    if classification == "MISSING_EVIDENCE":
        return f"Run or inspect the producer for {layer_id} evidence."
    if classification == "STALE_ARTIFACT":
        return f"Regenerate current-day {layer_id} evidence from its governed producer."
    if blocker:
        return f"Resolve {blocker} at the source artifact, then rerun dependent readiness aggregation."
    if not status or status == "UNKNOWN":
        return f"Inspect {layer_id} source artifact; state is not usable by readiness_kernel_v1."
    return "Review source artifact details."


def _summarize(doc: Dict[str, Any], status: str, blocker: str) -> Dict[str, Any]:
    summary: Dict[str, Any] = {"status": status, "canonical_blocker": blocker}
    for key in (
        "final_status",
        "canonical_phase",
        "boundary_status",
        "mode_state",
        "mode",
        "state",
        "operator_next_action",
        "reason_codes",
    ):
        if key in doc:
            summary[key] = doc.get(key)
    return summary


def _build_layer_from_file(*, spec: LayerSpec, path: Path, day: str, freshness_records: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    doc, error = read_json_dict(path)
    if error:
        classification = "MISSING_EVIDENCE" if error == "FILE_NOT_FOUND" else "UNKNOWN"
        status = "UNKNOWN"
        blocker = error
        return {
            "layer_id": spec.layer_id,
            "label": spec.label,
            "status": status,
            "classification": classification,
            "semantic": "unknown",
            "canonical_blocker": blocker,
            "source_path": str(path),
            "source_sha256": "",
            "produced_at_utc": "",
            "updated_at_utc": "",
            "next_action": _next_action(spec.layer_id, status, blocker, classification),
            "evidence_summary": {"read_error": error},
        }

    status = _first_value(doc, spec.status_keys) or "UNKNOWN"
    blocker = _first_value(doc, spec.blocker_keys)
    freshness_record = (freshness_records or {}).get(str(path))
    classification = _classification(status, blocker, doc, day, freshness_record)
    timestamp = _first_value(doc, spec.timestamp_keys) or iso_from_mtime(path) or ""
    evidence_summary = _summarize(doc, status, blocker)
    if freshness_record:
        evidence_summary["truth_freshness"] = {
            "freshness_status": freshness_record.get("freshness_status"),
            "freshness_policy_id": freshness_record.get("freshness_policy_id"),
            "age_seconds": freshness_record.get("age_seconds"),
            "canonical_blocker": freshness_record.get("canonical_blocker"),
        }
    return {
        "layer_id": spec.layer_id,
        "label": spec.label,
        "status": status,
        "classification": classification,
        "semantic": _semantic(classification, status),
        "canonical_blocker": blocker,
        "source_path": str(path),
        "source_sha256": _sha256(path),
        "produced_at_utc": timestamp,
        "updated_at_utc": _as_text(doc.get("updated_at_utc") or doc.get("updated_utc")),
        "next_action": _as_text(doc.get("operator_next_action")) or _next_action(spec.layer_id, status, blocker, classification),
        "evidence_summary": evidence_summary,
    }


def _build_phasec_layer(*, day: str, sleeve_truth_root: Path, freshness_records: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    spec = LayerSpec(
        layer_id="PHASEC",
        label="PhaseC",
        paths=(f"phaseC_preflight_v1/{day}/execution_identity_record.v1.json",),
        status_keys=("status", "identity_status", "phasec_status"),
        blocker_keys=("canonical_blocker", "veto_code", "first_blocker", "first_blocker_code"),
        timestamp_keys=("produced_at_utc", "produced_utc", "created_at_utc", "updated_at_utc"),
    )
    path = _latest_phasec_identity(sleeve_truth_root, day)
    if path is None:
        path = (sleeve_truth_root / spec.paths[0]).resolve()
    layer = _build_layer_from_file(spec=spec, path=path, day=day, freshness_records=freshness_records)
    if layer["classification"] == "UNKNOWN" and not layer["canonical_blocker"] and layer["source_sha256"]:
        layer["status"] = "CURRENT"
        layer["classification"] = "CURRENT"
        layer["semantic"] = "healthy"
        layer["next_action"] = "No operator action required for this layer."
        layer["evidence_summary"]["status"] = "CURRENT"
    return layer


def _legacy_surface_statuses(doc: Dict[str, Any]) -> Dict[str, str]:
    details = doc.get("canonical_readiness_authority", {}).get("details", {})
    surfaces = details.get("canonical_surfaces")
    if not isinstance(surfaces, list):
        return {}
    result: Dict[str, str] = {}
    for item in surfaces:
        if not isinstance(item, dict):
            continue
        surface = _as_text(item.get("artifact_type") or item.get("surface") or item.get("key"))
        status = _as_text(item.get("status_value") or item.get("status") or item.get("state"))
        if surface and status:
            result[surface] = status
    return result


def _aggregation_warnings(truth_root: Path, layers: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    path = (truth_root / "session_authority_status_v1" / "current.json").resolve()
    doc, error = read_json_dict(path)
    if error:
        return [
            {
                "code": "LEGACY_SESSION_STATUS_UNAVAILABLE",
                "message": "Legacy session_authority_status_v1 is unavailable; readiness kernel uses direct source artifacts.",
                "source_path": str(path),
                "read_error": error,
            }
        ]
    legacy_statuses = _legacy_surface_statuses(doc or {})
    if not legacy_statuses:
        return [
            {
                "code": "LEGACY_SESSION_STATUS_INCOMPLETE",
                "message": "Legacy session_authority_status_v1 has no canonical surfaces; ladder truth remains direct-source based.",
                "source_path": str(path),
            }
        ]

    warnings: List[Dict[str, Any]] = []
    layer_to_surface = {
        "SUBMIT_BOUNDARY": "submit_boundary_status_v1",
        "LEDGER": "aegis_day_run_v1",
        "CONTROL": "execution_mode_authority_v1",
    }
    for layer in layers:
        surface = layer_to_surface.get(str(layer.get("layer_id")))
        if not surface:
            continue
        legacy_status = legacy_statuses.get(surface)
        direct_status = _as_text(layer.get("status"))
        if legacy_status and direct_status and legacy_status != direct_status:
            warnings.append(
                {
                    "code": "AGGREGATION_MISMATCH",
                    "message": f"Legacy {surface} status {legacy_status} differs from direct source status {direct_status}.",
                    "layer_id": layer.get("layer_id"),
                    "legacy_status": legacy_status,
                    "direct_status": direct_status,
                    "legacy_source_path": str(path),
                    "direct_source_path": layer.get("source_path"),
                }
            )
    return warnings


def _overall(layers: Sequence[Dict[str, Any]]) -> Tuple[str, str, str]:
    ledger = next((layer for layer in layers if layer.get("layer_id") == "LEDGER"), None)
    if not ledger:
        return ("UNKNOWN", "DAY_RUN_LEDGER_MISSING", "Regenerate aegis_day_run_v1 before evaluating final readiness.")
    ledger_status = str(ledger.get("status") or "").upper()
    ledger_blocker = str(ledger.get("canonical_blocker") or "")
    if ledger.get("classification") in {"MISSING_EVIDENCE", "UNKNOWN"} and not ledger.get("source_sha256"):
        return ("UNKNOWN", ledger_blocker or "DAY_RUN_LEDGER_MISSING", str(ledger.get("next_action") or "Regenerate aegis_day_run_v1."))
    if ledger_blocker in OUT_OF_SESSION_BLOCKERS:
        return (
            "OUT_OF_SESSION",
            ledger_blocker,
            str(ledger.get("next_action") or "Rerun readiness during the next eligible market session."),
        )
    if ledger_blocker or ledger_status in BLOCK_LIKE:
        return ("BLOCKED", ledger_blocker or ledger_status or "DAY_RUN_LEDGER_NOT_READY", str(ledger.get("next_action") or "Resolve the day-run ledger blocker."))
    if ledger_status in {"PRE_MARKET_READY", "PAPER_READY", "PAPER_READY_WITH_DELAYED_DATA", "TRADING_ACTIVE", "EOD_COMPLETE", "READY"}:
        return ("READY", "", "No operator action required.")
    return ("NOT_READY", ledger_blocker, str(ledger.get("next_action") or "Review the day-run ledger for the next required action."))


def _overall_from_kernel(kernel: Dict[str, Any]) -> Tuple[str, str, str]:
    final_status = _as_text(kernel.get("final_status")).upper()
    blocker = _as_text(kernel.get("canonical_blocker") or kernel.get("first_blocker"))
    action = _as_text(kernel.get("operator_next_action")) or "Review unified_truth_kernel_v1."
    if not final_status:
        return ("UNKNOWN", "UNIFIED_TRUTH_KERNEL_EMPTY", "Regenerate unified_truth_kernel_v1.")
    if blocker or final_status in BLOCK_LIKE:
        return ("BLOCKED", blocker or final_status, action)
    if final_status in {"PRE_MARKET_READY", "PAPER_READY", "PAPER_READY_WITH_DELAYED_DATA", "TRADING_ACTIVE", "EOD_COMPLETE", "READY"}:
        return ("READY", "", action)
    return ("NOT_READY", blocker, action)


def build_readiness_kernel_v1(
    day: Optional[str] = None,
    *,
    truth_root: Optional[Path] = None,
    sleeve_truth_root: Optional[Path] = None,
    environment: str = DEFAULT_ENVIRONMENT,
) -> Dict[str, Any]:
    root = (truth_root or _phase_controlled_truth_root()).resolve()
    sleeve_root = (sleeve_truth_root or SLEEVE_TRUTH_ROOT).resolve()
    explicit_day = _as_text(day)
    resolved_day = explicit_day or _latest_control_plane_day(root) or resolve_ui_day(day) or "UNKNOWN"
    runtime_mode = runtime_mode_from_truth_root_v1(root)
    control_path = _control_plane_path(root, resolved_day)
    control, control_error = read_json_dict(control_path)
    control = control if isinstance(control, dict) else {}
    packet = _packet_status(root, runtime_mode)
    production_version = _production_version_status(root, runtime_mode)
    submit = _submit_projection(root, sleeve_root, resolved_day, runtime_mode)
    if not control:
        layers: List[Dict[str, Any]] = []
        overall_status = "UNKNOWN"
        canonical_blocker = "CONTROL_PLANE_MISSING"
        operator_next_action = f"Generate aegis_control_plane_v1 for {resolved_day}."
        current_phase = ""
        blocker_owner = ""
        recovery_commands: List[str] = []
        evidence_paths: List[str] = [str(control_path)]
        deferred_phases: List[str] = []
    else:
        layers = _phase_rows(control)
        control_final = _as_text(control.get("final_status")).upper()
        overall_status = "READY" if control_final == "READY" else "BLOCKED"
        canonical_blocker = _as_text(control.get("canonical_blocker"))
        operator_next_action = _as_text(control.get("recovery_action"))
        current_phase = _as_text(control.get("current_phase"))
        blocker_owner = _as_text(control.get("blocker_owner"))
        recovery_commands = [str(item) for item in control.get("recovery_commands", []) if str(item or "").strip()] if isinstance(control.get("recovery_commands"), list) else []
        evidence_paths = [str(item) for item in control.get("evidence_paths", []) if str(item or "").strip()] if isinstance(control.get("evidence_paths"), list) else []
        deferred_phases = [str(item) for item in control.get("deferred_phases", []) if str(item or "").strip()] if isinstance(control.get("deferred_phases"), list) else []
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": resolved_day,
        "environment": environment,
        "runtime_mode": runtime_mode,
        "truth_root": str(root),
        "primary_ui_authority": "aegis_control_plane_v1",
        "primary_authority_path": str(control_path),
        "control_plane_error": control_error or "",
        "overall_status": overall_status,
        "canonical_blocker": canonical_blocker,
        "current_phase": current_phase,
        "blocker_owner": blocker_owner,
        "operator_next_action": operator_next_action,
        "recovery_action": operator_next_action,
        "recovery_commands": recovery_commands,
        "evidence_paths": evidence_paths,
        "deferred_phases": deferred_phases,
        "submit": submit,
        "submit_status": submit.get("status"),
        "submit_source": submit.get("source"),
        "submit_canonical_blocker": submit.get("canonical_blocker"),
        "packet": packet,
        "packet_status": packet.get("status"),
        "production_version": production_version,
        "production_version_status": production_version.get("status"),
        "promoted_commit": production_version.get("promoted_commit"),
        "final_readiness_authority": "aegis_control_plane_v1",
        "truth_resolution_source_path": str(control_path) if control else "",
        "unified_truth_kernel_status": "SUPPORTING_EVIDENCE_ONLY",
        "supporting_evidence_only_paths": {
            "requirement_graph": _supporting_path(root, "aegis_requirement_graph_v1", resolved_day, "requirement_graph.v1.json"),
            "unified_truth_kernel": _supporting_path(root, "unified_truth_kernel_v1", resolved_day, "unified_truth_kernel.v1.json"),
            "day_run": _supporting_path(root, "aegis_day_run_v1", resolved_day, "day_run.v1.json"),
            "submit_boundary": _supporting_path(root, "submit_boundary_status_v1", resolved_day, "submit_boundary_status.v1.json"),
            "action_validity": _supporting_path(root, "action_validity_v1", resolved_day, "action_validity.v1.json"),
            "packet": str(Path(packet.get("path") or "")),
        },
        "supporting_evidence_only_layers": [layer.get("layer_id") for layer in layers if layer.get("status") != "BLOCKING_CURRENT_RUN"],
        "layers": layers,
        "warnings": (
            [
                {
                    "code": "CONTROL_PLANE_UNAVAILABLE",
                    "message": "UI cannot resolve readiness without aegis_control_plane_v1.",
                    "source_path": str(control_path),
                    "read_error": control_error,
                }
            ]
            if not control
            else []
        ),
    }
