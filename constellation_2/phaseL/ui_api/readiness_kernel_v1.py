from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .common import GLOBAL_TRUTH_ROOT, SLEEVE_TRUTH_ROOT, iso_from_mtime, read_json_dict, resolve_ui_day


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


def _classification(status: str, blocker: str, doc: Dict[str, Any], day: str) -> str:
    status_u = status.upper()
    blocker_u = blocker.upper()
    if _artifact_is_stale(doc, day):
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


def _build_layer_from_file(*, spec: LayerSpec, path: Path, day: str) -> Dict[str, Any]:
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
    classification = _classification(status, blocker, doc, day)
    timestamp = _first_value(doc, spec.timestamp_keys) or iso_from_mtime(path) or ""
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
        "evidence_summary": _summarize(doc, status, blocker),
    }


def _build_phasec_layer(*, day: str, sleeve_truth_root: Path) -> Dict[str, Any]:
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
    layer = _build_layer_from_file(spec=spec, path=path, day=day)
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


def build_readiness_kernel_v1(
    day: Optional[str] = None,
    *,
    truth_root: Optional[Path] = None,
    sleeve_truth_root: Optional[Path] = None,
    environment: str = DEFAULT_ENVIRONMENT,
) -> Dict[str, Any]:
    resolved_day = resolve_ui_day(day) or _as_text(day) or "UNKNOWN"
    root = (truth_root or GLOBAL_TRUTH_ROOT).resolve()
    sleeve_root = (sleeve_truth_root or SLEEVE_TRUTH_ROOT).resolve()

    layers: List[Dict[str, Any]] = []
    for spec in _layer_specs(resolved_day):
        layers.append(_build_layer_from_file(spec=spec, path=_find_path(root, spec), day=resolved_day))
        if spec.layer_id == "STRUCTURE":
            layers.append(_build_phasec_layer(day=resolved_day, sleeve_truth_root=sleeve_root))

    overall_status, canonical_blocker, operator_next_action = _overall(layers)
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": resolved_day,
        "environment": environment,
        "overall_status": overall_status,
        "canonical_blocker": canonical_blocker,
        "operator_next_action": operator_next_action,
        "final_readiness_authority": "aegis_day_run_ledger_v1",
        "supporting_evidence_only_layers": [
            layer.get("layer_id")
            for layer in layers
            if layer.get("layer_id") != "LEDGER"
        ],
        "layers": layers,
        "warnings": _aggregation_warnings(root, layers),
    }
