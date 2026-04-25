from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from constellation_2.common.bounded_obligation_pipeline_v1 import (
    PipelineBlockedError,
    blocked_pipeline_report_v1,
    execute_bounded_obligation_pipeline_v1,
)
from constellation_2.phaseL.ui.server import c2_ops_cockpit_status_v2_collector_v1 as collector_tool


PIPELINE_ID = "c2_ops_cockpit_status_v2_collector_v1"
TARGET_PATH_FAMILY = "constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_v1.py"


def _governing_refs_from_payload(payload: Mapping[str, Any]) -> list[dict[str, str]]:
    provenance = payload.get("provenance") if isinstance(payload.get("provenance"), Mapping) else {}
    rows: list[dict[str, str]] = []
    for path in provenance.get("source_paths") or []:
        if not isinstance(path, str) or not path.strip():
            continue
        rows.append(
            {
                "artifact_id": "collector_source_path",
                "artifact_path": str(Path(path).resolve()),
                "artifact_sha256": "",
            }
        )
    return rows


def resolve_cockpit_status_pipeline_inputs_v1(
    *,
    truth_root: Path,
    instance_config_path: Path,
    day: str,
    attempt_id: Optional[str],
    c3_status: Optional[Dict[str, Any]],
    pipeline_mode: str,
) -> Dict[str, Any]:
    mode = str(pipeline_mode or "").strip() or "normal"
    if mode != "normal":
        raise PipelineBlockedError(f"PIPELINE_MODE_UNSUPPORTED:{PIPELINE_ID}:{mode}")
    return {
        "truth_root": Path(truth_root).resolve(),
        "instance_config_path": Path(instance_config_path).resolve(),
        "day": str(day).strip(),
        "attempt_id": str(attempt_id or "").strip() or None,
        "c3_status": dict(c3_status or {}) if isinstance(c3_status, Mapping) else c3_status,
        "pipeline_mode": mode,
    }


def evaluate_cockpit_status_pipeline_v1(resolved: Mapping[str, Any]) -> Dict[str, Any]:
    payload = collector_tool._build_status_v2_core(
        Path(str(resolved["truth_root"])),
        Path(str(resolved["instance_config_path"])),
        str(resolved["day"]),
        resolved.get("attempt_id"),
        resolved.get("c3_status"),
    )
    return {
        "payload": payload,
        "governing_refs": _governing_refs_from_payload(payload),
    }


def persist_cockpit_status_pipeline_outputs_v1(
    resolved: Mapping[str, Any],
    evaluated: Mapping[str, Any],
) -> Dict[str, Any]:
    del resolved
    return {
        "governing_refs": list(evaluated.get("governing_refs") or []),
    }


def project_emit_cockpit_status_pipeline_v1(
    resolved: Mapping[str, Any],
    evaluated: Mapping[str, Any],
    persisted: Mapping[str, Any],
) -> Dict[str, Any]:
    del resolved
    payload = dict(evaluated["payload"])
    return {
        "payload": payload,
        "governing_refs": list(persisted.get("governing_refs") or evaluated.get("governing_refs") or []),
    }


def run_cockpit_status_obligation_pipeline_v1(
    *,
    truth_root: Path,
    instance_config_path: Path,
    day: str,
    attempt_id: Optional[str],
    c3_status: Optional[Dict[str, Any]] = None,
    pipeline_mode: str = "normal",
    budget_profile: str = "contract_default",
) -> Dict[str, Any]:
    mode = str(pipeline_mode or "").strip() or "normal"
    if mode != "normal":
        blocked = blocked_pipeline_report_v1(
            pipeline_id=PIPELINE_ID,
            pipeline_mode=mode,
            target_path_family=TARGET_PATH_FAMILY,
            blocked_reason=f"PIPELINE_MODE_UNSUPPORTED:{PIPELINE_ID}:{mode}",
            budget_profile=budget_profile,
        )
        return {
            "ok": False,
            "payload": {"pipeline_proof": dict(blocked["proof"])},
            "proof": dict(blocked["proof"]),
        }
    report = execute_bounded_obligation_pipeline_v1(
        pipeline_id=PIPELINE_ID,
        pipeline_mode=mode,
        target_path_family=TARGET_PATH_FAMILY,
        budget_profile=budget_profile,
        resolve_inputs=lambda: resolve_cockpit_status_pipeline_inputs_v1(
            truth_root=truth_root,
            instance_config_path=instance_config_path,
            day=day,
            attempt_id=attempt_id,
            c3_status=c3_status,
            pipeline_mode=mode,
        ),
        evaluate=evaluate_cockpit_status_pipeline_v1,
        persist_outputs=persist_cockpit_status_pipeline_outputs_v1,
        project_emit=project_emit_cockpit_status_pipeline_v1,
    )
    payload = dict(report["projected"].get("payload") or {})
    payload["pipeline_proof"] = dict(report["proof"])
    return {
        "ok": bool(report["ok"]),
        "payload": payload,
        "proof": dict(report["proof"]),
    }
