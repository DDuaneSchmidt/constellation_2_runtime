from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_idempotent_validated_json_v1,
    sha256_file_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance
from constellation_2.common.runtime_path_authority_v1 import (
    resolve_decision_truth_root_v1,
)
from constellation_2.common.runtime_authority_snapshot_bridge_v1 import (
    resolve_runtime_path_authority_snapshot_bridge_v1,
)
from constellation_2.common.control_plane_read_gateway_v1 import (
    read_control_plane_semantic_v1,
)


NEXT_DAY_READINESS_PROBE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/next_day_readiness_probe.v1.schema.json"
TRADE_SUBMIT_READINESS_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json"
RECURRENCE_KILL_GATE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/recurrence_kill_gate.v1.schema.json"


def resolve_next_day_readiness_probe_path(*, truth_root: Path, target_day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "next_day_readiness_probe_v1"
        / str(target_day_utc).strip()
        / "next_day_readiness_probe.v1.json"
    ).resolve()


def _day_minus_one(day_utc: str) -> str:
    return (date.fromisoformat(str(day_utc).strip()) - timedelta(days=1)).isoformat()


def _artifact_ref(path: Path) -> Dict[str, str]:
    return {
        "artifact_path": str(path),
        "artifact_sha256": sha256_file_v1(path),
    }


def _projection_status_map(evidence_projection: List[Dict[str, Any]]) -> Dict[str, str]:
    projection_status: Dict[str, str] = {}
    for row in evidence_projection:
        projection_id = str(row.get("projection_id") or "").strip()
        if not projection_id:
            continue
        projection_status[projection_id] = str(row.get("status") or "UNKNOWN").strip().upper() or "UNKNOWN"
    return projection_status


def _materialization_status_from_projection(status: str) -> str:
    normalized = str(status or "").strip().upper()
    if normalized in {"PASS", "FAIL"}:
        return "PASS"
    return "NOT_YET_MATERIALIZED_BUT_PROVABLE"

def derive_next_day_readiness_probe_payload(
    *,
    repo_root: Path,
    truth_root: Path,
    target_day_utc: str,
    environment: str,
    ib_account: str,
) -> Dict[str, Any]:
    resolved_truth_root = resolve_decision_truth_root_v1(truth_root, repo_root=repo_root)
    authority_snapshot = resolve_runtime_path_authority_snapshot_bridge_v1(
        repo_root=repo_root,
        caller="constellation_2/common/next_day_readiness_probe_v1.py",
    )
    current_day_utc = _day_minus_one(target_day_utc)

    semantic_view = read_control_plane_semantic_v1(
        domain="session",
        surface="next_day_readiness_probe_inputs",
        truth_root=resolved_truth_root,
        day_utc=target_day_utc,
        environment=environment,
        ib_account=ib_account,
    )
    semantic_payload = dict(semantic_view.payload)
    evidence_projection = list(semantic_payload.get("evidence_projection") or [])
    projection_status = _projection_status_map(evidence_projection)
    current_baseline_status = str(semantic_payload.get("current_baseline_status") or "").strip().upper()
    current_baseline_pass = current_baseline_status == "PASS"
    predicted_blocking_items = list(semantic_payload.get("predicted_blocking_items") or [])
    probe_status = str(semantic_payload.get("probe_status") or "UNKNOWN").strip().upper()
    confidence = str(semantic_payload.get("confidence") or "LOW").strip().upper()
    target_paper_status = projection_status.get("target_day_paper_policy", "UNKNOWN")
    target_trade_status = projection_status.get("target_day_trade_submit_readiness", "UNKNOWN")
    predicted_paper_policy_status = target_paper_status if target_paper_status in {"PASS", "FAIL"} else "UNKNOWN"

    capability_projection = [
        {
            "capability_id": "runtime_path_authority_valid",
            "status": "PASS",
            "basis": authority_snapshot,
        },
        {
            "capability_id": "current_day_baseline_green",
            "status": projection_status.get("current_day_baseline", "UNKNOWN"),
            "basis": ["current_day_baseline"],
        },
        {
            "capability_id": "target_day_policy_materialized",
            "status": _materialization_status_from_projection(target_paper_status),
            "basis": ["target_day_paper_policy"],
        },
        {
            "capability_id": "target_day_submit_readiness_materialized",
            "status": _materialization_status_from_projection(target_trade_status),
            "basis": ["target_day_trade_submit_readiness"],
        },
    ]

    provenance = resolve_release_provenance()
    return {
        "schema_id": "next_day_readiness_probe",
        "schema_version": "v1",
        "target_day_utc": str(target_day_utc).strip(),
        "reference_day_utc": current_day_utc,
        "truth_root": str(resolved_truth_root),
        "truth_sleeves_root": authority_snapshot["canonical_runtime_truth_sleeves_root"],
        "release_id": str(provenance.get("release_id") or "").strip(),
        "git_sha": str(provenance.get("git_sha") or "").strip(),
        "evidence_projection": evidence_projection,
        "capability_projection": capability_projection,
        "predicted_paper_policy_status": predicted_paper_policy_status,
        "predicted_blocking_items": predicted_blocking_items,
        "confidence": confidence,
        "probe_status": probe_status,
        "generated_at_utc": f"{str(target_day_utc).strip()}T00:00:00Z",
    }


def write_next_day_readiness_probe_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_next_day_readiness_probe_path(
            truth_root=truth_root,
            target_day_utc=str(payload.get("target_day_utc") or "").strip(),
        ),
        payload=payload,
        schema_relpath=NEXT_DAY_READINESS_PROBE_SCHEMA_RELPATH,
        volatile_field_names=("generated_at_utc",),
    )
