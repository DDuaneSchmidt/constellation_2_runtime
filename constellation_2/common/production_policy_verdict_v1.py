from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.common.capability_state_v1 import (
    CAPABILITY_POLICY_REGISTRY_RELPATH,
    resolve_capability_state_path,
    resolve_production_policy_verdict_path,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
    sha256_file_v1,
)


PRODUCTION_POLICY_VERDICT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/production_policy_verdict.v1.schema.json"


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def derive_production_policy_verdict_payload(
    *,
    repo_root: Path,
    truth_root: Path,
    capability_ref: SurfaceRefV1,
    gate_stack_path: Path,
) -> Dict[str, Any]:
    capability_payload = capability_ref.payload
    registry_path = (Path(repo_root).resolve() / CAPABILITY_POLICY_REGISTRY_RELPATH).resolve()
    registry_payload = _read_json(registry_path)
    capability_map = {
        str(row.get("capability_id") or "").strip(): row
        for row in (capability_payload.get("capabilities") or [])
        if isinstance(row, dict)
    }
    gate_stack_payload = _read_json(gate_stack_path)

    blocking_items: List[Dict[str, Any]] = []
    for row in registry_payload.get("capabilities") or []:
        if not isinstance(row, dict):
            continue
        capability_id = str(row.get("capability_id") or "").strip()
        capability_state = capability_map.get(capability_id) or {}
        status = str(capability_state.get("status") or "UNKNOWN").strip().upper()
        if status == "PASS":
            continue
        blocking_items.append(
            {
                "item_id": capability_id,
                "item_type": "capability",
                "status": status,
                "reason_codes": list(capability_state.get("reason_codes") or []),
            }
        )

    gate_stack_status = str(gate_stack_payload.get("status") or "FAIL").strip().upper()
    gate_stack_reason_codes = list(gate_stack_payload.get("reason_codes") or [])
    if gate_stack_status != "PASS":
        blocking_items.append(
            {
                "item_id": "gate_stack_verdict_v1",
                "item_type": "production_gate_stack",
                "status": gate_stack_status,
                "reason_codes": gate_stack_reason_codes,
            }
        )

    source_artifacts = list(capability_payload.get("source_artifacts") or [])
    source_artifacts.extend(
        [
            {
                "artifact_family": "capability_state_v1",
                "artifact_path": str(capability_ref.path),
                "artifact_sha256": capability_ref.sha256,
            },
            {
                "artifact_family": "gate_stack_verdict_v1",
                "artifact_path": str(gate_stack_path),
                "artifact_sha256": sha256_file_v1(gate_stack_path),
            },
            {
                "artifact_family": "capability_policy_registry_v1",
                "artifact_path": str(registry_path),
                "artifact_sha256": capability_payload.get("registry_ref", {}).get("artifact_sha256") or "",
            },
        ]
    )
    source_artifacts = sorted(
        {
            (row["artifact_family"], row["artifact_path"], row["artifact_sha256"]): row
            for row in source_artifacts
            if isinstance(row, dict)
        }.values(),
        key=lambda row: (row["artifact_family"], row["artifact_path"]),
    )

    return {
        "schema_id": "production_policy_verdict",
        "schema_version": "v1",
        "day_utc": str(capability_payload.get("day_utc") or "").strip(),
        "environment": str(capability_payload.get("environment") or "").strip(),
        "ib_account": str(capability_payload.get("ib_account") or "").strip(),
        "sleeve_id": str(capability_payload.get("sleeve_id") or "").strip(),
        "overall_status": "PASS" if not blocking_items else "FAIL",
        "production_allowed": not blocking_items,
        "blocking_items": blocking_items,
        "gate_stack_status": gate_stack_status,
        "gate_stack_reason_codes": gate_stack_reason_codes,
        "capability_state_ref": {
            "artifact_path": str(capability_ref.path),
            "artifact_sha256": capability_ref.sha256,
        },
        "gate_stack_ref": {
            "artifact_path": str(gate_stack_path),
            "artifact_sha256": sha256_file_v1(gate_stack_path),
        },
        "source_artifacts": source_artifacts,
        "release_id": str(capability_payload.get("release_id") or "").strip(),
        "git_sha": str(capability_payload.get("git_sha") or "").strip(),
        "generated_at_utc": str(capability_payload.get("generated_at_utc") or ""),
    }


def read_capability_state_ref(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_capability_state_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/capability_state.v1.schema.json",
    )


def write_production_policy_verdict_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_production_policy_verdict_path(truth_root=truth_root, day_utc=str(payload.get("day_utc") or "").strip()),
        payload=payload,
        schema_relpath=PRODUCTION_POLICY_VERDICT_SCHEMA_RELPATH,
        volatile_field_names=("generated_at_utc",),
    )
