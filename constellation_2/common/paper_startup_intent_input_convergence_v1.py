from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
)


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_startup_intent_input_convergence.v1.schema.json"


def resolve_paper_startup_intent_input_convergence_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "paper_startup_intent_input_convergence_v1"
        / str(day_utc).strip()
        / "paper_startup_intent_input_convergence.v1.json"
    ).resolve()


def _normalize_blocker_chain(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        if bool(row.get("required") is not True):
            continue
        blocker_code = str(row.get("blocker_code") or "").strip()
        artifact_id = str(row.get("artifact_id") or "").strip()
        summary = str(row.get("summary") or blocker_code or artifact_id).strip()
        if not blocker_code:
            continue
        key = (artifact_id, blocker_code)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "artifact_id": artifact_id,
                "blocker_code": blocker_code,
                "summary": summary,
            }
        )
    return normalized


def derive_paper_startup_intent_input_convergence_payload_v1(
    *,
    truth_root: Path,
    target_day: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    sleeve_truth_root: Path,
    required_inputs: List[str],
    artifact_results: List[Dict[str, Any]],
    source_refs: List[Dict[str, Any]],
    blocker_chain: List[Dict[str, str]] | None = None,
) -> Dict[str, Any]:
    blocker_rows = blocker_chain or _normalize_blocker_chain(artifact_results)
    convergence_status = "SUCCESS" if not blocker_rows else "BLOCKED"
    return {
        "schema_id": "paper_startup_intent_input_convergence_v1",
        "schema_version": 1,
        "generated_utc": f"{str(target_day).strip()}T00:00:00Z",
        "target_day": str(target_day).strip(),
        "environment": str(environment).strip().upper(),
        "ib_account": str(ib_account).strip(),
        "sleeve_id": str(sleeve_id).strip().upper(),
        "sleeve_truth_root": str(Path(sleeve_truth_root).resolve()),
        "authority_root": str(Path(truth_root).resolve()),
        "convergence_status": convergence_status,
        "required_inputs": [str(item).strip() for item in required_inputs if str(item).strip()],
        "artifact_results": artifact_results,
        "blocker_chain": blocker_rows,
        "source_refs": source_refs,
    }


def write_paper_startup_intent_input_convergence_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_paper_startup_intent_input_convergence_path(
            truth_root=truth_root,
            day_utc=str(payload.get("target_day") or "").strip(),
        ),
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("generated_utc",),
    )
