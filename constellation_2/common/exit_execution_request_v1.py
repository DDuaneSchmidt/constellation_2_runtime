from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from constellation_2.common.exit_decision_engine_v1 import (
    ACTION_EXIT_FULL,
    ACTION_TAKE_PARTIAL,
    ACTION_UPDATE_STOP,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    repo_git_sha_v1,
)


EXIT_EXECUTION_REQUEST_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/exit_execution_request.v1.schema.json"


def resolve_exit_execution_request_path(*, truth_root: Path, day_utc: str, position_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "positions_v1"
        / "exit_execution_request_v1"
        / parse_day_utc_v1(day_utc)
        / str(position_id).strip()
        / "exit_execution_request.v1.json"
    ).resolve()


def _normalize_provenance_refs(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        logical_name = str(row.get("logical_name") or "").strip()
        artifact_path = str(row.get("artifact_path") or "").strip()
        artifact_sha256 = str(row.get("artifact_sha256") or "").strip()
        if not logical_name and not artifact_path:
            continue
        key = (logical_name, artifact_path)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "logical_name": logical_name,
                "artifact_path": artifact_path,
                "artifact_sha256": artifact_sha256,
            }
        )
    return normalized


def derive_exit_execution_request_payload_v1(
    *,
    day_utc: str,
    position_id: str,
    exit_decision_payload: Mapping[str, Any],
    provenance_refs: Iterable[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    action = str(exit_decision_payload.get("decision_action") or "").strip().upper()
    eligibility = str(exit_decision_payload.get("exit_execution_eligibility") or "").strip().upper()
    if eligibility != "APPROVED":
        raise ValueError("EXIT_DECISION_NOT_EXECUTION_ELIGIBLE")
    if action not in {ACTION_UPDATE_STOP, ACTION_TAKE_PARTIAL, ACTION_EXIT_FULL}:
        raise ValueError(f"UNSUPPORTED_EXIT_EXECUTION_ACTION:{action}")

    execution_action = {
        ACTION_UPDATE_STOP: "AMEND_PROTECTION",
        ACTION_TAKE_PARTIAL: "REDUCE_POSITION",
        ACTION_EXIT_FULL: "CLOSE_POSITION",
    }[action]
    quantity_fraction = "1.0"
    stop_price = ""
    if action == ACTION_UPDATE_STOP:
        stop_price = str(exit_decision_payload.get("proposed_stop_price") or "").strip()
    elif action == ACTION_TAKE_PARTIAL:
        quantity_fraction = str(exit_decision_payload.get("partial_exit_fraction") or "0.50").strip()
    return {
        "schema_id": "exit_execution_request",
        "schema_version": "v1",
        "day_utc": parse_day_utc_v1(day_utc),
        "position_id": str(position_id).strip(),
        "decision_id": str(exit_decision_payload.get("decision_id") or "").strip(),
        "origin": str(exit_decision_payload.get("origin") or "").strip(),
        "risk_basis": str(exit_decision_payload.get("risk_basis") or "").strip(),
        "execution_action": execution_action,
        "order_type": "STOP" if action == ACTION_UPDATE_STOP else "MARKET",
        "quantity_fraction": quantity_fraction,
        "stop_price": stop_price,
        "decision_action": action,
        "scoring_eligibility": str(exit_decision_payload.get("scoring_eligibility") or "").strip(),
        "reason_codes": list(exit_decision_payload.get("reason_codes") or []),
        "produced_at_utc": now_utc_iso_v1(),
        "producer": producer_block_v1(
            module="constellation_2/common/exit_execution_request_v1.py",
            git_sha=repo_git_sha_v1(),
        ),
        "provenance_refs": _normalize_provenance_refs(provenance_refs),
    }


def write_exit_execution_request_v1(*, truth_root: Path, payload: Mapping[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_exit_execution_request_path(
            truth_root=truth_root,
            day_utc=str(payload.get("day_utc") or ""),
            position_id=str(payload.get("position_id") or ""),
        ),
        payload=dict(payload),
        schema_relpath=EXIT_EXECUTION_REQUEST_SCHEMA_RELPATH,
        volatile_field_names=("produced_at_utc",),
    )
