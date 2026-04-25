from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    now_utc_iso_v1,
    producer_block_v1,
    read_trading_day_state_machine_ref_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_summary_path,
    resolve_session_readiness_refresh_path,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_summary.v1.schema.json"


def build_operator_summary(*, summary_kind: str, day_utc: str, truth_root: Path) -> dict[str, Any]:
    kind = str(summary_kind or "").strip().lower()
    if kind != "preopen":
        raise ValueError(f"UNSUPPORTED_OPERATOR_SUMMARY_KIND:{summary_kind}")

    truth_root = resolve_fact_plane_truth_root_v1(truth_root)
    state_machine_ref = read_trading_day_state_machine_ref_v1(truth_root=truth_root, day_utc=day_utc)
    state_machine_payload = dict(state_machine_ref.payload)
    authority_result = state_machine_payload.get("supporting_session_authority")
    if not isinstance(authority_result, dict):
        raise ValueError("TRADING_DAY_STATE_MACHINE_SUPPORTING_SESSION_AUTHORITY_MISSING")
    first_true_blocker = state_machine_payload.get("first_true_blocker")
    if not isinstance(first_true_blocker, dict):
        raise ValueError("TRADING_DAY_STATE_MACHINE_FIRST_TRUE_BLOCKER_MISSING")
    refresh_path = resolve_session_readiness_refresh_path(truth_root=truth_root, day_utc=day_utc)

    blocking_codes = {
        str(code).strip()
        for code in (state_machine_payload.get("blocking_codes") or [])
        if str(code).strip()
    }
    supporting_regeneration_results = (
        state_machine_payload.get("supporting_regeneration_results")
        if isinstance(state_machine_payload.get("supporting_regeneration_results"), list)
        else []
    )
    startup_row = next(
        (
            row
            for row in supporting_regeneration_results
            if isinstance(row, dict) and str(row.get("logical_name") or "").strip() == "startup_proof_validation_v1"
        ),
        {},
    )
    startup_proof_status = str(startup_row.get("status") or "").strip().upper() or "NOT_EVALUATED"
    if startup_proof_status == "NOT_EVALUATED":
        blocking_codes.add("OPERATOR_SUMMARY_STARTUP_PROOF_NOT_EVALUATED")

    return {
        "schema_id": "operator_summary",
        "schema_version": "v1",
        "authority_scope": "DERIVED_ONLY_VIEW",
        "binding_classification": "LEGACY_DERIVED_ONLY",
        "summary_kind": "preopen",
        "day_utc": str(day_utc).strip(),
        "produced_at_utc": now_utc_iso_v1(),
        "producer": producer_block_v1(module="constellation_2/common/operator_summary_v1.py"),
        "control_plane_ref": str(state_machine_ref.path),
        "control_plane_id": str(state_machine_payload.get("state_machine_id") or "").strip(),
        "state_machine_id": str(state_machine_payload.get("state_machine_id") or "").strip(),
        "final_start_decision": str(state_machine_payload.get("final_start_decision") or "").strip().upper(),
        "first_true_blocker_code": str(first_true_blocker.get("first_true_blocker_code") or "").strip(),
        "ledger_ref": str(authority_result.get("paper_session_ledger_path") or "").strip(),
        "ledger_id": str(authority_result.get("ledger_id") or "").strip(),
        "authority_status": str(authority_result.get("ledger_authority_status") or "").strip().upper() or "NOT_EVALUATED",
        "submission_authorized": bool(authority_result.get("submission_authorized") is True),
        "summary_state": str(state_machine_payload.get("final_start_decision") or "").strip().upper(),
        "startup_proof_validation_ref": str(startup_row.get("path") or "").strip(),
        "startup_proof_status": startup_proof_status,
        "session_readiness_refresh_ref": str(refresh_path) if refresh_path.exists() and refresh_path.is_file() else "",
        "non_authority_notice": (
            "Derived only from trading_day_state_machine_v1. "
            "Do not treat this summary as an independent authority surface."
        ),
        "blocking_codes": sorted(blocking_codes),
    }


def write_operator_summary(*, summary_kind: str, day_utc: str, truth_root: Path) -> SurfaceRefV1:
    truth_root = resolve_fact_plane_truth_root_v1(truth_root)
    payload = build_operator_summary(summary_kind=summary_kind, day_utc=day_utc, truth_root=truth_root)
    return atomic_write_validated_json_v1(
        path=resolve_operator_summary_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
        schema_relpath=SCHEMA_RELPATH_V1,
    )
