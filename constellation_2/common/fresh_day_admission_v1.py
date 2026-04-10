from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.common.capability_state_v1 import resolve_paper_policy_verdict_path
from constellation_2.common.next_day_readiness_probe_v1 import resolve_next_day_readiness_probe_path
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import resolve_trading_day_state_machine_path
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance
from constellation_2.common.runtime_path_authority_v1 import (
    load_runtime_path_authority_v1,
    resolve_decision_truth_root_v1,
    resolve_runtime_path_authority_snapshot_v1,
)


FRESH_DAY_ADMISSION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/fresh_day_admission.v1.schema.json"
NEXT_DAY_READINESS_PROBE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/next_day_readiness_probe.v1.schema.json"
PAPER_POLICY_VERDICT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_policy_verdict.v1.schema.json"
TRADE_SUBMIT_READINESS_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json"
TRADING_DAY_STATE_MACHINE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_state_machine.v1.schema.json"


def resolve_fresh_day_admission_path(*, truth_root: Path, target_day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "fresh_day_admission_v1"
        / str(target_day_utc).strip()
        / "fresh_day_admission.v1.json"
    ).resolve()


def _day_minus_one(day_utc: str) -> str:
    return (date.fromisoformat(str(day_utc).strip()) - timedelta(days=1)).isoformat()


def _artifact_ref(path: Path) -> Dict[str, str]:
    return {
        "artifact_path": str(path),
        "artifact_sha256": sha256_file_v1(path),
    }


def _read_optional_surface(path: Path, schema_relpath: str) -> SurfaceRefV1 | None:
    if not path.exists() or not path.is_file():
        return None
    return read_validated_surface_v1(path=path, schema_relpath=schema_relpath)


def _trade_submit_history_path(*, truth_root: Path, day_utc: str, environment: str, ib_account: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "trade_submit_readiness_c2_v1"
        / "_history"
        / str(environment).strip().upper()
        / str(ib_account).strip()
        / str(day_utc).strip()
        / "status.json"
    ).resolve()


def _materialized_artifact_row(*, artifact_id: str, ref: SurfaceRefV1, artifact_status: str) -> Dict[str, str]:
    return {
        "artifact_id": artifact_id,
        "artifact_path": str(ref.path),
        "artifact_sha256": ref.sha256,
        "artifact_status": artifact_status,
    }


def derive_fresh_day_admission_payload(
    *,
    repo_root: Path,
    truth_root: Path,
    target_day_utc: str,
    environment: str,
    ib_account: str,
) -> Dict[str, Any]:
    resolved_truth_root = resolve_decision_truth_root_v1(truth_root, repo_root=repo_root)
    authority = load_runtime_path_authority_v1(repo_root=repo_root)
    authority_snapshot = resolve_runtime_path_authority_snapshot_v1(repo_root=repo_root)
    reference_day_utc = _day_minus_one(target_day_utc)
    probe_path = resolve_next_day_readiness_probe_path(truth_root=resolved_truth_root, target_day_utc=target_day_utc)
    paper_policy_path = resolve_paper_policy_verdict_path(truth_root=resolved_truth_root, day_utc=target_day_utc)
    trade_submit_path = _trade_submit_history_path(
        truth_root=resolved_truth_root,
        day_utc=target_day_utc,
        environment=environment,
        ib_account=ib_account,
    )
    trading_day_path = resolve_trading_day_state_machine_path(truth_root=resolved_truth_root, day_utc=target_day_utc)

    probe_ref = _read_optional_surface(probe_path, NEXT_DAY_READINESS_PROBE_SCHEMA_RELPATH)
    paper_policy_ref = _read_optional_surface(paper_policy_path, PAPER_POLICY_VERDICT_SCHEMA_RELPATH)
    trade_submit_ref = _read_optional_surface(trade_submit_path, TRADE_SUBMIT_READINESS_SCHEMA_RELPATH)
    trading_day_ref = _read_optional_surface(trading_day_path, TRADING_DAY_STATE_MACHINE_SCHEMA_RELPATH)

    required_target_day_artifacts = [
        {
            "artifact_id": "paper_policy_verdict_v1",
            "artifact_path": str(paper_policy_path),
            "requirement_class": "REQUIRED_PRE_ADMISSION",
        },
        {
            "artifact_id": "trade_submit_readiness_c2_v1",
            "artifact_path": str(trade_submit_path),
            "requirement_class": "REQUIRED_PRE_ADMISSION",
        },
        {
            "artifact_id": "trading_day_state_machine_v1",
            "artifact_path": str(trading_day_path),
            "requirement_class": "REQUIRED_PRE_ADMISSION",
        },
    ]

    materialized_target_day_artifacts: List[Dict[str, str]] = []
    blocking_items: List[Dict[str, Any]] = []
    missing_required_artifacts: List[Dict[str, str]] = []
    required_artifacts_ready = True

    if paper_policy_ref is None:
        required_artifacts_ready = False
        missing_required_artifacts.append(
            {"artifact_id": "paper_policy_verdict_v1", "artifact_path": str(paper_policy_path)}
        )
    else:
        paper_status = str(paper_policy_ref.payload.get("overall_status") or "").strip().upper()
        materialized_target_day_artifacts.append(
            _materialized_artifact_row(
                artifact_id="paper_policy_verdict_v1",
                ref=paper_policy_ref,
                artifact_status=paper_status or "UNKNOWN",
            )
        )
        if paper_status != "PASS":
            required_artifacts_ready = False
            blocking_items.append(
                {
                    "item_id": "paper_policy_verdict_v1",
                    "reason": "TARGET_DAY_PAPER_POLICY_NOT_PASS",
                    "artifact_ref": _artifact_ref(paper_policy_ref.path),
                }
            )

    if trade_submit_ref is None:
        required_artifacts_ready = False
        missing_required_artifacts.append(
            {"artifact_id": "trade_submit_readiness_c2_v1", "artifact_path": str(trade_submit_path)}
        )
    else:
        trade_submit_state = str(trade_submit_ref.payload.get("state") or "").strip().upper()
        trade_submit_ok = bool(trade_submit_ref.payload.get("ok") is True)
        trade_status = "PASS" if trade_submit_state == "OK" and trade_submit_ok else "FAIL"
        materialized_target_day_artifacts.append(
            _materialized_artifact_row(
                artifact_id="trade_submit_readiness_c2_v1",
                ref=trade_submit_ref,
                artifact_status=trade_status,
            )
        )
        if trade_status != "PASS":
            required_artifacts_ready = False
            blocking_items.append(
                {
                    "item_id": "trade_submit_readiness_c2_v1",
                    "reason": "TARGET_DAY_TRADE_SUBMIT_NOT_OK",
                    "artifact_ref": _artifact_ref(trade_submit_ref.path),
                }
            )

    if trading_day_ref is None:
        required_artifacts_ready = False
        missing_required_artifacts.append(
            {"artifact_id": "trading_day_state_machine_v1", "artifact_path": str(trading_day_path)}
        )
    else:
        trading_day_decision = str(trading_day_ref.payload.get("final_start_decision") or "").strip().upper()
        trading_day_status = "PASS" if trading_day_decision == "READY_NOW" else "FAIL"
        materialized_target_day_artifacts.append(
            _materialized_artifact_row(
                artifact_id="trading_day_state_machine_v1",
                ref=trading_day_ref,
                artifact_status=trading_day_status,
            )
        )
        if trading_day_status != "PASS":
            required_artifacts_ready = False
            blocking_items.append(
                {
                    "item_id": "trading_day_state_machine_v1",
                    "reason": "TARGET_DAY_TRADING_DAY_NOT_READY_NOW",
                    "artifact_ref": _artifact_ref(trading_day_ref.path),
                }
            )

    probe_status = "MISSING"
    if probe_ref is None:
        if not required_artifacts_ready:
            blocking_items.append(
                {
                    "item_id": "next_day_readiness_probe_v1",
                    "reason": "NEXT_DAY_READINESS_PROBE_NOT_MATERIALIZED",
                    "artifact_path": str(probe_path),
                }
            )
    else:
        probe_status = str(probe_ref.payload.get("probe_status") or "").strip().upper()
        if not required_artifacts_ready and probe_status != "READY":
            blocking_items.append(
                {
                    "item_id": "next_day_readiness_probe_v1",
                    "reason": f"NEXT_DAY_READINESS_PROBE_{probe_status or 'INVALID'}",
                    "artifact_ref": _artifact_ref(probe_ref.path),
                }
            )

    release = resolve_release_provenance()
    return {
        "schema_id": "fresh_day_admission",
        "schema_version": "v1",
        "target_day_utc": str(target_day_utc).strip(),
        "reference_day_utc": reference_day_utc,
        "admission_status": "ADMIT" if required_artifacts_ready else "BLOCKED",
        "probe_status": probe_status,
        "probe_artifact_path": str(probe_path),
        "blocking_items": blocking_items,
        "required_target_day_artifacts": required_target_day_artifacts,
        "materialized_target_day_artifacts": materialized_target_day_artifacts,
        "missing_required_artifacts": missing_required_artifacts,
        "path_authority_snapshot": authority_snapshot,
        "release_id": str(release.get("release_id") or "").strip(),
        "git_sha": str(release.get("git_sha") or "").strip(),
        "truth_root": str(resolved_truth_root),
        "truth_sleeves_root": str(authority.canonical_runtime_truth_sleeves_root),
        "evaluated_at_utc": f"{target_day_utc}T00:00:00Z",
    }


def write_fresh_day_admission_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_fresh_day_admission_path(
            truth_root=truth_root,
            target_day_utc=str(payload.get("target_day_utc") or "").strip(),
        ),
        payload=payload,
        schema_relpath=FRESH_DAY_ADMISSION_SCHEMA_RELPATH,
        volatile_field_names=("evaluated_at_utc",),
    )
