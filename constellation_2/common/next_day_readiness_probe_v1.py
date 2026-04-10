from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
    sha256_file_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance
from constellation_2.common.runtime_path_authority_v1 import (
    resolve_decision_truth_root_v1,
    resolve_runtime_path_authority_snapshot_v1,
)
from constellation_2.common.capability_state_v1 import (
    resolve_paper_policy_verdict_path,
    resolve_production_policy_verdict_path,
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


def _read_optional_surface(path: Path, schema_relpath: str) -> SurfaceRefV1 | None:
    if not path.exists() or not path.is_file():
        return None
    return read_validated_surface_v1(path=path, schema_relpath=schema_relpath)


def _trade_submit_path(*, truth_root: Path, day_utc: str, environment: str, ib_account: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "trade_submit_readiness_c2_v1"
        / "_history"
        / str(environment).strip().upper()
        / str(ib_account).strip()
        / str(day_utc).strip()
        / "status.json"
    ).resolve()


def _recurrence_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "recurrence_kill_gate_v1"
        / str(day_utc).strip()
        / "recurrence_kill_gate.v1.json"
    ).resolve()


def derive_next_day_readiness_probe_payload(
    *,
    repo_root: Path,
    truth_root: Path,
    target_day_utc: str,
    environment: str,
    ib_account: str,
) -> Dict[str, Any]:
    resolved_truth_root = resolve_decision_truth_root_v1(truth_root, repo_root=repo_root)
    authority_snapshot = resolve_runtime_path_authority_snapshot_v1(repo_root=repo_root)
    current_day_utc = _day_minus_one(target_day_utc)

    current_paper_ref = _read_optional_surface(
        resolve_paper_policy_verdict_path(truth_root=resolved_truth_root, day_utc=current_day_utc),
        "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_policy_verdict.v1.schema.json",
    )
    current_prod_ref = _read_optional_surface(
        resolve_production_policy_verdict_path(truth_root=resolved_truth_root, day_utc=current_day_utc),
        "governance/04_DATA/SCHEMAS/C2/REPORTS/production_policy_verdict.v1.schema.json",
    )
    current_trade_ref = _read_optional_surface(
        _trade_submit_path(
            truth_root=resolved_truth_root,
            day_utc=current_day_utc,
            environment=environment,
            ib_account=ib_account,
        ),
        TRADE_SUBMIT_READINESS_SCHEMA_RELPATH,
    )
    current_recurrence_ref = _read_optional_surface(
        _recurrence_path(truth_root=resolved_truth_root, day_utc=current_day_utc),
        RECURRENCE_KILL_GATE_SCHEMA_RELPATH,
    )

    target_paper_ref = _read_optional_surface(
        resolve_paper_policy_verdict_path(truth_root=resolved_truth_root, day_utc=target_day_utc),
        "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_policy_verdict.v1.schema.json",
    )
    target_trade_ref = _read_optional_surface(
        _trade_submit_path(
            truth_root=resolved_truth_root,
            day_utc=target_day_utc,
            environment=environment,
            ib_account=ib_account,
        ),
        TRADE_SUBMIT_READINESS_SCHEMA_RELPATH,
    )

    current_baseline_unknown = any(ref is None for ref in (current_paper_ref, current_prod_ref, current_trade_ref))
    evidence_projection: List[Dict[str, Any]] = [
        {
            "projection_id": "current_day_baseline",
            "status": (
                "UNKNOWN"
                if current_baseline_unknown
                else (
                    "PASS"
                    if str(current_paper_ref.payload.get("overall_status") or "").strip().upper() == "PASS"
                    and str(current_prod_ref.payload.get("overall_status") or "").strip().upper() == "PASS"
                    and str(current_trade_ref.payload.get("state") or "").strip().upper() == "OK"
                    and (
                        current_recurrence_ref is None
                        or str(current_recurrence_ref.payload.get("proof_status") or "").strip().upper() == "RECURRENCE_SAFE"
                    )
                    else "FAIL"
                )
            ),
            "basis": (
                []
                if current_paper_ref is None
                else [_artifact_ref(current_paper_ref.path)]
            )
            + (
                []
                if current_prod_ref is None
                else [_artifact_ref(current_prod_ref.path)]
            )
            + (
                []
                if current_trade_ref is None
                else [_artifact_ref(current_trade_ref.path)]
            )
            + ([] if current_recurrence_ref is None else [_artifact_ref(current_recurrence_ref.path)]),
        },
        {
            "projection_id": "target_day_paper_policy",
            "status": (
                str(target_paper_ref.payload.get("overall_status") or "").strip().upper()
                if target_paper_ref is not None
                else "UNKNOWN"
            ),
            "basis": [] if target_paper_ref is None else [_artifact_ref(target_paper_ref.path)],
        },
        {
            "projection_id": "target_day_trade_submit_readiness",
            "status": (
                "PASS"
                if target_trade_ref is not None
                and str(target_trade_ref.payload.get("state") or "").strip().upper() == "OK"
                else ("FAIL" if target_trade_ref is not None else "UNKNOWN")
            ),
            "basis": [] if target_trade_ref is None else [_artifact_ref(target_trade_ref.path)],
        },
    ]

    current_baseline_status = str(evidence_projection[0]["status"] or "").strip().upper()
    current_baseline_pass = current_baseline_status == "PASS"
    predicted_blocking_items: List[Dict[str, Any]] = []
    if current_baseline_status == "FAIL":
        predicted_blocking_items.append(
            {
                "item_id": "current_day_baseline",
                "reason": "CURRENT_DAY_BASELINE_NOT_GREEN",
            }
        )
    elif current_baseline_status == "UNKNOWN":
        predicted_blocking_items.append(
            {
                "item_id": "current_day_baseline",
                "reason": "CURRENT_DAY_BASELINE_NOT_MATERIALIZED",
            }
        )

    if target_paper_ref is None:
        predicted_paper_policy_status = "UNKNOWN"
        predicted_blocking_items.append(
            {"item_id": "target_day_paper_policy", "reason": "TARGET_DAY_PAPER_POLICY_NOT_MATERIALIZED"}
        )
    else:
        predicted_paper_policy_status = str(target_paper_ref.payload.get("overall_status") or "").strip().upper()
        if predicted_paper_policy_status != "PASS":
            predicted_blocking_items.append(
                {
                    "item_id": "target_day_paper_policy",
                    "reason": "TARGET_DAY_PAPER_POLICY_BLOCKED",
                }
            )

    if target_trade_ref is None:
        predicted_blocking_items.append(
            {"item_id": "target_day_trade_submit_readiness", "reason": "TARGET_DAY_TRADE_SUBMIT_NOT_MATERIALIZED"}
        )
    elif str(target_trade_ref.payload.get("state") or "").strip().upper() != "OK":
        predicted_blocking_items.append(
            {"item_id": "target_day_trade_submit_readiness", "reason": "TARGET_DAY_TRADE_SUBMIT_NOT_OK"}
        )

    if current_baseline_status == "FAIL":
        probe_status = "BLOCKED"
        confidence = "HIGH"
    elif current_baseline_status == "UNKNOWN":
        probe_status = "UNKNOWN"
        confidence = "LOW"
    elif target_paper_ref is not None and target_trade_ref is not None and predicted_paper_policy_status == "PASS":
        probe_status = "READY"
        confidence = "HIGH"
    elif target_paper_ref is not None or target_trade_ref is not None:
        probe_status = "BLOCKED"
        confidence = "HIGH"
    else:
        probe_status = "UNKNOWN"
        confidence = "LOW"

    capability_projection = [
        {
            "capability_id": "runtime_path_authority_valid",
            "status": "PASS",
            "basis": authority_snapshot,
        },
        {
            "capability_id": "current_day_baseline_green",
            "status": evidence_projection[0]["status"],
            "basis": [row["projection_id"] for row in evidence_projection[:1]],
        },
        {
            "capability_id": "target_day_policy_materialized",
            "status": "PASS" if target_paper_ref is not None else "NOT_YET_MATERIALIZED_BUT_PROVABLE",
            "basis": [evidence_projection[1]["projection_id"]],
        },
        {
            "capability_id": "target_day_submit_readiness_materialized",
            "status": "PASS" if target_trade_ref is not None else "NOT_YET_MATERIALIZED_BUT_PROVABLE",
            "basis": [evidence_projection[2]["projection_id"]],
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
