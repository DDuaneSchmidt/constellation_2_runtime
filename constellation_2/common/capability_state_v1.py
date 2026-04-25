from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_paper_trading_posture_ref_v1,
    read_startup_materialization_ref_v1,
    resolve_authoritative_repo_root_v1,
    resolve_fact_plane_truth_root_v1,
    sha256_file_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance, resolve_truth_sleeves_root
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    resolve_canonical_governed_sleeve_truth_root,
    resolve_governed_account_binding,
    resolve_governed_sleeve_truth_bindings,
    resolve_pointer_bound_handshake_state,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
CAPABILITY_POLICY_REGISTRY_RELPATH = "governance/02_REGISTRIES/CAPABILITY_POLICY_REGISTRY_V1.json"
CAPABILITY_POLICY_REGISTRY_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REGISTRIES/capability_policy_registry.v1.schema.json"
CAPABILITY_STATE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/capability_state.v1.schema.json"

CORE_GATE_IDS: Tuple[str, ...] = (
    "capital_risk_envelope_v2",
    "correlation_envelope_gate_v1",
    "liquidity_slippage_gate_v1",
    "operator_daily_gate_v3",
)
PRODUCTION_CERT_GATE_IDS: Tuple[str, ...] = (
    "feed_attestation_gate_v1",
    "heartbeat_gate_v1",
    "replay_certification_gate_v1",
)
ALL_TRACKED_GATE_IDS: Tuple[str, ...] = CORE_GATE_IDS + PRODUCTION_CERT_GATE_IDS


def _verdict_artifact_path(*, sleeve_truth_root: Path, family: str, day_utc: str, filename: str) -> Path:
    return (Path(sleeve_truth_root).resolve() / "reports" / family / str(day_utc).strip() / filename).resolve()


def resolve_capability_state_path(*, truth_root: Path, day_utc: str) -> Path:
    return (Path(truth_root).resolve() / "reports" / "capability_state_v1" / str(day_utc).strip() / "capability_state.v1.json").resolve()


def resolve_paper_policy_verdict_path(*, truth_root: Path, day_utc: str) -> Path:
    return (Path(truth_root).resolve() / "reports" / "paper_policy_verdict_v1" / str(day_utc).strip() / "paper_policy_verdict.v1.json").resolve()


def resolve_production_policy_verdict_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "production_policy_verdict_v1"
        / str(day_utc).strip()
        / "production_policy_verdict.v1.json"
    ).resolve()


def resolve_policy_diff_path(*, truth_root: Path, day_utc: str) -> Path:
    return (Path(truth_root).resolve() / "reports" / "policy_diff_v1" / str(day_utc).strip() / "policy_diff.v1.json").resolve()


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _artifact_ref(*, artifact_family: str, path: Path) -> Dict[str, str]:
    return {
        "artifact_family": artifact_family,
        "artifact_path": str(path),
        "artifact_sha256": sha256_file_v1(path),
    }


def _missing_artifact_ref(*, artifact_family: str, path: Path) -> Dict[str, str]:
    return {
        "artifact_family": artifact_family,
        "artifact_path": str(path),
        "artifact_sha256": "0" * 64,
    }


def _release_metadata() -> Dict[str, str]:
    release_id = "UNKNOWN"
    git_sha = "UNKNOWN"
    try:
        provenance = resolve_release_provenance()
        if isinstance(provenance, dict):
            release_id = str(provenance.get("release_id") or release_id).strip() or "UNKNOWN"
            git_sha = str(provenance.get("git_sha") or git_sha).strip() or "UNKNOWN"
    except Exception:
        pass
    if git_sha == "UNKNOWN":
        try:
            git_sha = (
                subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
                .decode("utf-8")
                .strip()
            )
        except Exception:
            git_sha = "UNKNOWN"
    return {"release_id": release_id, "git_sha": git_sha}


def _generated_at_utc(day_utc: str) -> str:
    return f"{str(day_utc).strip()}T00:00:00Z"


def _load_gate_hierarchy(repo_root: Path) -> Dict[str, Dict[str, Any]]:
    path = (Path(repo_root).resolve() / "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json").resolve()
    payload = _read_json(path)
    gates = payload.get("gates")
    if not isinstance(gates, list):
        raise ValueError(f"GATE_HIERARCHY_INVALID:path={path}")
    out: Dict[str, Dict[str, Any]] = {}
    for row in gates:
        if not isinstance(row, dict):
            continue
        gate_id = str(row.get("gate_id") or "").strip()
        if gate_id:
            out[gate_id] = row
    return out


def _load_capability_policy_registry(repo_root: Path) -> tuple[Dict[str, Any], Path, str]:
    path = (Path(repo_root).resolve() / CAPABILITY_POLICY_REGISTRY_RELPATH).resolve()
    payload = _read_json(path)
    return payload, path, sha256_file_v1(path)


def _resolve_primary_binding(*, repo_root: Path, environment: str, ib_account: str) -> Any:
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=Path(repo_root).resolve(),
        environment=str(environment).strip().upper(),
        requested_ib_account=str(ib_account).strip(),
    )
    for binding in bindings:
        if str(binding.sleeve_id).strip().upper() == "PRIMARY":
            return binding
    if not bindings:
        raise ValueError("NO_GOVERNED_SLEEVE_TRUTH_BINDINGS")
    return bindings[0]


def _gate_artifact_path(*, sleeve_truth_root: Path, gate_id: str, gate_relpaths: Dict[str, Dict[str, Any]], day_utc: str) -> Path:
    gate_row = gate_relpaths.get(gate_id) or {}
    rel = str(gate_row.get("artifact_relpath") or "").strip().lstrip("/")
    if not rel:
        return (Path(sleeve_truth_root).resolve() / "reports" / gate_id / str(day_utc).strip() / "__MISSING_RELPATH__").resolve()
    return (Path(sleeve_truth_root).resolve() / "reports" / gate_id / str(day_utc).strip() / rel).resolve()


def _capability_row(
    *,
    capability_id: str,
    status: str,
    kind: str,
    reason_codes: Iterable[str],
    source_artifacts: Iterable[Dict[str, str]],
    details: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "capability_id": capability_id,
        "status": str(status).strip().upper(),
        "kind": str(kind).strip(),
        "reason_codes": sorted({str(code).strip() for code in reason_codes if str(code).strip()}),
        "source_artifacts": list(source_artifacts),
        "details": details,
    }


def derive_capability_state_payload(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    ib_account: str,
    environment: str = "PAPER",
) -> Dict[str, Any]:
    resolved_repo_root = Path(repo_root).resolve()
    authoritative_repo_root = resolve_authoritative_repo_root_v1(resolved_repo_root)
    resolved_truth_root = resolve_decision_truth_root_v1(truth_root, repo_root=resolved_repo_root)
    env = str(environment).strip().upper()
    day = str(day_utc).strip()
    account = str(ib_account).strip()
    release_metadata = _release_metadata()
    gate_relpaths = _load_gate_hierarchy(resolved_repo_root)
    registry_payload, registry_path, registry_sha = _load_capability_policy_registry(resolved_repo_root)

    capability_rows: List[Dict[str, Any]] = []
    source_manifest: List[Dict[str, str]] = [_artifact_ref(artifact_family="capability_policy_registry_v1", path=registry_path)]

    try:
        account_binding = resolve_governed_account_binding(
            repo_root=authoritative_repo_root,
            environment=env,
            requested_ib_account=account,
        )
        primary_binding = _resolve_primary_binding(repo_root=authoritative_repo_root, environment=env, ib_account=account)
        row = _capability_row(
            capability_id="account_binding_valid",
            status="PASS",
            kind="HARD_OPERATIONAL_FACT",
            reason_codes=[],
            source_artifacts=[
                _artifact_ref(artifact_family="ib_account_registry_v1", path=account_binding.account_registry_path),
                _artifact_ref(artifact_family="sleeve_registry_v1", path=account_binding.sleeve_registry_path),
            ],
            details={
                "environment": env,
                "ib_account": account,
                "sleeve_id": str(primary_binding.sleeve_id),
                "truth_root": str(primary_binding.truth_root),
            },
        )
    except Exception as exc:
        row = _capability_row(
            capability_id="account_binding_valid",
            status="FAIL",
            kind="HARD_OPERATIONAL_FACT",
            reason_codes=[f"ACCOUNT_BINDING_INVALID:{type(exc).__name__}:{exc}"],
            source_artifacts=[],
            details={"environment": env, "ib_account": account},
        )
        primary_binding = None
    capability_rows.append(row)
    source_manifest.extend(row["source_artifacts"])

    startup_path = (resolved_truth_root / "reports" / "startup_materialization_v1" / day / "startup_materialization.v1.json").resolve()
    try:
        startup_ref = read_startup_materialization_ref_v1(truth_root=resolved_truth_root, day_utc=day)
        startup_status = str(startup_ref.payload.get("status") or "").strip().upper()
        startup_reason_codes = list(startup_ref.payload.get("blocking_codes") or [])
        row = _capability_row(
            capability_id="startup_materialization_ready",
            status="PASS" if startup_status == "SUCCESS" else "FAIL",
            kind="HARD_OPERATIONAL_FACT",
            reason_codes=startup_reason_codes if startup_status != "SUCCESS" else [],
            source_artifacts=[_artifact_ref(artifact_family="startup_materialization_v1", path=startup_ref.path)],
            details={"startup_status": startup_status},
        )
    except Exception as exc:
        row = _capability_row(
            capability_id="startup_materialization_ready",
            status="UNKNOWN",
            kind="HARD_OPERATIONAL_FACT",
            reason_codes=[f"STARTUP_MATERIALIZATION_UNAVAILABLE:{type(exc).__name__}:{exc}"],
            source_artifacts=[_missing_artifact_ref(artifact_family="startup_materialization_v1", path=startup_path)],
            details={},
        )
    capability_rows.append(row)
    source_manifest.extend(row["source_artifacts"])

    posture_path = (resolved_truth_root / "reports" / "paper_trading_posture_v1" / day / "paper_trading_posture.v1.json").resolve()
    try:
        posture_ref = read_paper_trading_posture_ref_v1(truth_root=resolved_truth_root, day_utc=day)
        posture_payload = posture_ref.payload
        posture_ready = (
            str(posture_payload.get("posture_status") or "").strip().upper() == "ENABLED"
            and bool(posture_payload.get("system_ready") is True)
            and str(posture_payload.get("freshness_verdict") or "").strip().upper() == "CURRENT"
            and str(posture_payload.get("linkage_verdict") or "").strip().upper() == "LINKED"
        )
        row = _capability_row(
            capability_id="paper_trading_posture_ready",
            status="PASS" if posture_ready else "FAIL",
            kind="SYNTHESIZED_SUMMARY",
            reason_codes=list(posture_payload.get("blocking_codes") or []),
            source_artifacts=[_artifact_ref(artifact_family="paper_trading_posture_v1", path=posture_ref.path)],
            details={
                "posture_status": str(posture_payload.get("posture_status") or "").strip().upper(),
                "system_ready": bool(posture_payload.get("system_ready") is True),
                "freshness_verdict": str(posture_payload.get("freshness_verdict") or "").strip().upper(),
                "linkage_verdict": str(posture_payload.get("linkage_verdict") or "").strip().upper(),
            },
        )
    except Exception as exc:
        row = _capability_row(
            capability_id="paper_trading_posture_ready",
            status="UNKNOWN",
            kind="SYNTHESIZED_SUMMARY",
            reason_codes=[f"PAPER_TRADING_POSTURE_UNAVAILABLE:{type(exc).__name__}:{exc}"],
            source_artifacts=[_missing_artifact_ref(artifact_family="paper_trading_posture_v1", path=posture_path)],
            details={},
        )
    capability_rows.append(row)
    source_manifest.extend(row["source_artifacts"])

    handshake_truth_root = Path(primary_binding.truth_root).resolve() if primary_binding is not None else (
        resolve_truth_sleeves_root().resolve() / "PRIMARY" / env
    ).resolve()
    handshake_pointer_path = (handshake_truth_root / "ib_api_handshake" / "latest_pointer.v1.json").resolve()
    handshake_artifact_path = (handshake_truth_root / "ib_api_handshake" / day / "ib_api_handshake.v1.json").resolve()
    try:
        handshake = resolve_pointer_bound_handshake_state(
            truth_root=handshake_truth_root,
            day_utc=day,
            environment=env,
            ib_account=account,
        )
        handshake_payload = _read_json(handshake.handshake_path)
        handshake_ready = bool(handshake_payload.get("ok") is True) and str(handshake_payload.get("status") or "").strip().upper() == "OK"
        row = _capability_row(
            capability_id="broker_connectivity_available",
            status="PASS" if handshake_ready else "FAIL",
            kind="HARD_OPERATIONAL_FACT",
            reason_codes=list(handshake_payload.get("reason_codes") or []),
            source_artifacts=[
                _artifact_ref(artifact_family="ib_api_handshake_latest_pointer_v1", path=handshake.pointer_path),
                _artifact_ref(artifact_family="ib_api_handshake_v1", path=handshake.handshake_path),
            ],
            details={
                "handshake_status": str(handshake_payload.get("status") or "").strip().upper(),
                "handshake_ok": bool(handshake_payload.get("ok") is True),
            },
        )
    except Exception as exc:
        row = _capability_row(
            capability_id="broker_connectivity_available",
            status="FAIL",
            kind="HARD_OPERATIONAL_FACT",
            reason_codes=[f"BROKER_CONNECTIVITY_UNAVAILABLE:{type(exc).__name__}:{exc}"],
            source_artifacts=[
                _missing_artifact_ref(artifact_family="ib_api_handshake_latest_pointer_v1", path=handshake_pointer_path),
                _missing_artifact_ref(artifact_family="ib_api_handshake_v1", path=handshake_artifact_path),
            ],
            details={},
        )
    capability_rows.append(row)
    source_manifest.extend(row["source_artifacts"])

    sleeve_truth_root = handshake_truth_root
    production_cert_truth_root = sleeve_truth_root
    if primary_binding is not None:
        try:
            production_cert_truth_root = resolve_canonical_governed_sleeve_truth_root(primary_binding)
        except Exception:
            production_cert_truth_root = sleeve_truth_root

    for capability_id, verdict_family, filename, accepted_statuses in (
        ("startup_authorization_gate_set_ready", "authorization_gate_verdict_v1", "authorization_gate_verdict.v1.json", {"PASS", "BOOTSTRAP_PASS"}),
        ("economic_health_gate_set_complete", "economic_health_gate_verdict_v1", "economic_health_gate_verdict.v1.json", {"PASS"}),
    ):
        verdict_path = _verdict_artifact_path(
            sleeve_truth_root=sleeve_truth_root,
            family=verdict_family,
            day_utc=day,
            filename=filename,
        )
        if not verdict_path.exists() or not verdict_path.is_file():
            row = _capability_row(
                capability_id=capability_id,
                status="FAIL",
                kind="SYNTHESIZED_SUMMARY",
                reason_codes=[f"{verdict_family}:MISSING"],
                source_artifacts=[_missing_artifact_ref(artifact_family=verdict_family, path=verdict_path)],
                details={"verdict_family": verdict_family, "sleeve_truth_root": str(sleeve_truth_root)},
            )
        else:
            verdict_payload = _read_json(verdict_path)
            verdict_status = str(verdict_payload.get("status") or "").strip().upper() or "UNKNOWN"
            reason_codes = [
                f"{verdict_family}:{verdict_status}",
                *[f"{verdict_family}:{code}" for code in (verdict_payload.get("reason_codes") or []) if str(code).strip()],
            ]
            row = _capability_row(
                capability_id=capability_id,
                status="PASS" if verdict_status in accepted_statuses else "FAIL",
                kind="SYNTHESIZED_SUMMARY",
                reason_codes=[] if verdict_status in accepted_statuses else reason_codes,
                source_artifacts=[_artifact_ref(artifact_family=verdict_family, path=verdict_path)],
                details={
                    "verdict_family": verdict_family,
                    "verdict_status": verdict_status,
                    "sleeve_truth_root": str(sleeve_truth_root),
                },
            )
        capability_rows.append(row)
        source_manifest.extend(row["source_artifacts"])

    for capability_id, gate_ids in (
        ("core_sleeve_gate_set_ready", CORE_GATE_IDS),
        ("production_certification_gate_set_complete", PRODUCTION_CERT_GATE_IDS),
    ):
        selected_sleeve_truth_root = (
            production_cert_truth_root if capability_id == "production_certification_gate_set_complete" else sleeve_truth_root
        )
        gate_refs: List[Dict[str, str]] = []
        gate_failures: List[str] = []
        gate_statuses: Dict[str, str] = {}
        for gate_id in gate_ids:
            gate_path = _gate_artifact_path(
                sleeve_truth_root=selected_sleeve_truth_root,
                gate_id=gate_id,
                gate_relpaths=gate_relpaths,
                day_utc=day,
            )
            if not gate_path.exists() or not gate_path.is_file():
                gate_refs.append(_missing_artifact_ref(artifact_family=gate_id, path=gate_path))
                gate_statuses[gate_id] = "MISSING"
                gate_failures.append(f"{gate_id}:MISSING")
                continue
            gate_payload = _read_json(gate_path)
            gate_status = str(gate_payload.get("status") or "").strip().upper() or "UNKNOWN"
            gate_statuses[gate_id] = gate_status
            gate_refs.append(_artifact_ref(artifact_family=gate_id, path=gate_path))
            if gate_status not in {"PASS", "OK"}:
                gate_failures.append(f"{gate_id}:{gate_status}")
                gate_failures.extend(
                    [f"{gate_id}:{code}" for code in (gate_payload.get("reason_codes") or []) if str(code).strip()]
                )
        row = _capability_row(
            capability_id=capability_id,
            status="PASS" if not gate_failures else "FAIL",
            kind="SYNTHESIZED_SUMMARY",
            reason_codes=gate_failures,
            source_artifacts=gate_refs,
            details={"gate_statuses": gate_statuses, "sleeve_truth_root": str(selected_sleeve_truth_root)},
        )
        capability_rows.append(row)
        source_manifest.extend(row["source_artifacts"])

    overall_status = "PASS" if all(str(row.get("status") or "") == "PASS" for row in capability_rows) else "FAIL"
    source_manifest_sorted = sorted(
        {
            (row["artifact_family"], row["artifact_path"], row["artifact_sha256"]): row
            for row in source_manifest
        }.values(),
        key=lambda row: (row["artifact_family"], row["artifact_path"]),
    )

    return {
        "schema_id": "capability_state",
        "schema_version": "v1",
        "day_utc": day,
        "environment": env,
        "ib_account": account,
        "sleeve_id": str(getattr(primary_binding, "sleeve_id", "PRIMARY")),
        "overall_status": overall_status,
        "capabilities": capability_rows,
        "source_artifacts": source_manifest_sorted,
        "registry_ref": {
            "artifact_path": str(registry_path),
            "artifact_sha256": registry_sha,
            "schema_id": str(registry_payload.get("schema_id") or ""),
            "schema_version": str(registry_payload.get("schema_version") or ""),
        },
        "release_id": release_metadata["release_id"],
        "git_sha": release_metadata["git_sha"],
        "generated_at_utc": _generated_at_utc(day),
    }


def write_capability_state_v1(
    *,
    truth_root: Path,
    payload: Dict[str, Any],
) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_capability_state_path(truth_root=truth_root, day_utc=str(payload.get("day_utc") or "").strip()),
        payload=payload,
        schema_relpath=CAPABILITY_STATE_SCHEMA_RELPATH,
        volatile_field_names=("generated_at_utc",),
    )
