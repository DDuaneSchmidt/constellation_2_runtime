from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from constellation_2.common.economic_state_authority_v1 import run_economic_state_authority_v1
from ops.aegis.event_append_transaction_v1 import (
    contract_input_hashes_for_paths_v1,
    emit_artifact_evidence_transaction_v1,
    sha256_file_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1

PRODUCER_VERSION = "v1"
ACTUAL_PRODUCER_BY_SCHEMA = {
    "C2_CASH_LEDGER_SNAPSHOT_V1": "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
    "C2_POSITIONS_SNAPSHOT_V5": "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5",
    "C2_POSITION_LIFECYCLE_SNAPSHOT": "ops/tools/run_position_lifecycle_snapshot_v2.py",
    "C2_ACCOUNTING_NAV_V2": "ops/tools/run_accounting_nav_v2_day_v1.py",
    "economic_state_build": "constellation_2.common.economic_state_authority_v1",
    "economic_state_package": "constellation_2.common.economic_state_authority_v1",
}


def git_sha_v1(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "-C", str(repo_root), "rev-parse", "HEAD"])
        return out.decode("utf-8").strip()
    except Exception:
        return "0" * 40


def read_json_v1(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def account_source_summary_v1(*, truth_root: Path, day_utc: str, result: dict[str, Any]) -> dict[str, Any]:
    build_obj = result.get("build_obj") if isinstance(result.get("build_obj"), dict) else {}
    refs = {}
    for row in build_obj.get("dependency_results") or []:
        if isinstance(row, dict):
            refs[str(row.get("dependency_id") or "")] = row
    cash_text = str((refs.get("cash_ledger_snapshot_v1") or {}).get("path") or "").strip()
    positions_text = str((refs.get("positions_snapshot_v5") or {}).get("path") or "").strip()
    nav_text = str((refs.get("accounting_nav_v2") or {}).get("path") or "").strip()
    cash_path = Path(cash_text) if cash_text else Path()
    positions_path = Path(positions_text) if positions_text else Path()
    nav_path = Path(nav_text) if nav_text else Path()
    cash = read_json_v1(cash_path) if cash_text and cash_path.exists() and cash_path.is_file() else {}
    positions = read_json_v1(positions_path) if positions_text and positions_path.exists() and positions_path.is_file() else {}
    nav = read_json_v1(nav_path) if nav_text and nav_path.exists() and nav_path.is_file() else {}
    return {
        "cash_source_type": str(cash.get("source_type") or "MISSING"),
        "positions_source_type": str(positions.get("source_type") or "MISSING"),
        "nav_source_type": str(nav.get("source_type") or "MISSING"),
        "cash_path": str(cash_path) if str(cash_path) != "." else "",
        "positions_path": str(positions_path) if str(positions_path) != "." else "",
        "nav_path": str(nav_path) if str(nav_path) != "." else "",
        "cash_hash": sha256_file_v1(cash_path) if cash_text and cash_path.exists() and cash_path.is_file() else "",
        "positions_hash": sha256_file_v1(positions_path) if positions_text and positions_path.exists() and positions_path.is_file() else "",
        "nav_hash": sha256_file_v1(nav_path) if nav_text and nav_path.exists() and nav_path.is_file() else "",
        "runtime_evaluation_hash": str(read_canonical_runtime_evaluation_v1(truth_root=truth_root, day_utc=day_utc).get("deterministic_output_hash") or ""),
    }


def _emit_for_path(
    *,
    truth_root: Path,
    day_utc: str,
    path: Path,
    payload: dict[str, Any],
    validation_status: str,
    run_id: str,
    git_sha: str,
    input_paths: list[Path],
) -> list[dict[str, Any]]:
    schema_id = str(payload.get("schema_id") or "")
    producer_id = ACTUAL_PRODUCER_BY_SCHEMA.get(schema_id)
    if not producer_id:
        return []
    source_hash = str(payload.get("source_hash") or payload.get("source_positions_hash") or "")
    if not source_hash and input_paths and input_paths[-1].exists() and input_paths[-1].is_file():
        source_hash = sha256_file_v1(input_paths[-1])
    return emit_artifact_evidence_transaction_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        artifact_path=path,
        payload=payload,
        producer_id=producer_id,
        producer_version=PRODUCER_VERSION,
        run_id=run_id,
        created_at_utc=str(payload.get("produced_utc") or payload.get("generated_utc") or payload.get("sealed_utc") or f"{day_utc}T00:00:00Z"),
        git_sha=git_sha,
        input_hashes=contract_input_hashes_for_paths_v1(
            input_paths,
            extra={
                "runtime_evaluation_hash": read_canonical_runtime_evaluation_v1(truth_root=truth_root, day_utc=day_utc).get("deterministic_output_hash", ""),
                "source_hash": source_hash,
            },
        ),
        validation_status=validation_status,
    )


def _account_artifact_paths(*, truth_root: Path, day_utc: str) -> list[Path]:
    return [
        truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json",
        truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json",
        truth_root / "position_lifecycle_v2" / day_utc / "position_lifecycle_snapshot.v2.json",
        truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json",
    ]


def _artifact_validation_status(payload: dict[str, Any]) -> str:
    status = str(payload.get("validation_status") or payload.get("status") or "").strip().upper()
    if status in {"VALID", "PASS", "OK", "ACTIVE", "BOOTSTRAP", "DEGRADED_OPERATOR_INPUT"}:
        return "VALID"
    return "REJECTED"


def build_and_write_account_economic_state_authority_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    operation_type: str = "fresh_paper_entry_v1",
    sleeve_id: str = "PRIMARY",
    environment: str = "PAPER",
    ib_account: str = "DUO847203",
    emit_events: bool = True,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo_root = Path(__file__).resolve().parents[2]
    result = run_economic_state_authority_v1(
        repo_root=repo_root,
        operation_type=operation_type,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        materialize=True,
        emit_package=True,
    )
    build_obj = result["build_obj"]
    build_path = Path(result["build_path"])
    package_path = Path(result["package_path"]) if result.get("package_path") else None
    validation_status = "VALID" if str(build_obj.get("closure_status") or "") == "COMPLETE" and package_path is not None else "REJECTED"
    emitted: list[dict[str, Any]] = []
    if emit_events:
        git_sha = git_sha_v1(repo_root)
        run_id = f"account_economic_state_authority_v1:{day_utc}:{build_obj.get('context_hash')}"
        dependency_paths = []
        emitted_paths: set[str] = set()
        for row in build_obj.get("dependency_results") or []:
            if not isinstance(row, dict):
                continue
            path = Path(str(row.get("path") or ""))
            if not path.exists() or not path.is_file():
                continue
            payload = read_json_v1(path)
            if str(payload.get("schema_id") or "") not in ACTUAL_PRODUCER_BY_SCHEMA:
                continue
            dependency_paths.append(path)
            emitted_paths.add(str(path.resolve()))
            row_status = "VALID" if row.get("status") == "PRESENT" else "REJECTED"
            emitted.extend(_emit_for_path(truth_root=root, day_utc=day_utc, path=path, payload=payload, validation_status=row_status, run_id=run_id, git_sha=git_sha, input_paths=dependency_paths))
        for path in _account_artifact_paths(truth_root=root, day_utc=day_utc):
            if not path.exists() or not path.is_file() or str(path.resolve()) in emitted_paths:
                continue
            payload = read_json_v1(path)
            dependency_paths.append(path)
            emitted_paths.add(str(path.resolve()))
            emitted.extend(_emit_for_path(truth_root=root, day_utc=day_utc, path=path, payload=payload, validation_status=_artifact_validation_status(payload), run_id=run_id, git_sha=git_sha, input_paths=dependency_paths))
        emitted.extend(_emit_for_path(truth_root=root, day_utc=day_utc, path=build_path, payload=build_obj, validation_status=validation_status, run_id=run_id, git_sha=git_sha, input_paths=dependency_paths))
        if package_path is not None and result.get("package_obj") is not None:
            emitted.extend(_emit_for_path(truth_root=root, day_utc=day_utc, path=package_path, payload=result["package_obj"], validation_status=validation_status, run_id=run_id, git_sha=git_sha, input_paths=[build_path, *dependency_paths]))
    source_summary = account_source_summary_v1(truth_root=root, day_utc=day_utc, result=result)
    return {
        "validation_status": validation_status,
        "closure_status": build_obj.get("closure_status"),
        "build_path": str(build_path),
        "build_hash": sha256_file_v1(build_path) if build_path.exists() else "",
        "package_path": str(package_path) if package_path is not None else "",
        "package_hash": sha256_file_v1(package_path) if package_path is not None and package_path.exists() else "",
        "first_real_blocker": build_obj.get("first_real_blocker"),
        "materialized_nodes": build_obj.get("materialized_nodes", []),
        "account_source_summary": source_summary,
        "emitted_event_statuses": [row.get("status") for row in emitted if isinstance(row, dict)],
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
