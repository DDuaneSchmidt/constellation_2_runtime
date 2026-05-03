#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseB.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.tools.aegis_artifact_ledger_v1 import (
    artifact_hash_v1,
    build_promotion_attestation_v1,
    find_artifact_ledger_record_v1,
    git_commit_v1,
    infer_runtime_root_v1,
    read_json_v1,
    verify_artifact_ledger_record_v1,
    write_artifact_ledger_record_v1,
    write_json_v1,
    write_promotion_attestation_v1,
)
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.aegis_runtime_mode_v1 import runtime_mode_from_truth_root_v1
from ops.tools.run_aegis_control_plane_v1 import require_control_plane_self_binding_v1
from ops.tools.run_aegis_control_plane_v1 import control_plane_acceptance_issues_v1

PRODUCER = "ops/tools/run_candidate_to_production_promotion_v1.py"


def _path_under(path: Path, root: Path) -> bool:
    resolved = Path(path).expanduser().resolve()
    root_resolved = Path(root).expanduser().resolve()
    return resolved == root_resolved or root_resolved in resolved.parents


def _rewrite_for_production(payload: dict[str, Any], *, destination: Path, production_truth_root: Path, runtime_root: Path) -> dict[str, Any]:
    out = json.loads(json.dumps(payload))
    out["truth_root"] = str(production_truth_root)
    out["runtime_root"] = str(runtime_root)
    out["runtime_mode"] = "PRODUCTION"
    out["artifact_path"] = str(destination)
    out["actual_artifact_path"] = str(destination)
    out["producer_contract_output_artifact_path"] = str(destination)
    attach_producer_contract_v1(
        out,
        producer_name=PRODUCER,
        producer_command=f"python3 {PRODUCER}",
        input_artifacts=[Path(str(payload.get("artifact_path") or destination))],
        output_artifacts=[destination],
        schema_versions={str(out.get("schema_id") or "artifact"): str(out.get("schema_version") or "")},
    )
    return out


def _use_existing_production_view_v1(
    *,
    destination: Path,
    production_truth_root: Path,
    runtime_root: Path,
    day_utc: str,
    artifact_type: str,
    schema_relpath: str,
) -> dict[str, Any]:
    if not destination.exists() or not destination.is_file():
        raise FileNotFoundError(f"PROMOTION_DESTINATION_PRODUCTION_ARTIFACT_MISSING:{destination}")
    payload = read_json_v1(destination)
    if artifact_type == "aegis_control_plane_v1":
        issues = control_plane_acceptance_issues_v1(payload, actual_path=destination, require_promotion=False)
        if issues:
            raise RuntimeError("PROMOTION_DESTINATION_CONTROL_PLANE_INVALID:" + json.dumps(issues, sort_keys=True))
    if schema_relpath:
        validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
    if str(payload.get("runtime_mode") or "").strip().upper() != "PRODUCTION":
        raise RuntimeError("PROMOTION_DESTINATION_RUNTIME_MODE_MISMATCH")
    if Path(str(payload.get("truth_root") or "")).expanduser().resolve() != production_truth_root:
        raise RuntimeError("PROMOTION_DESTINATION_TRUTH_ROOT_MISMATCH")
    if Path(str(payload.get("runtime_root") or "")).expanduser().resolve() != runtime_root:
        raise RuntimeError("PROMOTION_DESTINATION_RUNTIME_ROOT_MISMATCH")
    if str(payload.get("day_utc") or payload.get("day") or "").strip()[:10] != day_utc:
        raise RuntimeError("PROMOTION_DESTINATION_DAY_MISMATCH")
    return payload


def promote_candidate_to_production_v1(
    *,
    source_candidate_artifact_path: Path,
    destination_production_path: Path,
    candidate_truth_root: Path,
    production_truth_root: Path,
    day_utc: str,
    artifact_type: str,
    schema_relpath: str,
) -> tuple[Path, Path, dict[str, Any]]:
    source = Path(source_candidate_artifact_path).expanduser().resolve()
    destination = Path(destination_production_path).expanduser().resolve()
    candidate_root = Path(candidate_truth_root).expanduser().resolve()
    production_root = Path(production_truth_root).expanduser().resolve()
    runtime_root = infer_runtime_root_v1(production_root)
    blockers: list[str] = []
    if not _path_under(source, candidate_root):
        blockers.append("PROMOTION_SOURCE_NOT_UNDER_CANDIDATE_TRUTH")
    if not _path_under(destination, production_root):
        blockers.append("PROMOTION_DESTINATION_NOT_UNDER_PRODUCTION_TRUTH")
    if _path_under(source, production_root):
        blockers.append("PROMOTION_CANDIDATE_COPIED_UNDER_PRODUCTION")
    candidate = read_json_v1(source)
    if str(candidate.get("runtime_mode") or "").upper() != "CANDIDATE":
        blockers.append("PROMOTION_SOURCE_RUNTIME_MODE_MISMATCH")
    if str(candidate.get("truth_root") or "").strip() and Path(str(candidate.get("truth_root"))).expanduser().resolve() != candidate_root:
        blockers.append("PROMOTION_SOURCE_TRUTH_ROOT_MISMATCH")
    source_ledger = find_artifact_ledger_record_v1(truth_root=candidate_root, day=day_utc, artifact_path=source)
    if not source_ledger:
        blockers.append("PROMOTION_SOURCE_LEDGER_RECORD_MISSING")
    else:
        if str(source_ledger.get("producer_git_commit") or "").strip() != git_commit_v1():
            blockers.append("PROMOTION_SOURCE_PRODUCER_GIT_MISMATCH")
        ledger_issues = verify_artifact_ledger_record_v1(
            artifact_path=source,
            artifact_type=artifact_type,
            truth_root=candidate_root,
            runtime_root=infer_runtime_root_v1(candidate_root),
            runtime_mode="CANDIDATE",
            day=day_utc,
        )
        blockers.extend(row["code"] for row in ledger_issues)
    if schema_relpath:
        try:
            validate_against_repo_schema_v1(candidate, REPO_ROOT, schema_relpath)
        except Exception as exc:  # noqa: BLE001
            blockers.append(f"PROMOTION_SOURCE_SCHEMA_INVALID:{type(exc).__name__}")
    if blockers:
        raise RuntimeError("PROMOTION_REJECTED:" + ",".join(blockers))

    if artifact_type == "aegis_control_plane_v1" and destination.exists():
        production_payload = _use_existing_production_view_v1(
            destination=destination,
            production_truth_root=production_root,
            runtime_root=runtime_root,
            day_utc=day_utc,
            artifact_type=artifact_type,
            schema_relpath=schema_relpath,
        )
    else:
        production_payload = _rewrite_for_production(candidate, destination=destination, production_truth_root=production_root, runtime_root=runtime_root)
    if artifact_type == "aegis_control_plane_v1":
        require_control_plane_self_binding_v1(production_payload, actual_path=destination)
    if schema_relpath:
        validate_against_repo_schema_v1(production_payload, REPO_ROOT, schema_relpath)
    if not destination.exists() or artifact_type != "aegis_control_plane_v1":
        write_json_v1(destination, production_payload)
    _ledger_path, destination_ledger = write_artifact_ledger_record_v1(
        artifact_path=destination,
        artifact_type=artifact_type,
        truth_root=production_root,
        runtime_root=runtime_root,
        runtime_mode=runtime_mode_from_truth_root_v1(production_root),
        day=day_utc,
        recovery_command=f"PYTHONPATH=\"$PWD\" python3 {PRODUCER}",
    )
    artifact_id = str(destination_ledger.get("artifact_id") or "")
    attestation = build_promotion_attestation_v1(
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        source_candidate_artifact_path=source,
        source_ledger_record=source_ledger,
        destination_production_path=destination,
        validation_status="PASS",
        blockers=[],
    )
    attestation_path = write_promotion_attestation_v1(truth_root=production_root, day=day_utc, attestation=attestation)
    if str(source_ledger.get("artifact_hash") or "") != artifact_hash_v1(source):
        raise RuntimeError("PROMOTION_SOURCE_HASH_MISMATCH")
    return destination, attestation_path, production_payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_candidate_to_production_promotion_v1")
    parser.add_argument("--source_candidate_artifact_path", required=True)
    parser.add_argument("--destination_production_path", required=True)
    parser.add_argument("--candidate_truth_root", required=True)
    parser.add_argument("--production_truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--artifact_type", required=True)
    parser.add_argument("--schema_relpath", default="")
    args = parser.parse_args(argv)
    destination, attestation_path, payload = promote_candidate_to_production_v1(
        source_candidate_artifact_path=Path(args.source_candidate_artifact_path),
        destination_production_path=Path(args.destination_production_path),
        candidate_truth_root=Path(args.candidate_truth_root),
        production_truth_root=Path(args.production_truth_root),
        day_utc=str(args.day_utc).strip(),
        artifact_type=str(args.artifact_type).strip(),
        schema_relpath=str(args.schema_relpath or "").strip(),
    )
    print(
        json.dumps(
            {
                "status": "PASS",
                "destination_production_path": str(destination),
                "destination_hash": artifact_hash_v1(destination),
                "promotion_attestation_path": str(attestation_path),
                "artifact_status": payload.get("final_status") or payload.get("status") or payload.get("validation_status"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
