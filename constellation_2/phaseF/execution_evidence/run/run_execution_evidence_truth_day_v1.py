from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.execution_evidence.lib.paths_v1 import (
    REPO_ROOT,
    day_paths_v1,
    submission_artifact_dir_v1,
    submission_manifest_path_v1,
)
from constellation_2.phaseF.execution_evidence.lib.write_failure_v1 import build_failure_obj_v1, write_failure_immutable_v1

SCHEMA_SUBMISSION_MANIFEST_V2 = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_submission_manifest.v2.schema.json"
SCHEMA_LATEST_POINTER = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_latest_pointer.v1.schema.json"

PHASED_SUBMISSIONS_ROOT = (REPO_ROOT / "constellation_2/phaseD/outputs/submissions").resolve()

IDENTITY_FILES = [
    "order_plan.v1.json",
    "binding_record.v1.json",
    "mapping_ledger_record.v1.json",
]


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _read_json_object(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _day_from_zulu(ts: str, field: str) -> str:
    if not isinstance(ts, str) or "T" not in ts:
        raise ValueError(f"INVALID_TIMESTAMP: {field}")
    if not ts.endswith("Z"):
        raise ValueError(f"TIMESTAMP_NOT_Z_SUFFIX: {field}")
    return ts.split("T", 1)[0]


def _derive_day_utc_from_inputs(
    broker_sub: Optional[Dict[str, Any]],
    exec_evt: Optional[Dict[str, Any]],
    veto: Optional[Dict[str, Any]],
) -> str:
    if veto is not None:
        return _day_from_zulu(veto["observed_at_utc"], "veto_record.observed_at_utc")
    if broker_sub is not None:
        return _day_from_zulu(broker_sub["submitted_at_utc"], "broker_submission_record.submitted_at_utc")
    if exec_evt is not None:
        return _day_from_zulu(exec_evt["event_time_utc"], "execution_event_record.event_time_utc")
    raise ValueError("NO_TIMESTAMP_SOURCE_AVAILABLE")


def _maybe_copy_identity_file(
    *,
    src_dir: Path,
    dst_dir: Path,
    filename: str,
) -> Optional[Dict[str, str]]:
    p = src_dir / filename
    if not p.exists():
        return None
    if not p.is_file():
        raise RuntimeError(f"IDENTITY_INPUT_NOT_FILE: {str(p)}")
    b = p.read_bytes()
    wr = write_file_immutable_v1(path=dst_dir / filename, data=b, create_dirs=True)
    return {"path": str(dst_dir / filename), "sha256": wr.sha256}


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_execution_evidence_truth_day_v1",
        description="C2 Execution Evidence Truth Spine v1 (PhaseD → runtime/truth).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id (deterministic)")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()

    dp = day_paths_v1(day_utc)

    if not PHASED_SUBMISSIONS_ROOT.exists():
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/execution_evidence/run/run_execution_evidence_truth_day_v1.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["PHASED_SUBMISSIONS_ROOT_MISSING"],
            input_manifest=[{"type": "phaseD_submissions_root", "path": str(PHASED_SUBMISSIONS_ROOT), "sha256": "0"*64, "day_utc": None, "producer": "phaseD"}],
            code="FAIL_CORRUPT_INPUTS",
            message=f"Missing PhaseD submissions root: {str(PHASED_SUBMISSIONS_ROOT)}",
            details={"missing_path": str(PHASED_SUBMISSIONS_ROOT)},
            attempted_outputs=[{"path": str(dp.submissions_day_dir), "sha256": None}, {"path": str(dp.manifests_day_dir), "sha256": None}, {"path": str(dp.latest_path), "sha256": None}],
        )
        _ = write_failure_immutable_v1(failure_path=dp.failure_path, failure_obj=failure)
        print("FAIL: PHASED_SUBMISSIONS_ROOT_MISSING (failure artifact written)")
        return 2

    sub_dirs = sorted([p for p in PHASED_SUBMISSIONS_ROOT.iterdir() if p.is_dir()], key=lambda p: p.name)

    written_manifests: List[Tuple[str, str]] = []
    status = "OK"
    reason_codes: List[str] = []

    for sd in sub_dirs:
        submission_id = sd.name.strip()
        p_broker = sd / "broker_submission_record.v2.json"
        p_exec = sd / "execution_event_record.v1.json"
        p_veto = sd / "veto_record.v1.json"

        broker_obj = _read_json_object(p_broker) if p_broker.exists() else None
        exec_obj = _read_json_object(p_exec) if p_exec.exists() else None
        veto_obj = _read_json_object(p_veto) if p_veto.exists() else None

        if broker_obj is not None:
            validate_against_repo_schema_v1(broker_obj, REPO_ROOT, "constellation_2/schemas/broker_submission_record.v2.schema.json")
        if exec_obj is not None:
            validate_against_repo_schema_v1(exec_obj, REPO_ROOT, "constellation_2/schemas/execution_event_record.v1.schema.json")
        if veto_obj is not None:
            validate_against_repo_schema_v1(veto_obj, REPO_ROOT, "constellation_2/schemas/veto_record.v1.schema.json")

        derived_day = _derive_day_utc_from_inputs(broker_obj, exec_obj, veto_obj)
        if derived_day != day_utc:
            continue

        if broker_obj is None:
            status = "DEGRADED_MISSING_EXECUTION_EVENT"
            if "SKIPPED_VETO_ONLY_SUBMISSION_UNSUPPORTED_V1" not in reason_codes:
                reason_codes.append("SKIPPED_VETO_ONLY_SUBMISSION_UNSUPPORTED_V1")
            continue

        if exec_obj is None:
            status = "DEGRADED_MISSING_EXECUTION_EVENT"
            if "MISSING_EXECUTION_EVENT" not in reason_codes:
                reason_codes.append("MISSING_EXECUTION_EVENT")

        art_dir = submission_artifact_dir_v1(day_utc=day_utc, submission_id=submission_id)
        art_dir.mkdir(parents=True, exist_ok=True)

        b_broker = p_broker.read_bytes()
        wr_broker = write_file_immutable_v1(path=art_dir / "broker_submission_record.v2.json", data=b_broker, create_dirs=True)

        wr_exec = None
        if exec_obj is not None:
            b_exec = p_exec.read_bytes()
            wr_exec = write_file_immutable_v1(path=art_dir / "execution_event_record.v1.json", data=b_exec, create_dirs=True)

        # Mirror identity inputs when present.
        ptr_plan = _maybe_copy_identity_file(src_dir=sd, dst_dir=art_dir, filename="order_plan.v1.json")
        ptr_bind = _maybe_copy_identity_file(src_dir=sd, dst_dir=art_dir, filename="binding_record.v1.json")
        ptr_map = _maybe_copy_identity_file(src_dir=sd, dst_dir=art_dir, filename="mapping_ledger_record.v1.json")

        input_manifest = [
            {"type": "phaseD_submission_dir", "path": str(sd), "sha256": "0"*64, "day_utc": day_utc, "producer": "phaseD"}
        ]

        manifest_obj: Dict[str, Any] = {
            "schema_id": "C2_EXECUTION_EVIDENCE_SUBMISSION_MANIFEST_V2",
            "schema_version": 2,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "day_utc": day_utc,
            "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": "constellation_2/phaseF/execution_evidence/run/run_execution_evidence_truth_day_v1.py"},
            "status": status,
            "reason_codes": sorted(set(reason_codes)),
            "input_manifest": input_manifest,
            "submission": {
                "submission_id": submission_id,
                "source_dir": str(sd),
                "artifact_dir": str(art_dir),
                "broker_submission_record": {"path": str(art_dir / "broker_submission_record.v2.json"), "sha256": wr_broker.sha256},
                "execution_event_record": None if wr_exec is None else {"path": str(art_dir / "execution_event_record.v1.json"), "sha256": wr_exec.sha256},
                "veto_record": None,
                "order_plan": ptr_plan,
                "binding_record": ptr_bind,
                "mapping_ledger_record": ptr_map,
            },
        }

        validate_against_repo_schema_v1(manifest_obj, REPO_ROOT, SCHEMA_SUBMISSION_MANIFEST_V2)
        try:
            m_bytes = canonical_json_bytes_v1(manifest_obj) + b"\n"
        except CanonicalizationError as e:
            raise RuntimeError(f"MANIFEST_CANONICALIZATION_ERROR: {e}") from e

        m_path = submission_manifest_path_v1(day_utc=day_utc, submission_id=submission_id)
        wr_m = write_file_immutable_v1(path=m_path, data=m_bytes, create_dirs=True)
        written_manifests.append((submission_id, wr_m.sha256))

    summary = {
        "day_utc": day_utc,
        "submission_ids": [sid for (sid, _h) in sorted(written_manifests)],
        "manifest_hashes": [{"submission_id": sid, "sha256": h} for (sid, h) in sorted(written_manifests)],
    }
    submissions_day_sha256 = _sha256_bytes(canonical_json_bytes_v1(summary))

    latest_obj: Dict[str, Any] = {
        "schema_id": "C2_EXECUTION_EVIDENCE_LATEST_POINTER_V1",
        "schema_version": 1,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": "constellation_2/phaseF/execution_evidence/run/run_execution_evidence_truth_day_v1.py"},
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "pointers": {"submissions_day_dir": str(dp.submissions_day_dir), "submissions_day_sha256": submissions_day_sha256},
    }

    validate_against_repo_schema_v1(latest_obj, REPO_ROOT, SCHEMA_LATEST_POINTER)
    latest_bytes = canonical_json_bytes_v1(latest_obj) + b"\n"
    _ = write_file_immutable_v1(path=dp.latest_path, data=latest_bytes, create_dirs=True)

    print("OK: EXECUTION_EVIDENCE_DAY_COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
