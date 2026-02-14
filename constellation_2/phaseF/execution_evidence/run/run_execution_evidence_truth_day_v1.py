from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.execution_evidence.lib.paths_v1 import (
    PHASED_SUBMISSIONS_ROOT,
    REPO_ROOT,
    day_paths_v1,
    submission_artifact_dir_v1,
    submission_manifest_path_v1,
    submission_manifest_identity_patch_path_v1,
)
from constellation_2.phaseF.execution_evidence.lib.write_failure_v1 import build_failure_obj_v1, write_failure_immutable_v1


SCHEMA_LATEST_POINTER = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_latest_pointer.v1.schema.json"
SCHEMA_FAILURE = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_failure.v1.schema.json"
SCHEMA_SUBMISSION_MANIFEST_V2 = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_submission_manifest.v2.schema.json"
SCHEMA_SUBMISSION_MANIFEST_V1 = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_submission_manifest.v1.schema.json"
SCHEMA_MANIFEST_ID_PATCH_V1 = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_submission_manifest_identity_patch.v1.schema.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _lock_git_sha_if_exists(existing_path: Path, provided_sha: str) -> Optional[str]:
    if existing_path.exists() and existing_path.is_file():
        ex = _read_json_obj(existing_path)
        prod = ex.get("producer")
        ex_sha = prod.get("git_sha") if isinstance(prod, dict) else None
        if isinstance(ex_sha, str) and ex_sha.strip():
            if ex_sha.strip() != provided_sha:
                return ex_sha.strip()
    return None


def _day_scoped_sha_lock_from_manifests_dir(manifests_day_dir: Path, provided_sha: str) -> Optional[str]:
    """
    Day-scoped producer SHA lock.

    If the manifests day dir contains any JSON artifacts (manifest or patch),
    read the first one deterministically and enforce producer.git_sha == provided_sha.

    Returns the existing sha if it mismatches; otherwise None.
    """
    if not manifests_day_dir.exists() or not manifests_day_dir.is_dir():
        return None

    files = sorted([p for p in manifests_day_dir.iterdir() if p.is_file() and p.name.endswith(".json")], key=lambda p: p.name)
    if not files:
        return None

    ex = _read_json_obj(files[0])
    prod = ex.get("producer")
    ex_sha = prod.get("git_sha") if isinstance(prod, dict) else None
    if isinstance(ex_sha, str) and ex_sha.strip():
        if ex_sha.strip() != provided_sha:
            return ex_sha.strip()
    return None


def _derive_day_utc_from_inputs(broker_obj: Dict[str, Any], exec_obj: Optional[Dict[str, Any]], veto_obj: Optional[Dict[str, Any]]) -> str:
    """
    Deterministically derive day_utc from the first valid timestamp found across broker/exec/veto objects.

    Fail-closed if no valid timestamp is found.
    """
    candidates: List[str] = []

    def _add(obj: Optional[Dict[str, Any]], keys: List[str]) -> None:
        if not isinstance(obj, dict):
            return
        for k in keys:
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                candidates.append(v.strip())

    # Prefer broker timestamps, then execution, then veto.
    _add(broker_obj, ["created_at_utc", "created_time_utc", "created_utc", "submitted_at_utc", "submitted_time_utc"])
    _add(exec_obj, ["event_time_utc", "created_at_utc", "created_time_utc", "created_utc"])
    _add(veto_obj, ["created_at_utc", "created_time_utc", "created_utc"])

    for ts in candidates:
        # Accept full ISO "YYYY-MM-DDTHH:MM:SSZ" (or with fractional seconds), require 'T'.
        if "T" in ts and len(ts) >= 10:
            day = ts.split("T", 1)[0].strip()
            if len(day) == 10 and day[4] == "-" and day[7] == "-":
                return day

    raise ValueError("NO_VALID_UTC_TIMESTAMP_FOR_DAY_DERIVATION")


def _maybe_copy_identity_file(*, src_dir: Path, dst_dir: Path, filename: str) -> Optional[Dict[str, Any]]:
    p_src = (src_dir / filename).resolve()
    if not p_src.exists() or not p_src.is_file():
        return None
    b = p_src.read_bytes()
    wr = write_file_immutable_v1(path=(dst_dir / filename), data=b, create_dirs=True)
    return {"path": str(dst_dir / filename), "sha256": wr.sha256}


def _validate_manifest_any_version(obj: Dict[str, Any]) -> Tuple[str, int]:
    sid = str(obj.get("schema_id") or "").strip()
    sver = int(obj.get("schema_version") or 0)
    if sid == "C2_EXECUTION_EVIDENCE_SUBMISSION_MANIFEST_V2" and sver == 2:
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_SUBMISSION_MANIFEST_V2)
        return (sid, sver)
    if sid == "C2_EXECUTION_EVIDENCE_SUBMISSION_MANIFEST_V1" and sver == 1:
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_SUBMISSION_MANIFEST_V1)
        return (sid, sver)
    raise ValueError(f"UNSUPPORTED_MANIFEST_SCHEMA: {sid} v{sver}")


def _build_day_manifests_index_sha(manifests_day_dir: Path) -> str:
    # Deterministic day hash computed from the set of manifest + patch files present,
    # independent of what was "written this run".
    files = []
    if manifests_day_dir.exists() and manifests_day_dir.is_dir():
        for p in sorted([x for x in manifests_day_dir.iterdir() if x.is_file()], key=lambda x: x.name):
            if p.name.endswith(".json"):
                files.append({"name": p.name, "sha256": _sha256_file(p)})
    summary = {"files": files}
    return _sha256_bytes(canonical_json_bytes_v1(summary))


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_execution_evidence_truth_day_v1",
        description="C2 PhaseF Execution Evidence Truth Day v1 (mirrors PhaseD submission evidence into truth).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()

    dp = day_paths_v1(day_utc)

    # Producer sha lock on reruns: DAY-SCOPED (manifests/<day>/), not global latest.json.
    ex_sha = _day_scoped_sha_lock_from_manifests_dir(dp.manifests_day_dir, producer_sha)
    if ex_sha is not None:
        print(f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha} provided={producer_sha}", file=sys.stderr)
        return 4

    status = "OK"
    reason_codes: List[str] = []

    written_manifests: List[Tuple[str, str]] = []
    written_patches: List[Tuple[str, str]] = []

    if not PHASED_SUBMISSIONS_ROOT.exists():
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/execution_evidence/run/run_execution_evidence_truth_day_v1.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["PHASED_SUBMISSIONS_ROOT_MISSING"],
            input_manifest=[{"type": "phaseD_submissions_root", "path": str(PHASED_SUBMISSIONS_ROOT), "sha256": "0" * 64, "day_utc": None, "producer": "phaseD"}],
            code="FAIL_CORRUPT_INPUTS",
            message=f"Missing PhaseD submissions root: {str(PHASED_SUBMISSIONS_ROOT)}",
            details={"missing_path": str(PHASED_SUBMISSIONS_ROOT)},
            attempted_outputs=[{"path": str(dp.submissions_day_dir), "sha256": None}, {"path": str(dp.latest_path), "sha256": None}],
        )
        validate_against_repo_schema_v1(failure, REPO_ROOT, SCHEMA_FAILURE)
        b = canonical_json_bytes_v1(failure) + b"\n"
        _ = write_file_immutable_v1(path=dp.failure_path, data=b, create_dirs=True)
        print("FAIL: PHASED_SUBMISSIONS_ROOT_MISSING (failure artifact written)", file=sys.stderr)
        return 2

    sub_dirs = sorted([p for p in PHASED_SUBMISSIONS_ROOT.iterdir() if p.is_dir()], key=lambda p: p.name)

    for sd in sub_dirs:
        submission_id = sd.name.strip()

        p_broker = sd / "broker_submission_record.v2.json"
        p_exec = sd / "execution_event_record.v1.json"
        p_veto = sd / "veto_record.v1.json"

        broker_obj = None
        exec_obj = None
        veto_obj = None

        if p_broker.exists():
            broker_obj = _read_json_obj(p_broker)
            validate_against_repo_schema_v1(broker_obj, REPO_ROOT, "constellation_2/schemas/broker_submission_record.v2.schema.json")

        if p_exec.exists():
            exec_obj = _read_json_obj(p_exec)

        if p_veto.exists():
            veto_obj = _read_json_obj(p_veto)

        if broker_obj is None:
            continue

        if exec_obj is not None:
            validate_against_repo_schema_v1(exec_obj, REPO_ROOT, "constellation_2/schemas/execution_event_record.v1.schema.json")
        if veto_obj is not None:
            validate_against_repo_schema_v1(veto_obj, REPO_ROOT, "constellation_2/schemas/veto_record.v1.schema.json")

        derived_day = _derive_day_utc_from_inputs(broker_obj, exec_obj, veto_obj)
        if derived_day != day_utc:
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

        input_manifest = [{"type": "phaseD_submission_dir", "path": str(sd), "sha256": "0" * 64, "day_utc": day_utc, "producer": "phaseD"}]

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

        m_path = submission_manifest_path_v1(day_utc=day_utc, submission_id=submission_id)

        # Idempotent behavior:
        # - If manifest exists, do not rewrite. Validate it and (if needed) write identity patch sidecar.
        # - If manifest does not exist, write v2 manifest.
        if m_path.exists() and m_path.is_file():
            ex_manifest = _read_json_obj(m_path)
            _ = _validate_manifest_any_version(ex_manifest)

            # Identity patch is only needed when identity inputs are present (files exist) AND
            # either the existing manifest lacks pointers (v1) or we want an immutable record
            # of late-arriving identity inputs without rewriting the base manifest.
            patch_path = submission_manifest_identity_patch_path_v1(day_utc=day_utc, submission_id=submission_id)

            need_patch = False
            if (ptr_plan is not None) or (ptr_bind is not None) or (ptr_map is not None):
                need_patch = True

            if need_patch:
                # producer sha lock on reruns for patch
                ex_patch_sha = _lock_git_sha_if_exists(patch_path, producer_sha)
                if ex_patch_sha is not None:
                    print(f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_PATCH: existing={ex_patch_sha} provided={producer_sha}", file=sys.stderr)
                    return 4

                if not patch_path.exists():
                    patch_obj: Dict[str, Any] = {
                        "schema_id": "C2_EXECUTION_EVIDENCE_SUBMISSION_MANIFEST_IDENTITY_PATCH_V1",
                        "schema_version": 1,
                        "produced_utc": f"{day_utc}T00:00:00Z",
                        "day_utc": day_utc,
                        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": "constellation_2/phaseF/execution_evidence/run/run_execution_evidence_truth_day_v1.py"},
                        "status": "OK",
                        "reason_codes": ["IDENTITY_INPUTS_PRESENT_PATCH_V1"],
                        "submission_id": submission_id,
                        "base_manifest": {"path": str(m_path), "sha256": _sha256_file(m_path)},
                        "identity_inputs": {
                            "order_plan": ptr_plan,
                            "binding_record": ptr_bind,
                            "mapping_ledger_record": ptr_map,
                        },
                    }

                    validate_against_repo_schema_v1(patch_obj, REPO_ROOT, SCHEMA_MANIFEST_ID_PATCH_V1)
                    p_bytes = canonical_json_bytes_v1(patch_obj) + b"\n"
                    wr_p = write_file_immutable_v1(path=patch_path, data=p_bytes, create_dirs=True)
                    written_patches.append((submission_id, wr_p.sha256))
            continue

        # Manifest does not exist: write it (v2).
        try:
            m_bytes = canonical_json_bytes_v1(manifest_obj) + b"\n"
        except CanonicalizationError as e:
            raise RuntimeError(f"MANIFEST_CANONICALIZATION_ERROR: {e}") from e

        wr_m = write_file_immutable_v1(path=m_path, data=m_bytes, create_dirs=True)
        written_manifests.append((submission_id, wr_m.sha256))

    # Deterministic day hash based on the manifests directory contents (manifests + patches).
    submissions_day_sha256 = _build_day_manifests_index_sha(dp.manifests_day_dir)

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

    # Idempotent latest: if exists, do not rewrite.
    if not dp.latest_path.exists():
        _ = write_file_immutable_v1(path=dp.latest_path, data=latest_bytes, create_dirs=True)

    print("OK: EXECUTION_EVIDENCE_DAY_COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
