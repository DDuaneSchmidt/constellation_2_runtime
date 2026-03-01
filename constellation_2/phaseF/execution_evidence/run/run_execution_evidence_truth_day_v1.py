from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
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
SCHEMA_SUBMISSION_MANIFEST_V3 = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_submission_manifest.v3.schema.json"
SCHEMA_TOMBSTONE_V1 = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_quarantine_tombstone.v1.schema.json"
SCHEMA_NO_EXEC_V1 = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_no_execution_event.v1.schema.json"


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


def _derive_day_utc_from_inputs(
    broker_obj: Optional[Dict[str, Any]],
    exec_obj: Optional[Dict[str, Any]],
    veto_obj: Optional[Dict[str, Any]],
) -> str:
    """
    Deterministically derive day_utc from first valid timestamp across broker/exec/veto.
    Fail-closed if no valid timestamp found.
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
    # veto schema uses observed_at_utc (and may use created_* in future)
    _add(veto_obj, ["observed_at_utc", "created_at_utc", "created_time_utc", "created_utc"])

    for ts in candidates:
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
    if sid == "C2_EXECUTION_EVIDENCE_SUBMISSION_MANIFEST_V3" and sver == 3:
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_SUBMISSION_MANIFEST_V3)
        return (sid, sver)
    if sid == "C2_EXECUTION_EVIDENCE_SUBMISSION_MANIFEST_V2" and sver == 2:
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_SUBMISSION_MANIFEST_V2)
        return (sid, sver)
    if sid == "C2_EXECUTION_EVIDENCE_SUBMISSION_MANIFEST_V1" and sver == 1:
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_SUBMISSION_MANIFEST_V1)
        return (sid, sver)
    raise ValueError(f"UNSUPPORTED_MANIFEST_SCHEMA: {sid} v{sver}")


def _build_day_manifests_index_sha(manifests_day_dir: Path) -> str:
    files = []
    if manifests_day_dir.exists() and manifests_day_dir.is_dir():
        for p in sorted([x for x in manifests_day_dir.iterdir() if x.is_file()], key=lambda x: x.name):
            if p.name.endswith(".json"):
                files.append({"name": p.name, "sha256": _sha256_file(p)})
    summary = {"files": files}
    return _sha256_bytes(canonical_json_bytes_v1(summary))


def _tombstones_dir_for_day(dp) -> Path:
    return (dp.submissions_day_dir / "__tombstones__").resolve()


def _tombstone_path_for(dp, submission_id: str) -> Path:
    return (_tombstones_dir_for_day(dp) / f"quarantine_tombstone.{submission_id}.v1.json").resolve()


def _write_quarantine_tombstone(
    *,
    dp,
    day_utc: str,
    submission_id: str,
    producer_repo: str,
    producer_sha: str,
    quarantine_reason: str,
    original_hash: str,
    details: Dict[str, Any],
) -> str:
    obj: Dict[str, Any] = {
        "schema_id": "C2_EXECUTION_EVIDENCE_QUARANTINE_TOMBSTONE_V1",
        "schema_version": 1,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "submission_id": submission_id,
        "quarantine_reason": quarantine_reason,
        "original_hash": original_hash,
        "timestamp": f"{day_utc}T00:00:00Z",
        "authoritative": False,
        "details": details,
    }
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_TOMBSTONE_V1)
    b = canonical_json_bytes_v1(obj) + b"\n"
    tpath = _tombstone_path_for(dp, submission_id)
    if tpath.exists() and tpath.is_file():
        # Idempotent tombstone: never rewrite immutable truth.
        ex = _read_json_obj(tpath)
        validate_against_repo_schema_v1(ex, REPO_ROOT, SCHEMA_TOMBSTONE_V1)
        return _sha256_file(tpath)
    wr = write_file_immutable_v1(path=_tombstone_path_for(dp, submission_id), data=b, create_dirs=True)
    return wr.sha256


def _canonical_tmp_dir(dp, submission_id: str) -> Path:
    # deterministic-enough nonce using pid + timestamp; only used for temp dir names.
    nonce = f"{os.getpid()}_{int(datetime.now(timezone.utc).timestamp())}"
    return (dp.submissions_day_dir / f".tmp_{submission_id}_{nonce}").resolve()


def _atomic_publish_dir(tmp_dir: Path, final_dir: Path) -> None:
    if final_dir.exists():
        # Fail-closed: do not overwrite canonical submission dir
        raise RuntimeError(f"FINAL_DIR_ALREADY_EXISTS: {str(final_dir)}")
    tmp_dir.replace(final_dir)


def _require_nonempty_hash(obj: Dict[str, Any], field: str, label: str) -> None:
    v = obj.get(field)
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"MISSING_REQUIRED_HASH_FIELD: {label}.{field}")


def _write_no_execution_event(*, day_utc: str, submission_id: str, reason_code: str, reason_detail: str) -> Dict[str, Any]:
    obj: Dict[str, Any] = {
        "schema_id": "C2_EXECUTION_EVIDENCE_NO_EXECUTION_EVENT_V1",
        "schema_version": 1,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "submission_id": submission_id,
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "authoritative": True,
        "canonical_json_hash": None,
    }
    obj["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(obj)
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_NO_EXEC_V1)
    return obj


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
        p_auth = sd / "authorization_binding_record.v1.json"

        broker_obj: Optional[Dict[str, Any]] = None
        exec_obj: Optional[Dict[str, Any]] = None
        veto_obj: Optional[Dict[str, Any]] = None
        auth_obj: Optional[Dict[str, Any]] = None

        # terminal presence
        has_broker = p_broker.exists()
        has_veto = p_veto.exists()
        has_exec = p_exec.exists()

        if not has_broker and not has_veto:
            continue

        # parse/validate what exists; any schema/parse failure -> tombstone
        try:
            if has_broker:
                broker_obj = _read_json_obj(p_broker)
                validate_against_repo_schema_v1(broker_obj, REPO_ROOT, "constellation_2/schemas/broker_submission_record.v2.schema.json")
                _require_nonempty_hash(broker_obj, "canonical_json_hash", "broker_submission_record")

            if has_exec:
                exec_obj = _read_json_obj(p_exec)
                validate_against_repo_schema_v1(exec_obj, REPO_ROOT, "constellation_2/schemas/execution_event_record.v1.schema.json")
                _require_nonempty_hash(exec_obj, "canonical_json_hash", "execution_event_record")

            if has_veto:
                veto_obj = _read_json_obj(p_veto)
                validate_against_repo_schema_v1(veto_obj, REPO_ROOT, "constellation_2/schemas/veto_record.v1.schema.json")
                _require_nonempty_hash(veto_obj, "canonical_json_hash", "veto_record")

            if p_auth.exists():
                auth_obj = _read_json_obj(p_auth)
                validate_against_repo_schema_v1(auth_obj, REPO_ROOT, "constellation_2/schemas/authorization_binding_record.v1.schema.json")
                _require_nonempty_hash(auth_obj, "canonical_json_hash", "authorization_binding_record")
        except Exception as e:  # noqa: BLE001
            # Tombstone for parse/schema issues
            orig_hash = _sha256_bytes(canonical_json_bytes_v1({"source_dir": str(sd)}))
            _ = _write_quarantine_tombstone(
                dp=dp,
                day_utc=day_utc,
                submission_id=submission_id,
                producer_repo=producer_repo,
                producer_sha=producer_sha,
                quarantine_reason="SCHEMA_FAILURE",
                original_hash=orig_hash,
                details={"source_dir": str(sd), "error": repr(e)},
            )
            if "SCHEMA_FAILURE" not in reason_codes:
                reason_codes.append("SCHEMA_FAILURE")
            status = "FAIL_SCHEMA_VIOLATION"
            continue

        # day derivation must match requested day_utc
        try:
            derived_day = _derive_day_utc_from_inputs(broker_obj, exec_obj, veto_obj)
        except Exception as e:  # noqa: BLE001
            orig_hash = _sha256_bytes(canonical_json_bytes_v1({"source_dir": str(sd)}))
            _ = _write_quarantine_tombstone(
                dp=dp,
                day_utc=day_utc,
                submission_id=submission_id,
                producer_repo=producer_repo,
                producer_sha=producer_sha,
                quarantine_reason="PARSE_FAILURE",
                original_hash=orig_hash,
                details={"source_dir": str(sd), "error": repr(e)},
            )
            if "PARSE_FAILURE" not in reason_codes:
                reason_codes.append("PARSE_FAILURE")
            status = "FAIL_SCHEMA_VIOLATION"
            continue

        if derived_day != day_utc:
            continue

        # integrity checks: mixed terminal states forbidden
        if has_broker and has_veto:
            orig_hash = _sha256_bytes(canonical_json_bytes_v1({"source_dir": str(sd)}))
            _ = _write_quarantine_tombstone(
                dp=dp,
                day_utc=day_utc,
                submission_id=submission_id,
                producer_repo=producer_repo,
                producer_sha=producer_sha,
                quarantine_reason="INTEGRITY_FAILURE",
                original_hash=orig_hash,
                details={"source_dir": str(sd), "reason": "BROKER_AND_VETO_BOTH_PRESENT"},
            )
            if "INTEGRITY_FAILURE" not in reason_codes:
                reason_codes.append("INTEGRITY_FAILURE")
            status = "FAIL_SCHEMA_VIOLATION"
            continue

        # authorization binding required for any canonical publish
        if auth_obj is None:
            orig_hash = _sha256_bytes(canonical_json_bytes_v1({"source_dir": str(sd)}))
            _ = _write_quarantine_tombstone(
                dp=dp,
                day_utc=day_utc,
                submission_id=submission_id,
                producer_repo=producer_repo,
                producer_sha=producer_sha,
                quarantine_reason="INTEGRITY_FAILURE",
                original_hash=orig_hash,
                details={"source_dir": str(sd), "reason": "MISSING_AUTHORIZATION_BINDING_RECORD"},
            )
            if "MISSING_AUTHORIZATION_BINDING_RECORD" not in reason_codes:
                reason_codes.append("MISSING_AUTHORIZATION_BINDING_RECORD")
            status = "FAIL_SCHEMA_VIOLATION"
            continue

        # Build canonical bundle in temp dir then atomic rename
        final_dir = submission_artifact_dir_v1(day_utc=day_utc, submission_id=submission_id)
        tmp_dir = _canonical_tmp_dir(dp, submission_id)
        tmp_dir.mkdir(parents=True, exist_ok=False)

        try:
            # Always mirror auth binding
            b_auth = p_auth.read_bytes()
            wr_auth = write_file_immutable_v1(path=tmp_dir / "authorization_binding_record.v1.json", data=b_auth, create_dirs=True)

            wr_broker = None
            wr_exec = None
            wr_veto = None
            wr_noexec = None

            if has_veto:
                # NO_BROKER_ACTION bundle: auth + veto
                b_veto = p_veto.read_bytes()
                wr_veto = write_file_immutable_v1(path=tmp_dir / "veto_record.v1.json", data=b_veto, create_dirs=True)
            else:
                # broker bundle: auth + broker (+ exec or no_exec)
                b_broker = p_broker.read_bytes()
                wr_broker = write_file_immutable_v1(path=tmp_dir / "broker_submission_record.v2.json", data=b_broker, create_dirs=True)

                if has_exec:
                    b_exec = p_exec.read_bytes()
                    wr_exec = write_file_immutable_v1(path=tmp_dir / "execution_event_record.v1.json", data=b_exec, create_dirs=True)
                else:
                    # explicit NO_EXECUTION_EVENT
                    noexec_obj = _write_no_execution_event(
                        day_utc=day_utc,
                        submission_id=submission_id,
                        reason_code="NO_EXECUTION_EVENT_PRESENT_IN_PHASED",
                        reason_detail=f"PhaseD submission dir missing execution_event_record: {str(sd)}",
                    )
                    b_noexec = canonical_json_bytes_v1(noexec_obj) + b"\n"
                    wr_noexec = write_file_immutable_v1(path=tmp_dir / "no_execution_event.v1.json", data=b_noexec, create_dirs=True)
                    status = "DEGRADED_MISSING_EXECUTION_EVENT"
                    if "MISSING_EXECUTION_EVENT" not in reason_codes:
                        reason_codes.append("MISSING_EXECUTION_EVENT")

            # Mirror identity inputs when present.
            ptr_plan_v1 = _maybe_copy_identity_file(src_dir=sd, dst_dir=tmp_dir, filename="order_plan.v1.json")
            ptr_equity_plan_v1 = _maybe_copy_identity_file(src_dir=sd, dst_dir=tmp_dir, filename="equity_order_plan.v1.json")

            ptr_bind_v1 = _maybe_copy_identity_file(src_dir=sd, dst_dir=tmp_dir, filename="binding_record.v1.json")
            ptr_bind_v2 = _maybe_copy_identity_file(src_dir=sd, dst_dir=tmp_dir, filename="binding_record.v2.json")

            ptr_map_v1 = _maybe_copy_identity_file(src_dir=sd, dst_dir=tmp_dir, filename="mapping_ledger_record.v1.json")
            ptr_map_v2 = _maybe_copy_identity_file(src_dir=sd, dst_dir=tmp_dir, filename="mapping_ledger_record.v2.json")

            manifest_ptr_plan = ptr_plan_v1
            manifest_ptr_bind = ptr_bind_v1 if ptr_bind_v1 is not None else ptr_bind_v2
            manifest_ptr_map = ptr_map_v1 if ptr_map_v1 is not None else ptr_map_v2

            # Publish directory atomically
            _atomic_publish_dir(tmp_dir, final_dir)

            # Build manifest (after publish so paths are final)
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
                    "artifact_dir": str(final_dir),
                    "broker_submission_record": None if wr_broker is None else {"path": str(final_dir / "broker_submission_record.v2.json"), "sha256": wr_broker.sha256},
                    "execution_event_record": None if wr_exec is None else {"path": str(final_dir / "execution_event_record.v1.json"), "sha256": wr_exec.sha256},
                    "veto_record": None if wr_veto is None else {"path": str(final_dir / "veto_record.v1.json"), "sha256": wr_veto.sha256},
                    "order_plan": manifest_ptr_plan,
                    "binding_record": manifest_ptr_bind,
                    "mapping_ledger_record": manifest_ptr_map,
                },
            }

            # Manifest:
            # - broker bundles use manifest v2 (existing schema)
            # - veto-only bundles use manifest v3 (broker ptr nullable)
            if has_veto:
                manifest_obj["schema_id"] = "C2_EXECUTION_EVIDENCE_SUBMISSION_MANIFEST_V3"
                manifest_obj["schema_version"] = 3
                validate_against_repo_schema_v1(manifest_obj, REPO_ROOT, SCHEMA_SUBMISSION_MANIFEST_V3)
            else:
                validate_against_repo_schema_v1(manifest_obj, REPO_ROOT, SCHEMA_SUBMISSION_MANIFEST_V2)

            validate_against_repo_schema_v1(manifest_obj, REPO_ROOT, SCHEMA_SUBMISSION_MANIFEST_V2)

            m_path = submission_manifest_path_v1(day_utc=day_utc, submission_id=submission_id)

            if m_path.exists() and m_path.is_file():
                ex_manifest = _read_json_obj(m_path)
                _ = _validate_manifest_any_version(ex_manifest)

                patch_path = submission_manifest_identity_patch_path_v1(day_utc=day_utc, submission_id=submission_id)
                need_patch = False
                if (
                    (ptr_plan_v1 is not None)
                    or (ptr_equity_plan_v1 is not None)
                    or (ptr_bind_v1 is not None)
                    or (ptr_bind_v2 is not None)
                    or (ptr_map_v1 is not None)
                    or (ptr_map_v2 is not None)
                ):
                    need_patch = True

                if need_patch:
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
                                "order_plan": ptr_plan_v1,
                                "binding_record": manifest_ptr_bind,
                                "mapping_ledger_record": manifest_ptr_map,
                            },
                        }
                        validate_against_repo_schema_v1(patch_obj, REPO_ROOT, SCHEMA_MANIFEST_ID_PATCH_V1)
                        p_bytes = canonical_json_bytes_v1(patch_obj) + b"\n"
                        wr_p = write_file_immutable_v1(path=patch_path, data=p_bytes, create_dirs=True)
                        written_patches.append((submission_id, wr_p.sha256))
                continue

            m_bytes = canonical_json_bytes_v1(manifest_obj) + b"\n"
            wr_m = write_file_immutable_v1(path=m_path, data=m_bytes, create_dirs=True)
            written_manifests.append((submission_id, wr_m.sha256))

        except Exception as e:  # noqa: BLE001
            # If tmp_dir was created but not published, clean it up best-effort (non-truth)
            try:
                if tmp_dir.exists() and tmp_dir.is_dir():
                    for p in tmp_dir.rglob("*"):
                        if p.is_file():
                            p.unlink()
                    tmp_dir.rmdir()
            except Exception:
                pass

            orig_hash = _sha256_bytes(canonical_json_bytes_v1({"source_dir": str(sd)}))
            _ = _write_quarantine_tombstone(
                dp=dp,
                day_utc=day_utc,
                submission_id=submission_id,
                producer_repo=producer_repo,
                producer_sha=producer_sha,
                quarantine_reason="INTEGRITY_FAILURE",
                original_hash=orig_hash,
                details={"source_dir": str(sd), "error": repr(e)},
            )
            if "INTEGRITY_FAILURE" not in reason_codes:
                reason_codes.append("INTEGRITY_FAILURE")
            status = "FAIL_SCHEMA_VIOLATION"
            continue

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

    if not dp.latest_path.exists():
        _ = write_file_immutable_v1(path=dp.latest_path, data=latest_bytes, create_dirs=True)

    print("OK: EXECUTION_EVIDENCE_DAY_COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
