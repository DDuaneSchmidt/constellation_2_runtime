from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.execution_evidence.lib.paths_v1 import REPO_ROOT


SCHEMA_RELPATH_FAILURE = (
    "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_failure.v1.schema.json"
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def build_failure_obj_v1(
    *,
    day_utc: str,
    producer_repo: str,
    producer_git_sha: str,
    producer_module: str,
    status: str,
    reason_codes: List[str],
    input_manifest: List[Dict[str, Any]],
    code: str,
    message: str,
    details: Dict[str, Any],
    attempted_outputs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "schema_id": "C2_EXECUTION_EVIDENCE_FAILURE_V1",
        "schema_version": 1,
        "produced_utc": _utc_now_iso(),
        "day_utc": day_utc,
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_git_sha,
            "module": producer_module,
        },
        "status": status,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "failure": {
            "code": code,
            "message": message,
            "details": details,
            "attempted_outputs": attempted_outputs,
        },
    }


def validate_failure_obj_or_raise_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_FAILURE)


def write_failure_immutable_v1(*, failure_path: Path, failure_obj: Dict[str, Any]) -> str:
    validate_failure_obj_or_raise_v1(failure_obj)
    try:
        b = canonical_json_bytes_v1(failure_obj) + b"\n"
    except CanonicalizationError as e:
        raise RuntimeError(f"FAILURE_CANONICALIZATION_ERROR: {e}") from e

    res = write_file_immutable_v1(path=failure_path, data=b, create_dirs=True)
    return res.sha256
