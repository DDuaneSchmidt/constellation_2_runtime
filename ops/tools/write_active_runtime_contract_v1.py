#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_contract_v1 import (
    ACTIVE_RUNTIME_CONTRACT_PATH,
    write_active_runtime_contract_bytes,
)
from constellation_2.common.execution_identity_binding_v1 import (
    resolve_governed_execution_identity_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


ACTIVE_POINTER = Path("/home/node/constellation_active").resolve()
RUNTIME_DATA_ROOT = Path("/home/node/constellation_runtime_data").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json"
RELEASE_MANIFEST_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json"


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main() -> int:
    release_root = ACTIVE_POINTER
    if not release_root.exists() or not release_root.is_dir():
        raise SystemExit(f"FAIL: active release root missing: {release_root}")
    if not RUNTIME_DATA_ROOT.exists() or not RUNTIME_DATA_ROOT.is_dir():
        raise SystemExit(f"FAIL: runtime data root missing: {RUNTIME_DATA_ROOT}")

    manifest_path = (release_root / "release_manifest.v1.json").resolve()
    if not manifest_path.exists() or not manifest_path.is_file():
        raise SystemExit(f"FAIL: active release manifest missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_against_repo_schema_v1(manifest, release_root, RELEASE_MANIFEST_SCHEMA_RELPATH)

    canonical_truth_root = (RUNTIME_DATA_ROOT / "truth").resolve()
    truth_sleeves_root = (RUNTIME_DATA_ROOT / "truth_sleeves").resolve()
    if not canonical_truth_root.exists() or not canonical_truth_root.is_dir():
        raise SystemExit(f"FAIL: canonical truth root missing: {canonical_truth_root}")
    if not truth_sleeves_root.exists() or not truth_sleeves_root.is_dir():
        raise SystemExit(f"FAIL: truth sleeves root missing: {truth_sleeves_root}")

    payload = {
        "schema_id": "active_runtime_contract.v1",
        "schema_version": "v1",
        "release_id": str(manifest["release_id"]),
        "git_sha": str(manifest["git_sha"]),
        "authoritative_repo_root": str(REPO_ROOT),
        "release_root": str(release_root),
        "runtime_data_root": str(RUNTIME_DATA_ROOT),
        "canonical_truth_root": str(canonical_truth_root),
        "truth_sleeves_root": str(truth_sleeves_root),
        "pointer_index_family": "run_pointer_v1",
        "allowed_truth_roots": [
            str(canonical_truth_root),
            str(truth_sleeves_root),
        ],
        "provenance_mode": "release_manifest",
        "runtime_environment": "PAPER",
        "primary_execution_identity_ref": {
            "authority_owner": "execution_identity_binding_v1",
            "sleeve_id": "PRIMARY",
        },
        "generated_at_utc": _utc_now(),
        "status": "ACTIVE",
    }
    resolve_governed_execution_identity_v1(
        repo_root=REPO_ROOT,
        environment=str(payload["runtime_environment"]),
        sleeve_id=str(payload["primary_execution_identity_ref"]["sleeve_id"]),
    )
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)

    ACTIVE_RUNTIME_CONTRACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ACTIVE_RUNTIME_CONTRACT_PATH.write_bytes(write_active_runtime_contract_bytes(payload))
    print(json.dumps({"contract_path": str(ACTIVE_RUNTIME_CONTRACT_PATH), "release_id": payload["release_id"], "git_sha": payload["git_sha"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
