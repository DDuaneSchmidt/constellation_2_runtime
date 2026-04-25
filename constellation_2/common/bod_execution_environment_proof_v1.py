from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

from constellation_2.common.paper_session_fact_plane_v1 import (
    NON_AUTHORITY_SCOPE,
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    producer_block_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_bod_execution_environment_proof_path,
)


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/bod_execution_environment_proof.v1.schema.json"
BRIDGE_MODULE = "ops/tools/bridge_accounting_nav_v2_to_compat_v1.py"


def _bridge_probe_command(*, repo_root: Path, truth_root: Path, day_utc: str) -> list[str]:
    return [
        sys.executable,
        str((Path(repo_root).resolve() / BRIDGE_MODULE).resolve()),
        "--day_utc",
        str(day_utc).strip(),
        "--truth_root",
        str(Path(truth_root).resolve()),
        "--proof-mode",
        "import_only",
    ]


def derive_bod_execution_environment_proof_payload(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
) -> Dict[str, Any]:
    normalized_repo_root = Path(repo_root).resolve()
    normalized_truth_root = Path(truth_root).resolve()
    produced_at_utc = now_utc_iso_v1()

    import_ok = True
    import_error = ""
    try:
        import constellation_2  # noqa: F401
    except Exception as exc:  # pragma: no cover - exercised via failure-path tests
        import_ok = False
        import_error = f"{type(exc).__name__}: {exc}"

    cmd = _bridge_probe_command(
        repo_root=normalized_repo_root,
        truth_root=normalized_truth_root,
        day_utc=day_utc,
    )
    bridge_probe = subprocess.run(
        cmd,
        cwd=str(normalized_repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    bridge_payload: Dict[str, Any] = {}
    if bridge_probe.returncode == 0 and bridge_probe.stdout.strip():
        try:
            maybe_payload = json.loads(bridge_probe.stdout.strip().splitlines()[-1])
            if isinstance(maybe_payload, dict):
                bridge_payload = maybe_payload
        except json.JSONDecodeError:
            bridge_payload = {}

    blocking_codes: list[str] = []
    if not import_ok:
        blocking_codes.append("BOD_EXECUTION_SUBSTRATE_PROOF_FAIL:CONSTELLATION_2_IMPORT_FAIL")
    if bridge_probe.returncode != 0:
        blocking_codes.append("BOD_EXECUTION_SUBSTRATE_PROOF_FAIL:BRIDGE_IMPORT_PROBE_NONZERO")

    status = "PASS" if not blocking_codes else "BLOCKED_BY_DEFECT"
    return {
        "schema_id": "bod_execution_environment_proof",
        "schema_version": "v1",
        "authority_scope": NON_AUTHORITY_SCOPE,
        "day_utc": str(day_utc).strip(),
        "status": status,
        "blocking_codes": blocking_codes,
        "producer": producer_block_v1(module="constellation_2/common/bod_execution_environment_proof_v1.py"),
        "produced_at_utc": produced_at_utc,
        "python_executable": sys.executable,
        "repo_root": str(normalized_repo_root),
        "cwd": str(Path.cwd().resolve()),
        "pythonpath": os.environ.get("PYTHONPATH", ""),
        "virtual_env": os.environ.get("VIRTUAL_ENV", ""),
        "environment_snapshot": {
            "HOME": os.environ.get("HOME", ""),
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
            "VIRTUAL_ENV": os.environ.get("VIRTUAL_ENV", ""),
        },
        "sys_path_head": sys.path[:8],
        "constellation_2_import": {
            "ok": import_ok,
            "error": import_error,
        },
        "bridge_import_probe": {
            "cmd": cmd,
            "cwd": str(normalized_repo_root),
            "returncode": int(bridge_probe.returncode),
            "stdout": bridge_probe.stdout.strip(),
            "stderr": bridge_probe.stderr.strip(),
            "proof_payload": bridge_payload,
        },
        "human_readable_summary": (
            f"BOD execution substrate proof PASS for {day_utc}"
            if status == "PASS"
            else f"BOD execution substrate proof BLOCKED for {day_utc}: {', '.join(blocking_codes)}"
        ),
    }


def write_bod_execution_environment_proof_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_bod_execution_environment_proof_path(
            truth_root=truth_root,
            day_utc=str(payload.get("day_utc") or "").strip(),
        ),
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("produced_at_utc",),
    )
