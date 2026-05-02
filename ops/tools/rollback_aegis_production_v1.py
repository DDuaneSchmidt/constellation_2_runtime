#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.aegis_runtime_mode_v1 import (
    PRODUCTION_TRUTH_ROOT,
    git_commit_v1,
    now_iso_v1,
    production_version_path_v1,
    read_json_v1,
    write_json_v1,
)
from ops.tools.aegis_submit_enforcement_v1 import packet_currentness_v1
from ops.tools.promote_aegis_candidate_to_production_v1 import promotion_manifest_path


def rollback_manifest_path(production_root: Path, promotion_id: str) -> Path:
    return (production_root / "governance" / "rollbacks" / f"{promotion_id}.json").resolve()


def rollback_aegis_production_v1(*, promotion_id: str, production_root: Path, rolled_back_by: str = "operator") -> dict[str, Any]:
    version_path = production_version_path_v1(production_root)
    version = read_json_v1(version_path)
    rollback_commit = str(version.get("rollback_commit") or "").strip()
    current_promoted = str(version.get("promoted_commit") or "").strip()
    if not version:
        raise SystemExit("FAIL: PRODUCTION_VERSION_MISSING " + str(version_path))
    if not rollback_commit:
        raise SystemExit("FAIL: ROLLBACK_COMMIT_MISSING " + str(version_path))

    prior_manifest_path = promotion_manifest_path(production_root, str(version.get("promotion_id") or promotion_id))
    prior_manifest = read_json_v1(prior_manifest_path)
    if prior_manifest:
        prior_manifest["status"] = "ROLLED_BACK"
        prior_manifest["rolled_back_at_utc"] = now_iso_v1()
        prior_manifest["rolled_back_by"] = rolled_back_by
        write_json_v1(prior_manifest_path, prior_manifest)

    restored_version = {
        "schema_id": "production_version",
        "schema_version": "production_version.v1",
        "promoted_commit": rollback_commit,
        "promoted_at_utc": now_iso_v1(),
        "promoted_by": rolled_back_by,
        "promotion_id": f"rollback:{promotion_id}",
        "rollback_commit": current_promoted,
        "status": "ACTIVE",
    }
    write_json_v1(version_path, restored_version)

    manifest = {
        "schema_id": "aegis_production_rollback_manifest",
        "schema_version": "aegis_production_rollback_manifest.v1",
        "promotion_id": promotion_id,
        "previous_promoted_commit": current_promoted,
        "restored_promoted_commit": rollback_commit,
        "production_version_path": str(version_path),
        "prior_promotion_manifest_path": str(prior_manifest_path),
        "generated_at_utc": now_iso_v1(),
        "status": "ROLLED_BACK",
        "current_repo_commit": git_commit_v1(),
    }
    manifest_path = rollback_manifest_path(production_root, promotion_id)
    write_json_v1(manifest_path, manifest)

    env = dict(os.environ)
    env["AEGIS_RUNTIME_MODE"] = "PRODUCTION"
    env["AEGIS_PACKET_ROOT"] = str(production_root)
    packet = subprocess.run([sys.executable, "ops/tools/aegis_chatgpt_packet.py"], cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, check=False)
    manifest["packet_returncode"] = int(packet.returncode)
    manifest["packet_stdout"] = str(packet.stdout or "").strip()[-1200:]
    manifest["packet_stderr"] = str(packet.stderr or "").strip()[-1200:]
    manifest["packet_currentness"] = packet_currentness_v1(runtime_root=production_root, runtime_mode="PRODUCTION")
    write_json_v1(manifest_path, manifest)
    if packet.returncode != 0:
        raise SystemExit("FAIL: PRODUCTION_ROLLBACK_PACKET_REGEN_FAILED " + manifest["packet_stderr"])
    if manifest["packet_currentness"]["status"] != "CURRENT":
        raise SystemExit("FAIL: PRODUCTION_ROLLBACK_PACKET_NOT_CURRENT " + json.dumps(manifest["packet_currentness"], sort_keys=True))
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--promotion_id", required=True)
    parser.add_argument("--production_root", default="")
    parser.add_argument("--rolled_back_by", default="operator")
    args = parser.parse_args(argv)
    payload = rollback_aegis_production_v1(
        promotion_id=args.promotion_id,
        production_root=Path(args.production_root or PRODUCTION_TRUTH_ROOT).resolve(),
        rolled_back_by=args.rolled_back_by,
    )
    print(json.dumps({"promotion_id": payload["promotion_id"], "status": payload["status"], "restored_promoted_commit": payload["restored_promoted_commit"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
