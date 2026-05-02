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
    CANDIDATE_TRUTH_ROOT,
    PRODUCTION_TRUTH_ROOT,
    copy_candidate_artifacts_v1,
    git_commit_v1,
    production_version_path_v1,
    read_json_v1,
    write_json_v1,
    now_iso_v1,
)
from ops.tools.aegis_submit_enforcement_v1 import packet_currentness_v1
from ops.tools.run_aegis_production_promotion_gate_v1 import promotion_gate_path
from ops.tools.run_aegis_promotion_validation_ledger_v1 import promotion_validation_ledger_path


def promotion_manifest_path(production_root: Path, promotion_id: str) -> Path:
    return (production_root / "governance" / "promotions" / f"{promotion_id}.json").resolve()


def _default_artifacts(day_utc: str) -> list[str]:
    return [
        f"reports/aegis_day_run_v1/{day_utc}/day_run.v1.json",
        f"reports/aegis_control_plane_v1/{day_utc}/control_plane.v1.json",
        f"reports/aegis_operator_projection_v1/{day_utc}/operator_projection.v1.json",
        f"reports/aegis_requirement_graph_v1/{day_utc}/requirement_graph.v1.json",
        f"reports/submit_boundary_status_v1/{day_utc}/submit_boundary_status.v1.json",
        f"reports/action_validity_v1/{day_utc}/action_validity.v1.json",
        f"reports/truth_freshness_v1/{day_utc}/truth_freshness.v1.json",
        f"reports/aegis_promotion_validation_ledger_v1/{day_utc}/promotion_validation_ledger.v1.json",
    ]


def promote_candidate_to_production_v1(*, day_utc: str, promotion_id: str, candidate_root: Path, production_root: Path, promoted_by: str = "operator") -> dict[str, Any]:
    gate = read_json_v1(promotion_gate_path(candidate_root, day_utc, promotion_id))
    if gate.get("promotion_status") != "APPROVED_FOR_PROMOTION":
        raise SystemExit("FAIL: PROMOTION_GATE_NOT_APPROVED " + json.dumps({"promotion_id": promotion_id, "status": gate.get("promotion_status")}, sort_keys=True))
    commit = git_commit_v1()
    copied = copy_candidate_artifacts_v1(candidate_root=candidate_root, production_root=production_root, relative_paths=_default_artifacts(day_utc))
    version_path = production_version_path_v1(production_root)
    prior = read_json_v1(version_path)
    version = {
        "schema_id": "production_version",
        "schema_version": "production_version.v1",
        "promoted_commit": commit,
        "promoted_at_utc": now_iso_v1(),
        "promoted_by": promoted_by,
        "promotion_id": promotion_id,
        "rollback_commit": str(gate.get("rollback_commit") or prior.get("promoted_commit") or ""),
        "status": "ACTIVE",
    }
    write_json_v1(version_path, version)
    manifest = {
        "schema_id": "aegis_production_promotion_manifest",
        "schema_version": "aegis_production_promotion_manifest.v1",
        "promotion_id": promotion_id,
        "day_utc": day_utc,
        "promoted_commit": commit,
        "production_version_path": str(version_path),
        "copied_artifacts": copied,
        "generated_at_utc": now_iso_v1(),
        "status": "PROMOTED",
    }
    manifest_path = promotion_manifest_path(production_root, promotion_id)
    validation_ledger_src = read_json_v1(promotion_validation_ledger_path(truth_root=candidate_root, day_utc=day_utc))
    if validation_ledger_src:
        validation_ledger_src["promotion_status"] = "PROMOTED"
        validation_ledger_src["promoted_commit"] = commit
        validation_ledger_src["promotion_id"] = promotion_id
        validation_ledger_src["production_version_path"] = str(version_path)
        validation_ledger_src["truth_root"] = str(production_root.resolve())
        validation_ledger_src["runtime_root"] = str(production_root.resolve())
        validation_ledger_src["generated_at"] = now_iso_v1()
        write_json_v1(promotion_validation_ledger_path(truth_root=production_root, day_utc=day_utc), validation_ledger_src)
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
        raise SystemExit("FAIL: PRODUCTION_PACKET_REGEN_FAILED " + manifest["packet_stderr"])
    if manifest["packet_currentness"]["status"] != "CURRENT":
        raise SystemExit("FAIL: PRODUCTION_PACKET_NOT_CURRENT " + json.dumps(manifest["packet_currentness"], sort_keys=True))
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--promotion_id", required=True)
    parser.add_argument("--candidate_root", default="")
    parser.add_argument("--production_root", default="")
    parser.add_argument("--promoted_by", default="operator")
    args = parser.parse_args(argv)
    payload = promote_candidate_to_production_v1(
        day_utc=args.day_utc,
        promotion_id=args.promotion_id,
        candidate_root=Path(args.candidate_root or CANDIDATE_TRUTH_ROOT).resolve(),
        production_root=Path(args.production_root or PRODUCTION_TRUTH_ROOT).resolve(),
        promoted_by=args.promoted_by,
    )
    print(json.dumps({"promotion_id": payload["promotion_id"], "status": payload["status"], "promoted_commit": payload["promoted_commit"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
