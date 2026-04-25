#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.truth_root_v1 import resolve_truth_root


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/sleeve_allocation_epoch.v1.schema.json"
POLICY_RELPATH = "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json"
SLEEVE_REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"


def _parse_day(day: str) -> str:
    value = str(day or "").strip()
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {value!r}")
    return value


def _require_truth_root(raw: str) -> Path:
    if str(raw or "").strip():
        path = Path(raw).expanduser().resolve()
    else:
        path = resolve_truth_root(repo_root=REPO_ROOT).resolve()
    if not path.is_absolute() or not path.exists() or not path.is_dir():
        raise SystemExit(f"FAIL: invalid truth_root: {path}")
    return path


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_sha() -> str:
    try:
        output = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        value = output.decode("utf-8").strip()
    except Exception:
        value = "0" * 40
    if len(value) != 40:
        value = "0" * 40
    return value


def _write_immutable(path: Path, payload: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    candidate_sha = _sha256_bytes(payload)
    if path.exists():
        if not path.is_file():
            raise SystemExit(f"FAIL: TARGET_NOT_FILE: {path}")
        existing_sha = _sha256_file(path)
        if existing_sha == candidate_sha:
            return candidate_sha
        raise SystemExit(
            f"FAIL: ATTEMPTED_REWRITE_IMMUTABLE path={path} existing_sha={existing_sha} candidate_sha={candidate_sha}"
        )
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    tmp.replace(path)
    return candidate_sha


def _manifest_entry(*, type_name: str, path: Path, day_utc: str | None, producer: str) -> Dict[str, Any]:
    return {
        "type": type_name,
        "path": str(path),
        "sha256": _sha256_file(path),
        "day_utc": day_utc,
        "producer": producer,
    }


def _single_epoch_source(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise SystemExit(f"FAIL: PORTFOLIO_SNAPSHOT_MISSING: {path}")
    obj = _read_json_obj(path)
    if str(obj.get("schema_id") or "").strip() != "C2_PORTFOLIO_GOVERNANCE_SNAPSHOT_V1":
        raise SystemExit("FAIL: PORTFOLIO_SNAPSHOT_SCHEMA_ID_INVALID")
    return obj


def _epoch_id(day_utc: str, allocation_sha: str, portfolio_snapshot_id: str, policy_sha: str, sleeve_sha: str, gate_sha: str) -> str:
    seed = "\n".join(
        [
            "C2_SLEEVE_ALLOCATION_EPOCH_V1",
            day_utc,
            allocation_sha,
            portfolio_snapshot_id,
            policy_sha,
            sleeve_sha,
            gate_sha,
        ]
    ).encode("utf-8")
    return hashlib.sha256(seed).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_sleeve_allocation_epoch_day_v1")
    ap.add_argument("--day", required=True, help="UTC day in YYYY-MM-DD")
    ap.add_argument("--truth_root", default="", help="Optional explicit truth root")
    args = ap.parse_args()

    day_utc = _parse_day(args.day)
    truth_root = _require_truth_root(args.truth_root)

    allocation_path = (
        truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day_utc / "capital_authority_allocation.v1.json"
    ).resolve()
    portfolio_snapshot_path = (
        truth_root / "risk_v1" / "portfolio_governance_snapshot_v1" / day_utc / "portfolio_governance_snapshot.v1.json"
    ).resolve()
    policy_path = (REPO_ROOT / POLICY_RELPATH).resolve()
    sleeve_registry_path = (REPO_ROOT / SLEEVE_REGISTRY_RELPATH).resolve()

    for required_path in (allocation_path, policy_path, sleeve_registry_path):
        if not required_path.exists() or not required_path.is_file():
            raise SystemExit(f"FAIL: REQUIRED_INPUT_MISSING: {required_path}")

    allocation = _read_json_obj(allocation_path)
    portfolio_snapshot = _single_epoch_source(portfolio_snapshot_path)

    correlation_gate_binding = allocation.get("correlation_gate_binding")
    if not isinstance(correlation_gate_binding, dict):
        raise SystemExit("FAIL: CORRELATION_GATE_BINDING_MISSING")
    correlation_gate_hash = str(correlation_gate_binding.get("gate_artifact_sha256") or "").strip()
    if len(correlation_gate_hash) != 64:
        raise SystemExit("FAIL: CORRELATION_GATE_HASH_INVALID")

    portfolio = allocation.get("portfolio")
    if not isinstance(portfolio, dict):
        raise SystemExit("FAIL: ALLOCATION_PORTFOLIO_BLOCK_MISSING")
    per_sleeve = allocation.get("per_sleeve")
    if not isinstance(per_sleeve, list) or not per_sleeve:
        raise SystemExit("FAIL: ALLOCATION_PER_SLEEVE_MISSING")

    policy_sha = _sha256_file(policy_path)
    sleeve_sha = _sha256_file(sleeve_registry_path)
    allocation_sha = _sha256_file(allocation_path)
    portfolio_snapshot_id = str(portfolio_snapshot.get("portfolio_snapshot_id") or "").strip()
    if not portfolio_snapshot_id:
        raise SystemExit("FAIL: PORTFOLIO_SNAPSHOT_ID_MISSING")

    output_obj = {
        "schema_id": "C2_SLEEVE_ALLOCATION_EPOCH_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": str(allocation.get("produced_utc") or ""),
        "epoch_id": _epoch_id(day_utc, allocation_sha, portfolio_snapshot_id, policy_sha, sleeve_sha, correlation_gate_hash),
        "portfolio_snapshot_id": portfolio_snapshot_id,
        "policy_hash": policy_sha,
        "sleeve_registry_hash": sleeve_sha,
        "correlation_gate_hash": correlation_gate_hash,
        "portfolio_allowed_capital_at_risk_cents": int(portfolio["allowed_capital_at_risk_cents"]),
        "portfolio_used_capital_at_risk_cents": int(portfolio["used_capital_at_risk_cents"]),
        "portfolio_headroom_cents": int(portfolio["headroom_cents"]),
        "per_sleeve": [
            {
                "canonical_sleeve_id": str(item["sleeve_id"]),
                "engine_ids": [str(engine_id) for engine_id in item["engine_ids"]],
                "assigned_budget_cents": int(item["allowed_capital_at_risk_cents"]),
                "used_budget_cents": int(item["used_capital_at_risk_cents"]),
                "remaining_budget_cents": int(item["headroom_cents"]),
            }
            for item in per_sleeve
        ],
        "input_manifest": [
            _manifest_entry(
                type_name="capital_authority_allocation",
                path=allocation_path,
                day_utc=day_utc,
                producer="allocation_v1",
            ),
            _manifest_entry(
                type_name="portfolio_governance_snapshot",
                path=portfolio_snapshot_path,
                day_utc=day_utc,
                producer="risk_v1",
            ),
            _manifest_entry(
                type_name="policy_manifest",
                path=policy_path,
                day_utc=None,
                producer="governance",
            ),
            _manifest_entry(
                type_name="sleeve_registry",
                path=sleeve_registry_path,
                day_utc=None,
                producer="governance",
            ),
        ],
    }
    validate_against_repo_schema_v1(output_obj, REPO_ROOT, SCHEMA_RELPATH)
    out_path = (
        truth_root / "allocation_v1" / "sleeve_allocation_epoch_v1" / day_utc / f"{output_obj['epoch_id']}.sleeve_allocation_epoch.v1.json"
    ).resolve()
    payload = canonical_json_bytes_v1(output_obj) + b"\n"
    out_sha = _write_immutable(out_path, payload)
    print(
        "OK: SLEEVE_ALLOCATION_EPOCH_WRITTEN "
        f"day_utc={day_utc} path={out_path} sha256={out_sha} git_sha={_git_sha()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
