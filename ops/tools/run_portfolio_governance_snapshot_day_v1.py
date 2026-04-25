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


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/portfolio_governance_snapshot.v1.schema.json"
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


def _portfolio_snapshot_id(day_utc: str, envelope_sha: str, policy_sha: str, sleeve_sha: str) -> str:
    seed = "\n".join(
        [
            "C2_PORTFOLIO_GOVERNANCE_SNAPSHOT_V1",
            day_utc,
            envelope_sha,
            policy_sha,
            sleeve_sha,
        ]
    ).encode("utf-8")
    return hashlib.sha256(seed).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_portfolio_governance_snapshot_day_v1")
    ap.add_argument("--day", required=True, help="UTC day in YYYY-MM-DD")
    ap.add_argument("--truth_root", default="", help="Optional explicit truth root")
    args = ap.parse_args()

    day_utc = _parse_day(args.day)
    truth_root = _require_truth_root(args.truth_root)

    envelope_path = (truth_root / "reports" / "capital_risk_envelope_v2" / day_utc / "capital_risk_envelope.v2.json").resolve()
    policy_path = (REPO_ROOT / POLICY_RELPATH).resolve()
    sleeve_registry_path = (REPO_ROOT / SLEEVE_REGISTRY_RELPATH).resolve()
    out_path = (
        truth_root
        / "risk_v1"
        / "portfolio_governance_snapshot_v1"
        / day_utc
        / "portfolio_governance_snapshot.v1.json"
    ).resolve()

    for required_path in (envelope_path, policy_path, sleeve_registry_path):
        if not required_path.exists() or not required_path.is_file():
            raise SystemExit(f"FAIL: REQUIRED_INPUT_MISSING: {required_path}")

    envelope = _read_json_obj(envelope_path)
    policy = _read_json_obj(policy_path)
    _ = _read_json_obj(sleeve_registry_path)

    if str(policy.get("schema_id") or "").strip() != "C2_CAPITAL_AUTHORITY_POLICY_V1":
        raise SystemExit("FAIL: POLICY_SCHEMA_ID_INVALID")

    envelope_block = envelope.get("envelope")
    if not isinstance(envelope_block, dict):
        raise SystemExit("FAIL: ENVELOPE_BLOCK_MISSING")

    risk_budget_total = int(policy["portfolio_limits"]["max_capital_at_risk_cents"])
    risk_budget_effective = int(envelope_block["allowed_capital_at_risk_cents"])
    policy_sha = _sha256_file(policy_path)
    sleeve_sha = _sha256_file(sleeve_registry_path)
    envelope_sha = _sha256_file(envelope_path)

    output_obj = {
        "schema_id": "C2_PORTFOLIO_GOVERNANCE_SNAPSHOT_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": str(envelope.get("produced_utc") or ""),
        "portfolio_snapshot_id": _portfolio_snapshot_id(day_utc, envelope_sha, policy_sha, sleeve_sha),
        "policy_hash": policy_sha,
        "sleeve_registry_hash": sleeve_sha,
        "risk_budget_total": risk_budget_total,
        "risk_budget_effective": risk_budget_effective,
        "drawdown_scaling": str(envelope_block.get("multiplier") or ""),
        "correlation_compression": str(policy["portfolio_limits"]["max_pairwise_correlation_proxy"]),
        "hard_stop_state": str(envelope.get("status") or ""),
        "reserve_buffer": max(0, risk_budget_total - risk_budget_effective),
        "input_manifest": [
            _manifest_entry(
                type_name="capital_risk_envelope_v2",
                path=envelope_path,
                day_utc=day_utc,
                producer="reports/capital_risk_envelope_v2",
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
    payload = canonical_json_bytes_v1(output_obj) + b"\n"
    out_sha = _write_immutable(out_path, payload)
    print(
        "OK: PORTFOLIO_GOVERNANCE_SNAPSHOT_WRITTEN "
        f"day_utc={day_utc} path={out_path} sha256={out_sha} git_sha={_git_sha()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
