#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.verified_runtime_graph_v1 import (  # noqa: E402
    DEFAULT_MODULES_ROOT,
    DEFAULT_TRUTH_ROOT,
    _artifact_day,
    _artifact_path_from_spec,
    _load_manifest,
)

OPERATOR_PORTAL_CAPABILITIES = {
    "hypothesis_proposal_promotion_pipeline_v1",
    "operator_decision_dashboard_v1",
}


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def build_pre_graph_evidence_report_v1(*, truth_root: Path, day_utc: str, modules_root: Path = DEFAULT_MODULES_ROOT) -> dict[str, Any]:
    manifest_path = modules_root / "operator_portal" / "aegis.module.yaml"
    manifest = _load_manifest(manifest_path)
    evidence_specs: dict[str, dict[str, Any]] = {}
    for spec in manifest.get("evidence_artifacts") or []:
        if not isinstance(spec, dict):
            continue
        evidence_id = str(spec.get("evidence_id") or spec.get("artifact_id") or "")
        if evidence_id:
            evidence_specs[evidence_id] = spec

    capabilities: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []
    for capability in manifest.get("capabilities") or []:
        if not isinstance(capability, dict):
            continue
        capability_id = str(capability.get("capability_id") or "")
        if capability_id not in OPERATOR_PORTAL_CAPABILITIES:
            continue
        rows: list[dict[str, Any]] = []
        for evidence_id in [str(item) for item in capability.get("required_evidence") or [] if str(item)]:
            spec = evidence_specs.get(evidence_id)
            if spec is None:
                row = {
                    "capability": capability_id,
                    "required_evidence": evidence_id,
                    "status": "MISSING_EVIDENCE_SPEC",
                    "path": "",
                    "generated_at": "",
                    "content_hash": "",
                }
                rows.append(row)
                missing_rows.append(row)
                continue
            path = _artifact_path_from_spec(truth_root=truth_root, day_utc=day_utc, spec=spec)
            generated_at = ""
            content_hash = ""
            status = "MISSING"
            if path.exists():
                try:
                    payload = _read_json(path)
                    artifact_day = _artifact_day(payload) or day_utc
                    generated_at = str(payload.get("generated_at") or payload.get("generated_at_utc") or "")
                    content_hash = str(payload.get("content_hash") or _file_hash(path))
                    status = "PRESENT" if artifact_day == day_utc else "STALE"
                except (OSError, ValueError, json.JSONDecodeError):
                    status = "INVALID"
                    content_hash = _file_hash(path) if path.exists() else ""
            row = {
                "capability": capability_id,
                "required_evidence": evidence_id,
                "status": status,
                "path": str(path),
                "generated_at": generated_at,
                "content_hash": content_hash,
            }
            rows.append(row)
            if status != "PRESENT":
                missing_rows.append(row)
        capabilities.append({
            "capability": capability_id,
            "required_evidence_count": len(rows),
            "present_count": sum(1 for row in rows if row["status"] == "PRESENT"),
            "missing_count": sum(1 for row in rows if row["status"] != "PRESENT"),
            "evidence": rows,
        })
    return {
        "schema_id": "aegis_operator_portal_pre_graph_evidence_check_v1",
        "schema_version": "v1",
        "day_utc": day_utc,
        "status": "PASS" if not missing_rows else "FAIL",
        "capabilities": capabilities,
        "missing_evidence": missing_rows,
        "missing_count": len(missing_rows),
        "safety": {
            "read_only_check": True,
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "trade_advice_allowed": False,
            "real_capital_allocation_allowed": False,
            "autonomous_execution_allowed": False,
            "safety_gates_changed": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Check operator portal required evidence before verified graph generation.")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--modules-root", "--modules_root", dest="modules_root", default=str(DEFAULT_MODULES_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()
    report = build_pre_graph_evidence_report_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        modules_root=Path(args.modules_root),
    )
    print(json.dumps(report, sort_keys=True))
    if report["status"] != "PASS":
        for row in report["missing_evidence"]:
            print(
                "PRE_GRAPH_OPERATOR_PORTAL_EVIDENCE_MISSING:"
                f"{row['capability']}:{row['required_evidence']}:{row['status']}:{row['path']}",
                file=sys.stderr,
            )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
