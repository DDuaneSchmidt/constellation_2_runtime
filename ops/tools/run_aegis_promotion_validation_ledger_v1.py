#!/usr/bin/env python3
from __future__ import annotations

import argparse
import inspect
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import constellation_2  # noqa: E402
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1  # noqa: E402
from ops.tools.aegis_runtime_mode_v1 import (  # noqa: E402
    CANDIDATE_TRUTH_ROOT,
    git_commit_v1,
    git_dirty_status_v1,
    now_iso_v1,
    read_json_v1,
    read_production_version_v1,
    write_json_v1,
)

SCHEMA_VERSION = "aegis_promotion_validation_ledger.v1"
REGISTRY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "aegis_readiness_domain_registry_v1.json"


def promotion_validation_ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "aegis_promotion_validation_ledger_v1" / day_utc / "promotion_validation_ledger.v1.json").resolve()


def _result(status: str, **fields: Any) -> dict[str, Any]:
    return {"status": status, **fields}


def _import_preflight_result_v1() -> dict[str, Any]:
    path = str(Path(inspect.getfile(constellation_2)).resolve())
    expected = str((REPO_ROOT / "constellation_2" / "__init__.py").resolve())
    return _result(
        "PASS" if path == expected else "FAIL",
        cwd=str(Path.cwd().resolve()),
        python=sys.executable,
        constellation_2_path=path,
        expected_constellation_2_path=expected,
    )


def _validate_registry_contract_v1() -> tuple[dict[str, Any], dict[str, Any]]:
    blockers: list[dict[str, str]] = []
    registry = read_json_v1(REGISTRY_PATH)
    owners: dict[str, list[str]] = {}
    required_fields = ("artifact_path", "expected_path", "governed_producer", "producer_command", "recovery_command", "recovery_action")
    schema_checked = 0
    for domain in registry.get("domains") if isinstance(registry.get("domains"), list) else []:
        if not isinstance(domain, dict):
            continue
        for dep in domain.get("dependencies") if isinstance(domain.get("dependencies"), list) else []:
            if not isinstance(dep, dict):
                continue
            dep_id = str(dep.get("dependency_id") or "").strip()
            owner = str(dep.get("domain_owner") or dep.get("owning_domain") or "").strip()
            owners.setdefault(dep_id, []).append(owner)
            for field in required_fields:
                if not str(dep.get(field) or "").strip():
                    blockers.append({"code": "REGISTRY_DEPENDENCY_FIELD_MISSING", "dependency_id": dep_id, "field": field})
            if dep.get("schema_exempt") is True:
                if not str(dep.get("schema_exempt_reason") or "").strip():
                    blockers.append({"code": "REGISTRY_SCHEMA_EXEMPTION_REASON_MISSING", "dependency_id": dep_id})
            else:
                schema_checked += 1
                schema_path = str(dep.get("schema_path") or "").strip()
                if not schema_path or not (REPO_ROOT / schema_path).exists():
                    blockers.append({"code": "REGISTRY_SCHEMA_PATH_MISSING", "dependency_id": dep_id, "schema_path": schema_path})
    for dep_id, owner_list in owners.items():
        if len(owner_list) != 1:
            blockers.append({"code": "REGISTRY_DEPENDENCY_OWNER_NOT_UNIQUE", "dependency_id": dep_id, "owners": ",".join(owner_list)})
    registry_result = _result("PASS" if not blockers else "FAIL", dependency_count=len(owners), blockers=blockers)
    schema_result = _result("PASS" if not any(row["code"].startswith("REGISTRY_SCHEMA") for row in blockers) else "FAIL", schema_checked=schema_checked)
    return registry_result, schema_result


def _control_plane_result_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = truth_root / "reports" / "aegis_control_plane_v1" / day_utc / "control_plane.v1.json"
    payload = read_json_v1(path)
    if not payload:
        return _result("FAIL", path=str(path), blocker="CONTROL_PLANE_MISSING")
    if str(payload.get("day_utc") or "") != day_utc:
        return _result("FAIL", path=str(path), blocker="CONTROL_PLANE_WRONG_DAY", observed_day=str(payload.get("day_utc") or ""))
    if str(payload.get("final_status") or "").strip().upper() not in {"READY", "NOT_READY"}:
        return _result("FAIL", path=str(path), blocker="CONTROL_PLANE_STATUS_INVALID")
    return _result(
        "PASS",
        path=str(path),
        final_status=str(payload.get("final_status") or ""),
        canonical_blocker=str(payload.get("canonical_blocker") or ""),
        submit_allowed=bool(payload.get("submit_allowed") is True),
    )


def _packet_currentness_v1(*, runtime_root: Path, runtime_mode: str) -> dict[str, Any]:
    path = runtime_root / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
    meta: dict[str, str] = {}
    if path.exists() and path.is_file():
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[:80]:
            text = line.strip()
            if not text.startswith("- ") or ":" not in text:
                continue
            key, value = text[2:].split(":", 1)
            meta[key.strip()] = value.strip()
    current_commit = git_commit_v1()
    packet_commit = str(meta.get("git_commit") or "").strip()
    packet_mode = str(meta.get("runtime_mode") or "").strip().upper()
    expected_mode = str(runtime_mode or "").strip().upper()
    stale = not path.exists() or packet_commit != current_commit or packet_mode != expected_mode or git_dirty_status_v1() != "CLEAN"
    return {
        "path": str(path.resolve()),
        "exists": path.exists(),
        "generated_at_utc": str(meta.get("generated_at_utc") or ""),
        "runtime_mode": packet_mode,
        "expected_runtime_mode": expected_mode,
        "packet_git_commit": packet_commit,
        "current_git_commit": current_commit,
        "current_git_dirty_status": git_dirty_status_v1(),
        "status": "STALE" if stale else "CURRENT",
        "canonical_blocker": "AEGIS_PACKET_STALE" if stale else "",
    }


def build_promotion_validation_ledger_v1(
    *,
    day_utc: str,
    truth_root: Path,
    runtime_root: Path,
    focused_tests_passed: bool = False,
    focused_test_details: list[str] | None = None,
    hostile_audit_grade: float | None = None,
    audit_threshold: float = 92.0,
    packet_required: bool = False,
) -> dict[str, Any]:
    candidate_commit = git_commit_v1()
    dirty_status = git_dirty_status_v1()
    repo_clean = dirty_status == "CLEAN"
    import_result = _import_preflight_result_v1()
    registry_result, schema_result = _validate_registry_contract_v1()
    control_result = _control_plane_result_v1(truth_root=truth_root, day_utc=day_utc)
    focused_result = _result("PASS" if focused_tests_passed else "FAIL", tests=focused_test_details or [], passed=bool(focused_tests_passed))
    packet = _packet_currentness_v1(runtime_root=runtime_root, runtime_mode="CANDIDATE")
    production_version = read_production_version_v1()
    blockers: list[dict[str, str]] = []
    if not repo_clean:
        blockers.append({"code": "REPO_DIRTY", "dirty_status": dirty_status})
    for code, result in (
        ("IMPORT_PREFLIGHT_FAILED", import_result),
        ("FOCUSED_TESTS_NOT_PROVEN_PASS", focused_result),
        ("REGISTRY_VALIDATION_FAILED", registry_result),
        ("SCHEMA_VALIDATION_FAILED", schema_result),
        ("CONTROL_PLANE_EVALUATION_FAILED", control_result),
    ):
        if result.get("status") != "PASS":
            blockers.append({"code": code})
    if hostile_audit_grade is not None and hostile_audit_grade < audit_threshold:
        blockers.append({"code": "HOSTILE_AUDIT_GRADE_BELOW_THRESHOLD", "grade": str(hostile_audit_grade), "threshold": str(audit_threshold)})
    if packet_required and packet.get("status") != "CURRENT":
        blockers.append({"code": "PACKET_NOT_CURRENT", "path": str(packet.get("path") or "")})
    truth_root_text = str(truth_root.resolve())
    runtime_root_text = str(runtime_root.resolve())
    if truth_root.name not in {"candidate_truth", "production_truth"}:
        blockers.append({"code": "TRUTH_ROOT_NOT_MODE_ROOT", "truth_root": truth_root_text})
    if not str(truth_root.resolve()).startswith(runtime_root_text + "/") and truth_root.resolve() != runtime_root.resolve():
        blockers.append({"code": "TRUTH_ROOT_RUNTIME_ROOT_MISMATCH", "truth_root": truth_root_text, "runtime_root": runtime_root_text})
    payload = {
        "schema_id": "aegis_promotion_validation_ledger",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "candidate_commit": candidate_commit,
        "promoted_commit": str(production_version.get("promoted_commit") or ""),
        "repo_clean": repo_clean,
        "repo_dirty_status": dirty_status,
        "import_preflight_result": import_result,
        "focused_test_result": focused_result,
        "registry_validation_result": registry_result,
        "schema_validation_result": schema_result,
        "control_plane_result": control_result,
        "hostile_audit_grade": hostile_audit_grade,
        "audit_threshold": audit_threshold,
        "packet_commit": str(packet.get("packet_git_commit") or ""),
        "packet_currentness": packet,
        "truth_root": truth_root_text,
        "runtime_root": runtime_root_text,
        "generated_at": now_iso_v1(),
        "promotion_status": "CANDIDATE",
        "blockers": blockers,
    }
    return payload


def run_promotion_validation_ledger_v1(
    *,
    day_utc: str,
    truth_root: str = "",
    runtime_root: str = "",
    focused_tests_passed: bool = False,
    focused_test_details: list[str] | None = None,
    hostile_audit_grade: float | None = None,
    audit_threshold: float = 92.0,
    packet_required: bool = False,
) -> tuple[Path, dict[str, Any]]:
    truth = Path(truth_root or CANDIDATE_TRUTH_ROOT).resolve()
    runtime = Path(runtime_root or truth.parent).resolve()
    payload = build_promotion_validation_ledger_v1(
        day_utc=day_utc,
        truth_root=truth,
        runtime_root=runtime,
        focused_tests_passed=focused_tests_passed,
        focused_test_details=focused_test_details,
        hostile_audit_grade=hostile_audit_grade,
        audit_threshold=audit_threshold,
        packet_required=packet_required,
    )
    path = promotion_validation_ledger_path(truth_root=truth, day_utc=day_utc)
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_aegis_promotion_validation_ledger_v1.py",
        producer_command=f"python3 ops/tools/run_aegis_promotion_validation_ledger_v1.py --day_utc {day_utc}",
        input_artifacts=[REGISTRY_PATH, truth / "reports" / "aegis_control_plane_v1" / day_utc / "control_plane.v1.json"],
        output_artifacts=[path],
        schema_versions={"aegis_promotion_validation_ledger": SCHEMA_VERSION},
    )
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--runtime_root", default="")
    parser.add_argument("--focused_tests_passed", action="store_true")
    parser.add_argument("--test", action="append", default=[])
    parser.add_argument("--hostile_audit_grade", type=float, default=None)
    parser.add_argument("--audit_threshold", type=float, default=92.0)
    parser.add_argument("--packet_required", action="store_true")
    args = parser.parse_args(argv)
    path, payload = run_promotion_validation_ledger_v1(
        day_utc=args.day_utc,
        truth_root=args.truth_root,
        runtime_root=args.runtime_root,
        focused_tests_passed=bool(args.focused_tests_passed),
        focused_test_details=list(args.test or []),
        hostile_audit_grade=args.hostile_audit_grade,
        audit_threshold=float(args.audit_threshold),
        packet_required=bool(args.packet_required),
    )
    print(json.dumps({"promotion_validation_ledger_path": str(path), "promotion_status": payload["promotion_status"], "blockers": payload["blockers"]}, sort_keys=True))
    return 0 if not payload["blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
