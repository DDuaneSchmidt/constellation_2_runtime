from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.scheduled_run_dependency_manifest_v1 import build_scheduled_run_dependency_manifest_v1, scheduled_run_dependency_manifest_path_v1, write_scheduled_run_dependency_manifest_v1
from ops.aegis.scheduled_run_readiness_certificate_v1 import build_scheduled_run_readiness_certificate_v1, scheduled_run_readiness_certificate_path_v1, write_scheduled_run_readiness_certificate_v1
from ops.aegis.scheduled_run_reconciliation_v1 import build_scheduled_run_reconciliation_v1, scheduled_run_reconciliation_path_v1, write_scheduled_run_reconciliation_v1
from ops.aegis.scheduled_run_registry_v1 import build_scheduled_run_registry_v1, scheduled_run_registry_path_v1, write_scheduled_run_registry_v1
from ops.aegis.scheduled_run_safe_repair_v1 import scheduled_run_safe_repair_path_v1

REPORT_FAMILY = "aegis_scheduled_run_readiness_self_check_v1"
REPORT_FILENAME = "self_check.v1.json"


def scheduled_run_readiness_self_check_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_scheduled_run_readiness_self_check_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    registry = _load_or_write(root, day_utc, scheduled_run_registry_path_v1, build_scheduled_run_registry_v1, write_scheduled_run_registry_v1)
    manifest = _load_or_write(root, day_utc, scheduled_run_dependency_manifest_path_v1, build_scheduled_run_dependency_manifest_v1, write_scheduled_run_dependency_manifest_v1)
    certificate = _load_or_write(root, day_utc, scheduled_run_readiness_certificate_path_v1, build_scheduled_run_readiness_certificate_v1, write_scheduled_run_readiness_certificate_v1)
    reconciliation = _load_or_write(root, day_utc, scheduled_run_reconciliation_path_v1, build_scheduled_run_reconciliation_v1, write_scheduled_run_reconciliation_v1)
    repair = _read_json(scheduled_run_safe_repair_path_v1(truth_root=root, day_utc=day_utc))
    failures: list[dict[str, Any]] = []
    deps_by_run = manifest.get("dependencies_by_run_id") if isinstance(manifest.get("dependencies_by_run_id"), dict) else {}
    for run in registry.get("scheduled_runs", []):
        if not isinstance(run, dict) or run.get("enabled") is not True:
            continue
        run_id = str(run.get("scheduled_run_id") or "")
        if not deps_by_run.get(run_id):
            failures.append(_failure("SCHEDULED_RUN_MISSING_DEPENDENCIES", run_id))
    for dep in manifest.get("dependencies", []):
        if not isinstance(dep, dict):
            continue
        if not dep.get("producer_command") and not dep.get("no_repair_reason"):
            failures.append(_failure("DEPENDENCY_MISSING_PRODUCER", str(dep.get("dependency_id") or "")))
    for cert in certificate.get("certificates", []):
        if not isinstance(cert, dict):
            continue
        if not cert.get("source_hashes"):
            failures.append(_failure("CERTIFICATE_MISSING_HASH", str(cert.get("scheduled_run_id") or "")))
        if cert.get("readiness_status") == "CERTIFIED_READY" and cert.get("remaining_blockers"):
            failures.append(_failure("CERTIFIED_READY_WITH_BLOCKERS", str(cert.get("scheduled_run_id") or "")))
        if cert.get("readiness_status") == "EXPIRED":
            failures.append(_failure("EXPIRED_CERTIFICATE_USED", str(cert.get("scheduled_run_id") or "")))
    for repair_row in repair.get("repairs", []) if isinstance(repair.get("repairs"), list) else []:
        if not isinstance(repair_row, dict):
            continue
        if not repair_row.get("before_status") or not repair_row.get("after_status"):
            failures.append(_failure("REPAIR_MISSING_BEFORE_AFTER", str(repair_row.get("repair_id") or "")))
    for row in reconciliation.get("scheduled_runs", []):
        if not isinstance(row, dict):
            continue
        if row.get("run_result") not in {"SUCCESS", "NOT_EXECUTED", "NO_SETUP_NOT_APPLICABLE"} and not row.get("recommended_prevention"):
            failures.append(_failure("RUN_FAILED_WITHOUT_RECONCILIATION", str(row.get("scheduled_run_id") or "")))
    if _stable_hash(build_scheduled_run_readiness_certificate_v1(truth_root=root, day_utc=day_utc)) != _stable_hash(build_scheduled_run_readiness_certificate_v1(truth_root=root, day_utc=day_utc)):
        failures.append(_failure("NON_DETERMINISTIC_OUTPUT", "readiness_certificate"))
    return {
        "schema_id": "aegis_scheduled_run_readiness_self_check",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": str(certificate.get("generated_at") or f"{day_utc}T00:00:00Z"),
        "ok": not failures,
        "failure_count": len(failures),
        "failures": failures,
        "source_artifacts": {
            "scheduled_run_registry": str(scheduled_run_registry_path_v1(truth_root=root, day_utc=day_utc)),
            "dependency_manifest": str(scheduled_run_dependency_manifest_path_v1(truth_root=root, day_utc=day_utc)),
            "readiness_certificate": str(scheduled_run_readiness_certificate_path_v1(truth_root=root, day_utc=day_utc)),
            "scheduled_run_reconciliation": str(scheduled_run_reconciliation_path_v1(truth_root=root, day_utc=day_utc)),
        },
    }


def write_scheduled_run_readiness_self_check_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    path = scheduled_run_readiness_self_check_path_v1(truth_root=truth_root, day_utc=day_utc)
    body = dict(payload or build_scheduled_run_readiness_self_check_v1(truth_root=truth_root, day_utc=day_utc))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _load_or_write(root: Path, day: str, path_fn: Any, build_fn: Any, write_fn: Any) -> dict[str, Any]:
    path = path_fn(truth_root=root, day_utc=day)
    payload = _read_json(path)
    if payload:
        return payload
    payload = build_fn(truth_root=root, day_utc=day)
    write_fn(truth_root=root, day_utc=day, payload=payload)
    return payload


def _failure(code: str, subject: str) -> dict[str, str]:
    return {"failure_code": code, "subject": subject}


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}
