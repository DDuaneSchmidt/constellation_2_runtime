from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.scheduled_run_dependency_manifest_v1 import (
    build_scheduled_run_dependency_manifest_v1,
    scheduled_run_dependency_manifest_path_v1,
    write_scheduled_run_dependency_manifest_v1,
)
from ops.aegis.scheduled_run_registry_v1 import (
    build_scheduled_run_registry_v1,
    scheduled_run_registry_path_v1,
    write_scheduled_run_registry_v1,
)
from ops.aegis.scheduled_run_safe_repair_v1 import scheduled_run_safe_repair_path_v1

REPORT_FAMILY = "aegis_scheduled_run_readiness_certificate_v1"
REPORT_FILENAME = "readiness_certificate.v1.json"
ALLOWED_STATUSES = {"CERTIFIED_READY", "REPAIRED_CERTIFIED_READY", "BLOCKED", "EXPIRED", "NOT_APPLICABLE", "MARKET_NOT_READY"}


def scheduled_run_readiness_certificate_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_scheduled_run_readiness_certificate_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    registry_path = scheduled_run_registry_path_v1(truth_root=root, day_utc=day_utc)
    registry = _read_json(registry_path) or build_scheduled_run_registry_v1(truth_root=root, day_utc=day_utc)
    registry_path = write_scheduled_run_registry_v1(truth_root=root, day_utc=day_utc, payload=registry)
    manifest_path = scheduled_run_dependency_manifest_path_v1(truth_root=root, day_utc=day_utc)
    manifest = _read_json(manifest_path) or build_scheduled_run_dependency_manifest_v1(truth_root=root, day_utc=day_utc)
    manifest_path = write_scheduled_run_dependency_manifest_v1(truth_root=root, day_utc=day_utc, payload=manifest)
    repair_path = scheduled_run_safe_repair_path_v1(truth_root=root, day_utc=day_utc)
    repair = _read_json(repair_path)
    repairs = repair.get("repairs") if isinstance(repair.get("repairs"), list) else []
    repair_success_by_dep = {str(row.get("dependency_id")) for row in repairs if row.get("result") == "SUCCESS"}
    deps_by_run = manifest.get("dependencies_by_run_id") if isinstance(manifest.get("dependencies_by_run_id"), dict) else {}
    certificates: list[dict[str, Any]] = []
    for run in registry.get("scheduled_runs", []):
        if not isinstance(run, dict):
            continue
        run_id = str(run.get("scheduled_run_id") or "")
        deps = [row for row in deps_by_run.get(run_id, []) if isinstance(row, dict)]
        cert = _certificate(day_utc, run, deps, repairs, repair_success_by_dep, str(registry.get("generated_at") or f"{day_utc}T00:00:00Z"))
        certificates.append(cert)
    source_paths = [registry_path, manifest_path] + ([repair_path] if repair_path.exists() else [])
    return {
        "schema_id": "aegis_scheduled_run_readiness_certificate",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": str(registry.get("generated_at") or f"{day_utc}T00:00:00Z"),
        "certificates": certificates,
        "certificates_by_run_id": {row["scheduled_run_id"]: row for row in certificates},
        "summary": {
            "certificate_count": len(certificates),
            "certified_ready_count": sum(1 for row in certificates if row.get("readiness_status") in {"CERTIFIED_READY", "REPAIRED_CERTIFIED_READY"}),
            "blocked_count": sum(1 for row in certificates if row.get("readiness_status") == "BLOCKED"),
            "market_not_ready_count": sum(1 for row in certificates if row.get("readiness_status") == "MARKET_NOT_READY"),
            "next_scheduled_run_id": certificates[0]["scheduled_run_id"] if certificates else "",
            "next_readiness_status": certificates[0]["readiness_status"] if certificates else "NOT_APPLICABLE",
        },
        "source_artifacts": {"scheduled_run_registry": str(registry_path), "dependency_manifest": str(manifest_path), **({"safe_repair": str(repair_path)} if repair_path.exists() else {})},
        "source_hashes": {str(path): _sha256(path) for path in source_paths if path.exists()},
        "safety": {
            "trade_advice_allowed": False,
            "manual_trade_capture_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "live_trading_allowed": False,
            "policy_changed": False,
        },
    }


def write_scheduled_run_readiness_certificate_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    path = scheduled_run_readiness_certificate_path_v1(truth_root=truth_root, day_utc=day_utc)
    body = dict(payload or build_scheduled_run_readiness_certificate_v1(truth_root=truth_root, day_utc=day_utc))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _certificate(day: str, run: Mapping[str, Any], deps: list[dict[str, Any]], repairs: list[dict[str, Any]], repair_success_by_dep: set[str], generated_at: str) -> dict[str, Any]:
    run_id = str(run.get("scheduled_run_id") or "")
    blocking = [row for row in deps if row.get("is_blocking") is True and row.get("dependency_status") not in {"READY", "NO_SETUP_NOT_APPLICABLE"}]
    market_blocking = [row for row in blocking if row.get("dependency_key") == "MARKET_DATA"]
    repaired = [row for row in blocking if row.get("dependency_id") in repair_success_by_dep]
    remaining = [row for row in blocking if row.get("dependency_id") not in repair_success_by_dep]
    if not run.get("enabled", True):
        status = "NOT_APPLICABLE"
    elif remaining:
        status = "MARKET_NOT_READY" if market_blocking and len(market_blocking) == len(remaining) else "BLOCKED"
    elif repaired:
        status = "REPAIRED_CERTIFIED_READY"
    else:
        status = "CERTIFIED_READY"
    target = str(run.get("target_time_utc") or f"{day}T00:00:00Z")
    valid_until = _valid_until(target)
    source_artifacts = [str(row.get("current_artifact_path")) for row in deps if row.get("artifact_exists")]
    source_hashes = {str(row.get("current_artifact_path")): str(row.get("source_hash")) for row in deps if row.get("source_hash")}
    body = {
        "certificate_id": f"CERT:{day}:{run_id}",
        "scheduled_run_id": run_id,
        "target_run_time": target,
        "readiness_status": status,
        "readiness_valid_until": valid_until,
        "dependencies_checked": len(deps),
        "dependencies_ready": sum(1 for row in deps if row.get("dependency_status") in {"READY", "NO_SETUP_NOT_APPLICABLE"}),
        "dependencies_blocked": len(blocking),
        "repairs_attempted": len([row for row in repairs if str(row.get("dependency_id", "")).startswith(f"{run_id}:")]),
        "repairs_successful": len([row for row in repairs if str(row.get("dependency_id", "")).startswith(f"{run_id}:") and row.get("result") == "SUCCESS"]),
        "repairs_failed": len([row for row in repairs if str(row.get("dependency_id", "")).startswith(f"{run_id}:") and row.get("result") == "FAILED"]),
        "remaining_blockers": remaining,
        "source_artifacts": source_artifacts,
        "source_hashes": source_hashes,
        "generated_at": generated_at,
    }
    body["certificate_hash"] = hashlib.sha256(json.dumps(body, sort_keys=True).encode("utf-8")).hexdigest()
    if body["readiness_status"] not in ALLOWED_STATUSES:
        raise ValueError(f"invalid readiness status {body['readiness_status']}")
    return body


def _valid_until(target: str) -> str:
    try:
        dt = datetime.fromisoformat(target.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)
    return (dt + timedelta(minutes=30)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
