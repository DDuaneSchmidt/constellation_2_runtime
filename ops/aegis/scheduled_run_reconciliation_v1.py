from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.scheduled_run_readiness_certificate_v1 import (
    build_scheduled_run_readiness_certificate_v1,
    scheduled_run_readiness_certificate_path_v1,
    write_scheduled_run_readiness_certificate_v1,
)
from ops.aegis.scheduled_run_registry_v1 import build_scheduled_run_registry_v1

REPORT_FAMILY = "aegis_scheduled_run_reconciliation_v1"
REPORT_FILENAME = "scheduled_run_reconciliation.v1.json"


def scheduled_run_reconciliation_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_scheduled_run_reconciliation_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    cert_path = scheduled_run_readiness_certificate_path_v1(truth_root=root, day_utc=day_utc)
    cert_payload = _read_json(cert_path)
    if not cert_payload:
        cert_payload = build_scheduled_run_readiness_certificate_v1(truth_root=root, day_utc=day_utc)
        cert_path = write_scheduled_run_readiness_certificate_v1(truth_root=root, day_utc=day_utc, payload=cert_payload)
    registry = build_scheduled_run_registry_v1(truth_root=root, day_utc=day_utc)
    certs = cert_payload.get("certificates_by_run_id") if isinstance(cert_payload.get("certificates_by_run_id"), dict) else {}
    rows = [_reconcile(root, day_utc, run, certs.get(str(run.get("scheduled_run_id") or ""), {})) for run in registry.get("scheduled_runs", []) if isinstance(run, dict)]
    return {
        "schema_id": "aegis_scheduled_run_reconciliation",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": str(cert_payload.get("generated_at") or f"{day_utc}T00:00:00Z"),
        "scheduled_runs": rows,
        "scheduled_runs_by_id": {row["scheduled_run_id"]: row for row in rows},
        "summary": {
            "run_count": len(rows),
            "executed_count": sum(1 for row in rows if row.get("did_run_execute") is True),
            "predictable_failure_count": sum(1 for row in rows if row.get("was_failure_predictable") is True),
            "missing_certificate_count": sum(1 for row in rows if row.get("did_valid_certificate_exist") is False),
        },
        "source_artifacts": {"readiness_certificate": str(cert_path)},
        "source_hashes": {str(cert_path): _sha256(cert_path)} if cert_path.exists() else {},
        "safety": {
            "trade_advice_allowed": False,
            "manual_trade_capture_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "live_trading_allowed": False,
            "policy_changed": False,
        },
    }


def write_scheduled_run_reconciliation_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    path = scheduled_run_reconciliation_path_v1(truth_root=truth_root, day_utc=day_utc)
    body = dict(payload or build_scheduled_run_reconciliation_v1(truth_root=truth_root, day_utc=day_utc))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _reconcile(root: Path, day: str, run: Mapping[str, Any], cert: Mapping[str, Any]) -> dict[str, Any]:
    run_id = str(run.get("scheduled_run_id") or "")
    executed, result, evidence = _execution_state(root, day, run_id)
    valid_cert = bool(cert and cert.get("readiness_status") in {"CERTIFIED_READY", "REPAIRED_CERTIFIED_READY"})
    failure = "" if result in {"SUCCESS", "NO_SETUP_NOT_APPLICABLE", "NOT_EXECUTED"} else result
    predictable = bool(failure and cert and cert.get("readiness_status") not in {"CERTIFIED_READY", "REPAIRED_CERTIFIED_READY"})
    missed = []
    if failure and not cert:
        missed.append("READINESS_CERTIFICATE_MISSING")
    if predictable:
        missed.extend(str(row.get("dependency_id")) for row in cert.get("remaining_blockers", []) if isinstance(row, dict))
    return {
        "scheduled_run_id": run_id,
        "did_run_execute": executed,
        "did_valid_certificate_exist": valid_cert,
        "run_result": result,
        "failed_reason": failure,
        "was_failure_predictable": predictable,
        "missed_preflight_checks": sorted(set(missed)),
        "new_failure_class": "" if predictable or not failure else failure,
        "recommended_prevention": "Fix or safely repair readiness blockers before the next scheduled run." if predictable else ("Add a new preflight dependency for this failure class." if failure else "No prevention change required."),
        "evidence_path": str(evidence) if evidence else "",
    }


def _execution_state(root: Path, day: str, run_id: str) -> tuple[bool, str, Path | None]:
    reports = root / "reports"
    checks = {
        "CANDIDATE_GENERATION_0950": reports / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json",
        "MIDDAY_MONITORING": reports / "aegis_operator_action_model_v1" / day / "operator_action_model.v1.json",
        "EOD_OUTCOME_UPDATE": reports / "aegis_outcome_registry_v1" / day / "outcome_registry.v1.json",
        "RESEARCH_ALLOCATION": reports / "aegis_research_capital_allocation_v1" / day / "research_capital_allocation.v1.json",
        "VALIDATION_MATURITY": reports / "aegis_statistical_sufficiency_v1" / day / "statistical_sufficiency.v1.json",
        "OPERATOR_DASHBOARD_REFRESH": reports / "aegis_verified_runtime_graph_v1" / day / "portal_runtime_model.v1.json",
    }
    path = checks.get(run_id)
    if not path:
        return False, "NOT_EXECUTED", None
    if not path.exists():
        return False, "NOT_EXECUTED", path
    payload = _read_json(path)
    if payload.get("ok") is False:
        return True, "FAILED_ARTIFACT_REPORTED_NOT_OK", path
    return True, "SUCCESS", path


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
