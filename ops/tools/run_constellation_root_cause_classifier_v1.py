#!/usr/bin/env python3
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path("/home/node/constellation_2_runtime")
RUNTIME_ROOT = REPO_ROOT / "constellation_2" / "runtime"
TRUTH_ROOT = RUNTIME_ROOT / "truth"
SYSTEM_SNAPSHOT_ROOT = TRUTH_ROOT / "system_snapshot"
RUNTIME_STATE_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_runtime_state.v1.json"
OUTPUT_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_root_cause_report.v1.json"


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"FAIL_CLOSED: could not parse json: {path}: {exc}")


def ensure_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise SystemExit(f"FAIL_CLOSED: missing required {label}: {path}")


def latest_json_candidate_name(stream_summary: dict[str, Any]) -> str | None:
    latest = stream_summary.get("latest_json_file")
    if not isinstance(latest, str):
        return None
    return Path(latest).name


def is_invalid_candidate(stream_summary: dict[str, Any]) -> bool:
    name = latest_json_candidate_name(stream_summary)
    if name is None:
        return False
    return ".INVALID_" in name


def latest_day_of(stream_summaries: dict[str, Any], key: str) -> str | None:
    data = stream_summaries.get(key, {})
    day = data.get("latest_day")
    return day if isinstance(day, str) else None


def classify(runtime_state: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    stream_summaries = runtime_state.get("stream_summaries", {})
    diagnostics = runtime_state.get("self_diagnostics", {}).get("diagnostics", [])
    latest_operating_day = runtime_state.get("latest_operating_day")
    latest_execution_day = runtime_state.get("latest_execution_day")

    scope_health = runtime_state.get("scope_health", {})
    execution_health = scope_health.get("sleeve_execution_health", {})
    monitoring_health = scope_health.get("system_monitoring_health", {})
    overall_scope = scope_health.get("overall", {})

    execution_status = str(execution_health.get("status") or "UNKNOWN").upper()
    monitoring_status = str(monitoring_health.get("status") or "UNKNOWN").upper()
    overall_scope_status = str(overall_scope.get("status") or "UNKNOWN").upper()

    monitoring_freshness = monitoring_health.get("freshness", {})
    freshness_results = monitoring_freshness.get("surface_results", [])

    non_auth_surface = next((d for d in diagnostics if d.get("code") == "NON_AUTHORITATIVE_EXECUTION_SURFACE_PRESENT"), None)
    quarantine_diag = next((d for d in diagnostics if d.get("code") == "QUARANTINE_ACTIVITY_PRESENT"), None)
    monitoring_diag = next((d for d in diagnostics if d.get("code") == "MONITORING_FRESHNESS_POLICY_VIOLATION"), None)

    if execution_status == "FAIL":
        findings.append({
            "root_cause_code": "EXECUTION_AUTHORITY_FAIL",
            "severity": "BLOCKING",
            "confidence": "HIGH",
            "summary": (
                "Sleeve execution authority is FAIL for PAPER mode. "
                "Monitoring health cannot override execution authority failure."
            ),
            "evidence": [
                d.get("latest_verdict_path")
                for d in execution_health.get("sleeves", [])
                if isinstance(d, dict) and d.get("latest_verdict_path")
            ],
        })

    if execution_status == "PASS" and monitoring_status in {"DEGRADED", "FAIL"}:
        findings.append({
            "root_cause_code": "STALE_MONITORING",
            "severity": "ERROR",
            "confidence": "HIGH",
            "summary": (
                "Monitoring health is degraded while sleeve execution authority is PASS "
                f"(execution_day={latest_execution_day}, operating_day={latest_operating_day})."
            ),
            "evidence": monitoring_diag.get("evidence", []) if isinstance(monitoring_diag, dict) else [],
        })

    invalid_streams = []
    for key, summary in stream_summaries.items():
        if is_invalid_candidate(summary):
            invalid_streams.append({
                "stream": key,
                "latest_json_file": summary.get("latest_json_file"),
                "latest_day": summary.get("latest_day"),
            })
    if invalid_streams:
        findings.append({
            "root_cause_code": "INVALID_ARTIFACT_PREFERRED",
            "severity": "ERROR",
            "confidence": "HIGH",
            "summary": "At least one stream's latest selected artifact is an INVALID file, which can poison latest-state inference.",
            "evidence": invalid_streams,
        })

    if non_auth_surface is not None:
        findings.append({
            "root_cause_code": "NON_AUTHORITATIVE_SURFACE_COMPETITION",
            "severity": "WARN",
            "confidence": "HIGH",
            "summary": "A non-authoritative execution surface is present alongside the authoritative one.",
            "evidence": non_auth_surface.get("evidence", []),
        })

    if quarantine_diag is not None:
        findings.append({
            "root_cause_code": "QUARANTINE_ACTIVITY_PRESENT",
            "severity": "WARN",
            "confidence": "HIGH",
            "summary": "Quarantine roots are present under runtime, indicating unresolved or historical integrity incidents.",
            "evidence": quarantine_diag.get("evidence", []),
        })

    stale_surfaces = [
        r for r in freshness_results
        if isinstance(r, dict) and str(r.get("surface_id") or "").strip() == "capital_authority_allocation"
        and str(r.get("status") or "").upper() in {"DEGRADED", "FAIL"}
    ]
    if stale_surfaces:
        cap = stale_surfaces[0]
        findings.append({
            "root_cause_code": "CAPITAL_AUTHORITY_STALE",
            "severity": "WARN",
            "confidence": "MEDIUM",
            "summary": (
                "Capital authority surface freshness is outside governed expectation "
                f"(expected_day={cap.get('expected_day')}, actual_day={cap.get('actual_day')})."
            ),
            "evidence": [
                "constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1",
                "constellation_2/runtime/truth/reports/capital_risk_envelope_v2",
            ],
        })

    if execution_status == "PASS" and overall_scope_status == "PARTIALLY_PROVEN":
        findings.append({
            "root_cause_code": "SCOPE_HEALTH_SPLIT_ACTIVE",
            "severity": "INFO",
            "confidence": "HIGH",
            "summary": (
                "Scope-aware semantics active: execution authority PASS is preserved while monitoring remains degraded."
            ),
            "evidence": [
                "constellation_2/runtime/truth/system_snapshot/constellation_runtime_state.v1.json"
            ],
        })

    if not findings:
        findings.append({
            "root_cause_code": "NO_ROOT_CAUSE_CLASSIFIED",
            "severity": "INFO",
            "confidence": "LOW",
            "summary": "No higher-order root cause was classified from the current runtime state.",
            "evidence": [],
        })

    return findings


def overall_status(findings: list[dict[str, Any]]) -> str:
    severities = [f.get("severity") for f in findings]
    if "BLOCKING" in severities:
        return "BLOCKING"
    if "ERROR" in severities:
        return "DEGRADED"
    if "WARN" in severities:
        return "DEGRADED"
    return "OK"


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def main() -> None:
    ensure_exists(RUNTIME_STATE_PATH, "runtime state artifact")
    ensure_exists(REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_TRUTH_AUTHORITY_REGISTRY_V1.json", "truth authority registry")
    ensure_exists(REPO_ROOT / "governance" / "02_REGISTRIES" / "TRUTH_SURFACE_AUTHORITY_V1.json", "truth surface authority registry")

    runtime_state = read_json(RUNTIME_STATE_PATH)
    findings = classify(runtime_state)

    report = {
        "artifact_id": "constellation_root_cause_report",
        "schema_version": "1.0",
        "generated_utc": now_utc(),
        "input_artifact": str(RUNTIME_STATE_PATH.relative_to(REPO_ROOT)),
        "runtime_state_generated_utc": runtime_state.get("generated_utc"),
        "latest_operating_day": runtime_state.get("latest_operating_day"),
        "overall_status": overall_status(findings),
        "root_cause_count": len(findings),
        "root_causes": findings,
    }

    write_json(OUTPUT_PATH, report)
    print(f"WROTE: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
