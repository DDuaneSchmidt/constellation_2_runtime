from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from constellation_2.common.step_result_envelope_v1 import build_step_result_envelope_v1


SOURCE_REPO_ROOT = Path(__file__).resolve().parents[2]
SESSION_REFRESH_SCHEMA_PATH = (
    SOURCE_REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/session_readiness_refresh.v1.schema.json"
).resolve()
STEP_RESULT_SCHEMA_PATH = (
    SOURCE_REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/step_result_envelope.v1.schema.json"
).resolve()
DAY_AUTHORITY_SCHEMA_PATH = (
    SOURCE_REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/day_authority_decision.v1.schema.json"
).resolve()
TRADE_READINESS_STATUS_SCHEMA_PATH = (
    SOURCE_REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json"
).resolve()
TRUTH_SURFACE_AUTHORITY_PATH = (
    SOURCE_REPO_ROOT / "governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json"
).resolve()

BLOCKING_CLASS_GOVERNANCE_CONFIG = "GOVERNANCE_CONFIG_MISSING"
BLOCKING_CLASS_REGISTRY_MAPPING = "REGISTRY_MAPPING_INCOMPATIBILITY"
BLOCKING_CLASS_REQUIRED_SCHEMA = "REQUIRED_SCHEMA_MISSING"
BLOCKING_CLASS_ATTESTATION = "AUTHORITY_ATTESTATION_INCOMPATIBLE"
BLOCKING_CLASS_CURRENT_DAY_INPUTS = "CURRENT_DAY_INPUT_ARTIFACT_MISSING"
BLOCKING_CLASS_BUSINESS = "BUSINESS_DOMAIN_CONTRADICTION"
BLOCKING_CLASS_MONITORING = "MONITORING_ONLY_DEGRADATION"


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def _schema_presence_blockers() -> List[Dict[str, str]]:
    blockers: List[Dict[str, str]] = []
    for path in (
        SESSION_REFRESH_SCHEMA_PATH,
        STEP_RESULT_SCHEMA_PATH,
        DAY_AUTHORITY_SCHEMA_PATH,
        TRADE_READINESS_STATUS_SCHEMA_PATH,
    ):
        if not path.exists() or not path.is_file():
            blockers.append(
                {
                    "blocking_class": BLOCKING_CLASS_REQUIRED_SCHEMA,
                    "reason_code": f"MISSING_SCHEMA:{path.name}",
                    "evidence_ref": str(path),
                }
            )
    return blockers


def _truth_surface_compatibility() -> tuple[List[Dict[str, str]], List[str], Dict[str, Any]]:
    warnings: List[str] = []
    blockers: List[Dict[str, str]] = []
    details: List[str] = []
    registry = _read_json(TRUTH_SURFACE_AUTHORITY_PATH)
    mapping = registry.get("mapping")
    surfaces = registry.get("surfaces")
    if not isinstance(mapping, dict) or not isinstance(surfaces, list):
        blockers.append(
            {
                "blocking_class": BLOCKING_CLASS_REGISTRY_MAPPING,
                "reason_code": "TRUTH_SURFACE_AUTHORITY_MALFORMED",
                "evidence_ref": str(TRUTH_SURFACE_AUTHORITY_PATH),
            }
        )
        return blockers, warnings, {
            "mapping_status": "BLOCKED",
            "schema_status": "OK",
            "reader_compatibility_status": "UNKNOWN",
            "details": [f"TRUTH_SURFACE_AUTHORITY_MALFORMED:{TRUTH_SURFACE_AUTHORITY_PATH}"],
        }

    for row in surfaces:
        if not isinstance(row, dict):
            continue
        surface = str(row.get("surface") or "").strip()
        active = str(row.get("active") or "").strip()
        if not surface or not active:
            continue
        row_mapping = mapping.get(surface)
        if not isinstance(row_mapping, dict) or active not in row_mapping:
            detail = f"ACTIVE_SURFACE_MAPPING_MISSING:surface={surface}:active={active}"
            details.append(detail)
            if surface == "monitoring":
                warnings.append(detail)
            else:
                blockers.append(
                    {
                        "blocking_class": BLOCKING_CLASS_REGISTRY_MAPPING,
                        "reason_code": detail,
                        "evidence_ref": str(TRUTH_SURFACE_AUTHORITY_PATH),
                    }
                )
    mapping_status = "OK" if not blockers else "BLOCKED"
    if warnings and not blockers:
        mapping_status = "DEGRADED"
    return blockers, warnings, {
        "mapping_status": mapping_status,
        "schema_status": "OK",
        "reader_compatibility_status": "UNKNOWN",
        "details": details,
    }


def _trade_readiness_schema_compatibility() -> tuple[List[Dict[str, str]], Dict[str, Any]]:
    blockers: List[Dict[str, str]] = []
    details: List[str] = []
    schema = _read_json(TRADE_READINESS_STATUS_SCHEMA_PATH)
    required = schema.get("required")
    properties = schema.get("properties")
    if not isinstance(required, list) or not isinstance(properties, dict):
        blockers.append(
            {
                "blocking_class": BLOCKING_CLASS_ATTESTATION,
                "reason_code": "TRADE_READINESS_SCHEMA_MALFORMED",
                "evidence_ref": str(TRADE_READINESS_STATUS_SCHEMA_PATH),
            }
        )
        return blockers, {
            "mapping_status": "OK",
            "schema_status": "OK",
            "reader_compatibility_status": "BLOCKED",
            "details": ["TRADE_READINESS_SCHEMA_MALFORMED"],
        }

    required_set = {str(item).strip() for item in required if str(item).strip()}
    for field in ("day_utc", "session_authority_attestation", "run_state_authority_attestation"):
        if field not in required_set:
            detail = f"TRADE_READINESS_REQUIRED_FIELD_MISSING:{field}"
            details.append(detail)
            blockers.append(
                {
                    "blocking_class": BLOCKING_CLASS_ATTESTATION,
                    "reason_code": detail,
                    "evidence_ref": str(TRADE_READINESS_STATUS_SCHEMA_PATH),
                }
            )
    provenance = properties.get("provenance")
    if not isinstance(provenance, dict):
        detail = "TRADE_READINESS_PROVENANCE_SCHEMA_MISSING"
        details.append(detail)
        blockers.append(
            {
                "blocking_class": BLOCKING_CLASS_ATTESTATION,
                "reason_code": detail,
                "evidence_ref": str(TRADE_READINESS_STATUS_SCHEMA_PATH),
            }
        )
    else:
        provenance_required = provenance.get("required")
        provenance_required_set = {str(item).strip() for item in provenance_required or [] if str(item).strip()}
        if "sleeve_registry_sha256" not in provenance_required_set:
            detail = "TRADE_READINESS_SLEEVE_REGISTRY_SHA256_REQUIRED_MISSING"
            details.append(detail)
            blockers.append(
                {
                    "blocking_class": BLOCKING_CLASS_ATTESTATION,
                    "reason_code": detail,
                    "evidence_ref": str(TRADE_READINESS_STATUS_SCHEMA_PATH),
                }
            )
    return blockers, {
        "mapping_status": "OK",
        "schema_status": "OK",
        "reader_compatibility_status": "OK" if not blockers else "BLOCKED",
        "details": details,
    }


def run_day_authority_preflight_v1(
    *,
    truth_root: Path,
    day_utc: str,
    startup_materialization_result: Mapping[str, Any] | None,
    handshake_result: Mapping[str, Any] | None,
    global_gate_result: Mapping[str, Any] | None,
    scope_summary: Mapping[str, Any] | None,
    scoped_gate_results: Iterable[Mapping[str, Any]],
    producer_module: str,
    producer_git_sha: str,
    global_gate_nonblocking: bool,
) -> Dict[str, Any]:
    blockers: List[Dict[str, str]] = []
    warnings: List[str] = []
    schema_refs = [
        str(SESSION_REFRESH_SCHEMA_PATH),
        str(STEP_RESULT_SCHEMA_PATH),
        str(DAY_AUTHORITY_SCHEMA_PATH),
        str(TRADE_READINESS_STATUS_SCHEMA_PATH),
    ]

    blockers.extend(_schema_presence_blockers())

    mapping_blockers, mapping_warnings, mapping_status = _truth_surface_compatibility()
    blockers.extend(mapping_blockers)
    warnings.extend(mapping_warnings)

    compatibility_blockers, compatibility_status = _trade_readiness_schema_compatibility()
    blockers.extend(compatibility_blockers)

    startup_rc = int((startup_materialization_result or {}).get("returncode") or 0)
    if startup_materialization_result is not None and startup_rc != 0:
        blockers.append(
            {
                "blocking_class": BLOCKING_CLASS_GOVERNANCE_CONFIG,
                "reason_code": "STARTUP_MATERIALIZATION_FAILED",
                "evidence_ref": "results.startup_materialization",
            }
        )

    handshake_rc = int((handshake_result or {}).get("returncode") or 0)
    if handshake_result is not None and handshake_rc != 0:
        blockers.append(
            {
                "blocking_class": BLOCKING_CLASS_CURRENT_DAY_INPUTS,
                "reason_code": "IB_API_HANDSHAKE_FAILED",
                "evidence_ref": "results.ib_api_handshake",
            }
        )

    gate_rc = int((global_gate_result or {}).get("returncode") or 0)
    if global_gate_result is not None and gate_rc != 0 and not bool(global_gate_nonblocking):
        blockers.append(
            {
                "blocking_class": BLOCKING_CLASS_BUSINESS,
                "reason_code": "GLOBAL_GATE_REFRESH_BLOCKED",
                "evidence_ref": "results.global_gate_refresh",
            }
        )

    primary_ready = bool((scope_summary or {}).get("primary_ready") is True)
    primary_reason_codes = [
        str(item).strip()
        for item in ((scope_summary or {}).get("primary_blocker_reason_codes") or [])
        if str(item).strip()
    ]
    if scope_summary and not primary_ready:
        blockers.append(
            {
                "blocking_class": BLOCKING_CLASS_BUSINESS,
                "reason_code": primary_reason_codes[0] if primary_reason_codes else "PRIMARY_SCOPED_GATE_BLOCKED",
                "evidence_ref": "results.scope_summary.primary_ready",
            }
        )

    compatibility_details = []
    compatibility_details.extend(mapping_status.get("details") or [])
    compatibility_details.extend(compatibility_status.get("details") or [])
    blocking_class = "NONE" if not blockers else blockers[0]["blocking_class"]
    first_failure = None
    if blockers:
        first_failure = {
            "prerequisite_class": blockers[0]["blocking_class"],
            "reason_code": blockers[0]["reason_code"],
            "evidence_ref": blockers[0]["evidence_ref"],
        }
    validation_state = "PASS" if not blockers else "FAIL"
    envelope = build_step_result_envelope_v1(
        step_id="authority_kernel_preflight_v1",
        status="BLOCKED" if blockers else ("OK_WITH_WARNINGS" if warnings else "OK"),
        artifact_refs=[],
        counts={
            "blocking_count": len(blockers),
            "warning_count": len(warnings),
            "scoped_result_count": len(list(scoped_gate_results)),
        },
        warnings=warnings,
        blocking_defects=blockers,
        diagnostics={
            "day_utc": day_utc,
            "mode": "validation_result_only",
            "validation_state": validation_state,
            "blocking_class": blocking_class,
            "first_failure": first_failure,
            "scope_summary": dict(scope_summary or {}),
        },
        schema_refs=schema_refs,
    )
    return {
        "mode": "validation_result_only",
        "envelope": envelope,
        "validation_summary": {
            "validation_state": validation_state,
            "blocking_class": blocking_class,
            "first_failure": first_failure,
            "blocking_evidence": [row["reason_code"] for row in blockers],
            "missing_or_invalid_prerequisite_refs": [row["evidence_ref"] for row in blockers],
            "validation_refs_used": ["results.ib_api_handshake", "results.global_gate_refresh", "results.scope_summary"],
            "compatibility_status": {
                "mapping_status": mapping_status["mapping_status"],
                "schema_status": "OK" if not any(row["blocking_class"] == BLOCKING_CLASS_REQUIRED_SCHEMA for row in blockers) else "BLOCKED",
                "reader_compatibility_status": compatibility_status["reader_compatibility_status"],
                "details": compatibility_details,
            },
            "diagnostic_warnings": warnings,
            "run_metadata": {
                "truth_root": str(Path(truth_root).resolve()),
                "producer_module": str(producer_module or "").strip(),
                "producer_git_sha": str(producer_git_sha or "").strip(),
            },
        },
    }
