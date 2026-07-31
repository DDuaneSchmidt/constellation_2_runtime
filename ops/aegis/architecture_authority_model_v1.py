from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_ID = "aegis_architecture_authority_model_v1"
SCHEMA_VERSION = "1.0.0"

STRATEGIC_SYSTEM_OF_RECORD = "Paper Trading + Hypothesis Validation Architecture"
LEGACY_COMPATIBILITY_LAYER = "Aegis Lite"

ALLOWED_AUTHORITY_STATUSES = {
    "STRATEGIC_AUTHORITY",
    "COMPATIBILITY_AUTHORITY",
    "LEGACY_READ_ONLY",
    "DEPRECATED",
    "REMOVED",
}

ALLOWED_MIGRATION_STATES = {
    "ACTIVE",
    "MIGRATING",
    "LEGACY_COMPATIBILITY",
    "DEPRECATED",
    "REMOVED",
}

# Runtime truth still names these Lite-era artifacts. Operating status and EOD
# report migrated to strategic projections in the Lite operating-status migration.
RUNTIME_TRUTH_LITE_DEPENDENCIES = {
    "operator_execution_queue",
    "manual_trade_packet",
    "manual_execution_receipt",
}

STRATEGIC_AUTHORITIES: list[dict[str, Any]] = [
    {
        "artifact_name": "thesis_registry",
        "current_owner": "Research / thesis lifecycle",
        "strategic_owner": "Thesis Registry",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "research_hypothesis_registry",
        "current_owner": "Research Validation Engine",
        "strategic_owner": "Hypothesis Registry",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "candidate_generation",
        "current_owner": "Candidate generation producers",
        "strategic_owner": "Candidate Generation",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "candidate_generation_diagnostics",
        "current_owner": "Candidate diagnostics",
        "strategic_owner": "Candidate Diagnostics",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "paper_position_ledger",
        "current_owner": "Paper ledger",
        "strategic_owner": "Paper Position Ledger",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "outcome_registry",
        "current_owner": "Paper outcomes / outcome validation",
        "strategic_owner": "Outcome Registry",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "research_validation_samples",
        "current_owner": "Research validation samples",
        "strategic_owner": "Validation Samples",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "statistical_sufficiency",
        "current_owner": "Research Validation Engine",
        "strategic_owner": "Statistical Sufficiency",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "hypothesis_state_machine",
        "current_owner": "Research validation / qualification",
        "strategic_owner": "Hypothesis State Machine",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "research_portfolio",
        "current_owner": "Research portfolio",
        "strategic_owner": "Research Portfolio",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "research_capital_allocation",
        "current_owner": "Research capital allocation",
        "strategic_owner": "Research Capital Allocation",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "operator_action_model",
        "current_owner": "Operator action model",
        "strategic_owner": "Operator Action Model",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority.",
    },
    {
        "artifact_name": "aegis_strategic_operating_status",
        "current_owner": "Strategic operating status projection",
        "strategic_owner": "Paper Trading + Hypothesis Validation Architecture",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority projection for runtime truth.",
    },
    {
        "artifact_name": "aegis_research_eod_summary",
        "current_owner": "Research EOD summary projection",
        "strategic_owner": "Paper Trading + Hypothesis Validation Architecture",
        "authority_status": "STRATEGIC_AUTHORITY",
        "migration_state": "ACTIVE",
        "replacement_artifact": None,
        "deprecation_criteria": "Not applicable; strategic authority projection for runtime truth.",
    },
]

LITE_COMPATIBILITY_AUTHORITIES: list[dict[str, Any]] = [
    {
        "artifact_name": "aegis_lite_operating_status",
        "current_owner": "Aegis Lite",
        "strategic_owner": "aegis_strategic_operating_status_v1",
        "authority_status": "LEGACY_READ_ONLY",
        "migration_state": "MIGRATING",
        "replacement_artifact": "aegis_strategic_operating_status_v1",
        "deprecation_criteria": "All remaining UI/read-model compatibility consumers no longer reference aegis_lite_operating_status and audit still passes without it.",
        "why_it_still_exists": "Compatibility readers and historical audit flows may still inspect it; runtime truth no longer uses it for DATA_READY.",
        "consumed_by": ["legacy Lite UI/read models", "repair/readiness compatibility flows"],
        "audit_dependency_to_change": "Completed for DATA_READY; remaining work is compatibility consumer cleanup before deprecation.",
        "ui_language_change": "Use Aegis Operating Status; if this artifact appears, label it legacy compatibility layer.",
    },
    {
        "artifact_name": "aegis_lite_eod_report",
        "current_owner": "Aegis Lite",
        "strategic_owner": "aegis_research_eod_summary_v1",
        "authority_status": "LEGACY_READ_ONLY",
        "migration_state": "MIGRATING",
        "replacement_artifact": "aegis_research_eod_summary_v1",
        "deprecation_criteria": "All remaining UI/read-model compatibility consumers no longer reference aegis_lite_eod_report and audit still passes without it.",
        "why_it_still_exists": "Compatibility readers and historical audit flows may still inspect it; runtime truth no longer uses it for DATA_READY or EOD advisory mode readiness.",
        "consumed_by": ["legacy Lite UI/read models", "repair_center compatibility flows", "current_operator_truth_resolver compatibility flows"],
        "audit_dependency_to_change": "Completed for DATA_READY and EOD_ADVISORY_MODE; remaining work is compatibility consumer cleanup before deprecation.",
        "ui_language_change": "Use Research EOD Summary; if this artifact appears, label it legacy compatibility layer.",
    },
    {
        "artifact_name": "operator_execution_queue",
        "current_owner": "Aegis Lite",
        "strategic_owner": "Operator Action Model",
        "authority_status": "COMPATIBILITY_AUTHORITY",
        "migration_state": "LEGACY_COMPATIBILITY",
        "replacement_artifact": "operator_action_model + paper_review_queue",
        "deprecation_criteria": "Runtime truth no longer requires operator_execution_queue and operator action model owns actionability semantics.",
        "why_it_still_exists": "Runtime truth uses it as a manual queue compatibility artifact.",
        "consumed_by": ["runtime_truth_kernel", "paper_golden_path", "operational_maturity_hardening", "lineage tools"],
        "audit_dependency_to_change": "Depend on operator action model evidence instead of Lite queue evidence.",
        "ui_language_change": "Avoid execution queue unless explicitly labeled legacy/manual compatibility.",
    },
    {
        "artifact_name": "manual_trade_packet",
        "current_owner": "Aegis Lite",
        "strategic_owner": "Paper review / paper open authorization",
        "authority_status": "COMPATIBILITY_AUTHORITY",
        "migration_state": "LEGACY_COMPATIBILITY",
        "replacement_artifact": "paper_open_authorization + paper_review_queue + paper_trade_construction",
        "deprecation_criteria": "Runtime truth no longer requires manual_trade_packet and paper authorization/construction own paper path evidence.",
        "why_it_still_exists": "Runtime truth uses it for manual packet readiness and compatibility with the older manual-paper flow.",
        "consumed_by": ["runtime_truth_kernel", "paper_golden_path", "sleeve_attribution", "lineage", "receipt/outcome tools"],
        "audit_dependency_to_change": "Manual packet stops being readiness authority and becomes historical/manual compatibility evidence.",
        "ui_language_change": "Use manual compatibility packet rather than primary trading packet.",
    },
    {
        "artifact_name": "manual_execution_receipt",
        "current_owner": "Manual receipt bridge",
        "strategic_owner": "Paper receipts / paper position ledger",
        "authority_status": "COMPATIBILITY_AUTHORITY",
        "migration_state": "LEGACY_COMPATIBILITY",
        "replacement_artifact": "paper_receipt + paper_position_ledger + paper_trade_outcomes",
        "deprecation_criteria": "Runtime truth no longer requires manual_execution_receipt and paper receipts/ledger prove current-day paper state.",
        "why_it_still_exists": "Runtime truth and receipt/outcome bridge use it to prove manual paper capture or lack of capture.",
        "consumed_by": ["runtime_truth_kernel", "manual receipt recorder", "outcome recording", "paper_golden_path", "sleeve_attribution"],
        "audit_dependency_to_change": "Replace manual receipt bridge with paper receipt/ledger proof for paper-mode readiness.",
        "ui_language_change": "Use paper receipt / ledger evidence except in diagnostics.",
    },
    {
        "artifact_name": "historical_lite_reports",
        "current_owner": "Aegis Lite archive",
        "strategic_owner": "Evidence / audit archive",
        "authority_status": "LEGACY_READ_ONLY",
        "migration_state": "LEGACY_COMPATIBILITY",
        "replacement_artifact": "evidence lineage / audit detail",
        "deprecation_criteria": "Historical Lite reports are no longer used for current-day readiness and remain read-only for lineage only.",
        "why_it_still_exists": "Historical reports preserve audit lineage and migration evidence.",
        "consumed_by": ["historical reporting", "lineage tools", "audit detail"],
        "audit_dependency_to_change": "None for current-day readiness; keep read-only lineage access.",
        "ui_language_change": "Label as historical Lite evidence.",
    },
]

OPERATOR_LANGUAGE_RULES = {
    "preferred_language": [
        "Paper Trading",
        "Hypothesis Validation",
        "Research Portfolio",
        "Outcome Validation",
        "Operator Action Model",
        "Manual Compatibility Layer",
        "Paper session",
        "Paper ledger",
        "Paper review",
    ],
    "avoid_strategic_emphasis_on": [
        "Aegis Lite",
        "Lite Control Plane",
        "Lite Report as primary status",
        "Lite as main architecture",
        "Lite EOD as the top-level daily status",
    ],
    "lite_allowed_labels": [
        "Legacy compatibility layer",
        "Manual compatibility bridge",
        "Historical Lite evidence",
    ],
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def architecture_authority_model_path_v1(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "reports" / SCHEMA_ID / day_utc / "architecture_authority_model.v1.json"


def build_architecture_authority_model_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    authorities = [*STRATEGIC_AUTHORITIES, *LITE_COMPATIBILITY_AUTHORITIES]
    replacement_mapping = {
        row["artifact_name"]: row["replacement_artifact"]
        for row in LITE_COMPATIBILITY_AUTHORITIES
        if row.get("replacement_artifact")
    }
    deprecation_blockers = []
    for row in LITE_COMPATIBILITY_AUTHORITIES:
        artifact = row["artifact_name"]
        if artifact in RUNTIME_TRUTH_LITE_DEPENDENCIES:
            deprecation_blockers.append({
                "artifact_name": artifact,
                "blocker": "RUNTIME_TRUTH_STILL_REQUIRES_ARTIFACT",
                "required_before_deprecation": row["deprecation_criteria"],
                "replacement_artifact": row.get("replacement_artifact"),
            })
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": f"{SCHEMA_ID}:{day_utc}",
        "day_utc": day_utc,
        "generated_at": utc_now(),
        "strategic_system_of_record": {
            "name": STRATEGIC_SYSTEM_OF_RECORD,
            "classification": "STRATEGIC_SYSTEM_OF_RECORD",
            "summary": "Paper Trading + Hypothesis Validation owns future strategic Aegis truth.",
        },
        "legacy_compatibility_layers": [
            {
                "name": LEGACY_COMPATIBILITY_LAYER,
                "classification": "LEGACY_COMPATIBILITY_LAYER",
                "summary": "Aegis Lite remains only as a manual/historical compatibility bridge until runtime truth dependencies migrate.",
            }
        ],
        "artifact_authorities": authorities,
        "migration_states": {row["artifact_name"]: row["migration_state"] for row in authorities},
        "replacement_mapping": replacement_mapping,
        "runtime_truth_lite_dependencies": sorted(RUNTIME_TRUTH_LITE_DEPENDENCIES),
        "deprecation_blockers": deprecation_blockers,
        "operator_ui_language_rules": OPERATOR_LANGUAGE_RULES,
        "audit_runtime_truth_impact": {
            "runtime_dependencies_changed": False,
            "audit_weakened": False,
            "lite_removed": False,
            "trade_advice_enabled": False,
            "manual_capture_enabled": False,
            "broker_execution_enabled": False,
            "note": "This model declares authority and migration state only; it does not change runtime truth dependencies.",
        },
    }
    return payload


def write_architecture_authority_model_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    day_utc = str(payload["day_utc"])
    path = architecture_authority_model_path_v1(truth_root, day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def validate_architecture_authority_model_v1(payload: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    if payload.get("schema_id") != SCHEMA_ID:
        failures.append("invalid_schema_id")
    if payload.get("strategic_system_of_record", {}).get("classification") != "STRATEGIC_SYSTEM_OF_RECORD":
        failures.append("missing_strategic_system_of_record")
    if not any(layer.get("classification") == "LEGACY_COMPATIBILITY_LAYER" and layer.get("name") == LEGACY_COMPATIBILITY_LAYER for layer in payload.get("legacy_compatibility_layers", [])):
        failures.append("aegis_lite_not_classified_as_legacy_compatibility_layer")
    rows = payload.get("artifact_authorities") or []
    by_name = {row.get("artifact_name"): row for row in rows if isinstance(row, dict)}
    required_strategic = {
        "candidate_generation",
        "candidate_generation_diagnostics",
        "paper_position_ledger",
        "outcome_registry",
        "research_validation_samples",
        "statistical_sufficiency",
        "hypothesis_state_machine",
        "research_portfolio",
        "research_capital_allocation",
        "operator_action_model",
    }
    for artifact in required_strategic:
        row = by_name.get(artifact)
        if not row:
            failures.append(f"missing_strategic_artifact:{artifact}")
        elif row.get("authority_status") != "STRATEGIC_AUTHORITY" or not row.get("strategic_owner"):
            failures.append(f"invalid_strategic_owner:{artifact}")
    for artifact in RUNTIME_TRUTH_LITE_DEPENDENCIES:
        row = by_name.get(artifact)
        if not row:
            failures.append(f"missing_lite_artifact:{artifact}")
            continue
        if row.get("migration_state") not in ALLOWED_MIGRATION_STATES:
            failures.append(f"invalid_migration_state:{artifact}")
        if row.get("authority_status") in {"DEPRECATED", "REMOVED"}:
            failures.append(f"lite_deprecated_while_runtime_truth_requires:{artifact}")
        if not row.get("replacement_artifact"):
            failures.append(f"missing_replacement_artifact:{artifact}")
        if not row.get("deprecation_criteria"):
            failures.append(f"missing_deprecation_criteria:{artifact}")
    for row in rows:
        artifact = row.get("artifact_name")
        if row.get("authority_status") not in ALLOWED_AUTHORITY_STATUSES:
            failures.append(f"invalid_authority_status:{artifact}")
        if row.get("migration_state") not in ALLOWED_MIGRATION_STATES:
            failures.append(f"invalid_migration_state:{artifact}")
    if payload.get("audit_runtime_truth_impact", {}).get("runtime_dependencies_changed") is not False:
        failures.append("runtime_truth_dependency_change_not_allowed")
    for safety_key in ["trade_advice_enabled", "manual_capture_enabled", "broker_execution_enabled"]:
        if payload.get("audit_runtime_truth_impact", {}).get(safety_key) is not False:
            failures.append(f"safety_gate_changed:{safety_key}")
    return {"ok": not failures, "failure_count": len(failures), "failures": failures}


def build_and_write_architecture_authority_model_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    payload = build_architecture_authority_model_v1(truth_root=truth_root, day_utc=day_utc)
    validation = validate_architecture_authority_model_v1(payload)
    path = write_architecture_authority_model_v1(truth_root=truth_root, payload=payload)
    return {"ok": validation["ok"], "path": str(path), "payload": payload, "validation": validation}
