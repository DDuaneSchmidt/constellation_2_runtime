#!/usr/bin/env python3
import json
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime")
TRUTH_ROOT = REPO_ROOT / "constellation_2" / "runtime" / "truth"
SYSTEM_SNAPSHOT_ROOT = TRUTH_ROOT / "system_snapshot"
GOVERNANCE_ROOT = REPO_ROOT / "governance"
CONTRACTS_ROOT = GOVERNANCE_ROOT / "contracts"


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def assert_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise SystemExit(f"FAIL_CLOSED: missing required {label}: {path}")


def write_json(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=False)
        f.write("\n")


def write_text(path: Path, text: str) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write(text.rstrip() + "\n")


def main() -> None:
    generated_utc = now_utc()

    # Proven roots
    assert_exists(REPO_ROOT, "repo root")
    assert_exists(GOVERNANCE_ROOT, "governance root")
    assert_exists(TRUTH_ROOT, "truth root")
    assert_exists(REPO_ROOT / "ops" / "tools", "ops/tools")
    assert_exists(REPO_ROOT / "ops" / "run", "ops/run")
    assert_exists(GOVERNANCE_ROOT / "00_INDEX.md", "governance index")
    assert_exists(GOVERNANCE_ROOT / "00_MANIFEST.yaml", "governance manifest")
    assert_exists(GOVERNANCE_ROOT / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json", "sleeve registry")
    assert_exists(GOVERNANCE_ROOT / "02_REGISTRIES" / "C2_ENGINE_IDS_V1.md", "engine registry")
    assert_exists(GOVERNANCE_ROOT / "02_REGISTRIES" / "C2_TRUTH_AUTHORITY_REGISTRY_V1.json", "truth authority registry")
    assert_exists(GOVERNANCE_ROOT / "02_REGISTRIES" / "TRUTH_SURFACE_AUTHORITY_V1.json", "truth surface authority registry")
    assert_exists(GOVERNANCE_ROOT / "02_REGISTRIES" / "C2_SPINE_AUTHORITY_V1.json", "spine authority registry")

    ensure_dir(SYSTEM_SNAPSHOT_ROOT)
    ensure_dir(CONTRACTS_ROOT)

    architecture_snapshot = {
        "artifact_id": "constellation_system_snapshot",
        "schema_version": "1.0",
        "generated_utc": generated_utc,
        "system_identity": {
            "name": "Constellation 2.0",
            "repo_root": str(REPO_ROOT),
            "governance_root": str(GOVERNANCE_ROOT),
            "canonical_truth_root": str(TRUTH_ROOT),
            "authority_model": [
                "governance",
                "git",
                "runtime_truth"
            ],
            "deployment_mode": "PARTIALLY_PROVEN"
        },
        "topology": {
            "status": "PROVEN_ACTIVE",
            "multi_account_topology": "PROVEN_ACTIVE",
            "sleeve_partitioning": "PROVEN_ACTIVE"
        },
        "sleeves": [
            {
                "sleeve_id": "PRIMARY",
                "status": "PROVEN_ACTIVE",
                "mode": "PAPER",
                "ib_account": "DUO847203",
                "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                "symbols": [
                    "SPY"
                ]
            }
        ],
        "engines": [
            {
                "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                "status": "PROVEN_PRESENT"
            },
            {
                "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                "status": "PROVEN_PRESENT"
            },
            {
                "engine_id": "C2_MEAN_REVERSION_EQ_V1",
                "status": "PROVEN_PRESENT"
            },
            {
                "engine_id": "C2_EVENT_DISLOCATION_V1",
                "status": "PROVEN_PRESENT"
            },
            {
                "engine_id": "C2_DEFENSIVE_TAIL_V1",
                "status": "PROVEN_PRESENT"
            }
        ],
        "spines": [
            {
                "spine": "accounting",
                "active_version": "v2",
                "status": "PROVEN_ACTIVE"
            },
            {
                "spine": "monitoring",
                "active_version": "v1",
                "status": "PROVEN_ACTIVE"
            }
        ],
        "execution_lifecycle": [
            {
                "stage": "signal_or_strategy_output",
                "status": "PARTIALLY_PROVEN"
            },
            {
                "stage": "intent_generation",
                "status": "PROVEN_ACTIVE",
                "primary_stream": "constellation_2/runtime/truth/intents_v1/snapshots"
            },
            {
                "stage": "intent_day_rollup",
                "status": "PROVEN_ACTIVE",
                "primary_stream": "constellation_2/runtime/truth/intents_v1/day_rollup"
            },
            {
                "stage": "phaseC_preflight",
                "status": "PROVEN_ACTIVE",
                "primary_stream": "constellation_2/runtime/truth/phaseC_preflight_v1"
            },
            {
                "stage": "order_planning",
                "status": "PROVEN_ACTIVE",
                "embedded_artifacts": [
                    "equity_order_plan.v2.json",
                    "binding_record.v2.json",
                    "mapping_ledger_record.v2.json"
                ]
            },
            {
                "stage": "governed_submission",
                "status": "PROVEN_ACTIVE",
                "primary_stream": "constellation_2/runtime/truth/execution_evidence_v1/submissions"
            },
            {
                "stage": "submission_indexing",
                "status": "PROVEN_ACTIVE",
                "primary_stream": "constellation_2/runtime/truth/execution_evidence_v1/submission_index"
            },
            {
                "stage": "broker_event_capture",
                "status": "PROVEN_ACTIVE",
                "primary_stream": "constellation_2/runtime/truth/execution_evidence_v1/broker_events"
            },
            {
                "stage": "fill_ledger",
                "status": "PROVEN_ACTIVE",
                "primary_stream": "constellation_2/runtime/truth/fill_ledger_v1"
            },
            {
                "stage": "position_lifecycle",
                "status": "PROVEN_ACTIVE",
                "primary_stream": "constellation_2/runtime/truth/position_lifecycle_v2"
            },
            {
                "stage": "positions_truth",
                "status": "PROVEN_ACTIVE",
                "primary_stream": "constellation_2/runtime/truth/positions_v1"
            },
            {
                "stage": "reconciliation",
                "status": "PROVEN_ACTIVE",
                "primary_streams": [
                    "constellation_2/runtime/truth/reports/execution_reconciliation_v1",
                    "constellation_2/runtime/truth/reports/broker_reconciliation_v2",
                    "constellation_2/runtime/truth/reports/broker_reconciliation_v3",
                    "constellation_2/runtime/truth/reports/reconciliation_report_v2",
                    "constellation_2/runtime/truth/reports/reconciliation_report_v3",
                    "constellation_2/runtime/truth/exit_reconciliation_v1",
                    "constellation_2/runtime/truth/exposure_reconciliation_v2"
                ]
            }
        ],
        "runtime_truth_roots": [
            "constellation_2/runtime/truth",
            "constellation_2/runtime/truth_sleeves/PRIMARY/PAPER"
        ],
        "governance_root": str(GOVERNANCE_ROOT),
        "key_entrypoints": [
            "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
            "ops/run/c2_multi_sleeve_orchestrator_run_v1.sh",
            "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py",
            "ops/tools/run_c2_paper_day_orchestrator_v1.py",
            "ops/tools/run_c2_paper_day_orchestrator_v2.py",
            "ops/tools/run_c2_bond_sleeve_day_v1.py",
            "ops/tools/run_weekly_ai_auto_v1.py",
            "ops/tools/run_weekly_ai_loop_v1.py"
        ],
        "artifact_streams": [
            "intents_v1",
            "phaseC_preflight_v1",
            "execution_evidence_v1",
            "fill_ledger_v1",
            "position_lifecycle_v2",
            "positions_v1",
            "allocation_v1",
            "monitoring_v1",
            "reports",
            "risk_v1",
            "run_pointer_v1",
            "run_pointer_v2",
            "truth_sleeves/PRIMARY/PAPER"
        ],
        "unknowns": [
            "additional_sleeves_beyond_PRIMARY",
            "exact_engine_to_sleeve_mapping_for_all_registered_engines",
            "definitive_single_canonical_execution_timeline_source_between_execution_stream_v1_and_execution_evidence_v1_broker_events",
            "full_deployment_mode_semantics_beyond_proven_PAPER_primary_sleeve"
        ]
    }

    ai_architecture_index = {
        "artifact_id": "constellation_ai_architecture_index",
        "schema_version": "1.0",
        "generated_utc": generated_utc,
        "repo_root": str(REPO_ROOT),
        "core_directories": {
            "governance_directory": "governance",
            "schemas_directory": "governance/04_DATA/SCHEMAS",
            "runtime_truth_root": "constellation_2/runtime/truth",
            "reports_root": "constellation_2/runtime/reports",
            "sleeve_runtime_root": "constellation_2/runtime/truth_sleeves",
            "ops_tools_root": "ops/tools",
            "ops_run_root": "ops/run",
            "bond_sleeve_root": "constellation_2/bond_sleeve",
            "operator_inputs_root": "constellation_2/operator_inputs"
        },
        "critical_runtime_streams": [
            "constellation_2/runtime/truth/intents_v1/snapshots",
            "constellation_2/runtime/truth/intents_v1/day_rollup",
            "constellation_2/runtime/truth/phaseC_preflight_v1",
            "constellation_2/runtime/truth/execution_evidence_v1/submissions",
            "constellation_2/runtime/truth/execution_evidence_v1/submission_index",
            "constellation_2/runtime/truth/execution_evidence_v1/manifests",
            "constellation_2/runtime/truth/execution_evidence_v1/broker_events",
            "constellation_2/runtime/truth/fill_ledger_v1",
            "constellation_2/runtime/truth/position_lifecycle_v2",
            "constellation_2/runtime/truth/positions_v1",
            "constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1",
            "constellation_2/runtime/truth/monitoring_v1/lifecycle_monitor",
            "constellation_2/runtime/truth/monitoring_v1/paper_readiness",
            "constellation_2/runtime/truth/reports/operator_daily_gate_v3",
            "constellation_2/runtime/truth/reports/gate_stack_verdict_v1",
            "constellation_2/runtime/truth/reports/reconciliation_report_v3"
        ],
        "daily_authority_artifacts": [
            "constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1/{DAY}/capital_authority_allocation.v1.json",
            "constellation_2/runtime/truth/reports/capital_risk_envelope_v2/{DAY}/capital_risk_envelope.v2.json",
            "constellation_2/runtime/truth/reports/capital_seed_gate_v1/{DAY}/capital_seed_gate.v1.json",
            "constellation_2/runtime/truth/reports/operator_daily_gate_v3/{DAY}/operator_daily_gate.v3.json",
            "constellation_2/runtime/truth/reports/gate_stack_verdict_v1/{DAY}/gate_stack_verdict.v1.json",
            "constellation_2/runtime/truth/ib_api_handshake/{DAY}/ib_api_handshake.v1.json"
        ],
        "system_orchestrators": [
            "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
            "ops/run/c2_multi_sleeve_orchestrator_run_v1.sh",
            "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py",
            "ops/tools/run_c2_paper_day_orchestrator_v1.py",
            "ops/tools/run_c2_paper_day_orchestrator_v2.py",
            "ops/run/c2_supervisor_paper_v1.py",
            "ops/run/c2_supervisor_paper_v2.py"
        ],
        "contract_locations": [
            "governance/00_INDEX.md",
            "governance/00_MANIFEST.yaml",
            "governance/02_REGISTRIES",
            "governance/03_CONTRACTS",
            "governance/05_CONTRACTS"
        ],
        "unknowns": [
            "manifest_registration_for_new_ai_contract_docs_not_performed_by_this_script",
            "no_existing_governed_schema_proven_for_system_snapshot_artifacts"
        ]
    }

    capability_manifest = {
        "artifact_id": "constellation_capability_manifest",
        "schema_version": "1.0",
        "generated_utc": generated_utc,
        "system_name": "Constellation 2.0",
        "capabilities": {
            "signal_generation": {
                "capability_name": "signal_generation",
                "description": "Signal-producing engine surfaces are present, but end-to-end active signal emission stream is only partially proven from the supplied evidence.",
                "evidence_source": [
                    "constellation_2/phaseI",
                    "constellation_2/phaseH/tools/run_oms_decisions_day_v2.py"
                ],
                "input_artifacts": [],
                "output_artifacts": [],
                "owner_subsystem_or_path": "constellation_2/phaseI",
                "status": "PARTIALLY_PROVEN"
            },
            "intent_generation": {
                "capability_name": "intent_generation",
                "description": "Exposure intents are written into canonical truth.",
                "evidence_source": [
                    "constellation_2/runtime/truth/intents_v1/snapshots",
                    "constellation_2/runtime/truth/intents_v1/day_rollup"
                ],
                "input_artifacts": [],
                "output_artifacts": [
                    "exposure_intent.v1.json",
                    "exposure_intent.v2.json",
                    "intents_day_rollup.v1.json"
                ],
                "owner_subsystem_or_path": "constellation_2/runtime/truth/intents_v1",
                "status": "PROVEN_ACTIVE"
            },
            "preflight_validation": {
                "capability_name": "preflight_validation",
                "description": "Submit preflight decisions and veto records are produced in phase C truth.",
                "evidence_source": [
                    "constellation_2/runtime/truth/phaseC_preflight_v1"
                ],
                "input_artifacts": [
                    "equity_intent.v1.json"
                ],
                "output_artifacts": [
                    "submit_preflight_decision.v1.json",
                    "veto_record.v1.json"
                ],
                "owner_subsystem_or_path": "constellation_2/runtime/truth/phaseC_preflight_v1",
                "status": "PROVEN_ACTIVE"
            },
            "order_planning": {
                "capability_name": "order_planning",
                "description": "Order planning artifacts are present inside phase C preflight outputs.",
                "evidence_source": [
                    "constellation_2/runtime/truth/phaseC_preflight_v1/2026-03-06/e6f38cf89fd4c96aeabaa42725b30503550f6eda13f333278b0fe930f8436d3f/equity_order_plan.v2.json"
                ],
                "input_artifacts": [
                    "equity_intent.v1.json"
                ],
                "output_artifacts": [
                    "equity_order_plan.v2.json",
                    "binding_record.v2.json",
                    "mapping_ledger_record.v2.json"
                ],
                "owner_subsystem_or_path": "constellation_2/runtime/truth/phaseC_preflight_v1",
                "status": "PROVEN_ACTIVE"
            },
            "governed_submission": {
                "capability_name": "governed_submission",
                "description": "Governed submission evidence and submission indexes are written into execution evidence truth.",
                "evidence_source": [
                    "constellation_2/runtime/truth/execution_evidence_v1/submissions",
                    "constellation_2/runtime/truth/execution_evidence_v1/submission_index",
                    "constellation_2/runtime/truth/execution_evidence_v1/manifests"
                ],
                "input_artifacts": [
                    "equity_order_plan.v2.json"
                ],
                "output_artifacts": [
                    "submission manifests",
                    "submission indexes",
                    "submission directories"
                ],
                "owner_subsystem_or_path": "constellation_2/runtime/truth/execution_evidence_v1",
                "status": "PROVEN_ACTIVE"
            },
            "broker_execution_evidence": {
                "capability_name": "broker_execution_evidence",
                "description": "Broker events and baseline snapshots are captured in canonical truth.",
                "evidence_source": [
                    "constellation_2/runtime/truth/execution_evidence_v1/broker_events",
                    "constellation_2/runtime/truth/execution_evidence_v1/broker_baseline_snapshot_v1"
                ],
                "input_artifacts": [],
                "output_artifacts": [
                    "broker_event_day_manifest.v1.json",
                    "broker_event_log.v1.jsonl",
                    "broker_baseline_snapshot.v1.json"
                ],
                "owner_subsystem_or_path": "constellation_2/runtime/truth/execution_evidence_v1",
                "status": "PROVEN_ACTIVE"
            },
            "fill_ledger": {
                "capability_name": "fill_ledger",
                "description": "Deterministic fill ledger outputs are present in canonical truth.",
                "evidence_source": [
                    "constellation_2/runtime/truth/fill_ledger_v1"
                ],
                "input_artifacts": [
                    "broker events",
                    "submission evidence"
                ],
                "output_artifacts": [
                    "fill_ledger.v1.json"
                ],
                "owner_subsystem_or_path": "constellation_2/runtime/truth/fill_ledger_v1",
                "status": "PROVEN_ACTIVE"
            },
            "position_authority": {
                "capability_name": "position_authority",
                "description": "Positions and position lifecycle truth surfaces are present.",
                "evidence_source": [
                    "constellation_2/runtime/truth/positions_v1",
                    "constellation_2/runtime/truth/position_lifecycle_v2"
                ],
                "input_artifacts": [
                    "fill ledger",
                    "execution evidence"
                ],
                "output_artifacts": [
                    "positions_snapshot.v2.json",
                    "position_lifecycle_snapshot.v2.json"
                ],
                "owner_subsystem_or_path": "constellation_2/runtime/truth/positions_v1",
                "status": "PROVEN_ACTIVE"
            },
            "capital_authority": {
                "capability_name": "capital_authority",
                "description": "Capital authority allocation outputs and capital gate reports are present.",
                "evidence_source": [
                    "constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1",
                    "constellation_2/runtime/truth/reports/capital_risk_envelope_v2",
                    "constellation_2/runtime/truth/reports/capital_seed_gate_v1"
                ],
                "input_artifacts": [],
                "output_artifacts": [
                    "capital_authority_allocation.v1.json",
                    "capital_risk_envelope.v2.json",
                    "capital_seed_gate.v1.json"
                ],
                "owner_subsystem_or_path": "constellation_2/runtime/truth/allocation_v1",
                "status": "PROVEN_ACTIVE"
            },
            "reconciliation": {
                "capability_name": "reconciliation",
                "description": "Execution, broker, exposure, exit, and daily reconciliation/report surfaces are present.",
                "evidence_source": [
                    "constellation_2/runtime/truth/reports/execution_reconciliation_v1",
                    "constellation_2/runtime/truth/reports/broker_reconciliation_v2",
                    "constellation_2/runtime/truth/reports/broker_reconciliation_v3",
                    "constellation_2/runtime/truth/reports/reconciliation_report_v2",
                    "constellation_2/runtime/truth/reports/reconciliation_report_v3",
                    "constellation_2/runtime/truth/exit_reconciliation_v1",
                    "constellation_2/runtime/truth/exposure_reconciliation_v2"
                ],
                "input_artifacts": [
                    "positions",
                    "execution evidence",
                    "fill ledger"
                ],
                "output_artifacts": [
                    "execution reconciliation",
                    "broker reconciliation",
                    "reconciliation report",
                    "exit reconciliation",
                    "exposure reconciliation"
                ],
                "owner_subsystem_or_path": "constellation_2/runtime/truth/reports",
                "status": "PROVEN_ACTIVE"
            },
            "reporting": {
                "capability_name": "reporting",
                "description": "Daily operational, gate, and pipeline reports are written under canonical truth reports.",
                "evidence_source": [
                    "constellation_2/runtime/truth/reports/operator_daily_gate_v3",
                    "constellation_2/runtime/truth/reports/gate_stack_verdict_v1",
                    "constellation_2/runtime/truth/reports/pipeline_manifest_v3",
                    "constellation_2/runtime/truth/reports/preopen_preflight_v1"
                ],
                "input_artifacts": [],
                "output_artifacts": [
                    "operator_daily_gate.v3.json",
                    "gate_stack_verdict.v1.json",
                    "pipeline_manifest.v3.json",
                    "preopen_preflight.v1.json"
                ],
                "owner_subsystem_or_path": "constellation_2/runtime/truth/reports",
                "status": "PROVEN_ACTIVE"
            },
            "system_orchestration": {
                "capability_name": "system_orchestration",
                "description": "Paper-day and multi-sleeve orchestrator entrypoints are present.",
                "evidence_source": [
                    "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
                    "ops/run/c2_multi_sleeve_orchestrator_run_v1.sh",
                    "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py",
                    "ops/tools/run_c2_paper_day_orchestrator_v2.py"
                ],
                "input_artifacts": [],
                "output_artifacts": [
                    "runtime truth and reports produced by orchestrated daily runs"
                ],
                "owner_subsystem_or_path": "ops/run and ops/tools",
                "status": "PROVEN_ACTIVE"
            },
            "ai_analysis": {
                "capability_name": "ai_analysis",
                "description": "Weekly AI review packet generation is present in runtime reports.",
                "evidence_source": [
                    "ops/tools/run_weekly_ai_auto_v1.py",
                    "ops/tools/run_weekly_ai_loop_v1.py",
                    "ops/tools/run_weekly_ai_packet_v1.py",
                    "constellation_2/runtime/reports/weekly_ai_review/2026-03-06"
                ],
                "input_artifacts": [],
                "output_artifacts": [
                    "weekly_diagnostic_memo_YYYY-MM-DD.md",
                    "weekly_diagnostic_evidence_YYYY-MM-DD.json",
                    "weekly_diagnostic_support_YYYY-MM-DD.json",
                    "ai_response_YYYY-MM-DD.md"
                ],
                "owner_subsystem_or_path": "ops/tools and constellation_2/runtime/reports/weekly_ai_review",
                "status": "PROVEN_ACTIVE"
            },
            "bond_sleeve_support": {
                "capability_name": "bond_sleeve_support",
                "description": "Bond sleeve tool, operator inputs, and bond report outputs are present.",
                "evidence_source": [
                    "ops/tools/run_c2_bond_sleeve_day_v1.py",
                    "constellation_2/operator_inputs/bond_sleeve",
                    "constellation_2/runtime/truth/reports/bond_duration_report_v1",
                    "constellation_2/runtime/truth/reports/bond_ladder_recommendation_v1",
                    "constellation_2/runtime/truth/reports/bond_sleeve_policy_snapshot_v1"
                ],
                "input_artifacts": [
                    "bond_positions_v1.json",
                    "bond_sleeve_policy_v1.json"
                ],
                "output_artifacts": [
                    "bond_duration_report",
                    "bond_ladder_recommendation",
                    "bond_sleeve_capital_posture",
                    "bond_sleeve_policy_snapshot",
                    "bond_withdrawal_coverage_report",
                    "bond_yield_report"
                ],
                "owner_subsystem_or_path": "constellation_2/bond_sleeve and ops/tools",
                "status": "PROVEN_ACTIVE"
            }
        }
    }

    ai_reasoning_contract = f"""
# Constellation AI Reasoning Contract v1

## Purpose

This contract defines how any AI agent must reason about the Constellation repository and its runtime truth surfaces.

## Scope

This contract applies to AI analysis of:
- repository architecture
- runtime truth
- paper-trading readiness
- lifecycle completeness
- diagnostics
- capability claims
- sleeve-scoped and system-scoped truth surfaces

## Required Inputs

An AI agent must load these inputs in this order before making architecture or readiness claims:

1. `constellation_2/runtime/truth/system_snapshot/constellation_system_snapshot.v1.json`
2. `constellation_2/runtime/truth/system_snapshot/constellation_ai_architecture_index.v1.json`
3. `constellation_2/runtime/truth/system_snapshot/constellation_capability_manifest.v1.json`

The AI agent must also consult proven governance authorities when needed:
- `governance/00_INDEX.md`
- `governance/00_MANIFEST.yaml`
- `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`
- `governance/02_REGISTRIES/C2_ENGINE_IDS_V1.md`
- `governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json`
- `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- `governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json`

## Reasoning Procedure

1. Prove repo root, governance root, and canonical truth root.
2. Load the system snapshot first.
3. Load the AI architecture index second.
4. Load the capability manifest before any capability analysis.
5. Verify every referenced path before making a path-based claim.
6. Classify every major conclusion as one of:
   - PROVEN_ACTIVE
   - PROVEN_PRESENT
   - PARTIALLY_PROVEN
   - UNKNOWN
   - DEPRECATED only where explicit repo evidence proves deprecation
7. For readiness analysis, use the execution lifecycle encoded in the system snapshot.
8. Where multiple versioned surfaces exist, prefer the active authority declared in proven registries.
9. Where no active authority is proven, state the ambiguity explicitly.

## Evidence Standard

Valid proof types are limited to:
- path exists
- schema exists
- contract exists
- artifact stream exists
- script references component
- report references component
- runtime artifact sample exists

Naming alone is not sufficient proof of behavior.

## Allowed Inference Boundary

Allowed inference is limited to:
- restating behavior that is directly encoded by a proven contract, registry, schema, path, or runtime artifact sample
- connecting adjacent lifecycle surfaces only when both sides are independently proven
- using declared active versions from proven registries to select preferred surfaces

Disallowed inference includes:
- inventing sleeves, engines, paths, contracts, or artifact streams
- assuming active behavior solely from source code presence
- treating quarantined or non-authoritative roots as canonical
- promoting present-but-unproven behavior into proven capability

## Unknown Handling

If evidence is insufficient, the AI agent must:
- mark the field or conclusion `UNKNOWN`, or
- omit the field when omission is cleaner and does not hide uncertainty

The AI agent must not use placeholders, examples, or speculative filler in authoritative outputs.

## Fail-Closed Rules

The AI agent must fail closed when:
- repo root is unproven
- canonical truth root is unproven
- a claim depends on an unverified path
- a capability exceeds proven evidence
- readiness analysis requires lifecycle steps not proven in runtime truth or governance authority
- an authoritative version cannot be determined from proven registries and the ambiguity is material

## Operating Constraints

The AI agent must treat:
- `constellation_2/runtime/truth/` as canonical system truth
- `constellation_2/runtime/truth_sleeves/<sleeve_id>/<mode>/` as canonical sleeve partition truth where proven
- quarantined, archived, deprecated, or non-authoritative roots as non-canonical unless an explicit authority contract states otherwise

Generated UTC: {generated_utc}
""".strip()

    system_invariants = f"""
# Constellation System Invariants v1

## Purpose

This document records only those invariants that are already proven by repository architecture, registries, contracts, or runtime truth structure.

## Invariants

### INV-001 — Canonical truth root
- **Statement:** Authoritative immutable runtime output must live under `constellation_2/runtime/truth/`.
- **Rationale:** Governance index and manifest define the canonical truth root.
- **Proof basis:** `governance/00_INDEX.md`, `governance/00_MANIFEST.yaml`
- **Enforcement point if proven:** governance authority model and truth-root-preflight surfaces
- **Violation impact:** Canonical truth becomes ambiguous; audit integrity fails.

### INV-002 — Sleeve truth partitioning
- **Statement:** Sleeve-scoped truth must live under `truth_sleeves/<sleeve_id>/<mode>` where sleeve partitioning is in use.
- **Rationale:** Sleeve registry defines canonical sleeve truth partition pattern.
- **Proof basis:** `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`
- **Enforcement point if proven:** sleeve registry and runtime truth_sleeves paths
- **Violation impact:** Sleeve ownership becomes ambiguous and sleeve auditability fails.

### INV-003 — Registered engine IDs only
- **Statement:** Every `engine_id` must be one of the identifiers listed in the governed engine registry.
- **Rationale:** The engine registry explicitly states any other engine ID is invalid and must fail closed.
- **Proof basis:** `governance/02_REGISTRIES/C2_ENGINE_IDS_V1.md`
- **Enforcement point if proven:** validation-time engine ID checks
- **Violation impact:** Invalid engine attribution and non-governed execution lineage.

### INV-004 — Accounting spine authority
- **Statement:** The active accounting spine is `accounting_v2`.
- **Rationale:** Spine authority registry marks accounting v2 active and exclusive.
- **Proof basis:** `governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json`
- **Enforcement point if proven:** spine authority governance and consumers of accounting truth
- **Violation impact:** Split-brain accounting truth.

### INV-005 — Monitoring spine authority
- **Statement:** The active monitoring spine is `monitoring_v1`.
- **Rationale:** Spine authority registry marks monitoring v1 active and exclusive.
- **Proof basis:** `governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json`
- **Enforcement point if proven:** spine authority governance and consumers of monitoring truth
- **Violation impact:** Split-brain monitoring truth.

### INV-006 — Position lifecycle authority
- **Statement:** The authoritative position lifecycle root is `constellation_2/runtime/truth/position_lifecycle_v2`.
- **Rationale:** Truth authority registry declares v2 authoritative and v1 non-authoritative.
- **Proof basis:** `governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json`
- **Enforcement point if proven:** truth authority selection by readers and gates
- **Violation impact:** Position lifecycle reconstruction becomes non-authoritative.

### INV-007 — Positions snapshot authority
- **Statement:** The authoritative positions snapshot root is `constellation_2/runtime/truth/positions_v1/snapshots`.
- **Rationale:** Truth authority registry declares this authoritative for positions snapshots.
- **Proof basis:** `governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json`
- **Enforcement point if proven:** truth authority selection by readers and gates
- **Violation impact:** Position state may be read from deprecated or quarantined roots.

### INV-008 — Execution broker-events authority
- **Statement:** The authoritative broker-events execution evidence root is `constellation_2/runtime/truth/execution_evidence_v1/broker_events`.
- **Rationale:** Truth authority registry declares this authoritative and names v2 broker_events non-authoritative for this family.
- **Proof basis:** `governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json`
- **Enforcement point if proven:** execution evidence readers and reconcilers
- **Violation impact:** Broker event lineage can be reconstructed from the wrong source.

### INV-009 — Surface version exclusivity
- **Statement:** For exclusive surfaces, only the active declared version may be treated as authoritative from the declared enforcement date onward.
- **Rationale:** Truth surface authority registry declares active versions and exclusivity.
- **Proof basis:** `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- **Enforcement point if proven:** report readers, gates, diagnostics consumers
- **Violation impact:** Split-brain report interpretation.

### INV-010 — Operator daily gate authority
- **Statement:** `operator_daily_gate_v3` is the active authoritative operator daily gate surface from 2026-02-20 onward.
- **Rationale:** Surface authority registry declares operator_daily_gate active version v3 and exclusive.
- **Proof basis:** `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- **Enforcement point if proven:** operator readiness and daily verdict consumers
- **Violation impact:** Daily operational readiness may be evaluated against obsolete surfaces.

### INV-011 — Replay integrity authority
- **Statement:** `replay_integrity_v2` is the active authoritative replay integrity surface from 2026-02-20 onward.
- **Rationale:** Surface authority registry declares replay_integrity active version v2 and exclusive.
- **Proof basis:** `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- **Enforcement point if proven:** replay integrity readers and gates
- **Violation impact:** Replay audit conclusions may be based on obsolete evidence.

### INV-012 — Systemic risk gate authority
- **Statement:** `systemic_risk_gate_v3` is the active authoritative systemic risk gate surface from 2026-02-20 onward.
- **Rationale:** Surface authority registry declares systemic_risk_gate active version v3 and exclusive.
- **Proof basis:** `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- **Enforcement point if proven:** risk gating consumers
- **Violation impact:** Risk posture may be read from obsolete versions.

### INV-013 — Intent lineage into preflight
- **Statement:** Every proven phase C preflight decision or veto must correspond to an intent-scoped artifact set or intent-hash-scoped record.
- **Rationale:** Phase C runtime artifacts are emitted in intent-scoped names and directories.
- **Proof basis:** `constellation_2/runtime/truth/phaseC_preflight_v1`
- **Enforcement point if proven:** phase C writers and downstream lineage readers
- **Violation impact:** Intent-to-preflight traceability breaks.

### INV-014 — Order plan lineage
- **Statement:** Every proven equity order plan must belong to exactly one phase C preflight artifact set.
- **Rationale:** Order plans are embedded inside specific preflight artifact directories.
- **Proof basis:** `constellation_2/runtime/truth/phaseC_preflight_v1/.../equity_order_plan.v2.json`
- **Enforcement point if proven:** phase C materialization
- **Violation impact:** Order-plan governance lineage becomes ambiguous.

### INV-015 — Submission evidence lineage
- **Statement:** Every governed submission must appear in execution evidence submissions and/or submission index surfaces.
- **Rationale:** Submission truth surfaces exist specifically for governed submission evidence.
- **Proof basis:** `constellation_2/runtime/truth/execution_evidence_v1/submissions`, `constellation_2/runtime/truth/execution_evidence_v1/submission_index`
- **Enforcement point if proven:** execution evidence writers
- **Violation impact:** Submission traceability fails.

### INV-016 — Broker event lineage
- **Statement:** Broker event evidence must be captured under the authoritative broker-events truth family or be treated as non-authoritative.
- **Rationale:** Broker event family has an explicit authority root.
- **Proof basis:** `governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json`
- **Enforcement point if proven:** broker event observers and reconciliation readers
- **Violation impact:** Execution reconstruction may consume non-authoritative events.

### INV-017 — Fill ledger lineage
- **Statement:** Every authoritative fill ledger record must derive from execution evidence surfaces and remain inside canonical truth.
- **Rationale:** Fill ledger is a governed execution evidence output surface under canonical truth.
- **Proof basis:** `constellation_2/runtime/truth/fill_ledger_v1`
- **Enforcement point if proven:** fill ledger writer
- **Violation impact:** Fill attribution and reconciliation integrity fail.

### INV-018 — Fail-closed on unknown authority
- **Statement:** When multiple versions exist and no active authority is proven for the question at hand, the system or AI consumer must fail closed or declare the result UNKNOWN.
- **Rationale:** Governance and authority registries require explicit active authority selection.
- **Proof basis:** authority registries and governance authority model
- **Enforcement point if proven:** AI reasoning, gates, audits
- **Violation impact:** Silent acceptance of ambiguous truth surfaces.

Generated UTC: {generated_utc}
""".strip()

    event_timeline_spec = {
        "artifact_id": "constellation_event_timeline_spec",
        "schema_version": "1.0",
        "generated_utc": generated_utc,
        "normalized_fields": [
            "event_time_utc",
            "event_type",
            "event_class",
            "sleeve_id",
            "engine_id",
            "intent_id",
            "order_plan_id",
            "broker_order_id",
            "execution_event_id",
            "fill_id",
            "artifact_path",
            "source_stream",
            "status",
            "notes"
        ],
        "event_classes": [
            {
                "event_class": "INTENT",
                "status": "PROVEN_ACTIVE",
                "source_streams": [
                    "constellation_2/runtime/truth/intents_v1/snapshots",
                    "constellation_2/runtime/truth/intents_v1/day_rollup"
                ]
            },
            {
                "event_class": "PREFLIGHT",
                "status": "PROVEN_ACTIVE",
                "source_streams": [
                    "constellation_2/runtime/truth/phaseC_preflight_v1"
                ]
            },
            {
                "event_class": "ORDER_PLAN",
                "status": "PROVEN_ACTIVE",
                "source_streams": [
                    "constellation_2/runtime/truth/phaseC_preflight_v1"
                ]
            },
            {
                "event_class": "SUBMISSION",
                "status": "PROVEN_ACTIVE",
                "source_streams": [
                    "constellation_2/runtime/truth/execution_evidence_v1/submissions",
                    "constellation_2/runtime/truth/execution_evidence_v1/submission_index",
                    "constellation_2/runtime/truth/execution_evidence_v1/manifests"
                ]
            },
            {
                "event_class": "BROKER_EVENT",
                "status": "PROVEN_ACTIVE",
                "source_streams": [
                    "constellation_2/runtime/truth/execution_evidence_v1/broker_events"
                ]
            },
            {
                "event_class": "FILL",
                "status": "PROVEN_ACTIVE",
                "source_streams": [
                    "constellation_2/runtime/truth/fill_ledger_v1"
                ]
            },
            {
                "event_class": "POSITION",
                "status": "PROVEN_ACTIVE",
                "source_streams": [
                    "constellation_2/runtime/truth/position_lifecycle_v2",
                    "constellation_2/runtime/truth/positions_v1"
                ]
            },
            {
                "event_class": "RECONCILIATION",
                "status": "PROVEN_ACTIVE",
                "source_streams": [
                    "constellation_2/runtime/truth/reports/execution_reconciliation_v1",
                    "constellation_2/runtime/truth/reports/reconciliation_report_v3",
                    "constellation_2/runtime/truth/exit_reconciliation_v1",
                    "constellation_2/runtime/truth/exposure_reconciliation_v2"
                ]
            },
            {
                "event_class": "DIAGNOSTIC",
                "status": "PROVEN_ACTIVE",
                "source_streams": [
                    "constellation_2/runtime/truth/monitoring_v1/lifecycle_monitor",
                    "constellation_2/runtime/truth/monitoring_v1/paper_readiness",
                    "constellation_2/runtime/truth/reports/operator_daily_gate_v3",
                    "constellation_2/runtime/truth/reports/gate_stack_verdict_v1"
                ]
            }
        ],
        "ordering_rules": [
            "Order by event_time_utc when explicit timestamp exists.",
            "When explicit timestamp is absent, order by authoritative lifecycle class sequence: INTENT -> PREFLIGHT -> ORDER_PLAN -> SUBMISSION -> BROKER_EVENT -> FILL -> POSITION -> RECONCILIATION -> DIAGNOSTIC.",
            "When multiple artifacts exist for the same logical event, prefer the active authoritative surface version declared by governance registries.",
            "Quarantined, archived, or non-authoritative roots must not outrank canonical truth."
        ],
        "lineage_keys": [
            "sleeve_id",
            "engine_id",
            "intent_id",
            "order_plan_id",
            "broker_order_id",
            "execution_event_id",
            "fill_id",
            "artifact_path",
            "source_stream"
        ],
        "source_categories": [
            "canonical_truth",
            "sleeve_truth",
            "governed_report",
            "monitoring",
            "execution_evidence"
        ],
        "severity_status_classes": {
            "status_values": [
                "OK",
                "WARN",
                "FAIL",
                "BLOCKING",
                "UNKNOWN"
            ],
            "notes": "Status vocabulary is normalized for AI timeline reconstruction. Raw source vocabularies may differ."
        },
        "required_correlation_ids": {
            "intent_id": "PARTIALLY_PROVEN",
            "order_plan_id": "PARTIALLY_PROVEN",
            "broker_order_id": "PARTIALLY_PROVEN",
            "execution_event_id": "PARTIALLY_PROVEN",
            "fill_id": "PARTIALLY_PROVEN",
            "artifact_path": "PROVEN_ACTIVE",
            "source_stream": "PROVEN_ACTIVE"
        },
        "unknown_handling": {
            "rule": "If a source artifact lacks a normalized field, set that field to UNKNOWN or omit it in a way that preserves explicit uncertainty.",
            "non_authoritative_rule": "Do not normalize quarantined or non-authoritative roots into canonical timeline output unless they are explicitly tagged non-authoritative.",
            "ambiguity_rule": "When multiple candidate authoritative sources exist and no registry resolves precedence for the use case, classify precedence as UNKNOWN."
        }
    }

    system_diagnostics_spec = {
        "artifact_id": "constellation_system_diagnostics_spec",
        "schema_version": "1.0",
        "generated_utc": generated_utc,
        "health_domains": [
            {
                "domain": "pipeline_integrity",
                "status": "PROVEN_ACTIVE",
                "definition": "Integrity of orchestrated stage and pipeline outputs.",
                "expected_evidence_sources": [
                    "constellation_2/runtime/truth/reports/pipeline_manifest_v3",
                    "constellation_2/runtime/truth/reports/stage_run_result_v1",
                    "constellation_2/runtime/truth/reports/stage_set_run_result_v1"
                ]
            },
            {
                "domain": "lifecycle_completeness",
                "status": "PROVEN_ACTIVE",
                "definition": "Whether major lifecycle stages from intents through reconciliation are present for the day.",
                "expected_evidence_sources": [
                    "constellation_2/runtime/truth/monitoring_v1/lifecycle_monitor",
                    "constellation_2/runtime/truth/intents_v1",
                    "constellation_2/runtime/truth/phaseC_preflight_v1",
                    "constellation_2/runtime/truth/execution_evidence_v1",
                    "constellation_2/runtime/truth/fill_ledger_v1",
                    "constellation_2/runtime/truth/position_lifecycle_v2",
                    "constellation_2/runtime/truth/reports/reconciliation_report_v3"
                ]
            },
            {
                "domain": "artifact_presence",
                "status": "PROVEN_ACTIVE",
                "definition": "Presence of required truth and report artifacts.",
                "expected_evidence_sources": [
                    "constellation_2/runtime/truth/reports/gate_completeness_gate_v1",
                    "constellation_2/runtime/truth/reports/operator_daily_gate_v3"
                ]
            },
            {
                "domain": "reconciliation_status",
                "status": "PROVEN_ACTIVE",
                "definition": "Status of execution, broker, exposure, exit, and daily reconciliation outputs.",
                "expected_evidence_sources": [
                    "constellation_2/runtime/truth/reports/execution_reconciliation_v1",
                    "constellation_2/runtime/truth/reports/broker_reconciliation_v3",
                    "constellation_2/runtime/truth/reports/reconciliation_report_v3",
                    "constellation_2/runtime/truth/exposure_reconciliation_v2",
                    "constellation_2/runtime/truth/exit_reconciliation_v1"
                ]
            },
            {
                "domain": "capital_authority_readiness",
                "status": "PROVEN_ACTIVE",
                "definition": "Readiness and validity of capital allocation and capital risk authority surfaces.",
                "expected_evidence_sources": [
                    "constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1",
                    "constellation_2/runtime/truth/reports/capital_risk_envelope_v2",
                    "constellation_2/runtime/truth/reports/capital_seed_gate_v1"
                ]
            },
            {
                "domain": "execution_evidence_completeness",
                "status": "PROVEN_ACTIVE",
                "definition": "Completeness of submissions, manifests, broker events, and related evidence.",
                "expected_evidence_sources": [
                    "constellation_2/runtime/truth/execution_evidence_v1/submissions",
                    "constellation_2/runtime/truth/execution_evidence_v1/submission_index",
                    "constellation_2/runtime/truth/execution_evidence_v1/manifests",
                    "constellation_2/runtime/truth/execution_evidence_v1/broker_events"
                ]
            },
            {
                "domain": "fill_ledger_integrity",
                "status": "PROVEN_ACTIVE",
                "definition": "Presence and integrity of fill ledger outputs.",
                "expected_evidence_sources": [
                    "constellation_2/runtime/truth/fill_ledger_v1"
                ]
            },
            {
                "domain": "position_truth_consistency",
                "status": "PROVEN_ACTIVE",
                "definition": "Consistency between positions truth and lifecycle truth.",
                "expected_evidence_sources": [
                    "constellation_2/runtime/truth/positions_v1",
                    "constellation_2/runtime/truth/position_lifecycle_v2"
                ]
            },
            {
                "domain": "orchestrator_health",
                "status": "PROVEN_ACTIVE",
                "definition": "Health of orchestrator-driven daily execution surfaces.",
                "expected_evidence_sources": [
                    "constellation_2/runtime/truth/reports/orchestrator_run_verdict_v2",
                    "constellation_2/runtime/truth/reports/pipeline_manifest_v3"
                ]
            },
            {
                "domain": "report_generation_health",
                "status": "PROVEN_ACTIVE",
                "definition": "Health of daily generated operator, gate, and summary reports.",
                "expected_evidence_sources": [
                    "constellation_2/runtime/truth/reports/operator_daily_gate_v3",
                    "constellation_2/runtime/truth/reports/gate_stack_verdict_v1",
                    "constellation_2/runtime/truth/reports/preopen_preflight_v1"
                ]
            }
        ],
        "severity_levels": [
            "INFO",
            "WARN",
            "ERROR",
            "BLOCKING"
        ],
        "status_vocabulary": [
            "OK",
            "MISSING",
            "FAIL",
            "DEGRADED",
            "BLOCKING",
            "UNKNOWN"
        ],
        "blocking_rules": [
            "A domain is BLOCKING when the authoritative artifact required for daily readiness is missing or failed.",
            "A domain is BLOCKING when canonical truth is replaced by quarantined or non-authoritative evidence.",
            "A domain is BLOCKING when lifecycle completeness cannot be proven across required stages.",
            "A domain is BLOCKING when capital authority readiness, execution evidence completeness, or reconciliation status fails."
        ],
        "evidence_sources": [
            "constellation_2/runtime/truth",
            "constellation_2/runtime/truth_sleeves/PRIMARY/PAPER",
            "constellation_2/runtime/truth/reports",
            "constellation_2/runtime/truth/monitoring_v1",
            "constellation_2/runtime/reports/weekly_ai_review"
        ],
        "output_contract": {
            "minimum_fields": [
                "generated_utc",
                "day_utc",
                "overall_status",
                "domains",
                "blocking_failures",
                "nonblocking_degradations",
                "evidence_manifest"
            ],
            "domain_fields": [
                "domain",
                "status",
                "severity",
                "summary",
                "evidence_sources",
                "blocking",
                "unknowns"
            ],
            "unknown_handling": "Unknown evidence must remain explicit and must not be silently coerced to OK."
        }
    }

    write_json(SYSTEM_SNAPSHOT_ROOT / "constellation_system_snapshot.v1.json", architecture_snapshot)
    write_json(SYSTEM_SNAPSHOT_ROOT / "constellation_ai_architecture_index.v1.json", ai_architecture_index)
    write_json(SYSTEM_SNAPSHOT_ROOT / "constellation_capability_manifest.v1.json", capability_manifest)
    write_text(CONTRACTS_ROOT / "constellation_ai_reasoning_contract.v1.md", ai_reasoning_contract)
    write_text(CONTRACTS_ROOT / "constellation_system_invariants.v1.md", system_invariants)
    write_json(SYSTEM_SNAPSHOT_ROOT / "constellation_event_timeline_spec.v1.json", event_timeline_spec)
    write_json(SYSTEM_SNAPSHOT_ROOT / "constellation_system_diagnostics_spec.v1.json", system_diagnostics_spec)

    print("WROTE:")
    print(SYSTEM_SNAPSHOT_ROOT / "constellation_system_snapshot.v1.json")
    print(SYSTEM_SNAPSHOT_ROOT / "constellation_ai_architecture_index.v1.json")
    print(SYSTEM_SNAPSHOT_ROOT / "constellation_capability_manifest.v1.json")
    print(CONTRACTS_ROOT / "constellation_ai_reasoning_contract.v1.md")
    print(CONTRACTS_ROOT / "constellation_system_invariants.v1.md")
    print(SYSTEM_SNAPSHOT_ROOT / "constellation_event_timeline_spec.v1.json")
    print(SYSTEM_SNAPSHOT_ROOT / "constellation_system_diagnostics_spec.v1.json")
    print("")
    print("NOTE:")
    print("Manifest/index registration intentionally not modified by this script.")
    print("That requires full-file proof of current governance files before any rewrite.")


if __name__ == "__main__":
    main()
