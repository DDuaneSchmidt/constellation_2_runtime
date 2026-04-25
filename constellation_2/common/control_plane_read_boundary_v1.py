from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]

CLASSIFICATION_CONTROL_PLANE_SEMANTIC_CANDIDATE = "control_plane_semantic_candidate"
CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL = "diagnostic_or_proof_tool"
CLASSIFICATION_ORCHESTRATION_LAYER = "orchestration_layer"
CLASSIFICATION_INVALID_MIXED_SEMANTICS = "invalid_mixed_semantics"

READ_DOMINANCE_ACTIVE_PATHS: tuple[str, ...] = (
    "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
    "constellation_2/common/runtime_contract_v1.py",
    "constellation_2/common/session_authority_v1.py",
    "constellation_2/common/paper_session_fact_plane_v1.py",
    "constellation_2/common/market_calendar_coverage_authority_v1.py",
    "constellation_2/common/session_authority_monitor_v1.py",
    "constellation_2/phaseL/ui_api/common.py",
    "constellation_2/phaseL/ui_api/alerts_read_model.py",
    "constellation_2/phaseL/ui_api/integrity_read_model.py",
    "constellation_2/phaseL/ui_api/operations_read_model.py",
    "constellation_2/phaseL/ui_api/policy_evolution_state_read_model.py",
    "constellation_2/phaseL/ui_api/system_summary_read_model.py",
    "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py",
)

READ_BOUNDARY_CLASSIFICATIONS_V1: dict[str, dict[str, str]] = {
    "constellation_2/common/deployment_state_machine_v1.py": {
        "classification": CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL,
        "reason": "release/runtime inspection module compares active pointer, systemd unit, and active runtime contract for proof and release trust auditing.",
        "action": "allow explicit non-governed direct reads; exclude from control-plane read dominance enforcement.",
    },
    "ops/tools/refresh_market_data_manifest_and_rerun_gates_v1.sh": {
        "classification": CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL,
        "reason": "repair/proof shell script reruns writers and prints summarized artifact state from repo-local runtime proof paths.",
        "action": "allow explicit non-governed direct reads; exclude from control-plane read dominance enforcement.",
    },
    "ops/tools/run_constellation_root_cause_classifier_v1.py": {
        "classification": CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL,
        "reason": "root-cause report reads system snapshot and report directories to explain failure state rather than govern runtime admission.",
        "action": "allow explicit non-governed direct reads; exclude from control-plane read dominance enforcement.",
    },
    "ops/tools/run_regime_snapshot_v2.py": {
        "classification": CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL,
        "reason": "read-only regime snapshot proof tool reads envelope artifacts to emit a diagnostic snapshot artifact.",
        "action": "allow explicit non-governed direct reads; exclude from control-plane read dominance enforcement.",
    },
    "ops/tools/run_regime_snapshot_v3.py": {
        "classification": CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL,
        "reason": "read-only regime snapshot proof tool reads envelope artifacts to emit a diagnostic snapshot artifact.",
        "action": "allow explicit non-governed direct reads; exclude from control-plane read dominance enforcement.",
    },
    "ops/tools/run_sleeve_live_readiness_v1.py": {
        "classification": CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL,
        "reason": "readiness audit/report tool inspects gate and evidence artifacts to produce an operator-facing readiness proof, not governed runtime truth.",
        "action": "allow explicit non-governed direct reads; exclude from control-plane read dominance enforcement.",
    },
    "constellation_2/common/visibility_foundation_v1.py": {
        "classification": CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL,
        "reason": "visibility foundation derives diagnostic funnel, drift, and summary metrics from gate and evidence artifacts for observability reporting.",
        "action": "allow explicit non-governed direct reads; exclude from control-plane read dominance enforcement.",
    },
    "ops/tools/activate_constellation_release_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "release activation workflow mutates symlink/runtime contract and must remain a release orchestration tool rather than a semantic surface.",
        "action": "use gateway for governed release reads when later release-boundary cleanup reaches this layer.",
    },
    "ops/tools/build_constellation_release_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "release builder assembles immutable release artifacts and release manifest; it is not a control-plane semantic read surface.",
        "action": "retain as orchestration; do not classify as semantic gateway surface.",
    },
    "ops/tools/run_gate_stack_verdict_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "gate-stack verdict writer evaluates governed gate inputs and emits a new verdict artifact.",
        "action": "retain as orchestration; route governed input reads through gateway in a later orchestration cleanup pass.",
    },
    "ops/tools/run_operator_daily_gate_v2.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "daily gate writer evaluates governed inputs and writes a new gate artifact.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_operator_daily_gate_v3.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "daily gate writer evaluates governed inputs and writes a new gate artifact.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_paper_session_admission_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "admission orchestration runs subordinate tools, materializes artifacts, and records proof bundles across multiple domains.",
        "action": "retain as orchestration; route governed input reads through gateway where feasible.",
    },
    "ops/tools/run_paper_session_bootstrap_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "bootstrap orchestration coordinates startup materialization, capital seed, and session bootstrap writes across multiple artifact families.",
        "action": "retain as orchestration; route governed input reads through gateway where feasible.",
    },
    "ops/tools/run_pipeline_manifest_v2.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "pipeline manifest writer aggregates many execution and evidence inputs into a new manifest artifact.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_session_readiness_refresh_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "refresh tool launches subordinate writers and summarizes results across multiple readiness families.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_submit_boundary_status_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "submit-boundary writer derives and writes a new governed artifact from execution and runtime-control inputs.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_tomorrow_paper_startup_prep_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "startup prep orchestrates multiple producer tools and readiness proofs instead of exposing a stable semantic read surface.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_pipeline_manifest_v2_compat_from_attempt_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "compat manifest writer reconstructs a pipeline artifact from attempt outputs and belongs to orchestration rather than semantic reads.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_c2_capital_risk_envelope_gate_v2.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "capital-risk envelope gate writer evaluates governed prerequisites and emits a new gate artifact.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_capital_authority_allocation_day_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "capital-allocation writer derives and writes a governed day allocation artifact.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_c2_paper_day_orchestrator_v2.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "paper-day orchestrator coordinates many subordinate writers and runtime actions across the full startup sequence.",
        "action": "retain as orchestration; route governed control-plane reads through gateway in a later pass.",
    },
    "ops/tools/run_portfolio_governance_snapshot_day_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "portfolio governance snapshot writer aggregates governed inputs into a new daily snapshot artifact.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_production_policy_verdict_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "production policy verdict writer evaluates governed upstream artifacts and emits a new verdict artifact.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/run_recurrence_kill_gate_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "recurrence kill gate writer verifies deployment/runtime identity and emits a new gate artifact.",
        "action": "retain as orchestration; route governed control-plane reads through gateway in a later pass.",
    },
    "ops/tools/run_trade_submit_readiness_c2_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "trade-submit readiness writer computes and emits the readiness artifact for execution authority.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "ops/tools/write_active_runtime_contract_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "runtime-contract writer updates active runtime state and belongs to release/runtime orchestration rather than semantic reads.",
        "action": "retain as orchestration; route governed release reads through gateway in a later pass.",
    },
    "constellation_2/common/day_open_trigger_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "day-open trigger writer combines governed session inputs with trigger/attempt workflow state to emit a new trigger artifact.",
        "action": "retain as orchestration; route governed session inputs through gateway in a later pass.",
    },
    "constellation_2/common/capability_state_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "capability-state writer aggregates gate and policy inputs into a new governed capability state artifact.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "constellation_2/common/execution_build_authority_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "execution-build authority derives and writes a governed execution-build artifact from upstream controlled inputs.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "constellation_2/common/gate_authority_foundation_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "gate authority foundation evaluates governed gate inputs and writes authorization/economic/lifecycle gate outputs.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "constellation_2/common/governed_evaluation_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "governed evaluation writer reads policy and gate inputs to emit evaluation artifacts rather than expose a reusable semantic read surface.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "constellation_2/common/next_day_readiness_consistency_gate_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "consistency gate evaluates multiple governed session/execution artifacts and emits a gate result rather than a reusable semantic view.",
        "action": "retain as orchestration; route governed inputs through gateway in a later pass.",
    },
    "constellation_2/common/paper_day_orchestrator_pipeline_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "bounded pipeline wrapper coordinates orchestrator outputs and publication steps rather than acting as a semantic read surface.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "constellation_2/common/session_authority_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "session authority module owns target-day build, admission, and active-session writes; it is not a semantic surface.",
        "action": "retain as orchestration; keep gateway use for governed reads and avoid reclassifying it as a read model.",
    },
    "constellation_2/common/session_promotion_gate_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "session-promotion gate derives and writes promotion decisions from governed session inputs.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "constellation_2/common/subsystem_authority_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "subsystem authority coordinates subsystem status across multiple upstream artifacts and writes derived status artifacts.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "constellation_2/common/trade_submit_readiness_authority_v1.py": {
        "classification": CLASSIFICATION_ORCHESTRATION_LAYER,
        "reason": "trade-submit readiness authority validates and binds readiness artifacts for execution authority use.",
        "action": "retain as orchestration; route governed input reads through gateway in a later pass.",
    },
    "constellation_2/common/operator_day_authority_summary_v1.py": {
        "classification": CLASSIFICATION_INVALID_MIXED_SEMANTICS,
        "reason": "summary writer mixes control-plane session surfaces with execution reconciliation and state-machine messaging in one derived operator artifact.",
        "action": "block until the control-plane subset is separated from non-control-plane execution summary logic.",
    },
    "ops/tools/run_session_authority_v1.py": {
        "classification": CLASSIFICATION_INVALID_MIXED_SEMANTICS,
        "reason": "session authority tool still uses broad arbitrary authority-path loaders and mixed readiness/handshake orchestration outside the semantic gateway.",
        "action": "block until the tool is split into semantic reads plus orchestration-specific non-control-plane logic.",
    },
    "constellation_2/phaseL/ui_api/kernel_operator_shell_v1.py": {
        "classification": CLASSIFICATION_INVALID_MIXED_SEMANTICS,
        "reason": "operator shell mixes control-plane state with execution-kernel latest-file discovery and advisory workspace scanning.",
        "action": "block until control-plane reads are separated from non-control-plane workspace discovery.",
    },
    "constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_v1.py": {
        "classification": CLASSIFICATION_INVALID_MIXED_SEMANTICS,
        "reason": "cockpit status collector composes control-plane gate status with execution, accounting, and platform history directly in the UI collector.",
        "action": "block until control-plane portions are split behind the gateway or replaced by a governed semantic read model.",
    },
}

NON_GOVERNED_DIRECT_READ_BYPASS_PATHS: tuple[str, ...] = tuple(
    sorted(
        relpath
        for relpath, row in READ_BOUNDARY_CLASSIFICATIONS_V1.items()
        if row["classification"] == CLASSIFICATION_DIAGNOSTIC_OR_PROOF_TOOL
    )
)

CONTROL_PLANE_SURFACE_LITERALS: tuple[str, ...] = (
    "active_session_v1",
    "session_authority_status_v1",
    "session_authority_alert_v1",
    "market_calendar_coverage_status_v1",
    "target_day_build_v1",
    "target_day_admission_v1",
    "submit_boundary_status_v1",
    "paper_session_ledger_v1",
    "paper_day_control_plane_v1",
    "trade_submit_readiness_c2_v1",
    "replay_certification_gate_v1",
    "replay_certification_bundle_v1",
    "runtime_truth_integrity_result_v1",
    "alerts_projection_v1",
    "current_system_projection_v1",
    "policy_evolution_state_v1",
    "configuration_state_v1",
    "compiled_active_config_v1",
    "active_runtime_contract.v1.json",
    "release_manifest.v1.json",
)

DIRECT_READ_LINE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bread_json_dict\("),
    re.compile(r"\bread_validated_surface_v1\("),
    re.compile(r"\bread_json_object_v1\("),
    re.compile(r"\.read_text\("),
    re.compile(r"\bjson\.loads\("),
    re.compile(r"\bjson\.load\("),
    re.compile(r"\bjq -r\b"),
    re.compile(r"\[\[\s*!?\s*-f\s+"),
)

ALLOWED_READ_GATEWAY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bread_control_plane_surface_v1\("),
    re.compile(r"\bread_control_plane_collection_v1\("),
)


@dataclass(frozen=True)
class DirectControlPlaneReadHitV1:
    path: str
    line: int
    literal: str
    text: str


def direct_control_plane_read_hits_v1(repo_root: Path | str = REPO_ROOT) -> list[dict[str, Any]]:
    root = Path(repo_root).resolve()
    hits: list[dict[str, Any]] = []
    for relpath in READ_DOMINANCE_ACTIVE_PATHS:
        path = (root / relpath).resolve()
        if not path.exists() or not path.is_file():
            hits.append({"path": relpath, "line": 0, "literal": "ACTIVE_PATH_MISSING", "text": ""})
            continue
        lines = path.read_text(encoding="utf-8").splitlines()
        for lineno, line in enumerate(lines, start=1):
            if any(pattern.search(line) for pattern in ALLOWED_READ_GATEWAY_PATTERNS):
                continue
            if not any(pattern.search(line) for pattern in DIRECT_READ_LINE_PATTERNS):
                continue
            literal = next((item for item in CONTROL_PLANE_SURFACE_LITERALS if item in line), "")
            if not literal:
                continue
            hits.append({"path": relpath, "line": lineno, "literal": literal, "text": line.strip()})
    return hits


def classify_read_boundary_path_v1(relpath: str) -> dict[str, str] | None:
    return READ_BOUNDARY_CLASSIFICATIONS_V1.get(str(relpath).strip())
