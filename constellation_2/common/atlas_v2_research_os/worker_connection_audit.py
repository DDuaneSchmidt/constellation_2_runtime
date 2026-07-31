from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .worker_adapters import create_default_worker_adapters
from .worker_connection_rules import is_output_artifact_allowed, is_side_effect_allowed
from .worker_registry import validate_worker_contract

AUDIT_REPORT_DIRNAME = "worker_connection_audit"


@dataclass(frozen=True)
class WorkerConnectionAuditRow:
    component_name: str
    purpose: str
    inputs: list[str]
    outputs: list[str]
    artifact_types: list[str]
    side_effects: list[str]
    current_test_coverage: list[str]
    safe_to_connect: bool
    unsafe_to_connect: bool
    missing_information: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def inventory_existing_atlas_v2_components() -> list[dict[str, Any]]:
    rows = [
        WorkerConnectionAuditRow(
            component_name="AtlasV2ClaimIdeaGenerator",
            purpose="Generate bounded research claims from existing safe source records.",
            inputs=["Question", "ExperienceEvent", "historical Atlas V2 records"],
            outputs=["GeneratedResearchClaim"],
            artifact_types=["GeneratedResearchClaim"],
            side_effects=["generate_research_artifact", "artifact_store_write", "write_worker_run_record"],
            current_test_coverage=["test_atlas_v2_research_os_worker_connected_adapters.py", "ops/atlas claim generator tests"],
            safe_to_connect=True,
            unsafe_to_connect=False,
        ),
        WorkerConnectionAuditRow(
            component_name="AtlasV2ClaimToHypothesisGenerator",
            purpose="Convert generated claims into testable research hypotheses.",
            inputs=["GeneratedResearchClaim"],
            outputs=["ResearchHypothesis"],
            artifact_types=["ResearchHypothesis"],
            side_effects=["generate_research_artifact", "artifact_store_write", "write_worker_run_record"],
            current_test_coverage=["test_atlas_v2_research_os_worker_connected_adapters.py", "ops/atlas hypothesis generator tests"],
            safe_to_connect=True,
            unsafe_to_connect=False,
        ),
        WorkerConnectionAuditRow(
            component_name="AtlasV2CheapExperimentGenerator",
            purpose="Generate cheap experiment specifications without execution.",
            inputs=["ResearchHypothesis"],
            outputs=["CheapExperimentSpec"],
            artifact_types=["CheapExperimentSpec"],
            side_effects=["generate_research_artifact", "artifact_store_write", "write_worker_run_record"],
            current_test_coverage=["test_atlas_v2_research_os_worker_connected_adapters.py", "ops/atlas cheap experiment generator tests"],
            safe_to_connect=True,
            unsafe_to_connect=False,
        ),
        WorkerConnectionAuditRow(
            component_name="AtlasV2LearningValueEstimator",
            purpose="Estimate research learning value from bounded experience or result evidence.",
            inputs=["ExperimentResult", "ExperienceEvent"],
            outputs=["LearningEstimate"],
            artifact_types=["LearningEstimate"],
            side_effects=["generate_research_artifact", "artifact_store_write", "write_worker_run_record"],
            current_test_coverage=["test_atlas_v2_research_os_worker_connected_adapters.py", "ops/atlas learning estimator tests"],
            safe_to_connect=True,
            unsafe_to_connect=False,
        ),
        WorkerConnectionAuditRow(
            component_name="AtlasV2LearningEstimateEvaluation",
            purpose="Evaluate learning estimates against observed learning values without maturity escalation.",
            inputs=["LearningEstimate", "ExperimentResult"],
            outputs=["LearningEstimateEvaluation"],
            artifact_types=["LearningEstimateEvaluation"],
            side_effects=["generate_research_artifact", "artifact_store_write", "write_worker_run_record"],
            current_test_coverage=["test_atlas_v2_research_os_worker_connected_adapters.py", "ops/atlas learning estimator tests"],
            safe_to_connect=True,
            unsafe_to_connect=False,
        ),
        WorkerConnectionAuditRow(
            component_name="AtlasV2CheapExperimentExecutor",
            purpose="Execute cheap experiment specs and produce observed outcomes.",
            inputs=["CheapExperimentSpec"],
            outputs=["ExperimentResult", "ExperienceEvent"],
            artifact_types=["ExperimentResult", "ExperienceEvent"],
            side_effects=["experiment_execution"],
            current_test_coverage=["worker interface placeholder tests"],
            safe_to_connect=False,
            unsafe_to_connect=True,
            missing_information=["Execution boundary review is intentionally out of scope for this build."],
        ),
        WorkerConnectionAuditRow(
            component_name="AtlasV2AttentionAndMemoryCurators",
            purpose="Attention and memory maintenance workers remain interface placeholders.",
            inputs=["LearningEstimateEvaluation", "BacklogItem", "research artifacts"],
            outputs=["AttentionSignal", "LineageRecord", "BacklogItem"],
            artifact_types=["AttentionSignal", "LineageRecord", "BacklogItem"],
            side_effects=["update_memory", "update_backlog"],
            current_test_coverage=["worker interface placeholder tests"],
            safe_to_connect=False,
            unsafe_to_connect=False,
            missing_information=["Not targeted for connection in Build 008."],
        ),
    ]
    return [row.to_dict() for row in rows]


def build_worker_connection_audit(*, day: str | None = None) -> dict[str, Any]:
    day_value = day or date.today().isoformat()
    components = inventory_existing_atlas_v2_components()
    adapters = create_default_worker_adapters()
    contract_failures: list[str] = []
    for adapter in adapters:
        ok, failures = validate_worker_contract(adapter)
        if not ok:
            contract_failures.extend(f"{adapter.worker_id}: {failure}" for failure in failures)
    for component in components:
        outputs_allowed = all(is_output_artifact_allowed(item) for item in component["artifact_types"])
        side_effects_allowed = all(is_side_effect_allowed(item) for item in component["side_effects"])
        component["connection_rule_result"] = "PASS" if outputs_allowed and side_effects_allowed else "FAIL"
        if not outputs_allowed:
            component.setdefault("missing_information", []).append("One or more output artifact types are not allowed for connected workers.")
        if not side_effects_allowed:
            component.setdefault("missing_information", []).append("One or more side effects are not allowed for connected workers.")
    safe = [row["component_name"] for row in components if row["safe_to_connect"] and row["connection_rule_result"] == "PASS"]
    unsafe = [row["component_name"] for row in components if row["unsafe_to_connect"] or row["connection_rule_result"] == "FAIL"]
    return {
        "schema_id": "atlas_v2_research_os_worker_connection_audit_v1",
        "schema_version": "v1",
        "day": day_value,
        "component_count": len(components),
        "safe_to_connect": safe,
        "unsafe_to_connect": unsafe,
        "missing_information_count": sum(1 for row in components if row.get("missing_information")),
        "contract_audit_result": "PASS" if not contract_failures else "FAIL",
        "contract_failures": contract_failures,
        "components": components,
    }


def write_worker_connection_audit(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    report = build_worker_connection_audit(day=day)
    out_dir = Path(root) / AUDIT_REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "worker_connection_audit.v1.json"
    md_path = out_dir / "worker_connection_audit_summary.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_worker_connection_audit_summary(report), encoding="utf-8")
    return {"json": json_path, "summary": md_path}


def render_worker_connection_audit_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Atlas V2 Research OS Worker Connection Audit",
        "",
        f"Day: {report['day']}",
        f"Components inventoried: {report['component_count']}",
        f"Contract audit: {report['contract_audit_result']}",
        f"Safe to connect: {json.dumps(report['safe_to_connect'], sort_keys=True)}",
        f"Unsafe to connect: {json.dumps(report['unsafe_to_connect'], sort_keys=True)}",
        f"Missing information count: {report['missing_information_count']}",
        "",
    ]
    for component in report["components"]:
        lines.append(f"- {component['component_name']}: {component['connection_rule_result']}")
    lines.append("")
    return "\n".join(lines)
