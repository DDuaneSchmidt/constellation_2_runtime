from __future__ import annotations

from pathlib import Path
from typing import Any

from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .lineage import validate_lineage_integrity
from .orchestrator_governance import run_safety_gates, safety_gates_passed
from .orchestrator_models import LineageValidationResult
from .priority_engine import PriorityEngine
from .research_backlog import ResearchBacklog


def select_backlog_items(root: str | Path = DEFAULT_STORE_ROOT, *, limit: int = 1, seed: int | None = None) -> list[dict[str, Any]]:
    return PriorityEngine(ResearchBacklog(root), ArtifactStore(root)).select_next_items(limit=limit, exploration_rate=0.0, seed=seed)


def validate_run_eligibility(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    selected_backlog_items: list[dict[str, Any]] | None = None,
    runtime_truth: dict[str, Any] | None = None,
    verified_graph: dict[str, Any] | None = None,
    require_backlog_item: bool = True,
) -> dict[str, Any]:
    selected = list(selected_backlog_items or [])
    lineage_ok, lineage_failures = validate_lineage_integrity(ArtifactStore(root))
    lineage_result = LineageValidationResult(
        result="PASS" if lineage_ok else "FAIL",
        checked_artifacts=[item["artifact_id"] for item in ArtifactStore(root).list_artifacts()],
        hash_validation_details=lineage_failures,
    ).to_dict()
    gate_results = run_safety_gates(
        root,
        selected_backlog_items=selected,
        runtime_truth=runtime_truth,
        verified_graph=verified_graph,
    )
    skip_reason = None
    eligible = True
    if require_backlog_item and not selected:
        eligible = False
        skip_reason = "NO_ELIGIBLE_BACKLOG_ITEM"
    if not safety_gates_passed(gate_results):
        eligible = False
        if _gate_failed(gate_results, "authority_boundary"):
            graph_status = str((verified_graph or {}).get("graph_status") or "")
            skip_reason = "VERIFIED_GRAPH_BLOCKED" if graph_status == "BLOCKED" else "RUNTIME_TRUTH_BLOCKED"
        elif _gate_failed(gate_results, "forbidden_artifact_audit"):
            skip_reason = "FORBIDDEN_ARTIFACT_RISK"
        elif _gate_failed(gate_results, "lineage_integrity"):
            skip_reason = "LINEAGE_INTEGRITY_FAILED"
        else:
            skip_reason = "GOVERNANCE_NOT_READY"
    return {
        "eligible": eligible,
        "skip_reason": skip_reason,
        "selected_backlog_items": selected,
        "safety_gate_results": gate_results,
        "lineage_validation_result": lineage_result,
    }


def _gate_failed(results: list[dict[str, Any]], gate_id: str) -> bool:
    return any(row.get("gate_id") == gate_id and row.get("result") != "PASS" for row in results)
