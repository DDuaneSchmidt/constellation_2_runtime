from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .schemas import build_envelope, canonical_json_bytes


ARTIFACT_DIRS = {
    "governance_modules": "governance_modules",
    "parameter_groups": "parameter_groups",
    "proposals": "proposals",
    "proposal_diffs": "proposal_diffs",
    "evaluation_artifacts": "evaluation_artifacts",
    "approvals": "approvals",
    "canonical_graphs": "canonical_graphs",
    "activation_snapshots": "activation_snapshots",
    "rollback_records": "rollback_records",
    "lineage_records": "lineage_records",
    "audit_events": "audit_events",
    "verification_datasets": "verification_datasets",
    "verification_contexts": "verification_contexts",
    "decision_diff_artifacts": "decision_diff_artifacts",
    "behavioral_invariant_results": "behavioral_invariant_results",
    "impact_summaries": "impact_summaries",
    "interaction_analysis_results": "interaction_analysis_results",
    "expectation_records": "expectation_records",
    "realized_validation_results": "realized_validation_results",
    "verification_bundles": "verification_bundles",
    "external_reality_snapshots": "external_reality_snapshots",
    "internal_reality_snapshots": "internal_reality_snapshots",
    "position_reconciliation_artifacts": "position_reconciliation_artifacts",
    "taxlot_reconciliation_artifacts": "taxlot_reconciliation_artifacts",
    "execution_reconciliation_artifacts": "execution_reconciliation_artifacts",
    "valuation_reconciliation_artifacts": "valuation_reconciliation_artifacts",
    "pnl_reconciliation_artifacts": "pnl_reconciliation_artifacts",
    "reconciliation_results": "reconciliation_results",
    "discrepancy_classifications": "discrepancy_classifications",
    "correction_recommendations": "correction_recommendations",
    "reconciliation_bundles": "reconciliation_bundles",
    "drift_signals": "drift_signals",
    "regime_signals": "regime_signals",
    "impact_assessments": "impact_assessments",
    "ranked_issues": "ranked_issues",
    "adaptation_recommendations": "adaptation_recommendations",
    "proposal_candidates": "proposal_candidates",
    "operator_summaries": "operator_summaries",
    "adaptation_bundles": "adaptation_bundles",
    "candidate_actions": "candidate_actions",
    "autonomy_classifications": "autonomy_classifications",
    "autonomy_predicate_results": "autonomy_predicate_results",
    "authority_evaluations": "authority_evaluations",
    "prioritized_actions": "prioritized_actions",
    "execution_eligibilities": "execution_eligibilities",
    "operator_action_views": "operator_action_views",
    "autonomous_action_plans": "autonomous_action_plans",
    "autonomous_decision_bundles": "autonomous_decision_bundles",
    "execution_intents": "execution_intents",
    "pre_execution_gate_results": "pre_execution_gate_results",
    "execution_submissions": "execution_submissions",
    "execution_receipts": "execution_receipts",
    "execution_state_transitions": "execution_state_transitions",
    "execution_recovery_records": "execution_recovery_records",
    "deployment_states": "deployment_states",
    "health_check_results": "health_check_results",
    "execution_operator_views": "execution_operator_views",
    "execution_deployment_bundles": "execution_deployment_bundles",
    "state": "state",
}


class ArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        for dirname in ARTIFACT_DIRS.values():
            (self.root / dirname).mkdir(parents=True, exist_ok=True)

    def artifact_path(self, kind: str, artifact_id: str) -> Path:
        return self.root / ARTIFACT_DIRS[kind] / f"{artifact_id}.json"

    def write_immutable(
        self,
        kind: str,
        artifact_id: str,
        record: Any,
        *,
        artifact_type: str,
        created_at: str | None = None,
    ) -> Path:
        path = self.artifact_path(kind, artifact_id)
        payload = build_envelope(artifact_type, record, created_at=created_at)
        new_bytes = canonical_json_bytes(payload)
        if path.exists():
            existing = path.read_bytes()
            if existing != new_bytes:
                raise ValueError(f"IMMUTABLE_CONFLICT:{path}")
            return path
        path.write_bytes(new_bytes)
        return path

    def read(self, kind: str, artifact_id: str) -> dict[str, Any]:
        path = self.artifact_path(kind, artifact_id)
        if not path.exists():
            raise FileNotFoundError(path)
        return json.loads(path.read_text(encoding="utf-8"))

    def exists(self, kind: str, artifact_id: str) -> bool:
        return self.artifact_path(kind, artifact_id).exists()

    def write_pointer(self, pointer_name: str, payload: dict[str, Any]) -> Path:
        path = self.root / ARTIFACT_DIRS["state"] / f"{pointer_name}.json"
        path.write_bytes(canonical_json_bytes(payload))
        return path

    def read_pointer(self, pointer_name: str) -> dict[str, Any] | None:
        path = self.root / ARTIFACT_DIRS["state"] / f"{pointer_name}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def stream_path(self, stream_name: str) -> Path:
        return self.root / ARTIFACT_DIRS["state"] / f"{stream_name}.jsonl"

    def list_ids(self, kind: str) -> list[str]:
        directory = self.root / ARTIFACT_DIRS[kind]
        return sorted(path.stem for path in directory.glob("*.json"))
