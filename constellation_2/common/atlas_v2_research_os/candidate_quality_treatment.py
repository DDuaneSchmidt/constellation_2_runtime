from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .candidate_quality_baseline import QUALITY_ROOT
from .candidate_quality_governance import validate_candidate_quality_measurement_allowed
from .candidate_quality_models import CandidateQualityTreatment


class CandidateQualityTreatmentError(ValueError):
    pass


def treatment_path(treatment_id: str, root: str | Path = QUALITY_ROOT) -> Path:
    return Path(root) / "treatments" / f"{treatment_id}.json"


def create_candidate_quality_treatment(*, treatment_id: str, created_at: str, created_by: str, source_artifact_ids: list[str] | None = None, raw_signals: int, generated_candidates: int, rejected_candidates: int, gate_suppressions: int, portfolio_scoring_rejections: int, portfolio_scoring_passes: int, evidence_levels: list[str], hypotheses_tested: int, hypotheses_not_falsified: int, repeated_failures: int, failure_categories: list[str], measurement_window: dict[str, Any], signal_universe_id: str, candidate_factory_version: str, learning_input_ids: list[str], research_os_memory_ids: list[str] | None = None, baseline: dict[str, Any] | None = None, root: str | Path = QUALITY_ROOT, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    path = treatment_path(treatment_id, root)
    if path.exists():
        raise CandidateQualityTreatmentError(f"treatment already exists and is immutable: {treatment_id}")
    comparable = True
    reasons: list[str] = []
    if baseline is not None:
        for field, value in [("signal_universe_id", signal_universe_id), ("candidate_factory_version", candidate_factory_version), ("measurement_window", measurement_window)]:
            if baseline.get(field) != value:
                comparable = False
                reasons.append(f"{field} differs")
    row = CandidateQualityTreatment(
        treatment_id=treatment_id,
        created_at=created_at,
        created_by=created_by,
        source_artifact_ids=list(source_artifact_ids or []),
        raw_signals=int(raw_signals),
        generated_candidates=int(generated_candidates),
        rejected_candidates=int(rejected_candidates),
        gate_suppressions=int(gate_suppressions),
        portfolio_scoring_rejections=int(portfolio_scoring_rejections),
        portfolio_scoring_passes=int(portfolio_scoring_passes),
        evidence_levels=list(evidence_levels),
        hypotheses_tested=int(hypotheses_tested),
        hypotheses_not_falsified=int(hypotheses_not_falsified),
        repeated_failures=int(repeated_failures),
        failure_categories=list(failure_categories),
        measurement_window=dict(measurement_window),
        signal_universe_id=signal_universe_id,
        candidate_factory_version=candidate_factory_version,
        learning_input_ids=list(learning_input_ids),
        research_os_memory_ids=list(research_os_memory_ids or []),
        comparable_to_baseline=comparable,
        non_comparable_reasons=reasons,
        metadata=dict(metadata or {"measurement_only": True, "candidate_factory_modified": False}),
    ).to_dict()
    validate_treatment(row)
    validate_candidate_quality_measurement_allowed(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return row


def load_candidate_quality_treatment(treatment_id: str, root: str | Path = QUALITY_ROOT) -> dict[str, Any]:
    path = treatment_path(treatment_id, root)
    if not path.exists():
        raise CandidateQualityTreatmentError(f"treatment not found: {treatment_id}")
    return json.loads(path.read_text(encoding="utf-8"))


def validate_treatment(row: dict[str, Any]) -> bool:
    required = ["treatment_id", "created_at", "created_by", "learning_input_ids", "research_os_memory_ids", "raw_signals", "generated_candidates", "rejected_candidates", "gate_suppressions", "portfolio_scoring_rejections", "portfolio_scoring_passes", "evidence_levels", "hypotheses_tested", "hypotheses_not_falsified", "repeated_failures", "failure_categories", "measurement_window", "signal_universe_id", "candidate_factory_version"]
    missing = [field for field in required if field not in row or row[field] is None]
    if missing:
        raise CandidateQualityTreatmentError(f"treatment missing required fields: {missing}")
    if row.get("metadata", {}).get("candidate_factory_modified") is True:
        raise CandidateQualityTreatmentError("treatment must not modify candidate factory logic")
    return True
