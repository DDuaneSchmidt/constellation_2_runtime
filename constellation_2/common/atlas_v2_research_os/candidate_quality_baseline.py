from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .candidate_quality_governance import validate_candidate_quality_measurement_allowed
from .candidate_quality_models import CandidateQualityBaseline

QUALITY_ROOT = Path("reports/atlas_candidate_quality")


class CandidateQualityBaselineError(ValueError):
    pass


def baseline_path(baseline_id: str, root: str | Path = QUALITY_ROOT) -> Path:
    return Path(root) / "baselines" / f"{baseline_id}.json"


def create_candidate_quality_baseline(*, baseline_id: str, created_at: str, created_by: str, source_artifact_ids: list[str] | None = None, raw_signals: int, generated_candidates: int, rejected_candidates: int, gate_suppressions: int, portfolio_scoring_rejections: int, portfolio_scoring_passes: int, evidence_levels: list[str], hypotheses_tested: int, hypotheses_not_falsified: int, repeated_failures: int, failure_categories: list[str], measurement_window: dict[str, Any], signal_universe_id: str, candidate_factory_version: str, root: str | Path = QUALITY_ROOT, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    path = baseline_path(baseline_id, root)
    if path.exists():
        raise CandidateQualityBaselineError(f"baseline already exists and is immutable: {baseline_id}")
    row = CandidateQualityBaseline(
        baseline_id=baseline_id,
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
        metadata=dict(metadata or {"measurement_only": True, "candidate_promotion_authorized": False}),
    ).to_dict()
    validate_baseline(row)
    validate_candidate_quality_measurement_allowed(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return row


def load_candidate_quality_baseline(baseline_id: str, root: str | Path = QUALITY_ROOT) -> dict[str, Any]:
    path = baseline_path(baseline_id, root)
    if not path.exists():
        raise CandidateQualityBaselineError(f"baseline not found: {baseline_id}")
    return json.loads(path.read_text(encoding="utf-8"))


def validate_baseline(row: dict[str, Any]) -> bool:
    required = ["baseline_id", "created_at", "created_by", "raw_signals", "generated_candidates", "rejected_candidates", "gate_suppressions", "portfolio_scoring_rejections", "portfolio_scoring_passes", "evidence_levels", "hypotheses_tested", "hypotheses_not_falsified", "repeated_failures", "failure_categories", "measurement_window", "signal_universe_id", "candidate_factory_version"]
    missing = [field for field in required if field not in row or row[field] in (None, "")]
    if missing:
        raise CandidateQualityBaselineError(f"baseline missing required fields: {missing}")
    if row.get("metadata", {}).get("candidate_promotion_authorized") is True:
        raise CandidateQualityBaselineError("baseline must not authorize promotion")
    return True
