from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .paper_trading_queue_governance import validate_paper_trading_queue_allowed
from .paper_trading_queue_models import MEASUREMENT_ONLY_AUTHORITY_BOUNDARY, PaperTradeTestPlan, validate_paper_trade_test_plan_model

DEFAULT_SUCCESS_METRICS = ["win_rate", "expectancy", "sample_size", "hypothesis_survival"]
DEFAULT_FAILURE_METRICS = ["max_drawdown", "sample_size", "regime_specific_performance"]


def create_paper_trade_test_plan(
    *,
    test_plan_id: str,
    candidate_id: str,
    mechanism_tags: list[str],
    regime_context: str,
    entry_condition_description: str,
    exit_condition_description: str,
    invalidating_conditions: list[str],
    paper_observation_window: dict[str, Any],
    minimum_sample_size: int,
    source_artifact_ids: list[str],
    success_metrics: list[str] | None = None,
    failure_metrics: list[str] | None = None,
    risk_notes: list[str] | None = None,
    expected_failure_modes: list[str] | None = None,
    human_review_required: bool = True,
    created_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = PaperTradeTestPlan(
        test_plan_id=test_plan_id,
        candidate_id=candidate_id,
        created_at=created_at or now_utc(),
        mechanism_tags=list(mechanism_tags),
        regime_context=regime_context,
        entry_condition_description=entry_condition_description,
        exit_condition_description=exit_condition_description,
        invalidating_conditions=list(invalidating_conditions),
        paper_observation_window=dict(paper_observation_window),
        minimum_sample_size=int(minimum_sample_size),
        success_metrics=list(success_metrics or DEFAULT_SUCCESS_METRICS),
        failure_metrics=list(failure_metrics or DEFAULT_FAILURE_METRICS),
        risk_notes=list(risk_notes or []),
        expected_failure_modes=list(expected_failure_modes or []),
        source_artifact_ids=list(source_artifact_ids),
        human_review_required=bool(human_review_required),
        metadata={
            "measurement_only": True,
            "authority_boundary": MEASUREMENT_ONLY_AUTHORITY_BOUNDARY,
            **dict(metadata or {}),
        },
    ).to_dict()
    validate_test_plan(row)
    return row


def validate_test_plan(test_plan: dict[str, Any]) -> bool:
    validate_paper_trade_test_plan_model(test_plan)
    validate_paper_trading_queue_allowed(test_plan)
    if not test_plan.get("entry_condition_description"):
        raise ValueError("entry_condition_description is required")
    if not test_plan.get("exit_condition_description"):
        raise ValueError("exit_condition_description is required")
    if not test_plan.get("invalidating_conditions"):
        raise ValueError("invalidating_conditions are required")
    return True


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
