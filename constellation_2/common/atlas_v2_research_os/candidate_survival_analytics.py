from __future__ import annotations

import hashlib
import json
from typing import Any

from .paper_trade_outcome_models import CandidateFailureRecord, CandidateSuccessRecord, CandidateSurvivalRecord, PaperTradeOutcome

MIN_SURVIVAL_SAMPLE_SIZE = 5
MAX_ACCEPTABLE_DRAWDOWN = -0.2


def compute_candidate_survival(outcome: dict[str, Any] | PaperTradeOutcome) -> dict[str, Any]:
    row = _outcome_row(outcome)
    sample_size = int(row.get("sample_size", 0))
    wins = int(row.get("wins", 0))
    losses = int(row.get("losses", 0))
    win_rate = round(wins / sample_size, 6) if sample_size > 0 else None
    expectancy = float(row.get("expectancy", 0.0))
    profit_factor = float(row.get("profit_factor", 0.0))
    max_drawdown = float(row.get("max_drawdown", 0.0))
    reasons: list[str] = []
    if sample_size < MIN_SURVIVAL_SAMPLE_SIZE:
        state = "NEEDS_MORE_DATA"
        reasons.append("sample size below minimum paper observation threshold")
    elif row.get("hypothesis_falsified") or losses > wins or expectancy < 0 or profit_factor < 1:
        state = "FALSIFIED"
        reasons.extend(row.get("failure_reasons", []) or ["negative paper outcome"])
    elif row.get("hypothesis_weakened") or max_drawdown <= MAX_ACCEPTABLE_DRAWDOWN:
        state = "WEAKENED"
        reasons.extend(row.get("failure_reasons", []) or ["paper outcome weakened hypothesis"])
    elif row.get("hypothesis_confirmed") and expectancy > 0 and profit_factor >= 1:
        state = "SURVIVED_INITIAL_TEST"
        reasons.extend(row.get("success_reasons", []) or ["paper outcome survived initial test"])
    else:
        state = "PENDING"
        reasons.append("paper outcome is inconclusive")
    return CandidateSurvivalRecord(
        survival_id=f"survival-{_stable_id([row.get('outcome_id'), state])}",
        candidate_id=row["candidate_id"],
        outcome_id=row["outcome_id"],
        survival_state=state,
        sample_size=sample_size,
        win_rate=win_rate,
        expectancy=expectancy,
        profit_factor=profit_factor,
        max_drawdown=max_drawdown,
        reasons=reasons,
        source_artifact_ids=list(row.get("source_artifact_ids", [])),
        metadata={"paper_only": True, "wins": wins, "losses": losses},
    ).to_dict()


def candidate_failure_record(outcome: dict[str, Any] | PaperTradeOutcome, survival: dict[str, Any]) -> dict[str, Any] | None:
    outcome = _outcome_row(outcome)
    if survival["survival_state"] not in {"WEAKENED", "FALSIFIED"}:
        return None
    return CandidateFailureRecord(
        failure_id=f"candidate-failure-{_stable_id([outcome['outcome_id']])}",
        candidate_id=outcome["candidate_id"],
        outcome_id=outcome["outcome_id"],
        failure_reasons=list(outcome.get("failure_reasons", []) or survival.get("reasons", [])),
        survival_state=survival["survival_state"],
        source_artifact_ids=list(outcome.get("source_artifact_ids", [])),
        metadata={"paper_only": True},
    ).to_dict()


def candidate_success_record(outcome: dict[str, Any] | PaperTradeOutcome, survival: dict[str, Any]) -> dict[str, Any] | None:
    outcome = _outcome_row(outcome)
    if survival["survival_state"] != "SURVIVED_INITIAL_TEST":
        return None
    return CandidateSuccessRecord(
        success_id=f"candidate-success-{_stable_id([outcome['outcome_id']])}",
        candidate_id=outcome["candidate_id"],
        outcome_id=outcome["outcome_id"],
        success_reasons=list(outcome.get("success_reasons", []) or survival.get("reasons", [])),
        survival_state=survival["survival_state"],
        source_artifact_ids=list(outcome.get("source_artifact_ids", [])),
        metadata={"paper_only": True},
    ).to_dict()


def survival_counts(survival_records: list[dict[str, Any]]) -> dict[str, int]:
    counts = {state: 0 for state in ["PENDING", "SURVIVED_INITIAL_TEST", "WEAKENED", "FALSIFIED", "NEEDS_MORE_DATA", "RETIRED"]}
    for row in survival_records:
        counts[row["survival_state"]] = counts.get(row["survival_state"], 0) + 1
    return counts


def _stable_id(parts: list[Any]) -> str:
    return hashlib.sha1(json.dumps(parts, sort_keys=True).encode("utf-8")).hexdigest()[:12]


def _outcome_row(outcome: dict[str, Any] | PaperTradeOutcome) -> dict[str, Any]:
    if isinstance(outcome, PaperTradeOutcome):
        return outcome.to_dict()
    return outcome
