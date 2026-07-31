from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .paper_forward_observation_governance import validate_paper_forward_observation_allowed
from .paper_forward_observation_models import PAPER_FORWARD_OBSERVATION_AUTHORITY, PaperForwardObservationPlan, PaperForwardObservationQueueItem

DEFAULT_SUCCESS_METRICS = [
    "paper_forward_sample_size",
    "paper_forward_win_rate",
    "paper_forward_expectancy",
    "regime_consistency",
    "hypothesis_survival",
]
DEFAULT_FAILURE_METRICS = [
    "paper_forward_failure_rate",
    "invalidated_observation_count",
    "max_adverse_excursion_observed",
    "regime_specific_underperformance",
]


def build_paper_forward_observation_plans(root: str | Path = DEFAULT_STORE_ROOT, *, limit: int = 12, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    trial = _read_json(root_path / "methodology_trial" / "latest.json", {})
    latest_candidate_report = _read_json(root_path / "paper_trade_candidates" / "latest.json", {})
    eligible = sorted(trial.get("eligible_candidates", []), key=lambda row: (-float(row.get("edge_score", 0.0)), str(row.get("paper_trade_candidate_id") or "")))[:limit]
    now = created_at or _now()
    plans = [create_observation_plan(candidate, rank=index + 1, created_at=now) for index, candidate in enumerate(eligible)]
    queue = [create_human_review_queue_item(plan, rank=index + 1, created_at=now) for index, plan in enumerate(plans)]
    return {
        "schema_id": "atlas_v2_research_os_paper_forward_observation_plans_v1",
        "schema_version": "v1",
        "created_at": now,
        "source_methodology_trial_report": str(root_path / "methodology_trial" / "latest.json"),
        "source_paper_trade_candidate_report": str(root_path / "paper_trade_candidates" / "latest.json"),
        "source_latest_candidate_id": (latest_candidate_report.get("candidate") or {}).get("candidate_id", ""),
        "eligible_candidate_count": len(trial.get("eligible_candidates", [])),
        "plans_created": len(plans),
        "candidates_covered": [plan["candidate_id"] for plan in plans],
        "top_12_ranked": [
            {
                "rank": index + 1,
                "candidate_id": plan["candidate_id"],
                "mechanism": plan["mechanism"],
                "edge_score": plan["edge_score"],
                "replay_score": plan["replay_score"],
                "regime": plan["regime_constraints"].get("primary_regime"),
            }
            for index, plan in enumerate(plans)
        ],
        "observation_metrics": {
            "success_metrics": DEFAULT_SUCCESS_METRICS,
            "failure_metrics": DEFAULT_FAILURE_METRICS,
            "minimum_sample_size": 30,
            "default_window": {"mode": "PAPER_FORWARD_OBSERVATION_ONLY", "sessions": 30, "max_calendar_days": 45},
        },
        "plans": plans,
        "human_review_queue": queue,
        "governance_result": validate_plan_set(plans, queue),
        "guardrails": {
            "live_trading_added": False,
            "broker_execution_added": False,
            "capital_authority_added": False,
            "position_sizing_added": False,
            "automatic_paper_trade_placement_added": False,
            "candidate_production_promotion_added": False,
        },
        "recommendation": "Review paper-forward observation plans for human approval before any paper observation begins.",
    }


def create_observation_plan(candidate: dict[str, Any], *, rank: int, created_at: str) -> dict[str, Any]:
    candidate_id = str(candidate.get("paper_trade_candidate_id") or candidate.get("candidate_id") or "")
    mechanism = str(candidate.get("mechanism") or "UNKNOWN")
    hypothesis_id = str(candidate.get("hypothesis_id") or "")
    replay_id = str(candidate.get("replay_id") or "")
    regime = str(candidate.get("regime") or "UNKNOWN")
    conditions = _conditions_for_mechanism(mechanism)
    plan = PaperForwardObservationPlan(
        plan_id=f"pfo-{_short_id(candidate_id)}",
        candidate_id=candidate_id,
        created_at=created_at,
        mechanism=mechanism,
        hypothesis=f"Observe whether {mechanism} hypothesis {hypothesis_id} continues to show replay-supported behavior in paper-forward data.",
        source_hypothesis_id=hypothesis_id,
        source_replay_id=replay_id,
        edge_score=round(float(candidate.get("edge_score") or 0.0), 6),
        replay_score=round(float(candidate.get("replay_score") or 0.0), 6),
        entry_observation_condition=conditions["entry"],
        exit_observation_condition=conditions["exit"],
        invalidating_conditions=conditions["invalidating"],
        observation_window={"mode": "PAPER_FORWARD_OBSERVATION_ONLY", "sessions": 30, "max_calendar_days": 45},
        minimum_sample_size=30,
        success_metrics=list(DEFAULT_SUCCESS_METRICS),
        failure_metrics=list(DEFAULT_FAILURE_METRICS),
        regime_constraints={"primary_regime": regime, "allowed_regimes": [regime], "reject_if_regime_unknown": regime != "UNKNOWN"},
        human_review_required=True,
        metadata={
            "rank": rank,
            "measurement_only": True,
            "research_only": True,
            "authority_boundary": PAPER_FORWARD_OBSERVATION_AUTHORITY,
            "source_trial_index": candidate.get("trial_index"),
            "paper_trade_eligible": bool(candidate.get("paper_trade_eligible")),
        },
    ).to_dict()
    validate_paper_forward_observation_allowed(plan)
    return plan


def create_human_review_queue_item(plan: dict[str, Any], *, rank: int, created_at: str) -> dict[str, Any]:
    item = PaperForwardObservationQueueItem(
        queue_item_id=f"pfo-review-{_short_id(plan['candidate_id'])}",
        plan_id=plan["plan_id"],
        candidate_id=plan["candidate_id"],
        state="READY_FOR_HUMAN_REVIEW",
        priority=round(float(plan.get("edge_score", 0.0)) + float(plan.get("replay_score", 0.0)) / 10.0 - rank / 1000.0, 6),
        created_at=created_at,
        review_status="PENDING",
        human_review_required=True,
        metadata={"measurement_only": True, "research_only": True, "authority_boundary": PAPER_FORWARD_OBSERVATION_AUTHORITY, "rank": rank},
    ).to_dict()
    validate_paper_forward_observation_allowed(item)
    return item


def validate_plan_set(plans: list[dict[str, Any]], queue: list[dict[str, Any]]) -> dict[str, Any]:
    failures: list[str] = []
    for payload in plans + queue:
        try:
            validate_paper_forward_observation_allowed(payload)
        except Exception as exc:
            failures.append(str(exc))
    return {"status": "PASS" if not failures else "FAIL", "failures": failures}


def _conditions_for_mechanism(mechanism: str) -> dict[str, Any]:
    readable = mechanism.replace("_", " ").lower()
    return {
        "entry": f"Record a paper-forward observation only when the {readable} setup appears with the same regime and confirmation context used in replay; do not place an order.",
        "exit": "Close the observation record when the replay-defined follow-through window ends, the setup invalidates, or the paper-forward observation reaches its fixed review horizon.",
        "invalidating": [
            "Required market context is unavailable or stale.",
            "Observed regime differs from the plan regime constraints.",
            "Setup trigger cannot be reconstructed from paper-forward data.",
            "Governance, lineage, or evidence labels are incomplete.",
        ],
    }


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _short_id(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
