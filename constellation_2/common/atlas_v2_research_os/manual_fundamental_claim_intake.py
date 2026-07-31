from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .manual_fundamental_claim_models import (
    AUTHORITY_BOUNDARY,
    FALSIFIABLE_HYPOTHESIS,
    INITIAL_HYPOTHESES,
    MECHANISM_FAMILY,
    OPTIONAL_CONTROLS,
    RECOMMENDED_PIPELINE,
    REQUIRED_FIELDS,
    manual_pegy_claim,
)
from .manual_fundamental_claim_reports import write_manual_pegy_claim_report
from .research_backlog import BacklogError, ResearchBacklog

BACKLOG_ITEM_ID = "manual_fundamental_claim_pegy_ratio"


def run_manual_pegy_claim_intake(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_manual_pegy_claim_intake(root=root, created_at=created_at)
    write_manual_pegy_claim_report(report, root=root)
    return report


def build_manual_pegy_claim_intake(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    data_audit = audit_fundamental_data_availability(root_path)
    backlog_item = _ensure_backlog_item(root_path, created_at=created)
    return {
        "schema_id": "atlas_v2_research_os_manual_fundamental_claim_intake",
        "schema_version": "1.0",
        "report_type": "MANUAL_FUNDAMENTAL_CLAIM_INTAKE",
        "created_at": created,
        "day": created[:10],
        "manual_claim": manual_pegy_claim(),
        "required_research_framing": FALSIFIABLE_HYPOTHESIS,
        "data_requirements": {
            "required_fields": list(REQUIRED_FIELDS),
            "optional_controls": list(OPTIONAL_CONTROLS),
        },
        "initial_hypothesis_specs": list(INITIAL_HYPOTHESES),
        "data_availability_audit": data_audit,
        "research_backlog_item": backlog_item,
        "recommended_pipeline": list(RECOMMENDED_PIPELINE),
        "routing": {
            "route_to_paper_trade_candidates": False,
            "route_to_candidate_promotion": False,
            "next_route": "DATA_AVAILABILITY_AUDIT_THEN_CROSS_SECTIONAL_TEST_DESIGN",
        },
        "bias_risks": {
            "lookahead_bias": "BLOCKING_RISK_WITHOUT_POINT_IN_TIME_FUNDAMENTALS",
            "survivorship_bias": "BLOCKING_RISK_WITHOUT_SURVIVORSHIP_BIAS_FREE_UNIVERSE_DATA",
            "today_fundamentals_for_past_returns_allowed": False,
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Research-only manual claim intake.",
            "Atlas must not assume PEGY works.",
            "Do not label PEGY as an edge until tested.",
            "Do not route directly to paper trade candidates.",
            "Do not use today's fundamentals to test past returns unless point-in-time data exists.",
            "No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
        ],
        "summary": {
            "claim_captured": True,
            "hypothesis_spec_count": len(INITIAL_HYPOTHESES),
            "backlog_item_created_or_found": bool(backlog_item),
            "data_availability_result": data_audit["overall_classification"],
            "paper_candidate_routing_allowed": False,
            "authority": "RESEARCH_ONLY_NO_LIVE_CAPITAL_BROKER_POSITION_SIZING",
        },
    }


def audit_fundamental_data_availability(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    root_path = Path(root)
    repo_root = _repo_root_from_report_root(root_path)
    checks = [
        ("Does Atlas currently have fundamental data?", [repo_root / "data" / "fundamentals", root_path / "fundamental_data" / "latest.json"]),
        ("Does Atlas have forward EPS growth estimates?", [repo_root / "data" / "forward_eps_growth", root_path / "forward_eps_growth" / "latest.json"]),
        ("Does Atlas have dividend yield history?", [repo_root / "data" / "dividend_yield_history", root_path / "dividend_yield_history" / "latest.json"]),
        ("Does Atlas have point-in-time fundamentals?", [repo_root / "data" / "point_in_time_fundamentals", root_path / "point_in_time_fundamentals" / "latest.json"]),
        ("Does Atlas have survivorship-bias-free universe data?", [repo_root / "data" / "survivorship_bias_free_universe", root_path / "survivorship_bias_free_universe" / "latest.json"]),
    ]
    questions = []
    for question, paths in checks:
        existing = [str(path) for path in paths if path.exists()]
        questions.append(
            {
                "question": question,
                "answer": "YES" if existing else "NO",
                "classification": "AVAILABLE" if existing else "DATA_REQUIRED",
                "evidence_paths": existing,
            }
        )
    return {
        "overall_classification": "AVAILABLE" if all(item["classification"] == "AVAILABLE" for item in questions) else "DATA_REQUIRED",
        "questions": questions,
        "lookahead_bias_risk": "HIGH_WITHOUT_POINT_IN_TIME_FUNDAMENTALS",
        "survivorship_bias_risk": "HIGH_WITHOUT_SURVIVORSHIP_BIAS_FREE_UNIVERSE_DATA",
    }


def _ensure_backlog_item(root: Path, *, created_at: str) -> dict[str, Any]:
    backlog = ResearchBacklog(root)
    existing = backlog.get(BACKLOG_ITEM_ID, required=False)
    if existing:
        return existing
    try:
        return backlog.create_backlog_item(
            backlog_item_id=BACKLOG_ITEM_ID,
            item_type="FUNDAMENTAL_CLAIM_INVESTIGATION",
            title="Investigate manual PEGY ratio claim",
            description=FALSIFIABLE_HYPOTHESIS,
            created_at=created_at,
            created_by="MANUAL_USER_CLAIM",
            source_artifact_ids=[],
            state="READY_IF_DATA_AVAILABLE",
            cost_estimate=0.4,
            expected_learning_value=0.7,
            novelty_score=0.4,
            candidate_impact_estimate=0.0,
            evidence_gap_score=0.8,
            mechanism_tags=[MECHANISM_FAMILY, "PEGY", "FUNDAMENTAL_VALUATION"],
            priority_reasons=["manual_user_claim", "fundamental_data_required", "not_candidate_or_trade_route"],
            metadata={
                "type": "FUNDAMENTAL_CLAIM_INVESTIGATION",
                "source": "MANUAL_USER_CLAIM",
                "claim": "PEGY ratio",
                "state": "READY_IF_DATA_AVAILABLE",
                "route_to_paper_trade_candidates": False,
            },
        )
    except BacklogError:
        return backlog.get(BACKLOG_ITEM_ID)


def _repo_root_from_report_root(root: Path) -> Path:
    if root.parts[-2:] == ("reports", "atlas_v2_research_os"):
        return root.parents[1]
    return Path.cwd()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
