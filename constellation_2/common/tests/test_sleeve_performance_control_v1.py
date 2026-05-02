from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from ops.tools import run_sleeve_performance_control_v1 as spc
from ops.tools import run_strategy_edge_governance_v1 as seg


DAY = "2026-05-02"
SLEEVE = "C2_VOL_INCOME_DEFINED_RISK_V1"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _production_root(tmp_path: Path) -> Path:
    root = tmp_path / "production_truth"
    root.mkdir()
    return root


def _seed_performance_inputs(
    root: Path,
    *,
    classification: str = "FILLED",
    completeness: str = "COMPLETE",
    outcome_status: str = "CLOSED",
    edge_health: str = "STABLE",
    scorecard_status: str = "VALID",
    nav_total: int = 100000,
) -> None:
    _write(
        _report(root, "sleeve_intent_trade_attribution_v1", "sleeve_intent_trade_attribution.v1.json"),
        {
            "day_utc": DAY,
            "completeness_status": completeness,
            "sleeves": [{"sleeve_id": SLEEVE, "opportunity_count": 1, "classification_counts": {classification: 1}}],
            "opportunities": [
                {
                    "sleeve_id": SLEEVE,
                    "opportunity_id": "intent-1",
                    "final_classification": classification,
                    "completeness_status": completeness,
                    "submit_decision": "ATTEMPTED",
                }
            ],
        },
    )
    _write(
        _report(root, "trade_outcome_v1", "trade_outcome.v1.json"),
        {"day_utc": DAY, "status": "PASS", "outcome_status": outcome_status, "intent_id": "intent-1", "sleeve_id": SLEEVE, "realized_pnl": 5.0, "return_pct": 0.01},
    )
    _write(
        _report(root, "edge_attribution_v1", "edge_attribution.v1.json"),
        {"day_utc": DAY, "status": "PASS", "sleeves": [{"sleeve_id": SLEEVE, "edge_health": edge_health, "trades": 20}]},
    )
    _write(
        _report(root, "weekly_scorecard_view_v1", "weekly_scorecard_view.v1.json"),
        {"day_utc": DAY, "sleeve_rows": [{"sleeve_id": SLEEVE, "performance_status": scorecard_status}]},
    )
    _write(
        _report(root, "selection_quality_v1", "selection_quality.v1.json"),
        {"day_utc": DAY, "status": "PASS", "selected_sleeve_id": SLEEVE, "confidence_level": "HIGH"},
    )
    _write(
        _report(root, "decision_consistency_v1", "decision_consistency.v1.json"),
        {"day_utc": DAY, "status": "PASS", "ranking_stability": "STABLE"},
    )
    _write(
        _report(root, "regime_confidence_v1", "regime_confidence.v1.json"),
        {"day_utc": DAY, "status": "PASS", "confidence_level": "HIGH"},
    )
    _write(
        _report(root, "missed_opportunity_v1", "missed_opportunity.v1.json"),
        {"day_utc": DAY, "status": "PASS", "alternatives": []},
    )
    _write(
        _report(root, "risk_sizing_authority_v1", "risk_sizing_authority.v1.json"),
        {"day_utc": DAY, "status": "PASS", "risk_envelope": {"nav_total": nav_total}},
    )


def _control(root: Path) -> dict:
    payload = spc.build_sleeve_performance_control_v1(day_utc=DAY, truth_root=root)
    payload["artifact_path"] = str(spc.sleeve_performance_control_path(truth_root=root, day_utc=DAY))
    schema = json.loads(spc.SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate({**payload, "producer_contract_v1": {}})
    return payload


def _active() -> dict:
    return {
        "active_strategy_set": [
            {
                "strategy_id": "strategy-1",
                "sleeve_id": SLEEVE,
                "current_lifecycle_state": "ACTIVE",
                "status": "APPROVED_ACTIVE",
            }
        ]
    }


def _allocation() -> dict:
    return {
        "strategy_id": "strategy-1",
        "sleeve_id": SLEEVE,
        "risk_budget": {"max_capital_at_risk_pct": 0.01},
        "allocation_reason": "Evidence-backed sleeve.",
        "correlation_constraints": {},
        "drawdown_constraints": {},
        "confidence_adjustment": 1.0,
        "status": "PENDING",
    }


def test_no_completed_trades_cannot_be_allocation_eligible(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    _seed_performance_inputs(root, classification="INTENT_CREATED_NOT_SUBMITTED")

    row = next(item for item in _control(root)["sleeve_results"] if item["sleeve_id"] == SLEEVE)

    assert row["allocation_eligible"] is False
    assert "NO_COMPLETED_TRADES" in row["blockers"]


def test_missing_sleeve_intent_trade_attribution_is_explicit_blocker(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    _seed_performance_inputs(root)
    _report(root, "sleeve_intent_trade_attribution_v1", "sleeve_intent_trade_attribution.v1.json").unlink()

    row = next(item for item in _control(root)["sleeve_results"] if item["sleeve_id"] == SLEEVE)

    assert row["allocation_eligible"] is False
    assert "SLEEVE_INTENT_TRADE_ATTRIBUTION_MISSING" in row["blockers"]


def test_unknown_trade_outcome_blocks_sleeve_proof(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    _seed_performance_inputs(root, outcome_status="UNKNOWN")

    row = next(item for item in _control(root)["sleeve_results"] if item["sleeve_id"] == SLEEVE)

    assert row["allocation_eligible"] is False
    assert "TRADE_OUTCOME_UNKNOWN" in row["blockers"]


def test_unproven_edge_blocks_allocation_influence(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    _seed_performance_inputs(root, edge_health="UNPROVEN")

    row = next(item for item in _control(root)["sleeve_results"] if item["sleeve_id"] == SLEEVE)

    assert row["allocation_eligible"] is False
    assert "EDGE_UNPROVEN" in row["blockers"]


def test_not_enough_scorecard_blocks_allocation_influence(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    _seed_performance_inputs(root, scorecard_status="NOT_ENOUGH_EVIDENCE")

    row = next(item for item in _control(root)["sleeve_results"] if item["sleeve_id"] == SLEEVE)

    assert row["allocation_eligible"] is False
    assert "SCORECARD_NOT_ENOUGH_EVIDENCE" in row["blockers"]


def test_legacy_and_candidate_roots_cannot_influence_production_allocation(tmp_path: Path) -> None:
    for root_name in ("truth", "candidate_truth"):
        root = tmp_path / root_name
        root.mkdir()
        _seed_performance_inputs(root)
        row = next(item for item in _control(root)["sleeve_results"] if item["sleeve_id"] == SLEEVE)
        assert row["allocation_eligible"] is False
        assert "NON_PRODUCTION_TRUTH_ROOT" in row["blockers"]


def test_allocation_consumes_only_sleeve_performance_control(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    _seed_performance_inputs(root)
    control = _control(root)

    without_control = seg.build_strategy_allocation_plan_v1(day_utc=DAY, active_strategy_set=_active(), allocations=[_allocation()])
    with_control = seg.build_strategy_allocation_plan_v1(day_utc=DAY, active_strategy_set=_active(), allocations=[_allocation()], sleeve_performance_control=control)

    assert without_control["allocations"][0]["status"] == "BLOCKED"
    assert "SLEEVE_PERFORMANCE_CONTROL_MISSING" in without_control["allocations"][0]["blockers"]
    assert with_control["allocations"][0]["status"] == "APPROVED"


def test_nav_zero_is_surfaced_and_blocks_allocation(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    _seed_performance_inputs(root, nav_total=0)

    payload = _control(root)
    row = next(item for item in payload["sleeve_results"] if item["sleeve_id"] == SLEEVE)

    assert payload["risk_sizing_blocks_allocation"] is True
    assert payload["nav_value"] == 0
    assert "NAV_ZERO_OR_UNAVAILABLE" in row["blockers"]
    assert "RISK_SIZING_BLOCKS_ALLOCATION" in row["blockers"]


def test_existing_metric_outputs_are_consumed_not_recomputed(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    _seed_performance_inputs(root)

    row = next(item for item in _control(root)["sleeve_results"] if item["sleeve_id"] == SLEEVE)
    artifact_types = {ref["artifact_type"] for ref in row["evidence_refs"]}

    assert "edge_attribution_v1" in artifact_types
    assert "selection_quality_v1" in artifact_types
    assert "regime_confidence_v1" in artifact_types
    assert "sharpe" not in row


def test_aegis_not_ready_blocks_execution_even_if_sleeve_performance_is_proven(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    _seed_performance_inputs(root)
    control = _control(root)
    allocation = seg.build_strategy_allocation_plan_v1(day_utc=DAY, active_strategy_set=_active(), allocations=[_allocation()], sleeve_performance_control=control)
    _write(
        _report(root, "aegis_control_plane_v1", "control_plane.v1.json"),
        {"day_utc": DAY, "final_status": "NOT_READY", "canonical_blocker": "NON_TRADING_DAY", "submit_allowed": False},
    )

    eligibility = seg.build_strategy_execution_eligibility_v1(
        day_utc=DAY,
        truth_root=root,
        strategy_id="strategy-1",
        active_strategy_set=_active(),
        allocation_plan=allocation,
    )

    assert allocation["allocations"][0]["status"] == "APPROVED"
    assert eligibility["status"] == "BLOCKED"
    assert any(row["code"] == "AEGIS_CONTROL_PLANE_NOT_READY" for row in eligibility["blockers"])
