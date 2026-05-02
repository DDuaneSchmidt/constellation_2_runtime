from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from ops.tools import run_strategy_edge_governance_v1 as seg


DAY = "2026-05-02"


def _proposal() -> dict:
    return {
        "proposal_id": "proposal-1",
        "hypothesis": "Opening range continuation may persist after strong overnight breadth.",
        "signal_idea": "Measure breadth-adjusted opening range continuation.",
        "expected_market_behavior": "Continuation improves when breadth and volume confirmation align.",
        "data_required": ["minute_bars", "breadth", "volume"],
        "risks": ["regime_decay", "gap_reversal"],
        "created_by": "AI",
        "generated_at": "2026-05-02T15:00:00Z",
        "status": "PROPOSED",
    }


def _passing_evidence() -> dict:
    return {
        "proposal_id": "proposal-1",
        "strategy_id": "strategy-1",
        "validation_dataset": {"name": "paper_research_set", "start": "2025-01-01", "end": "2026-01-01"},
        "out_of_sample_result": {"status": "PASS", "sharpe": 1.2},
        "paper_result": {"status": "PASS", "trades": 50},
        "decay_checks": {"status": "PASS"},
        "regime_sensitivity": {"approved_regimes": ["REGIME_A"]},
        "failure_modes": ["gap_reversal"],
        "confidence_score": 0.82,
        "approval_recommendation": "APPROVE",
        "status": "PASS",
    }


def _strategy(**overrides: object) -> dict:
    row = {
        "strategy_id": "strategy-1",
        "version": "v1",
        "linked_signals": ["proposal-1"],
        "approved_regimes": ["REGIME_A"],
        "kill_criteria": [{"name": "max_drawdown", "triggered": False}],
        "current_lifecycle_state": "INACTIVE",
        "approval_artifact": "/approval/strategy-1.json",
        "status": "PENDING",
    }
    row.update(overrides)
    return row


def _allocation(strategy_id: str = "strategy-1") -> dict:
    return {
        "strategy_id": strategy_id,
        "risk_budget": {"max_capital_at_risk_pct": 0.01},
        "allocation_reason": "Approved strategy with evidence-backed confidence.",
        "correlation_constraints": {"max_pairwise_corr": 0.65},
        "drawdown_constraints": {"max_drawdown_pct": 0.03},
        "confidence_adjustment": 0.8,
        "status": "PENDING",
    }


def _control_plane(root: Path, *, ready: bool) -> None:
    path = root / "reports" / "aegis_control_plane_v1" / DAY / "control_plane.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "day_utc": DAY,
                "final_status": "READY" if ready else "NOT_READY",
                "current_domain": "" if ready else "SESSION_IDENTITY",
                "current_phase": "" if ready else "SESSION_AUTHORITY",
                "canonical_blocker": "" if ready else "NON_TRADING_DAY",
                "submit_allowed": bool(ready),
            }
        ),
        encoding="utf-8",
    )


def test_ai_proposal_cannot_activate_strategy() -> None:
    research = seg.build_research_proposal_ledger_v1(day_utc=DAY, proposals=[_proposal()])
    active = seg.build_active_strategy_set_v1(day_utc=DAY, evidence_ledger={"evidence": []}, strategies=[_strategy()], current_regime="REGIME_A")

    assert research["proposals"][0]["status"] == "PROPOSED"
    assert research["authority"] == "RESEARCH_PROPOSAL_ONLY"
    assert active["active_strategy_set"][0]["current_lifecycle_state"] == "BLOCKED"
    assert "EVIDENCE_NOT_APPROVED_FOR_USE" in active["active_strategy_set"][0]["blockers"]


def test_backtest_evidence_cannot_activate_strategy_directly() -> None:
    research = seg.build_research_proposal_ledger_v1(day_utc=DAY, proposals=[_proposal()])
    evidence = seg.build_strategy_evidence_ledger_v1(day_utc=DAY, research_ledger=research, evidence=[_passing_evidence()])

    assert evidence["evidence"][0]["status"] == "PASS"
    assert evidence["authority"] == "EVIDENCE_VALIDATION_ONLY"
    assert "active_strategy_set" not in evidence


def test_strategy_control_approves_only_with_passing_evidence_and_regime() -> None:
    research = seg.build_research_proposal_ledger_v1(day_utc=DAY, proposals=[_proposal()])
    evidence = seg.build_strategy_evidence_ledger_v1(day_utc=DAY, research_ledger=research, evidence=[_passing_evidence()])
    active = seg.build_active_strategy_set_v1(day_utc=DAY, evidence_ledger=evidence, strategies=[_strategy()], current_regime="REGIME_A")

    assert active["active_strategy_set"][0]["current_lifecycle_state"] == "ACTIVE"
    assert active["authority"] == "STRATEGY_CONTROL_PLANE"


def test_regime_mismatch_blocks_strategy_activation() -> None:
    research = seg.build_research_proposal_ledger_v1(day_utc=DAY, proposals=[_proposal()])
    evidence = seg.build_strategy_evidence_ledger_v1(day_utc=DAY, research_ledger=research, evidence=[_passing_evidence()])
    active = seg.build_active_strategy_set_v1(day_utc=DAY, evidence_ledger=evidence, strategies=[_strategy()], current_regime="REGIME_B")

    assert active["active_strategy_set"][0]["current_lifecycle_state"] == "BLOCKED"
    assert "REGIME_MISMATCH" in active["active_strategy_set"][0]["blockers"]


def test_strategy_kill_criteria_disables_strategy() -> None:
    research = seg.build_research_proposal_ledger_v1(day_utc=DAY, proposals=[_proposal()])
    evidence = seg.build_strategy_evidence_ledger_v1(day_utc=DAY, research_ledger=research, evidence=[_passing_evidence()])
    active = seg.build_active_strategy_set_v1(
        day_utc=DAY,
        evidence_ledger=evidence,
        strategies=[_strategy(kill_criteria=[{"name": "drawdown", "triggered": True}])],
        current_regime="REGIME_A",
    )

    assert active["active_strategy_set"][0]["current_lifecycle_state"] == "DISABLED"
    assert "STRATEGY_KILL_CRITERIA_TRIGGERED" in active["active_strategy_set"][0]["blockers"]


def test_allocation_rejects_unapproved_strategy() -> None:
    active = seg.build_active_strategy_set_v1(day_utc=DAY, evidence_ledger={"evidence": []}, strategies=[_strategy()], current_regime="REGIME_A")
    allocation = seg.build_strategy_allocation_plan_v1(day_utc=DAY, active_strategy_set=active, allocations=[_allocation()])

    assert allocation["allocations"][0]["status"] == "BLOCKED"
    assert "STRATEGY_NOT_ACTIVE_APPROVED" in allocation["allocations"][0]["blockers"]


def test_execution_rejects_strategy_without_approved_allocation(tmp_path: Path) -> None:
    _control_plane(tmp_path, ready=True)
    active = {"active_strategy_set": [{**_strategy(), "current_lifecycle_state": "ACTIVE", "status": "APPROVED_ACTIVE"}]}
    allocation = seg.build_strategy_allocation_plan_v1(day_utc=DAY, active_strategy_set=active, allocations=[_allocation("other")])

    eligibility = seg.build_strategy_execution_eligibility_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        strategy_id="strategy-1",
        active_strategy_set=active,
        allocation_plan=allocation,
    )

    assert eligibility["status"] == "BLOCKED"
    assert any(row["code"] == "STRATEGY_ALLOCATION_NOT_APPROVED" for row in eligibility["blockers"])


def test_aegis_not_ready_blocks_even_approved_strategy_and_allocation(tmp_path: Path) -> None:
    _control_plane(tmp_path, ready=False)
    active = {"active_strategy_set": [{**_strategy(), "current_lifecycle_state": "ACTIVE", "status": "APPROVED_ACTIVE"}]}
    allocation = {"allocations": [{**_allocation(), "status": "APPROVED"}]}

    eligibility = seg.build_strategy_execution_eligibility_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        strategy_id="strategy-1",
        active_strategy_set=active,
        allocation_plan=allocation,
    )

    assert eligibility["status"] == "BLOCKED"
    assert eligibility["submit_allowed"] is False
    assert any(row["code"] == "AEGIS_CONTROL_PLANE_NOT_READY" for row in eligibility["blockers"])


def test_every_decision_artifact_is_schema_valid_and_ledger_backed(tmp_path: Path) -> None:
    _control_plane(tmp_path, ready=False)
    result = seg.run_all_v1(DAY, tmp_path)

    for schema_id, key in [
        ("research_proposal_ledger", "research_proposal_ledger_path"),
        ("strategy_evidence_ledger", "strategy_evidence_ledger_path"),
        ("active_strategy_set", "active_strategy_set_path"),
        ("strategy_allocation_plan", "strategy_allocation_plan_path"),
        ("strategy_execution_eligibility", "strategy_execution_eligibility_path"),
    ]:
        schema = json.loads(seg.SCHEMAS[schema_id].read_text(encoding="utf-8"))
        payload = json.loads(Path(result[key]).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)
        assert "producer_contract_v1" in payload

