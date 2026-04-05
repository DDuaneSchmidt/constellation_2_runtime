from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


def _write_json(path: Path, obj: dict) -> None:
    path.write_bytes(canonical_json_bytes_v1(obj) + b"\n")


def _planning_snapshot() -> dict:
    return {
        "schema_id": "planning_snapshot",
        "schema_version": "v1",
        "planning_snapshot_id": "ps1",
        "advisory_packet_id": "ap1",
        "created_at": "2030-01-01T00:00:00Z",
        "version": "v1",
        "accounts": {"taxable_account_id": "tax", "cash_reserve_account_id": "cash", "spending_account_id": "spend"},
        "liquidity": {"cash_cents": 100},
        "spending": {"minimum_monthly_spending_cents": 200, "monthly_spending_cents": 300},
        "income": {"guaranteed_monthly_income_cents": 100},
        "tax_profile": {"present": True},
        "annuities": [{"annuity_id": "ann1", "phase": "payout"}],
    }


def _action(*, action_id: str, action_type: str, source_account: str | None, annual_amount: int, periodic_amount: int, periodicity: str, support_status: str = "fully_supported", blockers: list[str] | None = None) -> dict:
    return {
        "action_id": action_id,
        "plan_id": "plan1",
        "recommendation_id": f"rec-{action_id}",
        "domain_id": {"raise_cash_reserve": "liquidity", "withdraw_from_taxable": "withdrawal", "hold_annuity": "annuity", "collect_missing_input": "tax"}[action_type],
        "action_type": action_type,
        "action_status": "planned",
        "support_status": support_status,
        "priority": 1,
        "source_account": source_account,
        "destination_account": "dst",
        "annual_amount": annual_amount,
        "periodic_amount": periodic_amount,
        "periodicity": periodicity,
        "constraints": [],
        "blockers": blockers or [],
        "rationale_refs": [f"rationale:{action_id}"],
        "evidence_refs": [f"evidence:{action_id}"],
        "created_at": "2030-01-01T00:00:00Z",
        "version": "v1",
    }


def _decision_plan() -> dict:
    return {
        "schema_id": "decision_plan",
        "schema_version": "v1",
        "plan_id": "plan1",
        "planning_snapshot_id": "ps1",
        "advisory_packet_id": "ap1",
        "actions": [
            _action(action_id="a1", action_type="raise_cash_reserve", source_account="tax", annual_amount=10, periodic_amount=10, periodicity="annual"),
            _action(action_id="a2", action_type="hold_annuity", source_account=None, annual_amount=0, periodic_amount=0, periodicity="none"),
            _action(action_id="a3", action_type="withdraw_from_taxable", source_account="tax", annual_amount=1200, periodic_amount=100, periodicity="monthly"),
        ],
        "blocked_actions": [{
            "action_id": "b1",
            "plan_id": "plan1",
            "recommendation_id": "rec-b1",
            "domain_id": "tax",
            "action_type": "collect_missing_input",
            "support_status": "blocked",
            "blockers": ["TAX_PROFILE_MISSING"],
            "rationale_refs": ["rationale:b1"],
            "evidence_refs": ["evidence:b1"],
            "created_at": "2030-01-01T00:00:00Z",
            "version": "v1",
        }],
        "assumptions_used": [],
        "constraints_used": [],
        "replan_triggers": [],
        "version": "v1",
    }


def test_bridge_translation_and_proposal_are_deterministic(tmp_path: Path) -> None:
    planning = tmp_path / "planning_snapshot.v1.json"
    decision_plan = tmp_path / "decision_plan.v1.json"
    _write_json(planning, _planning_snapshot())
    _write_json(decision_plan, _decision_plan())
    out_root = Path("/tmp/constellation_2_foundation/advisor_runtime")
    translation_cmd = [sys.executable, str(ROOT / "ops" / "tools" / "run_advisor_trade_translation_v1.py"), "--decision_plan_json", str(decision_plan), "--planning_snapshot_json", str(planning), "--mode", "PAPER", "--day_utc", "2030-01-20", "--produced_utc", "2030-01-20T00:00:00Z", "--output_root", str(out_root)]
    proposal_cmd = [sys.executable, str(ROOT / "ops" / "tools" / "run_advisor_trade_intent_proposal_v1.py"), "--decision_plan_json", str(decision_plan), "--planning_snapshot_json", str(planning), "--mode", "PAPER", "--day_utc", "2030-01-20", "--produced_utc", "2030-01-20T00:00:00Z", "--output_root", str(out_root)]
    subprocess.run(translation_cmd, check=True)
    subprocess.run(proposal_cmd, check=True)
    translation_path = out_root / "PAPER" / "advisor_trade_translation_v1" / "2030-01-20" / "advisor_trade_translation.v1.json"
    proposal_path = out_root / "PAPER" / "advisor_trade_intent_proposal_v1" / "2030-01-20" / "advisor_trade_intent_proposal.v1.json"
    first_translation = translation_path.read_bytes()
    first_proposal = proposal_path.read_bytes()
    subprocess.run(translation_cmd, check=True)
    subprocess.run(proposal_cmd, check=True)
    assert first_translation == translation_path.read_bytes()
    assert first_proposal == proposal_path.read_bytes()
    translation_obj = json.loads(first_translation)
    proposal_obj = json.loads(first_proposal)
    assert [item["translation_status"] for item in translation_obj["translations"]] == ["non_trade_action", "non_trade_action", "proposed", "blocked"]
    assert proposal_obj["proposal_class"] == "withdrawal_candidate"
    assert proposal_obj["decision_action_id"] == "a3"
    assert proposal_obj["proposed_amount_cents"] == 100
