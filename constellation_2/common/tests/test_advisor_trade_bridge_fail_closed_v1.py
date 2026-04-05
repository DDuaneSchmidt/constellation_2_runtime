from __future__ import annotations

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
        "annuities": [],
    }


def _decision_plan_with_missing_source() -> dict:
    return {
        "schema_id": "decision_plan",
        "schema_version": "v1",
        "plan_id": "plan1",
        "planning_snapshot_id": "ps1",
        "advisory_packet_id": "ap1",
        "actions": [{
            "action_id": "a1",
            "plan_id": "plan1",
            "recommendation_id": "rec-a1",
            "domain_id": "withdrawal",
            "action_type": "withdraw_from_taxable",
            "action_status": "planned",
            "support_status": "fully_supported",
            "priority": 1,
            "source_account": None,
            "destination_account": "spend",
            "annual_amount": 1200,
            "periodic_amount": 100,
            "periodicity": "monthly",
            "constraints": [],
            "blockers": [],
            "rationale_refs": ["rationale:a1"],
            "evidence_refs": ["evidence:a1"],
            "created_at": "2030-01-01T00:00:00Z",
            "version": "v1",
        }],
        "blocked_actions": [],
        "assumptions_used": [],
        "constraints_used": [],
        "replan_triggers": [],
        "version": "v1",
    }


def test_bridge_proposal_fails_closed_on_missing_source_account(tmp_path: Path) -> None:
    planning = tmp_path / "planning_snapshot.v1.json"
    decision_plan = tmp_path / "decision_plan.v1.json"
    _write_json(planning, _planning_snapshot())
    _write_json(decision_plan, _decision_plan_with_missing_source())
    out_root = Path("/tmp/constellation_2_foundation/advisor_runtime")
    completed = subprocess.run([sys.executable, str(ROOT / "ops" / "tools" / "run_advisor_trade_intent_proposal_v1.py"), "--decision_plan_json", str(decision_plan), "--planning_snapshot_json", str(planning), "--mode", "PAPER", "--day_utc", "2030-01-20", "--produced_utc", "2030-01-20T00:00:00Z", "--output_root", str(out_root)], check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "NO_PROPOSED_TRADE_ACTION" in (completed.stderr + completed.stdout)


def test_bridge_translation_cli_rejects_truth_sleeves_output_root(tmp_path: Path) -> None:
    planning = tmp_path / "planning_snapshot.v1.json"
    decision_plan = tmp_path / "decision_plan.v1.json"
    _write_json(planning, _planning_snapshot())
    _write_json(decision_plan, _decision_plan_with_missing_source())
    completed = subprocess.run([sys.executable, str(ROOT / "ops" / "tools" / "run_advisor_trade_translation_v1.py"), "--decision_plan_json", str(decision_plan), "--planning_snapshot_json", str(planning), "--mode", "PAPER", "--day_utc", "2030-01-20", "--produced_utc", "2030-01-20T00:00:00Z", "--output_root", "/tmp/constellation_2_foundation/truth_sleeves/PAPER"], check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "OUTPUT_ROOT_MUST_BE_ADVISOR_RUNTIME_ROOT" in (completed.stderr + completed.stdout)
