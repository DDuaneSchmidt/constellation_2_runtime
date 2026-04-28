from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_authority_graph_v1 import build_aegis_authority_graph_v1
from constellation_2.common.aegis_daily_operator_summary_v1 import build_aegis_daily_operator_summary_v1
from constellation_2.common.aegis_day_evidence_ledger_v1 import finalize_aegis_day_evidence_ledger_v1, new_aegis_day_evidence_ledger_v1
from constellation_2.common.aegis_operating_contract_v1 import build_aegis_operating_contract_v1


DAY = "2026-04-27"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_operator_summary_answers_trade_status_and_evidence_paths(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    submission_id = "a" * 64
    _write_json(
        execution_root / "execution_evidence_v1" / "submissions" / DAY / submission_id / "broker_submission_record.v2.json",
        {"submission_id": submission_id, "broker_ids": {"order_id": None, "perm_id": None}},
    )
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"dry_run_policy": "YES", "broker_transmit_enabled": False, "broker_order_transmitted": False},
    )
    contract = build_aegis_operating_contract_v1(day_utc=DAY)
    graph = build_aegis_authority_graph_v1(day_utc=DAY, truth_root=truth_root, execution_root=execution_root, repo_root=REPO_ROOT)
    ledger = finalize_aegis_day_evidence_ledger_v1(
        new_aegis_day_evidence_ledger_v1(day_utc=DAY, mode="DRY_RUN", run_style="MANUAL"),
        final_daily_outcome="SUCCESS_DRY_RUN",
    )
    summary = build_aegis_daily_operator_summary_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        operating_contract=contract,
        authority_graph=graph,
        evidence_ledger=ledger,
    )
    assert summary["did_aegis_trade_today"] is True
    assert summary["dry_run"] is True
    assert summary["transmitted"] is False
    assert summary["broker_transmit_enabled"] is False
    assert "submit_boundary" in summary["evidence_paths"]


def test_operator_summary_no_silent_day_enforced_with_blocker(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "exec"
    contract = build_aegis_operating_contract_v1(day_utc=DAY)
    graph = build_aegis_authority_graph_v1(day_utc=DAY, truth_root=truth_root, execution_root=execution_root, repo_root=REPO_ROOT)
    ledger = finalize_aegis_day_evidence_ledger_v1(
        new_aegis_day_evidence_ledger_v1(day_utc=DAY, mode="DRY_RUN", run_style="MANUAL"),
        final_daily_outcome="BLOCKED_WITH_REASON",
        blockers=[{"code": "PREFLIGHT_BLOCKED", "owner": "paper_trading_day_authority", "producer_command": "npm run aegis:paper:preflight"}],
    )
    summary = build_aegis_daily_operator_summary_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        operating_contract=contract,
        authority_graph=graph,
        evidence_ledger=ledger,
    )
    assert summary["no_silent_day_outcome"] == "BLOCKED_WITH_REASON"
    assert summary["why_not"] == "PREFLIGHT_BLOCKED"
    assert summary["first_blocker_owner"] == "paper_trading_day_authority"
