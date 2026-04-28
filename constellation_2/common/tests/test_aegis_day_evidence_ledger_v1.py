from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_day_evidence_ledger_v1 import (
    finalize_aegis_day_evidence_ledger_v1,
    new_aegis_day_evidence_ledger_v1,
    record_command_v1,
    record_cycle_v1,
    record_phase_v1,
)


def test_ledger_records_command_start_end_exit_and_artifacts() -> None:
    ledger = new_aegis_day_evidence_ledger_v1(day_utc="2026-04-27", mode="DRY_RUN", run_style="MANUAL")
    record_command_v1(ledger, phase="PRE_MARKET", name="cmd", command=["true"], start_utc="a", end_utc="b", exit_code=0, inputs=["in"], outputs=["out"])
    assert ledger["commands"][0]["start_utc"] == "a"
    assert ledger["commands"][0]["end_utc"] == "b"
    assert ledger["commands"][0]["exit_code"] == 0
    assert ledger["commands"][0]["outputs"] == ["out"]


def test_ledger_records_blockers_and_safe_continuation() -> None:
    ledger = new_aegis_day_evidence_ledger_v1(day_utc="2026-04-27", mode="DRY_RUN", run_style="MANUAL")
    record_phase_v1(ledger, phase="SUBMIT", status="SKIPPED", reason="BLOCKED")
    final = finalize_aegis_day_evidence_ledger_v1(
        ledger,
        final_daily_outcome="BLOCKED_WITH_REASON",
        blockers=[{"code": "PREFLIGHT_BLOCKED", "owner": "paper_trading_day_authority"}],
        diagnostics=[{"phase": "PACKET", "status": "CONTINUED_AFTER_BLOCKER"}],
    )
    assert final["blockers"][0]["code"] == "PREFLIGHT_BLOCKED"
    assert final["diagnostics"][0]["status"] == "CONTINUED_AFTER_BLOCKER"


def test_final_outcome_is_never_unknown_when_evidence_exists() -> None:
    ledger = new_aegis_day_evidence_ledger_v1(day_utc="2026-04-27", mode="DRY_RUN", run_style="MANUAL")
    final = finalize_aegis_day_evidence_ledger_v1(ledger, final_daily_outcome="SUCCESS_DRY_RUN")
    assert final["final_daily_outcome"] == "SUCCESS_DRY_RUN"
    assert final["final_daily_outcome"] != "UNKNOWN"


def test_ledger_records_auto_cycle() -> None:
    ledger = new_aegis_day_evidence_ledger_v1(day_utc="2026-04-27", mode="DRY_RUN", run_style="AUTO")
    record_cycle_v1(
        ledger,
        cycle_id="AUTO-2026-04-27-0001",
        started_at_utc="a",
        ended_at_utc="b",
        market_session_state="MARKET_OPEN",
        authorities_before={"runtime_service_authority_v1": "PASS"},
        actions_taken=["RAN:aegis_paper_preflight"],
        submit_attempted=False,
        skip_reason="AUTO_SKIPPED_ALREADY_SUBMITTED",
        outputs={"ledger": "path"},
        blockers=[{"code": "AUTO_SKIPPED_ALREADY_SUBMITTED"}],
        next_wake_at_utc="c",
    )
    assert ledger["cycles"][0]["cycle_id"] == "AUTO-2026-04-27-0001"
    assert ledger["cycles"][0]["skip_reason"] == "AUTO_SKIPPED_ALREADY_SUBMITTED"
