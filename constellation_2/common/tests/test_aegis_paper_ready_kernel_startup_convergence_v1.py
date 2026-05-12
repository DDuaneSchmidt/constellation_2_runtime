from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_aegis_paper_ready_kernel_v1 as kernel


DAY = "2026-05-12"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _stage_script(command: list[str]) -> str:
    return Path(command[1]).name if len(command) > 1 else ""


def _runner(
    *,
    canonical_root: Path,
    sleeve_root: Path,
    startup_status: str,
    startup_missing_symbols: list[str] | None = None,
    call_log: list[str],
):
    def run(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        script = _stage_script(command)
        call_log.append(script)
        if script == "run_trading_day_readiness_authority_v1.py":
            _write_json(
                canonical_root / "reports/trading_day_readiness_authority_v1" / DAY / "trading_day_readiness_authority.v1.json",
                {"target_day": DAY, "status": "PASS", "readiness_mode": "INTRADAY_SUBMIT_READY"},
            )
            return subprocess.CompletedProcess(command, 0, stdout='{"status":"PASS"}\n', stderr="")
        if script == "run_broker_supply_v1.py":
            _write_json(
                sleeve_root / "reports/broker_supply_v1" / DAY / "broker_supply.v1.json",
                {"day_utc": DAY, "status": "PASS"},
            )
        elif script == "run_cash_ledger_from_broker_v1.py":
            _write_json(
                sleeve_root / "cash_ledger_v1/snapshots" / DAY / "cash_ledger_snapshot.v1.json",
                {"day_utc": DAY, "status": "OK", "cash_total_cents": 100_000, "nlv_total_cents": 100_000},
            )
        elif script == "run_positions_snapshot_day_v5.py":
            _write_json(
                sleeve_root / "positions_v1/snapshots" / DAY / "positions_snapshot.v5.json",
                {"day_utc": DAY, "status": "OK"},
            )
        elif script == "run_accounting_nav_v2_day_v1.py":
            _write_json(
                sleeve_root / "accounting_v2/nav" / DAY / "nav.v2.json",
                {"day_utc": DAY, "status": "ACTIVE", "nav": {"cash_total_cents": 100_000, "nav_total_cents": 100_000}},
            )
        elif script == "bridge_accounting_nav_v2_to_compat_v1.py":
            _write_json(
                sleeve_root / "accounting_compat_v1/nav" / DAY / "nav_snapshot.v1.json",
                {"day_utc": DAY, "status": "OK"},
            )
        elif script == "run_capital_supply_v1.py":
            _write_json(
                sleeve_root / "reports/capital_supply_v1" / DAY / "capital_supply.v1.json",
                {"day_utc": DAY, "status": "PASS"},
            )
        elif script == "run_paper_startup_intent_input_convergence_v1.py":
            missing = startup_missing_symbols or []
            _write_json(
                canonical_root
                / "reports/paper_startup_intent_input_convergence_v1"
                / DAY
                / "paper_startup_intent_input_convergence.v1.json",
                {
                    "target_day": DAY,
                    "convergence_status": startup_status,
                    "symbol_diagnostics": {
                        "required_snapshot_symbols": ["TLT"],
                        "materialized_snapshot_symbols": [] if missing else ["TLT"],
                        "missing_snapshot_symbols": missing,
                        "symbol_source": "ENGINE_MODEL_REGISTRY_V1+SLEEVE_CONTRACTS_V1",
                        "bridge_symbol_used_for_market_snapshot": False,
                    },
                },
            )
            return subprocess.CompletedProcess(command, 0 if startup_status == "SUCCESS" else 2, stdout="{}\n", stderr="")
        elif script == "run_trading_day_intent_generation_v1.py":
            _write_json(
                sleeve_root / "reports/trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json",
                {"day_utc": DAY, "final_status": "VALID_ZERO", "blocking_codes": []},
            )
            return subprocess.CompletedProcess(command, 0, stdout='{"final_status":"VALID_ZERO"}\n', stderr="")
        else:
            # Later stages are intentionally not part of these focused tests.
            return subprocess.CompletedProcess(command, 3, stdout="", stderr="STOP_AFTER_INTENT_GENERATION")
        return subprocess.CompletedProcess(command, 0, stdout="{}\n", stderr="")

    return run


def test_paper_ready_kernel_invokes_startup_convergence_before_intent_generation(tmp_path: Path) -> None:
    canonical_root = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    calls: list[str] = []

    kernel.run_paper_ready_kernel_v1(
        target_day=DAY,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        require_current_release=False,
        command_runner=_runner(
            canonical_root=canonical_root,
            sleeve_root=sleeve_root,
            startup_status="SUCCESS",
            call_log=calls,
        ),
    )

    assert "run_paper_startup_intent_input_convergence_v1.py" in calls
    assert "run_trading_day_intent_generation_v1.py" in calls
    assert calls.index("run_paper_startup_intent_input_convergence_v1.py") < calls.index(
        "run_trading_day_intent_generation_v1.py"
    )


def test_missing_tlt_blocks_at_startup_convergence_before_intent_generation(tmp_path: Path) -> None:
    canonical_root = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    calls: list[str] = []

    report = kernel.run_paper_ready_kernel_v1(
        target_day=DAY,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        require_current_release=False,
        command_runner=_runner(
            canonical_root=canonical_root,
            sleeve_root=sleeve_root,
            startup_status="BLOCKED",
            startup_missing_symbols=["TLT"],
            call_log=calls,
        ),
    )

    assert report["failed_stage_id"] == "paper_startup_intent_input_convergence"
    assert report["first_blocker"] == "MARKET_DATA_SNAPSHOT_V1_MISSING_SYMBOLS"
    assert report["actual_value"] == ["TLT"]
    assert "run_trading_day_intent_generation_v1.py" not in calls
    assert report["submit_allowed"] is False
    assert report["submission_authorized"] is False


def test_stale_spy_bridge_startup_artifact_is_refreshed_before_intent_generation(tmp_path: Path) -> None:
    canonical_root = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    stale_path = (
        canonical_root
        / "reports/paper_startup_intent_input_convergence_v1"
        / DAY
        / "paper_startup_intent_input_convergence.v1.json"
    )
    _write_json(
        stale_path,
        {
            "target_day": DAY,
            "convergence_status": "SUCCESS",
            "symbol_diagnostics": {
                "bridge_symbol": "SPY",
                "bridge_symbol_used_for_market_snapshot": True,
                "symbol_source": "BRIDGE_SYMBOL_ARGUMENT",
            },
        },
    )
    calls: list[str] = []

    kernel.run_paper_ready_kernel_v1(
        target_day=DAY,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        require_current_release=False,
        command_runner=_runner(
            canonical_root=canonical_root,
            sleeve_root=sleeve_root,
            startup_status="SUCCESS",
            call_log=calls,
        ),
    )

    refreshed = json.loads(stale_path.read_text(encoding="utf-8"))
    assert "run_paper_startup_intent_input_convergence_v1.py" in calls
    assert refreshed["symbol_diagnostics"]["bridge_symbol_used_for_market_snapshot"] is False
    assert refreshed["symbol_diagnostics"]["required_snapshot_symbols"] == ["TLT"]
