from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_aegis_paper_ready_kernel_v1 as kernel


DAY = "2026-05-13"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _verdict_path(root: Path) -> Path:
    return root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"


def _head_path(root: Path) -> Path:
    return root / "run_pointer_v2" / "canonical_authority_head.v1.json"


def _write_authority(root: Path) -> None:
    _write_json(
        _verdict_path(root),
        {
            "schema_id": "authorization_gate_verdict_v1",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "PASS",
            "reason_codes": ["AUTHORIZATION_GATES_PASS"],
        },
    )
    _write_json(
        _head_path(root),
        {
            "schema_id": "c2_run_pointer_canonical_authority_head",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "PASS",
            "authoritative": True,
            "points_to": str(_verdict_path(root)),
        },
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_selected_intent(sleeve_root: Path) -> Path:
    intent_path = sleeve_root / "intents_v1" / "snapshots" / DAY / "intent-1.exposure_intent.v1.json"
    _write_json(
        intent_path,
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "day_utc": DAY,
            "intent_id": "intent-1",
            "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "mode": "PAPER"},
            "underlying": {"symbol": "SPY"},
            "exposure_type": "LONG_EQUITY",
            "target_notional_pct": "0.01",
        },
    )
    intent_hash = _sha256(intent_path)
    selected = {
        "intent_id": "intent-1",
        "intent_hash": intent_hash,
        "intent_path": str(intent_path),
        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
        "symbol": "SPY",
    }
    _write_json(
        sleeve_root / "pointers" / "selected_intent_pointer.v1.json",
        {
            "schema_id": "selected_intent_pointer",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "SELECTED",
            "canonical_blocker": "",
            "selected_intent": selected,
        },
    )
    _write_json(
        sleeve_root / "reports" / "intent_arbitration_v1" / DAY / "intent_arbitration.v1.json",
        {
            "schema_id": "intent_arbitration_v1",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "SELECTED",
            "selected_intent": selected,
        },
    )
    return intent_path


def _write_exposure_net(sleeve_root: Path, *, day_utc: str = DAY, status: str = "OK", intent_path: Path | None = None) -> Path:
    manifest = []
    if intent_path is not None:
        manifest.append(
            {
                "day_utc": DAY,
                "path": str(intent_path),
                "producer": "intents_v1",
                "sha256": _sha256(intent_path),
                "type": "intents_snapshot",
            }
        )
    path = sleeve_root / "risk_v1" / "exposure_net_v1" / DAY / "exposure_net.v1.json"
    _write_json(
        path,
        {
            "schema_id": "C2_EXPOSURE_NET_V1",
            "schema_version": 1,
            "day_utc": day_utc,
            "status": status,
            "input_manifest": manifest,
            "portfolio": {"used_capital_at_risk_cents": 0},
        },
    )
    return path


def test_exposure_net_stage_runs_after_authority_freshness_and_before_capital_allocation() -> None:
    stages = kernel._stages(
        target_day=DAY,
        canonical_truth_root=Path("/tmp/canonical"),
        paper_sleeve_root=Path("/tmp/sleeve"),
        environment="PAPER",
        ib_account="DUO847203",
        release_commit="0" * 40,
    )
    stage_ids = [stage.stage_id for stage in stages]

    assert stage_ids.index("structure_decision_supply") < stage_ids.index("exposure_net")
    assert stage_ids.index("paper_authority_head_freshness") < stage_ids.index("exposure_net")
    assert stage_ids.index("exposure_net") < stage_ids.index("capital_authority_allocation")


def test_authorization_supply_uses_paper_sleeve_evidence_after_allocation() -> None:
    stages = kernel._stages(
        target_day=DAY,
        canonical_truth_root=Path("/tmp/canonical"),
        paper_sleeve_root=Path("/tmp/sleeve"),
        environment="PAPER",
        ib_account="DUO847203",
        release_commit="0" * 40,
    )
    stage_ids = [stage.stage_id for stage in stages]
    auth_supply = stages[stage_ids.index("authorization_supply")]

    assert stage_ids.index("capital_authority_allocation") < stage_ids.index("authorization_supply")
    assert auth_supply.truth_role == "PAPER_SLEEVE"
    assert auth_supply.command[-1] == "/tmp/sleeve"
    assert auth_supply.artifact_rel == Path("reports/authorization_supply_v1") / DAY / "authorization_supply.v1.json"


def _runner_until_exposure_net(
    *,
    canonical_root: Path,
    sleeve_root: Path,
    call_log: list[str],
    exposure_net_returncode: int,
    write_exposure_net: bool,
):
    def runner(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        script = Path(command[1]).name if len(command) > 1 else ""
        call_log.append(script)
        if script == "run_trading_day_readiness_authority_v1.py":
            _write_json(
                canonical_root / "reports/trading_day_readiness_authority_v1" / DAY / "trading_day_readiness_authority.v1.json",
                {"target_day": DAY, "status": "PASS", "readiness_mode": "INTRADAY_SUBMIT_READY"},
            )
        elif script == "run_broker_supply_v1.py":
            _write_json(sleeve_root / "reports/broker_supply_v1" / DAY / "broker_supply.v1.json", {"day_utc": DAY, "status": "PASS"})
        elif script == "run_cash_ledger_from_broker_v1.py":
            _write_json(sleeve_root / "cash_ledger_v1/snapshots" / DAY / "cash_ledger_snapshot.v1.json", {"day_utc": DAY, "status": "OK", "cash_total_cents": 100_000, "nlv_total_cents": 100_000})
        elif script == "run_positions_snapshot_day_v5.py":
            _write_json(sleeve_root / "positions_v1/snapshots" / DAY / "positions_snapshot.v5.json", {"day_utc": DAY, "status": "OK", "items": []})
        elif script == "run_accounting_nav_v2_day_v1.py":
            _write_json(sleeve_root / "accounting_v2/nav" / DAY / "nav.v2.json", {"day_utc": DAY, "status": "ACTIVE", "nav": {"cash_total_cents": 100_000, "nav_total_cents": 100_000}})
        elif script == "bridge_accounting_nav_v2_to_compat_v1.py":
            _write_json(sleeve_root / "accounting_compat_v1/nav" / DAY / "nav_snapshot.v1.json", {"day_utc": DAY, "status": "OK"})
        elif script == "run_capital_supply_v1.py":
            _write_json(sleeve_root / "reports/capital_supply_v1" / DAY / "capital_supply.v1.json", {"day_utc": DAY, "status": "PASS"})
        elif script == "run_paper_startup_intent_input_convergence_v1.py":
            _write_json(
                canonical_root / "reports/paper_startup_intent_input_convergence_v1" / DAY / "paper_startup_intent_input_convergence.v1.json",
                {
                    "target_day": DAY,
                    "generated_utc": "2099-01-01T00:00:00Z",
                    "convergence_status": "SUCCESS",
                    "symbol_diagnostics": {"bridge_symbol_used_for_market_snapshot": False, "missing_snapshot_symbols": []},
                },
            )
        elif script == "run_trading_day_intent_generation_v1.py":
            _write_json(sleeve_root / "reports/trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json", {"day_utc": DAY, "final_status": "INTENTS_PRESENT"})
        elif script == "run_portfolio_activation_gate_v1.py":
            _write_json(sleeve_root / "reports/portfolio_activation_gate_v1" / DAY / "portfolio_activation_gate.v1.json", {"day_utc": DAY, "environment": "PAPER", "status": "PASS"})
        elif script == "run_portfolio_scoring_v1.py":
            _write_json(sleeve_root / "reports/portfolio_scoring_v1" / DAY / "portfolio_scoring.v1.json", {"day_utc": DAY, "status": "PASS"})
        elif script == "run_intent_arbitration_v1.py":
            _write_selected_intent(sleeve_root)
        elif script == "run_risk_budget_supply_v1.py":
            _write_json(sleeve_root / "reports/risk_budget_supply_v1" / DAY / "risk_budget_supply.v1.json", {"day_utc": DAY, "status": "PASS"})
        elif script == "run_market_open_data_gate_v1.py":
            _write_json(sleeve_root / "reports/market_open_data_gate_v1" / DAY / "market_open_data_gate.v1.json", {"day_utc": DAY, "status": "PASS"})
        elif script == "run_structure_decision_supply_v1.py":
            _write_json(sleeve_root / "reports/structure_decision_supply_v1" / DAY / "structure_decision_supply.v1.json", {"day_utc": DAY, "status": "PASS"})
        elif script == "run_exposure_net_day_v1.py":
            if write_exposure_net:
                _write_json(
                    sleeve_root / "risk_v1/exposure_net_v1" / DAY / "exposure_net.v1.json",
                    {"schema_id": "C2_EXPOSURE_NET_V1", "schema_version": 1, "day_utc": DAY, "status": "OK"},
                )
            return subprocess.CompletedProcess(command, exposure_net_returncode, stdout="", stderr="producer failed")
        elif script == "run_capital_authority_allocation_day_v1.py":
            raise AssertionError("capital allocation must not run when exposure_net is unavailable")
        return subprocess.CompletedProcess(command, 0, stdout="{}\n", stderr="")

    return runner


def _runner_with_existing_exposure_net(
    *,
    canonical_root: Path,
    sleeve_root: Path,
    call_log: list[str],
):
    def runner(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        script = Path(command[1]).name if len(command) > 1 else ""
        call_log.append(script)
        if script == "run_exposure_net_day_v1.py":
            raise AssertionError("existing valid exposure_net must be reused without invoking producer")
        if script == "run_capital_authority_allocation_day_v1.py":
            _write_json(
                sleeve_root / "allocation_v1/capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json",
                {"schema_id": "C2_CAPITAL_AUTHORITY_ALLOCATION_V1", "schema_version": 1, "day_utc": DAY, "status": "OK"},
            )
        elif script == "run_phasec_identity_materializer_day_v1.py":
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="stop after allocation")
        else:
            return _runner_until_exposure_net(
                canonical_root=canonical_root,
                sleeve_root=sleeve_root,
                call_log=[],
                exposure_net_returncode=0,
                write_exposure_net=False,
            )(command, cwd)
        return subprocess.CompletedProcess(command, 0, stdout="{}\n", stderr="")

    return runner


def test_missing_exposure_net_stops_before_capital_authority_allocation(tmp_path: Path) -> None:
    canonical_root = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_authority(sleeve_root)
    call_log: list[str] = []

    report = kernel.run_paper_ready_kernel_v1(
        target_day=DAY,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        require_current_release=False,
        command_runner=_runner_until_exposure_net(
            canonical_root=canonical_root,
            sleeve_root=sleeve_root,
            call_log=call_log,
            exposure_net_returncode=1,
            write_exposure_net=False,
        ),
    )

    assert report["failed_stage_id"] == "exposure_net"
    assert report["first_blocker"] == "MISSING_ARTIFACT"
    assert "run_exposure_net_day_v1.py" in call_log
    assert "run_capital_authority_allocation_day_v1.py" not in call_log
    assert report["submit_allowed"] is False
    assert report["submission_authorized"] is False


def test_valid_existing_exposure_net_is_reused_without_overwrite_and_allocation_proceeds(tmp_path: Path) -> None:
    canonical_root = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_authority(sleeve_root)
    intent_path = _write_selected_intent(sleeve_root)
    exposure_path = _write_exposure_net(sleeve_root, intent_path=intent_path)
    before = exposure_path.read_bytes()
    call_log: list[str] = []

    report = kernel.run_paper_ready_kernel_v1(
        target_day=DAY,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        require_current_release=False,
        command_runner=_runner_with_existing_exposure_net(
            canonical_root=canonical_root,
            sleeve_root=sleeve_root,
            call_log=call_log,
        ),
    )

    exposure_stage = next(row for row in report["stage_results"] if row["stage_id"] == "exposure_net")
    assert exposure_stage["status"] == "PASS"
    assert exposure_stage["reuse_existing"] is True
    assert "run_exposure_net_day_v1.py" not in call_log
    assert "run_capital_authority_allocation_day_v1.py" in call_log
    assert exposure_path.read_bytes() == before


def test_wrong_day_existing_exposure_net_blocks_before_allocation(tmp_path: Path) -> None:
    canonical_root = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_authority(sleeve_root)
    intent_path = _write_selected_intent(sleeve_root)
    _write_exposure_net(sleeve_root, day_utc="2026-05-12", intent_path=intent_path)
    call_log: list[str] = []

    report = kernel.run_paper_ready_kernel_v1(
        target_day=DAY,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        require_current_release=False,
        command_runner=_runner_with_existing_exposure_net(
            canonical_root=canonical_root,
            sleeve_root=sleeve_root,
            call_log=call_log,
        ),
    )

    assert report["failed_stage_id"] == "exposure_net"
    assert report["first_blocker"] == "TARGET_DAY_DATE_MISMATCH"
    assert "run_exposure_net_day_v1.py" not in call_log
    assert "run_capital_authority_allocation_day_v1.py" not in call_log


def test_existing_exposure_net_missing_selected_intent_manifest_blocks_before_allocation(tmp_path: Path) -> None:
    canonical_root = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_authority(sleeve_root)
    _write_selected_intent(sleeve_root)
    _write_exposure_net(sleeve_root)
    call_log: list[str] = []

    report = kernel.run_paper_ready_kernel_v1(
        target_day=DAY,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        require_current_release=False,
        command_runner=_runner_with_existing_exposure_net(
            canonical_root=canonical_root,
            sleeve_root=sleeve_root,
            call_log=call_log,
        ),
    )

    assert report["failed_stage_id"] == "exposure_net"
    assert report["first_blocker"] == "EXPOSURE_NET_SELECTED_INTENT_NOT_IN_INPUT_MANIFEST"
    assert "run_exposure_net_day_v1.py" not in call_log
    assert "run_capital_authority_allocation_day_v1.py" not in call_log


def test_nonzero_exposure_net_producer_exit_blocks_even_with_valid_artifact(tmp_path: Path) -> None:
    canonical_root = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_authority(sleeve_root)
    call_log: list[str] = []

    report = kernel.run_paper_ready_kernel_v1(
        target_day=DAY,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        require_current_release=False,
        command_runner=_runner_until_exposure_net(
            canonical_root=canonical_root,
            sleeve_root=sleeve_root,
            call_log=call_log,
            exposure_net_returncode=2,
            write_exposure_net=True,
        ),
    )

    assert report["failed_stage_id"] == "exposure_net"
    assert report["first_blocker"] == "EXPOSURE_NET_PRODUCER_FAILED"
    assert "run_exposure_net_day_v1.py" in call_log
    assert "run_capital_authority_allocation_day_v1.py" not in call_log
    assert report["submit_allowed"] is False
    assert report["submission_authorized"] is False
