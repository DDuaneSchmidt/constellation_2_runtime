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


DAY = "2026-05-12"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_selected_long_equity_intent(sleeve_root: Path, *, day: str = DAY) -> tuple[str, Path, dict]:
    payload = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": "c2_trend_eq_spy",
        "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "mode": "PAPER"},
        "exposure_type": "LONG_EQUITY",
        "underlying": {"symbol": "SPY", "currency": "USD"},
        "target_notional_pct": "0.01",
        "constraints": {"max_risk_pct": "0.01", "stop_loss_bps": 1000},
    }
    raw = json.dumps(payload, sort_keys=True).encode("utf-8") + b"\n"
    intent_hash = hashlib.sha256(raw).hexdigest()
    intent_path = sleeve_root / "intents_v1" / "snapshots" / day / f"{intent_hash}.exposure_intent.v1.json"
    intent_path.parent.mkdir(parents=True, exist_ok=True)
    intent_path.write_bytes(raw)
    _write_json(
        sleeve_root / "pointers" / "selected_intent_pointer.v1.json",
        {
            "schema_id": "selected_intent_pointer",
            "day_utc": day,
            "status": "SELECTED",
            "selected_intent": {
                "intent_id": payload["intent_id"],
                "intent_hash": intent_hash,
                "intent_path": str(intent_path),
                "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                "symbol": "SPY",
            },
        },
    )
    return intent_hash, intent_path, payload


def _authority_stage() -> kernel.KernelStage:
    return kernel._stage(
        "paper_authority_head_freshness",
        "authorization",
        ["python3", "-c", "pass"],
        "PAPER_SLEEVE",
        Path("run_pointer_v2/canonical_authority_head.v1.json"),
        ("PASS",),
        "Inspect PAPER authorization_gate_verdict_v1 and canonical authority head; do not synthesize authority.",
        "Resolve same-day PAPER authorization gate verdict and canonical authority head before capital allocation.",
    )


def _risk_contract_stage() -> kernel.KernelStage:
    return kernel._stage(
        "risk_definition_contract",
        "risk",
        ["python3", "ops/tools/run_risk_definition_contract_v1.py"],
        "PAPER_SLEEVE",
        None,
        ("PASS",),
        "python3 ops/tools/run_risk_definition_contract_v1.py --day_utc {day} --truth_root {sleeve} --intent_hash <selected_intent_hash>",
        "Produce selected LONG_EQUITY stop-risk contract before capital allocation.",
    )


def _head_path(root: Path) -> Path:
    return root / "run_pointer_v2" / "canonical_authority_head.v1.json"


def _verdict_path(root: Path, day: str = DAY) -> Path:
    return root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json"


def _head_payload(root: Path, *, day: str = DAY, status: str = "PASS", authoritative: bool = True) -> dict:
    return {
        "schema_id": "c2_run_pointer_canonical_authority_head",
        "schema_version": "v1",
        "day_utc": day,
        "status": status,
        "authoritative": authoritative,
        "points_to": str(_verdict_path(root, day)),
    }


def _verdict_payload(*, day: str = DAY, status: str = "PASS", reason_codes: list[str] | None = None) -> dict:
    return {
        "schema_id": "authorization_gate_verdict_v1",
        "schema_version": "v1",
        "day_utc": day,
        "status": status,
        "reason_codes": reason_codes or [],
    }


def test_stale_authority_head_blocks_before_capital_allocation(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    stale_day = "2026-05-04"
    _write_json(_head_path(sleeve_root), _head_payload(sleeve_root, day=stale_day))
    _write_json(
        _verdict_path(sleeve_root),
        _verdict_payload(
            status="FAIL",
            reason_codes=[
                "AUTHORIZATION_GATE_NOT_PASS:correlation_envelope_gate_v1:MISSING",
                "AUTHORIZATION_GATE_NOT_PASS:replay_certification_gate_v1:MISSING",
            ],
        ),
    )

    result = kernel._validate_stage_artifact(stage=_authority_stage(), artifact_path=_head_path(sleeve_root), target_day=DAY)

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "AUTHORITY_HEAD_DAY_MISMATCH"
    assert result["failed_field"] == "day_utc"
    diagnostics = result["authority_head_freshness"]
    assert diagnostics["expected_day_utc"] == DAY
    assert diagnostics["observed_head_day_utc"] == stale_day
    assert diagnostics["same_day_authorization_verdict_status"] == "FAIL"
    assert diagnostics["authorization_missing_inputs"] == [
        "correlation_envelope_gate_v1",
        "replay_certification_gate_v1",
    ]


def test_missing_authority_head_blocks_clearly(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"

    result = kernel._validate_stage_artifact(stage=_authority_stage(), artifact_path=_head_path(sleeve_root), target_day=DAY)

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "AUTHORITY_HEAD_NOT_READY_FOR_DAY"
    assert result["failed_field"] == "run_pointer_v2/canonical_authority_head.v1.json"
    assert result["actual_value"] == "missing"
    assert result["authority_head_freshness"]["expected_day_utc"] == DAY


def test_same_day_fail_authorization_verdict_blocks_clearly(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_json(_head_path(sleeve_root), _head_payload(sleeve_root))
    _write_json(
        _verdict_path(sleeve_root),
        _verdict_payload(
            status="FAIL",
            reason_codes=[
                "AUTHORIZATION_GATE_NOT_PASS:correlation_envelope_gate_v1:MISSING",
                "AUTHORIZATION_GATE_NOT_PASS:replay_certification_gate_v1:MISSING",
            ],
        ),
    )

    result = kernel._validate_stage_artifact(stage=_authority_stage(), artifact_path=_head_path(sleeve_root), target_day=DAY)

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "AUTHORITY_HEAD_NOT_READY_FOR_DAY"
    assert result["failed_field"] == "points_to.status"
    assert result["actual_value"] == "FAIL"
    diagnostics = result["authority_head_freshness"]
    assert diagnostics["same_day_authorization_verdict_status"] == "FAIL"
    assert diagnostics["authorization_failure_reasons"] == [
        "AUTHORIZATION_GATE_NOT_PASS:correlation_envelope_gate_v1:MISSING",
        "AUTHORIZATION_GATE_NOT_PASS:replay_certification_gate_v1:MISSING",
    ]


def test_same_day_pass_or_bootstrap_head_allows_allocation_stage_to_run(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    for status in ("PASS", "BOOTSTRAP_PASS"):
        _write_json(_head_path(sleeve_root), _head_payload(sleeve_root, status=status))
        _write_json(_verdict_path(sleeve_root), _verdict_payload(status=status))

        result = kernel._validate_stage_artifact(stage=_authority_stage(), artifact_path=_head_path(sleeve_root), target_day=DAY)

        assert result["status"] == "PASS"
        assert result["artifact_status"] == status
        assert result["authority_head_freshness"]["same_day_authorization_verdict_status"] == status


def test_authority_head_stage_is_before_capital_allocation() -> None:
    stages = kernel._stages(
        target_day=DAY,
        canonical_truth_root=Path("/tmp/canonical"),
        paper_sleeve_root=Path("/tmp/sleeve"),
        environment="PAPER",
        ib_account="DUO847203",
        release_commit="0" * 40,
    )
    stage_ids = [stage.stage_id for stage in stages]

    assert stage_ids.index("paper_authority_pointer_refresh") < stage_ids.index("paper_authority_head_freshness")
    assert stage_ids.index("paper_authority_head_freshness") < stage_ids.index("capital_authority_allocation")


def test_risk_definition_contract_stage_runs_after_structure_before_allocation() -> None:
    stages = kernel._stages(
        target_day=DAY,
        canonical_truth_root=Path("/tmp/canonical"),
        paper_sleeve_root=Path("/tmp/sleeve"),
        environment="PAPER",
        ib_account="DUO847203",
        release_commit="0" * 40,
    )
    stage_ids = [stage.stage_id for stage in stages]

    assert stage_ids.index("structure_decision_supply") < stage_ids.index("risk_definition_contract")
    assert stage_ids.index("risk_definition_contract") < stage_ids.index("sleeve_edge_measurement")
    assert stage_ids.index("sleeve_edge_measurement") < stage_ids.index("governed_evaluation")
    assert stage_ids.index("governed_evaluation") < stage_ids.index("capital_authority_allocation")


def test_risk_definition_contract_stage_materializes_selected_long_equity_stop_contract(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    intent_hash, intent_path, intent = _write_selected_long_equity_intent(sleeve_root)
    call_log: list[list[str]] = []

    def runner(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        call_log.append(command)
        assert command[-1] == intent_hash
        contract_path = sleeve_root / "risk_definition_contract_v1" / DAY / intent_hash / "risk_definition_contract.v1.json"
        _write_json(
            contract_path,
            {
                "schema_id": "risk_definition_contract_v1",
                "schema_version": "v1",
                "day_utc": DAY,
                "validation_status": "PASS",
                "truth_root": str(sleeve_root.resolve()),
                "intent_id": intent["intent_id"],
                "intent_hash": intent_hash,
                "source_intent_path": str(intent_path),
                "risk_type": "STOP_BASED",
                "quantity_basis": {"basis": "ONE_UNIT_STOP_RISK_BOOTSTRAP", "quantity": 1},
                "risk_per_unit": 5000,
                "reference_price_source": str(sleeve_root / "market_data_snapshot_v1/snapshots" / DAY / "SPY.market_data_snapshot.v1.json"),
            },
        )
        return subprocess.CompletedProcess(command, 0, stdout='{"status":"PASS"}\n', stderr="")

    result = kernel._run_risk_definition_contract_stage(
        stage=_risk_contract_stage(),
        target_day=DAY,
        sleeve_root=sleeve_root,
        runner=runner,
        workdir=tmp_path,
    )

    assert result["status"] == "PASS"
    assert result["artifact_status"] == "PASS"
    assert result["risk_type"] == "STOP_BASED"
    assert result["quantity"] == 1
    assert result["risk_per_unit_cents"] == 5000
    assert call_log[0][:2] == ["python3", "ops/tools/run_risk_definition_contract_v1.py"]


def test_risk_definition_contract_stage_blocks_when_producer_does_not_write_contract(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_selected_long_equity_intent(sleeve_root)

    def runner(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 2, stdout='{"status":"FAIL"}\n', stderr="FAIL\n")

    result = kernel._run_risk_definition_contract_stage(
        stage=_risk_contract_stage(),
        target_day=DAY,
        sleeve_root=sleeve_root,
        runner=runner,
        workdir=tmp_path,
    )

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "RISK_DEFINITION_CONTRACT_MISSING"
    assert result["returncode"] == 2


def test_risk_definition_contract_validation_fails_closed_for_wrong_root_day_or_hash(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    intent_hash, intent_path, intent = _write_selected_long_equity_intent(sleeve_root)
    contract_path = sleeve_root / "risk_definition_contract_v1" / DAY / intent_hash / "risk_definition_contract.v1.json"
    base = {
        "schema_id": "risk_definition_contract_v1",
        "schema_version": "v1",
        "day_utc": DAY,
        "validation_status": "PASS",
        "truth_root": str(sleeve_root.resolve()),
        "intent_id": intent["intent_id"],
        "intent_hash": intent_hash,
        "source_intent_path": str(intent_path),
        "risk_type": "STOP_BASED",
        "quantity_basis": {"quantity": 1},
        "risk_per_unit": 5000,
    }

    for override, blocker in (
        ({"day_utc": "2026-05-11"}, "RISK_DEFINITION_CONTRACT_DAY_MISMATCH"),
        ({"truth_root": str(tmp_path / "wrong")}, "RISK_DEFINITION_CONTRACT_TRUTH_ROOT_MISMATCH"),
        ({"intent_hash": "b" * 64}, "RISK_DEFINITION_CONTRACT_INTENT_HASH_MISMATCH"),
    ):
        _write_json(contract_path, {**base, **override})
        result = kernel._validate_risk_definition_contract_artifact(
            artifact_path=contract_path,
            target_day=DAY,
            sleeve_root=sleeve_root,
            intent_hash=intent_hash,
            intent_id=intent["intent_id"],
            intent_path=intent_path,
            stage=_risk_contract_stage(),
        )
        assert result["status"] == "BLOCKED"
        assert result["first_blocker"] == blocker


def test_authority_head_freshness_stage_does_not_mutate_pointer_file(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_json(_head_path(sleeve_root), _head_payload(sleeve_root))
    _write_json(_verdict_path(sleeve_root), _verdict_payload())
    before = _head_path(sleeve_root).read_bytes()

    result = kernel._validate_stage_artifact(stage=_authority_stage(), artifact_path=_head_path(sleeve_root), target_day=DAY)

    assert result["status"] == "PASS"
    assert _head_path(sleeve_root).read_bytes() == before


def test_kernel_blocks_at_authority_head_before_running_capital_allocation(tmp_path: Path) -> None:
    canonical_root = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    call_log: list[str] = []

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
            _write_json(sleeve_root / "positions_v1/snapshots" / DAY / "positions_snapshot.v5.json", {"day_utc": DAY, "status": "OK"})
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
            _write_json(sleeve_root / "reports/intent_arbitration_v1" / DAY / "intent_arbitration.v1.json", {"day_utc": DAY, "status": "SELECTED", "selected_intent_id": "intent-1"})
        elif script == "run_aegis_requirement_graph_v1.py":
            _write_json(
                sleeve_root / "reports/aegis_requirement_graph_v1" / DAY / "requirement_graph.v1.json",
                {
                    "schema_id": "aegis_requirement_graph",
                    "schema_version": "aegis_requirement_graph.v1",
                    "day_utc": DAY,
                    "status": "PASS",
                    "truth_root": str(sleeve_root),
                    "active_intents": [],
                    "requirements": [],
                },
            )
        elif script == "run_risk_budget_supply_v1.py":
            _write_json(sleeve_root / "reports/risk_budget_supply_v1" / DAY / "risk_budget_supply.v1.json", {"day_utc": DAY, "status": "PASS"})
        elif script == "run_market_open_data_gate_v1.py":
            _write_json(sleeve_root / "reports/market_open_data_gate_v1" / DAY / "market_open_data_gate.v1.json", {"day_utc": DAY, "status": "PASS"})
        elif script == "run_market_data_supply_v1.py":
            _write_json(sleeve_root / "reports/market_data_supply_v1" / DAY / "market_data_supply.v1.json", {"day_utc": DAY, "status": "PASS", "requirements": []})
        elif script == "run_structure_decision_supply_v1.py":
            _write_json(sleeve_root / "reports/structure_decision_supply_v1" / DAY / "structure_decision_supply.v1.json", {"day_utc": DAY, "status": "PASS"})
        elif script == "run_pointer_append_v1.py":
            pass
        elif script == "run_capital_authority_allocation_day_v1.py":
            raise AssertionError("capital allocation must not run when authority head is missing")
        return subprocess.CompletedProcess(command, 0, stdout="{}\n", stderr="")

    report = kernel.run_paper_ready_kernel_v1(
        target_day=DAY,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        require_current_release=False,
        command_runner=runner,
    )

    assert report["failed_stage_id"] == "paper_authority_pointer_refresh"
    assert report["first_blocker"] == "AUTHORITY_HEAD_NOT_READY_FOR_DAY"
    assert "run_capital_authority_allocation_day_v1.py" not in call_log
    assert report["submit_allowed"] is False
    assert report["submission_authorized"] is False
