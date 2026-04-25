from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.governed_evaluation_v1 import materialize_governed_evaluation_day_v1


DAY = "2026-04-16"
PREV_DAY = "2026-04-15"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _bootstrap_truth_roots(tmp_path: Path) -> tuple[Path, Path]:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    truth_root.mkdir(parents=True, exist_ok=True)
    execution_root.mkdir(parents=True, exist_ok=True)
    return truth_root, execution_root


def _sleeve_fact_ledger_payload(*, sleeve_id: str, trade_facts: list[dict] | None = None) -> dict:
    rows = list(trade_facts or [])
    included_ids = [str(row.get("trade_identity_id") or "") for row in rows if row.get("included_in_metrics") is True]
    excluded_ids = [str(row.get("trade_identity_id") or "") for row in rows if row.get("included_in_metrics") is not True]
    return {
        "schema_id": "C2_SLEEVE_EDGE_FACT_LEDGER_V1",
        "schema_version": "v1",
        "produced_utc": f"{DAY}T20:00:00Z",
        "day_utc": DAY,
        "as_of_ts": f"{DAY}T20:00:00Z",
        "sleeve_id": sleeve_id,
        "strategy_family": sleeve_id,
        "source_execution_sleeve_id": "PRIMARY",
        "source_execution_root_path": "/tmp/truth_sleeves/PRIMARY/PAPER",
        "calculation_version": "sleeve_edge_measurement_v1",
        "core2_materialization_set_id": "a" * 64,
        "engine_ids": ["engine-1"],
        "fact_input_hash": "b" * 64,
        "input_manifest": [],
        "trade_facts": rows,
        "included_trade_ids": [item for item in included_ids if item],
        "excluded_trade_ids": [item for item in excluded_ids if item],
        "exclusion_details": [],
        "invalidity_reasons": [],
        "producer": {"repo": "constellation", "git_sha": "a" * 40, "module": "test"},
    }


def _sleeve_snapshot_payload(
    *,
    sleeve_id: str,
    fact_ledger_path: Path,
    sample_count: int,
    expectancy: str,
    native_net_pnl: str = "100.00",
    benchmark_invalid: bool = False,
) -> dict:
    invalidity_reasons = ["SLEEVE_EDGE_NATIVE_ENGINE_ATTRIBUTION_UNAVAILABLE"] if benchmark_invalid else []
    return {
        "schema_id": "C2_SLEEVE_EDGE_SNAPSHOT_V1",
        "schema_version": "v1",
        "snapshot_id": "c" * 64,
        "produced_utc": f"{DAY}T20:00:00Z",
        "day_utc": DAY,
        "as_of_ts": f"{DAY}T20:00:00Z",
        "sleeve_id": sleeve_id,
        "strategy_family": sleeve_id,
        "metric_window": {
            "sample_basis": "CLOSED_TRADES_ONLY",
            "recent_trade_count": 2,
            "baseline_trade_count": 3,
        },
        "included_trade_ids": [],
        "excluded_trade_ids": [],
        "exclusion_details": [],
        "fact_input_hash": "b" * 64,
        "calculation_version": "sleeve_edge_measurement_v1",
        "policy_version": "v1",
        "fact_ledger_ref": {
            "artifact_path": str(fact_ledger_path.resolve()),
            "artifact_sha256": "d" * 64,
        },
        "factual_metrics": {
            "native_trade_count": sample_count,
            "native_net_pnl": native_net_pnl,
            "native_gross_pnl": "101.00",
            "native_net_expectancy": expectancy,
            "native_expectancy_per_unit_risk": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "native_realized_drawdown": "-0.010000",
            "native_capital_efficiency": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "native_budget_utilization": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "native_recent_vs_baseline_drift": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "adopted_trade_count": 0,
            "adopted_net_pnl": "0",
            "adopted_management_expectancy": "0",
            "adopted_drawdown": "0",
            "adopted_capital_efficiency": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "fee_drag": "0",
            "measured_slippage_drag": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "execution_data_completeness": {
                "fill_price_complete": True,
                "fee_complete": True,
                "decision_price_complete": True,
                "state": "COMPLETE",
            },
            "unknown_attribution_count": 0,
            "sample_count": sample_count,
            "window_coverage": {
                "recent_required_trade_count": 2,
                "recent_available_trade_count": sample_count,
                "baseline_required_trade_count": 3,
                "baseline_available_trade_count": sample_count,
            },
            "required_inputs_complete": True,
            "invalidity_reasons": invalidity_reasons,
        },
        "unavailable_metrics": [],
        "qualification": {
            "edge_band": "QUALIFIED_POSITIVE",
            "execution_health_band": "ACCEPTABLE",
            "sample_sufficiency_band": "SUFFICIENT",
            "drift_band": "STABLE",
            "qualification_state": "QUALIFIED",
            "reason_codes": [],
        },
        "reason_codes": invalidity_reasons,
        "snapshot_lineage": {
            "core2_materialization_set_id": "a" * 64,
            "source_execution_sleeve_id": "PRIMARY",
            "previous_snapshot_id": None,
            "revision_type": "INITIAL_PUBLISH",
            "revision_reason": "",
            "prior_snapshot_ref": {"artifact_path": "", "snapshot_id": "", "qualification_state": ""},
            "state_transition_reason_codes": [],
        },
        "input_manifest": [],
        "producer": {"repo": "constellation", "git_sha": "a" * 40, "module": "test"},
    }


def _bootstrap_sleeve(
    execution_root: Path,
    *,
    sleeve_id: str,
    sample_count: int,
    expectancy: str,
    native_net_pnl: str = "100.00",
    slippage_status: str = "PASS",
    trade_net_pnls: list[str] | None = None,
) -> None:
    fact_path = execution_root / "reports" / "sleeve_edge_fact_ledger_v1" / DAY / sleeve_id / ("f" * 64) / "sleeve_edge_fact_ledger.v1.json"
    snapshot_path = execution_root / "reports" / "sleeve_edge_snapshot_v1" / DAY / sleeve_id / ("f" * 64) / "sleeve_edge_snapshot.v1.json"
    trade_facts: list[dict] = []
    for idx, net_pnl_text in enumerate(trade_net_pnls or []):
        buy_price = "10.00"
        close_price = f"{10.0 + float(net_pnl_text):.2f}"
        state_path = execution_root / "reports" / "sleeve_edge_fact_trade_state_v1" / DAY / sleeve_id / f"trade_{idx}.json"
        _write_json(
            state_path,
            {
                "incorporated_fills": [
                    {
                        "observed_utc": f"{DAY}T14:{idx:02d}:00Z",
                        "side": "BUY",
                        "fill_quantity": "1",
                        "fill_price": buy_price,
                        "commission": "0",
                    },
                    {
                        "observed_utc": f"{DAY}T15:{idx:02d}:00Z",
                        "side": "SELL",
                        "fill_quantity": "1",
                        "fill_price": close_price,
                        "commission": "0",
                    },
                ],
                "current_quantity": "0",
            },
        )
        trade_facts.append(
            {
                "trade_identity_id": f"{idx + 1:064x}"[-64:],
                "measurement_class": "NATIVE_ENTRY",
                "included_in_metrics": True,
                "incorporated_state_path": str(state_path.resolve()),
            }
        )
    _write_json(fact_path, _sleeve_fact_ledger_payload(sleeve_id=sleeve_id, trade_facts=trade_facts))
    _write_json(
        snapshot_path,
        _sleeve_snapshot_payload(
            sleeve_id=sleeve_id,
            fact_ledger_path=fact_path,
            sample_count=sample_count,
            expectancy=expectancy,
            native_net_pnl=native_net_pnl,
        ),
    )
    _write_json(
        execution_root / "reports" / "liquidity_slippage_gate_v1" / DAY / "liquidity_slippage_gate.v1.json",
        {"status": slippage_status},
    )
    _write_json(
        execution_root / "exit_reconciliation_v1" / DAY / "exit_reconciliation.v1.json",
        {"status": "PASS"},
    )


def _bootstrap_portfolio(truth_root: Path) -> None:
    _write_json(
        truth_root / "reports" / "economic_state_build_v1" / DAY / "ctx" / "economic_state_build.v1.json",
        {
            "day_utc": DAY,
            "economic_evaluation": {
                "performance_state": {
                    "portfolio": {
                        "current_nav_total": 101000,
                        "previous_nav_total": 100000,
                        "daily_pnl": 1000,
                        "daily_return": "0.01000000",
                        "status": "OK",
                    }
                },
                "benchmark_state": {
                    "policy_baseline": {
                        "benchmark_id": "PREV_NAV_ZERO_RETURN_BASELINE_V1",
                        "status": "OK",
                        "baseline_daily_return": "0.00000000",
                        "comparison_vs_portfolio_return": "0.01000000",
                        "reason_codes": [],
                    },
                    "external_benchmarks": [],
                    "reason_codes": [],
                },
            },
        },
    )
    _write_json(
        truth_root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json",
        {
            "status": "PASS",
            "envelope": {
                "allowed_capital_at_risk_cents": 10000,
                "portfolio_capital_at_risk_cents": 5000,
                "drawdown_pct": "-0.010000",
                "drawdown_abs": "-1000",
                "multiplier": "1.00",
            },
        },
    )


def _bootstrap_market_data(root: Path) -> None:
    _write_json(
        root / "market_data_snapshot_v1" / "snapshots" / DAY / "SPY.market_data_snapshot.v1.json",
        {"schema_id": "C2_MARKET_DATA_SNAPSHOT_V1", "schema_version": "v1", "day_utc": DAY, "symbol": "SPY", "close": "500.00"},
    )
    _write_json(
        root / "market_data_snapshot_v1" / "snapshots" / PREV_DAY / "SPY.market_data_snapshot.v1.json",
        {"schema_id": "C2_MARKET_DATA_SNAPSHOT_V1", "schema_version": "v1", "day_utc": PREV_DAY, "symbol": "SPY", "close": "495.00"},
    )


def test_insufficient_sample_forces_no_conclusion_and_scorecard_stays_ref_driven(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(execution_root, sleeve_id="C2_TREND_EQ_PRIMARY", sample_count=0, expectancy="5.00")
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )

    sleeve_validity = json.loads(Path(result["sleeves"][0]["validity_ref"]["path"]).read_text(encoding="utf-8"))
    sleeve_action = json.loads(Path(result["sleeves"][0]["action_ref"]["path"]).read_text(encoding="utf-8"))
    scorecard = json.loads(Path(result["weekly_scorecard_ref"]["path"]).read_text(encoding="utf-8"))

    assert sleeve_validity["validity_state"] == "insufficient_sample"
    assert sleeve_action["action_state"] == "no_conclusion"
    assert "native_net_pnl" not in scorecard["sleeve_rows"][0]
    assert scorecard["sleeve_rows"][0]["evaluation_ref"]["artifact_id"] == "sleeve_evaluation_state_v1"
    assert scorecard["sleeve_rows"][0]["action_ref"]["artifact_id"] == "sleeve_governance_action_state_v1"


def test_benchmark_policy_varies_by_sleeve_role(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(execution_root, sleeve_id="C2_TREND_EQ_PRIMARY", sample_count=6, expectancy="30.00")
    _bootstrap_sleeve(execution_root, sleeve_id="C2_DEFENSIVE_TAIL", sample_count=6, expectancy="30.00")
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY", "C2_DEFENSIVE_TAIL"],
    )

    validity_by_id = {
        row["sleeve_id"]: json.loads(Path(row["validity_ref"]["path"]).read_text(encoding="utf-8"))
        for row in result["sleeves"]
    }
    assert validity_by_id["C2_TREND_EQ_PRIMARY"]["benchmark_applicability_state"] == "benchmark_valid"
    assert validity_by_id["C2_DEFENSIVE_TAIL"]["benchmark_applicability_state"] == "benchmark_not_applicable"


def test_execution_contamination_blocks_sleeve_judgment(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(
        execution_root,
        sleeve_id="C2_TREND_EQ_PRIMARY",
        sample_count=6,
        expectancy="30.00",
        slippage_status="FAIL",
    )
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )

    sleeve_validity = json.loads(Path(result["sleeves"][0]["validity_ref"]["path"]).read_text(encoding="utf-8"))
    sleeve_evaluation = json.loads(Path(result["sleeves"][0]["evaluation_ref"]["path"]).read_text(encoding="utf-8"))
    assert sleeve_validity["validity_state"] == "execution_contaminated"
    assert sleeve_evaluation["edge_state"] == "not_enough_evidence"


def test_portfolio_path_emits_governed_artifacts(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(execution_root, sleeve_id="C2_TREND_EQ_PRIMARY", sample_count=6, expectancy="30.00")
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )

    portfolio_eval = json.loads(Path(result["portfolio"]["evaluation_ref"]["path"]).read_text(encoding="utf-8"))
    portfolio_action = json.loads(Path(result["portfolio"]["action_ref"]["path"]).read_text(encoding="utf-8"))
    assert portfolio_eval["risk_state"] == "risk_within_policy"
    assert portfolio_action["action_state"] == "continue"


def test_missing_sleeve_snapshot_writes_failsafe_action_artifact(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )

    sleeve_action = json.loads(Path(result["sleeves"][0]["action_ref"]["path"]).read_text(encoding="utf-8"))
    portfolio_action = json.loads(Path(result["portfolio"]["action_ref"]["path"]).read_text(encoding="utf-8"))

    assert sleeve_action["action_state"] == "no_conclusion"
    assert sleeve_action["closure_state"] == "DEGRADED"
    assert sleeve_action["first_blocker_code"] == "SLEEVE_EVALUATION_STATE_MISSING_FOR_ACTION_STATE"
    assert sleeve_action["missing_dependency_artifacts"] == ["sleeve_evaluation_state_v1"]
    assert "MISSING_SLEEVE_EDGE_SNAPSHOT" in " ".join(sleeve_action["action_reason_codes"])
    assert portfolio_action["action_state"] == "continue"


def test_missing_economic_state_writes_portfolio_failsafe_action_artifact(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(execution_root, sleeve_id="C2_TREND_EQ_PRIMARY", sample_count=6, expectancy="30.00")

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )

    portfolio_action = json.loads(Path(result["portfolio"]["action_ref"]["path"]).read_text(encoding="utf-8"))

    assert portfolio_action["action_state"] == "no_conclusion"
    assert portfolio_action["closure_state"] == "DEGRADED"
    assert portfolio_action["first_blocker_code"] == "PORTFOLIO_EVALUATION_STATE_MISSING_FOR_ACTION_STATE"
    assert portfolio_action["missing_dependency_artifacts"] == ["portfolio_evaluation_state_v1"]
    assert "MISSING_ECONOMIC_STATE_BUILD" in " ".join(portfolio_action["action_reason_codes"])


def test_no_trades_emits_sleeve_performance_no_trades_status(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(
        execution_root,
        sleeve_id="C2_TREND_EQ_PRIMARY",
        sample_count=0,
        expectancy="0.00",
        native_net_pnl="0.00",
        trade_net_pnls=[],
    )
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )
    perf = json.loads(Path(result["sleeves"][0]["performance_ref"]["path"]).read_text(encoding="utf-8"))
    assert perf["performance_status"] in {"NO_TRADES", "NOT_ENOUGH_EVIDENCE"}
    assert perf["trade_outcome_truth"]["trade_count"] == 0
    assert perf["trade_outcome_truth"]["win_rate"]["status"] == "UNAVAILABLE"


def test_single_losing_trade_reports_negative_pnl_and_zero_win_rate(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(
        execution_root,
        sleeve_id="C2_TREND_EQ_PRIMARY",
        sample_count=1,
        expectancy="-2.00",
        native_net_pnl="-2.00",
        trade_net_pnls=["-2.00"],
    )
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )
    perf = json.loads(Path(result["sleeves"][0]["performance_ref"]["path"]).read_text(encoding="utf-8"))
    assert perf["realized_pnl_truth"]["native_net_pnl"] == "-2.00"
    assert perf["trade_outcome_truth"]["trade_count"] == 1
    assert perf["trade_outcome_truth"]["win_rate"]["status"] == "AVAILABLE"
    assert perf["trade_outcome_truth"]["win_rate"]["value"] == "0.00000000"


def test_single_winning_trade_reports_positive_pnl_and_full_win_rate(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(
        execution_root,
        sleeve_id="C2_TREND_EQ_PRIMARY",
        sample_count=1,
        expectancy="2.00",
        native_net_pnl="2.00",
        trade_net_pnls=["2.00"],
    )
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )
    perf = json.loads(Path(result["sleeves"][0]["performance_ref"]["path"]).read_text(encoding="utf-8"))
    assert perf["realized_pnl_truth"]["native_net_pnl"] == "2.00"
    assert perf["trade_outcome_truth"]["trade_count"] == 1
    assert perf["trade_outcome_truth"]["win_rate"]["status"] == "AVAILABLE"
    assert perf["trade_outcome_truth"]["win_rate"]["value"] == "1.00000000"


def test_mixed_trades_compute_win_rate_avg_win_avg_loss_and_payoff(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(
        execution_root,
        sleeve_id="C2_TREND_EQ_PRIMARY",
        sample_count=3,
        expectancy="1.00",
        native_net_pnl="3.00",
        trade_net_pnls=["2.00", "-1.00", "2.00"],
    )
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )
    perf = json.loads(Path(result["sleeves"][0]["performance_ref"]["path"]).read_text(encoding="utf-8"))
    outcomes = perf["trade_outcome_truth"]
    assert outcomes["trade_count"] == 3
    assert outcomes["winning_trade_count"] == 2
    assert outcomes["losing_trade_count"] == 1
    assert outcomes["win_rate"]["value"] == "0.66666667"
    assert outcomes["avg_win"]["value"] == "2.00000000"
    assert outcomes["avg_loss"]["value"] == "1.00000000"
    assert outcomes["payoff_ratio"]["value"] == "2.00000000"


def test_missing_sleeve_snapshot_still_writes_failsafe_performance_artifact(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )
    perf_ref = result["sleeves"][0]["performance_ref"]
    assert perf_ref["artifact_id"] == "sleeve_performance_truth_v1"
    perf = json.loads(Path(perf_ref["path"]).read_text(encoding="utf-8"))
    assert perf["performance_status"] == "NOT_ENOUGH_EVIDENCE"
    assert "SLEEVE_PERFORMANCE_UPSTREAM_MISSING" in perf["reason_codes"]


def test_missing_economic_state_still_writes_portfolio_performance_artifact(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(execution_root, sleeve_id="C2_TREND_EQ_PRIMARY", sample_count=1, expectancy="2.00")

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )
    perf_ref = result["portfolio"]["performance_ref"]
    assert perf_ref["artifact_id"] == "portfolio_performance_truth_v1"
    perf = json.loads(Path(perf_ref["path"]).read_text(encoding="utf-8"))
    assert perf["performance_status"] == "NOT_ENOUGH_EVIDENCE"
    assert "PORTFOLIO_PERFORMANCE_UPSTREAM_MISSING" in perf["reason_codes"]


def test_weekly_scorecard_projects_performance_truth_fields(tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    _bootstrap_market_data(execution_root)
    _bootstrap_sleeve(
        execution_root,
        sleeve_id="C2_TREND_EQ_PRIMARY",
        sample_count=2,
        expectancy="1.50",
        native_net_pnl="3.00",
        trade_net_pnls=["2.00", "1.00"],
    )
    _bootstrap_portfolio(truth_root)

    result = materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )
    scorecard = json.loads(Path(result["weekly_scorecard_ref"]["path"]).read_text(encoding="utf-8"))
    sleeve_row = scorecard["sleeve_rows"][0]
    assert sleeve_row["performance_ref"]["artifact_id"] == "sleeve_performance_truth_v1"
    assert sleeve_row["performance_status"] in {"ACTIVE", "NOT_ENOUGH_EVIDENCE"}
    assert sleeve_row["realized_net_pnl"] == "3.00"
    assert sleeve_row["expectancy"] == "1.50"
    assert "win_rate" in sleeve_row
