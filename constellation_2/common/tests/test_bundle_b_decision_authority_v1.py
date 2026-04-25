from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

from constellation_2.common.governed_evaluation_v1 import materialize_governed_evaluation_day_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_authorization_artifacts_day_v1 as auth_writer
import ops.tools.run_capital_authority_allocation_day_v1 as bundle_b


ACCOUNT_ID = "DUO847203"
GIT_SHA = "7d64db4a5e4d68af1d89a56edf64fb9024bb218a"
SLEEVE_ENGINE_IDS = {
    "C2_TREND_EQ_PRIMARY": "C2_TREND_EQ_PRIMARY_V1",
    "C2_VOL_INCOME_DEFINED_RISK": "C2_VOL_INCOME_DEFINED_RISK_V1",
    "C2_MEAN_REVERSION_EQ": "C2_MEAN_REVERSION_EQ_V1",
    "C2_EVENT_DISLOCATION": "C2_EVENT_DISLOCATION_V1",
    "C2_DEFENSIVE_TAIL": "C2_DEFENSIVE_TAIL_V1",
    "C2_CROSS_ASSET_TREND": "C2_CROSS_ASSET_TREND_V1",
    "C2_MARKET_NEUTRAL_SPREAD": "C2_MARKET_NEUTRAL_SPREAD_V1",
}


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _prev_day(day_utc: str) -> str:
    return (date.fromisoformat(day_utc) - timedelta(days=1)).isoformat()


def _positions_snapshot_obj(*, day_utc: str, cash_total_cents: int, items: list[dict]) -> dict:
    return {
        "schema_id": "C2_POSITIONS_SNAPSHOT_V5",
        "schema_version": 5,
        "day_utc": day_utc,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "producer": {"repo": "constellation", "git_sha": GIT_SHA, "module": "test_bundle_b_decision_authority_v1.py"},
        "status": "OK",
        "reason_codes": ["TEST_POSITIONS_SNAPSHOT"],
        "input_manifest": [],
        "accounts": [
            {
                "account_id": ACCOUNT_ID,
                "currency": "USD",
                "cash_total_cents": cash_total_cents,
                "broker_cash_cents": cash_total_cents,
                "cash_source": "TEST",
                "reason_codes": [],
            }
        ],
        "items": items,
        "reconciliation": {
            "broker_statement_present": True,
            "broker_statement_path": "/tmp/broker.json",
            "cash_status": "MATCH",
            "cash_delta_cents": 0,
            "positions_status": "MATCH",
            "reason_codes": [],
            "position_mismatches": [],
        },
        "canonical_json_hash": "0" * 64,
    }


def _position(*, position_id: str, symbol: str, qty: int, avg_cost_cents: int, origin: str, engine_id: str) -> dict:
    return {
        "position_id": position_id,
        "account_id": ACCOUNT_ID,
        "origin": origin,
        "engine_id": engine_id,
        "source_intent_id": f"SRC:{position_id}",
        "intent_sha256": "1" * 64,
        "instrument": {"kind": "EQUITY", "symbol": symbol, "currency": "USD"},
        "qty": qty,
        "avg_cost_cents": avg_cost_cents,
        "opened_day_utc": "2026-04-20",
        "last_transition_utc": "2026-04-20T00:00:00Z",
        "last_transition_type": "OPEN",
        "lifecycle_state": "MANAGING" if qty else "CLOSED",
        "lifecycle_reason_code": "TEST",
        "status": "OPEN" if qty else "CLOSED",
        "lots": [],
        "reconciliation": {"broker_position_present": True, "broker_qty": str(qty), "status": "MATCH", "reason_codes": []},
    }


def _exposure_intent(*, day_utc: str, intent_id: str, engine_id: str, symbol: str, target_notional_pct: str, max_risk_pct: str = "0.01") -> dict:
    return {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": intent_id,
        "created_at_utc": f"{day_utc}T00:00:00Z",
        "engine": {"engine_id": engine_id, "suite": "C2_HYBRID_V1", "mode": "PAPER"},
        "underlying": {"symbol": symbol, "currency": "USD"},
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": target_notional_pct,
        "expected_holding_days": 10,
        "risk_class": "TREND",
        "constraints": {"max_risk_pct": max_risk_pct},
    }


def _short_vol_defined_exposure_intent(*, day_utc: str, intent_id: str, engine_id: str, symbol: str, target_notional_pct: str, max_risk_pct: str = "0.01") -> dict:
    return {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": intent_id,
        "created_at_utc": f"{day_utc}T00:00:00Z",
        "engine": {"engine_id": engine_id, "suite": "C2_HYBRID_V1", "mode": "PAPER"},
        "underlying": {"symbol": symbol, "currency": "USD"},
        "exposure_type": "SHORT_VOL_DEFINED",
        "target_notional_pct": target_notional_pct,
        "expected_holding_days": 7,
        "risk_class": "VOL_INCOME_DEFINED",
        "constraints": {"max_risk_pct": max_risk_pct},
        "option": {"direction": "SELL", "structure": "PUT"},
    }


def _options_intent(*, day_utc: str, intent_id: str, engine_id: str, symbol: str, max_contracts: int, max_risk_usd: str) -> dict:
    return {
        "schema_id": "options_intent",
        "schema_version": "v2",
        "intent_id": intent_id,
        "created_at_utc": f"{day_utc}T00:00:00Z",
        "engine": {"engine_id": engine_id, "suite": "C2_HYBRID_V1", "mode": "PAPER"},
        "underlying": {"symbol": symbol, "currency": "USD"},
        "risk": {"max_contracts": max_contracts, "max_risk_usd": max_risk_usd},
    }


def _seed_authority_inputs(truth_root: Path, day_utc: str, *, headroom_cents: int, nav_total_cents: int) -> None:
    _write_json(truth_root / "risk_v1" / "exposure_net_v1" / day_utc / "exposure_net.v1.json", {"schema_id": "exposure_net"})
    _write_json(
        truth_root / "reports" / "capital_risk_envelope_v2" / day_utc / "capital_risk_envelope.v2.json",
        {
            "status": "PASS",
            "envelope": {
                "headroom_cents": headroom_cents,
                "nav_total_cents": nav_total_cents,
                "allowed_capital_at_risk_cents": headroom_cents,
                "portfolio_capital_at_risk_cents": 0,
                "drawdown_pct": "-0.010000",
                "drawdown_abs": "-1000",
                "multiplier": "1.00",
            },
        },
    )
    multiplier_bp_by_sleeve = {
        sleeve.sleeve_id: 10000
        for sleeve in bundle_b._parse_sleeves(bundle_b._load_policy())
    }
    _write_json(
        truth_root / "reports" / "correlation_envelope_gate_v1" / day_utc / "correlation_envelope_gate.v1.json",
        {"status": "PASS", "caps": {"multiplier_bp_by_sleeve": multiplier_bp_by_sleeve}},
    )
    _write_json(
        truth_root / "reports" / "gate_stack_verdict_v1" / day_utc / "gate_stack_verdict.v1.json",
        {"schema_id": "gate_stack_verdict", "schema_version": "v1", "day_utc": day_utc, "status": "PASS"},
    )
    _write_json(
        truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
        {
            "schema_id": "c2_run_pointer_canonical_authority_head",
            "schema_version": "v1",
            "day_utc": day_utc,
            "status": "PASS",
            "authoritative": True,
            "points_to": f"reports/gate_stack_verdict_v1/{day_utc}/gate_stack_verdict.v1.json",
        },
    )


def _seed_positions_snapshot(truth_root: Path, day_utc: str, *, cash_total_cents: int, items: list[dict]) -> Path:
    path = truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json"
    _write_json(path, _positions_snapshot_obj(day_utc=day_utc, cash_total_cents=cash_total_cents, items=items))
    return path


def _seed_intents(truth_root: Path, day_utc: str, intents: list[dict]) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for index, intent in enumerate(intents, start=1):
        path = truth_root / "intents_v1" / "snapshots" / day_utc / f"{index:02d}.{intent['intent_id']}.json"
        _write_json(path, intent)
        out[str(intent["intent_id"])] = path
    return out


def _seed_phasec_defined_risk_candidate(
    truth_root: Path,
    *,
    day_utc: str,
    intent_hash: str,
    attempt_id: str,
    max_loss_usd: str,
    contracts: int,
) -> Path:
    execution_root = truth_root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"
    candidate = execution_root / "phaseC_preflight_v1" / day_utc / f"attempt_{attempt_id}" / intent_hash
    order_plan_path = candidate / "order_plan.v1.json"
    _write_json(
        order_plan_path,
        {
            "schema_id": "order_plan",
            "schema_version": "v1",
            "day_utc": day_utc,
            "environment": "PAPER",
            "risk_proof": {
                "defined_risk_proven": True,
                "contracts": int(contracts),
                "max_loss_usd": str(max_loss_usd),
            },
        },
    )
    _write_json(
        candidate / "execution_identity_record.v1.json",
        {
            "schema_id": "execution_identity_record",
            "schema_version": "v1",
            "day_utc": day_utc,
            "environment": "PAPER",
            "intent_hash": intent_hash,
            "source_refs": [
                {
                    "ref_type": "order_plan_ref",
                    "path": str(order_plan_path.resolve()),
                    "sha256": "3" * 64,
                }
            ],
        },
    )
    return candidate


def _mock_positions_loader(monkeypatch: pytest.MonkeyPatch, positions_path: Path) -> None:
    monkeypatch.setattr(
        bundle_b,
        "_load_positions_snapshot",
        lambda truth_root, day: (positions_path, _read_json(positions_path), bundle_b._sha256_file(positions_path)),
    )


def _mock_sleeve_edge(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        bundle_b,
        "_load_sleeve_edge_allocator_meta",
        lambda **kwargs: {
            "qualification_state": "QUALIFIED",
            "edge_band": "QUALIFIED_POSITIVE",
            "execution_health_band": "ACCEPTABLE",
            "sample_sufficiency_band": "SUFFICIENT",
            "drift_band": "STABLE",
            "reason_codes": [],
            "action_reason_code": "SLEEVE_EDGE_OK",
            "capital_multiplier_bp": 10000,
            "snapshot_path": "/tmp/sleeve_edge_snapshot.v1.json",
            "snapshot_sha256": "2" * 64,
            "snapshot_policy_version": "v1",
            "snapshot_calculation_version": "v1",
        },
    )
    monkeypatch.setattr(bundle_b, "_git_sha_failclosed", lambda: GIT_SHA)


def _mock_passthrough_governed_control(monkeypatch: pytest.MonkeyPatch) -> None:
    def _resolver(*, truth_root: Path, day_utc: str, sleeve_ids: list[str]) -> dict:
        return {
            "consumer_seam": "capital_authority_allocation_v1",
            "policy_registry_ref": {"path": str((REPO_ROOT / "governance/02_REGISTRIES/C2_GOVERNED_EVALUATION_POLICY_V1.json").resolve()), "sha256": "0" * 64},
            "scorecard_control_input_forbidden": True,
            "portfolio_control": {
                "scope_kind": "portfolio",
                "scope_id": "CONSTELLATION_PORTFOLIO",
                "execution_sleeve_id": "",
                "mode": "",
                "adoption_state": "ADOPTED",
                "artifact_status": "OK",
                "action_state": "continue",
                "mandatory_or_advisory": "ADVISORY",
                "headroom_multiplier_bp": 10000,
                "control_state": "allow",
                "reason_codes": ["TEST_GOVERNED_CONTROL_CONTINUE"],
                "action_reason_codes": [],
                "action_ref": {"artifact_id": "portfolio_governance_action_state_v1", "path": str((truth_root / "reports/portfolio_governance_action_state_v1" / day_utc / "portfolio_governance_action_state.v1.json").resolve()), "sha256": "1" * 64},
                "evaluation_ref": {},
                "policy_snapshot_ref": {},
                "diagnostic": "",
            },
            "sleeve_controls": [
                {
                    "scope_kind": "sleeve",
                    "scope_id": sleeve_id,
                    "execution_sleeve_id": "PRIMARY",
                    "mode": "PAPER",
                    "adoption_state": "ADOPTED",
                    "artifact_status": "OK",
                    "action_state": "continue",
                    "mandatory_or_advisory": "ADVISORY",
                    "headroom_multiplier_bp": 10000,
                    "control_state": "allow",
                    "reason_codes": ["TEST_GOVERNED_CONTROL_CONTINUE"],
                    "action_reason_codes": [],
                    "action_ref": {"artifact_id": "sleeve_governance_action_state_v1", "path": str((truth_root.parent / "truth_sleeves/PRIMARY/PAPER/reports/sleeve_governance_action_state_v1" / day_utc / sleeve_id / "sleeve_governance_action_state.v1.json").resolve()), "sha256": "2" * 64},
                    "evaluation_ref": {},
                    "policy_snapshot_ref": {},
                    "diagnostic": "",
                }
                for sleeve_id in sleeve_ids
            ],
            "portfolio_headroom_multiplier_bp": 10000,
            "sleeve_headroom_multiplier_bp_by_sleeve": {sleeve_id: 10000 for sleeve_id in sleeve_ids},
        }

    monkeypatch.setattr(bundle_b, "resolve_capital_authority_runtime_control_v1", _resolver)


def _binding(*, sleeve_id: str, mode: str, enabled: bool, account_id: str, allowed_engine_ids: tuple[str, ...]) -> bundle_b.ExecutionBinding:
    return bundle_b.ExecutionBinding(
        execution_sleeve_id=sleeve_id,
        mode=mode,
        enabled=enabled,
        account_id=account_id,
        allowed_engine_ids=allowed_engine_ids,
        allowed_execution_sleeve_ids=(sleeve_id,),
    )


def _strategy_sleeve_for_engine(engine_id: str) -> str:
    sleeves = bundle_b._parse_sleeves(bundle_b._load_policy())
    mapping = bundle_b._build_engine_to_sleeve(sleeves)
    return mapping[engine_id]


def _seed_previous_day_economic_build(
    truth_root: Path,
    *,
    day_utc: str,
    sleeve_signals: list[dict],
) -> Path:
    path = truth_root / "reports" / bundle_b.ECONOMIC_BUILD_FAMILY / day_utc / "ctx-test" / "economic_state_build.v1.json"
    _write_json(
        path,
        {
            "schema_id": "economic_state_build",
            "schema_version": "v1",
            "day_utc": day_utc,
            "closure_status": "COMPLETE",
            "economic_evaluation": {
                "reallocation_signal_state": {
                    "signals": sleeve_signals,
                    "reason_codes": [],
                }
            },
        },
    )
    return path


def _bootstrap_governed_market_data(execution_root: Path, *, day_utc: str) -> None:
    prev_day = _prev_day(day_utc)
    _write_json(
        execution_root / "market_data_snapshot_v1" / "snapshots" / day_utc / "SPY.market_data_snapshot.v1.json",
        {"schema_id": "C2_MARKET_DATA_SNAPSHOT_V1", "schema_version": "v1", "day_utc": day_utc, "symbol": "SPY", "close": "500.00"},
    )
    _write_json(
        execution_root / "market_data_snapshot_v1" / "snapshots" / prev_day / "SPY.market_data_snapshot.v1.json",
        {"schema_id": "C2_MARKET_DATA_SNAPSHOT_V1", "schema_version": "v1", "day_utc": prev_day, "symbol": "SPY", "close": "495.00"},
    )


def _bootstrap_governed_sleeve(
    execution_root: Path,
    *,
    day_utc: str,
    sleeve_id: str,
    sample_count: int,
    expectancy: str,
    slippage_status: str = "PASS",
) -> None:
    fact_path = execution_root / "reports" / "sleeve_edge_fact_ledger_v1" / day_utc / sleeve_id / ("f" * 64) / "sleeve_edge_fact_ledger.v1.json"
    snapshot_path = execution_root / "reports" / "sleeve_edge_snapshot_v1" / day_utc / sleeve_id / ("f" * 64) / "sleeve_edge_snapshot.v1.json"
    _write_json(
        fact_path,
        {
            "schema_id": "C2_SLEEVE_EDGE_FACT_LEDGER_V1",
            "schema_version": "v1",
            "produced_utc": f"{day_utc}T20:00:00Z",
            "day_utc": day_utc,
            "as_of_ts": f"{day_utc}T20:00:00Z",
            "sleeve_id": sleeve_id,
            "strategy_family": sleeve_id,
            "source_execution_sleeve_id": "PRIMARY",
            "source_execution_root_path": str(execution_root.resolve()),
            "calculation_version": "sleeve_edge_measurement_v1",
            "core2_materialization_set_id": "a" * 64,
            "engine_ids": [SLEEVE_ENGINE_IDS[sleeve_id]],
            "fact_input_hash": "b" * 64,
            "input_manifest": [],
            "trade_facts": [],
            "included_trade_ids": [],
            "excluded_trade_ids": [],
            "exclusion_details": [],
            "invalidity_reasons": [],
            "producer": {"repo": "constellation", "git_sha": GIT_SHA, "module": "test_bundle_b_decision_authority_v1.py"},
        },
    )
    _write_json(
        snapshot_path,
        {
            "schema_id": "C2_SLEEVE_EDGE_SNAPSHOT_V1",
            "schema_version": "v1",
            "snapshot_id": "c" * 64,
            "produced_utc": f"{day_utc}T20:00:00Z",
            "day_utc": day_utc,
            "as_of_ts": f"{day_utc}T20:00:00Z",
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
                "artifact_path": str(fact_path.resolve()),
                "artifact_sha256": "d" * 64,
            },
            "factual_metrics": {
                "native_trade_count": sample_count,
                "native_net_pnl": "100.00",
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
                "invalidity_reasons": [],
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
            "reason_codes": [],
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
            "producer": {"repo": "constellation", "git_sha": GIT_SHA, "module": "test_bundle_b_decision_authority_v1.py"},
        },
    )
    _write_json(
        execution_root / "reports" / "liquidity_slippage_gate_v1" / day_utc / "liquidity_slippage_gate.v1.json",
        {"status": slippage_status},
    )
    _write_json(
        execution_root / "exit_reconciliation_v1" / day_utc / "exit_reconciliation.v1.json",
        {"status": "PASS"},
    )


def _bootstrap_governed_portfolio(
    truth_root: Path,
    *,
    day_utc: str,
    benchmark_status: str = "OK",
    benchmark_comparison: str = "0.01000000",
) -> None:
    _write_json(
        truth_root / "reports" / "economic_state_build_v1" / day_utc / "ctx" / "economic_state_build.v1.json",
        {
            "day_utc": day_utc,
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
                        "status": benchmark_status,
                        "baseline_daily_return": "0.00000000",
                        "comparison_vs_portfolio_return": benchmark_comparison,
                        "reason_codes": [],
                    },
                    "external_benchmarks": [],
                    "reason_codes": [],
                },
            },
        },
    )


def _emit_governed_action_artifacts(
    truth_root: Path,
    *,
    day_utc: str,
    sleeve_sample_count: int,
    benchmark_status: str = "OK",
    sleeve_ids: list[str] | None = None,
) -> dict:
    execution_root = truth_root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True, exist_ok=True)
    _bootstrap_governed_market_data(execution_root, day_utc=day_utc)
    target_sleeve_ids = list(sleeve_ids or ["C2_TREND_EQ_PRIMARY"])
    for sleeve_id in target_sleeve_ids:
        _bootstrap_governed_sleeve(
            execution_root,
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            sample_count=sleeve_sample_count,
            expectancy="30.00",
        )
    _bootstrap_governed_portfolio(
        truth_root,
        day_utc=day_utc,
        benchmark_status=benchmark_status,
    )
    return materialize_governed_evaluation_day_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_ids=target_sleeve_ids,
    )


def _run_bundle_b(day_utc: str, truth_root: Path, *, authority_verdict_path: Path | None = None) -> Path:
    argv = [
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
        "--canonical_sequence_owner",
        bundle_b.CANONICAL_SEQUENCE_OWNER,
    ]
    if authority_verdict_path is not None:
        argv.extend(["--authority_verdict_path", str(authority_verdict_path)])
    rc = bundle_b.main(argv)
    assert rc == 0
    return truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day_utc / "capital_authority_allocation.v1.json"


def _run_auth_writer(day_utc: str, truth_root: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(auth_writer, "_git_sha", lambda: GIT_SHA)
    monkeypatch.setattr(auth_writer, "validate_against_repo_schema_v1", lambda *args, **kwargs: None)
    rc = auth_writer.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    assert rc == 0
    return truth_root / "engine_activity_v1" / "authorization_v1" / day_utc


def test_bundle_b_builds_canonical_chain_for_imported_and_native_positions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    day_utc = "2026-04-20"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=250_000, nav_total_cents=1_000_000)
    positions_path = _seed_positions_snapshot(
        truth_root,
        day_utc,
        cash_total_cents=910_000,
        items=[
            _position(position_id="imported-spy", symbol="SPY", qty=5, avg_cost_cents=10_000, origin="IMPORTED", engine_id="imported_position"),
            _position(position_id="native-iwm", symbol="IWM", qty=1, avg_cost_cents=10_000, origin="NATIVE", engine_id="C2_TREND_EQ_PRIMARY_V1"),
            _position(position_id="native-qqq", symbol="QQQ", qty=2, avg_cost_cents=10_000, origin="NATIVE", engine_id="C2_MEAN_REVERSION_EQ_V1"),
            _position(position_id="native-gld", symbol="GLD", qty=1, avg_cost_cents=10_000, origin="NATIVE", engine_id="C2_VOL_INCOME_DEFINED_RISK_V1"),
        ],
    )
    _seed_intents(
        truth_root,
        day_utc,
        [
            _exposure_intent(day_utc=day_utc, intent_id="intent-spy-close-0001", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", target_notional_pct="0"),
            _exposure_intent(day_utc=day_utc, intent_id="intent-iwm-add-00001", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="IWM", target_notional_pct="0.050000"),
            _exposure_intent(day_utc=day_utc, intent_id="intent-qqq-reduce01", engine_id="C2_MEAN_REVERSION_EQ_V1", symbol="QQQ", target_notional_pct="0.010000"),
            _exposure_intent(day_utc=day_utc, intent_id="intent-gld-close01", engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="GLD", target_notional_pct="0"),
            _exposure_intent(day_utc=day_utc, intent_id="intent-tlt-open001", engine_id="C2_DEFENSIVE_TAIL_V1", symbol="TLT", target_notional_pct="0.030000", max_risk_pct="0.002"),
        ],
    )

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    _mock_passthrough_governed_control(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=(
                        "C2_TREND_EQ_PRIMARY_V1",
                        "C2_MEAN_REVERSION_EQ_V1",
                        "C2_VOL_INCOME_DEFINED_RISK_V1",
                        "C2_DEFENSIVE_TAIL_V1",
                    ),
                )
            ],
            "3" * 64,
            "4" * 64,
        ),
    )

    alloc_path = _run_bundle_b(day_utc, truth_root)
    alloc = _read_json(alloc_path)

    candidates = {row["intent_id"]: row for row in alloc["decision_chain"]["candidate_actions"]}
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}
    assert {row["action_type"] for row in candidates.values()} == {"ADD", "CLOSE", "OPEN", "REDUCE"}

    assert candidates["intent-spy-close-0001"]["position_id"] == "imported-spy"
    assert authorized["intent-spy-close-0001"]["authorization_outcome"] == "APPROVED"
    assert authorized["intent-spy-close-0001"]["authorized_quantity"] == 5

    assert authorized["intent-iwm-add-00001"]["action_type"] == "ADD"
    assert authorized["intent-iwm-add-00001"]["authorized_quantity"] == 1
    assert authorized["intent-qqq-reduce01"]["action_type"] == "REDUCE"
    assert authorized["intent-qqq-reduce01"]["authorized_quantity"] == 1
    assert authorized["intent-gld-close01"]["action_type"] == "CLOSE"
    assert authorized["intent-gld-close01"]["authorized_quantity"] == 1
    assert authorized["intent-tlt-open001"]["action_type"] == "OPEN"
    assert authorized["intent-tlt-open001"]["authorized_quantity"] == 1

    assert alloc["allocation_state"]["target_basis"] == "INTENT_TARGET_NOTIONAL_PCT"
    assert alloc["allocation_state"]["actual_basis"] == "POSITIONS_COST_BASIS_ABS_OVER_NAV_BASIS"
    assert alloc["allocation_state"]["max_abs_drift_notional_pct"] == "0.050000"
    assert alloc["sleeve_account_authority_state"]["bindings"][0]["account_id"] == ACCOUNT_ID
    assert alloc["reason_codes"][0] == "BUNDLE_B_CANONICAL_DECISION_CHAIN_V1"

    auth_dir = _run_auth_writer(day_utc, truth_root, monkeypatch)
    auth_payloads = {path.name: _read_json(path) for path in sorted(auth_dir.glob("*.authorization.v1.json"))}
    assert len(auth_payloads) == 5
    sample = next(iter(auth_payloads.values()))
    validate_against_repo_schema_v1(sample, REPO_ROOT, auth_writer.SCHEMA_RELPATH)
    assert sample["input_manifest"][1]["path"] == str(alloc_path)
    assert sample["status"] == "AUTHORIZED"
    assert set(sample["constitutional_authorization"].keys()) == {
        "authorization_source",
        "decision_enum",
        "effective_scope",
        "expires_at",
        "fact_bundle_hash",
        "issued_at",
        "policy_version",
        "proposal_hash",
    }
    assert "authorization_id" not in sample["constitutional_authorization"]
    assert "issuer_identity" not in sample["constitutional_authorization"]
    assert "schema_id" not in sample["constitutional_authorization"]
    assert "schema_version" not in sample["constitutional_authorization"]
    assert sample["constitutional_shadow"]["constitutional_authorization"]["schema_id"] == "constitutional_authorization"
    assert sample["constitutional_shadow"]["constitutional_authorization"]["schema_version"] == "v1"
    assert sample["constitutional_shadow"]["constitutional_authorization"]["issuer_identity"]["producer_module"] == (
        "ops/tools/run_authorization_artifacts_day_v1.py"
    )


def test_bundle_b_emits_resized_blocked_and_rejected_outcomes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    day_utc = "2026-04-21"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=20_000, nav_total_cents=1_000_000)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=1_000_000, items=[])
    _seed_intents(
        truth_root,
        day_utc,
        [
            _options_intent(day_utc=day_utc, intent_id="intent-options-resize", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", max_contracts=3, max_risk_usd="300"),
            _exposure_intent(day_utc=day_utc, intent_id="intent-no-position-exit", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="QQQ", target_notional_pct="0"),
            _exposure_intent(day_utc=day_utc, intent_id="intent-blocked-mode", engine_id="C2_DEFENSIVE_TAIL_V1", symbol="TLT", target_notional_pct="0.020000"),
        ],
    )

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    _mock_passthrough_governed_control(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_TREND_EQ_PRIMARY_V1", "C2_VOL_INCOME_DEFINED_RISK_V1"),
                ),
                _binding(
                    sleeve_id="SECONDARY",
                    mode="INACTIVE",
                    enabled=False,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_DEFENSIVE_TAIL_V1",),
                ),
            ],
            "5" * 64,
            "6" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}
    assert authorized["intent-options-resize"]["authorization_outcome"] == "RESIZED"
    assert authorized["intent-options-resize"]["authorized_quantity"] == 2
    assert "BUNDLE_B_RESIZED" in authorized["intent-options-resize"]["reason_codes"]
    assert authorized["intent-no-position-exit"]["authorization_outcome"] == "REJECTED"
    assert authorized["intent-blocked-mode"]["authorization_outcome"] == "BLOCKED"


def test_bundle_b_short_vol_defined_uses_phasec_defined_risk_when_headroom_sufficient(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day_utc = "2026-05-02"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=170_000, nav_total_cents=6_665_900)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=6_665_900, items=[])
    intent_paths = _seed_intents(
        truth_root,
        day_utc,
        [
            _short_vol_defined_exposure_intent(
                day_utc=day_utc,
                intent_id="intent-short-vol-defined",
                engine_id="C2_VOL_INCOME_DEFINED_RISK_V1",
                symbol="SPY",
                target_notional_pct="0.010000",
                max_risk_pct="0.01",
            ),
        ],
    )
    intent_hash = bundle_b._intent_hash_from_snapshot_path(intent_paths["intent-short-vol-defined"])
    _seed_phasec_defined_risk_candidate(
        truth_root,
        day_utc=day_utc,
        intent_hash=intent_hash,
        attempt_id="A0009",
        max_loss_usd="388.00",
        contracts=1,
    )

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    _mock_passthrough_governed_control(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_VOL_INCOME_DEFINED_RISK_V1",),
                )
            ],
            "6" * 64,
            "7" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}
    row = authorized["intent-short-vol-defined"]
    assert row["authorization_outcome"] == "APPROVED"
    assert row["requested_quantity"] == 1
    assert row["requested_quantity_basis"] == "INTENT_RISK_BUDGET"
    assert row["risk_per_unit_cents"] == 38_800
    assert row["required_risk_cents"] == 38_800
    assert row["authorized_quantity"] == 1


def test_bundle_b_short_vol_defined_rejects_when_defined_risk_exceeds_headroom(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day_utc = "2026-05-03"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=130_000, nav_total_cents=6_665_900)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=6_665_900, items=[])
    intent_paths = _seed_intents(
        truth_root,
        day_utc,
        [
            _short_vol_defined_exposure_intent(
                day_utc=day_utc,
                intent_id="intent-short-vol-headroom-blocked",
                engine_id="C2_VOL_INCOME_DEFINED_RISK_V1",
                symbol="SPY",
                target_notional_pct="0.010000",
                max_risk_pct="0.01",
            ),
        ],
    )
    intent_hash = bundle_b._intent_hash_from_snapshot_path(intent_paths["intent-short-vol-headroom-blocked"])
    _seed_phasec_defined_risk_candidate(
        truth_root,
        day_utc=day_utc,
        intent_hash=intent_hash,
        attempt_id="A0010",
        max_loss_usd="388.00",
        contracts=1,
    )

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    _mock_passthrough_governed_control(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_VOL_INCOME_DEFINED_RISK_V1",),
                )
            ],
            "8" * 64,
            "9" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}
    row = authorized["intent-short-vol-headroom-blocked"]
    assert row["authorization_outcome"] == "REJECTED"
    assert row["requested_quantity"] == 1
    assert row["risk_per_unit_cents"] == 38_800
    assert row["required_risk_cents"] == 38_800
    assert row["authorized_quantity"] == 0
    assert "BUNDLE_B_HEADROOM_REJECTED" in row["reason_codes"]


def test_bundle_b_short_vol_defined_fails_closed_when_defined_risk_evidence_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day_utc = "2026-05-04"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=170_000, nav_total_cents=6_665_900)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=6_665_900, items=[])
    _seed_intents(
        truth_root,
        day_utc,
        [
            _short_vol_defined_exposure_intent(
                day_utc=day_utc,
                intent_id="intent-short-vol-missing-risk",
                engine_id="C2_VOL_INCOME_DEFINED_RISK_V1",
                symbol="SPY",
                target_notional_pct="0.010000",
                max_risk_pct="0.01",
            ),
        ],
    )

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    _mock_passthrough_governed_control(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_VOL_INCOME_DEFINED_RISK_V1",),
                )
            ],
            "a" * 64,
            "b" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}
    row = authorized["intent-short-vol-missing-risk"]
    assert row["authorization_outcome"] == "REJECTED"
    assert row["requested_quantity"] == 0
    assert row["risk_per_unit_cents"] == 0
    assert row["authorized_quantity"] == 0
    assert "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE" in row["reason_codes"]
    assert "BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN" in row["reason_codes"]


def test_bundle_b_short_vol_defined_ignores_non_matching_phasec_intent_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day_utc = "2026-05-05"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=170_000, nav_total_cents=6_665_900)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=6_665_900, items=[])
    intent_paths = _seed_intents(
        truth_root,
        day_utc,
        [
            _short_vol_defined_exposure_intent(
                day_utc=day_utc,
                intent_id="intent-short-vol-current",
                engine_id="C2_VOL_INCOME_DEFINED_RISK_V1",
                symbol="SPY",
                target_notional_pct="0.010000",
                max_risk_pct="0.01",
            ),
        ],
    )
    current_hash = bundle_b._intent_hash_from_snapshot_path(intent_paths["intent-short-vol-current"])
    _seed_phasec_defined_risk_candidate(
        truth_root,
        day_utc=day_utc,
        intent_hash="9" * 64,
        attempt_id="A0008",
        max_loss_usd="1000.00",
        contracts=1,
    )
    _seed_phasec_defined_risk_candidate(
        truth_root,
        day_utc=day_utc,
        intent_hash=current_hash,
        attempt_id="A0009",
        max_loss_usd="388.00",
        contracts=1,
    )

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    _mock_passthrough_governed_control(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_VOL_INCOME_DEFINED_RISK_V1",),
                )
            ],
            "c" * 64,
            "d" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}
    row = authorized["intent-short-vol-current"]
    assert row["authorization_outcome"] == "APPROVED"
    assert row["risk_per_unit_cents"] == 38_800
    assert row["authorized_quantity"] == 1


def test_bundle_b_auth_bridge_requires_canonical_decision_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    day_utc = "2026-04-22"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=200_000, nav_total_cents=1_000_000)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=1_000_000, items=[])
    _seed_intents(
        truth_root,
        day_utc,
        [
            _exposure_intent(day_utc=day_utc, intent_id="intent-open", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", target_notional_pct="0.020000"),
        ],
    )

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    _mock_passthrough_governed_control(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [_binding(sleeve_id="PRIMARY", mode="PAPER", enabled=True, account_id=ACCOUNT_ID, allowed_engine_ids=("C2_TREND_EQ_PRIMARY_V1",))],
            "7" * 64,
            "8" * 64,
        ),
    )

    alloc_path = _run_bundle_b(day_utc, truth_root)
    alloc = _read_json(alloc_path)
    alloc.pop("decision_chain")
    _write_json(alloc_path, alloc)

    monkeypatch.setattr(auth_writer, "_git_sha", lambda: GIT_SHA)
    with pytest.raises(SystemExit, match="BUNDLE_B_DECISION_CHAIN_MISSING"):
        auth_writer.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])


def test_bundle_b_preserves_portfolio_cap_across_strategy_sleeves(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    day_utc = "2026-04-23"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=10_000, nav_total_cents=1_000_000)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=1_000_000, items=[])
    _seed_intents(
        truth_root,
        day_utc,
        [
            _exposure_intent(day_utc=day_utc, intent_id="intent-sleeve-one", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", target_notional_pct="0.020000", max_risk_pct="0.01"),
            _exposure_intent(day_utc=day_utc, intent_id="intent-sleeve-two", engine_id="C2_VOL_INCOME_DEFINED_RISK_V1", symbol="QQQ", target_notional_pct="0.020000", max_risk_pct="0.01"),
        ],
    )

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    _mock_passthrough_governed_control(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_TREND_EQ_PRIMARY_V1", "C2_VOL_INCOME_DEFINED_RISK_V1"),
                )
            ],
            "9" * 64,
            "a" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}
    assert authorized["intent-sleeve-one"]["authorization_outcome"] == "APPROVED"
    assert authorized["intent-sleeve-two"]["authorization_outcome"] == "REJECTED"
    assert authorized["intent-sleeve-two"]["reason_codes"][-1] == "BUNDLE_B_HEADROOM_REJECTED"


def test_bundle_b_consumes_previous_day_bundle_c_reallocation_signal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    day_utc = "2026-04-24"
    prev_day_utc = "2026-04-23"
    truth_root = tmp_path / "truth"
    engine_id = "C2_TREND_EQ_PRIMARY_V1"
    strategy_sleeve_id = _strategy_sleeve_for_engine(engine_id)
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=10_000, nav_total_cents=1_000_000)
    _seed_previous_day_economic_build(
        truth_root,
        day_utc=prev_day_utc,
        sleeve_signals=[
            {
                "sleeve_id": strategy_sleeve_id,
                "signal": "DECREASE",
                "reason_code": "UNDERPERFORMING_PORTFOLIO",
                "observed_return": "-0.030000",
                "portfolio_return": "0.010000",
            }
        ],
    )
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=1_000_000, items=[])
    _seed_intents(
        truth_root,
        day_utc,
        [
            _exposure_intent(
                day_utc=day_utc,
                intent_id="intent-economic-haircut",
                engine_id=engine_id,
                symbol="SPY",
                target_notional_pct="0.020000",
                max_risk_pct="0.01",
            ),
        ],
    )

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    _mock_passthrough_governed_control(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=(engine_id,),
                )
            ],
            "b" * 64,
            "c" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}
    assert authorized["intent-economic-haircut"]["authorization_outcome"] == "REJECTED"
    assert "BUNDLE_C_REALLOCATION_SIGNAL_DECREASE_CAP_APPLIED" in authorized["intent-economic-haircut"]["reason_codes"]
    assert alloc["allocation_state"]["reallocation_state"]["bundle_c_signal_status"] == "OK"
    assert alloc["allocation_state"]["reallocation_state"]["bundle_c_signal_source_day_utc"] == prev_day_utc
    per_sleeve = {row["sleeve_id"]: row for row in alloc["per_sleeve"]}
    assert per_sleeve[strategy_sleeve_id]["economic_signal"] == "DECREASE"
    assert per_sleeve[strategy_sleeve_id]["economic_signal_capital_multiplier_bp"] == 5000


def test_bundle_b_accepts_explicit_day_scoped_authority_verdict_for_historical_materialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day_utc = "2026-04-26"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=250_000, nav_total_cents=1_000_000)
    _write_json(
        truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
        {
            "schema_id": "c2_run_pointer_canonical_authority_head",
            "schema_version": "v1",
            "day_utc": "2026-04-27",
            "status": "PASS",
            "authoritative": True,
            "points_to": "reports/gate_stack_verdict_v1/2026-04-27/gate_stack_verdict.v1.json",
        },
    )
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=1_000_000, items=[])
    _seed_intents(
        truth_root,
        day_utc,
        [
            _exposure_intent(
                day_utc=day_utc,
                intent_id="intent-historical-authority-open",
                engine_id="C2_TREND_EQ_PRIMARY_V1",
                symbol="SPY",
                target_notional_pct="0.010000",
            ),
        ],
    )

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    _mock_passthrough_governed_control(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_TREND_EQ_PRIMARY_V1",),
                )
            ],
            "d" * 64,
            "e" * 64,
        ),
    )

    authority_verdict_path = truth_root / "reports" / "gate_stack_verdict_v1" / day_utc / "gate_stack_verdict.v1.json"
    alloc = _read_json(_run_bundle_b(day_utc, truth_root, authority_verdict_path=authority_verdict_path))
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}
    assert authorized["intent-historical-authority-open"]["authorization_outcome"] == "APPROVED"


def test_bundle_b_consumes_governed_action_artifacts_and_not_scorecard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    day_utc = "2026-04-28"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=100_000, nav_total_cents=1_000_000)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=1_000_000, items=[])
    _seed_intents(
        truth_root,
        day_utc,
        [
            _exposure_intent(
                day_utc=day_utc,
                intent_id="intent-governed-open",
                engine_id="C2_TREND_EQ_PRIMARY_V1",
                symbol="SPY",
                target_notional_pct="0.020000",
                max_risk_pct="0.01",
            ),
        ],
    )
    evaluation_result = _emit_governed_action_artifacts(
        truth_root,
        day_utc=day_utc,
        sleeve_sample_count=0,
        benchmark_status="INVALID",
    )
    scorecard_path = Path(evaluation_result["weekly_scorecard_ref"]["path"])
    scorecard = _read_json(scorecard_path)
    scorecard["sleeve_rows"][0]["action_state"] = "continue"
    scorecard["portfolio_summary"]["action_state"] = "continue"
    _write_json(scorecard_path, scorecard)

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_TREND_EQ_PRIMARY_V1",),
                )
            ],
            "d" * 64,
            "e" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    control_state = alloc["governed_evaluation_control_state"]
    portfolio_control = control_state["portfolio_control"]
    sleeve_control = next(row for row in control_state["sleeve_controls"] if row["scope_id"] == "C2_TREND_EQ_PRIMARY")
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}
    per_sleeve = {row["sleeve_id"]: row for row in alloc["per_sleeve"]}

    assert portfolio_control["artifact_status"] == "OK"
    assert portfolio_control["action_state"] == "no_conclusion"
    assert portfolio_control["headroom_multiplier_bp"] == 5000
    assert sleeve_control["artifact_status"] == "OK"
    assert sleeve_control["action_state"] == "no_conclusion"
    assert sleeve_control["headroom_multiplier_bp"] == 5000
    assert per_sleeve["C2_TREND_EQ_PRIMARY"]["allowed_capital_at_risk_cents"] == sleeve_control["effective_headroom_cents"]
    assert alloc["portfolio"]["allowed_capital_at_risk_cents"] == portfolio_control["effective_headroom_cents"]
    assert authorized["intent-governed-open"]["authorization_outcome"] == "APPROVED"
    assert control_state["scorecard_control_input_forbidden"] is True


def test_bundle_b_fails_safe_when_portfolio_action_artifact_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    day_utc = "2026-04-29"
    truth_root = tmp_path / "truth"
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=100_000, nav_total_cents=1_000_000)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=1_000_000, items=[])
    _seed_intents(
        truth_root,
        day_utc,
        [
            _exposure_intent(
                day_utc=day_utc,
                intent_id="intent-failsafe-open",
                engine_id="C2_TREND_EQ_PRIMARY_V1",
                symbol="SPY",
                target_notional_pct="0.020000",
                max_risk_pct="0.01",
            ),
        ],
    )
    evaluation_result = _emit_governed_action_artifacts(
        truth_root,
        day_utc=day_utc,
        sleeve_sample_count=6,
        benchmark_status="OK",
    )
    Path(evaluation_result["portfolio"]["action_ref"]["path"]).unlink()

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_TREND_EQ_PRIMARY_V1",),
                )
            ],
            "f" * 64,
            "a" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    portfolio_control = alloc["governed_evaluation_control_state"]["portfolio_control"]
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}

    assert portfolio_control["artifact_status"] == "MISSING"
    assert portfolio_control["control_state"] == "fail_safe_block_new_risk"
    assert portfolio_control["headroom_multiplier_bp"] == 0
    assert alloc["portfolio"]["allowed_capital_at_risk_cents"] == 0
    assert authorized["intent-failsafe-open"]["authorization_outcome"] == "REJECTED"


def test_bundle_b_multi_sleeve_adopted_bindings_consume_action_artifacts_and_leave_inactive_sleeves_passthrough(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day_utc = "2026-04-30"
    truth_root = tmp_path / "truth"
    active_sleeves = [
        "C2_TREND_EQ_PRIMARY",
        "C2_VOL_INCOME_DEFINED_RISK",
        "C2_MEAN_REVERSION_EQ",
        "C2_EVENT_DISLOCATION",
        "C2_DEFENSIVE_TAIL",
    ]
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=200_000, nav_total_cents=1_000_000)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=1_000_000, items=[])
    _seed_intents(
        truth_root,
        day_utc,
        [
            _exposure_intent(day_utc=day_utc, intent_id="intent-trend-open", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", target_notional_pct="0.020000"),
            _exposure_intent(day_utc=day_utc, intent_id="intent-meanrev-open", engine_id="C2_MEAN_REVERSION_EQ_V1", symbol="QQQ", target_notional_pct="0.020000"),
        ],
    )
    evaluation_result = _emit_governed_action_artifacts(
        truth_root,
        day_utc=day_utc,
        sleeve_sample_count=6,
        benchmark_status="OK",
        sleeve_ids=active_sleeves,
    )
    scorecard_path = Path(evaluation_result["weekly_scorecard_ref"]["path"])
    scorecard = _read_json(scorecard_path)
    for row in scorecard["sleeve_rows"]:
        row["action_state"] = "pause"
        row["display_grade"] = "PAUSE"
    _write_json(scorecard_path, scorecard)

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_TREND_EQ_PRIMARY_V1", "C2_MEAN_REVERSION_EQ_V1"),
                )
            ],
            "1" * 64,
            "2" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    control_by_id = {
        row["scope_id"]: row
        for row in alloc["governed_evaluation_control_state"]["sleeve_controls"]
    }
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}

    for sleeve_id in active_sleeves:
        assert control_by_id[sleeve_id]["adoption_state"] == "ADOPTED"
        assert control_by_id[sleeve_id]["artifact_status"] == "OK"
        assert control_by_id[sleeve_id]["action_state"] == "continue"
        assert control_by_id[sleeve_id]["headroom_multiplier_bp"] == 10000
    for sleeve_id in ("C2_CROSS_ASSET_TREND", "C2_MARKET_NEUTRAL_SPREAD"):
        assert control_by_id[sleeve_id]["adoption_state"] == "NOT_ADOPTED"
        assert control_by_id[sleeve_id]["artifact_status"] == "NOT_ADOPTED"
        assert control_by_id[sleeve_id]["control_state"] == "not_adopted_passthrough"
        assert control_by_id[sleeve_id]["headroom_multiplier_bp"] == 10000
    assert authorized["intent-trend-open"]["authorization_outcome"] == "APPROVED"
    assert alloc["governed_evaluation_control_state"]["scorecard_control_input_forbidden"] is True


def test_bundle_b_fails_safe_when_adopted_sleeve_action_artifact_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    day_utc = "2026-05-01"
    truth_root = tmp_path / "truth"
    active_sleeves = [
        "C2_TREND_EQ_PRIMARY",
        "C2_VOL_INCOME_DEFINED_RISK",
        "C2_MEAN_REVERSION_EQ",
        "C2_EVENT_DISLOCATION",
        "C2_DEFENSIVE_TAIL",
    ]
    _seed_authority_inputs(truth_root, day_utc, headroom_cents=100_000, nav_total_cents=1_000_000)
    positions_path = _seed_positions_snapshot(truth_root, day_utc, cash_total_cents=1_000_000, items=[])
    _seed_intents(
        truth_root,
        day_utc,
        [
            _exposure_intent(day_utc=day_utc, intent_id="intent-trend-open", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", target_notional_pct="0.020000"),
            _exposure_intent(day_utc=day_utc, intent_id="intent-meanrev-open", engine_id="C2_MEAN_REVERSION_EQ_V1", symbol="QQQ", target_notional_pct="0.020000"),
        ],
    )
    evaluation_result = _emit_governed_action_artifacts(
        truth_root,
        day_utc=day_utc,
        sleeve_sample_count=6,
        benchmark_status="OK",
        sleeve_ids=active_sleeves,
    )
    Path(
        next(
            row["action_ref"]["path"]
            for row in evaluation_result["sleeves"]
            if row["sleeve_id"] == "C2_MEAN_REVERSION_EQ"
        )
    ).unlink()

    _mock_positions_loader(monkeypatch, positions_path)
    _mock_sleeve_edge(monkeypatch)
    monkeypatch.setattr(
        bundle_b,
        "_load_execution_bindings",
        lambda: (
            [
                _binding(
                    sleeve_id="PRIMARY",
                    mode="PAPER",
                    enabled=True,
                    account_id=ACCOUNT_ID,
                    allowed_engine_ids=("C2_TREND_EQ_PRIMARY_V1", "C2_MEAN_REVERSION_EQ_V1"),
                )
            ],
            "3" * 64,
            "4" * 64,
        ),
    )

    alloc = _read_json(_run_bundle_b(day_utc, truth_root))
    control_by_id = {
        row["scope_id"]: row
        for row in alloc["governed_evaluation_control_state"]["sleeve_controls"]
    }
    authorized = {row["intent_id"]: row for row in alloc["decision_chain"]["authorized_trade_intents"]}

    assert control_by_id["C2_TREND_EQ_PRIMARY"]["artifact_status"] == "OK"
    assert control_by_id["C2_TREND_EQ_PRIMARY"]["action_state"] == "continue"
    assert control_by_id["C2_TREND_EQ_PRIMARY"]["headroom_multiplier_bp"] == 10000
    assert control_by_id["C2_MEAN_REVERSION_EQ"]["artifact_status"] == "MISSING"
    assert control_by_id["C2_MEAN_REVERSION_EQ"]["control_state"] == "fail_safe_block_new_risk"
    assert control_by_id["C2_MEAN_REVERSION_EQ"]["headroom_multiplier_bp"] == 0
    assert authorized["intent-trend-open"]["authorization_outcome"] == "APPROVED"
    assert authorized["intent-meanrev-open"]["authorization_outcome"] == "REJECTED"


def test_auth_bridge_selects_effective_intents_deterministically_without_mtime(tmp_path: Path) -> None:
    intents_dir = tmp_path / "truth" / "intents_v1" / "snapshots" / "2026-04-25"
    intents_dir.mkdir(parents=True, exist_ok=True)
    stale_path = intents_dir / "01.intent-duplicate.stale.json"
    fresh_path = intents_dir / "02.intent-duplicate.fresh.json"
    _write_json(
        stale_path,
        _exposure_intent(
            day_utc="2026-04-25",
            intent_id="intent-duplicate",
            engine_id="C2_TREND_EQ_PRIMARY_V1",
            symbol="SPY",
            target_notional_pct="0.010000",
        ),
    )
    fresh_obj = _exposure_intent(
        day_utc="2026-04-25",
        intent_id="intent-duplicate",
        engine_id="C2_TREND_EQ_PRIMARY_V1",
        symbol="SPY",
        target_notional_pct="0.020000",
    )
    fresh_obj["created_at_utc"] = "2026-04-25T00:05:00Z"
    _write_json(fresh_path, fresh_obj)

    os.utime(stale_path, (2_000_000_000, 2_000_000_000))
    os.utime(fresh_path, (1_000_000_000, 1_000_000_000))

    selected = auth_writer._select_effective_intents(intents_dir)

    assert selected == [fresh_path]
