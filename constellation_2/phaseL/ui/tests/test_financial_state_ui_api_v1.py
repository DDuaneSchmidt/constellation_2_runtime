from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseL.ui_api.financial_state_read_model import build_financial_state_view


DAY = "2026-04-16"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def test_financial_state_view_uses_canonical_backend_sources(monkeypatch, tmp_path: Path) -> None:
    global_truth = tmp_path / "runtime" / "truth"
    sleeve_truth = tmp_path / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    repo_root = tmp_path / "repo"

    monkeypatch.setattr("constellation_2.phaseL.ui_api.financial_state_read_model.GLOBAL_TRUTH_ROOT", global_truth)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.financial_state_read_model.SLEEVE_TRUTH_ROOT", sleeve_truth)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.financial_state_read_model.REPO_ROOT", repo_root)

    _write_json(
        repo_root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
        {
            "schema_id": "c2_ib_account_registry",
            "accounts": [
                {
                    "account_id": "DUO847203",
                    "environment": "PAPER",
                    "enabled_for_submission": True,
                    "allowed_engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                    "allowed_sleeve_ids": ["PRIMARY"],
                    "allowed_symbols": None,
                    "notes": ["registry-note"],
                }
            ],
        },
    )
    _write_json(
        global_truth / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "status": "ACTIVE",
            "produced_utc": f"{DAY}T00:00:00Z",
            "nav": {
                "currency": "USD",
                "cash_total": 100000,
                "nav_total": 125000,
                "gross_positions_value": 25000,
                "realized_pnl_to_date": 500,
                "unrealized_pnl": 1200,
                "components": [
                    {
                        "kind": "ETF",
                        "symbol": "SPY",
                        "qty": "10",
                        "mv": 25000,
                        "mark": {
                            "asof_utc": f"{DAY}T00:00:00Z",
                            "source": "BROKER_STATEMENT",
                            "last": 500,
                        },
                    }
                ],
            },
        },
    )
    _write_json(
        global_truth / "cash_ledger_v1" / "snapshots" / DAY / "cash_ledger_snapshot.v1.json",
        {
            "schema_id": "C2_CASH_LEDGER_SNAPSHOT_V1",
            "status": "OK",
            "snapshot": {
                "account_id": "DUO847203",
                "currency": "USD",
                "cash_total_cents": 10000000,
                "nlv_total_cents": 12500000,
                "available_funds_cents": 8000000,
                "excess_liquidity_cents": 6000000,
                "observed_at_utc": f"{DAY}T00:00:00Z",
            },
        },
    )
    _write_json(
        sleeve_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json",
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V5",
            "schema_version": 5,
            "day_utc": DAY,
            "produced_utc": f"{DAY}T00:00:00Z",
            "producer": {"repo": "constellation", "git_sha": "1" * 7, "module": "test"},
            "status": "OK",
            "reason_codes": [],
            "input_manifest": [{"type": "other", "path": "/tmp/in.json", "sha256": "0" * 64}],
            "accounts": [
                {
                    "account_id": "DUO847203",
                    "currency": "USD",
                    "cash_total_cents": 10000000,
                    "broker_cash_cents": 10000000,
                    "cash_source": "CASH_LEDGER_ONLY",
                    "reason_codes": [],
                }
            ],
            "items": [
                {
                    "position_id": "pos-spy",
                    "account_id": "DUO847203",
                    "origin": "NATIVE",
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "source_intent_id": "intent-spy",
                    "intent_sha256": "2" * 64,
                    "instrument": {"kind": "EQUITY", "symbol": "SPY"},
                    "qty": 10,
                    "avg_cost_cents": 49000,
                    "opened_day_utc": DAY,
                    "last_transition_utc": f"{DAY}T00:00:00Z",
                    "last_transition_type": "OPEN",
                    "lifecycle_state": "OPEN",
                    "lifecycle_reason_code": "NATIVE_FILL_APPLIED",
                    "status": "OPEN",
                    "lots": [],
                    "reconciliation": {
                        "broker_position_present": True,
                        "broker_qty": "10",
                        "status": "MATCH",
                        "reason_codes": [],
                    },
                }
            ],
            "reconciliation": {
                "broker_statement_present": True,
                "broker_statement_path": "/tmp/broker.json",
                "cash_status": "MATCH",
                "cash_delta_cents": 0,
                "positions_status": "MATCH",
                "reason_codes": [],
                "position_mismatches": [],
            },
            "canonical_json_hash": "3" * 64,
        },
    )
    _write_json(
        sleeve_truth / "risk_v1" / "exposure_net_v1" / DAY / "exposure_net.v1.json",
        {
            "schema_id": "C2_EXPOSURE_NET_V1",
            "status": "OK",
            "portfolio": {
                "gross_notional_usd": "25000",
                "net_notional_usd": "25000",
                "capital_at_risk_cents": 2500000,
                "symbol_count": 1,
                "by_symbol": [
                    {
                        "symbol": "SPY",
                        "gross_notional_usd": "25000",
                        "net_notional_usd": "25000",
                        "capital_at_risk_cents": 2500000,
                        "sector": "Equity",
                    }
                ],
            },
        },
    )
    _write_json(
        sleeve_truth / "risk_v1" / "portfolio_governance_snapshot_v1" / DAY / "portfolio_governance_snapshot.v1.json",
        {
            "schema_id": "C2_PORTFOLIO_GOVERNANCE_SNAPSHOT_V1",
            "schema_version": 1,
            "day_utc": DAY,
            "produced_utc": f"{DAY}T00:00:00Z",
            "portfolio_snapshot_id": "4" * 64,
            "policy_hash": "5" * 64,
            "sleeve_registry_hash": "6" * 64,
            "risk_budget_total": 10000000,
            "risk_budget_effective": 7500000,
            "drawdown_scaling": "1.0",
            "correlation_compression": "0.85",
            "hard_stop_state": "PASS",
            "reserve_buffer": 2500000,
            "input_manifest": [{"type": "policy_manifest", "path": "/tmp/policy.json", "sha256": "7" * 64, "day_utc": None, "producer": "governance"}],
        },
    )

    payload = build_financial_state_view(DAY)

    assert payload["view_name"] == "financial_state"
    assert payload["contract_id"] == "financial_state_v1"
    assert payload["financial_status"] == "OK"
    assert payload["investable_summary"]["investable_assets_total_usd"] == 125000.0
    assert payload["liquidity_summary"]["cash_total_usd"] == 100000.0
    assert payload["reserve_summary"]["reserve_buffer_usd"] == 25000.0
    assert payload["account_rollup_summary"]["total_accounts"] == 1
    assert payload["account_rollups"][0]["full_account_number"] == "DUO847203"
    assert payload["account_rollups"][0]["buying_power_usd"] == 80000.0
    assert payload["holdings_rollup"]["total_holdings"] == 1
    assert payload["concentration_summary"]["top_symbol_exposures"][0]["symbol"] == "SPY"
    assert len(payload["authority_inputs"]) == 6
    assert payload["_metadata_validation"]["ok"] is True


def test_financial_state_view_fails_closed_when_required_reserve_input_is_missing(monkeypatch, tmp_path: Path) -> None:
    global_truth = tmp_path / "runtime" / "truth"
    sleeve_truth = tmp_path / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
    repo_root = tmp_path / "repo"

    monkeypatch.setattr("constellation_2.phaseL.ui_api.financial_state_read_model.GLOBAL_TRUTH_ROOT", global_truth)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.financial_state_read_model.SLEEVE_TRUTH_ROOT", sleeve_truth)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.financial_state_read_model.REPO_ROOT", repo_root)

    _write_json(
        repo_root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
        {"schema_id": "c2_ib_account_registry", "accounts": [{"account_id": "DUO847203", "environment": "PAPER"}]},
    )
    _write_json(
        global_truth / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {"schema_id": "C2_ACCOUNTING_NAV_V2", "produced_utc": f"{DAY}T00:00:00Z", "nav": {"nav_total": 125000, "cash_total": 100000, "gross_positions_value": 25000}},
    )
    _write_json(
        global_truth / "cash_ledger_v1" / "snapshots" / DAY / "cash_ledger_snapshot.v1.json",
        {"schema_id": "C2_CASH_LEDGER_SNAPSHOT_V1", "snapshot": {"account_id": "DUO847203", "cash_total_cents": 10000000, "nlv_total_cents": 12500000, "observed_at_utc": f"{DAY}T00:00:00Z"}},
    )
    _write_json(
        sleeve_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json",
        {"schema_id": "C2_POSITIONS_SNAPSHOT_V5", "produced_utc": f"{DAY}T00:00:00Z", "accounts": [{"account_id": "DUO847203", "currency": "USD", "cash_total_cents": 10000000}], "items": [], "reconciliation": {}},
    )
    _write_json(
        sleeve_truth / "risk_v1" / "exposure_net_v1" / DAY / "exposure_net.v1.json",
        {"schema_id": "C2_EXPOSURE_NET_V1", "portfolio": {"gross_notional_usd": "25000", "net_notional_usd": "25000", "capital_at_risk_cents": 2500000, "by_symbol": []}},
    )

    payload = build_financial_state_view(DAY)

    assert payload["contract_id"] == "financial_state_v1"
    assert payload["financial_status"] == "FAIL_CLOSED"
    assert payload["truth_state"] == "fail_closed"
    assert "PORTFOLIO_GOVERNANCE_SNAPSHOT_V1_FILE_NOT_FOUND" in payload["financial_warnings"]
    assert payload["investable_summary"]["investable_assets_total_usd"] is None


def test_command_and_portfolio_now_read_same_canonical_financial_authority() -> None:
    root = Path(__file__).resolve().parents[4]
    domain_client = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "domain_client" / "index.js"
    ).read_text(encoding="utf-8")
    pages = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
    ).read_text(encoding="utf-8")
    main_js = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "main.js"
    ).read_text(encoding="utf-8")

    assert 'query("/api/financial-state")' in domain_client
    command_section = pages.split("async function renderCommandPage", 1)[1].split("async function renderPortfolioPage", 1)[0]
    assert "async function renderPortfolioPage" in pages
    portfolio_section = pages.split("async function renderPortfolioPage", 1)[1].split("async function renderAdvisoryPage", 1)[0]
    assert "fetchFinancialState()" in command_section
    assert "fetchFinancialState()" in portfolio_section
    assert "fetchPositions()" not in portfolio_section
    assert "fetchOrders()" not in portfolio_section
    assert "fetchFinancialState()" in main_js
    assert "topPortfolioValue" in main_js
