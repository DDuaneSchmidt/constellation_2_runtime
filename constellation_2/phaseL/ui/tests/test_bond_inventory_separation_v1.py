from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseL.ui.server import c2_ops_cockpit_status_v2_collector_v1 as collector
from constellation_2.phaseL.ui_api import sleeve_evaluation_read_model as sleeves_model
from ops.tools import run_c2_multi_sleeve_orchestrator_v1 as multi_orch
from ops.tools import run_bond_manual_monitor_v1 as bond_monitor


DAY = "2026-05-25"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _bond_registry() -> dict:
    return {
        "schema_id": "c2_sleeve_registry",
        "schema_version": "v1",
        "sleeves": [
            {
                "sleeve_id": "PRIMARY",
                "enabled": True,
                "mode": "PAPER",
                "execution_mode": "AUTO",
                "status": "PRODUCTION",
                "ib_account": "DUO847203",
                "symbols": ["SPY"],
                "truth_partition": "truth_sleeves/PRIMARY/PAPER",
            },
            {
                "sleeve_id": "BOND",
                "enabled": True,
                "mode": "PAPER",
                "execution_mode": "MANUAL",
                "status": "MANUAL_PRODUCTION",
                "asset_class": "FIXED_INCOME",
                "sleeve_type": "BOND",
                "display_name": "Bond Sleeve",
                "ui_visible": True,
                "allocator_visible": True,
                "automated_execution_enabled": False,
                "broker_execution_allowed": False,
                "advisory_only": True,
                "ib_account": "DUO847203",
                "symbols": ["AGG", "BND", "TLT"],
                "truth_partition": "truth_sleeves/BOND/PAPER",
            },
        ],
    }


def test_repo_registry_declares_bond_manual_advisory_visible() -> None:
    registry = json.loads(Path("governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").read_text(encoding="utf-8"))
    bond = next(row for row in registry["sleeves"] if row["sleeve_id"] == "BOND")

    assert bond["enabled"] is True
    assert bond["execution_mode"] == "MANUAL"
    assert bond["status"] == "MANUAL_PRODUCTION"
    assert bond["asset_class"] == "FIXED_INCOME"
    assert bond["ui_visible"] is True
    assert bond["allocator_visible"] is True
    assert bond["automated_execution_enabled"] is False
    assert bond["broker_execution_allowed"] is False


def test_bond_is_excluded_from_automated_paper_orchestrator() -> None:
    bond = next(row for row in _bond_registry()["sleeves"] if row["sleeve_id"] == "BOND")

    assert multi_orch.sleeve_is_automated_paper_execution(bond) is False
    rollup_entry = multi_orch.manual_sleeve_rollup_entry(bond)
    assert rollup_entry["status"] == "SKIP_MANUAL_ADVISORY"
    assert rollup_entry["reason_code"] == "SLEEVE_MANUAL_ADVISORY_NOT_AUTOMATED_PAPER"


def test_cockpit_sleeve_strip_includes_bond_without_automated_artifacts(monkeypatch, tmp_path: Path) -> None:
    registry_path = tmp_path / "C2_SLEEVE_REGISTRY_V1.json"
    _write_json(registry_path, _bond_registry())
    monkeypatch.setattr(collector, "SLEEVE_REGISTRY", registry_path)
    monkeypatch.setattr(
        collector,
        "_load_sleeve_policy",
        lambda: [
            {
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "display_name": "Trend Sleeve",
                "engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                "priority_rank": 10,
                "max_capital_at_risk_cents": 120000,
            }
        ],
    )
    monkeypatch.setattr(collector, "_active_engine_ids_for_day", lambda truth_root, day: [])

    rows, warnings = collector._build_sleeve_strip_rows(
        truth_root=tmp_path,
        day=DAY,
        mode_from_attempt="PAPER",
        primary_account="DUO847203",
        fallback_rows=[],
    )

    bond = next(row for row in rows if row["sleeve_id"] == "BOND")
    assert not any("BOND" in warning for warning in warnings)
    assert bond["name"] == "Bond Sleeve"
    assert bond["execution_mode"] == "MANUAL"
    assert bond["execution_label"] == "Manual execution"
    assert bond["advisory_label"] == "Advisory only"
    assert bond["broker_execution_allowed"] is False
    assert bond["automated_execution_allowed"] is False


def test_sleeve_evaluation_payload_includes_bond_manual_even_without_readiness(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    truth_root = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_json(repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json", _bond_registry())
    _write_json(
        repo_root / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json",
        {"schema_id": "C2_CAPITAL_AUTHORITY_POLICY_V1", "sleeves": []},
    )
    monkeypatch.setattr(sleeves_model, "REPO_ROOT", repo_root)
    monkeypatch.setattr(sleeves_model, "GLOBAL_TRUTH_ROOT", truth_root)
    monkeypatch.setattr(sleeves_model, "SLEEVE_TRUTH_ROOT", sleeve_truth)

    payload = sleeves_model.build_sleeve_evaluation_view(DAY)
    bond = next(row for row in payload["sleeves"] if row["sleeve_id"] == "BOND")

    assert bond["display_name"] == "Bond Sleeve"
    assert bond["execution_mode"] == "MANUAL"
    assert bond["manual_execution_only"] is True
    assert bond["advisory_only"] is True
    assert bond["readiness_diagnostic_code"] == "MANUAL_ADVISORY_NO_AUTOMATED_READINESS_REQUIRED"
    assert bond["readiness_grade_1_to_7"] is None
    assert bond["recommendation"]["recommendation_state"] == "unavailable"


def test_bond_latest_manual_recommendation_surfaces_when_artifact_exists(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    truth_root = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_json(repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json", _bond_registry())
    _write_json(
        repo_root / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json",
        {"schema_id": "C2_CAPITAL_AUTHORITY_POLICY_V1", "sleeves": []},
    )
    _write_json(
        truth_root / "reports/bond_sleeve_recommendation_v2" / DAY / "bond_sleeve_recommendation.v2.json",
        {
            "schema_id": "bond_sleeve_recommendation",
            "schema_version": "v2",
            "day_utc": DAY,
            "produced_utc": "2026-05-25T14:30:00Z",
            "sleeve_id": "BOND",
            "recommendation_state": "REBALANCE_REQUIRED",
            "operator_next_step": "Review ladder manually.",
            "manual_execution_only": True,
            "advisory_only": True,
            "broker_execution_allowed": False,
        },
    )
    monkeypatch.setattr(sleeves_model, "REPO_ROOT", repo_root)
    monkeypatch.setattr(sleeves_model, "GLOBAL_TRUTH_ROOT", truth_root)
    monkeypatch.setattr(sleeves_model, "SLEEVE_TRUTH_ROOT", sleeve_truth)

    payload = sleeves_model.build_sleeve_evaluation_view(DAY)
    bond = next(row for row in payload["sleeves"] if row["sleeve_id"] == "BOND")

    assert bond["recommendation"]["recommendation_state"] == "REBALANCE_REQUIRED"
    assert bond["latest_evaluation"]["produced_utc"] == "2026-05-25T14:30:00Z"
    assert bond["readiness_grade_next_step"] == "Review ladder manually."


def test_cockpit_bond_view_reads_latest_manual_recommendation(monkeypatch, tmp_path: Path) -> None:
    registry_path = tmp_path / "C2_SLEEVE_REGISTRY_V1.json"
    truth_root = tmp_path / "truth"
    artifact_path = truth_root / "reports/bond_sleeve_recommendation_v2" / DAY / "bond_sleeve_recommendation.v2.json"
    pointer_path = artifact_path.parent / "display_head_pointer.v1.json"
    _write_json(registry_path, _bond_registry())
    _write_json(
        artifact_path,
        {
            "schema_id": "bond_sleeve_recommendation",
            "schema_version": "v2",
            "day_utc": DAY,
            "produced_utc": "2026-05-25T15:00:00Z",
            "sleeve_id": "BOND",
            "recommendation_state": "MANUAL_REVIEW",
            "operator_next_step": "Review duration ladder manually.",
            "manual_execution_only": True,
            "advisory_only": True,
            "broker_execution_allowed": False,
        },
    )
    _write_json(
        pointer_path,
        {
            "schema_id": "C2_BOND_DISPLAY_HEAD_POINTER_V1",
            "status": "PASS",
            "day_utc": DAY,
            "produced_utc": "2026-05-25T15:01:00Z",
            "points_to": str(artifact_path),
            "points_to_sha256": "0" * 64,
        },
    )
    (artifact_path.parent / "canonical_pointer_index.v1.jsonl").write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(collector, "SLEEVE_REGISTRY", registry_path)
    monkeypatch.setattr(collector, "GLOBAL_RUNTIME_TRUTH_ROOT", truth_root)
    monkeypatch.setattr(collector, "BOND_POSITIONS_INPUT_PATH", tmp_path / "missing_positions.json")
    monkeypatch.setattr(collector, "BOND_POLICY_INPUT_PATH", tmp_path / "missing_policy.json")
    monkeypatch.setattr(collector, "BOND_MACRO_POLICY_INPUT_PATH", tmp_path / "missing_macro.json")

    view, _warnings, _missing, _source_paths, _mtimes = collector._collect_bond_sleeve_view(DAY)

    assert view["sleeve_id"] == "BOND"
    assert view["display_name"] == "Bond Sleeve"
    assert view["available"] is True
    assert view["execution_label"] == "Manual execution"
    assert view["advisory_label"] == "Advisory only"
    assert view["broker_execution_allowed"] is False
    assert view["automated_execution_allowed"] is False
    assert view["latest_evaluation"]["present"] is True
    assert view["latest_evaluation"]["recommendation_state"] == "MANUAL_REVIEW"


def test_operator_shell_renders_bond_manual_labels_without_trade_button() -> None:
    source = Path("constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    sleeves_section = source.split("async function renderSleevesPage", 1)[1].split("async function renderTaxPage", 1)[0]
    operations_section = source.split("async function renderOperationsPage", 1)[1].split("function renderSystemActions", 1)[0]

    assert "Bond Sleeve" in operations_section
    assert "Manual execution" in sleeves_section
    assert "Manual execution" in operations_section
    assert "Advisory only" in sleeves_section
    assert "Advisory only" in operations_section
    assert "Execute trade" not in sleeves_section


def test_advisor_bond_positions_input_contains_core_symbols_and_cspf_watchlist() -> None:
    positions_path = Path("constellation_2/operator_inputs/bond_sleeve/bond_positions_v1.json")
    policy_path = Path("constellation_2/operator_inputs/bond_sleeve/bond_sleeve_policy_v1.json")
    positions = json.loads(positions_path.read_text(encoding="utf-8"))
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    registry = json.loads(Path("governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").read_text(encoding="utf-8"))
    bond_registry = next(row for row in registry["sleeves"] if row["sleeve_id"] == "BOND")

    core_symbols = {row["symbol"] for row in positions["positions"]}
    watchlist_symbols = {row["symbol"] for row in positions["watchlist_positions"]}

    assert core_symbols == {"JMST", "SUB", "VGSH", "DFGFX", "DFGBX"}
    assert "CSPF" not in core_symbols
    assert watchlist_symbols == {"CSPF"}
    assert {"JMST", "SUB", "VGSH", "DFGFX", "DFGBX"}.issubset(set(bond_registry["symbols"]))
    assert "CSPF" in set(bond_registry["excluded_or_watchlist_symbols"])
    assert policy["core_symbols"] == ["DFGBX", "DFGFX", "JMST", "SUB", "VGSH"]
    assert policy["excluded_or_watchlist_symbols"] == ["CSPF"]

    expected = {
        "JMST": ("FIXED_INCOME", "municipal", "ultra_short", "tax_exempt_muni"),
        "SUB": ("FIXED_INCOME", "municipal", "short", "tax_exempt_muni"),
        "VGSH": ("FIXED_INCOME", "treasury", "short", "federal_taxable_state_exempt_interest"),
        "DFGFX": ("FIXED_INCOME", "global_fixed_income", "short", "taxable"),
        "DFGBX": ("FIXED_INCOME", "global_fixed_income", "intermediate_short", "taxable"),
    }
    by_symbol = {row["symbol"]: row for row in positions["positions"]}
    for symbol, (asset_class, bond_type, duration_bucket, tax_treatment) in expected.items():
        row = by_symbol[symbol]
        assert row["asset_class"] == asset_class
        assert row["bond_type"] == bond_type
        assert row["duration_bucket"] == duration_bucket
        assert row["tax_treatment"] == tax_treatment
        assert row["execution_mode"] == "MANUAL"
        assert row["sleeve"] == "BOND"
        assert row["review_status"] == "OK"

    cspf = positions["watchlist_positions"][0]
    assert cspf["symbol"] == "CSPF"
    assert cspf["sleeve"] == "INCOME_HYBRID"
    assert cspf["bond_type"] == "preferred_income"
    assert cspf["duration_bucket"] == "not_core_bond"
    assert cspf["review_status"] == "NEEDS_REVIEW"
    assert "Do not include in core BOND safety allocation" in cspf["note"]


def test_cockpit_bond_holdings_payload_includes_advisor_positions(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    artifact_path = truth_root / "reports/bond_sleeve_recommendation_v2" / DAY / "bond_sleeve_recommendation.v2.json"
    _write_json(
        artifact_path,
        {
            "schema_id": "bond_sleeve_recommendation",
            "schema_version": "v2",
            "day_utc": DAY,
            "produced_utc": "2026-05-25T16:00:00Z",
            "sleeve_id": "BOND",
            "recommendation_state": "NEEDS_REVIEW",
            "manual_execution_only": True,
            "advisory_only": True,
            "broker_execution_allowed": False,
        },
    )
    _write_json(
        artifact_path.parent / "display_head_pointer.v1.json",
        {
            "schema_id": "C2_BOND_DISPLAY_HEAD_POINTER_V1",
            "status": "PASS",
            "day_utc": DAY,
            "produced_utc": "2026-05-25T16:00:01Z",
            "points_to": str(artifact_path),
            "points_to_sha256": "0" * 64,
        },
    )
    (artifact_path.parent / "canonical_pointer_index.v1.jsonl").write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr(collector, "GLOBAL_RUNTIME_TRUTH_ROOT", truth_root)
    monkeypatch.setattr(collector, "BOND_POSITIONS_INPUT_PATH", Path("constellation_2/operator_inputs/bond_sleeve/bond_positions_v1.json").resolve())
    monkeypatch.setattr(collector, "BOND_POLICY_INPUT_PATH", Path("constellation_2/operator_inputs/bond_sleeve/bond_sleeve_policy_v1.json").resolve())
    monkeypatch.setattr(collector, "BOND_MACRO_POLICY_INPUT_PATH", Path("constellation_2/operator_inputs/bond_sleeve/bond_macro_policy_v1.json").resolve())

    view, warnings, _missing, _source_paths, _mtimes = collector._collect_bond_sleeve_view(DAY)
    holdings = view["operator_holdings"]

    assert holdings["positions_count"] == 5
    assert holdings["watchlist_count"] == 1
    assert {row["symbol"] for row in holdings["positions"]} == {"JMST", "SUB", "VGSH", "DFGFX", "DFGBX"}
    assert holdings["watchlist_positions"][0]["symbol"] == "CSPF"
    assert holdings["watchlist_positions"][0]["sleeve"] == "INCOME_HYBRID"
    assert holdings["watchlist_positions"][0]["review_status"] == "NEEDS_REVIEW"
    assert view["latest_evaluation"]["recommendation_state"] == "NEEDS_REVIEW"
    assert view["manual_execution_only"] is True
    assert view["broker_execution_allowed"] is False
    assert not [warning for warning in warnings if "BOND_POSITION" in warning]


def test_bond_manual_monitor_recommendation_carries_current_operator_holdings() -> None:
    artifact = bond_monitor.build_manual_recommendation(DAY, "2026-05-25T16:30:00Z")
    holdings = artifact["current_operator_holdings"]

    assert artifact["execution_mode"] == "MANUAL"
    assert artifact["manual_execution_only"] is True
    assert artifact["advisory_only"] is True
    assert artifact["broker_execution_allowed"] is False
    assert artifact["recommendation_state"] == "NEEDS_REVIEW"
    assert holdings["core_symbols"] == ["DFGBX", "DFGFX", "JMST", "SUB", "VGSH"]
    assert holdings["watchlist_symbols"] == ["CSPF"]
    assert holdings["needs_review_symbols"] == ["CSPF"]


def test_operator_shell_bond_card_renders_holdings_and_no_execute_trade_button() -> None:
    source = Path("constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    operations_section = source.split("async function renderOperationsPage", 1)[1].split("function renderSystemActions", 1)[0]

    assert "bondCoreHoldings" in operations_section
    assert "bondWatchlistHoldings" in operations_section
    assert "Core holdings" in operations_section
    assert "Watchlist holdings" in operations_section
    assert "Manual execution" in operations_section
    assert "Advisory only" in operations_section
    assert "Execute trade" not in operations_section
