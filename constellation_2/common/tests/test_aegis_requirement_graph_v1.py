from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.aegis_chatgpt_packet as packet  # noqa: E402
import ops.tools.run_aegis_day_v1 as day_run  # noqa: E402
import ops.tools.run_aegis_requirement_graph_v1 as graph  # noqa: E402
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402
from ops.tools import run_aegis_bod_prepare_v1 as bod  # noqa: E402


def _ctx(tmp_path: Path, day: str = "2026-04-29") -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(
        day_utc=day,
        environment="PAPER",
        truth_root=truth,
        execution_root=execution,
        runtime_root=runtime,
        operator_input_root=operator,
        ib_account="DU123456",
    )


def _write_intent(ctx: bod.BodContext, *, symbol: str = "SPY", option: bool = True) -> Path:
    path = ctx.execution_root / "intents_v1" / "snapshots" / ctx.day_utc / f"{symbol.lower()}.exposure_intent.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_id": "exposure_intent",
        "intent_id": f"intent_{symbol.lower()}_{ctx.day_utc}",
        "underlying": {"symbol": symbol},
        "exposure_type": "SHORT_VOL_DEFINED" if option else "EQUITY_LONG",
    }
    if option:
        payload["option"] = {"structure": "PUT", "direction": "SELL"}
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def _write_permission_diagnostic(ctx: bod.BodContext, symbol: str = "SPY") -> Path:
    path = (
        ctx.execution_root
        / "reports"
        / "options_chain_capture_ib_day_v1"
        / ctx.day_utc
        / f"ib_capture_{symbol}_{ctx.day_utc.replace('-', '')}T010203Z_test"
        / "options_chain_capture_diagnostic.v1.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "status": "FAIL",
                "error": "OPTIONS_CAPTURE_FAILED_ALL_MARKET_DATA_TYPES:market_data_type=3:NO_VALID_OPTION_QUOTES_CAPTURED",
                "ib_errors": [
                    {
                        "error_code": 10091,
                        "error_detail": "Part of requested market data requires additional subscription for API.SPY ARCA/TOP/ALL",
                    }
                ],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def test_spy_options_intent_creates_owned_market_data_requirements(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_intent(ctx, symbol="SPY", option=True)

    payload = graph.build_requirement_graph(ctx)
    market_nodes = [node for node in payload["requirements"] if node["owner_phase"] == "MARKET_DATA" and node["instrument"] == "SPY"]

    assert {node["required_artifact"] for node in market_nodes} >= {
        "underlying_spot",
        "option_chain",
        "bid_ask_quotes",
        "freshness_certificate",
        "options_snapshot_artifact",
    }
    assert all(node["source_type"] == "ACTIVE_INTENT" for node in market_nodes if node["instrument"] == "SPY")
    assert all(node["source_id"] == "intent_spy_2026-04-29" for node in market_nodes if node["instrument"] == "SPY")


def test_no_active_spy_intent_means_spy_options_not_required(tmp_path: Path) -> None:
    payload = graph.build_requirement_graph(_ctx(tmp_path))

    assert not [node for node in payload["requirements"] if node["instrument"] == "SPY"]


def test_default_spy_requirement_without_owner_is_not_created(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_intent(ctx, symbol="QQQ", option=True)

    payload = graph.build_requirement_graph(ctx)

    assert not [node for node in payload["requirements"] if node["instrument"] == "SPY"]


def test_missing_options_snapshot_points_to_active_intent_source(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_intent(ctx, symbol="SPY", option=True)

    payload = graph.build_requirement_graph(ctx)
    snapshot = next(node for node in payload["requirements"] if node["required_artifact"] == "options_snapshot_artifact")

    assert snapshot["status"] == "BLOCKED"
    assert snapshot["source_id"] == "intent_spy_2026-04-29"
    assert snapshot["blocker"] == "OPTIONS_CHAIN_SNAPSHOT_MISSING"
    assert snapshot["canonical_blocker"] == "OPTIONS_CHAIN_SNAPSHOT_MISSING"
    assert snapshot["schema_path"] == ""
    assert snapshot["freshness_policy"]
    assert snapshot["blocking_class"] == "HARD_BLOCKER"
    assert snapshot["dependencies"] == []


def test_ib_permission_failure_maps_to_operator_action(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_intent(ctx, symbol="SPY", option=True)
    _write_permission_diagnostic(ctx)

    payload = graph.build_requirement_graph(ctx)
    root = next(node for node in payload["requirements"] if node["owner_phase"] == "MARKET_DATA" and node["instrument"] == "SPY")

    assert root["owner_phase"] == "MARKET_DATA"
    assert root["blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
    assert root["source_id"] == "intent_spy_2026-04-29"
    assert "Restore IB market-data permissions" in root["operator_next_action"]


def test_missing_phasec_evidence_links_to_lifecycle_requirement(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_intent(ctx, symbol="SPY", option=True)
    payload = graph.build_requirement_graph(ctx)

    phasec = next(
        node
        for node in payload["requirements"]
        if node["required_artifact"] == "phasec_defined_risk_evidence" and node["source_type"] == "ACTIVE_INTENT"
    )

    assert phasec["owner_phase"] == "AUTHORIZATION"
    assert phasec["source_id"] == "intent_spy_2026-04-29"
    assert phasec["consumer"] == "authorization_artifacts_day_v1"
    assert phasec["producer_command"].startswith("python3 ops/tools/run_phasec_identity_materializer_day_v1.py")


def test_day_ledger_uses_market_data_supply_blocker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    phase_ctx = day_run.PhaseContext(
        day_utc="2026-04-29",
        environment="PAPER",
        truth_root=tmp_path / "truth",
        execution_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        runtime_root=tmp_path / "runtime",
        operator_input_root=tmp_path / "operator",
        ib_account="DU123456",
    )
    phase_ctx.truth_root.mkdir(parents=True)
    phase_ctx.execution_root.mkdir(parents=True)
    graph_path = day_run._requirement_graph_path(phase_ctx)
    graph_path.parent.mkdir(parents=True)
    graph_path.write_text(
        json.dumps(
            {
                "day_utc": "2026-04-29",
                "root_requirement": {
                    "owner_phase": "MARKET_DATA",
                    "requirement_id": "MARKET_DATA:intent_spy_2026-04-29:SPY:OPTIONS_SNAPSHOT",
                    "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
                },
            }
        ),
        encoding="utf-8",
    )
    supply_path = phase_ctx.truth_root / "reports" / "market_data_supply_v1" / phase_ctx.day_utc / "market_data_supply.v1.json"
    supply_path.parent.mkdir(parents=True)
    supply_path.write_text(
        json.dumps(
            {
                "status": "BLOCKED",
                "canonical_blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
                "requirements": [
                    {
                        "requirement_id": "MARKET_DATA:intent_spy_2026-04-29:SPY:OPTIONS_SNAPSHOT",
                        "source_type": "ACTIVE_INTENT",
                        "source_id": "intent_spy_2026-04-29",
                        "instrument": "SPY",
                        "data_type": "OPTIONS_SNAPSHOT",
                        "required": True,
                    }
                ],
                "provider_checks": [
                    {
                        "provider": "IBKR",
                        "instrument": "SPY",
                        "capability": "OPTIONS_BID_ASK_QUOTES",
                        "status": "UNAVAILABLE",
                        "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
                    }
                ],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(day_run.bod, "_run_child_with_retries", lambda *args, **kwargs: {"status": "BLOCKED", "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED", "duration_ms": 1})
    monkeypatch.setattr(day_run.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="", stderr=""))
    monkeypatch.setattr(day_run, "_run_steps", lambda *args, **kwargs: ([{"status": "BLOCKED", "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED"}], [str(supply_path)], ["OPTIONS_MARKET_DATA_PERMISSION_DENIED"]))

    row = day_run._phase_market_data(phase_ctx, {})

    assert row["canonical_blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
    assert "requirement_id=MARKET_DATA:intent_spy_2026-04-29:SPY:OPTIONS_SNAPSHOT" in row["blocker_detail"]
    assert "provider_capability=OPTIONS_BID_ASK_QUOTES" in row["blocker_detail"]


def test_packet_displays_requirement_graph_root_details(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    ledger = truth / "reports" / "aegis_day_run_v1" / "2026-04-29" / "day_run.v1.json"
    req = truth / "reports" / "aegis_requirement_graph_v1" / "2026-04-29" / "requirement_graph.v1.json"
    ledger.parent.mkdir(parents=True)
    req.parent.mkdir(parents=True)
    ledger.write_text(
        json.dumps(
            {
                "day_utc": "2026-04-29",
                "environment": "PAPER",
                "final_status": "NOT_READY",
                "canonical_phase": "MARKET_DATA",
                "canonical_blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
                "root_cause_chain": [],
                "downstream_consequences": [],
                "operator_next_action": "x",
            }
        ),
        encoding="utf-8",
    )
    req.write_text(
        json.dumps(
            {
                "day_utc": "2026-04-29",
                "status": "BLOCKED",
                "canonical_blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
                "root_requirement": {
                    "owner_phase": "MARKET_DATA",
                    "requirement_id": "REQ1",
                    "source_type": "ACTIVE_INTENT",
                    "source_id": "intent_spy_2026-04-29",
                    "producer_command": "python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc 2026-04-29",
                    "expected_path": "/runtime/options",
                    "operator_next_action": "Restore IB market-data permissions",
                },
            }
        ),
        encoding="utf-8",
    )
    roots = packet.RootResolution(truth, execution, tmp_path / "truth_sleeves", "test", "test", "")
    monkeypatch.setattr(
        packet,
        "_build_current_calendar_day_runtime_status",
        lambda _roots: packet.CurrentCalendarDayStatus("2026-04-29", "true", False, "x", "GRANTED", "NOT_READY", "LEGACY", "legacy", "legacy"),
    )
    monkeypatch.setattr(
        packet,
        "_build_latest_trading_day_evidence_status",
        lambda _roots: packet.LatestTradingDayEvidenceStatus(
            "2026-04-29", "NOT_READY", "LEGACY", "x", "x", "x", "x", "x", "x", "x", "x", "x", "x", "x",
            "MANUAL_MODE_READY", "MISSING_REQUIRED_DATA", "PRE_SUBMIT_BLOCKER", "MARKET_DATA_BLOCKED", "1", "",
            "OK", "0", "0", "BLOCKED", "{}", "DRY_RUN_LOCKED", "PAPER", "false", "NO_TRADES_CLOSED", "x", "x", "legacy"
        ),
    )

    section = packet._build_paper_status(roots).section

    assert "- requirement_graph_path: " in section
    assert "- requirement_id: REQ1" in section
    assert "- requirement_source_id: intent_spy_2026-04-29" in section
    assert "- requirement_operator_next_action: Restore IB market-data permissions" in section


def test_wrong_day_requirement_graph_is_rejected(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    path = truth / "reports" / "aegis_requirement_graph_v1" / "2026-04-29" / "requirement_graph.v1.json"
    path.parent.mkdir(parents=True)
    path.write_text('{"day_utc":"2026-04-28"}', encoding="utf-8")
    roots = packet.RootResolution(truth, tmp_path / "exec", tmp_path / "truth_sleeves", "test", "test", "")

    status = packet._load_requirement_graph_status(roots, "2026-04-29")

    assert status.canonical_blocker == "WRONG_DAY_REQUIREMENT_GRAPH"


def test_requirement_graph_generation_writes_only_runtime_artifact(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_intent(ctx, symbol="SPY", option=True)
    payload = graph.build_requirement_graph(ctx)
    path = graph.requirement_graph_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    graph._write_json(path, payload)

    assert path.exists()
    assert str(path).startswith(str(ctx.truth_root))
    assert not str(path).startswith(str(REPO_ROOT))


def test_present_lifecycle_requirement_passes(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    target = ctx.truth_root / "reports" / "market_data_authority_v1" / ctx.day_utc / "market_data_authority.v1.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps({"day_utc": ctx.day_utc, "status": "PASS", "producer_contract_v1": {"deterministic_fingerprint": "test"}}), encoding="utf-8")

    payload = graph.build_requirement_graph(ctx)
    node = next(row for row in payload["requirements"] if row["source_id"] == "MARKET_DATA")

    assert node["status"] == "SATISFIED"
    assert node["canonical_blocker"] == ""


def test_operator_statement_is_human_supplied_external_input_with_provenance(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    statement = bod.resolve_operator_statement_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    statement.parent.mkdir(parents=True, exist_ok=True)
    statement.write_text(
        json.dumps(
            {
                "observed_at_utc": f"{ctx.day_utc}T00:00:00Z",
                "currency": "USD",
                "cash_total": "0.00",
                "nlv_total": "0.00",
                "account_id": ctx.ib_account,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    payload = graph.build_requirement_graph(ctx)
    node = next(row for row in payload["requirements"] if row["requirement_id"] == "BOD_INPUTS:operator_statement")

    assert node["status"] == "SATISFIED"
    assert node["canonical_blocker"] == ""
    assert node["human_supplied_external_input"] is True
    assert node["external_input_provenance_status"] == "PASS"


def test_operator_statement_external_input_requires_provenance_metadata(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    statement = bod.resolve_operator_statement_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    statement.parent.mkdir(parents=True, exist_ok=True)
    statement.write_text(json.dumps({"observed_at_utc": f"{ctx.day_utc}T00:00:00Z"}, sort_keys=True), encoding="utf-8")

    payload = graph.build_requirement_graph(ctx)
    node = next(row for row in payload["requirements"] if row["requirement_id"] == "BOD_INPUTS:operator_statement")

    assert node["status"] == "BLOCKED"
    assert node["canonical_blocker"] == "OPERATOR_INPUT_PROVENANCE_MISSING"
    assert "currency" in node["external_input_missing_metadata"]


def test_stale_lifecycle_requirement_is_explicit(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    target = ctx.truth_root / "reports" / "market_data_authority_v1" / ctx.day_utc / "market_data_authority.v1.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps({"day_utc": "2026-04-28", "status": "PASS"}), encoding="utf-8")

    payload = graph.build_requirement_graph(ctx)
    node = next(row for row in payload["requirements"] if row["source_id"] == "MARKET_DATA")

    assert node["status"] == "STALE"
    assert node["canonical_blocker"] == "STALE_ARTIFACT"
    assert "Regenerate stale artifact" in node["operator_next_action"]


def test_requirement_graph_report_validates_schema(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    monkeypatch.setattr(graph.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    path, payload = graph.run_requirement_graph_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert path.exists()
    assert payload["producer_contract_v1"]["deterministic_fingerprint"]
    validate_against_repo_schema_v1(
        payload,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_requirement_graph.v1.schema.json",
    )
