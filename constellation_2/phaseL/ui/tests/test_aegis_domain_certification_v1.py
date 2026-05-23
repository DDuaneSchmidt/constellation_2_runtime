from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.domain_source_builders_v1 import build_domain_source_artifact_v1
from ops.aegis.market_calendar.session_calendar_v1 import session_classification_v1
from ops.tools.validate_domain_source_v1 import validate_domain_source_v1
from ops.aegis.domain_certification_v1 import (
    build_domain_certification_report_v1,
    write_domain_certification_report_v1,
)
from ops.aegis.domain_repair_orchestrator_v1 import run_domain_repair_orchestration_v1
from ops.aegis.repair_center_projection_v1 import build_repair_center_projection_v1
from ops.aegis.operator_state.runtime_timeline_projection_v1 import build_runtime_timeline_projection_v1


ROOT = Path(__file__).resolve().parents[4]


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def _write_calendar(root: Path, day: str) -> None:
    _write_json(
        root / "market_calendar_v1" / "NYSE" / f"{day[:4]}.jsonl",
        {
            "day_utc": day,
            "exchange": "NYSE",
            "is_trading_session": True,
            "source_hash": "a" * 64,
            "source_name": "test_calendar",
        },
    )


def _write_market_inputs(root: Path, day: str, *, conflict: bool = False) -> None:
    provider_results = [{"provider": "TEST_PRIMARY", "status": "OK", "fetched_symbols": ["SPY", "AAPL"], "missing_symbols": ["VIX"]}]
    if conflict:
        provider_results.append({"provider": "TEST_SECONDARY", "status": "CONFLICTED", "failure_reason": "PROVIDER_CONFLICT", "fetched_symbols": ["SPY"], "missing_symbols": []})
    _write_json(
        root / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json",
        {
            "schema_id": "market_data_inputs",
            "day_utc": day,
            "generated_at_utc": f"{day}T20:00:00Z",
            "provider_results": provider_results,
            "input_records": [
                {
                    "data_item_id": "market.price.SPY",
                    "symbol": "SPY",
                    "validation_status": "VALID",
                    "source_vendor": "TEST_PRIMARY",
                    "raw_source_hash": "b" * 64,
                },
                {
                    "data_item_id": "market.price.AAPL",
                    "symbol": "AAPL",
                    "validation_status": "VALID",
                    "source_vendor": "TEST_PRIMARY",
                    "raw_source_hash": "c" * 64,
                },
                {
                    "data_item_id": "market.volatility.VIX",
                    "symbol": "VIX",
                    "validation_status": "UNAVAILABLE_EXTERNAL_SOURCE",
                    "source_vendor": "CBOE",
                    "reason": "CBOE_VIX_ROW_NOT_FOUND",
                },
            ],
        },
    )


def test_domain_certification_isolates_missing_macro_to_dependent_sleeve(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)

    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day, generated_at_utc=f"{day}T21:00:00Z")
    domains = {row["domain_id"]: row for row in report["domains"]}
    sleeves = {row["sleeve_id"]: row for row in report["sleeve_domain_statuses"]}

    assert domains["US_EQUITIES_INTRADAY"]["certification_status"] == "CERTIFIED"
    assert domains["MACRO_CALENDAR"]["certification_status"] == "DELAYED"
    assert sleeves["C2_TREND_EQ_PRIMARY_V1"]["sleeve_certification_status"] == "READY_WITH_WARNINGS"
    assert "MACRO_CALENDAR" not in sleeves["C2_TREND_EQ_PRIMARY_V1"]["blocking_domains"]
    assert sleeves["C2_EVENT_DISLOCATION_V1"]["sleeve_certification_status"] == "BLOCKED"
    assert sleeves["C2_EVENT_DISLOCATION_V1"]["blocking_domains"] == ["MACRO_CALENDAR"]


def test_domain_provider_conflict_marks_domain_conflicted(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day, conflict=True)

    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    domains = {row["domain_id"]: row for row in report["domains"]}

    assert domains["US_EQUITIES_INTRADAY"]["certification_status"] == "CONFLICTED"
    assert domains["US_EQUITIES_INTRADAY"]["quorum_state"] == "CONFLICTED"


def test_single_provider_risk_is_recorded_without_false_failure(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)

    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    domain = next(row for row in report["domains"] if row["domain_id"] == "US_EQUITIES_INTRADAY")

    assert domain["certification_status"] == "CERTIFIED"
    assert domain["single_provider_risk"] is True
    assert domain["quorum_state"] == "SINGLE_PROVIDER_RISK_RECORDED"


def test_optional_domain_missing_degrades_confidence_not_certification(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)

    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    sleeves = {row["sleeve_id"]: row for row in report["sleeve_domain_statuses"]}

    assert sleeves["C2_MEAN_REVERSION_EQ_V1"]["sleeve_certification_status"] == "READY_WITH_WARNINGS"
    assert "VOLATILITY" in sleeves["C2_MEAN_REVERSION_EQ_V1"]["optional_degraded_domains"]
    assert "VOLATILITY" not in sleeves["C2_MEAN_REVERSION_EQ_V1"]["blocking_domains"]


def test_delayed_domains_have_concrete_repair_guidance(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)

    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    delayed_domains = [row for row in report["domains"] if row["certification_status"] == "DELAYED"]
    plans = {row["domain_id"]: row for row in report["domain_repair_actions"]}

    assert set(plans) == {row["domain_id"] for row in delayed_domains}
    for domain in delayed_domains:
        plan = plans[domain["domain_id"]]
        assert plan["repair_button_label"]
        assert plan["repair_action"]
        assert plan["repair_steps"]
        assert plan["required_source_artifact"]["label"]
        assert plan["last_attempt"]
        assert plan["next_retry"]
        assert domain["repair_plan"]["domain_id"] == domain["domain_id"]
        assert plan["safety"]["broker_submit_transmit_allowed"] is False
        assert plan["safety"]["broker_execution_allowed"] is False
        assert plan["safety"]["autonomous_execution_allowed"] is False
        assert plan["safety"]["trade_advice_allowed"] is False


def test_source_missing_repair_guidance_is_specific_not_vague_vendor_wording(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)

    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    plans = {row["domain_id"]: row for row in report["domain_repair_actions"]}

    assert plans["US_EQUITIES_EOD"]["reason"] == "FINAL_EOD_ARTIFACT_MISSING"
    assert plans["US_EQUITIES_EOD"]["repair_mode"] == "AUTO_REBUILD"
    assert plans["US_EQUITIES_EOD"]["repair_action"] == "Run/retry EOD market-data artifact build"
    assert "Validate generated final EOD artifact" in plans["US_EQUITIES_EOD"]["repair_steps"]
    assert plans["US_EQUITIES_EOD"]["automatic_repair_available"] is True
    assert plans["US_EQUITIES_EOD"]["operator_action_required"] is False

    assert plans["MACRO_CALENDAR"]["next_retry"] == "Source setup required"
    assert plans["MACRO_CALENDAR"]["repair_action"] == "Open/upload/configure macro calendar source"
    assert "C2_EVENT_DISLOCATION_V1" in plans["MACRO_CALENDAR"]["affected_sleeves"]
    assert plans["EARNINGS_EVENTS"]["repair_action"] == "Open/upload/configure earnings events source"
    assert plans["CORPORATE_ACTIONS"]["repair_action"] == "Open/upload/configure corporate actions source"

    source_missing_blob = json.dumps([plans["MACRO_CALENDAR"], plans["EARNINGS_EVENTS"], plans["CORPORATE_ACTIONS"]]).lower()
    assert "vendor" not in source_missing_blob


def test_domain_certification_report_writes_domain_snapshots(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)
    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    paths = write_domain_certification_report_v1(truth_root=tmp_path, day_utc=day, payload=report)

    assert Path(paths["json"]).exists()
    assert Path(paths["txt"]).exists()
    assert Path(paths["domain:US_EQUITIES_INTRADAY"]).exists()


def test_runtime_timeline_exposes_domain_certification_grid(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_calendar(tmp_path, day)
    _write_market_inputs(tmp_path, day)
    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    write_domain_certification_report_v1(truth_root=tmp_path, day_utc=day, payload=report)

    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot={"current_day_status": {"intraday_operational_ready": True}},
        now_utc=f"{day}T21:00:00Z",
        repo_root=ROOT,
    )

    assert payload["domain_certification"]["summary"]["certified_domains"] >= 1
    assert any(row["domain_id"] == "MACRO_CALENDAR" for row in payload["domain_certification_grid"])
    assert any(row["domain_id"] == "MACRO_CALENDAR" for row in payload["domain_repair_actions"])
    assert any(alert["alert_type"] == "domain_certification_blocker" for alert in payload["alerts"])



def test_us_equities_eod_auto_rebuilds_and_certifies(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)

    def fake_runner(args: list[str]) -> dict:
        if "build_domain_source_v1.py" in " ".join(args) or "refresh_aegis_market_data_v1.py" in " ".join(args):
            _write_json(
                tmp_path / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json",
                {
                    "schema_id": "aegis_market_data",
                    "day_utc": day,
                    "status": "CURRENT",
                    "validation_status": "VALID",
                    "final_eod_certification_status": "VALID",
                    "requested_symbols": ["SPY", "AAPL"],
                    "fetched_symbols": ["SPY", "AAPL"],
                    "provider_results": [{"provider": "TEST_EOD", "status": "OK", "fetched_symbols": ["SPY", "AAPL"]}],
                },
            )
        return {"args": args, "exit_code": 0, "stdout_tail": "ok", "stderr_tail": "", "started_at_utc": f"{day}T22:00:00Z", "completed_at_utc": f"{day}T22:00:01Z"}

    result = run_domain_repair_orchestration_v1(truth_root=tmp_path, day_utc=day, domain_id="US_EQUITIES_EOD", execute=True, runner=fake_runner)
    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    domains = {row["domain_id"]: row for row in report["domains"]}

    assert result["results"][0]["status"] == "COMPLETED"
    assert domains["US_EQUITIES_EOD"]["certification_status"] == "CERTIFIED"
    assert Path(result["lifecycle_paths"]["json"]).exists()
    lifecycle = result["lifecycle"]["latest_by_domain"]["US_EQUITIES_EOD"]
    assert lifecycle["repair_stage"] == "COMPLETED"


def test_missing_external_sources_are_setup_required_not_fake_repaired(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)

    result = run_domain_repair_orchestration_v1(truth_root=tmp_path, day_utc=day, domain_id="MACRO_CALENDAR", execute=True)
    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    domains = {row["domain_id"]: row for row in report["domains"]}

    assert result["results"][0]["status"] == "SOURCE_SETUP_REQUIRED"
    assert domains["MACRO_CALENDAR"]["certification_status"] == "DELAYED"
    assert domains["MACRO_CALENDAR"]["repair_plan"]["repair_mode"] == "SOURCE_SETUP_REQUIRED"


def test_adding_macro_source_then_recertifying_clears_required_blocker(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)
    _write_json(
        tmp_path / "reports" / "macro_calendar_v1" / day / "macro_calendar.v1.json",
        {
            "schema_id": "macro_calendar_v1",
            "day_utc": day,
            "validation_status": "VALID",
            "events": [{"date": day, "event_name": "Test macro event", "country": "US", "impact": "LOW"}],
        },
    )

    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    domains = {row["domain_id"]: row for row in report["domains"]}
    sleeves = {row["sleeve_id"]: row for row in report["sleeve_domain_statuses"]}

    assert domains["MACRO_CALENDAR"]["certification_status"] == "CERTIFIED"
    assert sleeves["C2_EVENT_DISLOCATION_V1"]["sleeve_certification_status"] == "READY_WITH_WARNINGS"
    assert "MACRO_CALENDAR" not in sleeves["C2_EVENT_DISLOCATION_V1"]["blocking_domains"]


def test_repair_lifecycle_persists_after_refresh(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)
    run_domain_repair_orchestration_v1(truth_root=tmp_path, day_utc=day, domain_id="MACRO_CALENDAR", execute=True)

    projection = build_repair_center_projection_v1(truth_root=tmp_path, day_utc=day)
    macro = next(row for row in projection["repair_items"] if row["domain_id"] == "MACRO_CALENDAR")

    assert macro["status"] == "SOURCE_SETUP_REQUIRED"
    assert macro["section_id"] == "source_setup_required"
    assert projection["domain_repair_lifecycle"]["latest_by_domain"]["MACRO_CALENDAR"]["repair_stage"] == "SOURCE_SETUP_REQUIRED"



def test_domain_source_builder_returns_setup_required_without_config(tmp_path: Path, monkeypatch) -> None:
    day = "2026-05-21"
    monkeypatch.delenv("AEGIS_MACRO_CALENDAR_SOURCE_FILE", raising=False)

    result = build_domain_source_artifact_v1(truth_root=tmp_path, day_utc=day, domain_id="MACRO_CALENDAR")

    assert result["ok"] is False
    assert result["result_status"] == "SOURCE_SETUP_REQUIRED"
    assert "AEGIS_MACRO_CALENDAR_SOURCE_FILE" in result["required_config_keys"]
    assert "macro_calendar.v1.json" in result["required_output_path"]
    assert "event_name" in result["required_fields"]


def test_macro_calendar_builder_writes_valid_artifact_and_certifies(tmp_path: Path, monkeypatch) -> None:
    day = "2026-05-21"
    source = tmp_path / "incoming" / "macro.json"
    _write_json(
        source,
        {
            "events": [
                {
                    "event_name": "FOMC Minutes",
                    "time": f"{day}T18:00:00Z",
                    "country": "US",
                    "importance": "HIGH",
                    "source": "TEST_MACRO_FEED",
                }
            ]
        },
    )
    monkeypatch.setenv("AEGIS_MACRO_CALENDAR_SOURCE_FILE", str(source))

    build = build_domain_source_artifact_v1(truth_root=tmp_path, day_utc=day, domain_id="MACRO_CALENDAR")
    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    domains = {row["domain_id"]: row for row in report["domains"]}

    assert build["ok"] is True
    assert Path(build["artifact_path"]).exists()
    assert domains["MACRO_CALENDAR"]["certification_status"] == "CERTIFIED"


def test_earnings_and_corporate_action_builders_validate_required_fields(tmp_path: Path, monkeypatch) -> None:
    day = "2026-05-21"
    earnings = tmp_path / "incoming" / "earnings.json"
    actions = tmp_path / "incoming" / "actions.json"
    _write_json(
        earnings,
        {
            "events": [
                {
                    "symbol": "NVDA",
                    "company": "NVIDIA Corporation",
                    "report_date": day,
                    "report_time": "AFTER_MARKET",
                    "confirmed_or_estimated": "CONFIRMED",
                    "source": "TEST_EARNINGS_FEED",
                }
            ]
        },
    )
    _write_json(
        actions,
        {
            "actions": [
                {
                    "symbol": "NVDA",
                    "action_type": "DIVIDEND",
                    "effective_date": day,
                    "source": "TEST_CORP_ACTION_FEED",
                }
            ]
        },
    )
    monkeypatch.setenv("AEGIS_EARNINGS_EVENTS_SOURCE_FILE", str(earnings))
    monkeypatch.setenv("AEGIS_CORPORATE_ACTIONS_SOURCE_FILE", str(actions))

    earnings_result = build_domain_source_artifact_v1(truth_root=tmp_path, day_utc=day, domain_id="EARNINGS_EVENTS")
    actions_result = build_domain_source_artifact_v1(truth_root=tmp_path, day_utc=day, domain_id="CORPORATE_ACTIONS")

    assert earnings_result["ok"] is True
    assert actions_result["ok"] is True
    assert validate_domain_source_v1(truth_root=tmp_path, day_utc=day, domain_id="EARNINGS_EVENTS")["validation_status"] == "VALID"
    assert validate_domain_source_v1(truth_root=tmp_path, day_utc=day, domain_id="CORPORATE_ACTIONS")["validation_status"] == "VALID"


def test_us_equities_eod_builder_writes_canonical_artifact_and_certifies(tmp_path: Path, monkeypatch) -> None:
    day = "2026-05-21"
    source = tmp_path / "incoming" / "final_eod.json"
    _write_json(
        source,
        {
            "schema_id": "provider_final_eod",
            "day_utc": day,
            "market_session_date": day,
            "status": "CURRENT",
            "validation_status": "VALID",
            "final_eod_certification_status": "VALID",
            "requested_symbols": ["SPY", "AAPL"],
            "fetched_symbols": ["SPY", "AAPL"],
            "symbols": {
                "SPY": {"market_session_date": day, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100},
                "AAPL": {"market_session_date": day, "open": 3, "high": 4, "low": 3, "close": 4, "volume": 200},
            },
            "provider_results": [{"provider": "TEST_EOD", "status": "OK", "fetched_symbols": ["SPY", "AAPL"]}],
        },
    )
    monkeypatch.setenv("AEGIS_FINAL_EOD_MARKET_DATA_SOURCE_FILE", str(source))
    monkeypatch.setattr("ops.aegis.domain_source_builders_v1._required_eod_symbols_v1", lambda day_utc: ["SPY", "AAPL"])

    build = build_domain_source_artifact_v1(truth_root=tmp_path, day_utc=day, domain_id="US_EQUITIES_EOD")
    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    domains = {row["domain_id"]: row for row in report["domains"]}

    assert build["ok"] is True
    assert "final_eod_market_data_v1" in build["artifact_path"]
    assert Path(build["artifact_path"]).exists()
    assert domains["US_EQUITIES_EOD"]["certification_status"] == "CERTIFIED"



def test_domain_repair_orchestrator_builds_configured_source_and_recources_certification(tmp_path: Path, monkeypatch) -> None:
    day = "2026-05-21"
    _write_market_inputs(tmp_path, day)
    source = tmp_path / "incoming" / "macro.json"
    _write_json(
        source,
        {
            "events": [
                {
                    "event_name": "Payrolls",
                    "time": f"{day}T12:30:00Z",
                    "country": "US",
                    "importance": "HIGH",
                    "source": "TEST_MACRO_FEED",
                }
            ]
        },
    )
    monkeypatch.setenv("AEGIS_MACRO_CALENDAR_SOURCE_FILE", str(source))

    result = run_domain_repair_orchestration_v1(truth_root=tmp_path, day_utc=day, domain_id="MACRO_CALENDAR", execute=True)
    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    domains = {row["domain_id"]: row for row in report["domains"]}

    assert result["results"][0]["status"] == "COMPLETED"
    assert domains["MACRO_CALENDAR"]["certification_status"] == "CERTIFIED"
    assert (tmp_path / "reports" / "macro_calendar_v1" / day / "macro_calendar.v1.json").exists()


def test_may_22_2026_equity_calendar_regular_close_bonds_early_close() -> None:
    equities = session_classification_v1(domain_id="US_EQUITIES_EOD", day_utc="2026-05-22")
    rates = session_classification_v1(domain_id="RATES_BONDS", day_utc="2026-05-22")

    assert equities["calendar"] == "NYSE/NASDAQ"
    assert equities["session_type"] == "REGULAR"
    assert equities["close_time_local"] == "16:00:00"
    assert equities["sifma_fixed_income_early_close_applied"] is False
    assert rates["calendar"] == "SIFMA_FIXED_INCOME"
    assert rates["session_type"] == "EARLY_CLOSE"
    assert rates["close_time_local"] == "14:00:00"
    assert rates["sifma_fixed_income_early_close_applied"] is True


def test_us_equities_eod_incomplete_coverage_writes_precise_rejected_artifact(tmp_path: Path, monkeypatch) -> None:
    day = "2026-05-22"
    source = tmp_path / "incoming" / "partial_final_eod.json"
    _write_json(
        source,
        {
            "schema_id": "aegis_market_data",
            "day_utc": day,
            "market_session_date": day,
            "status": "STALE",
            "validation_status": "PARTIAL_DATA_AVAILABLE",
            "requested_symbols": ["SPY", "QQQ", "IWM"],
            "fetched_symbols": ["SPY", "QQQ"],
            "final_eod_symbols": ["SPY"],
            "stale_symbols": ["QQQ"],
            "symbols": {
                "SPY": {"market_session_date": day, "freshness_status": "CURRENT", "data_finality": "FINAL_EOD", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100, "usable_for": {"final_eod_certification": True}},
                "QQQ": {"market_session_date": "2026-05-21", "freshness_status": "STALE", "data_finality": "FINAL_EOD", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100, "usable_for": {"final_eod_certification": False}},
            },
            "provider_results": [{"provider": "TEST", "request_status": "STALE", "fetched_symbols": ["SPY", "QQQ"], "missing_symbols": ["IWM"], "stale_symbols": ["QQQ"]}],
        },
    )
    monkeypatch.setenv("AEGIS_FINAL_EOD_MARKET_DATA_SOURCE_FILE", str(source))
    monkeypatch.setattr("ops.aegis.domain_source_builders_v1._required_eod_symbols_v1", lambda day_utc: ["SPY", "QQQ", "IWM"])

    result = build_domain_source_artifact_v1(truth_root=tmp_path, day_utc=day, domain_id="US_EQUITIES_EOD")
    artifact = tmp_path / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json"
    payload = json.loads(artifact.read_text(encoding="utf-8"))

    assert result["ok"] is False
    assert result["status"] == "PROVIDER_INCOMPLETE"
    assert "MISSING_SYMBOL_COVERAGE" in result["failure_reason"]
    assert "STALE_PROVIDER_ROWS" in result["failure_reason"]
    assert "FINAL_EOD_STATUS_NOT_VALID" not in result["failure_reason"]
    assert result["missing_symbols"] == ["IWM", "QQQ"]
    assert result["stale_date_symbols"] == ["QQQ"]
    assert artifact.exists()
    assert payload["validation_status"] == "REJECTED"
    assert payload["status"] == "PROVIDER_INCOMPLETE"
    assert payload["missing_symbols"] == ["IWM", "QQQ"]


def test_us_equities_eod_stale_prior_day_rows_are_rejected(tmp_path: Path, monkeypatch) -> None:
    day = "2026-05-22"
    source = tmp_path / "incoming" / "stale_final_eod.json"
    _write_json(
        source,
        {
            "schema_id": "provider_final_eod",
            "day_utc": day,
            "market_session_date": day,
            "status": "CURRENT",
            "validation_status": "VALID",
            "requested_symbols": ["SPY"],
            "fetched_symbols": ["SPY"],
            "symbols": {"SPY": {"market_session_date": "2026-05-21", "freshness_status": "STALE", "data_finality": "FINAL_EOD", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100}},
        },
    )
    monkeypatch.setenv("AEGIS_FINAL_EOD_MARKET_DATA_SOURCE_FILE", str(source))

    result = build_domain_source_artifact_v1(truth_root=tmp_path, day_utc=day, domain_id="US_EQUITIES_EOD")

    assert result["ok"] is False
    assert "STALE_PROVIDER_ROWS" in result["failure_reason"]
    assert result["stale_date_symbols"] == ["SPY"]


def test_failed_eod_repair_is_visible_with_precise_missing_symbols(tmp_path: Path) -> None:
    day = "2026-05-22"
    _write_market_inputs(tmp_path, day)

    def fake_runner(args: list[str]) -> dict:
        return {
            "args": args,
            "exit_code": 2,
            "stdout_tail": json.dumps({
                "ok": False,
                "status": "PROVIDER_INCOMPLETE",
                "result_status": "FAILED",
                "failure_reason": "MISSING_SYMBOL_COVERAGE,STALE_PROVIDER_ROWS",
                "missing_symbols": ["IWM", "QQQ"],
                "stale_date_symbols": ["QQQ"],
                "provider_results": [{"provider": "STOOQ", "request_status": "TIMEOUT", "failure_reason": "MARKET_DATA_REFRESH_TIMEOUT"}],
            }),
            "stderr_tail": "",
            "started_at_utc": f"{day}T22:00:00Z",
            "completed_at_utc": f"{day}T22:00:01Z",
        }

    run_domain_repair_orchestration_v1(truth_root=tmp_path, day_utc=day, domain_id="US_EQUITIES_EOD", execute=True, runner=fake_runner)
    projection = build_repair_center_projection_v1(truth_root=tmp_path, day_utc=day)
    eod = next(row for row in projection["repair_items"] if row["domain_id"] == "US_EQUITIES_EOD")

    assert eod["status"] == "FAILED"
    assert eod["section_id"] == "repair_failed"
    assert "provider coverage" in eod["plain_english_problem"].lower()
    assert "Missing symbols: IWM, QQQ" in eod["result"]["plain_english_result"]
    assert eod["result"]["failure_details"]["stale_date_symbols"] == ["QQQ"]


def test_repair_center_exposes_eod_source_template_validate_certify_actions(tmp_path: Path) -> None:
    day = "2026-05-22"
    _write_market_inputs(tmp_path, day)
    _write_json(
        tmp_path / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json",
        {
            "schema_id": "final_eod_market_data_v1",
            "day_utc": day,
            "market_session_date": day,
            "status": "PROVIDER_INCOMPLETE",
            "validation_status": "REJECTED",
            "final_eod_certification_status": "REJECTED",
            "requested_symbols": ["SPY", "QQQ"],
            "final_eod_symbols": ["SPY"],
            "reason_codes": ["MISSING_SYMBOL_COVERAGE"],
            "provider_coverage_action_item": {
                "status": "PROVIDER_COVERAGE_INCOMPLETE",
                "missing_symbols": ["QQQ"],
                "stale_symbols": [],
                "timeout_symbols": [],
                "required_universe_size": 2,
                "covered_count": 1,
            },
        },
    )

    projection = build_repair_center_projection_v1(truth_root=tmp_path, day_utc=day)
    eod = next(row for row in projection["repair_items"] if row["domain_id"] == "US_EQUITIES_EOD")
    labels = {row["label"] for row in [eod["primary_command"], *eod["secondary_commands"]]}
    command_ids = {row["command_id"] for row in [eod["primary_command"], *eod["secondary_commands"]]}

    assert "Upload/configure EOD source" in eod["next_step"] or "Upload/configure" in eod["primary_command"]["label"]
    assert "Download CSV template" in labels
    assert "Validate source file" in labels
    assert "Certify from source file" in labels
    assert {"DOWNLOAD_EOD_SOURCE_TEMPLATE", "VALIDATE_SOURCE", "CERTIFY_SOURCE"} <= command_ids
    assert eod["source_config_key"] == "AEGIS_US_EQUITIES_EOD_SOURCE_FILE"

