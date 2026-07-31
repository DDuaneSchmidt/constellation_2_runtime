from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_eod_artifact_contract_v1 import build_candidate_consumption_audit_v1
from ops.aegis.domain_source_builders_v1 import _write_immutable_final_eod_artifact_v1
from ops.aegis.dynamic_certification_queue_v1 import (
    build_dynamic_certification_queue_v1,
    certify_dynamic_queue_symbols_v1,
    dynamic_certification_queue_path_v1,
    write_dynamic_certification_queue_v1,
)
from ops.aegis.market_data.market_data_provider_v1 import ProviderResult

DAY = "2026-05-20"
NOW = f"{DAY}T21:30:00Z"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _audit_row(symbol: str, *, score: float, sleeve: str = "TREND", candidate_id: str | None = None, near: bool = False) -> dict:
    cid = candidate_id or f"candidate-{symbol}-{score}"
    return {
        "candidate_id": cid,
        "raw_intent_id": f"intent-{cid}",
        "symbol": symbol,
        "sleeve_id": sleeve,
        "source_status": "CANDIDATE_CREATED",
        "score": score,
        "near_promotion": near,
        "consumption_category": "EXCLUDED_UNCOVERED_SYMBOL",
        "consumption_reason": f"{symbol} is outside the certified EOD universe used by the report.",
        "covered_by_certified_eod": False,
        "source_artifact_path": f"/truth/raw/{cid}.json",
    }


def _write_audit(root: Path, day: str, rows: list[dict]) -> None:
    counts: dict[str, int] = {}
    for row in rows:
        category = str(row.get("consumption_category") or "UNKNOWN")
        counts[category] = counts.get(category, 0) + 1
    _write_json(
        root / "reports" / "candidate_consumption_audit_v1" / day / "candidate_consumption_audit.v1.json",
        {
            "schema_id": "candidate_consumption_audit",
            "schema_version": "v1",
            "artifact_id": "candidate_consumption_audit_v1",
            "trading_date": day,
            "created_at_utc": f"{day}T21:00:00Z",
            "raw_candidate_count": len(rows),
            "promoted_candidate_count": counts.get("PROMOTED", 0),
            "excluded_candidate_count": len(rows) - counts.get("PROMOTED", 0),
            "consumption_counts": counts,
            "candidate_rows": rows,
            "certified_universe_symbols": ["QQQ"],
        },
    )


def _provider_row(symbol: str, day: str = DAY) -> dict:
    return {
        "symbol": symbol,
        "canonical_symbol": symbol,
        "provider": "TIINGO",
        "market_session_date": day,
        "data_timestamp_utc": f"{day}T21:00:00Z",
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 1000000,
        "freshness_status": "CURRENT",
        "data_finality": "FINAL_EOD",
        "finalization_status": "FINAL",
    }


def _provider_result(symbols: list[str], *, day: str = DAY, status: str = "SUCCESS", include_rows: bool = True) -> ProviderResult:
    rows = {symbol: _provider_row(symbol, day) for symbol in symbols} if include_rows else {}
    return ProviderResult(
        provider="TIINGO",
        request_status=status,
        timestamp_utc=f"{day}T21:00:00Z",
        returned_data_date=day,
        symbols=rows,
        breadth={},
        requested_symbols=tuple(symbols),
        fetched_symbols=tuple(rows),
        missing_symbols=tuple(sorted(set(symbols) - set(rows))),
        normalized_records=(),
        provider_coverage_plan={"status": "TEST_PROVIDER_PLAN", "unsupported_symbols": []},
    )


def _write_final_eod(root: Path, day: str, symbols: list[str]) -> dict[str, str]:
    payload = {
        "schema_id": "final_eod_market_data_v1",
        "schema_version": "v1",
        "day_utc": day,
        "trading_day": day,
        "market_session_date": day,
        "status": "CURRENT",
        "validation_status": "VALID",
        "final_eod_certification_status": "VALID",
        "requested_symbols": symbols,
        "fetched_symbols": symbols,
        "final_eod_symbols": symbols,
        "symbols": {symbol: _provider_row(symbol, day) for symbol in symbols},
    }
    return _write_immutable_final_eod_artifact_v1(
        root=root,
        day_utc=day,
        manifest_path=root / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json",
        payload=payload,
    )


def test_uncovered_low_score_noise_is_not_queued(tmp_path: Path) -> None:
    _write_audit(tmp_path, DAY, [_audit_row("SPY", score=0.1)])

    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert queue["requested_symbols"] == []
    assert queue["certification_status"] == "EMPTY"
    spy = next(row for row in queue["all_uncovered_symbol_evaluations"] if row["symbol"] == "SPY")
    assert spy["certification_status"] == "NOT_QUEUED_LOW_PRESSURE"
    assert spy["queued"] is False


def test_high_score_recurring_uncovered_symbol_is_queued(tmp_path: Path) -> None:
    prior_day = "2026-05-19"
    _write_audit(tmp_path, prior_day, [_audit_row("SPY", score=0.82, sleeve="TREND", candidate_id="spy-prior")])
    _write_audit(tmp_path, DAY, [_audit_row("SPY", score=0.91, sleeve="EVENT", candidate_id="spy-current", near=True)])

    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert queue["requested_symbols"] == ["SPY"]
    row = queue["queued_symbols"][0]
    assert row["symbol"] == "SPY"
    assert row["recurrence_count_5d"] == 2
    assert row["sleeve_frequency"] == 2
    assert "high candidate score" in row["reason"]
    assert "near-promotion" in row["reason"]


def test_active_uncovered_intent_with_repeated_sleeve_pressure_is_queued_without_score(tmp_path: Path) -> None:
    _write_audit(
        tmp_path,
        DAY,
        [
            {**_audit_row("CRWD", score=0.0, sleeve="TREND", candidate_id="crwd-trend"), "source_status": "CANDIDATE_CREATED"},
            {**_audit_row("CRWD", score=0.0, sleeve="EVENT", candidate_id="crwd-event"), "source_status": "NO_SIGNAL"},
            {**_audit_row("CRWD", score=0.0, sleeve="MEAN", candidate_id="crwd-mean"), "source_status": "SUPPRESSED"},
            {**_audit_row("NOISE", score=0.0, sleeve="TREND", candidate_id="noise-trend"), "source_status": "NO_SIGNAL"},
            {**_audit_row("NOISE", score=0.0, sleeve="EVENT", candidate_id="noise-event"), "source_status": "SUPPRESSED"},
        ],
    )

    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert queue["requested_symbols"] == ["CRWD"]
    crwd = queue["queued_symbols"][0]
    assert crwd["active_intent_pressure_count"] == 1
    assert "active candidate/intent pressure" in crwd["reason"]
    noise = next(row for row in queue["all_uncovered_symbol_evaluations"] if row["symbol"] == "NOISE")
    assert noise["queued"] is False


def test_queued_symbol_certifies_and_candidate_can_pass_uncovered_blocker(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_audit(tmp_path, DAY, [_audit_row("SPY", score=0.91, candidate_id="spy-current", near=True)])
    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)

    result = certify_dynamic_queue_symbols_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        generated_at_utc=NOW,
        provider_fetcher=lambda **kwargs: _provider_result(list(kwargs["symbols"])),
    )

    assert result["ok"] is True
    assert result["certified_symbols"] == ["SPY"]
    manifest = json.loads((tmp_path / "reports" / "final_eod_market_data_v1" / DAY / "final_eod_market_data.v1.json").read_text(encoding="utf-8"))
    artifact = json.loads(Path(manifest["current_artifact_path"]).read_text(encoding="utf-8"))
    assert "SPY" in artifact["final_eod_symbols"]

    raw = {"candidate_id": "spy-current", "symbol": "SPY", "sleeve_id": "TREND", "status": "CANDIDATE_CREATED"}
    before = build_candidate_consumption_audit_v1(
        trading_date=DAY,
        run_id="before",
        created_at_utc=NOW,
        raw_rows=[raw],
        consumed_candidates=[{"candidate_id": "spy-current"}],
        promoted_sleeve_manifest={"manifest_status": "EFFECTIVE", "promoted_sleeves": [{"sleeve_id": "TREND", "promotion_status": "promoted"}], "promoted_sleeve_count": 1},
        certified_symbols=["QQQ"],
        certified_artifact_path="/truth/final-eod.json",
        input_contract={"status": "OK", "blockers": []},
    )
    after = build_candidate_consumption_audit_v1(
        trading_date=DAY,
        run_id="after",
        created_at_utc=NOW,
        raw_rows=[raw],
        consumed_candidates=[{"candidate_id": "spy-current"}],
        promoted_sleeve_manifest={"manifest_status": "EFFECTIVE", "promoted_sleeves": [{"sleeve_id": "TREND", "promotion_status": "promoted"}], "promoted_sleeve_count": 1},
        certified_symbols=artifact["final_eod_symbols"],
        certified_artifact_path=manifest["current_artifact_path"],
        input_contract={"status": "OK", "blockers": []},
    )
    assert before["candidate_rows"][0]["covered_by_certified_eod"] is False
    assert after["candidate_rows"][0]["covered_by_certified_eod"] is True
    assert after["candidate_rows"][0]["consumption_category"] == "PROMOTED"


def test_crwd_golden_dynamic_certification_moves_from_uncovered_to_policy_and_score(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    crwd_rows = [
        {**_audit_row("CRWD", score=0.0, sleeve="C2_EVENT_DISLOCATION_V1", candidate_id="crwd-event"), "source_status": "NO_SIGNAL"},
        {**_audit_row("CRWD", score=0.0, sleeve="C2_MEAN_REVERSION_EQ_V1", candidate_id="crwd-mean"), "source_status": "SUPPRESSED"},
        {**_audit_row("CRWD", score=0.0, sleeve="C2_TREND_EQ_PRIMARY_V1", candidate_id="crwd-trend"), "source_status": "CANDIDATE_CREATED"},
        {**_audit_row("WIDE", score=0.1, sleeve="C2_EVENT_DISLOCATION_V1", candidate_id="wide-still-uncovered"), "source_status": "NO_SIGNAL"},
    ]
    _write_audit(tmp_path, DAY, crwd_rows)

    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)

    assert queue["requested_symbols"][0] == "CRWD"
    crwd_queue = next(row for row in queue["queued_symbols"] if row["symbol"] == "CRWD")
    assert crwd_queue["active_intent_pressure_count"] == 1
    assert crwd_queue["sleeve_frequency"] == 3

    def _fetch(**kwargs) -> ProviderResult:
        provider = kwargs["config_override"].primary
        symbols = tuple(kwargs["symbols"])
        if provider == "LOCAL_CACHE":
            return ProviderResult(provider=provider, request_status="FAILED", timestamp_utc=NOW, returned_data_date="", symbols={}, breadth={}, requested_symbols=symbols, missing_symbols=symbols, normalized_records=())
        assert provider == "TIINGO"
        return ProviderResult(provider=provider, request_status="SUCCESS", timestamp_utc=NOW, returned_data_date=DAY, symbols={"CRWD": _provider_row("CRWD")}, breadth={}, requested_symbols=symbols, fetched_symbols=("CRWD",), normalized_records=())

    result = certify_dynamic_queue_symbols_v1(truth_root=tmp_path, day_utc=DAY, symbols=["CRWD"], generated_at_utc=NOW, provider_fetcher=_fetch)

    assert result["result_status"] == "CERTIFIED"
    assert result["certified_symbols"] == ["CRWD"]
    manifest = json.loads((tmp_path / "reports" / "final_eod_market_data_v1" / DAY / "final_eod_market_data.v1.json").read_text(encoding="utf-8"))
    artifact = json.loads(Path(manifest["current_artifact_path"]).read_text(encoding="utf-8"))
    assert "CRWD" in artifact["final_eod_symbols"]

    raw_rows = [
        {"candidate_id": "crwd-event", "symbol": "CRWD", "sleeve_id": "C2_EVENT_DISLOCATION_V1", "status": "NO_SIGNAL"},
        {"candidate_id": "crwd-mean", "symbol": "CRWD", "sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "status": "SUPPRESSED"},
        {"candidate_id": "crwd-trend", "symbol": "CRWD", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "status": "CANDIDATE_CREATED"},
        {"candidate_id": "wide-still-uncovered", "symbol": "WIDE", "sleeve_id": "C2_EVENT_DISLOCATION_V1", "status": "CANDIDATE_CREATED"},
    ]
    before = build_candidate_consumption_audit_v1(
        trading_date=DAY,
        run_id="before-crwd-certification",
        created_at_utc=NOW,
        raw_rows=raw_rows,
        consumed_candidates=[],
        promoted_sleeve_manifest={"manifest_status": "INEFFECTIVE", "promoted_sleeves": [], "promoted_sleeve_count": 0},
        certified_symbols=["QQQ"],
        certified_artifact_path="/truth/final-eod-before.json",
        input_contract={"status": "NO_PROMOTABLE_CANDIDATES", "blockers": []},
    )
    after = build_candidate_consumption_audit_v1(
        trading_date=DAY,
        run_id="after-crwd-certification",
        created_at_utc=NOW,
        raw_rows=raw_rows,
        consumed_candidates=[],
        promoted_sleeve_manifest={"manifest_status": "INEFFECTIVE", "promoted_sleeves": [], "promoted_sleeve_count": 0},
        certified_symbols=artifact["final_eod_symbols"],
        certified_artifact_path=manifest["current_artifact_path"],
        input_contract={"status": "NO_PROMOTABLE_CANDIDATES", "blockers": []},
    )

    assert before["consumption_counts"]["EXCLUDED_UNCOVERED_SYMBOL"] == 4
    assert after["consumption_counts"]["EXCLUDED_UNCOVERED_SYMBOL"] == 1
    crwd_after = [row for row in after["candidate_rows"] if row["symbol"] == "CRWD"]
    assert {row["consumption_category"] for row in crwd_after} == {"EXCLUDED_LOW_SCORE", "EXCLUDED_POLICY"}
    assert all(row["covered_by_certified_eod"] is True for row in crwd_after)
    assert after["promoted_candidate_count"] == 0

    promoted = build_candidate_consumption_audit_v1(
        trading_date=DAY,
        run_id="after-crwd-promotion-gates-pass",
        created_at_utc=NOW,
        raw_rows=[raw_rows[2]],
        consumed_candidates=[{"candidate_id": "crwd-trend"}],
        promoted_sleeve_manifest={"manifest_status": "EFFECTIVE", "promoted_sleeves": [{"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "promotion_status": "promoted"}], "promoted_sleeve_count": 1},
        certified_symbols=artifact["final_eod_symbols"],
        certified_artifact_path=manifest["current_artifact_path"],
        input_contract={"status": "PASS", "blockers": []},
    )
    assert promoted["promoted_candidate_count"] == 1
    assert promoted["candidate_rows"][0]["consumption_category"] == "PROMOTED"


def test_open_position_missing_mark_queues_dynamic_certification(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_json(
        tmp_path / "reports" / "captured_ticket_history_v1" / DAY / "ticket_dow" / "captured_ticket_history.v1.json",
        {
            "schema_id": "captured_ticket_history",
            "schema_version": "v1",
            "day_utc": DAY,
            "ticket_id": "ticket:dow",
            "symbol": "DOW",
            "side": "BUY",
            "quantity": 166,
            "fill_price": "36.06",
            "stop_price": "34.26",
            "sleeve_id": "C2_MEAN_REVERSION_EQ_V1",
            "captured_at_utc": f"{DAY}T17:16:00Z",
            "history_hash": "dow-history",
        },
    )

    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert "DOW" in queue["requested_symbols"]
    row = next(item for item in queue["queued_symbols"] if item["symbol"] == "DOW")
    assert row["open_position_mark_required_count"] == 1
    assert "open captured position requires certified mark" in row["reason"]
    providers = row["expected_provider_coverage"]["approved_providers"]
    assert "TIINGO" in providers
    assert "ALPHA_VANTAGE" in providers


def test_provider_failure_keeps_queued_symbol_excluded(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_audit(tmp_path, DAY, [_audit_row("SPY", score=0.91, candidate_id="spy-current")])
    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)

    result = certify_dynamic_queue_symbols_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        generated_at_utc=NOW,
        provider_fetcher=lambda **kwargs: _provider_result(list(kwargs["symbols"]), status="FAILED", include_rows=False),
    )

    assert result["ok"] is False
    assert result["result_status"] == "FAILED"
    assert result["failed_symbols"] == [{"symbol": "SPY", "reason": "MISSING_PROVIDER_ROW"}]
    manifest = json.loads((tmp_path / "reports" / "final_eod_market_data_v1" / DAY / "final_eod_market_data.v1.json").read_text(encoding="utf-8"))
    artifact = json.loads(Path(manifest["current_artifact_path"]).read_text(encoding="utf-8"))
    assert artifact["final_eod_symbols"] == ["QQQ"]


def test_provider_rate_limit_reason_is_preserved_for_dynamic_certification(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_audit(tmp_path, DAY, [_audit_row("CRWD", score=0.91, candidate_id="crwd-current")])
    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)

    def _fetch(**kwargs) -> ProviderResult:
        provider = kwargs["config_override"].primary
        symbols = tuple(kwargs["symbols"])
        if provider == "LOCAL_CACHE":
            return ProviderResult(provider=provider, request_status="FAILED", timestamp_utc=NOW, returned_data_date="", symbols={}, breadth={}, requested_symbols=symbols, missing_symbols=symbols, normalized_records=())
        return ProviderResult(provider=provider, request_status="RATE_LIMITED", timestamp_utc=NOW, returned_data_date="", symbols={}, breadth={}, requested_symbols=symbols, missing_symbols=symbols, failure_reason="TIINGO_RATE_LIMIT_429", provider_attempts=({"provider": provider, "status": "RATE_LIMITED", "http_status": 429},), normalized_records=())

    result = certify_dynamic_queue_symbols_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW, provider_fetcher=_fetch)

    assert result["ok"] is False
    assert result["failed_symbols"] == [{"symbol": "CRWD", "reason": "TIINGO_RATE_LIMIT_429"}]
    assert result["certification_results"][0]["lineage"]["provider_refresh_failure_reason"] == "TIINGO_RATE_LIMIT_429"


def test_provider_timeout_reason_is_preserved_for_dynamic_certification(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_audit(tmp_path, DAY, [_audit_row("CRWD", score=0.91, candidate_id="crwd-current")])
    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)

    def _fetch(**kwargs) -> ProviderResult:
        provider = kwargs["config_override"].primary
        symbols = tuple(kwargs["symbols"])
        if provider == "LOCAL_CACHE":
            return ProviderResult(provider=provider, request_status="FAILED", timestamp_utc=NOW, returned_data_date="", symbols={}, breadth={}, requested_symbols=symbols, missing_symbols=symbols, normalized_records=())
        return ProviderResult(provider=provider, request_status="TIMEOUT", timestamp_utc=NOW, returned_data_date="", symbols={}, breadth={}, requested_symbols=symbols, missing_symbols=symbols, failure_reason="TIINGO_TIMEOUT", provider_attempts=({"provider": provider, "status": "TIMEOUT"},), normalized_records=())

    result = certify_dynamic_queue_symbols_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW, provider_fetcher=_fetch)

    assert result["ok"] is False
    assert result["failed_symbols"] == [{"symbol": "CRWD", "reason": "TIINGO_TIMEOUT"}]
    assert result["certification_results"][0]["lineage"]["provider_refresh_failure_reason"] == "TIINGO_TIMEOUT"


def test_stale_local_cache_triggers_tiingo_refresh_for_dynamic_certification(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_audit(tmp_path, DAY, [_audit_row("SPY", score=0.91, candidate_id="spy-current")])
    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)
    calls: list[tuple[str, tuple[str, ...]]] = []

    def _fetch(**kwargs) -> ProviderResult:
        provider = kwargs["config_override"].primary
        symbols = tuple(kwargs["symbols"])
        calls.append((provider, symbols))
        if provider == "LOCAL_CACHE":
            stale = {**_provider_row("SPY", "2026-05-19"), "provider": "LOCAL_CACHE", "freshness_status": "STALE"}
            return ProviderResult(provider=provider, request_status="STALE", timestamp_utc=NOW, returned_data_date="2026-05-19", symbols={"SPY": stale}, breadth={}, requested_symbols=symbols, fetched_symbols=("SPY",), stale_symbols=("SPY",), normalized_records=())
        assert provider == "TIINGO"
        return ProviderResult(provider=provider, request_status="SUCCESS", timestamp_utc=NOW, returned_data_date=DAY, symbols={"SPY": _provider_row("SPY")}, breadth={}, requested_symbols=symbols, fetched_symbols=("SPY",), normalized_records=())

    result = certify_dynamic_queue_symbols_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW, provider_fetcher=_fetch)

    assert result["ok"] is True
    assert calls == [("LOCAL_CACHE", ("SPY",)), ("TIINGO", ("SPY",))]
    assert result["certification_results"][0]["provider"] == "TIINGO"
    assert result["certification_results"][0]["lineage"]["local_cache_status"] == "STALE_OR_WRONG_TRADING_DAY"
    assert result["certification_results"][0]["lineage"]["provider_refresh_provider"] == "TIINGO"


def test_dynamic_certification_uses_final_eod_provider_path_not_intraday_env(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "LOCAL_CACHE")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "STOOQ")
    monkeypatch.setenv("AEGIS_MARKET_DATA_INTRADAY_PROVIDER", "STOOQ")
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_audit(tmp_path, DAY, [_audit_row("CRWD", score=0.91, candidate_id="crwd-current")])
    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)
    calls: list[tuple[str, str, str, tuple[str, ...]]] = []

    def _fetch(**kwargs) -> ProviderResult:
        config = kwargs["config_override"]
        provider = config.primary
        symbols = tuple(kwargs["symbols"])
        calls.append((provider, config.fallback, config.market_data_mode, symbols))
        assert config.market_data_mode == "FINAL_EOD_CERTIFIED"
        assert config.intraday_provider == ""
        if provider == "LOCAL_CACHE":
            stale = {**_provider_row("CRWD", "2026-05-19"), "provider": "LOCAL_CACHE", "freshness_status": "STALE"}
            return ProviderResult(provider=provider, request_status="STALE", timestamp_utc=NOW, returned_data_date="2026-05-19", symbols={"CRWD": stale}, breadth={}, requested_symbols=symbols, fetched_symbols=("CRWD",), stale_symbols=("CRWD",), normalized_records=())
        assert provider == "TIINGO"
        assert config.fallback == "ALPHA_VANTAGE"
        return ProviderResult(provider=provider, request_status="SUCCESS", timestamp_utc=NOW, returned_data_date=DAY, symbols={"CRWD": _provider_row("CRWD")}, breadth={}, requested_symbols=symbols, fetched_symbols=("CRWD",), normalized_records=())

    result = certify_dynamic_queue_symbols_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW, provider_fetcher=_fetch)

    assert result["ok"] is True
    assert calls == [("LOCAL_CACHE", "", "FINAL_EOD_CERTIFIED", ("CRWD",)), ("TIINGO", "ALPHA_VANTAGE", "FINAL_EOD_CERTIFIED", ("CRWD",))]
    assert not any(provider == "STOOQ" for provider, _fallback, _mode, _symbols in calls)
    assert result["certification_results"][0]["provider"] == "TIINGO"


def test_tiingo_valid_queued_symbol_certifies_when_local_cache_missing(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_audit(tmp_path, DAY, [_audit_row("SPY", score=0.91, candidate_id="spy-current")])
    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)

    def _fetch(**kwargs) -> ProviderResult:
        provider = kwargs["config_override"].primary
        symbols = tuple(kwargs["symbols"])
        if provider == "LOCAL_CACHE":
            return ProviderResult(provider=provider, request_status="FAILED", timestamp_utc=NOW, returned_data_date="", symbols={}, breadth={}, requested_symbols=symbols, missing_symbols=symbols, normalized_records=())
        assert provider == "TIINGO"
        return ProviderResult(provider=provider, request_status="SUCCESS", timestamp_utc=NOW, returned_data_date=DAY, symbols={"SPY": _provider_row("SPY")}, breadth={}, requested_symbols=symbols, fetched_symbols=("SPY",), normalized_records=())

    result = certify_dynamic_queue_symbols_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW, provider_fetcher=_fetch)

    assert result["result_status"] == "CERTIFIED"
    assert result["certified_symbols"] == ["SPY"]


def test_stale_wrong_day_row_fails_only_after_provider_refresh_also_fails(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_audit(tmp_path, DAY, [_audit_row("SPY", score=0.91, candidate_id="spy-current")])
    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)
    calls: list[str] = []

    def _fetch(**kwargs) -> ProviderResult:
        provider = kwargs["config_override"].primary
        symbols = tuple(kwargs["symbols"])
        calls.append(provider)
        stale = {**_provider_row("SPY", "2026-05-19"), "provider": provider, "freshness_status": "STALE"}
        return ProviderResult(provider=provider, request_status="STALE", timestamp_utc=NOW, returned_data_date="2026-05-19", symbols={"SPY": stale}, breadth={}, requested_symbols=symbols, fetched_symbols=("SPY",), stale_symbols=("SPY",), normalized_records=())

    result = certify_dynamic_queue_symbols_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW, provider_fetcher=_fetch)

    assert calls == ["LOCAL_CACHE", "TIINGO"]
    assert result["ok"] is False
    assert result["failed_symbols"] == [{"symbol": "SPY", "reason": "STALE_OR_WRONG_TRADING_DAY"}]
    assert result["certification_results"][0]["lineage"]["provider_refresh_status"] == "STALE_OR_WRONG_TRADING_DAY"


def test_vix_dynamic_certification_routes_to_cboe(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_audit(tmp_path, DAY, [_audit_row("VIX", score=0.91, candidate_id="vix-current")])
    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)
    calls: list[tuple[str, tuple[str, ...]]] = []

    def _fetch(**kwargs) -> ProviderResult:
        provider = kwargs["config_override"].primary
        symbols = tuple(kwargs["symbols"])
        calls.append((provider, symbols))
        if provider == "LOCAL_CACHE":
            return ProviderResult(provider=provider, request_status="FAILED", timestamp_utc=NOW, returned_data_date="", symbols={}, breadth={}, requested_symbols=symbols, missing_symbols=symbols, normalized_records=())
        assert provider == "CBOE"
        row = {**_provider_row("VIX"), "provider": "CBOE"}
        return ProviderResult(provider=provider, request_status="SUCCESS", timestamp_utc=NOW, returned_data_date=DAY, symbols={"VIX": row}, breadth={}, requested_symbols=symbols, fetched_symbols=("VIX",), normalized_records=())

    result = certify_dynamic_queue_symbols_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW, provider_fetcher=_fetch)

    assert result["ok"] is True
    assert calls == [("LOCAL_CACHE", ("VIX",)), ("CBOE", ("VIX",))]
    assert result["certification_results"][0]["provider"] == "CBOE"


def test_dynamic_certification_records_tier1_baseline_without_mutating_it(tmp_path: Path) -> None:
    _write_final_eod(tmp_path, DAY, ["QQQ"])
    _write_audit(tmp_path, DAY, [_audit_row("SPY", score=0.91, candidate_id="spy-current")])
    queue = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=queue)

    def _fetch(**kwargs) -> ProviderResult:
        provider = kwargs["config_override"].primary
        symbols = tuple(kwargs["symbols"])
        if provider == "LOCAL_CACHE":
            return ProviderResult(provider=provider, request_status="FAILED", timestamp_utc=NOW, returned_data_date="", symbols={}, breadth={}, requested_symbols=symbols, missing_symbols=symbols, normalized_records=())
        return ProviderResult(provider=provider, request_status="SUCCESS", timestamp_utc=NOW, returned_data_date=DAY, symbols={"SPY": _provider_row("SPY")}, breadth={}, requested_symbols=symbols, fetched_symbols=("SPY",), normalized_records=())

    result = certify_dynamic_queue_symbols_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW, provider_fetcher=_fetch)

    dynamic_universe = json.loads(Path(result["dynamic_certified_universe_artifact_path"]).read_text(encoding="utf-8"))
    assert dynamic_universe["tier_1_stable_symbols"] == ["QQQ"]
    assert dynamic_universe["dynamic_certified_symbols"] == ["SPY"]
    manifest = json.loads((tmp_path / "reports" / "final_eod_market_data_v1" / DAY / "final_eod_market_data.v1.json").read_text(encoding="utf-8"))
    artifact = json.loads(Path(manifest["current_artifact_path"]).read_text(encoding="utf-8"))
    assert artifact["dynamic_certification_expansion"]["certified_symbols"] == ["SPY"]
    assert artifact["final_eod_symbols"] == ["QQQ", "SPY"]


def test_no_fixed_symbol_count_assumptions_in_dynamic_certification_paths() -> None:
    sources = "\n".join(
        (REPO_ROOT / rel).read_text(encoding="utf-8")
        for rel in [
            "ops/aegis/dynamic_certification_queue_v1.py",
            "ops/tools/manage_us_equities_eod_source_v1.py",
            "ops/aegis/domain_source_builders_v1.py",
        ]
    )
    assert "broad_universe_membership_alone_queues_symbol" in sources
    assert "requested_symbol_count" in sources
    assert " == " + str(40 + 3) not in sources
    assert " == " + str(400 + 33) not in sources
    assert "range(" + str(400 + 33) not in sources


def test_dynamic_certification_replay_is_deterministic(tmp_path: Path) -> None:
    _write_audit(tmp_path, DAY, [_audit_row("SPY", score=0.91, candidate_id="spy-current", near=True)])

    first = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    second = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert first["content_hash"] == second["content_hash"]
    assert first["requested_symbols"] == second["requested_symbols"]
    assert first["queued_symbols"] == second["queued_symbols"]


def test_queue_artifact_uses_content_addressed_hash(tmp_path: Path) -> None:
    _write_audit(tmp_path, DAY, [_audit_row("SPY", score=0.91)])
    payload = build_dynamic_certification_queue_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    paths = write_dynamic_certification_queue_v1(truth_root=tmp_path, payload=payload)

    assert Path(paths["json"]) == dynamic_certification_queue_path_v1(truth_root=tmp_path, day_utc=DAY)
    written = json.loads(Path(paths["json"]).read_text(encoding="utf-8"))
    assert written["content_hash"] == paths["content_hash"]
    assert written["broker_execution_allowed"] is False
    assert written["autonomous_execution_allowed"] is False
