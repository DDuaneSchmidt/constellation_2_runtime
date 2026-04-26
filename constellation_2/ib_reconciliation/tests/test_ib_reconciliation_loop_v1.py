from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.ib_reconciliation.ai_exception_packet_v1 import (
    build_ai_exception_packet_v1,
    default_ai_review_template_v1,
    enforce_ai_review_guardrails_v1,
    render_ai_exception_packet_markdown_v1,
)
from constellation_2.ib_reconciliation.ib_flex_normalizer_v1 import (
    IBFlexNormalizationError,
    normalize_ib_flex_xml_v1,
)
from constellation_2.ib_reconciliation.mismatch_classifier_v1 import classify_mismatches_v1
from constellation_2.ib_reconciliation.paths_v1 import (
    PathContractError,
    REPO_ROOT,
    ensure_not_under_repo_v1,
    runtime_root_v1,
    truth_surface_report_path_v1,
)
from constellation_2.ib_reconciliation.reconciliation_matcher_v1 import (
    RULE_FALLBACK_SYMBOL_SIDE_QTY_TIME_WINDOW,
    RULE_ORDER_ID_EXACT,
    RULE_PERM_ID_EXACT,
    match_expected_to_ib_v1,
)
from constellation_2.ib_reconciliation.schema_v1 import write_deterministic_json_v1


FIXTURE_XML = (
    Path(__file__).resolve().parent / "fixtures" / "ib_flex_sample.xml"
).resolve()


def _expected_activity(*, fills: list[dict] | None = None, orders: list[dict] | None = None) -> dict:
    return {
        "schema_version": "aegis_expected_activity.v1",
        "day_utc": "2026-04-24",
        "source_artifacts": ["/tmp/source.json"],
        "expected_orders": orders or [],
        "expected_fills": fills or [],
        "expected_positions": [],
    }


def _normalized_trades(*, trades: list[dict]) -> dict:
    return {
        "schema_version": "normalized_ib_trades.v1",
        "day_utc": "2026-04-24",
        "source_report_path": "/tmp/ib.xml",
        "trades": trades,
    }


def _ib_trade(
    *,
    symbol: str = "AAPL",
    side: str = "BUY",
    quantity: str = "10",
    price: str = "100",
    order_id: str = "0",
    perm_id: str = "0",
    trade_time_utc: str = "2026-04-24T14:30:00Z",
    order_ref: str = "",
) -> dict:
    attrs = {}
    if order_ref:
        attrs["orderRef"] = order_ref
    return {
        "account_id": "DU12345",
        "symbol": symbol,
        "asset_class": "STK",
        "side": side,
        "quantity": quantity,
        "price": price,
        "currency": "USD",
        "trade_time_utc": trade_time_utc,
        "ib_order_id": order_id,
        "ib_perm_id": perm_id,
        "commission": "0.5",
        "raw_ref": {"attributes": attrs},
    }


def _expected_fill(
    *,
    symbol: str = "AAPL",
    side: str = "BUY",
    quantity: str = "10",
    expected_price: str = "100",
    order_id: str = "0",
    perm_id: str = "0",
    submission_id: str = "submission-1",
    expected_time_utc: str = "2026-04-24T14:30:00Z",
) -> dict:
    return {
        "attempt_id": "attempt-1",
        "submission_id": submission_id,
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "expected_price": expected_price,
        "broker_order_id": order_id,
        "broker_perm_id": perm_id,
        "expected_time_utc": expected_time_utc,
    }


def test_01_parse_valid_ib_flex_xml_trade() -> None:
    normalized = normalize_ib_flex_xml_v1("2026-04-24", FIXTURE_XML)
    trades = normalized["normalized_trades"]["trades"]
    assert len(trades) == 1
    assert trades[0]["symbol"] == "AAPL"
    assert trades[0]["ib_perm_id"] == "222"
    assert normalized["normalized_positions"] is not None
    assert normalized["normalized_cash"] is not None


def test_02_malformed_xml_fails_closed(tmp_path: Path) -> None:
    bad_xml = (tmp_path / "bad.xml").resolve()
    bad_xml.write_text("<Flex><Trade></Flex>", encoding="utf-8")
    with pytest.raises(IBFlexNormalizationError):
        normalize_ib_flex_xml_v1("2026-04-24", bad_xml)


def test_03_exact_perm_id_match_pass() -> None:
    expected = _expected_activity(fills=[_expected_fill(perm_id="555", order_id="0")])
    ib = _normalized_trades(trades=[_ib_trade(order_id="999", perm_id="555")])
    match = match_expected_to_ib_v1(expected, ib)
    assert match["matched_items"][0]["match_rule"] == RULE_PERM_ID_EXACT
    result = classify_mismatches_v1(
        day_utc="2026-04-24",
        expected_activity=expected,
        normalized_ib_trades=ib,
        normalized_ib_positions=None,
        normalized_ib_cash=None,
        match_result=match,
    )
    assert result["status"] == "PASS"


def test_04_exact_order_id_match_pass() -> None:
    expected = _expected_activity(fills=[_expected_fill(order_id="888", perm_id="0")])
    ib = _normalized_trades(trades=[_ib_trade(order_id="888", perm_id="0")])
    match = match_expected_to_ib_v1(expected, ib)
    assert match["matched_items"][0]["match_rule"] == RULE_ORDER_ID_EXACT
    result = classify_mismatches_v1(
        day_utc="2026-04-24",
        expected_activity=expected,
        normalized_ib_trades=ib,
        normalized_ib_positions=None,
        normalized_ib_cash=None,
        match_result=match,
    )
    assert result["status"] == "PASS"


def test_05_unexpected_ib_trade_is_fail_high() -> None:
    expected = _expected_activity(fills=[])
    ib = _normalized_trades(trades=[_ib_trade(order_id="1", perm_id="2")])
    match = match_expected_to_ib_v1(expected, ib)
    result = classify_mismatches_v1(
        day_utc="2026-04-24",
        expected_activity=expected,
        normalized_ib_trades=ib,
        normalized_ib_positions=None,
        normalized_ib_cash=None,
        match_result=match,
    )
    assert result["status"] == "FAIL"
    assert any(m["type"] == "UNEXPECTED_IB_TRADE" and m["severity"] == "HIGH" for m in result["mismatches"])


def test_06_missing_ib_trade_is_fail_high() -> None:
    expected = _expected_activity(fills=[_expected_fill(order_id="1", perm_id="1")])
    ib = _normalized_trades(trades=[])
    match = match_expected_to_ib_v1(expected, ib)
    result = classify_mismatches_v1(
        day_utc="2026-04-24",
        expected_activity=expected,
        normalized_ib_trades=ib,
        normalized_ib_positions=None,
        normalized_ib_cash=None,
        match_result=match,
    )
    assert result["status"] == "FAIL"
    assert any(m["type"] == "MISSING_IB_TRADE" and m["severity"] == "HIGH" for m in result["mismatches"])


def test_07_partial_fill_mismatch_warn() -> None:
    expected = _expected_activity(fills=[_expected_fill(order_id="22", perm_id="22", quantity="10")])
    ib = _normalized_trades(trades=[_ib_trade(order_id="22", perm_id="22", quantity="4")])
    match = match_expected_to_ib_v1(expected, ib)
    result = classify_mismatches_v1(
        day_utc="2026-04-24",
        expected_activity=expected,
        normalized_ib_trades=ib,
        normalized_ib_positions=None,
        normalized_ib_cash=None,
        match_result=match,
    )
    assert result["status"] == "WARN"
    assert any(m["type"] == "PARTIAL_FILL_MISMATCH" for m in result["mismatches"])


def test_08_fallback_low_confidence_cannot_full_pass() -> None:
    expected = _expected_activity(
        fills=[
            _expected_fill(
                order_id="0",
                perm_id="0",
                quantity="10",
                expected_time_utc="2026-04-24T14:30:00Z",
            )
        ]
    )
    ib = _normalized_trades(
        trades=[
            _ib_trade(
                order_id="0",
                perm_id="0",
                quantity="10",
                trade_time_utc="2026-04-24T14:35:00Z",
            )
        ]
    )
    match = match_expected_to_ib_v1(expected, ib)
    assert match["matched_items"][0]["match_rule"] == RULE_FALLBACK_SYMBOL_SIDE_QTY_TIME_WINDOW
    result = classify_mismatches_v1(
        day_utc="2026-04-24",
        expected_activity=expected,
        normalized_ib_trades=ib,
        normalized_ib_positions=None,
        normalized_ib_cash=None,
        match_result=match,
    )
    assert result["status"] == "WARN"
    assert any(m["type"] == "LOW_CONFIDENCE_MATCH_ONLY" for m in result["mismatches"])


def test_09_position_mismatch_fail_high() -> None:
    expected = _expected_activity(fills=[])
    expected["expected_positions"] = [
        {"account_id": "DU12345", "symbol": "AAPL", "asset_class": "STK", "quantity": "10"}
    ]
    ib = _normalized_trades(trades=[])
    ib_positions = {
        "schema_version": "normalized_ib_positions.v1",
        "day_utc": "2026-04-24",
        "source_report_path": "/tmp/ib.xml",
        "positions": [
            {
                "account_id": "DU12345",
                "symbol": "AAPL",
                "asset_class": "STK",
                "quantity": "8",
                "market_price": "190",
                "market_value": "1520",
                "currency": "USD",
                "raw_ref": {"attributes": {}},
            }
        ],
    }
    match = match_expected_to_ib_v1(expected, ib)
    result = classify_mismatches_v1(
        day_utc="2026-04-24",
        expected_activity=expected,
        normalized_ib_trades=ib,
        normalized_ib_positions=ib_positions,
        normalized_ib_cash=None,
        match_result=match,
    )
    assert result["status"] == "FAIL"
    assert any(m["type"] == "POSITION_MISMATCH" and m["severity"] == "HIGH" for m in result["mismatches"])


def test_10_cash_mismatch_above_threshold_fail_high() -> None:
    expected = _expected_activity(fills=[])
    expected["expected_cash_balances"] = [{"account_id": "DU12345", "currency": "USD", "cash_balance": "100"}]
    ib = _normalized_trades(trades=[])
    ib_cash = {
        "schema_version": "normalized_ib_cash.v1",
        "day_utc": "2026-04-24",
        "source_report_path": "/tmp/ib.xml",
        "balances": [
            {
                "account_id": "DU12345",
                "currency": "USD",
                "cash_balance": "10",
                "raw_ref": {"attributes": {}},
            }
        ],
    }
    match = match_expected_to_ib_v1(expected, ib)
    result = classify_mismatches_v1(
        day_utc="2026-04-24",
        expected_activity=expected,
        normalized_ib_trades=ib,
        normalized_ib_positions=None,
        normalized_ib_cash=ib_cash,
        match_result=match,
    )
    assert result["status"] == "FAIL"
    assert any(m["type"] == "CASH_BALANCE_MISMATCH" and m["severity"] == "HIGH" for m in result["mismatches"])


def test_11_ai_packet_generated_from_reconciliation() -> None:
    reconciliation_payload = {
        "schema_version": "ib_reconciliation.v1",
        "day_utc": "2026-04-24",
        "status": "WARN",
        "aegis_expected_path": "/tmp/aegis_expected_activity.v1.json",
        "ib_actual_paths": {"normalized_trades": "/tmp/normalized_ib_trades.v1.json"},
        "matched_items": [],
        "mismatches": [
            {
                "mismatch_id": "MISMATCH-0001",
                "type": "LOW_CONFIDENCE_MATCH_ONLY",
                "severity": "MEDIUM",
                "symbol": "AAPL",
                "account_id": "",
                "aegis_attempt_id": "attempt-1",
                "ib_order_id": "0",
                "ib_perm_id": "0",
                "description": "fallback used",
                "evidence_paths": [],
                "deterministic_rule": "FALLBACK_SYMBOL_SIDE_QTY_TIME_WINDOW",
            }
        ],
        "summary": {},
        "generated_utc": "2026-04-24T15:00:00Z",
    }
    packet = build_ai_exception_packet_v1(
        day_utc="2026-04-24",
        reconciliation_payload=reconciliation_payload,
        reconciliation_path="/tmp/ib_reconciliation.v1.json",
    )
    md = render_ai_exception_packet_markdown_v1(packet)
    assert packet["schema_version"] == "ib_reconciliation_ai_packet.v1"
    assert "Is this safe to ignore?" in md
    assert "LOW_CONFIDENCE_MATCH_ONLY" in md


def test_12_ai_cannot_override_deterministic_reconciliation() -> None:
    reconciliation_payload = {"status": "FAIL"}
    review = default_ai_review_template_v1("2026-04-24")
    review["review_status"] = "NO_ACTION"
    with pytest.raises(RuntimeError):
        enforce_ai_review_guardrails_v1(
            reconciliation_payload=reconciliation_payload,
            ai_review_payload=review,
        )


def test_13_truth_surface_copy_written_under_runtime_data_only(tmp_path: Path) -> None:
    report_path = truth_surface_report_path_v1("2026-04-24")
    assert str(report_path).startswith("/home/node/constellation_runtime_data/")
    payload = {"schema_version": "test", "value": "ok"}
    out_path = (tmp_path / "payload.json").resolve()
    write_deterministic_json_v1(out_path, payload)
    assert json.loads(out_path.read_text(encoding="utf-8"))["value"] == "ok"


def test_14_repo_writes_avoided(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IB_RECONCILIATION_ALLOW_ANY_RUNTIME_ROOT", "1")
    monkeypatch.setenv("IB_RECONCILIATION_RUNTIME_ROOT", str((tmp_path / "ib_reconciliation_runtime").resolve()))
    resolved_root = runtime_root_v1()
    assert str(resolved_root).startswith(str(tmp_path.resolve()))
    with pytest.raises(PathContractError):
        ensure_not_under_repo_v1(REPO_ROOT / "constellation_2")
