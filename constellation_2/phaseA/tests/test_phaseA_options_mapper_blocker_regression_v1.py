from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
from pathlib import Path

from constellation_2.phaseA.lib.canon_json_v1 import inject_canonical_hash_field, load_json_file
from constellation_2.phaseA.lib.map_vertical_spread_v1 import map_vertical_spread_offline


def _runtime_like_intent(*, min_volume: int = 0, max_bid_ask_spread: str = "0.10") -> dict:
    intent = load_json_file(Path("/home/node/constellation/constellation_2/acceptance/samples/sample_options_intent.v2.json"))
    intent["created_at_utc"] = "2026-04-24T21:07:00Z"
    intent["selection_policy"]["expiry_policy"]["target_dte_min"] = 1
    intent["selection_policy"]["expiry_policy"]["target_dte_max"] = 7
    intent["selection_policy"]["liquidity_policy"]["min_open_interest"] = 0
    intent["selection_policy"]["liquidity_policy"]["min_volume"] = int(min_volume)
    intent["selection_policy"]["liquidity_policy"]["max_bid_ask_spread"] = str(max_bid_ask_spread)
    intent["canonical_json_hash"] = None
    return intent


def _contract(*, strike: str, bid: str, ask: str, conid: int) -> dict:
    return {
        "contract_key": f"SPY|2026-04-27T00:00:00Z|PUT|{strike}",
        "expiry_utc": "2026-04-27T00:00:00Z",
        "strike": strike,
        "right": "PUT",
        "bid": bid,
        "ask": ask,
        "open_interest": 0,
        "volume": 0,
        "ib": {
            "conId": conid,
            "localSymbol": f"SPY   260427P{strike.replace('.', '').rjust(8, '0')}",
            "tradingClass": "SPY",
            "exchange": "SMART",
            "currency": "USD",
            "multiplier": 100,
        },
    }


def _runtime_like_chain() -> dict:
    return {
        "schema_id": "options_chain_snapshot",
        "schema_version": "v1",
        "as_of_utc": "2026-04-24T21:06:56Z",
        "underlying": {"symbol": "SPY", "spot_price": "713.95", "spot_as_of_utc": "2026-04-24T21:06:56Z"},
        "contracts": [
            _contract(strike="700.00", bid="0.25", ask="0.26", conid=872979981),
            _contract(strike="703.00", bid="0.45", ask="0.46", conid=873301777),
            _contract(strike="705.00", bid="0.66", ask="0.67", conid=872979996),
            _contract(strike="709.00", bid="1.28", ask="1.30", conid=873301850),
            _contract(strike="715.00", bid="3.29", ask="3.39", conid=872980041),
        ],
        "provenance": {
            "source": "TEST_FIXTURE",
            "capture_method": "UNIT_TEST",
            "capture_host": "local",
            "capture_run_id": "phasea_blocker_regression",
        },
        "canonical_json_hash": None,
    }


def _freshness_for_chain(chain: dict) -> dict:
    _, chain_hash = inject_canonical_hash_field(chain, "canonical_json_hash")
    return {
        "schema_id": "freshness_certificate",
        "schema_version": "v1",
        "issued_at_utc": "2026-04-24T21:07:00Z",
        "valid_from_utc": "2026-04-24T21:06:56Z",
        "valid_until_utc": "2026-04-24T21:11:56Z",
        "snapshot_hash": chain_hash,
        "snapshot_as_of_utc": "2026-04-24T21:06:56Z",
        "source": "TEST_FIXTURE",
        "capture_method": "UNIT_TEST",
        "policy": {"max_age_seconds": 300, "clock_skew_tolerance_seconds": 5},
        "canonical_json_hash": None,
    }


def _map(intent: dict, chain: dict, cert: dict):
    return map_vertical_spread_offline(
        intent=intent,
        chain=chain,
        cert=cert,
        now_utc="2026-04-24T21:07:33Z",
        tick_size="0.01",
        pointers=["intent.json", "chain.json", "cert.json"],
    )


def test_valid_options_intent_and_snapshot_map_to_selected_put_spread() -> None:
    result = _map(_runtime_like_intent(min_volume=0), _runtime_like_chain(), _freshness_for_chain(_runtime_like_chain()))
    assert result.ok is True
    assert result.order_plan is not None
    legs = result.order_plan["legs"]
    assert legs[0]["action"] == "SELL"
    assert legs[0]["right"] == "PUT"
    assert legs[0]["strike"] == "705.00"
    assert legs[1]["action"] == "BUY"
    assert legs[1]["right"] == "PUT"
    assert legs[1]["strike"] == "700.00"


def test_missing_mapper_required_fields_fail_closed_with_clear_blocker() -> None:
    intent = _runtime_like_intent(min_volume=0)
    del intent["selection_policy"]["pricing_policy"]
    result = _map(intent, _runtime_like_chain(), _freshness_for_chain(_runtime_like_chain()))
    assert result.ok is False
    assert result.veto_record is not None
    assert result.veto_record["reason_code"] == "C2_MAPPING_FAIL_CLOSED_REQUIRED"
    assert "Schema validation failed for options_intent.v2" in str(result.veto_record["reason_detail"])


def test_no_valid_quoted_puts_fail_closed() -> None:
    intent = _runtime_like_intent(min_volume=0, max_bid_ask_spread="0.001")
    result = _map(intent, _runtime_like_chain(), _freshness_for_chain(_runtime_like_chain()))
    assert result.ok is False
    assert result.veto_record is not None
    assert "No candidate expiries satisfy DTE_WINDOW + liquidity + right." in str(result.veto_record["reason_detail"])


def test_quote_decimal_normalization_still_maps() -> None:
    chain = _runtime_like_chain()
    for c in chain["contracts"]:
        c["bid"] = format(Decimal(c["bid"]).normalize(), "f")
        c["ask"] = format(Decimal(c["ask"]).normalize(), "f")
    result = _map(_runtime_like_intent(min_volume=0), chain, _freshness_for_chain(chain))
    assert result.ok is True
    assert result.order_plan is not None
    assert result.order_plan["order_terms"]["limit_price"] == "0.39"


def test_selection_is_deterministic_for_identical_inputs() -> None:
    intent = _runtime_like_intent(min_volume=0)
    chain = _runtime_like_chain()
    cert = _freshness_for_chain(chain)
    r1 = _map(deepcopy(intent), deepcopy(chain), deepcopy(cert))
    r2 = _map(deepcopy(intent), deepcopy(chain), deepcopy(cert))
    assert r1.ok is True
    assert r2.ok is True
    assert r1.order_plan is not None and r2.order_plan is not None
    assert r1.mapping_ledger_record is not None and r2.mapping_ledger_record is not None
    assert r1.order_plan["canonical_json_hash"] == r2.order_plan["canonical_json_hash"]
    assert r1.mapping_ledger_record["canonical_json_hash"] == r2.mapping_ledger_record["canonical_json_hash"]


def test_max_defined_loss_cents_is_computable_from_mapping_output() -> None:
    result = _map(_runtime_like_intent(min_volume=0), _runtime_like_chain(), _freshness_for_chain(_runtime_like_chain()))
    assert result.ok is True
    assert result.order_plan is not None
    max_loss_usd = Decimal(str(result.order_plan["risk_proof"]["max_loss_usd"]))
    max_defined_loss_cents = int((max_loss_usd * Decimal("100")).quantize(Decimal("1")))
    assert max_defined_loss_cents == 46100
