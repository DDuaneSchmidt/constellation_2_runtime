# constellation_2/phaseB/lib/build_options_chain_snapshot_v1.py
#
# Constellation 2.0 — Phase B
# Build OptionsChainSnapshot v1 from a Phase B raw input object (offline only).
#
# Fail-closed:
# - Any invalid raw input => raise (tool must exit non-zero, no partial outputs)
# - Any schema validation failure => raise
#
# Determinism:
# - No use of system time
# - All decimals emitted as fixed 2dp strings
# - Contracts sorted lexicographically by contract_key
# - Features emitted in the same deterministic order as contracts

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from constellation_2.phaseB.lib.canon_json_v1 import CanonicalizationError, _walk_assert_no_floats
from constellation_2.phaseB.lib.decimal_determinism_v1 import (
    DecimalDeterminismError,
    decimal_to_str_2dp_v1,
    mid_2dp_str_v1,
    parse_decimal_strict_v1,
    sub_2dp_str_v1,
)
from constellation_2.phaseB.lib.validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1


class RawInputError(Exception):
    pass


def _parse_utc_z(ts: Any, field_name: str) -> str:
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise RawInputError(f"TIMESTAMP_NOT_Z_UTC: {field_name}")
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception as e:  # noqa: BLE001
        raise RawInputError(f"TIMESTAMP_INVALID: {field_name}: {e}") from e
    if dt.tzinfo is None:
        raise RawInputError(f"TIMESTAMP_MISSING_TZ: {field_name}")
    # Normalize to UTC and return with 'Z'
    dt2 = dt.astimezone(timezone.utc)
    return dt2.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _require_dict(x: Any, field_name: str) -> Dict[str, Any]:
    if not isinstance(x, dict):
        raise RawInputError(f"FIELD_NOT_OBJECT: {field_name}")
    return x


def _require_list(x: Any, field_name: str) -> List[Any]:
    if not isinstance(x, list):
        raise RawInputError(f"FIELD_NOT_ARRAY: {field_name}")
    return x


def _require_str(x: Any, field_name: str, min_len: int = 1, max_len: int = 200) -> str:
    if not isinstance(x, str):
        raise RawInputError(f"FIELD_NOT_STRING: {field_name}")
    s = x.strip()
    if len(s) < min_len:
        raise RawInputError(f"FIELD_STRING_TOO_SHORT: {field_name}")
    if len(s) > max_len:
        raise RawInputError(f"FIELD_STRING_TOO_LONG: {field_name}")
    return s


def _require_int_nonneg(x: Any, field_name: str) -> int:
    if not isinstance(x, int):
        raise RawInputError(f"FIELD_NOT_INT: {field_name}")
    if x < 0:
        raise RawInputError(f"FIELD_INT_NEGATIVE: {field_name}")
    return x


def _dte_days_calendar(as_of_utc: str, expiry_utc: str) -> int:
    as_of = datetime.fromisoformat(as_of_utc.replace("Z", "+00:00")).date()
    exp = datetime.fromisoformat(expiry_utc.replace("Z", "+00:00")).date()
    d = (exp - as_of).days
    if d < 0:
        raise RawInputError("EXPIRY_BEFORE_AS_OF_FORBIDDEN")
    return d


def _contract_key(symbol: str, expiry_utc: str, right: str, strike_2dp: str) -> str:
    return f"{symbol}|{expiry_utc}|{right}|{strike_2dp}"


def build_options_chain_snapshot_v1(raw: Dict[str, Any], repo_root: Path) -> Dict[str, Any]:
    # Determinism guard: forbid floats anywhere in raw input.
    try:
        _walk_assert_no_floats(raw, "$")
    except CanonicalizationError as e:
        raise RawInputError(f"RAW_FLOAT_FORBIDDEN: {e}") from e

    # Required top-level fields
    as_of_utc = _parse_utc_z(raw.get("as_of_utc"), "as_of_utc")

    underlying_raw = _require_dict(raw.get("underlying"), "underlying")
    symbol = _require_str(underlying_raw.get("symbol"), "underlying.symbol", min_len=1, max_len=16)

    spot_as_of_utc = _parse_utc_z(underlying_raw.get("spot_as_of_utc"), "underlying.spot_as_of_utc")
    try:
        spot_price_2dp = decimal_to_str_2dp_v1(parse_decimal_strict_v1(underlying_raw.get("spot_price"), "underlying.spot_price"), "underlying.spot_price")
    except DecimalDeterminismError as e:
        raise RawInputError(str(e)) from e

    prov_raw = _require_dict(raw.get("provenance"), "provenance")
    provenance = {
        "source": _require_str(prov_raw.get("source"), "provenance.source", max_len=200),
        "capture_method": _require_str(prov_raw.get("capture_method"), "provenance.capture_method", max_len=200),
        "capture_host": _require_str(prov_raw.get("capture_host"), "provenance.capture_host", max_len=200),
        "capture_run_id": _require_str(prov_raw.get("capture_run_id"), "provenance.capture_run_id", max_len=200),
    }

    # Derivation policy (raw->snapshot)
    pol_raw = _require_dict(raw.get("policy"), "policy")
    dte_method = _require_str(pol_raw.get("dte_method"), "policy.dte_method", max_len=64)
    if dte_method != "CALENDAR_DAYS_UTC":
        raise RawInputError("POLICY_DTE_METHOD_UNSUPPORTED")

    pricing_pol_raw = _require_dict(pol_raw.get("pricing_policy"), "policy.pricing_policy")
    mid_def = _require_str(pricing_pol_raw.get("mid_definition"), "policy.pricing_policy.mid_definition", max_len=64)
    if mid_def != "(bid+ask)/2":
        raise RawInputError("POLICY_MID_DEFINITION_UNSUPPORTED")

    liq_pol_raw = _require_dict(pol_raw.get("liquidity_policy"), "policy.liquidity_policy")
    min_oi = _require_int_nonneg(liq_pol_raw.get("min_open_interest"), "policy.liquidity_policy.min_open_interest")
    min_vol = _require_int_nonneg(liq_pol_raw.get("min_volume"), "policy.liquidity_policy.min_volume")
    try:
        max_spread_2dp = decimal_to_str_2dp_v1(parse_decimal_strict_v1(liq_pol_raw.get("max_bid_ask_spread"), "policy.liquidity_policy.max_bid_ask_spread"), "policy.liquidity_policy.max_bid_ask_spread")
    except DecimalDeterminismError as e:
        raise RawInputError(str(e)) from e

    derivation_policy = {
        "dte_method": "CALENDAR_DAYS_UTC",
        "liquidity_policy": {
            "min_open_interest": min_oi,
            "min_volume": min_vol,
            "max_bid_ask_spread": max_spread_2dp,
        },
        "pricing_policy": {"mid_definition": "(bid+ask)/2"},
    }

    contracts_in = _require_list(raw.get("contracts"), "contracts")
    if len(contracts_in) < 1:
        raise RawInputError("CONTRACTS_EMPTY_FORBIDDEN")

    # Normalize contracts and compute deterministic keys
    normalized_contracts: List[Dict[str, Any]] = []
    for i, c in enumerate(contracts_in):
        cobj = _require_dict(c, f"contracts[{i}]")

        expiry_utc = _parse_utc_z(cobj.get("expiry_utc"), f"contracts[{i}].expiry_utc")
        right = _require_str(cobj.get("right"), f"contracts[{i}].right", max_len=8)
        if right not in ("CALL", "PUT"):
            raise RawInputError(f"CONTRACT_RIGHT_INVALID: contracts[{i}].right")

        try:
            strike_2dp = decimal_to_str_2dp_v1(parse_decimal_strict_v1(cobj.get("strike"), f"contracts[{i}].strike"), f"contracts[{i}].strike")
            bid_2dp = decimal_to_str_2dp_v1(parse_decimal_strict_v1(cobj.get("bid"), f"contracts[{i}].bid"), f"contracts[{i}].bid")
            ask_2dp = decimal_to_str_2dp_v1(parse_decimal_strict_v1(cobj.get("ask"), f"contracts[{i}].ask"), f"contracts[{i}].ask")
        except DecimalDeterminismError as e:
            raise RawInputError(str(e)) from e

        oi = _require_int_nonneg(cobj.get("open_interest"), f"contracts[{i}].open_interest")
        vol = _require_int_nonneg(cobj.get("volume"), f"contracts[{i}].volume")

        ib_raw = _require_dict(cobj.get("ib"), f"contracts[{i}].ib")
        conid = ib_raw.get("conId")
        if not isinstance(conid, int) or conid < 1:
            raise RawInputError(f"IB_CONID_INVALID: contracts[{i}].ib.conId")

        multiplier = ib_raw.get("multiplier")
        if multiplier != 100:
            raise RawInputError(f"IB_MULTIPLIER_INVALID: contracts[{i}].ib.multiplier")

        ib = {
            "conId": conid,
            "localSymbol": _require_str(ib_raw.get("localSymbol"), f"contracts[{i}].ib.localSymbol", max_len=64),
            "tradingClass": _require_str(ib_raw.get("tradingClass"), f"contracts[{i}].ib.tradingClass", max_len=64),
            "exchange": _require_str(ib_raw.get("exchange"), f"contracts[{i}].ib.exchange", max_len=16),
            "currency": _require_str(ib_raw.get("currency"), f"contracts[{i}].ib.currency", min_len=3, max_len=3),
            "multiplier": 100,
        }

        ck = _contract_key(symbol, expiry_utc, right, strike_2dp)

        normalized_contracts.append(
            {
                "contract_key": ck,
                "expiry_utc": expiry_utc,
                "strike": strike_2dp,
                "right": right,
                "bid": bid_2dp,
                "ask": ask_2dp,
                "open_interest": oi,
                "volume": vol,
                "ib": ib,
            }
        )

    # Deterministic ordering
    normalized_contracts.sort(key=lambda x: x["contract_key"])

    # Derived features, aligned to contract order
    features: List[Dict[str, Any]] = []
    for c in normalized_contracts:
        dte = _dte_days_calendar(as_of_utc, c["expiry_utc"])
        # Spread and mid are deterministically 2dp; fail closed if ask<bid or negative.
        spread = sub_2dp_str_v1(c["ask"], c["bid"], "derived.bid_ask_spread")
        mid = mid_2dp_str_v1(c["bid"], c["ask"], "derived.mid")

        # Liquidity policy
        # is_liquid: oi >= min_oi AND vol >= min_vol AND spread <= max_spread
        try:
            spread_dec = parse_decimal_strict_v1(spread, "derived.bid_ask_spread")
            max_spread_dec = parse_decimal_strict_v1(max_spread_2dp, "policy.liquidity_policy.max_bid_ask_spread")
        except DecimalDeterminismError as e:
            raise RawInputError(str(e)) from e

        is_liquid = (int(c["open_interest"]) >= min_oi) and (int(c["volume"]) >= min_vol) and (spread_dec <= max_spread_dec)

        features.append(
            {
                "contract_key": c["contract_key"],
                "dte_days": dte,
                "is_liquid": bool(is_liquid),
                "bid_ask_spread": spread,
                "mid": mid,
            }
        )

    snapshot = {
        "schema_id": "options_chain_snapshot",
        "schema_version": "v1",
        "as_of_utc": as_of_utc,
        "underlying": {
            "symbol": symbol,
            "spot_price": spot_price_2dp,
            "spot_as_of_utc": spot_as_of_utc,
        },
        "contracts": normalized_contracts,
        "derived": {
            "derivation_policy": derivation_policy,
            "features": features,
        },
        "provenance": provenance,
        "canonical_json_hash": None,
    }

    # Schema validate (fail-closed)
    try:
        validate_against_repo_schema_v1(
            snapshot,
            repo_root=repo_root,
            schema_relpath="constellation_2/schemas/options_chain_snapshot.v1.schema.json",
        )
    except SchemaValidationError as e:
        raise RawInputError(f"SNAPSHOT_SCHEMA_INVALID: {e}") from e

    return snapshot
