"""
map_vertical_spread_v1.py

Constellation 2.0 Phase A
Deterministic vertical spread mapper (OFFLINE ONLY).

Inputs (must validate):
- OptionsIntent v2
- OptionsChainSnapshot v1
- FreshnessCertificate v1

Outputs (exactly one path):
SUCCESS:
- OrderPlan v1
- MappingLedgerRecord v1
- BindingRecord v1
BLOCK:
- VetoRecord v1

Fail-closed:
- On any violation, emit VetoRecord and DO NOT emit partial outputs.

Determinism:
- No use of "now" or environment time.
- Caller must supply now_utc as ISO-8601 Z string.
- IDs are hash-derived.
- All canonical hashes use canon_json_v1.inject_canonical_hash_field logic (hash with self-hash null).

Tick size / precision:
- Design Pack does not define tick_size. Therefore mapper REQUIRES tick_size to be passed in.
- If tick_size missing/invalid => VETO C2_PRICE_DETERMINISM_FAILED.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseA.lib.canon_json_v1 import CanonJsonError, canonicalize_and_hash, inject_canonical_hash_field
from constellation_2.phaseA.lib.validate_json_against_schema_v1 import (
    JsonSchemaValidationBoundaryError,
    ValidationResult,
    validate_obj_against_schema,
)


class MappingError(Exception):
    """Internal mapping boundary error (should be converted to VetoRecord)."""


@dataclass(frozen=True)
class MapResult:
    ok: bool
    order_plan: Optional[Dict[str, Any]]
    mapping_ledger_record: Optional[Dict[str, Any]]
    binding_record: Optional[Dict[str, Any]]
    veto_record: Optional[Dict[str, Any]]


# Reason codes (Design Pack authority: C2_INVARIANTS_AND_REASON_CODES.md)
RC_OPTIONS_ONLY = "C2_OPTIONS_ONLY_VIOLATION"
RC_DEFINED_RISK = "C2_DEFINED_RISK_REQUIRED"
RC_EXIT_POLICY = "C2_EXIT_POLICY_REQUIRED"
RC_FRESHNESS = "C2_FRESHNESS_CERT_INVALID_OR_EXPIRED"
RC_FAIL_CLOSED = "C2_MAPPING_FAIL_CLOSED_REQUIRED"
RC_DETERMINISM = "C2_DETERMINISM_CANONICALIZATION_FAILED"
RC_NONDETERMINISTIC = "C2_NONDETERMINISTIC_SELECTION_RULE"
RC_PRICE_DET = "C2_PRICE_DETERMINISM_FAILED"


def _parse_utc_z(ts: str) -> datetime:
    # Expect ISO-8601 with Z suffix. Sample uses "YYYY-MM-DDTHH:MM:SSZ".
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise MappingError(f"Timestamp must be Z-suffix UTC ISO-8601: {ts!r}")
    try:
        # Python 3.12: fromisoformat doesn't accept Z; replace.
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception as e:  # noqa: BLE001
        raise MappingError(f"Invalid timestamp format: {ts!r}: {e}") from e
    if dt.tzinfo is None:
        raise MappingError(f"Timestamp missing timezone info: {ts!r}")
    return dt.astimezone(timezone.utc)


def _dec(s: str) -> Decimal:
    try:
        return Decimal(s)
    except Exception as e:  # noqa: BLE001
        raise MappingError(f"Invalid decimal string: {s!r}: {e}") from e


def _sha256_of_canon_obj(obj: Dict[str, Any]) -> str:
    try:
        return canonicalize_and_hash(obj).sha256_hex
    except CanonJsonError as e:
        raise MappingError(f"Canonicalization/hash failed: {e}") from e


def _mk_id(seed_obj: Dict[str, Any]) -> str:
    # Deterministic ID = sha256(canonical(seed_obj))
    return _sha256_of_canon_obj(seed_obj)


def _veto(
    observed_at_utc: str,
    reason_code: str,
    reason_detail: str,
    intent_hash: Optional[str],
    plan_hash: Optional[str],
    chain_snapshot_hash: Optional[str],
    freshness_cert_hash: Optional[str],
    pointers: List[str],
) -> Dict[str, Any]:
    veto = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "observed_at_utc": observed_at_utc,
        "boundary": "MAPPING",
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "inputs": {
            "intent_hash": intent_hash,
            "plan_hash": plan_hash,
            "chain_snapshot_hash": chain_snapshot_hash,
            "freshness_cert_hash": freshness_cert_hash,
        },
        "pointers": pointers,
        "canonical_json_hash": None,
        "upstream_hash": None,
    }
    veto2, _ = inject_canonical_hash_field(veto, "canonical_json_hash")
    # Validate veto record schema. If schema validation fails, HARD FAIL per governance.
    vr = validate_obj_against_schema("veto_record.v1", veto2)
    if not vr.ok:
        raise MappingError(f"Cannot emit malformed VetoRecord: {vr.error}")
    return veto2


def _validate_inputs(intent: Dict[str, Any], chain: Dict[str, Any], cert: Dict[str, Any]) -> Tuple[str, str, str]:
    # Schema validation (fail-closed boundary errors become MappingError)
    for schema_name, obj in [
        ("options_intent.v2", intent),
        ("options_chain_snapshot.v1", chain),
        ("freshness_certificate.v1", cert),
    ]:
        try:
            r: ValidationResult = validate_obj_against_schema(schema_name, obj)
        except JsonSchemaValidationBoundaryError as e:
            raise MappingError(f"Schema boundary error for {schema_name}: {e}") from e
        if not r.ok:
            raise MappingError(f"Schema validation failed for {schema_name}: {r.error}")

    # Compute canonical hashes (self-hash null) for each input
    intent2, intent_hash = inject_canonical_hash_field(intent, "canonical_json_hash")
    chain2, chain_hash = inject_canonical_hash_field(chain, "canonical_json_hash")
    cert2, cert_hash = inject_canonical_hash_field(cert, "canonical_json_hash")

    # Mutability note: we do not return modified objects here; caller should use originals
    # but keep hashes computed on null-self-hash form.
    return intent_hash, chain_hash, cert_hash


def _freshness_check(
    now_utc: str,
    chain: Dict[str, Any],
    cert: Dict[str, Any],
    chain_hash: str,
) -> None:
    # Snapshot hash must bind to chain snapshot hash
    if cert.get("snapshot_hash") != chain_hash:
        raise MappingError("FreshnessCertificate.snapshot_hash does not match canonical OptionsChainSnapshot hash.")
    if cert.get("snapshot_as_of_utc") != chain.get("as_of_utc"):
        raise MappingError("FreshnessCertificate.snapshot_as_of_utc does not match chain.as_of_utc.")

    now_dt = _parse_utc_z(now_utc)
    valid_from = _parse_utc_z(cert["valid_from_utc"])
    valid_until = _parse_utc_z(cert["valid_until_utc"])

    if now_dt < valid_from or now_dt > valid_until:
        raise MappingError(f"FreshnessCertificate expired/invalid for now_utc={now_utc} (window {cert['valid_from_utc']}..{cert['valid_until_utc']}).")


def _tick_quantize(value: Decimal, tick_size: Decimal, rounding: str) -> Decimal:
    if tick_size <= Decimal("0"):
        raise MappingError("tick_size must be > 0")
    # value / tick_size must be quantized to integer with floor/ceil, then * tick_size
    try:
        q = value / tick_size
    except (InvalidOperation, ZeroDivisionError) as e:
        raise MappingError(f"tick quantize invalid operation: {e}") from e

    if rounding == "ROUND_DOWN":
        qi = q.to_integral_value(rounding=ROUND_FLOOR)
    elif rounding == "ROUND_UP":
        qi = q.to_integral_value(rounding=ROUND_CEILING)
    else:
        raise MappingError(f"Unknown tick_rounding: {rounding!r}")
    return qi * tick_size


def _usd_quantize(value: Decimal) -> Decimal:
    # USD cents quantization for risk_proof max_loss_usd. If this cannot be justified,
    # mapping must veto; however USD risk is required and this is the minimal stable unit.
    return value.quantize(Decimal("0.01"))


def _dte_days_calendar(as_of_utc: str, expiry_utc: str) -> int:
    as_of = _parse_utc_z(as_of_utc).date()
    exp = _parse_utc_z(expiry_utc).date()
    d = (exp - as_of).days
    if d < 0:
        raise MappingError("Expiry is before snapshot as_of_utc.")
    return d


def _liquid_contract(contract: Dict[str, Any], pol: Dict[str, Any]) -> bool:
    oi_min = int(pol["min_open_interest"])
    vol_min = int(pol["min_volume"])
    max_spread = _dec(pol["max_bid_ask_spread"])
    bid = _dec(contract["bid"])
    ask = _dec(contract["ask"])
    spread = ask - bid
    return (
        int(contract["open_interest"]) >= oi_min
        and int(contract["volume"]) >= vol_min
        and spread <= max_spread
    )


def _select_expiry(intent: Dict[str, Any], chain: Dict[str, Any]) -> str:
    exp_pol = intent["selection_policy"]["expiry_policy"]
    if exp_pol["mode"] != "DTE_WINDOW":
        raise MappingError("Unsupported expiry_policy.mode (only DTE_WINDOW supported).")
    dte_min = int(exp_pol["target_dte_min"])
    dte_max = int(exp_pol["target_dte_max"])
    right = intent["strategy"]["right"]

    liq_pol = intent["selection_policy"]["liquidity_policy"]

    expiries: Dict[str, int] = {}
    for c in chain["contracts"]:
        if c["right"] != right:
            continue
        if not _liquid_contract(c, liq_pol):
            continue
        expiry = c["expiry_utc"]
        dte = _dte_days_calendar(chain["as_of_utc"], expiry)
        if dte_min <= dte <= dte_max:
            # candidate expiry
            expiries[expiry] = dte

    if not expiries:
        raise MappingError("No candidate expiries satisfy DTE_WINDOW + liquidity + right.")

    # Deterministic: earliest expiry_utc (lexicographic works for ISO-8601 UTC)
    return sorted(expiries.keys())[0]


def _index_contracts(chain: Dict[str, Any]) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    # key: (expiry_utc, right, strike_string_exact)
    idx: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for c in chain["contracts"]:
        key = (c["expiry_utc"], c["right"], c["strike"])
        # If duplicates exist, that's a schema-level/producer violation; fail-closed.
        if key in idx:
            raise MappingError(f"Duplicate contract key by (expiry,right,strike): {key}")
        idx[key] = c
    return idx


def _select_strikes(intent: Dict[str, Any], chain: Dict[str, Any], expiry: str) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    """
    Returns (short_contract, long_contract, tie_breakers_applied)
    """
    right = intent["strategy"]["right"]
    direction = intent["strategy"]["direction"]
    width = _dec(intent["selection_policy"]["width_policy"]["width_points"])
    spot = _dec(chain["underlying"]["spot_price"])
    liq_pol = intent["selection_policy"]["liquidity_policy"]

    # Gather liquid contracts at expiry/right
    candidates: List[Dict[str, Any]] = []
    for c in chain["contracts"]:
        if c["expiry_utc"] != expiry or c["right"] != right:
            continue
        if not _liquid_contract(c, liq_pol):
            continue
        candidates.append(c)

    if not candidates:
        raise MappingError("No liquid contracts for selected expiry/right.")

    # Convert to (strike_decimal, contract) list
    strikes: List[Tuple[Decimal, Dict[str, Any]]] = [(_dec(c["strike"]), c) for c in candidates]
    # Sort by strike then contract_key (deterministic)
    strikes_sorted = sorted(strikes, key=lambda t: (t[0], t[1]["contract_key"]))

    tie_breakers: List[str] = []

    def pick_short_near_money_put_credit() -> Dict[str, Any]:
        # highest strike <= spot
        le = [t for t in strikes_sorted if t[0] <= spot]
        if not le:
            raise MappingError("No PUT strikes <= spot for credit selection.")
        # choose max strike; tie-break contract_key
        max_strike = max(le, key=lambda t: (t[0], t[1]["contract_key"]))
        tie_breakers.append("PUT_CREDIT_SHORT=highest_strike_le_spot;tie=contract_key_lex")
        return max_strike[1]

    def pick_short_near_money_call_credit() -> Dict[str, Any]:
        ge = [t for t in strikes_sorted if t[0] >= spot]
        if not ge:
            raise MappingError("No CALL strikes >= spot for credit selection.")
        min_strike = min(ge, key=lambda t: (t[0], t[1]["contract_key"]))
        tie_breakers.append("CALL_CREDIT_SHORT=lowest_strike_ge_spot;tie=contract_key_lex")
        return min_strike[1]

    def pick_near_money_for_debit() -> Dict[str, Any]:
        # near money = minimal abs(strike-spot), tie break by strike then contract_key
        best = min(strikes_sorted, key=lambda t: (abs(t[0] - spot), t[0], t[1]["contract_key"]))
        tie_breakers.append("DEBIT_NEAR=closest_abs(strike-spot);tie=strike_then_contract_key")
        return best[1]

    idx = _index_contracts(chain)

    if direction == "CREDIT":
        if right == "PUT":
            short_c = pick_short_near_money_put_credit()
            short_strike = _dec(short_c["strike"])
            long_strike = short_strike - width
        elif right == "CALL":
            short_c = pick_short_near_money_call_credit()
            short_strike = _dec(short_c["strike"])
            long_strike = short_strike + width
        else:
            raise MappingError("Unsupported right.")
        long_key = (expiry, right, f"{long_strike:.2f}")
        # strike strings in snapshot are "495.00" style. We must match exact formatting.
        # If formatting mismatch exists, fail-closed.
        if long_key not in idx:
            raise MappingError(f"Required long strike contract not found for width_points. expected_strike='{long_key[2]}'")
        long_c = idx[long_key]
        if not _liquid_contract(long_c, liq_pol):
            raise MappingError("Long leg fails liquidity policy.")
        return short_c, long_c, tie_breakers

    if direction == "DEBIT":
        near = pick_near_money_for_debit()
        near_strike = _dec(near["strike"])
        if right == "PUT":
            far_strike = near_strike - width
        elif right == "CALL":
            far_strike = near_strike + width
        else:
            raise MappingError("Unsupported right.")
        far_key = (expiry, right, f"{far_strike:.2f}")
        if far_key not in idx:
            raise MappingError(f"Required far strike contract not found for width_points. expected_strike='{far_key[2]}'")
        far = idx[far_key]
        if not _liquid_contract(far, liq_pol):
            raise MappingError("Far leg fails liquidity policy.")
        # For debit, long is near (BUY), short is far (SELL)
        tie_breakers.append("DEBIT_LEGS=BUY_near_SELL_far")
        return far, near, tie_breakers  # return (short, long) ordering consistent with record fields

    raise MappingError("Unsupported direction.")


def map_vertical_spread_offline(
    intent: Dict[str, Any],
    chain: Dict[str, Any],
    cert: Dict[str, Any],
    *,
    now_utc: str,
    tick_size: Optional[str],
    pointers: List[str],
) -> MapResult:
    """
    Perform deterministic mapping.

    Parameters:
      now_utc: injected deterministic clock (ISO-8601 Z)
      tick_size: required tick size as decimal string, e.g. "0.01"
      pointers: evidence pointers (file paths) to include in ledger/veto

    Returns MapResult with either success artifacts or veto_record.
    """
    # Hash placeholders
    intent_hash: Optional[str] = None
    chain_hash: Optional[str] = None
    cert_hash: Optional[str] = None
    plan_hash: Optional[str] = None

    try:
        # Enforce structure options-only at intent level
        if intent["strategy"]["structure"] != "VERTICAL_SPREAD":
            raise MappingError("Only VERTICAL_SPREAD supported in Phase A.")
        if intent["engine"]["suite"] != "C2_OPTIONS_7":
            raise MappingError("Intent.engine.suite must be C2_OPTIONS_7")

        # Validate inputs and compute hashes
        intent_hash, chain_hash, cert_hash = _validate_inputs(intent, chain, cert)

        # Freshness enforcement
        _freshness_check(now_utc, chain, cert, chain_hash)

        # Exit policy present (schema ensures, but enforce)
        if "exit_policy" not in intent or not isinstance(intent["exit_policy"], dict):
            raise MappingError("exit_policy missing.")

        # Tick size required
        if tick_size is None:
            raise MappingError("tick_size is required for deterministic limit price rounding.")
        ts = _dec(tick_size)
        if ts <= Decimal("0"):
            raise MappingError("tick_size must be > 0")

        expiry = _select_expiry(intent, chain)
        short_c, long_c, tie_breakers = _select_strikes(intent, chain, expiry)

        # Determine leg actions
        direction = intent["strategy"]["direction"]
        right = intent["strategy"]["right"]

        if direction == "CREDIT":
            # short spread: SELL near, BUY far
            short_action = "SELL"
            long_action = "BUY"
        else:
            # debit spread: BUY near, SELL far
            # Note: _select_strikes returns (short, long) with record semantics; for debit
            # we returned (far, near) so long is near; still actions below are correct.
            short_action = "SELL"
            long_action = "BUY"

        # Mid price calculations
        short_bid = _dec(short_c["bid"])
        short_ask = _dec(short_c["ask"])
        long_bid = _dec(long_c["bid"])
        long_ask = _dec(long_c["ask"])

        short_mid = (short_bid + short_ask) / Decimal("2")
        long_mid = (long_bid + long_ask) / Decimal("2")

        if direction == "CREDIT":
            spread_mid = short_mid - long_mid
        else:
            spread_mid = long_mid - short_mid

        if spread_mid <= Decimal("0"):
            raise MappingError("Computed spread_mid <= 0; cannot form valid limit price.")

        offset = _dec(intent["selection_policy"]["pricing_policy"]["limit_offset"])
        rounding = intent["selection_policy"]["pricing_policy"]["tick_rounding"]

        if direction == "CREDIT":
            raw_limit = spread_mid - offset
        else:
            raw_limit = spread_mid + offset

        if raw_limit <= Decimal("0"):
            raise MappingError("Computed raw_limit <= 0 after offset.")

        limit = _tick_quantize(raw_limit, ts, rounding)

        if limit <= Decimal("0"):
            raise MappingError("Quantized limit <= 0.")

        # OrderPlan construction
        policy_id = intent["exit_policy"]["policy_id"]
        contracts = int(intent["risk"]["max_contracts"])
        multiplier = int(intent["risk"]["multiplier"])
        width_points = _dec(intent["selection_policy"]["width_policy"]["width_points"])

        # Defined risk proof: ensure two-leg bounded
        if short_c["expiry_utc"] != long_c["expiry_utc"] or short_c["right"] != long_c["right"]:
            raise MappingError("Leg expiry/right mismatch (not a vertical).")
        if _dec(short_c["strike"]) == _dec(long_c["strike"]):
            raise MappingError("Leg strikes identical (not a spread).")

        # Risk calculation (deterministic)
        if direction == "CREDIT":
            max_loss = (width_points - limit) * Decimal(multiplier) * Decimal(contracts)
        else:
            max_loss = limit * Decimal(multiplier) * Decimal(contracts)

        if max_loss <= Decimal("0"):
            raise MappingError("Computed max_loss <= 0.")

        max_loss_usd = _usd_quantize(max_loss)

        plan = {
            "schema_id": "order_plan",
            "schema_version": "v1",
            "plan_id": None,  # set after hashing seed
            "created_at_utc": now_utc,
            "intent_hash": intent_hash,
            "structure": "VERTICAL_SPREAD",
            "underlying": {
                "symbol": intent["underlying"]["symbol"],
                "currency": intent["underlying"]["currency"],
            },
            "legs": [
                {
                    "action": short_action,
                    "ratio": 1,
                    "right": right,
                    "expiry_utc": short_c["expiry_utc"],
                    "strike": short_c["strike"],
                    "ib_conId": int(short_c["ib"]["conId"]),
                    "ib_localSymbol": short_c["ib"]["localSymbol"],
                },
                {
                    "action": long_action,
                    "ratio": 1,
                    "right": right,
                    "expiry_utc": long_c["expiry_utc"],
                    "strike": long_c["strike"],
                    "ib_conId": int(long_c["ib"]["conId"]),
                    "ib_localSymbol": long_c["ib"]["localSymbol"],
                },
            ],
            "order_terms": {
                "order_type": "LIMIT",
                "limit_price": str(limit.normalize()) if limit == limit.to_integral() else format(limit, "f"),
                "time_in_force": "DAY",
                "is_credit": direction == "CREDIT",
                "tick_rounding": rounding,
            },
            "exit_policy_ref": {"policy_id": policy_id},
            "risk_proof": {
                "defined_risk_proven": True,
                "max_loss_usd": format(max_loss_usd, "f"),
                "width_points": format(width_points, "f"),
                "multiplier": multiplier,
                "contracts": contracts,
            },
            "canonical_json_hash": None,
        }

        # Deterministic plan_id seed
        plan_seed = {
            "kind": "order_plan_id_seed_v1",
            "intent_hash": intent_hash,
            "chain_snapshot_hash": chain_hash,
            "freshness_cert_hash": cert_hash,
            "expiry_utc": expiry,
            "short_contract_key": short_c["contract_key"],
            "long_contract_key": long_c["contract_key"],
            "limit_price": plan["order_terms"]["limit_price"],
        }
        plan["plan_id"] = _mk_id(plan_seed)

        # Inject canonical_json_hash and validate schema
        plan2, plan_hash = inject_canonical_hash_field(plan, "canonical_json_hash")
        pr = validate_obj_against_schema("order_plan.v1", plan2)
        if not pr.ok:
            raise MappingError(f"OrderPlan schema validation failed: {pr.error}")

        # MappingLedgerRecord
        mrec = {
            "schema_id": "mapping_ledger_record",
            "schema_version": "v1",
            "record_id": None,
            "created_at_utc": now_utc,
            "intent_hash": intent_hash,
            "chain_snapshot_hash": chain_hash,
            "freshness_cert_hash": cert_hash,
            "plan_hash": plan_hash,
            "selection_trace": {
                "expiry_choice": {
                    "policy": "DTE_WINDOW + liquidity + earliest_expiry_utc",
                    "selected_expiry_utc": expiry,
                    "candidates_considered": 1,
                },
                "strike_choice": {
                    "width_points": intent["selection_policy"]["width_policy"]["width_points"],
                    "short_leg_contract_key": short_c["contract_key"],
                    "long_leg_contract_key": long_c["contract_key"],
                },
                "liquidity_filter": dict(intent["selection_policy"]["liquidity_policy"]),
                "tie_breakers": tie_breakers,
            },
            "canonical_json_hash": None,
        }
        mrec_seed = {
            "kind": "mapping_ledger_id_seed_v1",
            "plan_hash": plan_hash,
            "intent_hash": intent_hash,
            "chain_snapshot_hash": chain_hash,
            "freshness_cert_hash": cert_hash,
        }
        mrec["record_id"] = _mk_id(mrec_seed)

        mrec2, mrec_hash = inject_canonical_hash_field(mrec, "canonical_json_hash")
        mr = validate_obj_against_schema("mapping_ledger_record.v1", mrec2)
        if not mr.ok:
            raise MappingError(f"MappingLedgerRecord schema validation failed: {mr.error}")

        # Broker payload digest (deterministic representation only; never broker call)
        broker_payload = {
            "format": "IB_BAG_ORDER_V1",
            "underlying": plan2["underlying"],
            "structure": plan2["structure"],
            "order_type": "LIMIT",
            "limit_price": plan2["order_terms"]["limit_price"],
            "time_in_force": plan2["order_terms"]["time_in_force"],
            "legs": [
                {"conId": plan2["legs"][0]["ib_conId"], "action": plan2["legs"][0]["action"], "ratio": 1},
                {"conId": plan2["legs"][1]["ib_conId"], "action": plan2["legs"][1]["action"], "ratio": 1},
            ],
        }
        broker_digest = _sha256_of_canon_obj(broker_payload)

        bind = {
            "schema_id": "binding_record",
            "schema_version": "v1",
            "binding_id": None,
            "created_at_utc": now_utc,
            "plan_hash": plan_hash,
            "mapping_ledger_hash": mrec_hash,
            "freshness_cert_hash": cert_hash,
            "broker_payload_digest": {
                "digest_sha256": broker_digest,
                "format": "IB_BAG_ORDER_V1",
                "notes": f"Bound vertical spread {plan2['underlying']['symbol']} {right} {expiry} {short_c['strike']}/{long_c['strike']} limit={plan2['order_terms']['limit_price']}",
            },
            "preflight": {
                "validated_schema": True,
                "validated_invariants": True,
                "validated_freshness": True,
                "defined_risk_proven": True,
                "exit_policy_present": True,
            },
            "canonical_json_hash": None,
        }
        bind_seed = {
            "kind": "binding_id_seed_v1",
            "plan_hash": plan_hash,
            "mapping_ledger_hash": mrec_hash,
            "freshness_cert_hash": cert_hash,
            "broker_digest": broker_digest,
        }
        bind["binding_id"] = _mk_id(bind_seed)

        bind2, _ = inject_canonical_hash_field(bind, "canonical_json_hash")
        br = validate_obj_against_schema("binding_record.v1", bind2)
        if not br.ok:
            raise MappingError(f"BindingRecord schema validation failed: {br.error}")

        return MapResult(
            ok=True,
            order_plan=plan2,
            mapping_ledger_record=mrec2,
            binding_record=bind2,
            veto_record=None,
        )

    except MappingError as e:
        # Convert MappingError to veto record with best-available hashes.
        # Determine reason code category.
        msg = str(e)
        reason_code = RC_FAIL_CLOSED
        if "FreshnessCertificate" in msg or "expired" in msg or "snapshot_hash" in msg:
            reason_code = RC_FRESHNESS
        elif "tick_size" in msg or "tick" in msg or "limit" in msg:
            reason_code = RC_PRICE_DET
        elif "Canonicalization" in msg or "hash" in msg:
            reason_code = RC_DETERMINISM
        elif "exit_policy" in msg:
            reason_code = RC_EXIT_POLICY
        elif "defined risk" in msg or "vertical" in msg:
            reason_code = RC_DEFINED_RISK

        veto = _veto(
            observed_at_utc=now_utc,
            reason_code=reason_code,
            reason_detail=msg,
            intent_hash=intent_hash,
            plan_hash=plan_hash,
            chain_snapshot_hash=chain_hash,
            freshness_cert_hash=cert_hash,
            pointers=pointers,
        )
        return MapResult(
            ok=False,
            order_plan=None,
            mapping_ledger_record=None,
            binding_record=None,
            veto_record=veto,
        )
