#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.c2_risk_policy_loader_v1 import RiskPolicyLoaderError, load_risk_policy_for_engine_or_fail
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from ops.tools.aegis_artifact_ledger_v1 import infer_runtime_root_v1, write_artifact_ledger_record_v1
from ops.tools.aegis_runtime_mode_v1 import runtime_mode_from_truth_root_v1

PRODUCER = "ops/tools/run_risk_definition_contract_v1.py"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/risk_definition_contract.v1.schema.json"
ORDER_PLAN_SCHEMA_RELPATH = "constellation_2/schemas/order_plan.v1.schema.json"
OPTIONS_CHAIN_SCHEMA_RELPATH = "constellation_2/schemas/options_chain_snapshot.v1.schema.json"


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"])
        return out.decode("utf-8").strip()
    except Exception:
        return "0" * 40


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload_bytes = canonical_json_bytes_v1(payload) + b"\n"
    path.write_bytes(payload_bytes)
    return canonical_hash_for_c2_artifact_v1(payload)


def risk_contract_path_v1(*, truth_root: Path, day_utc: str, intent_hash: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "risk_definition_contract_v1"
        / day_utc
        / intent_hash.lower()
        / "risk_definition_contract.v1.json"
    )


def _intent_path(*, truth_root: Path, day_utc: str, intent_hash: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "intents_v1"
        / "snapshots"
        / day_utc
        / f"{intent_hash.lower()}.exposure_intent.v1.json"
    )


def _find_intent_by_id(*, truth_root: Path, day_utc: str, intent_id: str) -> tuple[str, Path, dict[str, Any]]:
    day_root = Path(truth_root).resolve() / "intents_v1" / "snapshots" / day_utc
    if not day_root.exists():
        raise FileNotFoundError(f"INTENTS_DAY_DIR_MISSING:{day_root}")
    matches: list[tuple[int, str, str, Path, dict[str, Any]]] = []
    for path in sorted(day_root.glob("*.json")):
        obj = _read_json(path)
        if str(obj.get("intent_id") or "").strip() == intent_id:
            name = path.name
            suffix = ".exposure_intent.v1.json"
            intent_hash = name[: -len(suffix)] if name.endswith(suffix) else canonical_hash_for_c2_artifact_v1(obj)
            stat = path.stat()
            matches.append((int(stat.st_mtime_ns), name, intent_hash.lower(), path.resolve(), obj))
    if matches:
        _mtime_ns, _name, resolved_hash, resolved_path, resolved_obj = sorted(matches, key=lambda item: (item[0], item[1]))[-1]
        return resolved_hash, resolved_path, resolved_obj
    raise FileNotFoundError(f"EXPOSURE_INTENT_NOT_FOUND:intent_id={intent_id}")


def _market_reference_price(*, truth_root: Path, day_utc: str, symbol: str) -> tuple[Decimal | None, str]:
    sym = str(symbol or "").strip().upper()
    snapshot_path = (
        Path(truth_root).resolve()
        / "market_data_snapshot_v1"
        / "snapshots"
        / day_utc
        / f"{sym}.market_data_snapshot.v1.json"
    )
    if snapshot_path.exists() and snapshot_path.is_file():
        payload = _read_json(snapshot_path)
        if str(payload.get("day_utc") or "").strip() == day_utc and str(payload.get("symbol") or "").strip().upper() == sym:
            value = payload.get("close")
            if value is not None:
                dec = Decimal(str(value).strip())
                if dec > 0:
                    return dec, str(snapshot_path.resolve())
    jsonl_path = Path(truth_root).resolve() / "market_data_snapshot_v1" / sym / f"{day_utc[:4]}.jsonl"
    if not jsonl_path.exists() or not jsonl_path.is_file():
        return None, ""
    latest: Decimal | None = None
    cutoff = f"{day_utc}T23:59:59Z"
    for line in jsonl_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            continue
        if str(row.get("symbol") or "").strip().upper() != sym:
            continue
        ts = str(row.get("timestamp_utc") or "").strip()
        if not ts or ts > cutoff or row.get("close") is None:
            continue
        dec = Decimal(str(row.get("close")).strip())
        if dec > 0:
            latest = dec
    return (latest, str(jsonl_path.resolve())) if latest is not None else (None, "")


def _price_text(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _risk_cents(*, reference_price: Decimal, stop_loss_bps: int) -> int:
    cents = reference_price * Decimal("100") * Decimal(stop_loss_bps) / Decimal("10000")
    return int(cents.to_integral_value(rounding=ROUND_CEILING))


def _stop_price(*, reference_price: Decimal, side: str, stop_loss_bps: int) -> str:
    distance = Decimal(stop_loss_bps) / Decimal("10000")
    normalized_side = str(side or "BUY").strip().upper()
    if normalized_side == "SELL":
        return _price_text(reference_price * (Decimal("1") + distance))
    return _price_text(reference_price * (Decimal("1") - distance))


def _latest_phasec_order_plan(*, truth_root: Path, day_utc: str, intent_hash: str) -> Path | None:
    roots = [Path(truth_root).resolve()]
    sleeve_root = (Path(truth_root).resolve().parent / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    if sleeve_root.exists():
        roots.append(sleeve_root)
    candidates: list[Path] = []
    for root in roots:
        candidates.extend((root / "phaseC_preflight_v1" / day_utc).glob(f"attempt_*/{intent_hash.lower()}/order_plan.v1.json"))
    return sorted(candidates, key=lambda item: (item.stat().st_mtime_ns, str(item)))[-1].resolve() if candidates else None


def _latest_phasec_veto(*, truth_root: Path, day_utc: str, intent_hash: str) -> Path | None:
    roots = [Path(truth_root).resolve()]
    sleeve_root = (Path(truth_root).resolve().parent / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    if sleeve_root.exists():
        roots.append(sleeve_root)
    candidates: list[Path] = []
    for root in roots:
        candidates.extend((root / "phaseC_preflight_v1" / day_utc).glob(f"attempt_*/{intent_hash.lower()}.veto_record.v1.json"))
    return sorted(candidates, key=lambda item: (item.stat().st_mtime_ns, str(item)))[-1].resolve() if candidates else None


def _phasec_veto_blocker(veto_path: Path | None) -> str:
    if veto_path is None or not veto_path.exists() or not veto_path.is_file():
        return ""
    try:
        veto = _read_json(veto_path)
    except Exception:
        return "RISK_CONTRACT_DEFINED_RISK_PHASEC_VETO_UNREADABLE"
    reason = str(veto.get("reason_detail") or veto.get("reason_code") or "UNKNOWN").strip()
    normalized = reason.split(":", 1)[0].strip().upper() if reason else "UNKNOWN"
    return f"RISK_CONTRACT_DEFINED_RISK_PHASEC_VETO:{normalized}"


def _options_chain_ref(order_plan: dict[str, Any]) -> str:
    for ref in order_plan.get("source_refs") or order_plan.get("evidence_refs") or []:
        if not isinstance(ref, dict):
            continue
        raw = str(ref.get("path") or ref.get("artifact_path") or "").strip()
        if raw and "options_chain" in raw:
            return raw
    raw = str(order_plan.get("options_chain_ref") or "").strip()
    return raw


def _validate_with_schema(payload: dict[str, Any], relpath: str, error_prefix: str) -> None:
    schema = json.loads((REPO_ROOT / relpath).read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError(error_prefix + ":" + ";".join(error.message for error in errors[:3]))


def _structure_decision_supply_path(order_plan_path: Path) -> Path:
    return (order_plan_path.parent / "structure_decision_supply.v1.json").resolve()


def _options_chain_ref_from_structure_supply(order_plan_path: Path) -> tuple[str, str]:
    supply_path = _structure_decision_supply_path(order_plan_path)
    if not supply_path.exists() or not supply_path.is_file():
        return "", ""
    supply = _read_json(supply_path)
    market_open_data = supply.get("market_open_data") if isinstance(supply.get("market_open_data"), dict) else {}
    snapshot_path = str(market_open_data.get("snapshot_path") or "").strip()
    if snapshot_path:
        return snapshot_path, str(supply_path)
    return "", str(supply_path)


def _order_plan_ref(order_plan_path: Path, order_plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "path": str(order_plan_path.resolve()),
        "plan_id": str(order_plan.get("plan_id") or ""),
        "sha256": _sha256_file(order_plan_path.resolve()),
    }


def _defined_risk_proof_ref(order_plan: dict[str, Any]) -> dict[str, Any]:
    risk_proof = order_plan.get("risk_proof") if isinstance(order_plan.get("risk_proof"), dict) else {}
    return {
        "source": "order_plan.risk_proof",
        "defined_risk_proven": risk_proof.get("defined_risk_proven") is True,
        "max_loss_usd": str(risk_proof.get("max_loss_usd") or ""),
        "contracts": risk_proof.get("contracts"),
        "width_points": str(risk_proof.get("width_points") or ""),
        "multiplier": risk_proof.get("multiplier"),
    }


def _options_chain_contract_index(snapshot: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    contracts = snapshot.get("contracts")
    if not isinstance(contracts, list):
        return {}
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        key = (
            str(contract.get("expiry_utc") or "").strip(),
            str(contract.get("right") or "").strip().upper(),
            str(contract.get("strike") or "").strip(),
        )
        out[key] = contract
    return out


def _validate_defined_risk_order_plan(
    *,
    order_plan_path: Path,
    order_plan: dict[str, Any],
    source_intent: dict[str, Any],
    day_utc: str,
) -> list[str]:
    blockers: list[str] = []
    try:
        _validate_with_schema(order_plan, ORDER_PLAN_SCHEMA_RELPATH, "RISK_CONTRACT_DEFINED_RISK_ORDER_PLAN_SCHEMA_INVALID")
    except Exception as exc:
        blockers.append(f"RISK_CONTRACT_DEFINED_RISK_ORDER_PLAN_INVALID:{type(exc).__name__}")
        return blockers

    if str(order_plan.get("structure") or "").strip().upper() != "VERTICAL_SPREAD":
        blockers.append("RISK_CONTRACT_DEFINED_RISK_STRUCTURE_UNSUPPORTED")
    underlying = source_intent.get("underlying") if isinstance(source_intent.get("underlying"), dict) else {}
    plan_underlying = order_plan.get("underlying") if isinstance(order_plan.get("underlying"), dict) else {}
    if str(plan_underlying.get("symbol") or "").strip().upper() != str(underlying.get("symbol") or "").strip().upper():
        blockers.append("RISK_CONTRACT_DEFINED_RISK_ORDER_PLAN_SYMBOL_MISMATCH")
    legs = order_plan.get("legs")
    if not isinstance(legs, list) or len(legs) < 2:
        blockers.append("RISK_CONTRACT_DEFINED_RISK_LEGS_MISSING")
    else:
        actions = {str(leg.get("action") or "").strip().upper() for leg in legs if isinstance(leg, dict)}
        expiries = {str(leg.get("expiry_utc") or "").strip() for leg in legs if isinstance(leg, dict)}
        rights = {str(leg.get("right") or "").strip().upper() for leg in legs if isinstance(leg, dict)}
        if actions != {"BUY", "SELL"}:
            blockers.append("RISK_CONTRACT_DEFINED_RISK_LEG_ACTIONS_INVALID")
        if len(expiries) != 1 or not next(iter(expiries), ""):
            blockers.append("RISK_CONTRACT_DEFINED_RISK_EXPIRY_MISSING")
        if len(rights) != 1 or not next(iter(rights), ""):
            blockers.append("RISK_CONTRACT_DEFINED_RISK_RIGHT_MISSING")

    chain_ref = _options_chain_ref(order_plan)
    if not chain_ref:
        chain_ref, _supply_ref = _options_chain_ref_from_structure_supply(order_plan_path)
    if not chain_ref:
        blockers.append("RISK_CONTRACT_OPTIONS_CHAIN_REF_MISSING")
        return blockers
    chain_path = Path(chain_ref).expanduser().resolve()
    if not chain_path.exists() or not chain_path.is_file():
        blockers.append("RISK_CONTRACT_OPTIONS_CHAIN_REF_MISSING")
        return blockers
    try:
        chain = _read_json(chain_path)
        _validate_with_schema(chain, OPTIONS_CHAIN_SCHEMA_RELPATH, "RISK_CONTRACT_OPTIONS_CHAIN_SCHEMA_INVALID")
    except Exception as exc:
        blockers.append(f"RISK_CONTRACT_OPTIONS_CHAIN_INVALID:{type(exc).__name__}")
        return blockers
    if not str(chain.get("as_of_utc") or "").startswith(day_utc):
        blockers.append("RISK_CONTRACT_OPTIONS_CHAIN_WRONG_DAY")
    chain_underlying = chain.get("underlying") if isinstance(chain.get("underlying"), dict) else {}
    if str(chain_underlying.get("symbol") or "").strip().upper() != str(underlying.get("symbol") or "").strip().upper():
        blockers.append("RISK_CONTRACT_OPTIONS_CHAIN_SYMBOL_MISMATCH")
    chain_index = _options_chain_contract_index(chain)
    for idx, leg in enumerate(legs if isinstance(legs, list) else []):
        if not isinstance(leg, dict):
            continue
        key = (
            str(leg.get("expiry_utc") or "").strip(),
            str(leg.get("right") or "").strip().upper(),
            str(leg.get("strike") or "").strip(),
        )
        contract = chain_index.get(key)
        if contract is None:
            blockers.append(f"RISK_CONTRACT_DEFINED_RISK_LEG_NOT_IN_OPTIONS_CHAIN:{idx}")
            continue
        ib = contract.get("ib") if isinstance(contract.get("ib"), dict) else {}
        try:
            if int(leg.get("ib_conId")) != int(ib.get("conId")):
                blockers.append(f"RISK_CONTRACT_DEFINED_RISK_LEG_CONID_MISMATCH:{idx}")
        except Exception:
            blockers.append(f"RISK_CONTRACT_DEFINED_RISK_LEG_CONID_MISSING:{idx}")
    return blockers


def build_risk_definition_contract_v1(*, day_utc: str, truth_root: Path, intent_hash: str = "", intent_id: str = "") -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    if intent_hash:
        source_path = _intent_path(truth_root=truth_root, day_utc=day_utc, intent_hash=intent_hash)
        intent_obj = _read_json(source_path)
        resolved_hash = intent_hash.lower()
    else:
        resolved_hash, source_path, intent_obj = _find_intent_by_id(truth_root=truth_root, day_utc=day_utc, intent_id=intent_id)
    resolved_intent_id = str(intent_obj.get("intent_id") or intent_id).strip()
    exposure_type = str(intent_obj.get("exposure_type") or "").strip().upper()
    engine = intent_obj.get("engine") if isinstance(intent_obj.get("engine"), dict) else {}
    underlying = intent_obj.get("underlying") if isinstance(intent_obj.get("underlying"), dict) else {}
    symbol = str(underlying.get("symbol") or "").strip().upper()
    currency = str(underlying.get("currency") or "USD").strip().upper() or "USD"
    sleeve_id = str(engine.get("engine_id") or "").strip() or str(intent_obj.get("sleeve_id") or "UNKNOWN").strip()
    blockers: list[str] = []
    risk_type = "DEFINED_RISK" if exposure_type == "SHORT_VOL_DEFINED" else "STOP_BASED"
    payload: dict[str, Any] = {
        "schema_id": "risk_definition_contract_v1",
        "schema_version": "v1",
        "contract_id": "",
        "day_utc": day_utc,
        "sleeve_id": sleeve_id,
        "intent_id": resolved_intent_id,
        "intent_hash": resolved_hash,
        "instrument": {"kind": "OPTION_STRATEGY" if risk_type == "DEFINED_RISK" else "EQUITY", "symbol": symbol, "currency": currency},
        "contract_type": risk_type,
        "risk_type": risk_type,
        "source_intent_path": str(source_path.resolve()),
        "generated_at": _now_iso(),
        "git_commit": _git_sha(),
        "truth_root": str(truth_root),
        "producer": {"repo": "constellation", "module": PRODUCER, "git_sha": _git_sha()},
        "validation_status": "FAIL",
        "blockers": blockers,
        "stop_loss_bps": None,
        "stop_loss_price": None,
        "reference_price_source": None,
        "reference_price": None,
        "risk_per_unit": None,
        "quantity_basis": None,
        "structure_type": None,
        "strikes": None,
        "expiry": None,
        "max_loss": None,
        "max_gain": None,
        "breakeven": None,
        "options_chain_ref": None,
        "order_plan_ref": None,
        "defined_risk_proof": None,
        "canonical_json_hash": None
    }
    if not resolved_intent_id:
        blockers.append("RISK_CONTRACT_INTENT_ID_MISSING")
    if not symbol:
        blockers.append("RISK_CONTRACT_INSTRUMENT_SYMBOL_MISSING")

    if risk_type == "STOP_BASED":
        constraints = intent_obj.get("constraints") if isinstance(intent_obj.get("constraints"), dict) else {}
        try:
            stop_bps = int(constraints.get("stop_loss_bps"))
        except (TypeError, ValueError):
            stop_bps = 0
        if stop_bps <= 0:
            blockers.append("RISK_CONTRACT_STOP_LOSS_BPS_MISSING")
        else:
            engine_id = str(engine.get("engine_id") or "").strip()
            try:
                governed_policy = load_risk_policy_for_engine_or_fail(engine_id)
                governed_stop_bps = int(governed_policy.get("stop_loss_bps_default"))
            except (RiskPolicyLoaderError, TypeError, ValueError) as exc:
                blockers.append(f"RISK_CONTRACT_GOVERNED_POLICY_UNAVAILABLE:{type(exc).__name__}")
                governed_stop_bps = 0
            if governed_stop_bps > 0 and stop_bps != governed_stop_bps:
                blockers.append(
                    f"RISK_CONTRACT_STOP_LOSS_BPS_POLICY_MISMATCH:expected={governed_stop_bps}:actual={stop_bps}"
                )
        price, price_source = _market_reference_price(truth_root=truth_root, day_utc=day_utc, symbol=symbol)
        if price is None:
            blockers.append("RISK_CONTRACT_REFERENCE_PRICE_MISSING")
        if stop_bps > 0 and price is not None:
            payload.update(
                {
                    "stop_loss_bps": stop_bps,
                    "stop_loss_price": _stop_price(reference_price=price, side="BUY", stop_loss_bps=stop_bps),
                    "reference_price_source": price_source,
                    "reference_price": _price_text(price),
                    "risk_per_unit": _risk_cents(reference_price=price, stop_loss_bps=stop_bps),
                    "quantity_basis": {"basis": "ONE_UNIT_STOP_RISK_BOOTSTRAP", "quantity": 1},
                }
            )
    elif risk_type == "DEFINED_RISK":
        order_plan_path = _latest_phasec_order_plan(truth_root=truth_root, day_utc=day_utc, intent_hash=resolved_hash)
        order_plan = _read_json(order_plan_path) if order_plan_path is not None else {}
        if order_plan:
            blockers.extend(
                _validate_defined_risk_order_plan(
                    order_plan_path=order_plan_path,
                    order_plan=order_plan,
                    source_intent=intent_obj,
                    day_utc=day_utc,
                )
            )
        risk_proof = order_plan.get("risk_proof") if isinstance(order_plan.get("risk_proof"), dict) else {}
        legs = order_plan.get("legs") if isinstance(order_plan.get("legs"), list) else []
        chain_ref = _options_chain_ref(order_plan)
        chain_ref_source = ""
        if order_plan and not chain_ref and order_plan_path is not None:
            chain_ref, chain_ref_source = _options_chain_ref_from_structure_supply(order_plan_path)
        if not order_plan:
            blockers.append("RISK_CONTRACT_DEFINED_RISK_ORDER_PLAN_MISSING")
            veto_blocker = _phasec_veto_blocker(_latest_phasec_veto(truth_root=truth_root, day_utc=day_utc, intent_hash=resolved_hash))
            if veto_blocker:
                blockers.append(veto_blocker)
        if risk_proof.get("defined_risk_proven") is not True:
            blockers.append("RISK_CONTRACT_DEFINED_RISK_NOT_PROVEN")
        if not legs:
            blockers.append("RISK_CONTRACT_DEFINED_RISK_LEGS_MISSING")
        if not chain_ref:
            blockers.append("RISK_CONTRACT_OPTIONS_CHAIN_REF_MISSING")
        contracts = risk_proof.get("contracts")
        max_loss_usd = risk_proof.get("max_loss_usd")
        try:
            quantity = int(contracts)
        except (TypeError, ValueError):
            quantity = 0
        try:
            max_loss = int((Decimal(str(max_loss_usd)) * Decimal("100")).to_integral_value(rounding=ROUND_CEILING))
        except Exception:
            max_loss = 0
        if quantity <= 0:
            blockers.append("RISK_CONTRACT_DEFINED_RISK_QUANTITY_MISSING")
        if max_loss <= 0:
            blockers.append("RISK_CONTRACT_DEFINED_RISK_MAX_LOSS_MISSING")
        if not blockers and legs and quantity > 0 and max_loss > 0 and chain_ref:
            payload.update(
                {
                    "structure_type": str(order_plan.get("structure") or "DEFINED_RISK_OPTIONS"),
                    "strikes": [
                        {
                            "action": str(leg.get("action") or ""),
                            "right": str(leg.get("right") or ""),
                            "strike": str(leg.get("strike") or ""),
                            "expiry": str(leg.get("expiry_utc") or leg.get("expiration") or ""),
                        }
                        for leg in legs
                        if isinstance(leg, dict)
                    ],
                    "expiry": str((legs[0] if isinstance(legs[0], dict) else {}).get("expiry_utc") or (legs[0] if isinstance(legs[0], dict) else {}).get("expiration") or ""),
                    "max_loss": max_loss,
                    "max_gain": None,
                    "breakeven": None,
                    "options_chain_ref": chain_ref,
                    "order_plan_ref": _order_plan_ref(order_plan_path, order_plan) if order_plan_path is not None else None,
                    "defined_risk_proof": _defined_risk_proof_ref(order_plan),
                    "quantity_basis": {"basis": "DEFINED_RISK_CONTRACTS", "quantity": quantity},
                    "risk_per_unit": (max_loss + quantity - 1) // quantity,
                }
            )
    if not blockers:
        payload["validation_status"] = "PASS"
    payload["blockers"] = blockers
    payload["contract_id"] = canonical_hash_for_c2_artifact_v1({**payload, "contract_id": "", "canonical_json_hash": None})
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1({**payload, "canonical_json_hash": None})
    return payload


def validate_risk_definition_contract_v1(payload: dict[str, Any]) -> None:
    schema = json.loads((REPO_ROOT / SCHEMA_RELPATH).read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError("RISK_DEFINITION_CONTRACT_SCHEMA_INVALID:" + ";".join(error.message for error in errors[:3]))


def load_valid_risk_definition_contract_v1(*, truth_root: Path, day_utc: str, intent_hash: str, intent_id: str = "") -> tuple[Path, dict[str, Any], str]:
    path = risk_contract_path_v1(truth_root=truth_root, day_utc=day_utc, intent_hash=intent_hash)
    if not path.exists() or not path.is_file():
        return path, {}, "RISK_DEFINITION_CONTRACT_MISSING"
    try:
        payload = _read_json(path)
        validate_risk_definition_contract_v1(payload)
    except Exception as exc:
        return path, {}, f"RISK_DEFINITION_CONTRACT_INVALID:{type(exc).__name__}"
    if str(payload.get("day_utc") or "") != day_utc:
        return path, payload, "RISK_DEFINITION_CONTRACT_DAY_MISMATCH"
    if str(payload.get("intent_hash") or "").lower() != intent_hash.lower():
        return path, payload, "RISK_DEFINITION_CONTRACT_INTENT_HASH_MISMATCH"
    if intent_id and str(payload.get("intent_id") or "") != intent_id:
        return path, payload, "RISK_DEFINITION_CONTRACT_INTENT_ID_MISMATCH"
    if str(payload.get("truth_root") or "") != str(Path(truth_root).resolve()):
        return path, payload, "RISK_DEFINITION_CONTRACT_TRUTH_ROOT_MISMATCH"
    if str(payload.get("git_commit") or "").strip() != _git_sha():
        return path, payload, "RISK_DEFINITION_CONTRACT_GIT_COMMIT_MISMATCH"
    producer = payload.get("producer") if isinstance(payload.get("producer"), dict) else {}
    if str(producer.get("module") or "").strip() != PRODUCER:
        return path, payload, "RISK_DEFINITION_CONTRACT_PRODUCER_MISMATCH"
    if str(producer.get("git_sha") or "").strip() != _git_sha():
        return path, payload, "RISK_DEFINITION_CONTRACT_PRODUCER_GIT_COMMIT_MISMATCH"
    source_path = Path(str(payload.get("source_intent_path") or "")).expanduser().resolve()
    expected_source_path = _intent_path(truth_root=Path(truth_root).resolve(), day_utc=day_utc, intent_hash=intent_hash)
    if source_path != expected_source_path:
        return path, payload, "RISK_DEFINITION_CONTRACT_SOURCE_INTENT_PATH_MISMATCH"
    if not source_path.exists() or not source_path.is_file():
        return path, payload, "RISK_DEFINITION_CONTRACT_SOURCE_INTENT_MISSING"
    try:
        source_intent = _read_json(source_path)
    except Exception:
        return path, payload, "RISK_DEFINITION_CONTRACT_SOURCE_INTENT_INVALID"
    if str(source_intent.get("intent_id") or "").strip() != str(payload.get("intent_id") or "").strip():
        return path, payload, "RISK_DEFINITION_CONTRACT_SOURCE_INTENT_ID_MISMATCH"
    if str(payload.get("validation_status") or "").upper() != "PASS":
        blockers = ",".join(str(item) for item in payload.get("blockers") or [])
        return path, payload, f"RISK_DEFINITION_CONTRACT_FAILED:{blockers or 'UNKNOWN'}"
    return path, payload, ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_risk_definition_contract_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--intent_hash", default="")
    parser.add_argument("--intent_id", default="")
    args = parser.parse_args(argv)
    payload = build_risk_definition_contract_v1(
        day_utc=str(args.day_utc).strip(),
        truth_root=Path(args.truth_root).expanduser().resolve(),
        intent_hash=str(args.intent_hash or "").strip(),
        intent_id=str(args.intent_id or "").strip(),
    )
    validate_risk_definition_contract_v1(payload)
    out = risk_contract_path_v1(
        truth_root=Path(args.truth_root).expanduser().resolve(),
        day_utc=str(args.day_utc).strip(),
        intent_hash=str(payload["intent_hash"]),
    )
    sha = _write_json(out, payload)
    write_artifact_ledger_record_v1(
        artifact_path=out,
        artifact_type="risk_definition_contract_v1",
        truth_root=Path(args.truth_root).expanduser().resolve(),
        runtime_root=infer_runtime_root_v1(Path(args.truth_root).expanduser().resolve()),
        runtime_mode=runtime_mode_from_truth_root_v1(Path(args.truth_root).expanduser().resolve()),
        day=str(args.day_utc).strip(),
        recovery_command=f"PYTHONPATH=\"$PWD\" python3 {PRODUCER} --day_utc {str(args.day_utc).strip()} --truth_root {Path(args.truth_root).expanduser().resolve()} --intent_hash {str(payload['intent_hash'])}",
        sleeve=str(payload.get("sleeve_id") or ""),
        artifact_id=f"risk_definition_contract_v1:{str(args.day_utc).strip()}:{str(payload['intent_hash'])}",
    )
    print(json.dumps({"status": payload["validation_status"], "path": str(out), "sha256": sha, "blockers": payload["blockers"]}, sort_keys=True))
    return 0 if payload["validation_status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
