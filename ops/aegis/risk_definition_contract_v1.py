from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.c2_risk_policy_loader_v1 import RiskPolicyLoaderError, load_risk_policy_for_engine_or_fail
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.capital_authority_allocation_v1 import allocation_path_v1
from ops.aegis.event_append_transaction_v1 import (
    contract_input_hashes_for_paths_v1,
    emit_artifact_evidence_transaction_v1,
    sha256_file_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1, runtime_evaluation_path_v1

SCHEMA_ID = "risk_definition_contract_v1"
SCHEMA_VERSION = "v1"
PRODUCER_ID = "ops/tools/build_risk_definition_contract_v1.py"
PRODUCER_VERSION = "v1"
CONSTRUCTION_FAMILY = "paper_trade_construction_v1"


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def risk_definition_contract_path_v1(*, truth_root: Path | str, day_utc: str, intent_hash: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "risk_definition_contract_v1" / day_utc / intent_hash.lower() / "risk_definition_contract.v1.json"


def execution_mirror_risk_definition_contract_path_v1(*, truth_root: Path | str, day_utc: str, intent_hash: str) -> Path:
    root = Path(truth_root).expanduser().resolve()
    return root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "risk_definition_contract_v1" / day_utc / intent_hash.lower() / "risk_definition_contract.v1.json"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "-C", str(_repo_root()), "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "0" * 40


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _money(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")


def _construction_path(root: Path, day: str) -> Path:
    return root / "reports" / CONSTRUCTION_FAMILY / day / "paper_trade_construction.v1.json"


def _selected_pointer_path(root: Path) -> Path:
    return root / "pointers" / "selected_intent_pointer.v1.json"


def _intent_path_candidates(root: Path, day: str, intent_hash: str) -> list[Path]:
    h = intent_hash.lower()
    return [
        root / "intents_v1" / "snapshots" / day / f"{h}.exposure_intent.v1.json",
        root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "intents_v1" / "snapshots" / day / f"{h}.exposure_intent.v1.json",
    ]


def _resolve_intent(*, root: Path, day: str, intent_hash: str = "", intent_id: str = "") -> tuple[str, Path, dict[str, Any]]:
    pointer = _read_json(_selected_pointer_path(root))
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    selected_hash = str(selected.get("intent_hash") or "").lower()
    selected_id = str(selected.get("intent_id") or "")
    if selected and (not intent_hash or selected_hash == intent_hash.lower()) and (not intent_id or selected_id == intent_id):
        path = Path(str(selected.get("intent_path") or "")).expanduser().resolve()
        if path.exists():
            return selected_hash, path, _read_json(path)
    if intent_hash:
        for path in _intent_path_candidates(root, day, intent_hash):
            if path.exists():
                return intent_hash.lower(), path.resolve(), _read_json(path)
    if intent_id:
        for base in [root / "intents_v1" / "snapshots" / day, root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "intents_v1" / "snapshots" / day]:
            if not base.exists():
                continue
            for path in sorted(base.glob("*.exposure_intent.v1.json")):
                payload = _read_json(path)
                if str(payload.get("intent_id") or "") == intent_id:
                    return path.name.split(".exposure_intent.v1.json", 1)[0].lower(), path.resolve(), payload
    return intent_hash.lower(), Path(), {}


def _authorized_row(capital: Mapping[str, Any], intent_id: str) -> dict[str, Any]:
    chain = capital.get("decision_chain") if isinstance(capital.get("decision_chain"), Mapping) else {}
    rows = chain.get("authorized_trade_intents") if isinstance(chain.get("authorized_trade_intents"), list) else []
    for row in rows:
        if isinstance(row, Mapping) and str(row.get("intent_id") or "") == intent_id:
            return dict(row)
    return {}


def _limit_used(row: Mapping[str, Any]) -> dict[str, Any]:
    limit = row.get("allocation_limit_used") if isinstance(row.get("allocation_limit_used"), Mapping) else {}
    return dict(limit)


def _market_reference_price(*, truth_root: Path, day_utc: str, symbol: str) -> tuple[Decimal | None, str]:
    sym = symbol.upper()
    snapshot_path = truth_root / "market_data_snapshot_v1" / "snapshots" / day_utc / f"{sym}.market_data_snapshot.v1.json"
    if snapshot_path.exists():
        payload = _read_json(snapshot_path)
        if str(payload.get("day_utc") or "") == day_utc and str(payload.get("symbol") or "").upper() == sym:
            dec = _dec(payload.get("close"))
            if dec and dec > 0:
                return dec, str(snapshot_path.resolve())
    jsonl_path = truth_root / "market_data_snapshot_v1" / sym / f"{day_utc[:4]}.jsonl"
    if not jsonl_path.exists():
        return None, ""
    latest: Decimal | None = None
    latest_ts = ""
    for line in jsonl_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict) or str(row.get("symbol") or "").upper() != sym:
            continue
        ts = str(row.get("timestamp_utc") or "")
        if ts[:10] != day_utc:
            continue
        dec = _dec(row.get("close"))
        if dec and dec > 0 and ts >= latest_ts:
            latest = dec
            latest_ts = ts
    return (latest, str(jsonl_path.resolve())) if latest is not None else (None, str(jsonl_path.resolve()))


def _risk_policy_stop_bps(engine_id: str) -> int:
    try:
        policy = load_risk_policy_for_engine_or_fail(engine_id)
        return int(policy.get("stop_loss_bps_default") or 0)
    except (RiskPolicyLoaderError, TypeError, ValueError):
        return 0


def _stop_price(*, reference_price: Decimal, side: str, stop_loss_bps: int) -> str:
    distance = Decimal(stop_loss_bps) / Decimal("10000")
    if side.upper() in {"SHORT", "SELL"}:
        return _money(reference_price * (Decimal("1") + distance))
    return _money(reference_price * (Decimal("1") - distance))


def _risk_per_unit_cents(*, reference_price: Decimal, stop_loss_bps: int) -> int:
    cents = reference_price * Decimal("100") * Decimal(stop_loss_bps) / Decimal("10000")
    return int(cents.to_integral_value(rounding=ROUND_CEILING))


def build_risk_definition_contract_v1(*, truth_root: Path | str, day_utc: str, intent_hash: str = "", intent_id: str = "", generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    construction_path = _construction_path(root, day_utc)
    construction = _read_json(construction_path)
    runtime_path = runtime_evaluation_path_v1(truth_root=root, day_utc=day_utc)
    runtime = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    runtime_hash = str(runtime.get("deterministic_output_hash") or "")
    selected_hash = intent_hash or str(construction.get("selected_exposure_intent_hash") or "")
    selected_id = intent_id or str(construction.get("selected_exposure_intent_id") or "")
    resolved_hash, source_path, intent = _resolve_intent(root=root, day=day_utc, intent_hash=selected_hash, intent_id=selected_id)
    selected_id = str(intent.get("intent_id") or selected_id)
    engine = intent.get("engine") if isinstance(intent.get("engine"), Mapping) else {}
    underlying = intent.get("underlying") if isinstance(intent.get("underlying"), Mapping) else {}
    constraints = intent.get("constraints") if isinstance(intent.get("constraints"), Mapping) else {}
    sleeve_id = str(construction.get("sleeve_id") or construction.get("engine_id") or engine.get("engine_id") or "")
    symbol = str(construction.get("symbol") or underlying.get("symbol") or "").upper()
    side = str(construction.get("direction") or "LONG").upper()
    capital_path = allocation_path_v1(truth_root=root, day_utc=day_utc)
    capital = _read_json(capital_path)
    capital_hash = sha256_file_v1(capital_path) if capital_path.exists() else ""
    row = _authorized_row(capital, selected_id)
    limit = _limit_used(row)
    requested_risk = _dec(construction.get("max_loss_estimate")) or _dec(limit.get("requested_risk"))
    max_risk = _dec(limit.get("max_risk")) or _dec(row.get("authorized_risk"))
    requested_notional = _dec(construction.get("suggested_notional")) or _dec(limit.get("requested_notional"))
    quantity = int(construction.get("suggested_quantity") or limit.get("requested_quantity") or row.get("authorized_quantity") or 0)
    reference_price = _dec(construction.get("entry_reference_price"))
    reference_source = "paper_trade_construction.entry_reference_price" if reference_price else ""
    if reference_price is None:
        reference_price, reference_source = _market_reference_price(truth_root=root, day_utc=day_utc, symbol=symbol)
    stop_bps = 0
    try:
        stop_bps = int(constraints.get("stop_loss_bps") or construction.get("stop_loss_bps") or 0)
    except (TypeError, ValueError):
        stop_bps = 0
    policy_stop_bps = _risk_policy_stop_bps(sleeve_id)
    stop_loss_price = str(construction.get("stop_price") or "")
    if reference_price and stop_bps > 0 and not stop_loss_price:
        stop_loss_price = _stop_price(reference_price=reference_price, side=side, stop_loss_bps=stop_bps)
    risk_per_unit = None
    if reference_price and stop_bps > 0:
        risk_per_unit = _risk_per_unit_cents(reference_price=reference_price, stop_loss_bps=stop_bps)

    blockers: list[str] = []
    if not runtime_hash:
        blockers.append("RUNTIME_EVALUATION_MISSING")
    if not construction:
        blockers.append("PAPER_TRADE_CONSTRUCTION_MISSING")
    if construction and str(construction.get("runtime_evaluation_hash") or "") and runtime_hash and str(construction.get("runtime_evaluation_hash") or "") != runtime_hash:
        blockers.append("RUNTIME_HASH_MISMATCH")
    if not capital:
        blockers.append("CAPITAL_AUTHORITY_ALLOCATION_MISSING")
    if capital and str(capital.get("runtime_evaluation_hash") or "") and runtime_hash and str(capital.get("runtime_evaluation_hash") or "") != runtime_hash:
        blockers.append("CAPITAL_AUTHORITY_RUNTIME_HASH_MISMATCH")
    if not intent:
        blockers.append("SOURCE_INTENT_MISSING")
    if construction and selected_id and str(construction.get("selected_exposure_intent_id") or "") != selected_id:
        blockers.append("SELECTED_EXPOSURE_INTENT_MISMATCH")
    construction_selected_hash = str(construction.get("selected_exposure_intent_hash") or "").lower()
    if construction and resolved_hash and construction_selected_hash and construction_selected_hash != resolved_hash.lower():
        blockers.append("SELECTED_EXPOSURE_INTENT_HASH_MISMATCH")
    if not row:
        blockers.append("CAPITAL_AUTHORITY_INTENT_MISSING")
    if max_risk is None:
        blockers.append("RISK_LIMIT_MISSING")
    if requested_risk is None:
        blockers.append("REQUESTED_RISK_MISSING")
    if requested_risk is not None and max_risk is not None and requested_risk > max_risk:
        blockers.append("RISK_LIMIT_EXCEEDED")
    if stop_bps <= 0:
        blockers.append("STOP_LOSS_BPS_MISSING")
    if policy_stop_bps > 0 and stop_bps > 0 and stop_bps != policy_stop_bps:
        blockers.append("STOP_LOSS_BPS_POLICY_MISMATCH")
    if reference_price is None:
        blockers.append("REFERENCE_PRICE_MISSING")
    if quantity <= 0:
        blockers.append("QUANTITY_MISSING")
    if not symbol:
        blockers.append("SYMBOL_MISSING")

    validation_status = "PASS" if not blockers else "FAIL"
    construction_contract_hash = str(construction.get("construction_contract_hash") or "")
    construction_artifact_hash = sha256_file_v1(construction_path) if construction_path.exists() else ""
    input_paths = [path for path in [runtime_path, construction_path, capital_path, source_path] if path and Path(path).exists()]
    input_hashes = contract_input_hashes_for_paths_v1(
        input_paths,
        extra={
            "runtime_evaluation_hash": runtime_hash,
            "construction_contract_hash": construction_contract_hash,
            "capital_allocation_hash": capital_hash,
            "intent_hash": resolved_hash,
        },
    )
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "contract_id": "",
        "artifact_id": "",
        "day_utc": day_utc,
        "sleeve_id": sleeve_id,
        "symbol": symbol,
        "side": "BUY" if side in {"LONG", "BUY"} else "SELL",
        "intent_id": selected_id,
        "intent_hash": resolved_hash,
        "exposure_id": selected_id,
        "ticket_id": str(construction.get("ticket_id") or ""),
        "instrument": {"kind": "EQUITY", "symbol": symbol, "currency": str(underlying.get("currency") or "USD")},
        "contract_type": "STOP_BASED",
        "risk_type": "STOP_BASED",
        "risk_model_version": "aegis.stop_based_equity_risk.v1",
        "risk_measure": "MAX_STOP_LOSS_ESTIMATE_USD",
        "risk_measure_definition": {
            "measure": "max_loss_estimate",
            "formula": "abs(entry_reference_price - stop_loss_price) * quantity",
            "currency": "USD",
            "requested_risk": _money(requested_risk),
            "max_risk": _money(max_risk),
            "validation_status": "VALID" if requested_risk is not None and max_risk is not None and requested_risk <= max_risk else "BLOCKED",
        },
        "max_risk": _money(max_risk),
        "max_risk_cents": int((max_risk * Decimal("100")).to_integral_value(rounding=ROUND_CEILING)) if max_risk is not None else 0,
        "requested_risk": _money(requested_risk),
        "requested_notional": _money(requested_notional),
        "requested_quantity": quantity,
        "stop_loss_bps": stop_bps if stop_bps > 0 else None,
        "stop_loss_price": stop_loss_price or None,
        "stop_loss_definition": {
            "source": "exposure_intent.constraints.stop_loss_bps",
            "stop_loss_bps": stop_bps,
            "governed_policy_stop_loss_bps": policy_stop_bps,
            "stop_loss_price": stop_loss_price,
        },
        "reference_price_source": reference_source or None,
        "reference_price": _money(reference_price),
        "risk_per_unit": risk_per_unit,
        "quantity_basis": {"basis": "CONSTRUCTION_SUGGESTED_QUANTITY", "quantity": quantity} if quantity > 0 else None,
        "sizing_method": "construction_quantity_with_stop_loss_risk_bound",
        "source_intent_path": str(source_path.resolve()) if str(source_path) else "",
        "generated_at": generated_at_utc or str(runtime.get("generated_at_utc") or runtime.get("generated_at") or now_utc_v1()),
        "generated_at_utc": generated_at_utc or str(runtime.get("generated_at_utc") or runtime.get("generated_at") or now_utc_v1()),
        "git_commit": _git_sha(),
        "truth_root": str(root),
        "producer": {"repo": "constellation", "module": PRODUCER_ID, "git_sha": _git_sha()},
        "runtime_evaluation_hash": runtime_hash,
        "runtime_evaluation_path": str(runtime_path),
        "construction_contract_id": str(construction.get("construction_contract_id") or ""),
        "construction_contract_hash": construction_contract_hash,
        "paper_trade_construction_id": str(construction.get("paper_trade_construction_id") or construction.get("construction_id") or ""),
        "paper_trade_construction_path": str(construction_path),
        "paper_trade_construction_hash": construction_artifact_hash,
        "capital_allocation_path": str(capital_path),
        "capital_allocation_hash": capital_hash,
        "capital_allocation_status": str(capital.get("validation_status") or ""),
        "execution_mirror_path": str(execution_mirror_risk_definition_contract_path_v1(truth_root=root, day_utc=day_utc, intent_hash=resolved_hash)) if resolved_hash else "",
        "execution_mirror_required": (root.parent / "truth_sleeves" / "PRIMARY" / "PAPER").exists(),
        "allocation_limit_used": limit,
        "input_hashes": input_hashes,
        "validation_status": validation_status,
        "blockers": sorted(set(blockers)),
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "canonical_json_hash": None,
        "output_hash": "",
        "structure_type": None,
        "strikes": None,
        "expiry": None,
        "max_loss": None,
        "max_gain": None,
        "breakeven": None,
        "options_chain_ref": None,
        "order_plan_ref": None,
        "defined_risk_proof": None,
    }
    core = {**payload, "contract_id": "", "artifact_id": "", "canonical_json_hash": None, "output_hash": ""}
    payload["contract_id"] = stable_hash_v1(core)
    payload["artifact_id"] = f"risk_definition_contract_v1:{day_utc}:{payload['contract_id'][:24]}"
    payload["canonical_json_hash"] = stable_hash_v1({**payload, "canonical_json_hash": None, "output_hash": ""})
    payload["output_hash"] = stable_hash_v1({**payload, "output_hash": ""})
    return payload


def write_risk_definition_contract_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any], emit_events: bool = True) -> Path:
    root = Path(truth_root).expanduser().resolve()
    path = risk_definition_contract_path_v1(truth_root=root, day_utc=day_utc, intent_hash=str(payload.get("intent_hash") or ""))
    payload_dict = dict(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload_dict) + b"\n")
    mirror_path = Path(str(payload_dict.get("execution_mirror_path") or "")).expanduser()
    mirror_written = False
    mirror_base = root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"
    if mirror_path and str(mirror_path) != "." and mirror_base.exists():
        mirror_path = mirror_path.resolve()
        mirror_path.parent.mkdir(parents=True, exist_ok=True)
        mirror_path.write_bytes(canonical_json_bytes_v1(payload_dict) + b"\n")
        mirror_written = True
    if emit_events:
        for artifact in ([path] + ([mirror_path] if mirror_written else [])):
            emit_artifact_evidence_transaction_v1(
                truth_root=root,
                day_utc=day_utc,
                artifact_path=artifact,
                payload=payload_dict,
                producer_id=PRODUCER_ID,
                producer_version=PRODUCER_VERSION,
                created_at_utc=str(payload.get("generated_at_utc") or payload.get("generated_at") or ""),
                input_hashes=dict(payload.get("input_hashes") or {}),
                validation_status="VALID" if str(payload.get("validation_status") or "").upper() == "PASS" else "REJECTED",
            )
    return path


def build_and_write_risk_definition_contract_v1(*, truth_root: Path | str, day_utc: str, intent_hash: str = "", intent_id: str = "", generated_at_utc: str | None = None, emit_events: bool = True) -> tuple[dict[str, Any], Path]:
    payload = build_risk_definition_contract_v1(truth_root=truth_root, day_utc=day_utc, intent_hash=intent_hash, intent_id=intent_id, generated_at_utc=generated_at_utc)
    path = write_risk_definition_contract_v1(truth_root=truth_root, day_utc=day_utc, payload=payload, emit_events=emit_events)
    return {**payload, "artifact_path": str(path), "artifact_hash": sha256_file_v1(path)}, path
