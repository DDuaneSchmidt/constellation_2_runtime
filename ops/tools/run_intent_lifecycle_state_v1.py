#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    parse_day_utc_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_paper_intent_truth_root_v1,
)
from constellation_2.common.trading_day_readiness_authority_v1 import (
    PREOPEN_MODES,
    read_or_evaluate_trading_day_readiness_authority_v1,
)
from ops.tools import run_sleeve_evaluation_kernel_v1 as sleeve_kernel
from ops.tools.run_position_lifecycle_state_v1 import position_lifecycle_state_path

PAPER_MODE = "PAPER"
PRODUCER = "ops/tools/run_intent_lifecycle_state_v1.py"
ACTIVE_ORDER_STATES = {
    "ACCEPTED",
    "ACKNOWLEDGED",
    "ACTIVE",
    "HELD",
    "NEW",
    "OPEN",
    "PARTIALLY_FILLED",
    "PENDING",
    "PENDING_NEW",
    "PRE_SUBMITTED",
    "QUEUED",
    "SUBMITTED",
    "WORKING",
}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def intent_lifecycle_state_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "intent_lifecycle_state_v1" / day_utc / "intent_lifecycle_state.v1.json"


def _latest_scan_rollup_path(truth_root: Path, day_utc: str) -> Path:
    pointer = _read_json(Path(truth_root).resolve() / "pointers" / "latest_scan_cycle_pointer.v1.json")
    if str(pointer.get("day_utc") or "") == day_utc:
        artifact_root = str(pointer.get("artifact_root") or "").strip()
        if artifact_root:
            path = Path(artifact_root) / "scan_rollup.v1.json"
            if path.is_file():
                return path.resolve()
    return sleeve_kernel.sleeve_evaluation_rollup_path(truth_root=truth_root, day_utc=day_utc)


def _load_outcomes(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    rollup = _read_json(_latest_scan_rollup_path(truth_root, day_utc))
    rows = rollup.get("outcomes") if isinstance(rollup.get("outcomes"), list) else rollup.get("sleeve_outcomes")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _signal_signature(outcome: dict[str, Any]) -> list[dict[str, str]]:
    intents = outcome.get("output_intents") if isinstance(outcome.get("output_intents"), list) else []
    if intents:
        return sleeve_kernel._intent_signature(intents)
    signature = outcome.get("intent_signature") if isinstance(outcome.get("intent_signature"), list) else []
    return [row for row in signature if isinstance(row, dict)]


def _signal_state(outcome: dict[str, Any]) -> str:
    status = str(outcome.get("status") or "").strip().upper()
    signal = outcome.get("signal_state") if isinstance(outcome.get("signal_state"), dict) else {}
    state = str(signal.get("state") or "").strip().upper()
    if state == "ACTIVE" and _signal_signature(outcome):
        return "ACTIVE"
    if status == "INTENT_CREATED" and _signal_signature(outcome):
        return "ACTIVE"
    if status in {"NO_INTENT", "DISABLED", "FILTERED_OUT"}:
        return "INACTIVE"
    if status in {"BLOCKED", "DEGRADED"}:
        return "UNKNOWN"
    return state if state in {"ACTIVE", "INACTIVE", "UNKNOWN"} else "UNKNOWN"


def lifecycle_fields_from_outcome(outcome: dict[str, Any]) -> dict[str, str]:
    intents = outcome.get("output_intents") if isinstance(outcome.get("output_intents"), list) else []
    primary = intents[0] if intents and isinstance(intents[0], dict) else {}
    return {
        "symbol": str(primary.get("symbol") or outcome.get("intent_symbol") or outcome.get("producer_requested_symbol") or "").strip().upper(),
        "underlying": str(primary.get("underlying") or primary.get("symbol") or outcome.get("intent_symbol") or outcome.get("producer_requested_symbol") or "").strip().upper(),
        "exposure_type": str(primary.get("exposure_type") or outcome.get("exposure_type") or _default_exposure_type(str(outcome.get("engine_id") or ""))).strip().upper(),
    }


def _default_exposure_type(engine_id: str) -> str:
    engine = str(engine_id or "").strip().upper()
    if "VOL_INCOME" in engine:
        return "SHORT_VOL_DEFINED_RISK"
    if "DEFENSIVE_TAIL" in engine:
        return "TAIL_HEDGE"
    if "MARKET_NEUTRAL" in engine:
        return "MARKET_NEUTRAL_PAIR"
    return "LONG_EQUITY"


def _previous_signature(previous: dict[str, Any]) -> list[dict[str, str]]:
    signature = previous.get("intent_signature") if isinstance(previous.get("intent_signature"), list) else []
    return [row for row in signature if isinstance(row, dict)]


def is_unchanged_signal(outcome: dict[str, Any], previous: dict[str, Any]) -> bool:
    previous_status = str(previous.get("current_status") or previous.get("status") or "").strip().upper()
    previous_signal = previous.get("signal_state") if isinstance(previous.get("signal_state"), dict) else {}
    current_signature = _signal_signature(outcome)
    return (
        str(outcome.get("status") or "").strip().upper() == "INTENT_CREATED"
        and previous_status in {"INTENT_CREATED", "NO_INTENT"}
        and str(previous_signal.get("state") or "").strip().upper() == "ACTIVE"
        and bool(current_signature)
        and current_signature == _previous_signature(previous)
    )


def _as_list(payload: dict[str, Any], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    value: Any = payload
    for key in keys:
        if not isinstance(value, dict):
            return []
        value = value.get(key)
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _positions_candidates(*, truth_root: Path, intent_truth_root: Path, day_utc: str) -> list[Path]:
    return [
        Path(intent_truth_root).resolve() / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json",
        Path(intent_truth_root).resolve() / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json",
        Path(truth_root).resolve() / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json",
        Path(truth_root).resolve() / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json",
    ]


def _load_positions(*, truth_root: Path, intent_truth_root: Path, day_utc: str) -> tuple[str, list[dict[str, Any]], str, list[str]]:
    evidence = [str(path) for path in _positions_candidates(truth_root=truth_root, intent_truth_root=intent_truth_root, day_utc=day_utc)]
    for path in _positions_candidates(truth_root=truth_root, intent_truth_root=intent_truth_root, day_utc=day_utc):
        if not path.is_file():
            continue
        payload = _read_json(path)
        status = str(payload.get("status") or payload.get("snapshot_status") or "OK").strip().upper()
        payload_day = str(payload.get("day_utc") or payload.get("as_of_day_utc") or day_utc).strip()
        if payload_day and payload_day != day_utc:
            return "POSITION_STATE_STALE", [], str(path), evidence
        if status in {"BLOCKED", "ERROR", "FAILED", "STALE"}:
            return "POSITION_STATE_STALE", [], str(path), evidence
        items = _as_list(payload, ("items",)) or _as_list(payload, ("positions", "items")) or _as_list(payload, ("positions",))
        return "OK", items, str(path), evidence
    return "POSITION_STATE_STALE", [], "", evidence


def _flatten_rows(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(value, dict):
        rows.append(value)
        for child in value.values():
            rows.extend(_flatten_rows(child))
    elif isinstance(value, list):
        for child in value:
            rows.extend(_flatten_rows(child))
    return rows


def _order_candidates(*, truth_root: Path, intent_truth_root: Path, day_utc: str) -> list[Path]:
    roots = [Path(intent_truth_root).resolve(), Path(truth_root).resolve()]
    rels = [
        ("submission_index_v1", day_utc, "submission_index.v1.json"),
        ("submission_index_v1", day_utc, "submission_index.v2.json"),
        ("execution_evidence_v1", "current_head", day_utc, "current_head.v1.json"),
        ("fill_ledger_v1", day_utc, "fill_ledger.v1.json"),
        ("execution_lifecycle_authority_v1", day_utc, "execution_lifecycle_authority.v1.json"),
    ]
    return [root.joinpath(*rel) for root in roots for rel in rels]


def _load_order_rows(*, truth_root: Path, intent_truth_root: Path, day_utc: str) -> tuple[list[dict[str, Any]], list[str]]:
    evidence: list[str] = []
    rows: list[dict[str, Any]] = []
    for path in _order_candidates(truth_root=truth_root, intent_truth_root=intent_truth_root, day_utc=day_utc):
        evidence.append(str(path))
        if not path.is_file():
            continue
        payload = _read_json(path)
        evidence.append(str(path))
        rows.extend(_flatten_rows(payload))
    return rows, sorted(set(evidence))


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _nested(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    instrument = row.get("instrument") if isinstance(row.get("instrument"), dict) else {}
    for key in keys:
        if key in instrument:
            return instrument.get(key)
    contract = row.get("contract") if isinstance(row.get("contract"), dict) else {}
    for key in keys:
        if key in contract:
            return contract.get(key)
    return ""


def _symbol_values(row: dict[str, Any]) -> set[str]:
    values = {
        _norm(_nested(row, "symbol", "ticker")),
        _norm(_nested(row, "underlying", "underlying_symbol", "root_symbol")),
    }
    underlying = row.get("underlying")
    if isinstance(underlying, dict):
        values.add(_norm(underlying.get("symbol")))
    return {value for value in values if value}


def _row_engine(row: dict[str, Any]) -> str:
    engine = row.get("engine") if isinstance(row.get("engine"), dict) else {}
    return _norm(
        row.get("engine_id")
        or row.get("sleeve_id")
        or row.get("strategy_engine_id")
        or row.get("native_engine_id")
        or row.get("source_engine_id")
        or engine.get("engine_id")
    )


def _row_account(row: dict[str, Any]) -> str:
    account = row.get("account") if isinstance(row.get("account"), dict) else {}
    return _norm(row.get("account_id") or row.get("ib_account") or row.get("account") or account.get("account_id") or account.get("ib_account"))


def _row_environment(row: dict[str, Any]) -> str:
    return _norm(row.get("environment") or row.get("mode") or PAPER_MODE)


def _quantity(row: dict[str, Any]) -> float:
    for key in ("quantity", "qty", "position", "net_quantity", "open_quantity", "remaining_quantity"):
        value = row.get(key)
        if isinstance(value, dict):
            value = value.get("value") or value.get("amount")
        try:
            return float(value)
        except Exception:
            continue
    return 0.0


def _compatible_exposure(expected: str, row: dict[str, Any]) -> bool:
    expected = _norm(expected)
    actual = _norm(row.get("exposure_type") or row.get("risk_class") or row.get("asset_class") or row.get("instrument_type"))
    if expected == "MARKET_NEUTRAL_PAIR":
        legs = row.get("legs")
        return isinstance(legs, list) and len([leg for leg in legs if isinstance(leg, dict)]) >= 2
    if expected == "LONG_EQUITY":
        return not actual or any(token in actual for token in ("EQUITY", "ETF", "STOCK", "LONG"))
    if expected == "SHORT_VOL_DEFINED_RISK":
        return any(token in actual for token in ("VOL", "OPTION", "SPREAD", "DEFINED")) if actual else True
    if expected == "TAIL_HEDGE":
        return any(token in actual for token in ("TAIL", "HEDGE", "DEFENSIVE", "OPTION", "TREASURY", "BOND")) if actual else True
    return True


def _same_symbol(row: dict[str, Any], *, symbol: str, underlying: str) -> bool:
    values = _symbol_values(row)
    wanted = {_norm(symbol), _norm(underlying)}
    wanted.discard("")
    return bool(values.intersection(wanted))


def _same_identity(row: dict[str, Any], *, sleeve_id: str, engine_id: str, environment: str, account_id: str) -> tuple[bool, bool]:
    row_engine = _row_engine(row)
    engine_match = row_engine in {_norm(sleeve_id), _norm(engine_id)}
    attribution_unknown = not row_engine
    row_env = _row_environment(row)
    env_match = not row_env or row_env == _norm(environment)
    row_account = _row_account(row)
    account_match = not account_id or not row_account or row_account == _norm(account_id)
    return engine_match and env_match and account_match, attribution_unknown and env_match and account_match


def _position_match_state(
    *,
    rows: list[dict[str, Any]],
    sleeve_id: str,
    engine_id: str,
    symbol: str,
    underlying: str,
    exposure_type: str,
    environment: str,
    account_id: str,
) -> str:
    uncertain = False
    for row in rows:
        if not _same_symbol(row, symbol=symbol, underlying=underlying):
            continue
        identity_match, attribution_unknown = _same_identity(row, sleeve_id=sleeve_id, engine_id=engine_id, environment=environment, account_id=account_id)
        if attribution_unknown and abs(_quantity(row)) > 0:
            uncertain = True
            continue
        if not identity_match:
            continue
        if not _compatible_exposure(exposure_type, row):
            continue
        status = _norm(row.get("status") or row.get("state") or "")
        if abs(_quantity(row)) > 0 or status in {"ACTIVE", "OPEN"}:
            return "POSITION_OPEN"
    return "POSITION_MATCH_UNCERTAIN" if uncertain else "NO_POSITION"


def _order_match_state(
    *,
    rows: list[dict[str, Any]],
    sleeve_id: str,
    engine_id: str,
    symbol: str,
    underlying: str,
    exposure_type: str,
    environment: str,
    account_id: str,
) -> str:
    uncertain = False
    for row in rows:
        if not _same_symbol(row, symbol=symbol, underlying=underlying):
            continue
        status = _norm(row.get("status") or row.get("state") or row.get("order_status") or row.get("lifecycle_state"))
        if status and status not in ACTIVE_ORDER_STATES:
            continue
        identity_match, attribution_unknown = _same_identity(row, sleeve_id=sleeve_id, engine_id=engine_id, environment=environment, account_id=account_id)
        if attribution_unknown:
            uncertain = True
            continue
        if not identity_match:
            continue
        if _compatible_exposure(exposure_type, row):
            return "ORDER_PENDING"
    return "ORDER_MATCH_UNCERTAIN" if uncertain else "NO_ORDER"


def _load_position_lifecycle_rows(*, truth_root: Path, day_utc: str) -> tuple[str, list[dict[str, Any]], str]:
    path = position_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc)
    if not path.is_file():
        return "", [], str(path)
    payload = _read_json(path)
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    return str(payload.get("position_truth_status") or payload.get("status") or ""), [row for row in rows if isinstance(row, dict)], str(path)


def _position_lifecycle_match(
    *,
    rows: list[dict[str, Any]],
    sleeve_id: str,
    engine_id: str,
    symbol: str,
    underlying: str,
    exposure_type: str,
    environment: str,
    account_id: str,
) -> dict[str, Any]:
    best: dict[str, Any] = {}
    uncertain = False
    stale = False
    for row in rows:
        if not _same_symbol(row, symbol=symbol, underlying=underlying):
            continue
        reasons = row.get("reason_codes") if isinstance(row.get("reason_codes"), list) else []
        reason_set = {str(reason).strip().upper() for reason in reasons}
        if "POSITION_STATE_STALE" in reason_set:
            stale = True
        if "POSITION_MATCH_UNCERTAIN" in reason_set:
            uncertain = True
        identity_match, attribution_unknown = _same_identity(row, sleeve_id=sleeve_id, engine_id=engine_id, environment=environment, account_id=account_id)
        if attribution_unknown:
            uncertain = True
            continue
        if identity_match and _compatible_exposure(exposure_type, row):
            best = row
            break
    state = _norm(best.get("lifecycle_state")) if best else ""
    if stale and not best:
        return {
            "position_state": "POSITION_STATE_STALE",
            "order_state": "NO_ORDER",
            "matching_position_id": "",
            "matching_lifecycle_state": "UNKNOWN",
            "position_truth_status": "POSITION_STATE_STALE",
            "reentry_allowed_by_position_lifecycle": False,
        }
    if uncertain and not best:
        return {
            "position_state": "POSITION_MATCH_UNCERTAIN",
            "order_state": "NO_ORDER",
            "matching_position_id": "",
            "matching_lifecycle_state": "UNKNOWN",
            "position_truth_status": "POSITION_MATCH_UNCERTAIN",
            "reentry_allowed_by_position_lifecycle": False,
        }
    if not best:
        return {}
    if state in {"POSITION_OPEN", "PARTIALLY_FILLED", "EXIT_PENDING"}:
        position_state = "POSITION_OPEN"
        order_state = "NO_ORDER"
        reentry_allowed = False
    elif state == "ORDER_PENDING":
        position_state = "NO_POSITION"
        order_state = "ORDER_PENDING"
        reentry_allowed = False
    elif state in {"POSITION_CLOSED", "CANCELED", "FAILED", "NO_POSITION"}:
        position_state = "NO_POSITION"
        order_state = "NO_ORDER"
        reentry_allowed = True
    elif state == "UNKNOWN":
        reasons = best.get("reason_codes") if isinstance(best.get("reason_codes"), list) else []
        reason_set = {str(reason).strip().upper() for reason in reasons}
        if "POSITION_STATE_STALE" in reason_set:
            position_state = "POSITION_STATE_STALE"
        elif "POSITION_MATCH_UNCERTAIN" in reason_set:
            position_state = "POSITION_MATCH_UNCERTAIN"
        else:
            position_state = "POSITION_MATCH_UNCERTAIN"
        order_state = "NO_ORDER"
        reentry_allowed = False
    else:
        position_state = "NO_POSITION"
        order_state = "NO_ORDER"
        reentry_allowed = True
    return {
        "position_state": position_state,
        "order_state": order_state,
        "matching_position_id": str(best.get("position_id") or "") if best else "",
        "matching_lifecycle_state": state or "",
        "position_truth_status": str(best.get("position_truth_status") or position_state or ""),
        "reentry_allowed_by_position_lifecycle": reentry_allowed,
    }


def _lifecycle_decision(
    *,
    signal_state: str,
    unchanged_signal: bool,
    position_state: str,
    order_state: str,
    readiness_mode: str = "",
) -> tuple[str, list[str], bool]:
    if signal_state == "INACTIVE":
        return "NO_INTENT", ["SIGNAL_INACTIVE"], False
    if signal_state == "UNKNOWN":
        if str(readiness_mode or "").strip().upper() in PREOPEN_MODES:
            return "NO_INTENT", ["PREOPEN_INPUTS_NOT_REQUIRED"], False
        return "BLOCKED", ["SIGNAL_STATE_UNKNOWN"], False
    if not unchanged_signal:
        return "INTENT_CREATED", ["SIGNAL_CHANGED"], True
    reasons = ["UNCHANGED_SIGNAL"]
    if position_state == "POSITION_OPEN":
        return "NO_INTENT", reasons + ["POSITION_ALREADY_OPEN"], False
    if order_state == "ORDER_PENDING":
        return "NO_INTENT", reasons + ["ORDER_ALREADY_PENDING"], False
    if position_state == "POSITION_STATE_STALE":
        return "BLOCKED", reasons + ["POSITION_STATE_STALE"], False
    if position_state == "POSITION_MATCH_UNCERTAIN":
        return "BLOCKED", reasons + ["POSITION_MATCH_UNCERTAIN"], False
    if order_state == "ORDER_STATE_STALE":
        return "BLOCKED", reasons + ["ORDER_STATE_STALE"], False
    if order_state == "ORDER_MATCH_UNCERTAIN":
        return "BLOCKED", reasons + ["ORDER_MATCH_UNCERTAIN"], False
    if position_state == "NO_POSITION" and order_state == "NO_ORDER":
        return "INTENT_CREATED", reasons + ["PERSISTENT_SIGNAL_NO_POSITION"], True
    return "BLOCKED", reasons + ["POSITION_MATCH_UNCERTAIN"], False


def build_intent_lifecycle_state_v1(
    *,
    day_utc: str,
    truth_root: Path,
    environment: str = PAPER_MODE,
    intent_truth_root: Path | None = None,
    outcomes: list[dict[str, Any]] | None = None,
    previous_by_engine: dict[str, dict[str, Any]] | None = None,
    account_id: str = "",
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    intent_root = Path(intent_truth_root).resolve() if intent_truth_root is not None else resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT)
    input_outcomes = [row for row in outcomes if isinstance(row, dict)] if isinstance(outcomes, list) else _load_outcomes(truth_root=truth_root, day_utc=day_utc)
    previous_by_engine = previous_by_engine if isinstance(previous_by_engine, dict) else {}
    readiness_path, readiness = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=day_utc,
        truth_root=truth_root,
        execution_root=intent_root,
        environment=environment,
    )
    readiness_mode = str(readiness.get("readiness_mode") or "").strip().upper()
    positions_status, positions, positions_path, position_evidence = _load_positions(truth_root=truth_root, intent_truth_root=intent_root, day_utc=day_utc)
    order_rows, order_evidence = _load_order_rows(truth_root=truth_root, intent_truth_root=intent_root, day_utc=day_utc)
    position_lifecycle_status, position_lifecycle_rows, position_lifecycle_path = _load_position_lifecycle_rows(truth_root=truth_root, day_utc=day_utc)
    rows: list[dict[str, Any]] = []
    produced_at = _now_iso()
    for outcome in input_outcomes:
        sleeve_id = str(outcome.get("sleeve_id") or outcome.get("engine_id") or "").strip()
        engine_id = str(outcome.get("engine_id") or sleeve_id).strip()
        previous = previous_by_engine.get(engine_id) or previous_by_engine.get(sleeve_id) or {}
        fields = lifecycle_fields_from_outcome(outcome)
        signal_signature = _signal_signature(outcome)
        unchanged_signal = is_unchanged_signal(outcome, previous)
        signal_state = _signal_state(outcome)
        lifecycle_match = _position_lifecycle_match(
            rows=position_lifecycle_rows,
            sleeve_id=sleeve_id,
            engine_id=engine_id,
            symbol=fields["symbol"],
            underlying=fields["underlying"],
            exposure_type=fields["exposure_type"],
            environment=environment,
            account_id=account_id,
        ) if position_lifecycle_rows else {}
        if lifecycle_match:
            position_state = str(lifecycle_match["position_state"])
            order_state = str(lifecycle_match["order_state"])
        elif positions_status == "POSITION_STATE_STALE":
            position_state = "POSITION_STATE_STALE" if signal_state == "ACTIVE" else "NO_POSITION"
        else:
            position_state = _position_match_state(
                rows=positions,
                sleeve_id=sleeve_id,
                engine_id=engine_id,
                symbol=fields["symbol"],
                underlying=fields["underlying"],
                exposure_type=fields["exposure_type"],
                environment=environment,
                account_id=account_id,
            )
        if not lifecycle_match:
            order_state = _order_match_state(
                rows=order_rows,
                sleeve_id=sleeve_id,
                engine_id=engine_id,
                symbol=fields["symbol"],
                underlying=fields["underlying"],
                exposure_type=fields["exposure_type"],
                environment=environment,
                account_id=account_id,
            )
        decision, reasons, reentry_eligible = _lifecycle_decision(
            signal_state=signal_state,
            unchanged_signal=unchanged_signal,
            position_state=position_state,
            order_state=order_state,
            readiness_mode=readiness_mode,
        )
        rows.append(
            {
                "day_utc": day_utc,
                "sleeve_id": sleeve_id,
                "engine_id": engine_id,
                "symbol": fields["symbol"],
                "underlying": fields["underlying"],
                "exposure_type": fields["exposure_type"],
                "signal_state": signal_state,
                "signal_signature": signal_signature,
                "prior_signal_signature": _previous_signature(previous),
                "unchanged_signal": unchanged_signal,
                "matching_position_state": position_state,
                "matching_order_state": order_state,
                "position_lifecycle_state_path": position_lifecycle_path if lifecycle_match else "",
                "matching_position_id": str(lifecycle_match.get("matching_position_id") or ""),
                "matching_lifecycle_state": str(lifecycle_match.get("matching_lifecycle_state") or ""),
                "position_truth_status": str(lifecycle_match.get("position_truth_status") or positions_status),
                "reentry_allowed_by_position_lifecycle": bool(lifecycle_match.get("reentry_allowed_by_position_lifecycle")) if lifecycle_match else reentry_eligible,
                "reentry_eligible": reentry_eligible,
                "lifecycle_decision": decision,
                "lifecycle_reason_codes": sorted(set(reasons)),
                "evidence_paths": sorted(set([path for path in [positions_path, position_lifecycle_path if lifecycle_match else "", str(outcome.get("artifact_path") or "")] + position_evidence + order_evidence if path])),
                "produced_at_utc": produced_at,
                "producer": PRODUCER,
            }
        )
    counts = {
        "INTENT_CREATED": len([row for row in rows if row["lifecycle_decision"] == "INTENT_CREATED"]),
        "NO_INTENT": len([row for row in rows if row["lifecycle_decision"] == "NO_INTENT"]),
        "BLOCKED": len([row for row in rows if row["lifecycle_decision"] == "BLOCKED"]),
        "DEGRADED": len([row for row in rows if row["lifecycle_decision"] == "DEGRADED"]),
        "reentry_intent_count": len([row for row in rows if "PERSISTENT_SIGNAL_NO_POSITION" in row["lifecycle_reason_codes"]]),
        "suppressed_position_count": len([row for row in rows if "POSITION_ALREADY_OPEN" in row["lifecycle_reason_codes"]]),
        "suppressed_order_count": len([row for row in rows if "ORDER_ALREADY_PENDING" in row["lifecycle_reason_codes"]]),
        "uncertain_position_count": len([row for row in rows if "POSITION_MATCH_UNCERTAIN" in row["lifecycle_reason_codes"]]),
    }
    out_path = intent_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "intent_lifecycle_state",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "account_id": account_id,
        "status": "BLOCKED" if counts["BLOCKED"] else "PASS",
        "canonical_blocker": "INTENT_LIFECYCLE_BLOCKED" if counts["BLOCKED"] else "",
        "positions_snapshot_path": positions_path,
        "position_snapshot_status": positions_status,
        "position_lifecycle_state_path": position_lifecycle_path if position_lifecycle_rows else "",
        "position_lifecycle_status": position_lifecycle_status,
        "readiness_authority_path": str(readiness_path),
        "readiness_mode": readiness_mode,
        "evidence_policy_used": readiness.get("evidence_policy") if isinstance(readiness.get("evidence_policy"), dict) else {},
        "intent_truth_root": str(intent_root),
        "rows": rows,
        "counts": counts,
        "produced_at_utc": produced_at,
        "producer": PRODUCER,
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def rows_by_engine(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    return {str(row.get("engine_id") or row.get("sleeve_id") or ""): row for row in rows if isinstance(row, dict)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_intent_lifecycle_state_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--account_id", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_intent_lifecycle_state_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        environment=str(args.environment).strip().upper(),
        account_id=str(args.account_id or "").strip(),
    )
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": payload["artifact_path"], "counts": payload["counts"]}, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
