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

PAPER_MODE = "PAPER"
PRODUCER = "ops/tools/run_position_lifecycle_state_v1.py"
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
CANCELED_STATES = {"CANCELED", "CANCELLED", "EXPIRED"}
FAILED_STATES = {"FAILED", "ERROR", "REJECTED", "DENIED"}
CLOSED_STATES = {"CLOSED", "DONE", "FILLED_AND_CLOSED", "POSITION_CLOSED"}


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


def _runtime_resilience_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "runtime_resilience_authority_v1" / day_utc / "runtime_resilience_authority.v1.json"


def position_lifecycle_state_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "position_lifecycle_state_v1" / day_utc / "position_lifecycle_state.v1.json"


def _as_rows(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(value, dict):
        rows.append(value)
        for child in value.values():
            rows.extend(_as_rows(child))
    elif isinstance(value, list):
        for child in value:
            rows.extend(_as_rows(child))
    return rows


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _nested(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    for parent_key in ("instrument", "contract", "order", "execution", "position"):
        parent = row.get(parent_key) if isinstance(row.get(parent_key), dict) else {}
        for key in keys:
            if key in parent:
                return parent.get(key)
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
    return _norm(row.get("account_id") or row.get("ib_account") or account.get("account_id") or account.get("ib_account"))


def _row_environment(row: dict[str, Any]) -> str:
    return _norm(row.get("environment") or row.get("mode") or PAPER_MODE)


def _quantity(row: dict[str, Any]) -> float:
    for key in ("quantity_open", "open_quantity", "quantity", "qty", "position", "net_quantity", "filled_quantity", "filled_qty"):
        value = row.get(key)
        if isinstance(value, dict):
            value = value.get("value") or value.get("amount")
        try:
            return float(value)
        except Exception:
            continue
    return 0.0


def _float_value(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, dict):
            value = value.get("value") or value.get("amount")
        try:
            return float(value)
        except Exception:
            continue
    return None


def _same_symbol(row: dict[str, Any], *, symbol: str, underlying: str) -> bool:
    wanted = {_norm(symbol), _norm(underlying)}
    wanted.discard("")
    return bool(wanted and _symbol_values(row).intersection(wanted))


def _same_identity(row: dict[str, Any], *, sleeve_id: str, engine_id: str, environment: str, account: str) -> tuple[bool, bool]:
    row_engine = _row_engine(row)
    engine_match = row_engine in {_norm(sleeve_id), _norm(engine_id)}
    attribution_unknown = not row_engine
    row_env = _row_environment(row)
    env_match = not row_env or row_env == _norm(environment)
    row_account = _row_account(row)
    account_match = not account or not row_account or row_account == _norm(account)
    return engine_match and env_match and account_match, attribution_unknown and env_match and account_match


def _compatible_exposure(expected: str, row: dict[str, Any]) -> bool:
    expected = _norm(expected)
    actual = _norm(row.get("exposure_type") or row.get("risk_class") or row.get("asset_class") or row.get("instrument_type"))
    if expected == "MARKET_NEUTRAL_PAIR":
        legs = row.get("legs")
        return isinstance(legs, list) and len([leg for leg in legs if isinstance(leg, dict)]) >= 2
    if expected == "LONG_EQUITY":
        return not actual or any(token in actual for token in ("EQUITY", "ETF", "STOCK", "LONG"))
    if expected in {"SHORT_VOL_DEFINED", "SHORT_VOL_DEFINED_RISK"}:
        return any(token in actual for token in ("VOL", "OPTION", "SPREAD", "DEFINED")) if actual else True
    if expected == "TAIL_HEDGE":
        return any(token in actual for token in ("TAIL", "HEDGE", "DEFENSIVE", "OPTION", "TREASURY", "BOND")) if actual else True
    return True


def _matching_rows(
    rows: list[dict[str, Any]],
    *,
    sleeve_id: str,
    engine_id: str,
    symbol: str,
    underlying: str,
    exposure_type: str,
    environment: str,
    account: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    matched: list[dict[str, Any]] = []
    uncertain: list[dict[str, Any]] = []
    for row in rows:
        if not _same_symbol(row, symbol=symbol, underlying=underlying):
            continue
        identity_match, attribution_unknown = _same_identity(row, sleeve_id=sleeve_id, engine_id=engine_id, environment=environment, account=account)
        if attribution_unknown and abs(_quantity(row)) > 0:
            uncertain.append(row)
            continue
        if identity_match and _compatible_exposure(exposure_type, row):
            matched.append(row)
    return matched, uncertain


def _first_status(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        status = _norm(row.get("lifecycle_state") or row.get("status") or row.get("state") or row.get("order_status") or row.get("submit_mode_status"))
        if status:
            return status
    return ""


def _first_text(rows: list[dict[str, Any]], *keys: str) -> str:
    for row in rows:
        for key in keys:
            value = row.get(key)
            if isinstance(value, dict):
                value = value.get("id") or value.get("value")
            text = _text(value)
            if text:
                return text
    return ""


def _paths_for_candidates(paths: list[Path]) -> list[str]:
    return sorted({str(path.resolve()) for path in paths})


def _load_rows_from_paths(paths: list[Path]) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    evidence = _paths_for_candidates(paths)
    for path in paths:
        if not path.is_file():
            continue
        rows.extend(_as_rows(_read_json(path)))
    return rows, evidence


def _position_snapshot_candidates(truth_root: Path, execution_root: Path, day_utc: str) -> list[Path]:
    return [
        execution_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json",
        execution_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json",
        execution_root / "positions_snapshot_v2" / "snapshots" / day_utc / "positions_snapshot.v2.json",
        truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json",
        truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json",
    ]


def _load_positions(truth_root: Path, execution_root: Path, day_utc: str) -> tuple[str, list[dict[str, Any]], str, list[str]]:
    candidates = _position_snapshot_candidates(truth_root, execution_root, day_utc)
    evidence = _paths_for_candidates(candidates)
    for path in candidates:
        if not path.is_file():
            continue
        payload = _read_json(path)
        status = _norm(payload.get("status") or payload.get("snapshot_status") or "OK")
        payload_day = _text(payload.get("day_utc") or payload.get("as_of_day_utc") or day_utc)
        if payload_day and payload_day != day_utc:
            return "POSITION_STATE_STALE", [], str(path), evidence
        if status in {"BLOCKED", "ERROR", "FAILED", "STALE"}:
            return "POSITION_STATE_STALE", [], str(path), evidence
        items = payload.get("items")
        if not isinstance(items, list):
            items = payload.get("positions")
        if isinstance(items, dict):
            items = items.get("items")
        rows = [row for row in items if isinstance(row, dict)] if isinstance(items, list) else []
        return "OK", rows, str(path), evidence
    return "POSITION_STATE_STALE", [], "", evidence


def _selected_intent(truth_root: Path) -> tuple[dict[str, Any], str]:
    path = truth_root / "pointers" / "selected_intent_pointer.v1.json"
    pointer = _read_json(path)
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    merged = dict(selected)
    for key in ("selected_intent_id", "intent_id", "symbol", "underlying", "engine_id", "sleeve_id", "environment"):
        if key in pointer and key not in merged:
            merged[key] = pointer.get(key)
    merged["pointer_status"] = pointer.get("status")
    return merged, str(path.resolve())


def _artifact_paths(truth_root: Path, execution_root: Path, day_utc: str) -> dict[str, list[Path]]:
    broker_dirs = [
        truth_root / "execution_evidence_v1" / "submissions" / day_utc,
        execution_root / "execution_evidence_v1" / "submissions" / day_utc,
    ]
    broker_paths: list[Path] = []
    for root in broker_dirs:
        if root.is_dir():
            broker_paths.extend(sorted(root.rglob("broker_submission_record*.json")))
    fill_roots = [truth_root / "fill_ledger_v1" / day_utc, execution_root / "fill_ledger_v1" / day_utc]
    fill_paths: list[Path] = []
    for root in fill_roots:
        if root.is_dir():
            fill_paths.extend(sorted(root.glob("*.json")))
    return {
        "authorization_supply": [truth_root / "reports" / "authorization_supply_v1" / day_utc / "authorization_supply.v1.json"],
        "submission_index": [
            truth_root / "submission_index_v1" / day_utc / "submission_index.v1.json",
            execution_root / "submission_index_v1" / day_utc / "submission_index.v1.json",
        ],
        "current_head": [
            truth_root / "execution_evidence_v1" / "current_head" / day_utc / "current_head.v1.json",
            execution_root / "execution_evidence_v1" / "current_head" / day_utc / "current_head.v1.json",
        ],
        "broker_submission_record": broker_paths,
        "execution_lifecycle_authority": [
            truth_root / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json",
            execution_root / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json",
        ],
        "fill_ledger": fill_paths,
        "attribution": [
            truth_root / "reports" / "engine_attribution_history_v1" / day_utc / "engine_attribution_history.v1.json",
            execution_root / "accounting_v2" / "attribution" / day_utc / "engine_attribution.v2.json",
        ],
    }


def _classify_lifecycle(
    *,
    matched_positions: list[dict[str, Any]],
    uncertain_positions: list[dict[str, Any]],
    matched_orders: list[dict[str, Any]],
    matched_fills: list[dict[str, Any]],
    matched_lifecycle: list[dict[str, Any]],
    positions_status: str,
) -> tuple[str, list[str]]:
    if positions_status == "POSITION_STATE_STALE":
        return "UNKNOWN", ["POSITION_STATE_STALE"]
    if uncertain_positions:
        return "UNKNOWN", ["POSITION_MATCH_UNCERTAIN"]
    all_status_rows = matched_orders + matched_fills + matched_lifecycle + matched_positions
    status = _first_status(all_status_rows)
    if status in FAILED_STATES:
        return "FAILED", ["SUBMISSION_FAILED"]
    if status in CANCELED_STATES:
        return "CANCELED", ["SUBMISSION_CANCELED"]
    if status in CLOSED_STATES:
        return "POSITION_CLOSED", ["POSITION_CLOSED"]
    for row in matched_positions:
        if abs(_quantity(row)) > 0 or _norm(row.get("state") or row.get("status")) in {"ACTIVE", "OPEN"}:
            return "POSITION_OPEN", []
    fill_qty = sum(abs(_quantity(row)) for row in matched_fills)
    if fill_qty > 0:
        if status == "PARTIALLY_FILLED" or any(_float_value(row, "remaining_quantity", "remaining_qty") not in (None, 0.0) for row in matched_fills):
            return "PARTIALLY_FILLED", []
        return "POSITION_OPEN", []
    if any((_first_status([row]) in ACTIVE_ORDER_STATES) or not _first_status([row]) for row in matched_orders):
        return "ORDER_PENDING", []
    return "NO_POSITION", []


def _position_row(
    *,
    day_utc: str,
    selected: dict[str, Any],
    selected_pointer_path: str,
    environment: str,
    account: str,
    execution_root: Path,
    positions_status: str,
    positions_path: str,
    positions: list[dict[str, Any]],
    rows_by_family: dict[str, list[dict[str, Any]]],
    evidence_paths: list[str],
) -> dict[str, Any]:
    intent_id = _text(selected.get("intent_id") or selected.get("selected_intent_id"))
    sleeve_id = _text(selected.get("sleeve_id") or selected.get("engine_id"))
    engine_id = _text(selected.get("engine_id") or sleeve_id)
    symbol = _norm(selected.get("symbol") or selected.get("underlying"))
    underlying = _norm(selected.get("underlying") or symbol)
    exposure_type = _norm(selected.get("exposure_type") or selected.get("risk_class") or ("SHORT_VOL_DEFINED_RISK" if "VOL_INCOME" in engine_id.upper() else "LONG_EQUITY"))
    selected_account = _text(selected.get("account") or selected.get("account_id") or account)
    matched_positions, uncertain_positions = _matching_rows(
        positions,
        sleeve_id=sleeve_id,
        engine_id=engine_id,
        symbol=symbol,
        underlying=underlying,
        exposure_type=exposure_type,
        environment=environment,
        account=selected_account,
    )
    matched_orders: list[dict[str, Any]] = []
    for family in ("authorization_supply", "submission_index", "current_head", "broker_submission_record"):
        matched, _uncertain = _matching_rows(
            rows_by_family.get(family, []),
            sleeve_id=sleeve_id,
            engine_id=engine_id,
            symbol=symbol,
            underlying=underlying,
            exposure_type=exposure_type,
            environment=environment,
            account=selected_account,
        )
        matched_orders.extend(matched)
    matched_fills, _fill_uncertain = _matching_rows(
        rows_by_family.get("fill_ledger", []),
        sleeve_id=sleeve_id,
        engine_id=engine_id,
        symbol=symbol,
        underlying=underlying,
        exposure_type=exposure_type,
        environment=environment,
        account=selected_account,
    )
    matched_lifecycle, _life_uncertain = _matching_rows(
        rows_by_family.get("execution_lifecycle_authority", []),
        sleeve_id=sleeve_id,
        engine_id=engine_id,
        symbol=symbol,
        underlying=underlying,
        exposure_type=exposure_type,
        environment=environment,
        account=selected_account,
    )
    lifecycle_state, reason_codes = _classify_lifecycle(
        matched_positions=matched_positions,
        uncertain_positions=uncertain_positions,
        matched_orders=matched_orders,
        matched_fills=matched_fills,
        matched_lifecycle=matched_lifecycle,
        positions_status=positions_status,
    )
    if not intent_id and not symbol:
        lifecycle_state = "UNKNOWN"
        reason_codes = sorted(set(reason_codes + ["SELECTED_INTENT_MISSING"]))
    quantity_open = sum(_quantity(row) for row in matched_positions)
    realized = next((value for value in (_float_value(row, "realized_pnl", "realized_pnl_usd") for row in rows_by_family.get("attribution", [])) if value is not None), None)
    unrealized = next((value for value in (_float_value(row, "unrealized_pnl", "unrealized_pnl_usd") for row in matched_positions) if value is not None), None)
    submission_id = _first_text(matched_orders, "submission_id", "submission_record_id", "client_order_id")
    fill_id = _first_text(matched_fills, "fill_id", "execution_id", "exec_id", "broker_exec_id")
    broker_order_id = _first_text(matched_orders, "broker_order_id", "order_id")
    perm_id = _first_text(matched_orders, "perm_id", "broker_perm_id")
    position_id = "|".join([environment, selected_account or "UNKNOWN_ACCOUNT", sleeve_id or engine_id or "UNKNOWN_ENGINE", symbol or underlying or "UNKNOWN_SYMBOL"])
    return {
        "position_id": position_id,
        "sleeve_id": sleeve_id,
        "engine_id": engine_id,
        "intent_id": intent_id,
        "submission_id": submission_id,
        "fill_id": fill_id,
        "broker_order_id": broker_order_id,
        "perm_id": perm_id,
        "symbol": symbol,
        "underlying": underlying,
        "exposure_type": exposure_type,
        "account": selected_account,
        "environment": environment,
        "lifecycle_state": lifecycle_state,
        "quantity_open": quantity_open,
        "avg_entry_price": _float_value(matched_positions[0], "avg_entry_price", "average_cost", "avg_cost") if matched_positions else None,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "opened_at_utc": _first_text(matched_positions + matched_fills + matched_orders, "opened_at_utc", "filled_at_utc", "submitted_at_utc", "created_at_utc"),
        "closed_at_utc": _first_text(matched_positions + matched_fills + matched_lifecycle, "closed_at_utc", "canceled_at_utc", "failed_at_utc"),
        "evidence_paths": sorted(set([selected_pointer_path, positions_path, *evidence_paths])),
        "reason_codes": sorted(set(reason_codes)),
        "position_truth_status": positions_status,
        "selected_intent_path": _text(selected.get("intent_path")),
        "execution_root": str(execution_root),
    }


def build_position_lifecycle_state_v1(
    *,
    day_utc: str,
    truth_root: Path,
    environment: str = PAPER_MODE,
    execution_root: Path | None = None,
    account: str = "",
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution = Path(execution_root).resolve() if execution_root is not None else resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT)
    selected, selected_pointer_path = _selected_intent(truth_root)
    paths = _artifact_paths(truth_root, execution, day_utc)
    rows_by_family: dict[str, list[dict[str, Any]]] = {}
    evidence_paths: list[str] = [selected_pointer_path]
    for family, family_paths in paths.items():
        rows, evidence = _load_rows_from_paths(family_paths)
        rows_by_family[family] = rows
        evidence_paths.extend(evidence)
    positions_status, positions, positions_path, position_evidence = _load_positions(truth_root, execution, day_utc)
    evidence_paths.extend(position_evidence)
    runtime_path = _runtime_resilience_path(truth_root=truth_root, day_utc=day_utc)
    runtime_resilience = _read_json(runtime_path)
    row = _position_row(
        day_utc=day_utc,
        selected=selected,
        selected_pointer_path=selected_pointer_path,
        environment=environment,
        account=account,
        execution_root=execution,
        positions_status=positions_status,
        positions_path=positions_path,
        positions=positions,
        rows_by_family=rows_by_family,
        evidence_paths=evidence_paths,
    )
    rows = [row]
    counts = {
        "open_position_count": len([item for item in rows if item["lifecycle_state"] == "POSITION_OPEN"]),
        "pending_order_count": len([item for item in rows if item["lifecycle_state"] == "ORDER_PENDING"]),
        "closed_position_count": len([item for item in rows if item["lifecycle_state"] == "POSITION_CLOSED"]),
        "uncertain_position_count": len([item for item in rows if "POSITION_MATCH_UNCERTAIN" in item["reason_codes"]]),
    }
    status = "FAIL" if any("POSITION_STATE_STALE" in item["reason_codes"] for item in rows) else ("DEGRADED" if counts["uncertain_position_count"] else "PASS")
    out_path = position_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "position_lifecycle_state",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "account": account,
        "status": status,
        "canonical_blocker": "POSITION_STATE_STALE" if status == "FAIL" else ("POSITION_MATCH_UNCERTAIN" if status == "DEGRADED" else ""),
        "truth_root": str(truth_root),
        "execution_root": str(execution),
        "positions_snapshot_path": positions_path,
        "position_truth_status": positions_status,
        "runtime_resilience_authority_path": str(runtime_path),
        "runtime_resilience_status": str(runtime_resilience.get("status") or "UNKNOWN").strip().upper(),
        "runtime_resilience_blocker": str(runtime_resilience.get("canonical_blocker") or "").strip().upper(),
        "rows": rows,
        "counts": counts,
        "produced_at_utc": _now_iso(),
        "producer": PRODUCER,
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_position_lifecycle_state_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--execution_root", default="")
    parser.add_argument("--account", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    execution_root = Path(args.execution_root).resolve() if str(args.execution_root or "").strip() else None
    payload = build_position_lifecycle_state_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        environment=str(args.environment).strip().upper(),
        execution_root=execution_root,
        account=str(args.account or "").strip(),
    )
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": payload["artifact_path"], "counts": payload["counts"]}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "DEGRADED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
