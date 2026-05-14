#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.run_intent_arbitration_v1 import selected_intent_pointer_path
from ops.tools.run_market_data_supply_v1 import _validate_equity_snapshot

SCHEMA_VERSION = "equity_market_data_snapshot_required_day.v1"
SNAPSHOT_SCHEMA_ID = "market_data_snapshot_v1"
FRESHNESS_SECONDS_DEFAULT = 300
CAPTURE_SECONDS_DEFAULT = 15
IB_CLIENT_ID_DEFAULT = 17
EQUITY_EXPOSURE_TYPES = {"LONG_EQUITY", "EQUITY_SPOT"}


class EquityCaptureError(Exception):
    pass


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _stable_json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _quarantine_existing(path: Path) -> Path:
    qdir = (path.parent / "__quarantine__").resolve()
    qdir.mkdir(parents=True, exist_ok=True)
    qpath = (qdir / f"{path.name}.REPLACED_{_sha256_file(path)}.json").resolve()
    os.replace(path, qpath)
    return qpath


def _write_owner_refreshed_json(path: Path, payload: dict[str, Any]) -> tuple[str, str, str]:
    data = _stable_json_bytes(payload)
    candidate_sha = _sha256_bytes(data)
    quarantine_path = ""
    if path.exists():
        existing_sha = _sha256_file(path)
        if existing_sha == candidate_sha:
            return "EXISTS_IDENTICAL", candidate_sha, quarantine_path
        quarantine_path = str(_quarantine_existing(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)
    return "WROTE" if not quarantine_path else "REFRESHED", candidate_sha, quarantine_path


def _snapshot_path(*, truth_root: Path, day_utc: str, symbol: str) -> Path:
    return (
        truth_root
        / "market_data_snapshot_v1"
        / "snapshots"
        / day_utc
        / f"{symbol.upper()}.market_data_snapshot.v1.json"
    ).resolve()


def _freshness_path(*, truth_root: Path, day_utc: str, symbol: str) -> Path:
    return (
        truth_root
        / "market_data_snapshot_v1"
        / "snapshots"
        / day_utc
        / f"{symbol.upper()}.freshness_certificate.v1.json"
    ).resolve()


def _report_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "equity_market_data_snapshot_required_day_v1"
        / day_utc
        / "equity_market_data_snapshot_required_day.v1.json"
    ).resolve()


def _dec(value: Any, field: str) -> Decimal:
    try:
        dec = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise EquityCaptureError(f"EQUITY_QUOTE_FIELD_INVALID:{field}") from exc
    if dec <= Decimal("0"):
        raise EquityCaptureError(f"EQUITY_QUOTE_FIELD_NON_POSITIVE:{field}")
    return dec


def _money(value: Any, field: str) -> str:
    return str(_dec(value, field).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _first_price(quote: dict[str, Any], *keys: str) -> tuple[str, str]:
    for key in keys:
        raw = quote.get(key)
        if raw not in (None, ""):
            return _money(raw, key), key
    return "", ""


def _select_spot(quote: dict[str, Any]) -> tuple[str, str]:
    value, source = _first_price(quote, "last", "delayed_last", "close", "delayed_close")
    if value:
        return value, source
    bid, _ = _first_price(quote, "bid", "delayed_bid")
    ask, _ = _first_price(quote, "ask", "delayed_ask")
    if bid and ask:
        mid = (_dec(bid, "bid") + _dec(ask, "ask")) / Decimal("2")
        return str(mid.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)), "bid_ask_midpoint"
    raise EquityCaptureError("EQUITY_UNDERLYING_SPOT_MISSING")


def _has_bid_ask(quote: dict[str, Any]) -> bool:
    try:
        _first_price(quote, "bid")
        _first_price(quote, "ask")
        if quote.get("bid") not in (None, "") and quote.get("ask") not in (None, ""):
            return True
        if quote.get("delayed_bid") not in (None, "") and quote.get("delayed_ask") not in (None, ""):
            return True
    except EquityCaptureError:
        return False
    return False


def _market_data_mode(quote: dict[str, Any], requested_type: int) -> str:
    try:
        callback = int(quote.get("market_data_type_callback"))
    except Exception:
        callback = int(requested_type)
    if callback == 1:
        return "LIVE"
    if callback in {3, 4} or quote.get("delayed_bid") not in (None, "") or quote.get("delayed_ask") not in (None, ""):
        return "DELAYED"
    return "UNKNOWN"


def _ib_capture_quote(*, symbol: str, host: str, port: int, client_id: int, max_capture_seconds: int) -> dict[str, Any]:
    try:
        from ops.tools.run_options_chain_capture_ib_day_v1 import (  # type: ignore
            CaptureError,
            _IbCaptureClient,
            _stock_contract,
        )
    except Exception as exc:  # noqa: BLE001
        raise EquityCaptureError(f"IBAPI_IMPORT_ERROR:{exc}") from exc

    client = _IbCaptureClient()
    try:
        try:
            client.connect_and_wait(host=host, port=int(port), client_id=int(client_id), timeout_seconds=min(8.0, float(max_capture_seconds)))
        except CaptureError as exc:
            raise EquityCaptureError(str(exc)) from exc
        details = client.request_contract_details(contract=_stock_contract(symbol=symbol), timeout_seconds=8.0)
        if not details:
            raise EquityCaptureError(f"EQUITY_CONTRACT_DETAILS_MISSING:{symbol}")
        contract = details[0].contract
        attempts: list[dict[str, Any]] = []
        for market_data_type in (1, 3, 4):
            client.reqMarketDataType(int(market_data_type))
            quote = client.request_snapshot(contract=contract, generic_ticks="", timeout_seconds=4.0)
            quote["market_data_type_requested"] = int(market_data_type)
            quote["ib_errors"] = client.error_events()
            attempts.append(dict(quote))
            if _has_bid_ask(quote):
                spot, spot_source = _select_spot(quote)
                quote["spot"] = spot
                quote["spot_source"] = spot_source
                quote["market_data_mode"] = _market_data_mode(quote, int(market_data_type))
                quote["attempts"] = attempts
                return quote
        raise EquityCaptureError("EQUITY_QUOTES_MISSING_BID_ASK")
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


def _normalize_capture_quote(capture: dict[str, Any]) -> dict[str, Any]:
    quote = dict(capture)
    if not _has_bid_ask(quote):
        raise EquityCaptureError("EQUITY_QUOTES_MISSING_BID_ASK")
    spot, spot_source = _select_spot(quote)
    quote["spot"] = spot
    quote["spot_source"] = str(quote.get("spot_source") or spot_source)
    if quote.get("bid") not in (None, ""):
        quote["bid"] = _money(quote.get("bid"), "bid")
    if quote.get("ask") not in (None, ""):
        quote["ask"] = _money(quote.get("ask"), "ask")
    if quote.get("delayed_bid") not in (None, ""):
        quote["delayed_bid"] = _money(quote.get("delayed_bid"), "delayed_bid")
    if quote.get("delayed_ask") not in (None, ""):
        quote["delayed_ask"] = _money(quote.get("delayed_ask"), "delayed_ask")
    for key in ("last", "delayed_last", "close", "delayed_close"):
        if quote.get(key) not in (None, ""):
            quote[key] = _money(quote.get(key), key)
    return quote


def _capture_equity_quote(*, symbol: str, host: str, port: int, client_id: int, max_capture_seconds: int) -> dict[str, Any]:
    return _normalize_capture_quote(
        _ib_capture_quote(
            symbol=symbol,
            host=host,
            port=int(port),
            client_id=int(client_id),
            max_capture_seconds=int(max_capture_seconds),
        )
    )


def _selected_pointer_equity_symbols(*, truth_root: Path, day_utc: str) -> tuple[bool, list[str]]:
    pointer_path = selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc)
    pointer = _read_json(pointer_path)
    if not pointer:
        return False, []
    if str(pointer.get("status") or "").strip().upper() != "SELECTED":
        return True, []
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    intent_path = str(selected.get("intent_path") or "").strip()
    payload = _read_json(Path(intent_path).expanduser().resolve()) if intent_path else {}
    source = payload or selected
    exposure_type = str(source.get("exposure_type") or source.get("structure") or "").strip().upper()
    if exposure_type not in EQUITY_EXPOSURE_TYPES:
        return True, []
    underlying = source.get("underlying") if isinstance(source.get("underlying"), dict) else {}
    symbol = str(underlying.get("symbol") or source.get("symbol") or selected.get("symbol") or "").strip().upper()
    return True, [symbol] if symbol else []


def _existing_validation(*, day_utc: str, environment: str, truth_root: Path, symbol: str, eval_time_utc: str) -> tuple[bool, str, dict[str, Any]]:
    ctx = bod.BodContext(
        day_utc=day_utc,
        environment=environment,
        truth_root=truth_root,
        execution_root=truth_root,
        runtime_root=truth_root.parent,
        operator_input_root=truth_root / "operator_inputs",
        ib_account="",
    )
    blocker, artifact = _validate_equity_snapshot(ctx=ctx, instrument=symbol, eval_time_utc=eval_time_utc, data_mode="EQUITY")
    return not blocker, blocker, artifact


def _build_snapshot_payload(
    *,
    day_utc: str,
    symbol: str,
    quote: dict[str, Any],
    eval_time_utc: str,
    host: str,
    port: int,
    client_id: int,
) -> dict[str, Any]:
    as_of = str(quote.get("quote_as_of_utc") or quote.get("as_of_utc") or eval_time_utc).strip()
    as_of_dt = _parse_iso(as_of)
    if as_of_dt is None or as_of_dt.date().isoformat() != day_utc:
        raise EquityCaptureError("EQUITY_MARKET_DATA_STALE")
    spot = _money(quote.get("spot"), "spot")
    payload: dict[str, Any] = {
        "schema_id": SNAPSHOT_SCHEMA_ID,
        "schema_version": "v1",
        "day_utc": day_utc,
        "symbol": symbol.upper(),
        "quote_as_of_utc": as_of,
        "timestamp_utc": as_of,
        "spot": spot,
        "last": _money(quote.get("last"), "last") if quote.get("last") not in (None, "") else spot,
        "close": _money(quote.get("close"), "close") if quote.get("close") not in (None, "") else spot,
        "provenance": {
            "source": "IBKR_API",
            "capture_method": "IBKR_EQUITY_SNAPSHOT_REQUIRED_DAY",
            "market_data_mode": str(quote.get("market_data_mode") or "UNKNOWN"),
            "market_data_type_requested": quote.get("market_data_type_requested"),
            "market_data_type_callback": quote.get("market_data_type_callback"),
            "host": host,
            "port": int(port),
            "client_id": int(client_id),
            "producer": "ops/tools/run_equity_market_data_snapshot_required_day_v1.py",
        },
        "quote": {},
    }
    if quote.get("bid") not in (None, "") and quote.get("ask") not in (None, ""):
        payload["bid"] = _money(quote.get("bid"), "bid")
        payload["ask"] = _money(quote.get("ask"), "ask")
        payload["quote"] = {"bid": payload["bid"], "ask": payload["ask"]}
    elif quote.get("delayed_bid") not in (None, "") and quote.get("delayed_ask") not in (None, ""):
        payload["delayed_bid"] = _money(quote.get("delayed_bid"), "delayed_bid")
        payload["delayed_ask"] = _money(quote.get("delayed_ask"), "delayed_ask")
        payload["quote"] = {"delayed_bid": payload["delayed_bid"], "delayed_ask": payload["delayed_ask"]}
    else:
        raise EquityCaptureError("EQUITY_QUOTES_MISSING_BID_ASK")
    return payload


def _build_freshness_payload(*, day_utc: str, symbol: str, quote_as_of_utc: str, eval_time_utc: str, freshness_seconds: int) -> dict[str, Any]:
    as_of = _parse_iso(quote_as_of_utc)
    eval_time = _parse_iso(eval_time_utc)
    if as_of is None or eval_time is None:
        raise EquityCaptureError("EQUITY_MARKET_DATA_TIMESTAMP_INVALID")
    if as_of.date().isoformat() != day_utc:
        raise EquityCaptureError("EQUITY_MARKET_DATA_STALE")
    valid_until = max(as_of, eval_time) + timedelta(seconds=int(freshness_seconds))
    return {
        "schema_id": "market_data_freshness_certificate",
        "schema_version": "v1",
        "day_utc": day_utc,
        "symbol": symbol.upper(),
        "evidence_as_of_utc": quote_as_of_utc,
        "evaluated_at_utc": eval_time_utc,
        "valid_until_utc": valid_until.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "freshness_seconds": int(freshness_seconds),
        "producer": "ops/tools/run_equity_market_data_snapshot_required_day_v1.py",
    }


def build_equity_market_data_snapshot_required_day(
    *,
    day_utc: str,
    environment: str,
    truth_root: Path,
    symbols: list[str],
    symbols_from_intents: bool,
    capture_missing: bool,
    eval_time_utc: str,
) -> dict[str, Any]:
    pointer_present = False
    pointer_symbols: list[str] = []
    if symbols_from_intents:
        pointer_present, pointer_symbols = _selected_pointer_equity_symbols(truth_root=truth_root, day_utc=day_utc)
    required_symbols = sorted({str(symbol or "").strip().upper() for symbol in [*symbols, *pointer_symbols] if str(symbol or "").strip()})
    host = str(os.environ.get("C2_IB_HOST") or "127.0.0.1").strip()
    port = int(str(os.environ.get("C2_IB_PORT") or "4002").strip())
    client_id = int(str(os.environ.get("C2_EQUITY_SNAPSHOT_IB_CLIENT_ID") or os.environ.get("C2_IB_EQUITY_CLIENT_ID") or IB_CLIENT_ID_DEFAULT).strip())
    max_capture_seconds = int(str(os.environ.get("C2_EQUITY_SNAPSHOT_MAX_SECONDS") or CAPTURE_SECONDS_DEFAULT).strip())
    freshness_seconds = int(str(os.environ.get("C2_EQUITY_SNAPSHOT_FRESHNESS_SECONDS") or FRESHNESS_SECONDS_DEFAULT).strip())

    results: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []
    if not required_symbols:
        return {
            "schema_id": "equity_market_data_snapshot_required_day",
            "schema_version": SCHEMA_VERSION,
            "day_utc": day_utc,
            "environment": environment,
            "truth_root": str(truth_root),
            "generated_at_utc": _now_iso(),
            "status": "OK",
            "canonical_blocker": "",
            "required_symbols": [],
            "selected_pointer_present": pointer_present,
            "reason": "NO_ACTIVE_EQUITY_INTENTS",
            "results": [],
            "steps": [],
        }

    for symbol in required_symbols:
        valid_before, blocker_before, artifact_before = _existing_validation(
            day_utc=day_utc,
            environment=environment,
            truth_root=truth_root,
            symbol=symbol,
            eval_time_utc=eval_time_utc,
        )
        if valid_before:
            results.append(
                {
                    "symbol": symbol,
                    "status": "PASS",
                    "reason_code": "",
                    "path": artifact_before.get("snapshot_path"),
                    "freshness_certificate_path": artifact_before.get("freshness_certificate_path"),
                    "action": "EXISTS_VALID",
                }
            )
            continue
        if not capture_missing:
            results.append(
                {
                    "symbol": symbol,
                    "status": "FAIL",
                    "reason_code": blocker_before or "EQUITY_MARKET_DATA_SNAPSHOT_MISSING",
                    "path": str(_snapshot_path(truth_root=truth_root, day_utc=day_utc, symbol=symbol)),
                    "freshness_certificate_path": str(_freshness_path(truth_root=truth_root, day_utc=day_utc, symbol=symbol)),
                    "action": "CAPTURE_DISABLED",
                }
            )
            continue
        try:
            quote = _capture_equity_quote(
                symbol=symbol,
                host=host,
                port=port,
                client_id=client_id,
                max_capture_seconds=max_capture_seconds,
            )
            snapshot = _build_snapshot_payload(
                day_utc=day_utc,
                symbol=symbol,
                quote=quote,
                eval_time_utc=eval_time_utc,
                host=host,
                port=port,
                client_id=client_id,
            )
            cert = _build_freshness_payload(
                day_utc=day_utc,
                symbol=symbol,
                quote_as_of_utc=str(snapshot["quote_as_of_utc"]),
                eval_time_utc=eval_time_utc,
                freshness_seconds=freshness_seconds,
            )
            snap_path = _snapshot_path(truth_root=truth_root, day_utc=day_utc, symbol=symbol)
            cert_path = _freshness_path(truth_root=truth_root, day_utc=day_utc, symbol=symbol)
            snap_action, snap_sha, snap_quarantine = _write_owner_refreshed_json(snap_path, snapshot)
            cert_action, cert_sha, cert_quarantine = _write_owner_refreshed_json(cert_path, cert)
            steps.append(
                {
                    "symbol": symbol,
                    "producer": "equity_market_data_snapshot_required_day_v1",
                    "snapshot_action": snap_action,
                    "freshness_action": cert_action,
                    "snapshot_path": str(snap_path),
                    "freshness_certificate_path": str(cert_path),
                    "snapshot_sha256": snap_sha,
                    "freshness_certificate_sha256": cert_sha,
                    "snapshot_quarantine_path": snap_quarantine,
                    "freshness_quarantine_path": cert_quarantine,
                }
            )
            valid_after, blocker_after, artifact_after = _existing_validation(
                day_utc=day_utc,
                environment=environment,
                truth_root=truth_root,
                symbol=symbol,
                eval_time_utc=eval_time_utc,
            )
            results.append(
                {
                    "symbol": symbol,
                    "status": "PASS" if valid_after else "FAIL",
                    "reason_code": "" if valid_after else blocker_after,
                    "path": artifact_after.get("snapshot_path") or str(snap_path),
                    "freshness_certificate_path": artifact_after.get("freshness_certificate_path") or str(cert_path),
                    "action": snap_action,
                }
            )
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "symbol": symbol,
                    "status": "FAIL",
                    "reason_code": str(exc) or "EQUITY_SNAPSHOT_CAPTURE_FAILED",
                    "path": str(_snapshot_path(truth_root=truth_root, day_utc=day_utc, symbol=symbol)),
                    "freshness_certificate_path": str(_freshness_path(truth_root=truth_root, day_utc=day_utc, symbol=symbol)),
                    "action": "CAPTURE_FAILED",
                }
            )

    failed = [row for row in results if row.get("status") != "PASS"]
    return {
        "schema_id": "equity_market_data_snapshot_required_day",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "environment": environment,
        "truth_root": str(truth_root),
        "generated_at_utc": _now_iso(),
        "status": "OK" if not failed else "BLOCKED_VALID",
        "canonical_blocker": "" if not failed else str(failed[0].get("reason_code") or "EQUITY_MARKET_DATA_SNAPSHOT_MISSING"),
        "required_symbols": required_symbols,
        "selected_pointer_present": pointer_present,
        "results": results,
        "steps": steps,
        "producer_command": (
            f"python3 ops/tools/run_equity_market_data_snapshot_required_day_v1.py --day_utc {day_utc} "
            f"--truth_root {truth_root} --symbols_from_intents {'YES' if symbols_from_intents else 'NO'}"
        ),
        "eval_time_utc": eval_time_utc,
    }


def run_equity_market_data_snapshot_required_day_v1(
    day_utc: str,
    environment: str,
    truth_root: str,
    symbols: list[str] | None = None,
    symbols_from_intents: bool = True,
    capture_missing: bool = True,
    eval_time_utc: str = "",
) -> tuple[Path, dict[str, Any]]:
    parsed_day = parse_day_utc_v1(day_utc)
    root = Path(truth_root).expanduser().resolve() if str(truth_root or "").strip() else bod._resolve_context(parsed_day, environment, "").execution_root
    eval_time = str(eval_time_utc or "").strip() or _now_iso()
    payload = build_equity_market_data_snapshot_required_day(
        day_utc=parsed_day,
        environment=str(environment or "PAPER").strip().upper(),
        truth_root=root,
        symbols=list(symbols or []),
        symbols_from_intents=bool(symbols_from_intents),
        capture_missing=bool(capture_missing),
        eval_time_utc=eval_time,
    )
    path = _report_path(truth_root=root, day_utc=parsed_day)
    input_artifacts = [selected_intent_pointer_path(truth_root=root, day_utc=parsed_day)]
    output_artifacts: list[Any] = [path]
    for row in payload.get("results", []):
        if isinstance(row, dict):
            output_artifacts.append(row.get("path"))
            output_artifacts.append(row.get("freshness_certificate_path"))
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_equity_market_data_snapshot_required_day_v1.py",
        producer_command=f"python3 ops/tools/run_equity_market_data_snapshot_required_day_v1.py --day_utc {parsed_day} --environment {environment}",
        input_artifacts=input_artifacts,
        output_artifacts=output_artifacts,
        schema_versions={"equity_market_data_snapshot_required_day": SCHEMA_VERSION, SNAPSHOT_SCHEMA_ID: "v1"},
    )
    _write_owner_refreshed_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_equity_market_data_snapshot_required_day_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--symbols_from_intents", choices=["YES", "NO"], default="YES")
    parser.add_argument("--capture_missing", choices=["YES", "NO"], default="YES")
    parser.add_argument("--eval_time_utc", default="")
    args = parser.parse_args(argv)
    symbols = [str(symbol or "").strip().upper() for symbol in args.symbol if str(symbol or "").strip()]
    path, payload = run_equity_market_data_snapshot_required_day_v1(
        day_utc=parse_day_utc_v1(args.day_utc),
        environment=str(args.environment or "PAPER").strip().upper(),
        truth_root=str(args.truth_root or ""),
        symbols=symbols,
        symbols_from_intents=args.symbols_from_intents == "YES",
        capture_missing=args.capture_missing == "YES",
        eval_time_utc=str(args.eval_time_utc or "").strip(),
    )
    print(
        json.dumps(
            {
                "status": payload["status"],
                "canonical_blocker": payload["canonical_blocker"],
                "equity_market_data_snapshot_required_day_path": str(path),
                "results": payload.get("results", []),
            },
            sort_keys=True,
        )
    )
    return 0 if payload.get("status") == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
