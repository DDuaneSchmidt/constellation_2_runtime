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

from constellation_2.common.aegis_market_context_v1 import (  # noqa: E402
    build_event_market_snapshot_v1,
    now_utc_v1,
    validate_event_market_snapshot_v1,
    write_event_market_snapshot_v1,
)
from ops.aegis.intelligence_common_v1 import latest_json_v1  # noqa: E402
from ops.aegis.market_context_demand_v1 import (  # noqa: E402
    build_market_context_demand_v1,
    market_context_snapshot_inputs_v1,
    write_market_context_demand_v1,
)
from ops.aegis.event_append_transaction_v1 import (  # noqa: E402
    contract_input_hashes_for_paths_v1,
    emit_artifact_evidence_transaction_v1,
    sha256_file_v1,
)


def _today_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _read_json(path: str) -> Any:
    return json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def _read_json_path(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
    except (OSError, json.JSONDecodeError):
        return []
    return rows


def _day(value: Any) -> str:
    return str(value or "")[:10]


def _pct(current: Any, prior: Any) -> str:
    try:
        current_f = float(current)
        prior_f = float(prior)
    except (TypeError, ValueError):
        return ""
    if prior_f == 0:
        return ""
    return f"{((current_f - prior_f) / prior_f) * 100:.6f}".rstrip("0").rstrip(".")


def _sma(rows: list[dict[str, Any]], window: int) -> float | None:
    closes = []
    for row in rows[-window:]:
        try:
            closes.append(float(row.get("close")))
        except (TypeError, ValueError):
            return None
    if len(closes) < window:
        return None
    return sum(closes) / len(closes)


def _symbol_rows(root: Path, symbol: str, day_utc: str) -> tuple[Path, list[dict[str, Any]]]:
    path = root / "market_data_snapshot_v1" / symbol / f"{day_utc[:4]}.jsonl"
    rows = _read_jsonl(path)
    rows = [row for row in rows if str(row.get("symbol") or "").upper() == symbol]
    rows.sort(key=lambda row: str(row.get("timestamp_utc") or ""))
    return path, rows


def _derive_instrument(data: dict[str, Any], *, symbol: str, rows: list[dict[str, Any]], day_utc: str) -> None:
    prefix = symbol.lower()
    target_index = next((idx for idx, row in enumerate(rows) if _day(row.get("timestamp_utc")) == day_utc), None)
    if target_index is None:
        return
    target = rows[target_index]
    previous = rows[target_index - 1] if target_index > 0 else {}
    data[f"{prefix}_price"] = target.get("close", "")
    data[f"{prefix}_prev_close"] = previous.get("close", "")
    data[f"{prefix}_return_pct"] = _pct(target.get("close"), previous.get("close"))


def _derive_trend(data: dict[str, Any], *, rows: list[dict[str, Any]], day_utc: str, prefix: str) -> None:
    target_index = next((idx for idx, row in enumerate(rows) if _day(row.get("timestamp_utc")) == day_utc), None)
    if target_index is None:
        return
    target = rows[target_index]
    history = rows[: target_index + 1]
    close = target.get("close")
    if len(history) > 20:
        data[f"{prefix}_20d_return_pct"] = _pct(close, history[-21].get("close"))
    if len(history) > 50:
        data[f"{prefix}_50d_return_pct"] = _pct(close, history[-51].get("close"))
    sma20 = _sma(history, 20)
    sma50 = _sma(history, 50)
    try:
        close_f = float(close)
    except (TypeError, ValueError):
        close_f = None
    if close_f is not None and sma20 is not None:
        data[f"{prefix}_above_20dma"] = close_f > sma20
    if close_f is not None and sma50 is not None:
        data[f"{prefix}_above_50dma"] = close_f > sma50


def _calendar_context(truth_root: Path, day_utc: str) -> dict[str, str]:
    path = truth_root / "market_calendar_v1" / "NYSE" / f"{day_utc[:4]}.jsonl"
    for row in _read_jsonl(path):
        row_day = str(row.get("date") or row.get("day_utc") or "")[:10]
        if row_day != day_utc:
            continue
        if row.get("is_trading_session") is True:
            return {"trading_day_type": "TRADING_DAY", "market_open_status": "OPEN"}
        if row.get("is_trading_session") is False:
            return {"trading_day_type": "NON_TRADING_DAY", "market_open_status": "CLOSED"}
    return {}


def _derive_market_data_from_truth(*, truth_root: Path, day_utc: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    data: dict[str, Any] = {
        "snapshot_id": f"event_market_snapshot:{day_utc}:canonical_market_data_snapshot_v1",
        "data_snapshot_refs": [],
        "source_lineage": [],
        **_calendar_context(truth_root, day_utc),
    }
    lineage: list[dict[str, Any]] = []
    manifest_path = truth_root / "market_data_snapshot_v1" / "dataset_manifest.json"
    manifest = _read_json_path(manifest_path) if manifest_path.exists() else {}
    latest_source_ts = str(manifest.get("source_snapshot_utc") or "")
    if manifest_path.exists():
        lineage.append({"artifact_type": "market_data_snapshot_v1:dataset_manifest", "path": str(manifest_path)})
        data["data_snapshot_refs"].append(str(manifest_path))
    for symbol in ("SPY", "QQQ", "VIX"):
        path, rows = _symbol_rows(truth_root, symbol, day_utc)
        if path.exists():
            lineage.append({"artifact_type": f"market_data_snapshot_v1:{symbol}", "path": str(path)})
            data["data_snapshot_refs"].append(str(path))
        if rows:
            latest_source_ts = max([latest_source_ts, *[str(row.get("ingested_utc") or "") for row in rows if row.get("ingested_utc")]])
        _derive_instrument(data, symbol=symbol, rows=rows, day_utc=day_utc)
        if symbol in {"SPY", "QQQ"}:
            _derive_trend(data, rows=rows, day_utc=day_utc, prefix=symbol.lower())
    if latest_source_ts:
        data["source_timestamp_utc"] = latest_source_ts
        data["latest_source_timestamp_utc"] = latest_source_ts
    data["source_lineage"] = lineage
    return data, lineage


def _market_data_report_context(*, truth_root: Path, day_utc: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    path, payload = latest_json_v1(truth_root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    if not path or not payload:
        return {}, []
    registry_path, registry_payload = latest_json_v1(truth_root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    derived_data, derived_lineage = _derive_market_data_from_truth(truth_root=truth_root, day_utc=day_utc)
    data: dict[str, Any] = {
        "snapshot_id": f"event_market_snapshot:{day_utc}:aegis_market_data_v1",
        "day_utc": day_utc,
        "market_data_status": payload.get("status") or "UNKNOWN",
        "market_data_mode": payload.get("market_data_mode") or payload.get("mode") or "",
        "status": payload.get("status") or "UNKNOWN",
        "missing_fields": payload.get("missing_fields") if isinstance(payload.get("missing_fields"), list) else [],
        "stale_fields": payload.get("stale_fields") if isinstance(payload.get("stale_fields"), list) else [],
        "usable_for_candidate_generation": bool(payload.get("usable_for_candidate_generation")),
        "provider_statuses": payload.get("provider_results") if isinstance(payload.get("provider_results"), list) else [],
        "source_hashes": {"aegis_market_data_v1": _sha256(path), **({"aegis_data_registry_v1": _sha256(registry_path)} if registry_path else {})},
        "data_snapshot_refs": [str(path)],
        "source_timestamp_utc": payload.get("generated_at_utc") or "",
        "latest_source_timestamp_utc": payload.get("generated_at_utc") or "",
        **_calendar_context(truth_root, day_utc),
    }
    symbol_source_timestamps: list[str] = []
    for symbol, row in (payload.get("symbols") or {}).items():
        if not isinstance(row, dict):
            continue
        prefix = str(symbol).lower()
        data[prefix] = {
            "price": row.get("last_price") or row.get("close") or derived_data.get(f"{prefix}_price") or "",
            "prev_close": row.get("prev_close") or derived_data.get(f"{prefix}_prev_close") or "",
            "return_pct": row.get("return_pct") or derived_data.get(f"{prefix}_return_pct") or "",
        }
        source_ts = str(row.get("source_timestamp_utc") or row.get("data_timestamp_utc") or "")
        if source_ts:
            symbol_source_timestamps.append(source_ts)
    if symbol_source_timestamps and str(data.get("market_data_mode") or "").strip().upper() == "FINAL_EOD_CERTIFIED":
        data["source_timestamp_utc"] = max(symbol_source_timestamps)
        data["latest_source_timestamp_utc"] = max(symbol_source_timestamps)
    for key in (
        "spy_20d_return_pct",
        "spy_50d_return_pct",
        "qqq_20d_return_pct",
        "qqq_50d_return_pct",
        "spy_above_20dma",
        "spy_above_50dma",
        "qqq_above_20dma",
        "qqq_above_50dma",
    ):
        if data.get(key) in (None, "") and key in derived_data:
            data[key] = derived_data[key]
    for ref in derived_data.get("data_snapshot_refs") or []:
        if ref not in data["data_snapshot_refs"]:
            data["data_snapshot_refs"].append(ref)
    data["symbols"] = payload.get("symbols") if isinstance(payload.get("symbols"), dict) else {}
    data["per_symbol_status"] = {
        str(symbol): {
            "freshness_status": row.get("freshness_status"),
            "market_session_date": row.get("market_session_date"),
            "provider": row.get("provider") or row.get("source"),
            "data_timestamp_utc": row.get("data_timestamp_utc"),
        }
        for symbol, row in (payload.get("symbols") or {}).items()
        if isinstance(row, dict)
    }
    breadth = payload.get("breadth") if isinstance(payload.get("breadth"), dict) else {}
    data["breadth"] = {
        "advance_decline_delta": breadth.get("advance_decline_delta") or "",
        "breadth_down_pct": breadth.get("breadth_down_pct") or "",
    }
    data["breadth_status"] = {
        "freshness_status": breadth.get("freshness_status") or "MISSING",
        "source": breadth.get("source"),
        "market_session_date": breadth.get("market_session_date"),
        "data_timestamp_utc": breadth.get("data_timestamp_utc"),
    }
    lineage = [{"artifact_type": "aegis_market_data_v1", "path": str(path)}]
    for row in derived_lineage:
        if isinstance(row, dict) and row not in lineage:
            lineage.append(row)
    if registry_path:
        lineage.append({"artifact_type": "aegis_data_registry_v1", "path": str(registry_path)})
        data["global_context_status"] = _registry_global_context_status(registry_payload)
    data["source_lineage"] = lineage
    return data, lineage


def _registry_global_context_status(registry_payload: dict[str, Any]) -> dict[str, Any]:
    items = registry_payload.get("data_items") if isinstance(registry_payload.get("data_items"), list) else []
    return {
        str(item.get("data_item_id")): {
            "status": item.get("status"),
            "provider": item.get("provider"),
            "data_timestamp_utc": item.get("data_timestamp_utc"),
        }
        for item in items
        if isinstance(item, dict) and str(item.get("data_item_id") or "").startswith(("market.price.", "market.volatility.", "market.breadth."))
    }


def _sha256(path: Path) -> str:
    try:
        import hashlib

        return hashlib.sha256(path.read_bytes()).hexdigest()
    except Exception:
        return ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_event_market_snapshot_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--market_data_json", default="")
    parser.add_argument("--macro_calendar_json", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = args.day_utc or _today_utc()
    generated_at = args.generated_at_utc or now_utc_v1()
    derived_lineage: list[dict[str, Any]] = []
    market_data = _read_json(args.market_data_json) if args.market_data_json else {}
    if not args.market_data_json:
        demand_payload = build_market_context_demand_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at)
        write_market_context_demand_v1(truth_root=truth_root, day_utc=day_utc, payload=demand_payload)
        market_data = market_context_snapshot_inputs_v1(demand_payload, truth_root=truth_root, day_utc=day_utc)
        derived_lineage = market_data.get("source_lineage") if isinstance(market_data.get("source_lineage"), list) else []
        if not market_data:
            market_data, derived_lineage = _market_data_report_context(truth_root=truth_root, day_utc=day_utc)
        if not market_data:
            market_data, derived_lineage = _derive_market_data_from_truth(truth_root=truth_root, day_utc=day_utc)
    if not isinstance(market_data, dict):
        raise SystemExit("market_data_json must contain a JSON object")
    macro_calendar = _read_json(args.macro_calendar_json) if args.macro_calendar_json else market_data.get("macro_events")
    lineage = []
    if args.market_data_json:
        lineage.append({"artifact_type": "market_data_json", "path": str(Path(args.market_data_json).expanduser().resolve())})
    if args.macro_calendar_json:
        lineage.append({"artifact_type": "macro_calendar_json", "path": str(Path(args.macro_calendar_json).expanduser().resolve())})
    lineage.extend(derived_lineage)
    source_paths = [Path(str(row.get("path"))) for row in lineage if isinstance(row, dict) and str(row.get("path") or "")]
    source_hashes = {str(path): sha256_file_v1(path) for path in source_paths if path.exists() and path.is_file()}
    if source_hashes:
        market_data["source_hashes"] = {**(market_data.get("source_hashes") if isinstance(market_data.get("source_hashes"), dict) else {}), **source_hashes}

    snapshot = build_event_market_snapshot_v1(
        day_utc=day_utc,
        generated_at_utc=generated_at,
        market_data=market_data,
        macro_calendar=macro_calendar,
        source_lineage=lineage,
    )
    validate_event_market_snapshot_v1(snapshot)
    path = write_event_market_snapshot_v1(truth_root=truth_root, payload=snapshot)
    event_results = emit_artifact_evidence_transaction_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        artifact_path=path,
        payload=snapshot,
        producer_id="ops/tools/build_event_market_snapshot_v1.py",
        producer_version="v1",
        run_id=f"build_event_market_snapshot_v1:{day_utc}:{generated_at}",
        created_at_utc=generated_at,
        input_hashes=contract_input_hashes_for_paths_v1(
            source_paths,
            extra={
                "raw_source_hash": "|".join(source_hashes[key] for key in sorted(source_hashes)),
                "external_source_vendor": "canonical_truth_market_data",
                "retrieval_timestamp_utc": str(snapshot.get("generated_at_utc") or generated_at),
            },
        ),
        validation_status="VALID" if source_hashes else "UNAVAILABLE_EXTERNAL_SOURCE",
    )
    print(
        json.dumps(
            {
                "path": str(path),
                "event_ids": [str(row.get("event", {}).get("event_id") or "") for row in event_results],
                "day_utc": day_utc,
                "regime_label": snapshot["regime_label"],
                "volatility_classification": snapshot["volatility_classification"],
                "breadth_classification": snapshot["breadth_classification"],
                "macro_event_risk_level": snapshot["macro_event_risk_level"],
                "stale_data_status": snapshot["stale_data_status"],
                "broker_submit_required": False,
                "manual_execution_only": True,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
