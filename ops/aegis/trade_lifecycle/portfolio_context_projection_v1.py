from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from ops.aegis.trade_lifecycle.exit_review_projection_v1 import build_exit_review_projection_v1
from ops.aegis.trade_lifecycle.thesis_lifecycle_v1 import build_thesis_state_projection_v1, thesis_state_projection_path_v1
from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import (
    build_paper_trade_evaluation_projection_v1,
    build_trade_lifecycle_ledger_v1,
    paper_trade_evaluation_projection_path_v1,
    trade_lifecycle_ledger_path_v1,
)

SCHEMA_ID = "portfolio_context_projection"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "portfolio_context_projection_v1"
REPO_ROOT = Path(__file__).resolve().parents[3]
SLEEVE_REGISTRY_PATH = REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"

SAFETY_FLAGS = {
    "read_only": True,
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "sleeve_score_mutation_allowed": False,
    "candidate_promotion_allowed": False,
    "position_sizing_allowed": False,
    "scheduler_mutation_allowed": False,
}

REGIME_CATEGORIES = [
    "mean_reversion",
    "momentum_trend",
    "event_dislocation",
    "volatility_compression",
    "defensive_tail_protection",
    "macro_rates_regime",
]

FACTOR_CATEGORIES = [
    "value",
    "momentum",
    "quality",
    "size",
    "volatility",
    "cyclicals_defensives",
    "rates_sensitivity",
    "dollar_commodity_sensitivity",
]


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _upper(value: Any, default: str = "UNKNOWN") -> str:
    return _text(value, default).upper()


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _load_sleeve_registry(path: Path = SLEEVE_REGISTRY_PATH) -> dict[str, dict[str, Any]]:
    payload = _read_json(path)
    rows = payload.get("sleeves") if isinstance(payload.get("sleeves"), list) else []
    return {str(row.get("sleeve_id") or "").strip().upper(): dict(row) for row in rows if isinstance(row, Mapping) and row.get("sleeve_id")}


def _candidate_market_metadata(root: Path, day_utc: str) -> dict[str, dict[str, Any]]:
    path = root / "reports" / "final_eod_market_data_v1" / str(day_utc) / "final_eod_market_data.v1.json"
    pointer = _read_json(path)
    current = _text(pointer.get("current_artifact_path"))
    payload = _read_json(Path(current).expanduser()) if current else pointer
    candidates: list[Any] = []
    for key in ("normalized_records", "rows", "records", "symbol_rows", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    symbols = payload.get("symbols")
    if isinstance(symbols, dict):
        candidates.extend(symbols.values())
    out: dict[str, dict[str, Any]] = {}
    for item in candidates:
        if not isinstance(item, Mapping):
            continue
        symbol = _upper(item.get("canonical_symbol") or item.get("symbol") or item.get("ticker"), "")
        if not symbol:
            continue
        out[symbol] = {
            "symbol": symbol,
            "sector": _text(item.get("sector") or item.get("gics_sector") or item.get("market_sector")),
            "asset_class": _text(item.get("asset_class") or item.get("instrument_asset_class")),
            "factor_tags": _list_text(item.get("factor_tags") or item.get("factors")),
            "regime_dependency": _text(item.get("regime_dependency") or item.get("regime")),
            "source_artifact": str(Path(current).expanduser() if current else path),
            "content_hash": _text(payload.get("content_hash") or _sha256(Path(current).expanduser() if current else path)),
        }
    return out


def _list_text(value: Any) -> list[str]:
    if isinstance(value, str):
        if not value.strip():
            return []
        return [part.strip() for part in value.replace(";", ",").split(",") if part.strip()]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _setup_type_for(trade: Mapping[str, Any], sleeve: Mapping[str, Any]) -> str:
    explicit = _text(trade.get("setup_type") or trade.get("strategy_setup") or sleeve.get("setup_type") or sleeve.get("sleeve_type"))
    if explicit:
        return explicit.upper()
    text = " ".join([_text(trade.get("sleeve_id")), _text(trade.get("hypothesis_id")), _text(sleeve.get("display_name"))]).upper()
    if "MEAN" in text and "REVERSION" in text:
        return "MEAN_REVERSION"
    if "MOMENTUM" in text or "TREND" in text:
        return "MOMENTUM_TREND"
    if "EVENT" in text or "DISLOCATION" in text:
        return "EVENT_DISLOCATION"
    if "VOL" in text:
        return "VOLATILITY_COMPRESSION"
    if "BOND" in text or "RATE" in text or "FIXED_INCOME" in text:
        return "MACRO_RATES"
    return "UNKNOWN"


def _regime_categories_for(trade: Mapping[str, Any], setup_type: str, sleeve: Mapping[str, Any], market: Mapping[str, Any]) -> list[str]:
    explicit = [_normalize_regime(item) for item in _list_text(trade.get("regime_dependency") or trade.get("regime_dependencies") or market.get("regime_dependency") or sleeve.get("regime_dependency"))]
    categories = [item for item in explicit if item]
    setup = setup_type.upper()
    sleeve_text = " ".join([_text(trade.get("sleeve_id")), _text(sleeve.get("asset_class")), _text(sleeve.get("sleeve_type"))]).upper()
    if setup == "MEAN_REVERSION":
        categories.append("mean_reversion")
    if setup in {"MOMENTUM", "MOMENTUM_TREND", "TREND"}:
        categories.append("momentum_trend")
    if setup == "EVENT_DISLOCATION":
        categories.append("event_dislocation")
    if setup == "VOLATILITY_COMPRESSION":
        categories.append("volatility_compression")
    if "TAIL" in setup or "DEFENSIVE" in setup:
        categories.append("defensive_tail_protection")
    if setup == "MACRO_RATES" or "BOND" in sleeve_text or "FIXED_INCOME" in sleeve_text or "RATE" in setup:
        categories.append("macro_rates_regime")
    return sorted(set([item for item in categories if item in REGIME_CATEGORIES]))


def _normalize_regime(value: Any) -> str:
    text = _text(value).lower().replace("-", "_").replace("/", "_").replace(" ", "_")
    aliases = {
        "mean_reversion": "mean_reversion",
        "momentum": "momentum_trend",
        "trend": "momentum_trend",
        "momentum_trend": "momentum_trend",
        "event": "event_dislocation",
        "event_dislocation": "event_dislocation",
        "volatility": "volatility_compression",
        "volatility_compression": "volatility_compression",
        "defensive": "defensive_tail_protection",
        "tail_protection": "defensive_tail_protection",
        "defensive_tail_protection": "defensive_tail_protection",
        "macro": "macro_rates_regime",
        "rates": "macro_rates_regime",
        "macro_rates": "macro_rates_regime",
        "macro_rates_regime": "macro_rates_regime",
    }
    return aliases.get(text, "")


def _factor_categories_for(trade: Mapping[str, Any], sleeve: Mapping[str, Any], market: Mapping[str, Any]) -> list[str]:
    raw = []
    raw.extend(_list_text(trade.get("factor_tags") or trade.get("factors") or trade.get("factor_exposure")))
    raw.extend(_list_text(market.get("factor_tags") or market.get("factors")))
    raw.extend(_list_text(sleeve.get("factor_tags") or sleeve.get("factors")))
    categories = [_normalize_factor(item) for item in raw]
    text = " ".join([_text(trade.get("sleeve_id")), _text(sleeve.get("asset_class")), _text(sleeve.get("sleeve_type"))]).upper()
    if "BOND" in text or "FIXED_INCOME" in text:
        categories.append("rates_sensitivity")
    return sorted(set([item for item in categories if item in FACTOR_CATEGORIES]))


def _normalize_factor(value: Any) -> str:
    text = _text(value).lower().replace("-", "_").replace("/", "_").replace(" ", "_")
    aliases = {
        "value": "value",
        "momentum": "momentum",
        "quality": "quality",
        "size": "size",
        "small_size": "size",
        "vol": "volatility",
        "volatility": "volatility",
        "cyclical": "cyclicals_defensives",
        "cyclicals": "cyclicals_defensives",
        "defensive": "cyclicals_defensives",
        "defensives": "cyclicals_defensives",
        "cyclicals_defensives": "cyclicals_defensives",
        "rates": "rates_sensitivity",
        "rate": "rates_sensitivity",
        "duration": "rates_sensitivity",
        "rates_sensitivity": "rates_sensitivity",
        "dollar": "dollar_commodity_sensitivity",
        "commodity": "dollar_commodity_sensitivity",
        "commodities": "dollar_commodity_sensitivity",
        "dollar_commodity_sensitivity": "dollar_commodity_sensitivity",
    }
    return aliases.get(text, "")


def _asset_class_for(trade: Mapping[str, Any], sleeve: Mapping[str, Any], market: Mapping[str, Any]) -> str:
    explicit = _text(trade.get("asset_class") or market.get("asset_class") or sleeve.get("asset_class"))
    if explicit:
        return explicit.upper()
    symbol = _upper(trade.get("symbol"), "")
    if symbol in {"TLT", "IEF", "SHY", "BND", "AGG", "LQD", "HYG", "BIL", "TIP", "VGSH", "JMST", "SUB"}:
        return "FIXED_INCOME"
    return "EQUITY"


def _sector_for(trade: Mapping[str, Any], market: Mapping[str, Any]) -> str:
    return _text(trade.get("sector") or trade.get("gics_sector") or trade.get("market_sector") or market.get("sector"), "UNKNOWN").upper()


def _direction_for(trade: Mapping[str, Any]) -> str:
    side = _upper(trade.get("side") or trade.get("direction"), "LONG")
    if side in {"SELL", "SHORT"}:
        return "SHORT"
    return "LONG"


def _trade_notional(trade: Mapping[str, Any]) -> float:
    qty = abs(_number(trade.get("quantity")) or 0.0)
    mark = _number(trade.get("current_mark"))
    entry = _number(trade.get("entry_price"))
    price = mark if mark is not None else entry if entry is not None else 0.0
    return round(qty * abs(price), 6)


def _normalize_trade_row(trade: Mapping[str, Any], sleeve_registry: Mapping[str, dict[str, Any]], market_metadata: Mapping[str, dict[str, Any]], thesis_by_trade: Mapping[str, dict[str, Any]]) -> dict[str, Any]:
    sleeve_id = _upper(trade.get("sleeve_id"), "UNATTRIBUTED")
    symbol = _upper(trade.get("symbol"), "UNKNOWN")
    sleeve = sleeve_registry.get(sleeve_id, {})
    market = market_metadata.get(symbol, {})
    setup_type = _setup_type_for(trade, sleeve)
    factors = _factor_categories_for(trade, sleeve, market)
    regimes = _regime_categories_for(trade, setup_type, sleeve, market)
    sector = _sector_for(trade, market)
    thesis = thesis_by_trade.get(_text(trade.get("trade_id")), {})
    return {
        "trade_id": _text(trade.get("trade_id")),
        "position_id": _text(trade.get("position_id"), _text(trade.get("trade_id")).replace(":", "_")),
        "symbol": symbol,
        "sector": sector,
        "sleeve_id": sleeve_id,
        "setup_type": setup_type,
        "direction": _direction_for(trade),
        "asset_class": _asset_class_for(trade, sleeve, market),
        "quantity": trade.get("quantity"),
        "notional_exposure": _trade_notional(trade),
        "unrealized_pnl": round(_number(trade.get("unrealized_pnl")) or 0.0, 6),
        "factor_categories": factors,
        "regime_categories": regimes,
        "risk_drivers": sorted(set([sector, setup_type, *factors, *regimes]) - {"UNKNOWN", ""}),
        "current_thesis_state": _text(thesis.get("current_thesis_state"), "INCONCLUSIVE"),
        "source_artifacts": sorted(set([str(item) for item in trade.get("source_artifacts") or [] if str(item)] + [_text(market.get("source_artifact"))] ) - {""}),
    }


def _rollup(rows: Sequence[Mapping[str, Any]], key: str) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for row in rows:
        value = _text(row.get(key), "UNKNOWN")
        bucket = buckets.setdefault(value, {key: value, "open_trade_count": 0, "notional_exposure": 0.0, "unrealized_pnl_contribution": 0.0, "trade_ids": []})
        bucket["open_trade_count"] += 1
        bucket["notional_exposure"] += _number(row.get("notional_exposure")) or 0.0
        bucket["unrealized_pnl_contribution"] += _number(row.get("unrealized_pnl")) or 0.0
        bucket["trade_ids"].append(_text(row.get("trade_id")))
    out = []
    total_notional = sum(_number(row.get("notional_exposure")) or 0.0 for row in rows) or 0.0
    for bucket in buckets.values():
        bucket["notional_exposure"] = round(bucket["notional_exposure"], 6)
        bucket["unrealized_pnl_contribution"] = round(bucket["unrealized_pnl_contribution"], 6)
        bucket["exposure_pct"] = round(bucket["notional_exposure"] / total_notional, 6) if total_notional else 0.0
        bucket["trade_ids"] = sorted(set(bucket["trade_ids"]))
        out.append(bucket)
    return sorted(out, key=lambda item: (-float(item.get("notional_exposure") or 0.0), str(item.get(key) or "")))


def _expanded_rollup(rows: Sequence[Mapping[str, Any]], key: str, values_key: str) -> list[dict[str, Any]]:
    expanded = []
    for row in rows:
        values = row.get(values_key) if isinstance(row.get(values_key), list) else []
        for value in values:
            expanded.append({**dict(row), key: value})
    return _rollup(expanded, key)


def _concentration_warnings(rows: Sequence[Mapping[str, Any]], rollups: Mapping[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    thresholds = {
        "symbol": ("by_symbol", "EXCESSIVE_SYMBOL_EXPOSURE", 2, 0.50),
        "sector": ("by_sector", "EXCESSIVE_SECTOR_EXPOSURE", 3, 0.60),
        "sleeve_id": ("by_sleeve", "EXCESSIVE_SLEEVE_EXPOSURE", 3, 0.75),
        "setup_type": ("by_setup_type", "EXCESSIVE_SETUP_TYPE_EXPOSURE", 3, 0.75),
    }
    for field, (rollup_key, warning_type, count_threshold, pct_threshold) in thresholds.items():
        for bucket in rollups.get(rollup_key, []):
            count = int(bucket.get("open_trade_count") or 0)
            pct = float(bucket.get("exposure_pct") or 0.0)
            if count >= count_threshold or pct >= pct_threshold:
                warnings.append({
                    "warning_type": warning_type,
                    "dimension": field,
                    "value": bucket.get(field),
                    "severity": "INFO",
                    "message": f"Informational concentration: {bucket.get(field)} represents {count} open trade(s) and {round(pct * 100, 2)}% of open notional context.",
                    "affected_trade_ids": bucket.get("trade_ids") or [],
                    "advisory_only": True,
                    **SAFETY_FLAGS,
                })
    risk_driver_buckets: dict[str, list[str]] = {}
    for row in rows:
        for driver in row.get("risk_drivers") or []:
            risk_driver_buckets.setdefault(str(driver), []).append(_text(row.get("trade_id")))
    for driver, trade_ids in sorted(risk_driver_buckets.items()):
        unique = sorted(set(trade_ids))
        if len(unique) >= 2:
            warnings.append({
                "warning_type": "CLUSTERED_RISK_DRIVER_EXPOSURE",
                "dimension": "risk_driver",
                "value": driver,
                "severity": "INFO",
                "message": f"Informational clustered risk driver: {driver} appears across {len(unique)} open trades.",
                "affected_trade_ids": unique,
                "advisory_only": True,
                **SAFETY_FLAGS,
            })
    return warnings


def _overlap_type(rows: Sequence[Mapping[str, Any]]) -> str:
    directions = {str(row.get("direction") or "") for row in rows}
    symbols = {str(row.get("symbol") or "") for row in rows}
    if len(directions) > 1:
        return "conflicting_overlap"
    if len(symbols) == 1:
        return "duplicate-risk_overlap"
    return "confirming_overlap"


def _sleeve_overlap(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[tuple[str, str, list[Mapping[str, Any]]]] = []
    for dimension in ("symbol", "sector"):
        buckets: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            value = _text(row.get(dimension), "UNKNOWN")
            if value != "UNKNOWN":
                buckets.setdefault(value, []).append(row)
        candidates.extend((dimension, value, bucket) for value, bucket in buckets.items())
    for dimension, values_key in (("factor", "factor_categories"), ("regime_dependency", "regime_categories")):
        buckets: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            for value in row.get(values_key) or []:
                buckets.setdefault(str(value), []).append(row)
        candidates.extend((dimension, value, bucket) for value, bucket in buckets.items())
    overlaps = []
    seen = set()
    for dimension, value, bucket in candidates:
        sleeves = sorted(set(_text(row.get("sleeve_id"), "UNATTRIBUTED") for row in bucket))
        if len(sleeves) < 2:
            continue
        trade_ids = sorted(set(_text(row.get("trade_id")) for row in bucket))
        key = (dimension, value, tuple(sleeves), tuple(trade_ids))
        if key in seen:
            continue
        seen.add(key)
        overlap_type = _overlap_type(bucket)
        overlaps.append({
            "overlap_type": overlap_type,
            "dimension": dimension,
            "value": value,
            "sleeve_ids": sleeves,
            "affected_trade_ids": trade_ids,
            "message": f"{overlap_type.replace('_', ' ')} on {dimension}={value} across sleeves {', '.join(sleeves)}.",
            "advisory_only": True,
            **SAFETY_FLAGS,
        })
    return sorted(overlaps, key=lambda row: (str(row.get("dimension")), str(row.get("value")), str(row.get("overlap_type"))))


def _context_rollup(rows: Sequence[Mapping[str, Any]], categories: Sequence[str], values_key: str, label_key: str) -> list[dict[str, Any]]:
    out = []
    for category in categories:
        bucket_rows = [row for row in rows if category in (row.get(values_key) or [])]
        trade_ids = sorted(set(_text(row.get("trade_id")) for row in bucket_rows))
        notional = round(sum(_number(row.get("notional_exposure")) or 0.0 for row in bucket_rows), 6)
        pnl = round(sum(_number(row.get("unrealized_pnl")) or 0.0 for row in bucket_rows), 6)
        out.append({
            label_key: category,
            "open_trade_count": len(bucket_rows),
            "notional_exposure": notional,
            "unrealized_pnl_contribution": pnl,
            "affected_trade_ids": trade_ids,
        })
    return out


def _evidence_links(root: Path, day_utc: str, trade_projection: Mapping[str, Any], exit_review_projection: Mapping[str, Any], thesis_projection: Mapping[str, Any]) -> list[dict[str, str]]:
    links = [
        {
            "logical_name": "paper_trade_evaluation_projection_v1",
            "artifact_path": str(paper_trade_evaluation_projection_path_v1(truth_root=root, day_utc=day_utc)),
            "artifact_sha256": _text(trade_projection.get("content_hash")),
        },
        {
            "logical_name": "exit_review_projection_v1",
            "artifact_path": str(root / "reports" / "exit_review_projection_v1" / str(day_utc) / "exit_review_projection.v1.json"),
            "artifact_sha256": _text(exit_review_projection.get("content_hash")),
        },
        {
            "logical_name": "trade_lifecycle_ledger_v1",
            "artifact_path": str(trade_lifecycle_ledger_path_v1(truth_root=root, day_utc=day_utc)),
            "artifact_sha256": _text((trade_projection.get("trade_lifecycle_ledger") or {}).get("content_hash")),
        },
        {
            "logical_name": "thesis_state_projection_v1",
            "artifact_path": str(thesis_state_projection_path_v1(truth_root=root, day_utc=day_utc)),
            "artifact_sha256": _text(thesis_projection.get("content_hash")),
        },
        {
            "logical_name": "c2_sleeve_registry_v1",
            "artifact_path": str(SLEEVE_REGISTRY_PATH),
            "artifact_sha256": _sha256(SLEEVE_REGISTRY_PATH),
        },
    ]
    return links


def build_portfolio_context_projection_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    paper_trade_evaluation_projection: Mapping[str, Any] | None = None,
    exit_review_projection: Mapping[str, Any] | None = None,
    thesis_state_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    trade_projection = dict(paper_trade_evaluation_projection or build_paper_trade_evaluation_projection_v1(truth_root=root, day_utc=day_utc))
    exit_projection = dict(exit_review_projection or build_exit_review_projection_v1(truth_root=root, day_utc=day_utc))
    thesis_projection = dict(thesis_state_projection or exit_projection.get("thesis_state_projection_v1") or build_thesis_state_projection_v1(truth_root=root, day_utc=day_utc, trades=[row for row in trade_projection.get("all_trades") or [] if isinstance(row, Mapping)]))
    sleeve_registry = _load_sleeve_registry()
    market_metadata = _candidate_market_metadata(root, str(day_utc))
    thesis_by_trade = {str(row.get("trade_id") or ""): dict(row) for row in thesis_projection.get("rows") or [] if isinstance(row, Mapping)}
    open_trades = [row for row in trade_projection.get("open_trades") or [] if isinstance(row, Mapping)]
    affected_trades = [_normalize_trade_row(row, sleeve_registry, market_metadata, thesis_by_trade) for row in open_trades]
    rollups = {
        "by_symbol": _rollup(affected_trades, "symbol"),
        "by_sector": _rollup(affected_trades, "sector"),
        "by_sleeve": _rollup(affected_trades, "sleeve_id"),
        "by_setup_type": _rollup(affected_trades, "setup_type"),
        "by_direction": _rollup(affected_trades, "direction"),
        "by_asset_class": _rollup(affected_trades, "asset_class"),
    }
    sector_missing = [row.get("trade_id") for row in affected_trades if row.get("sector") == "UNKNOWN"]
    factor_missing = [row.get("trade_id") for row in affected_trades if not row.get("factor_categories")]
    regime_missing = [row.get("trade_id") for row in affected_trades if not row.get("regime_categories")]
    missing_data = []
    if sector_missing:
        missing_data.append({"field": "sector", "status": "PARTIAL", "affected_trade_ids": sorted(set(sector_missing)), "message": "Sector metadata unavailable for some open trades."})
    if factor_missing:
        missing_data.append({"field": "factor", "status": "PARTIAL", "affected_trade_ids": sorted(set(factor_missing)), "message": "Factor metadata unavailable for some open trades."})
    if regime_missing:
        missing_data.append({"field": "regime_dependency", "status": "PARTIAL", "affected_trade_ids": sorted(set(regime_missing)), "message": "Regime dependency metadata unavailable for some open trades."})
    confidence = "HIGH" if not missing_data else "PARTIAL"
    exposure_summary = {
        "open_trade_count": len(affected_trades),
        "total_notional_exposure": round(sum(_number(row.get("notional_exposure")) or 0.0 for row in affected_trades), 6),
        "total_unrealized_pnl": round(sum(_number(row.get("unrealized_pnl")) or 0.0 for row in affected_trades), 6),
        **rollups,
    }
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": str(day_utc),
        "generated_at_utc": utc_now_v1(),
        "exposure_summary": exposure_summary,
        "concentration_warnings": _concentration_warnings(affected_trades, rollups),
        "sleeve_overlap": _sleeve_overlap(affected_trades),
        "regime_concentration": _context_rollup(affected_trades, REGIME_CATEGORIES, "regime_categories", "regime_category"),
        "factor_exposure": _context_rollup(affected_trades, FACTOR_CATEGORIES, "factor_categories", "factor_category"),
        "affected_trades": sorted(affected_trades, key=lambda row: (str(row.get("symbol")), str(row.get("trade_id")))),
        "evidence_links": _evidence_links(root, str(day_utc), trade_projection, exit_projection, thesis_projection),
        "confidence": confidence,
        "missing_data": missing_data,
        "advisory_only": True,
        "context_only": True,
        **SAFETY_FLAGS,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def portfolio_context_projection_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / "portfolio_context_projection.v1.json"


def write_portfolio_context_projection_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> dict[str, str]:
    path = portfolio_context_projection_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    return {"portfolio_context_projection": str(path), "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload))}
