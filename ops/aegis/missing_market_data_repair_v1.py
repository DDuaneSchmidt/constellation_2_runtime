from __future__ import annotations

import csv
import json
import re
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.dormant_sleeve_signal_generation_diagnostics_v1 import (
    build_dormant_sleeve_signal_generation_diagnostics_v1,
)
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.missing_market_data_requirement_resolver_v1 import (
    build_missing_market_data_requirement_resolver_v1,
)
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1
from ops.aegis.sleeve_throughput_diagnostics_v1 import build_sleeve_throughput_diagnostics_v1

FAMILY = "aegis_missing_market_data_repair_v1"
FILENAME = "missing_market_data_repair.v1.json"
POLICY_VERSION = "AEGIS_MISSING_MARKET_DATA_REPAIR_V1"

TARGET_SLEEVES = {
    "C2_DEFENSIVE_TAIL_V1": {
        "sleeve_name": "Defensive Tail",
        "symbol": "TLT",
        "repair_type": "ROUTING_REPAIR",
    },
    "C2_EVENT_DISLOCATION_V1": {
        "sleeve_name": "Event Dislocation",
        "symbol": "GLD",
        "repair_type": "INGESTION_COMPLETENESS_REPAIR",
    },
}

SAFETY = {
    "read_only": False,
    "diagnostics_only": False,
    "repair_scope": "market_data_routing_and_completeness_only",
    "no_sleeve_logic_mutation": True,
    "no_strategy_logic_mutation": True,
    "no_threshold_mutation": True,
    "no_candidate_scoring_mutation": True,
    "no_quality_mutation": True,
    "no_allocation_mutation": True,
    "no_signal_fabrication": True,
    "no_candidate_fabrication": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def missing_market_data_repair_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_missing_market_data_repair_v1(
    *, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    computed_at = computed_at_utc or f"{day}T00:00:00Z"
    prior = read_json_v1(missing_market_data_repair_path_v1(truth_root=root, day_utc=day))
    before_t03 = _read_t03(root, day)
    original_by_sleeve = _original_t03_by_sleeve(prior, before_t03)

    repair_results = {
        "C2_DEFENSIVE_TAIL_V1": _repair_defensive_tail(root, day),
        "C2_EVENT_DISLOCATION_V1": _repair_event_dislocation(root, day),
    }
    repair_results = _preserve_prior_repair_results(prior, repair_results)

    post_payloads = _post_diagnostics(root, day, computed_at)
    rows = [
        _build_row(
            root=root,
            day=day,
            computed_at=computed_at,
            sleeve_id=sleeve_id,
            repair=repair_results[sleeve_id],
            original_t03=original_by_sleeve.get(sleeve_id, {}),
            post_payloads=post_payloads,
        )
        for sleeve_id in sorted(TARGET_SLEEVES)
    ]
    summary = _summary(rows)
    payload: dict[str, Any] = {
        "schema_id": "aegis_missing_market_data_repair",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": computed_at,
        "sleeves": rows,
        "portfolio_summary": summary,
        "summary": summary,
        "answer": _answer(rows),
        "source_artifact_paths": _source_paths(root, day),
        "source_artifact_hashes": {name: file_hash_v1(Path(path)) for name, path in _source_paths(root, day).items() if Path(path).exists()},
        "safety_statement": "T04 repairs only AEGIS-owned declarative market-data routing and governed historical-bar completeness. It does not change sleeve logic, trigger thresholds, candidate scoring, quality logic, allocation logic, broker/live trading, or fabricate signals/candidates.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1({**payload, "content_hash": ""})
    return payload


def write_missing_market_data_repair_v1(
    *, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_missing_market_data_repair_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(missing_market_data_repair_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _preserve_prior_repair_results(prior: Mapping[str, Any], current: Mapping[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {key: dict(value) for key, value in current.items()}
    for row in _rows(prior, "sleeves"):
        sleeve_id = _text(row.get("sleeve_id"))
        if sleeve_id not in out:
            continue
        original_resolution = _text(row.get("original_t03_resolution_type"))
        prior_repaired = _text(row.get("repair_status")) == "REPAIRED"
        original_required_repair = original_resolution not in {"", "NO_ACTION_REQUIRED", "UNKNOWN_DETERMINISTIC_BLOCKER"}
        if (prior_repaired or original_required_repair) and _text(out[sleeve_id].get("repair_status")) == "NO_REPAIR_REQUIRED":
            out[sleeve_id]["repair_status"] = "REPAIRED"
            prior_type = _text(row.get("repair_type"))
            target_type = _text(TARGET_SLEEVES.get(sleeve_id, {}).get("repair_type"))
            out[sleeve_id]["repair_type"] = prior_type if prior_type and prior_type != "NO_REPAIR" else target_type or _text(out[sleeve_id].get("repair_type"))
            if not out[sleeve_id].get("created_paths"):
                out[sleeve_id]["created_paths"] = _list(row.get("created_paths"))
    return out


def _repair_defensive_tail(root: Path, day: str) -> dict[str, Any]:
    sleeve_root = _sleeve_truth_root(root)
    symbol = "TLT"
    source_file = root / "market_data_snapshot_v1" / symbol / f"{day[:4]}.jsonl"
    source_row = _jsonl_day_row(source_file, day)
    required_paths = {
        "market_snapshot": sleeve_root / "market_data_snapshot_v1" / "snapshots" / day / f"{symbol}.market_data_snapshot.v1.json",
        "nav_v1": sleeve_root / "accounting_v1" / "nav" / day / "nav_snapshot.v1.json",
        "positions_v2": sleeve_root / "positions_snapshot_v2" / "snapshots" / day / "positions_snapshot.v2.json",
    }
    created: list[str] = []
    blockers: list[str] = []

    if source_row:
        path = required_paths["market_snapshot"]
        if not path.exists():
            payload = _market_snapshot_payload(symbol, day, source_file, source_row)
            _write_json_if_changed(path, payload)
            created.append(str(path))
    else:
        blockers.append(f"missing_source_row:{source_file}")

    nav_source = sleeve_root / "accounting_v2" / "nav" / day / "nav.v2.json"
    nav_payload = read_json_v1(nav_source)
    if nav_payload:
        path = required_paths["nav_v1"]
        if not path.exists():
            _write_json_if_changed(path, _nav_v1_payload(day, nav_source, nav_payload))
            created.append(str(path))
    else:
        blockers.append(f"missing_nav_source:{nav_source}")

    position_source = sleeve_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json"
    position_failure = sleeve_root / "positions_v1" / "failures" / day / "positions_snapshot.v5.failure.json"
    position_payload = read_json_v1(position_source)
    failure_payload = read_json_v1(position_failure)
    if position_payload or failure_payload or nav_payload.get("source_type") == "STATIC_RISK_BUDGET_BOOTSTRAP":
        path = required_paths["positions_v2"]
        if not path.exists():
            _write_json_if_changed(path, _positions_v2_payload(day, position_source, position_payload, position_failure, failure_payload, nav_source, nav_payload))
            created.append(str(path))
    else:
        blockers.append(f"missing_positions_source:{position_source}")

    remaining = [str(path) for path in required_paths.values() if not path.exists()]
    status = "REPAIRED" if not blockers and not remaining else "NOT_REPAIRED_RUNTIME_ERROR"
    if not created and not remaining and not blockers:
        status = "NO_REPAIR_REQUIRED"
    return {
        "repair_status": status,
        "repair_type": "ROUTING_REPAIR" if status != "NO_REPAIR_REQUIRED" else "NO_REPAIR",
        "required_market_data_items": ["market.price.TLT", "runtime.nav_snapshot", "runtime.positions_snapshot"],
        "required_fields": ["price", "snapshot_json", "history.drawdown_pct", "positions.items"],
        "source_data_found": bool(source_row and nav_payload and (position_payload or failure_payload or nav_payload.get("source_type") == "STATIC_RISK_BUDGET_BOOTSTRAP")),
        "source_data_path": str(source_file),
        "routed_data_path": ", ".join(str(path) for path in required_paths.values()),
        "source_fresh": bool(source_row and _text(source_row.get("day_utc")) == day),
        "data_quality_status": "CURRENT" if not remaining and not blockers else "INCOMPLETE",
        "remaining_blocker": "NONE" if not remaining and not blockers else "MISSING_RUNTIME_ROUTE",
        "blocker_reason": "; ".join(blockers + remaining) if blockers or remaining else "NONE",
        "source_paths": {key: str(value) for key, value in required_paths.items()},
        "created_paths": created,
    }


def _repair_event_dislocation(root: Path, day: str) -> dict[str, Any]:
    symbol = "GLD"
    year_file = root / "market_data_snapshot_v1" / symbol / f"{day[:4]}.jsonl"
    raw_csv = root / "reports" / "aegis_market_data_v1" / day / "raw" / "STOOQ" / f"{symbol}.quote.csv"
    before = _jsonl_day_row(year_file, day)
    if before:
        return {
            "repair_status": "NO_REPAIR_REQUIRED",
            "repair_type": "NO_REPAIR",
            "required_market_data_items": [f"historical_bar.{symbol}.{day}"],
            "required_fields": ["open", "high", "low", "close", "volume"],
            "source_data_found": True,
            "source_data_path": str(year_file),
            "routed_data_path": str(year_file),
            "source_fresh": True,
            "data_quality_status": "CURRENT",
            "remaining_blocker": "NONE",
            "blocker_reason": "NONE",
            "source_paths": {"historical_jsonl": str(year_file)},
            "created_paths": [],
        }
    quote = _stooq_quote_row(raw_csv, symbol, day)
    if not quote:
        return {
            "repair_status": "NOT_REPAIRED_SOURCE_TRULY_MISSING",
            "repair_type": "NO_REPAIR",
            "required_market_data_items": [f"historical_bar.{symbol}.{day}"],
            "required_fields": ["open", "high", "low", "close", "volume"],
            "source_data_found": False,
            "source_data_path": str(raw_csv),
            "routed_data_path": str(year_file),
            "source_fresh": False,
            "data_quality_status": "INCOMPLETE",
            "remaining_blocker": f"historical_bar.{symbol}.{day}",
            "blocker_reason": f"Governed raw STOOQ source does not contain {symbol} {day}.",
            "source_paths": {"raw_stooq_quote": str(raw_csv), "historical_jsonl": str(year_file)},
            "created_paths": [],
        }
    appended = _append_historical_day_bar(root, year_file, raw_csv, quote)
    return {
        "repair_status": "REPAIRED" if appended else "NO_REPAIR_REQUIRED",
        "repair_type": "INGESTION_COMPLETENESS_REPAIR" if appended else "NO_REPAIR",
        "required_market_data_items": [f"historical_bar.{symbol}.{day}"],
        "required_fields": ["open", "high", "low", "close", "volume"],
        "source_data_found": True,
        "source_data_path": str(raw_csv),
        "routed_data_path": str(year_file),
        "source_fresh": True,
        "data_quality_status": "CURRENT",
        "remaining_blocker": "NONE",
        "blocker_reason": "NONE",
        "source_paths": {"raw_stooq_quote": str(raw_csv), "historical_jsonl": str(year_file)},
        "created_paths": [str(year_file)] if appended else [],
    }


def _post_diagnostics(root: Path, day: str, computed_at: str) -> dict[str, Any]:
    t01 = build_sleeve_throughput_diagnostics_v1(truth_root=root, day_utc=day, computed_at_utc=computed_at)
    t02 = build_dormant_sleeve_signal_generation_diagnostics_v1(truth_root=root, day_utc=day, computed_at_utc=computed_at)
    t03 = build_missing_market_data_requirement_resolver_v1(truth_root=root, day_utc=day, computed_at_utc=computed_at)
    return {"t01": t01, "t02": t02, "t03": t03, "latest_rollup": _latest_sleeve_rollup(root, day)}


def _build_row(
    *,
    root: Path,
    day: str,
    computed_at: str,
    sleeve_id: str,
    repair: Mapping[str, Any],
    original_t03: Mapping[str, Any],
    post_payloads: Mapping[str, Any],
) -> dict[str, Any]:
    post_t01 = _first_sleeve(post_payloads.get("t01"), sleeve_id, "sleeves")
    post_t02 = _first_sleeve(post_payloads.get("t02"), sleeve_id, "dormant_sleeves")
    post_t03 = _first_sleeve(post_payloads.get("t03"), sleeve_id, "sleeves")
    latest_eval = _first_sleeve(post_payloads.get("latest_rollup"), sleeve_id, "outcomes")
    target = TARGET_SLEEVES[sleeve_id]
    status = _text(repair.get("repair_status"))
    post_t01_status = _post_t01_status(post_t01, latest_eval)
    post_t02_reason = _post_t02_reason(post_t02, latest_eval)
    remaining_blocker = _remaining_blocker(repair, post_t02, post_t03, latest_eval)
    owner = "NONE" if remaining_blocker == "NONE" else "AEGIS_SYSTEM"
    row = {
        "schema_id": "aegis_missing_market_data_repair_row",
        "schema_version": "v1",
        "day_utc": day,
        "computed_at_utc": computed_at,
        "sleeve_id": sleeve_id,
        "sleeve_name": _text(original_t03.get("sleeve_name")) or target["sleeve_name"],
        "original_t03_resolution_type": _text(original_t03.get("resolution_type")) or "UNKNOWN_DETERMINISTIC_BLOCKER",
        "repair_attempted": status not in {"NO_REPAIR_REQUIRED"},
        "repair_status": status,
        "repair_type": _text(repair.get("repair_type")),
        "required_market_data_items": _list(repair.get("required_market_data_items")),
        "required_fields": _list(repair.get("required_fields")),
        "source_data_found": bool(repair.get("source_data_found")),
        "source_data_path": _text(repair.get("source_data_path")),
        "routed_data_path": _text(repair.get("routed_data_path")),
        "source_fresh": bool(repair.get("source_fresh")),
        "data_quality_status": _text(repair.get("data_quality_status")),
        "post_repair_market_data_status": _post_market_status(post_t03, repair),
        "post_repair_signal_generation_status": _text(latest_eval.get("current_status") or latest_eval.get("status") or post_t02.get("trigger_evaluation_status") or post_t02.get("signal_producer_status") or post_t01.get("throughput_status")),
        "post_repair_candidate_status": _candidate_status(post_t01, latest_eval),
        "post_repair_t01_status": post_t01_status,
        "post_repair_t02_reason_code": post_t02_reason,
        "post_repair_t03_resolution_type": _text(post_t03.get("resolution_type")) or "NO_T03_ROW",
        "remaining_blocker": remaining_blocker,
        "blocker_reason": _text(repair.get("blocker_reason")),
        "owner": owner,
        "david_action_required": False,
        "next_action": _next_action(status, post_t02, post_t03),
        "source_paths": dict(repair.get("source_paths") or {}),
        "created_paths": _list(repair.get("created_paths")),
        "original_t03_link": {
            "artifact_id": "aegis_missing_market_data_requirement_resolver_v1",
            "path": str(report_path_v1(root, "aegis_missing_market_data_requirement_resolver_v1", day, "missing_market_data_requirement_resolver.v1.json")),
            "sleeve_id": sleeve_id,
            "resolution_type": _text(original_t03.get("resolution_type")),
        },
        "post_repair_evidence_links": {
            "t01": str(report_path_v1(root, "aegis_sleeve_throughput_diagnostics_v1", day, "sleeve_throughput_diagnostics.v1.json")),
            "t02": str(report_path_v1(root, "aegis_dormant_sleeve_signal_generation_diagnostics_v1", day, "dormant_sleeve_signal_generation_diagnostics.v1.json")),
            "t03": str(report_path_v1(root, "aegis_missing_market_data_requirement_resolver_v1", day, "missing_market_data_requirement_resolver.v1.json")),
            "latest_sleeve_evaluation": _text(latest_eval.get("artifact_path")),
        },
        **SAFETY,
    }
    row["row_hash"] = stable_hash_v1({**row, "row_hash": ""})
    return row


def _market_snapshot_payload(symbol: str, day: str, source_file: Path, source_row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "bars": [dict(source_row)],
        "day_utc": day,
        "schema_id": "C2_MARKET_DATA_SNAPSHOT_V1",
        "schema_version": "v1",
        "source_provenance": {
            "market_data_yearly_jsonl": {
                "matching_day_row_count": 1,
                "path": str(source_file),
                "row_count": _jsonl_count(source_file),
                "sha256": _sha256_file(source_file),
            }
        },
        "symbol": symbol,
    }


def _nav_v1_payload(day: str, source: Path, nav: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "day_utc": day,
        "history": {"drawdown_pct": _text(_dict(nav.get("history")).get("drawdown_pct")) or "0.000000"},
        "input_manifest": [{"path": str(source), "sha256": _sha256_file(source), "type": "compatibility_source", "producer": "accounting_nav_v2"}],
        "producer": {"module": "ops.aegis.missing_market_data_repair_v1", "repo": "constellation"},
        "reason_codes": ["COMPAT_BRIDGE_FROM_NAV_V2"],
        "schema_id": "C2_NAV_SNAPSHOT_V1",
        "schema_version": "v1",
        "source_type": _text(nav.get("source_type")),
        "status": "OK",
    }


def _positions_v2_payload(
    day: str,
    source: Path,
    positions: Mapping[str, Any],
    failure: Path,
    failure_payload: Mapping[str, Any],
    nav_source: Path,
    nav: Mapping[str, Any],
) -> dict[str, Any]:
    if positions:
        items = _dict(_dict(positions.get("positions")).get("positions")).get("items") or _dict(positions.get("positions")).get("items") or []
        manifest = [{"path": str(source), "sha256": _sha256_file(source), "type": "compatibility_source", "producer": "positions_snapshot_v5"}]
        reason = ["COMPAT_BRIDGE_FROM_POSITIONS_V5"]
    else:
        items = []
        manifest = [
            {"path": str(failure), "sha256": _sha256_file(failure), "type": "bootstrap_failure_evidence", "producer": _text(failure_payload.get("producer")) or "positions_snapshot_v5"},
            {"path": str(nav_source), "sha256": _sha256_file(nav_source), "type": "bootstrap_nav_evidence", "producer": "accounting_nav_v2"},
        ]
        reason = ["DAY0_BOOTSTRAP_EMPTY_POSITIONS_COMPAT_BRIDGE"]
    return {
        "day_utc": day,
        "input_manifest": manifest,
        "positions": {
            "asof_utc": f"{day}T00:00:00Z",
            "currency": "USD",
            "items": items if isinstance(items, list) else [],
            "notes": ["COMPATIBILITY BRIDGE: derived only from canonical runtime positions/bootstrap evidence"],
        },
        "produced_utc": f"{day}T00:00:00Z",
        "producer": {"module": "ops.aegis.missing_market_data_repair_v1", "repo": "constellation"},
        "reason_codes": reason,
        "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
        "schema_version": 2,
        "status": "OK" if positions else "BOOTSTRAP",
    }


def _append_historical_day_bar(root: Path, year_file: Path, raw_csv: Path, quote: Mapping[str, Any]) -> bool:
    year_file.parent.mkdir(parents=True, exist_ok=True)
    rows = _read_jsonl(year_file)
    day = _text(quote.get("day_utc"))
    if any(_text(row.get("day_utc")) == day for row in rows):
        return False
    row = dict(quote)
    row["raw_source_hash"] = _sha256_file(raw_csv)
    row["source_hash"] = _sha256_file(raw_csv)
    row["source_url_or_path"] = str(raw_csv)
    row["synthetic_data"] = False
    rows.append(row)
    rows.sort(key=lambda item: _text(item.get("day_utc") or item.get("timestamp_utc")))
    year_file.write_text("".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8")
    _update_dataset_manifest(root, year_file)
    return True


def _update_dataset_manifest(root: Path, year_file: Path) -> None:
    manifest = root / "market_data_snapshot_v1" / "dataset_manifest.json"
    payload = read_json_v1(manifest)
    files = payload.get("files")
    if not isinstance(files, list):
        return
    rel = str(year_file.relative_to(root / "market_data_snapshot_v1"))
    digest = _sha256_file(year_file)
    found = False
    for row in files:
        if isinstance(row, dict) and _text(row.get("file")) == rel:
            row["sha256"] = digest
            found = True
    if not found:
        symbol = rel.split("/", 1)[0]
        files.append({"file": rel, "sha256": digest, "symbol": symbol, "year": int(year_file.stem)})
        files.sort(key=lambda item: _text(item.get("file")) if isinstance(item, Mapping) else "")
    payload["files"] = files
    payload["last_completeness_repair_utc"] = _text(payload.get("last_completeness_repair_utc")) or ""
    manifest.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _stooq_quote_row(path: Path, symbol: str, day: str) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if _text(row.get("Date")) == day and _text(row.get("Symbol")).upper().startswith(symbol):
                timestamp = f"{row.get('Date')}T{row.get('Time') or '00:00:00'}Z"
                return {
                    "close": _num(row.get("Close")),
                    "day_utc": day,
                    "high": _num(row.get("High")),
                    "low": _num(row.get("Low")),
                    "open": _num(row.get("Open")),
                    "provider": "STOOQ",
                    "retrieved_at_utc": timestamp,
                    "source": "STOOQ",
                    "symbol": symbol,
                    "timestamp_utc": timestamp,
                    "transformed_hash": stable_hash_v1(dict(row)),
                    "value": _num(row.get("Close")),
                    "volume": int(float(row.get("Volume") or 0)),
                }
    return {}


def _read_t03(root: Path, day: str) -> dict[str, Any]:
    return read_json_v1(report_path_v1(root, "aegis_missing_market_data_requirement_resolver_v1", day, "missing_market_data_requirement_resolver.v1.json"))


def _original_t03_by_sleeve(prior: Mapping[str, Any], t03: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in _rows(prior, "sleeves"):
        sid = _text(row.get("sleeve_id"))
        if sid:
            out[sid] = {"sleeve_name": row.get("sleeve_name"), "resolution_type": row.get("original_t03_resolution_type")}
    for row in _rows(t03, "sleeves"):
        sid = _text(row.get("sleeve_id"))
        if sid and sid not in out:
            out[sid] = dict(row)
    return out


def _source_paths(root: Path, day: str) -> dict[str, str]:
    return {
        "t01": str(report_path_v1(root, "aegis_sleeve_throughput_diagnostics_v1", day, "sleeve_throughput_diagnostics.v1.json")),
        "t02": str(report_path_v1(root, "aegis_dormant_sleeve_signal_generation_diagnostics_v1", day, "dormant_sleeve_signal_generation_diagnostics.v1.json")),
        "t03": str(report_path_v1(root, "aegis_missing_market_data_requirement_resolver_v1", day, "missing_market_data_requirement_resolver.v1.json")),
        "dataset_manifest": str(root / "market_data_snapshot_v1" / "dataset_manifest.json"),
    }


def _summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "total_targeted_sleeves": len(rows),
        "repaired_count": sum(1 for row in rows if row["repair_status"] == "REPAIRED"),
        "not_repaired_count": sum(1 for row in rows if row["repair_status"].startswith("NOT_REPAIRED") or row["repair_status"] == "UNKNOWN_DETERMINISTIC_BLOCKER"),
        "no_repair_required_count": sum(1 for row in rows if row["repair_status"] == "NO_REPAIR_REQUIRED"),
        "david_action_required_count": sum(1 for row in rows if row["david_action_required"]),
        "aegis_system_remaining_action_count": sum(1 for row in rows if row["owner"] == "AEGIS_SYSTEM"),
        "sleeves_now_flowing_count": sum(1 for row in rows if row["post_repair_t01_status"] == "FLOWING"),
        "sleeves_now_valid_no_signal_count": sum(1 for row in rows if row["post_repair_t02_reason_code"] in {"VALID_NO_SIGNAL_CONDITIONS", "TRIGGER_THRESHOLDS_NOT_MET"}),
        "sleeves_still_blocked_count": sum(1 for row in rows if row["remaining_blocker"] != "NONE"),
    }


def _answer(rows: list[dict[str, Any]]) -> str:
    parts = ", ".join(f"{row['sleeve_id']}={row['repair_status']}->{row['post_repair_t03_resolution_type']}" for row in rows)
    return f"T04 targeted {len(rows)} AEGIS-owned missing-market-data sleeves. Repair proof: {parts}."


def _post_market_status(post_t03: Mapping[str, Any], repair: Mapping[str, Any]) -> str:
    if _text(post_t03.get("resolution_type")) == "NO_ACTION_REQUIRED":
        return "AVAILABLE"
    if _text(repair.get("remaining_blocker")) == "NONE":
        return "REPAIRED_PENDING_REEVALUATION"
    return "BLOCKED"


def _post_t01_status(post_t01: Mapping[str, Any], latest_eval: Mapping[str, Any]) -> str:
    status = _text(latest_eval.get("current_status") or latest_eval.get("status")).upper()
    if status in {"INTENT_CREATED", "CANDIDATE_CREATED"}:
        return "FLOWING"
    if status in {"NO_INTENT", "NO_SIGNAL"}:
        return "DORMANT"
    if status == "BLOCKED":
        return "BLOCKED"
    return _text(post_t01.get("throughput_status") or post_t01.get("sleeve_status")) or "UNKNOWN"


def _post_t02_reason(post_t02: Mapping[str, Any], latest_eval: Mapping[str, Any]) -> str:
    status = _text(latest_eval.get("current_status") or latest_eval.get("status")).upper()
    blocker = _text(latest_eval.get("canonical_blocker"))
    if status in {"INTENT_CREATED", "CANDIDATE_CREATED"}:
        return "NOT_DORMANT"
    if status in {"NO_INTENT", "NO_SIGNAL"} and not blocker:
        return "VALID_NO_SIGNAL_CONDITIONS"
    if blocker:
        return blocker
    return _text(post_t02.get("dormant_reason_code")) or "UNKNOWN"


def _candidate_status(post_t01: Mapping[str, Any], latest_eval: Mapping[str, Any]) -> str:
    status = _text(latest_eval.get("current_status") or latest_eval.get("status")).upper()
    if status in {"INTENT_CREATED", "CANDIDATE_CREATED"}:
        return "CANDIDATES_PRESENT"
    if post_t01 and int(post_t01.get("candidate_count") or 0) > 0:
        return "CANDIDATES_PRESENT"
    return "NO_CANDIDATES_PRESENT" if post_t01 or latest_eval else "UNKNOWN"


def _remaining_blocker(repair: Mapping[str, Any], post_t02: Mapping[str, Any], post_t03: Mapping[str, Any], latest_eval: Mapping[str, Any]) -> str:
    if _text(repair.get("remaining_blocker")) != "NONE":
        return _text(repair.get("remaining_blocker"))
    if _text(post_t03.get("resolution_type")) not in {"", "NO_ACTION_REQUIRED"}:
        return _text(post_t03.get("resolution_type"))
    latest_status = _text(latest_eval.get("current_status") or latest_eval.get("status")).upper()
    latest_blocker = _text(latest_eval.get("canonical_blocker"))
    if latest_status in {"INTENT_CREATED", "CANDIDATE_CREATED", "NO_INTENT", "NO_SIGNAL"} and not latest_blocker:
        return "NONE"
    if latest_blocker:
        return latest_blocker
    if _text(post_t02.get("dormant_reason_code")) == "MISSING_MARKET_DATA":
        return "MISSING_MARKET_DATA"
    return "NONE"


def _next_action(status: str, post_t02: Mapping[str, Any], post_t03: Mapping[str, Any]) -> str:
    if status.startswith("NOT_REPAIRED"):
        return "Do not fabricate market data; preserve blocker and collect governed source evidence."
    if _text(post_t03.get("resolution_type")) == "NO_ACTION_REQUIRED":
        return "No market-data repair action remains; use regenerated sleeve diagnostics for any normal non-market-data blocker."
    if _text(post_t02.get("dormant_reason_code")) and _text(post_t02.get("dormant_reason_code")) != "MISSING_MARKET_DATA":
        return "Market-data blocker cleared; current zero-signal status is explained by T02."
    return "Rerun sleeve evaluation, T01, T02, and T03 to refresh post-repair diagnostics."


def _latest_sleeve_rollup(root: Path, day: str) -> dict[str, Any]:
    base = root / "reports" / "sleeve_evaluation_kernel_v1" / day
    candidates = [path for path in base.glob("*/sleeve_evaluation_rollup.v1.json") if path.is_file()]
    if not candidates:
        return {}
    latest = max(candidates, key=lambda path: path.stat().st_mtime_ns)
    payload = read_json_v1(latest)
    if payload:
        payload["artifact_path"] = str(latest)
    return payload


def _sleeve_truth_root(root: Path) -> Path:
    return root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"


def _jsonl_day_row(path: Path, day: str) -> dict[str, Any]:
    for row in _read_jsonl(path):
        if _text(row.get("day_utc")) == day or _text(row.get("timestamp_utc")).startswith(day):
            return row
    return {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _jsonl_count(path: Path) -> int:
    return len(_read_jsonl(path))


def _write_json_if_changed(path: Path, payload: Mapping[str, Any]) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _sha256_file(path: Path) -> str:
    try:
        return sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _first_sleeve(payload: Any, sleeve_id: str, key: str) -> dict[str, Any]:
    for row in _rows(payload, key):
        if _text(row.get("sleeve_id") or row.get("engine_id")) == sleeve_id:
            return row
    return {}


def _rows(payload: Any, key: str) -> list[dict[str, Any]]:
    data = _dict(payload)
    value = data.get(key)
    return [dict(row) for row in value if isinstance(row, Mapping)] if isinstance(value, list) else []


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _num(value: Any) -> float:
    return float(_text(value) or 0)
