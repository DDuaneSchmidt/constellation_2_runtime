#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root

ENGINE_REGISTRY_RELPATH = "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
TARGET_CONTRACTS = {
    "C2_CROSS_ASSET_TREND_V1": "governance/05_CONTRACTS/C2/cross_asset_trend_v1.contract.md",
    "C2_MARKET_NEUTRAL_SPREAD_V1": "governance/05_CONTRACTS/C2/market_neutral_spread_v1.contract.md",
}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return payload


def _section(text: str, title: str) -> str:
    marker = f"### {title}"
    start = text.find(marker)
    if start < 0:
        return ""
    rest = text[start + len(marker) :]
    match = re.search(r"\n###\s+", rest)
    return rest[: match.start()] if match else rest


def _symbols_from_contract(engine_id: str, text: str) -> list[str]:
    if engine_id == "C2_CROSS_ASSET_TREND_V1":
        body = _section(text, "1.1 Universe")
        symbols = re.findall(r"^\s*-\s*([A-Z]{2,5})\s*$", body, flags=re.MULTILINE)
    elif engine_id == "C2_MARKET_NEUTRAL_SPREAD_V1":
        body = _section(text, "1.1 Pairs")
        symbols = []
        for left, right in re.findall(r"^\s*-\s*([A-Z]{2,5})\s+vs\s+([A-Z]{2,5})\s*$", body, flags=re.MULTILINE):
            symbols.extend([left, right])
    else:
        symbols = []
    return sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})


def _validation_rules(engine_id: str, text: str) -> dict[str, Any]:
    if engine_id == "C2_CROSS_ASSET_TREND_V1":
        slow = int((re.search(r"SMA_SLOW\s*=\s*(\d+)", text) or ["", "100"])[1])
        return {
            "lookback_required": f"SMA_SLOW={slow}",
            "bar_interval": "1 day",
            "minimum_history_days": slow,
            "validation_rules": [
                "dataset exists",
                "all contract universe symbols present in manifest",
                "manifest sha256 entries match files",
                "rows are valid JSON objects with positive close and UTC timestamps",
                f"at least {slow} bars up to day_utc",
                "latest bar timestamp day equals day_utc",
                "no missing bars versus SPY reference sessions over the minimum-history window",
                "symbols match contract-derived registry activation universe",
            ],
        }
    lookback = int((re.search(r"LOOKBACK\s*=\s*(\d+)", text) or ["", "60"])[1])
    return {
        "lookback_required": f"LOOKBACK={lookback}",
        "bar_interval": "1 day",
        "minimum_history_days": lookback,
        "validation_rules": [
            "dataset exists",
            "all pair symbols present in manifest",
            "manifest sha256 entries match files",
            "rows are valid JSON objects with positive close and UTC timestamps",
            f"at least {lookback} aligned bars up to day_utc",
            "latest bar timestamp day equals day_utc",
            "no missing bars versus SPY reference sessions over the minimum-history window",
            "symbols match contract-derived registry activation universe",
        ],
    }


def resolve_required_sleeve_datasets(*, repo_root: Path = REPO_ROOT) -> list[dict[str, Any]]:
    registry_path = (repo_root / ENGINE_REGISTRY_RELPATH).resolve()
    registry = _read_json(registry_path)
    registry_by_id = {
        str(row.get("engine_id") or ""): row
        for row in registry.get("engines", [])
        if isinstance(row, dict)
    }
    rows: list[dict[str, Any]] = []
    for engine_id, contract_rel in TARGET_CONTRACTS.items():
        contract_path = (repo_root / contract_rel).resolve()
        text = contract_path.read_text(encoding="utf-8")
        required_symbols = _symbols_from_contract(engine_id, text)
        rules = _validation_rules(engine_id, text)
        rows.append(
            {
                "sleeve_id": engine_id,
                "required_symbols": required_symbols,
                "registry_allowed_symbols_current": [
                    str(symbol).strip().upper()
                    for symbol in registry_by_id.get(engine_id, {}).get("allowed_symbols", [])
                    if str(symbol).strip()
                ],
                "dataset_source": {
                    "engine_registry_path": str(registry_path),
                    "contract_path": str(contract_path),
                    "symbol_authority": "contract_required_universe_or_pairs",
                },
                **rules,
            }
        )
    return rows


def _manifest_entries(manifest: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for entry in manifest.get("files", []):
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol") or "").strip().upper()
        rel_file = str(entry.get("file") or "").strip()
        if symbol and rel_file:
            out.setdefault(symbol, []).append(dict(entry))
    return out


def _rows_for_symbol(*, md_root: Path, entries: list[dict[str, Any]], symbol: str, day_utc: str) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for entry in entries:
        rel_file = str(entry.get("file") or "")
        path = (md_root / rel_file).resolve()
        if not str(path).startswith(str(md_root.resolve())):
            errors.append(f"MANIFEST_PATH_ESCAPES_ROOT:{rel_file}")
            continue
        if not path.is_file():
            errors.append(f"DATA_FILE_MISSING:{rel_file}")
            continue
        expected_sha = str(entry.get("sha256") or "").strip().lower()
        actual_sha = _sha256_file(path)
        if expected_sha and expected_sha != actual_sha:
            errors.append(f"SHA256_MISMATCH:{rel_file}")
            continue
        with path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    errors.append(f"JSONL_PARSE_FAILED:{rel_file}:{line_no}")
                    continue
                if not isinstance(row, dict):
                    errors.append(f"JSONL_ROW_NOT_OBJECT:{rel_file}:{line_no}")
                    continue
                if str(row.get("symbol") or "").strip().upper() != symbol:
                    continue
                ts = str(row.get("timestamp_utc") or "")
                if len(ts) < 10 or not ts.endswith("Z"):
                    errors.append(f"BAD_TIMESTAMP:{rel_file}:{line_no}")
                    continue
                if ts[:10] > day_utc:
                    continue
                try:
                    close = float(row.get("close"))
                except Exception:
                    errors.append(f"BAD_CLOSE:{rel_file}:{line_no}")
                    continue
                if close <= 0.0:
                    errors.append(f"NONPOSITIVE_CLOSE:{rel_file}:{line_no}")
                    continue
                rows.append(row)
    rows.sort(key=lambda row: str(row.get("timestamp_utc") or ""))
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        ts = str(row.get("timestamp_utc") or "")
        if ts in seen:
            errors.append(f"DUPLICATE_TIMESTAMP:{symbol}:{ts}")
            continue
        seen.add(ts)
        deduped.append(row)
    return deduped, errors


def build_sleeve_dataset_readiness(
    *,
    day_utc: str,
    truth_root: Path,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    requirements = resolve_required_sleeve_datasets(repo_root=repo_root)
    md_root = Path(truth_root).resolve() / "market_data_snapshot_v1"
    manifest_path = md_root / "dataset_manifest.json"
    manifest = _read_json(manifest_path) if manifest_path.is_file() else {}
    entries_by_symbol = _manifest_entries(manifest)
    reference_rows, _reference_errors = _rows_for_symbol(md_root=md_root, entries=entries_by_symbol.get("SPY", []), symbol="SPY", day_utc=day_utc)
    reference_days = [str(row.get("timestamp_utc") or "")[:10] for row in reference_rows]
    rows_out: list[dict[str, Any]] = []
    for requirement in requirements:
        min_history = int(requirement["minimum_history_days"])
        required_symbols = list(requirement["required_symbols"])
        symbol_results: list[dict[str, Any]] = []
        missing_symbols: list[str] = []
        stale_symbols: list[str] = []
        insufficient_history_symbols: list[str] = []
        forbidden_gap_symbols: list[str] = []
        for symbol in required_symbols:
            entries = entries_by_symbol.get(symbol, [])
            if not entries:
                missing_symbols.append(symbol)
                symbol_results.append({"symbol": symbol, "status": "MISSING", "rows": 0, "missing_bars": [], "latest_bar_day": ""})
                continue
            rows, errors = _rows_for_symbol(md_root=md_root, entries=entries, symbol=symbol, day_utc=day_utc)
            days = [str(row.get("timestamp_utc") or "")[:10] for row in rows]
            latest = days[-1] if days else ""
            if latest != day_utc:
                stale_symbols.append(symbol)
            if len(rows) < min_history:
                insufficient_history_symbols.append(symbol)
            expected_recent = reference_days[-min_history:] if len(reference_days) >= min_history else reference_days
            missing_bars = sorted(set(expected_recent) - set(days))
            if missing_bars:
                forbidden_gap_symbols.append(symbol)
            status = "PASS" if not errors and latest == day_utc and len(rows) >= min_history and not missing_bars else "FAIL"
            symbol_results.append(
                {
                    "symbol": symbol,
                    "provider": "IB historical daily bars",
                    "start_timestamp": str(rows[0].get("timestamp_utc") or "") if rows else "",
                    "end_timestamp": str(rows[-1].get("timestamp_utc") or "") if rows else "",
                    "rows": len(rows),
                    "missing_bars": missing_bars,
                    "stale_status": "CURRENT" if latest == day_utc else "STALE",
                    "validation_status": status,
                    "errors": errors,
                }
            )
        dataset_ready = not (missing_symbols or stale_symbols or insufficient_history_symbols or forbidden_gap_symbols)
        rows_out.append(
            {
                **requirement,
                "dataset_ready": dataset_ready,
                "missing_symbols": sorted(set(missing_symbols)),
                "stale_symbols": sorted(set(stale_symbols)),
                "insufficient_history_symbols": sorted(set(insufficient_history_symbols)),
                "forbidden_gap_symbols": sorted(set(forbidden_gap_symbols)),
                "activation_blocker": "" if dataset_ready else "SLEEVE_DATASET_NOT_READY",
                "symbol_results": symbol_results,
            }
        )
    status = "PASS" if all(row["dataset_ready"] for row in rows_out) else "FAIL"
    out_path = Path(truth_root).resolve() / "reports" / "sleeve_dataset_readiness_v1" / day_utc / "sleeve_dataset_readiness.v1.json"
    for row in rows_out:
        row["validation_artifact_path"] = str(out_path)

    payload = {
        "schema_id": "sleeve_dataset_readiness",
        "schema_version": "v1",
        "day_utc": day_utc,
        "created_at_utc": _now_iso(),
        "status": status,
        "canonical_blocker": "" if status == "PASS" else "SLEEVE_DATASET_NOT_READY",
        "market_data_manifest_path": str(manifest_path),
        "market_data_manifest_sha256": _sha256_file(manifest_path) if manifest_path.is_file() else "",
        "rows": rows_out,
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_sleeve_dataset_readiness_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="", help="Canonical truth root; defaults to runtime contract canonical truth root.")
    args = ap.parse_args()
    day = str(args.day_utc).strip()
    truth_root = Path(args.truth_root).expanduser().resolve() if str(args.truth_root or "").strip() else Path(resolve_canonical_truth_root()).resolve()
    payload = build_sleeve_dataset_readiness(day_utc=day, truth_root=truth_root)
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "artifact_path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
