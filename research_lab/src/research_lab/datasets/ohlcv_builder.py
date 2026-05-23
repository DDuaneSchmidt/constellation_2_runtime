from __future__ import annotations

import platform
import sys
from datetime import date
from pathlib import Path
from typing import Any

from research_lab import SCHEMA_VERSION
from research_lab.audit.audit_log import write_audit_event
from research_lab.acquisition.csv_readiness import validate_readiness_report_for_build
from research_lab.bars.bar_policy import require_bar_policy
from research_lab.bars.canonical_daily_bars import attach_dataset_snapshot_id, canonicalize_daily_ohlcv_rows, symbols_in_rows
from research_lab.contracts.schemas import validate_contract
from research_lab.datasets.coverage_report import build_coverage_report, write_coverage_report
from research_lab.datasets.dataset_registry import append_dataset_registry_entry
from research_lab.datasets.validation import validate_ohlcv_records
from research_lab.providers.base import DailyOHLCVProvider
from research_lab.providers.factory import get_daily_ohlcv_provider
from research_lab.storage.hashing import content_hash, sha256_hex, short_hash, utc_now_iso
from research_lab.storage.manifest_io import write_json
from research_lab.storage.parquet_io import file_sha256, write_parquet_records
from research_lab.storage.paths import dataset_uri, ensure_store_layout
from research_lab.universes.universe_registry import load_universe_snapshot
from research_lab.providers.local_csv_daily import local_csv_root


def _compact_date(value: str) -> str:
    return value.replace("-", "")


def _package_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for name in ["pandas", "pyarrow", "fastparquet", "duckdb", "yfinance"]:
        try:
            module = __import__(name)
            versions[name] = str(getattr(module, "__version__", "unknown"))
        except Exception:
            versions[name] = "NOT_INSTALLED"
    return versions


def _dataset_content_hash(snapshot: dict[str, Any]) -> str:
    return content_hash(
        snapshot,
        exclude={
            "dataset_snapshot_id",
            "created_at",
            "content_hash",
            "source_hash",
            "storage_uri",
            "quality_report_uri",
        },
        sort_lists=False,
    )


def build_ohlcv_dataset_snapshot(
    *,
    dataset_type: str,
    provider_name: str,
    interval: str,
    bar_policy_version: str,
    universe_snapshot_id: str,
    start_date: str,
    end_date: str,
    provider: DailyOHLCVProvider | None = None,
    store_root: Path | None = None,
    created_by: str = "Aegis",
    command: str = "",
    allow_test_parquet_fallback: bool = False,
    allow_missing_symbols: bool = False,
    require_readiness_report: bool = False,
    readiness_report_path: Path | None = None,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    require_bar_policy(bar_policy_version)
    started_at = utc_now_iso()
    universe = load_universe_snapshot(universe_snapshot_id, store_root=store)
    symbols_requested = [str(row["symbol"]).upper() for row in universe["symbols"]]

    write_audit_event(
        actor=created_by,
        entity_type="dataset_snapshot_build",
        entity_id=universe_snapshot_id,
        action="ohlcv_dataset_build_started",
        reason="Started daily OHLCV dataset snapshot build.",
        metadata={
            "provider": provider_name,
            "interval": interval,
            "bar_policy_version": bar_policy_version,
            "start_date": start_date,
            "end_date": end_date,
            "symbols_requested": symbols_requested,
            "allow_missing_symbols": allow_missing_symbols,
        },
        store_root=store,
    )

    raw_by_symbol: dict[str, list[dict[str, Any]]] = {}
    fetch_errors: dict[str, str] = {}
    canonical_without_id: list[dict[str, Any]] = []
    raw_adjustment_policies: set[str] = set()
    try:
        if provider_name == "local_csv" and require_readiness_report:
            report_path = readiness_report_path or (local_csv_root() / "csv_readiness_report.json")
            validate_readiness_report_for_build(
                report_path=report_path,
                universe_snapshot_id=universe_snapshot_id,
                csv_root=local_csv_root(),
            )
        active_provider = provider or get_daily_ohlcv_provider(provider_name)
        provider_version = str(active_provider.provider_version)
        adjustment_policy = str(getattr(active_provider, "adjustment_policy", "provider_adjusted_close"))
        for symbol in symbols_requested:
            try:
                raw_rows = active_provider.fetch_daily_ohlcv(
                    symbol,
                    date.fromisoformat(start_date),
                    date.fromisoformat(end_date),
                )
                canonical_rows = canonicalize_daily_ohlcv_rows(raw_rows, bar_policy_version=bar_policy_version)
                raw_by_symbol[symbol] = [dict(row) for row in raw_rows] if isinstance(raw_rows, list) else canonical_rows
                if isinstance(raw_rows, list):
                    raw_adjustment_policies.update(
                        str(row.get("adjustment_policy") or "").strip()
                        for row in raw_rows
                        if str(row.get("adjustment_policy") or "").strip()
                    )
                canonical_without_id.extend(canonical_rows)
                write_audit_event(
                    actor=created_by,
                    entity_type="raw_symbol_ohlcv",
                    entity_id=symbol,
                    action="raw_symbol_fetch_completed",
                    reason="Fetched raw daily OHLCV rows from provider.",
                    metadata={"row_count": len(canonical_rows), "provider": provider_name},
                    store_root=store,
                )
            except Exception as exc:
                fetch_errors[symbol] = str(exc)
                diagnostic = getattr(exc, "diagnostic", {})
                write_audit_event(
                    actor=created_by,
                    entity_type="raw_symbol_ohlcv",
                    entity_id=symbol,
                    action="raw_symbol_fetch_failed",
                    reason=str(exc),
                    metadata={"provider": provider_name, "diagnostic": diagnostic},
                    store_root=store,
                )

        if fetch_errors and not allow_missing_symbols:
            raise RuntimeError(
                "Missing or failed symbol fetches with allow_missing_symbols=false: "
                + ", ".join(f"{symbol}: {error}" for symbol, error in sorted(fetch_errors.items()))
            )
        if not canonical_without_id:
            raise RuntimeError("No OHLCV rows loaded; refusing to create a successful dataset snapshot")

        base_hash = sha256_hex(
            {
                "dataset_type": dataset_type,
                "provider": provider_name,
                "provider_version": provider_version,
                "interval": interval,
                "bar_policy_version": bar_policy_version,
                "universe_snapshot_id": universe_snapshot_id,
                "start_date": start_date,
                "end_date": end_date,
                "canonical_rows_without_dataset_id": canonical_without_id,
            }
        )
        dataset_snapshot_id = (
            f"ds_{dataset_type}_{interval}_{universe['universe_name']}_"
            f"{_compact_date(start_date)}_{_compact_date(end_date)}_{short_hash(base_hash)}"
        )
        dataset_dir = store / "datasets" / dataset_snapshot_id
        if dataset_dir.exists():
            raise FileExistsError(f"Refusing to overwrite immutable dataset snapshot: {dataset_dir}")
        raw_root = dataset_dir / "data" / "raw" / f"provider={provider_name}" / f"interval={interval}"
        canonical_path = dataset_dir / "data" / "canonical" / "daily_ohlcv.parquet"
        dataset_dir.mkdir(parents=True, exist_ok=False)

        raw_file_hashes: dict[str, str] = {}
        for symbol, rows in sorted(raw_by_symbol.items()):
            raw_path = raw_root / f"symbol={symbol}.parquet"
            write_parquet_records(raw_path, rows, allow_json_fallback=allow_test_parquet_fallback)
            raw_file_hashes[symbol] = file_sha256(raw_path)

        canonical_rows = attach_dataset_snapshot_id(canonical_without_id, dataset_snapshot_id)
        write_parquet_records(canonical_path, canonical_rows, allow_json_fallback=allow_test_parquet_fallback)
        canonical_file_hash = file_sha256(canonical_path)
        write_audit_event(
            actor=created_by,
            entity_type="dataset_snapshot",
            entity_id=dataset_snapshot_id,
            action="canonical_dataset_written",
            new_state_hash=canonical_file_hash,
            reason="Wrote canonical daily OHLCV dataset file.",
            metadata={"canonical_path": str(canonical_path), "row_count": len(canonical_rows)},
            store_root=store,
        )

        symbols_loaded = symbols_in_rows(canonical_rows)
        effective_adjustment_policy = (
            "unadjusted_close_as_adj_close"
            if "unadjusted_close_as_adj_close" in raw_adjustment_policies
            else adjustment_policy
        )
        quality_report = validate_ohlcv_records(
            canonical_rows,
            symbols_requested=symbols_requested,
            canonical_written=canonical_path.exists(),
            adjustment_policy=effective_adjustment_policy,
        )
        quality_report["raw_adjustment_policies"] = sorted(raw_adjustment_policies)
        quality_report["fetch_errors"] = fetch_errors
        if fetch_errors and allow_missing_symbols and quality_report["quality_status"] == "pass":
            quality_report["quality_status"] = "pass_with_warnings"
            if "some_symbols_missing" not in quality_report["warning_reasons"]:
                quality_report["warning_reasons"].append("some_symbols_missing")
        quality_report_path = dataset_dir / "quality_report.json"
        write_json(quality_report_path, quality_report, overwrite=False)
        quality_report_hash = file_sha256(quality_report_path)
        coverage_report = build_coverage_report(
            dataset_snapshot_id=dataset_snapshot_id,
            universe_snapshot_id=universe_snapshot_id,
            symbols_requested=symbols_requested,
            symbols_loaded=symbols_loaded,
            symbols_missing=quality_report["symbols_missing"],
            canonical_rows=canonical_rows,
            start_date=start_date,
            end_date=end_date,
            created_at=started_at,
        )
        coverage_report_path, coverage_report_hash = write_coverage_report(dataset_dir, coverage_report)
        write_audit_event(
            actor=created_by,
            entity_type="dataset_snapshot",
            entity_id=dataset_snapshot_id,
            action="quality_validation_completed",
            new_state_hash=quality_report_hash,
            reason="Completed OHLCV dataset quality validation.",
            metadata={"quality_status": quality_report["quality_status"], "warning_reasons": quality_report["warning_reasons"]},
            store_root=store,
        )

        snapshot: dict[str, Any] = {
            "dataset_snapshot_id": dataset_snapshot_id,
            "dataset_type": dataset_type,
            "provider": provider_name,
            "provider_version": provider_version,
            "interval": interval,
            "bar_policy_version": bar_policy_version,
            "universe_snapshot_id": universe_snapshot_id,
            "start_date": start_date,
            "end_date": end_date,
            "symbol_count": len(symbols_loaded),
            "symbols": symbols_loaded,
            "storage_uri": dataset_uri(dataset_snapshot_id),
            "canonical_format": "parquet.daily_ohlcv.v1",
            "row_count": len(canonical_rows),
            "quality_status": quality_report["quality_status"],
            "quality_report_uri": f"{dataset_uri(dataset_snapshot_id)}/quality_report.json",
            "coverage_report_uri": f"{dataset_uri(dataset_snapshot_id)}/coverage_report.json",
            "content_hash": "",
            "source_hash": sha256_hex({"universe_content_hash": universe["content_hash"], "raw_file_hashes": raw_file_hashes}),
            "created_at": started_at,
            "created_by": created_by,
            "schema_version": SCHEMA_VERSION,
            "canonical_file_hash": canonical_file_hash,
            "coverage_report_hash": coverage_report_hash,
            "adjustment_policy": effective_adjustment_policy,
            "provider_notes": {
                "provider_adjustment_policy": adjustment_policy,
                "effective_adjustment_policy": effective_adjustment_policy,
                "raw_adjustment_policies": sorted(raw_adjustment_policies),
                "adj_close_is_provider_adjusted": effective_adjustment_policy != "unadjusted_close_as_adj_close",
            },
        }
        snapshot["content_hash"] = _dataset_content_hash(snapshot)
        validate_contract("dataset_snapshot", snapshot)
        dataset_snapshot_path = dataset_dir / "dataset_snapshot.json"
        write_json(dataset_snapshot_path, snapshot, overwrite=False)
        dataset_snapshot_hash = file_sha256(dataset_snapshot_path)

        manifest = {
            "dataset_snapshot_id": dataset_snapshot_id,
            "universe_snapshot_id": universe_snapshot_id,
            "provider": provider_name,
            "provider_version": provider_version,
            "interval": interval,
            "bar_policy_version": bar_policy_version,
            "start_date": start_date,
            "end_date": end_date,
            "symbols_requested": symbols_requested,
            "symbols_loaded": symbols_loaded,
            "symbols_missing": quality_report["symbols_missing"],
            "row_count": len(canonical_rows),
            "raw_file_hashes": raw_file_hashes,
            "canonical_file_hash": canonical_file_hash,
            "quality_report_hash": quality_report_hash,
            "coverage_report_hash": coverage_report_hash,
            "dataset_snapshot_hash": dataset_snapshot_hash,
            "created_at": started_at,
            "created_by": created_by,
            "python_version": sys.version,
            "package_versions": _package_versions(),
            "command": command,
            "fetch_errors": fetch_errors,
            "adjustment_policy": effective_adjustment_policy,
            "allow_missing_symbols": allow_missing_symbols,
            "provider_notes": snapshot["provider_notes"],
            "schema_version": "ohlcv_dataset_manifest.v1",
        }
        manifest_path = dataset_dir / "manifest.json"
        write_json(manifest_path, manifest, overwrite=False)

        registry_row = append_dataset_registry_entry(snapshot, store_root=store)
        write_audit_event(
            actor=created_by,
            entity_type="dataset_snapshot",
            entity_id=dataset_snapshot_id,
            action="ohlcv_dataset_build_completed",
            new_state_hash=snapshot["content_hash"],
            reason="Completed immutable daily OHLCV dataset snapshot build.",
            metadata={"registry_row": registry_row, "quality_status": snapshot["quality_status"]},
            store_root=store,
        )
        return {
            "dataset_snapshot": snapshot,
            "manifest": manifest,
            "quality_report": quality_report,
            "coverage_report": coverage_report,
            "registry_row": registry_row,
        }
    except Exception as exc:
        write_audit_event(
            actor=created_by,
            entity_type="dataset_snapshot_build",
            entity_id=universe_snapshot_id,
            action="ohlcv_dataset_build_failed",
            reason=str(exc),
            metadata={
                "provider": provider_name,
                "interval": interval,
                "bar_policy_version": bar_policy_version,
                "fetch_errors": fetch_errors,
            },
            store_root=store,
        )
        raise
