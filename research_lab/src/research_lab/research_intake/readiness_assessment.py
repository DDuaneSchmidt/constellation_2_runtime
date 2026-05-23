from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.events.event_definitions import SUPPORTED_EVENT_TYPES
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout

try:
    from ops.aegis.research_lab.research_data_readiness_v1 import governed_market_data_availability_v1
except Exception:  # pragma: no cover - research_lab can run without the Aegis ops package.
    governed_market_data_availability_v1 = None  # type: ignore[assignment]

SCHEMA_VERSION = "research_readiness_assessment.v1"
DATA_STATUSES = {"available", "partially_available", "missing_or_future", "unknown"}
SAMPLE_STATUSES = {"sufficient_likely", "insufficient_likely", "unknown", "not_checked"}
BREADTH_REQUIRED_FAMILIES = {"breadth_collapse", "breadth_recovery"}
MACRO_REQUIRED_FAMILIES = {"macro_headline_shock", "rate_shock", "credit_stress"}



def _use_governed_market_data(store: Path) -> bool:
    if os.environ.get("AEGIS_TRUTH_ROOT"):
        return True
    try:
        repo_store = (Path(__file__).resolve().parents[3] / "research_store").resolve()
        return store.resolve() == repo_store
    except Exception:
        return False


def _symbols_from_snapshot(snapshot: dict[str, Any]) -> list[str]:
    raw = snapshot.get("symbols") or snapshot.get("symbols_loaded") or []
    out: set[str] = set()
    for item in raw:
        if isinstance(item, dict):
            symbol = item.get("symbol")
        else:
            symbol = item
        text = str(symbol or "").strip().upper()
        if text:
            out.add(text)
    return sorted(out)


def _dataset_candidates(store: Path, required_symbols: list[str]) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    required = set(required_symbols)
    candidates: list[dict[str, Any]] = []
    available_union: set[str] = set()
    for row in read_jsonl(store / "registries" / "dataset_snapshots.jsonl"):
        dataset_id = str(row.get("dataset_snapshot_id") or "")
        if not dataset_id:
            continue
        manifest_path = store / "datasets" / dataset_id / "manifest.json"
        snapshot_path = store / "datasets" / dataset_id / "dataset_snapshot.json"
        manifest: dict[str, Any] = {}
        if manifest_path.exists():
            manifest = read_json(manifest_path)
        elif snapshot_path.exists():
            manifest = read_json(snapshot_path)
        quality = str(row.get("quality_status") or manifest.get("quality_status") or "unknown")
        row_count = int(row.get("row_count") or manifest.get("row_count") or 0)
        symbols = sorted({str(sym).upper() for sym in (manifest.get("symbols_loaded") or []) if str(sym).strip()})
        if not symbols and quality in {"pass", "pass_with_warnings"} and row_count > 0:
            symbols = _symbols_from_snapshot({"symbols": manifest.get("symbols") or row.get("symbols") or []})
        if quality not in {"pass", "pass_with_warnings"} or row_count <= 0:
            symbols = []
        overlap = sorted(required.intersection(symbols))
        if overlap:
            available_union.update(overlap)
            candidates.append(
                {
                    "dataset_snapshot_id": dataset_id,
                    "quality_status": quality,
                    "available_symbols": overlap,
                    "missing_symbols": sorted(required.difference(symbols)),
                    "symbol_count": row.get("symbol_count") or manifest.get("symbol_count") or len(symbols),
                    "start_date": row.get("start_date") or manifest.get("start_date") or "",
                    "end_date": row.get("end_date") or manifest.get("end_date") or "",
                }
            )
    return candidates, sorted(available_union), sorted(required.difference(available_union))


def _universe_candidates(store: Path, required_symbols: list[str]) -> list[dict[str, Any]]:
    required = set(required_symbols)
    candidates: list[dict[str, Any]] = []
    for row in read_jsonl(store / "registries" / "universe_snapshots.jsonl"):
        universe_id = str(row.get("universe_snapshot_id") or "")
        if not universe_id:
            continue
        path = store / "universes" / universe_id / "universe_snapshot.json"
        if not path.exists():
            continue
        snapshot = read_json(path)
        symbols = set(_symbols_from_snapshot(snapshot))
        overlap = sorted(required.intersection(symbols))
        if overlap:
            candidates.append(
                {
                    "universe_snapshot_id": universe_id,
                    "universe_name": snapshot.get("universe_name") or row.get("universe_name") or "",
                    "available_symbols": overlap,
                    "missing_symbols": sorted(required.difference(symbols)),
                    "symbol_count": snapshot.get("symbol_count") or len(symbols),
                    "data_readiness_note": "universe_definition_only_not_loaded_market_data",
                }
            )
    return candidates


def _cost_model_available(store: Path) -> bool:
    return bool(read_jsonl(store / "registries" / "cost_model_snapshots.jsonl"))


def _latest_registry_artifact_available(store: Path, registry_name: str, artifact_root: str, artifact_key: str, filename: str) -> bool:
    for row in reversed(read_jsonl(store / "registries" / registry_name)):
        artifact_id = str(row.get(artifact_key) or "")
        if artifact_id and (store / artifact_root / artifact_id / filename).exists():
            return True
    return False


def _breadth_snapshot_available(store: Path) -> bool:
    return _latest_registry_artifact_available(store, "breadth_snapshots.jsonl", "breadth", "breadth_snapshot_id", "breadth_snapshot.json")


def _macro_event_calendar_available(store: Path) -> bool:
    return _latest_registry_artifact_available(store, "macro_event_calendar_snapshots.jsonl", "macro_events", "macro_event_calendar_snapshot_id", "macro_event_calendar_snapshot.json")


def _event_definition_supported(proposal: dict[str, Any]) -> bool:
    definitions = (proposal.get("proposed_event_definition") or {}).get("default_event_definitions") or []
    if not definitions:
        return False
    return all(str(item.get("type") or "") in SUPPORTED_EVENT_TYPES for item in definitions)


def _forward_windows_supported(proposal: dict[str, Any]) -> bool:
    windows = proposal.get("proposed_forward_windows") or []
    return bool(windows) and all(isinstance(window, int) and window > 0 for window in windows)


def _regime_dimensions_supported(proposal: dict[str, Any]) -> bool:
    return isinstance(proposal.get("proposed_regime_dimensions") or [], list)


def recompute_research_readiness_assessment_hash(assessment: dict[str, Any]) -> str:
    return content_hash(assessment, exclude={"research_readiness_assessment_id", "assessed_at", "content_hash"}, sort_lists=False)


def build_research_readiness_assessment(*, proposal: dict[str, Any], store_root: Path | None = None, assessed_by: str = "Aegis", assessed_at: str | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    event_family_id = str(proposal["event_family_id"])
    required_symbols = sorted({str(sym).upper() for sym in proposal.get("proposed_universe") or [] if str(sym).strip()})
    dataset_candidates, available_symbols, missing_symbols = _dataset_candidates(store, required_symbols)
    market_availability = (
        governed_market_data_availability_v1(day_utc=str((assessed_at or utc_now_iso())[:10]), required_symbols=required_symbols)
        if governed_market_data_availability_v1 and required_symbols and _use_governed_market_data(store)
        else {}
    )
    market_available_symbols = sorted({str(sym).upper() for sym in market_availability.get("available_symbols") or []})
    market_missing_symbols = sorted({str(sym).upper() for sym in market_availability.get("missing_symbols") or []})
    market_stale_symbols = sorted({str(sym).upper() for sym in market_availability.get("stale_symbols") or []})
    if market_availability.get("ready") is True:
        available_symbols = market_available_symbols
        missing_symbols = []
    elif market_available_symbols:
        available_symbols = sorted(set(available_symbols) | set(market_available_symbols))
        missing_symbols = sorted((set(required_symbols) - set(available_symbols)) | set(market_missing_symbols) | set(market_stale_symbols))
    universe_candidates = _universe_candidates(store, required_symbols)
    event_supported = _event_definition_supported(proposal)
    windows_supported = _forward_windows_supported(proposal)
    regime_supported = _regime_dimensions_supported(proposal)
    cost_available = _cost_model_available(store)
    requires_breadth = event_family_id in BREADTH_REQUIRED_FAMILIES
    requires_macro = event_family_id in MACRO_REQUIRED_FAMILIES
    breadth_available = _breadth_snapshot_available(store)
    macro_available = _macro_event_calendar_available(store)
    if missing_symbols and available_symbols:
        data_status = "partially_available"
    elif missing_symbols:
        data_status = "missing_or_future"
    elif required_symbols:
        data_status = "available"
    else:
        data_status = str(proposal.get("data_requirement_status") or "unknown")
    if data_status not in DATA_STATUSES:
        data_status = "unknown"
    blocking: list[str] = []
    warnings: list[str] = []
    if missing_symbols:
        blocking.append("missing_required_symbols")
    if market_stale_symbols:
        blocking.append("stale_intraday_market_data")
    if requires_breadth and not breadth_available:
        blocking.append("missing_breadth_snapshot")
    if requires_macro and not macro_available:
        blocking.append("missing_macro_event_calendar")
    if not event_supported:
        blocking.append("unsupported_event_definition")
    if not windows_supported:
        blocking.append("unsupported_forward_windows")
    if not cost_available:
        blocking.append("missing_cost_model")
    if not regime_supported:
        warnings.append("regime_dimensions_not_checked")
    if universe_candidates and missing_symbols:
        warnings.append("required_symbols_exist_in_universe_definition_but_not_loaded_dataset")
    ready = data_status in {"available", "partially_available"} and event_supported and windows_supported and cost_available and not blocking
    if ready:
        next_actions = ["accept_for_research"]
    else:
        next_actions = []
        if "missing_required_symbols" in blocking:
            next_actions.append("enable_required_data")
        if "missing_breadth_snapshot" in blocking:
            next_actions.append("build_breadth_snapshot")
        if "missing_macro_event_calendar" in blocking:
            next_actions.append("import_macro_event_calendar")
        next_actions.append("watchlist")
    payload = {
        "research_readiness_assessment_id": "",
        "hypothesis_proposal_id": str(proposal["hypothesis_proposal_id"]),
        "event_family_id": event_family_id,
        "assessed_at": assessed_at or utc_now_iso(),
        "assessed_by": str(assessed_by or "Aegis"),
        "data_requirement_status": data_status,
        "required_symbols": required_symbols,
        "available_symbols": available_symbols,
        "missing_symbols": missing_symbols,
        "dataset_snapshot_candidates": dataset_candidates,
        "universe_snapshot_candidates": universe_candidates,
        "market_data_availability": market_availability,
        "required_data_mode": market_availability.get("required_data_mode") or "INTRADAY_OPERATIONAL",
        "intraday_market_data_available": bool(market_availability.get("ready")) if market_availability else False,
        "final_eod_required": bool(market_availability.get("final_eod_required")) if market_availability else False,
        "final_eod_certification_status": market_availability.get("final_eod_certification_status") or "",
        "operator_data_status": market_availability.get("operator_status") or ("Intraday market data available." if ready else "Waiting for current intraday market data."),
        "requires_breadth_snapshot": requires_breadth,
        "breadth_snapshot_available": breadth_available,
        "requires_macro_event_calendar": requires_macro,
        "macro_event_calendar_available": macro_available,
        "event_definition_supported": event_supported,
        "forward_windows_supported": windows_supported,
        "regime_dimensions_supported": regime_supported,
        "cost_model_available": cost_available,
        "sample_size_estimate_status": "not_checked",
        "blocking_items": blocking,
        "warnings": warnings,
        "ready_for_research": ready,
        "next_allowed_actions": next_actions,
        "schema_version": SCHEMA_VERSION,
        "research_label": "RESEARCH_ONLY",
        "content_hash": "",
    }
    fingerprint = recompute_research_readiness_assessment_hash(payload)
    payload["research_readiness_assessment_id"] = f"rra_{short_hash(content_hash({'fingerprint': fingerprint, 'assessed_at': payload['assessed_at']}), 16)}"
    payload["content_hash"] = fingerprint
    validate_research_readiness_assessment(payload)
    return payload


def validate_research_readiness_assessment(assessment: dict[str, Any]) -> None:
    validate_contract("research_readiness_assessment", assessment)
    if assessment.get("data_requirement_status") not in DATA_STATUSES:
        raise ValueError("invalid data_requirement_status")
    if assessment.get("sample_size_estimate_status") not in SAMPLE_STATUSES:
        raise ValueError("invalid sample_size_estimate_status")
    actual = recompute_research_readiness_assessment_hash(assessment)
    if actual != assessment.get("content_hash"):
        raise ValueError(f"ResearchReadinessAssessment content_hash mismatch: expected {assessment.get('content_hash')}, got {actual}")
