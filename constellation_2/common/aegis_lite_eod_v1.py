from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
OVERLAP_REVIEW_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_edge_overlap_review.v1.schema.json"
EOD_REPORT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_lite_eod_report.v1.schema.json"
EXECUTABLE_RECOMMENDATIONS = {"approve", "approve_reduced_size"}
SUPPORTED_MANUAL_TRADE_CLASSES = {
    "LONG_EQUITY",
    "SHORT_EQUITY",
    "LONG_CALL_OPTION",
    "LONG_PUT_OPTION",
    "ETF_ROTATION_PAIR",
}
BLOCKING_RECOMMENDATIONS = {
    "block_due_to_duplicate_edge",
    "block_due_to_concentration",
    "manual_review_required",
}
CRITICAL_CANDIDATE_FIELDS = (
    "symbol",
    "direction",
    "instrument_type",
    "entry_reference_price",
    "stop_price",
    "stop_logic",
    "risk_per_trade",
    "sizing_guidance",
)


def now_utc_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sleeve_edge_overlap_review_path_v1(*, truth_root: Path, day_utc: str, run_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "sleeve_edge_overlap_review_v1"
        / day_utc
        / _safe_run_id(run_id)
        / "sleeve_edge_overlap_review.v1.json"
    )


def aegis_lite_eod_report_path_v1(*, truth_root: Path, day_utc: str, run_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "aegis_lite_eod_report_v1"
        / day_utc
        / _safe_run_id(run_id)
        / "aegis_lite_eod_report.v1.json"
    )


def validate_sleeve_edge_overlap_review_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, OVERLAP_REVIEW_SCHEMA_RELPATH)


def validate_aegis_lite_eod_report_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, EOD_REPORT_SCHEMA_RELPATH)


def write_sleeve_edge_overlap_review_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_sleeve_edge_overlap_review_v1(payload)
    path = sleeve_edge_overlap_review_path_v1(
        truth_root=truth_root,
        day_utc=str(payload["day_utc"]),
        run_id=str(payload["run_id"]),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def write_aegis_lite_eod_report_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_aegis_lite_eod_report_v1(payload)
    path = aegis_lite_eod_report_path_v1(
        truth_root=truth_root,
        day_utc=str(payload["day_utc"]),
        run_id=str(payload["run_id"]),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def sha256_file_v1(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_ref_v1(path: str | Path, *, artifact_type: str) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    return {
        "artifact_type": artifact_type,
        "path": str(resolved),
        "exists": resolved.exists(),
        "sha256": sha256_file_v1(resolved),
    }


def normalize_trade_candidate_v1(raw: dict[str, Any], *, ordinal: int = 1) -> dict[str, Any]:
    candidate_id = _text(raw.get("candidate_id")) or f"CANDIDATE_{ordinal:03d}"
    reason_codes = _string_list(raw.get("reason_codes"))
    blockers: list[str] = []
    suggested_quantity = _int(raw.get("suggested_quantity"))
    if not _text(raw.get("entry_reference_price")):
        blockers.append("ENTRY_REFERENCE_MISSING")
    if not _text(raw.get("stop_price")) or not _text(raw.get("stop_logic")):
        blockers.append("STOP_RISK_MISSING")
    if not _text(raw.get("risk_per_trade")):
        blockers.append("RISK_PER_TRADE_MISSING")
    if not _text(raw.get("sizing_guidance")):
        blockers.append("SIZING_GUIDANCE_MISSING")
    if suggested_quantity <= 0:
        blockers.append("REQUESTED_QUANTITY_MISSING")
    for field_name in ("symbol", "direction", "instrument_type"):
        if not _text(raw.get(field_name)):
            blockers.append(f"{field_name.upper()}_MISSING")
    instrument_type = _text(raw.get("instrument_type") or raw.get("exposure") or "UNKNOWN").upper()
    if instrument_type in {"EQUITY", "LONG_EQUITY", "EQUITY_SPOT"}:
        instrument_type = "EQUITY_SPOT" if instrument_type == "EQUITY_SPOT" else "LONG_EQUITY"
    direction = _text(raw.get("direction") or "UNKNOWN").upper()
    trade_class = _trade_class_from_candidate(instrument_type=instrument_type, direction=direction)
    if trade_class not in SUPPORTED_MANUAL_TRADE_CLASSES:
        blockers.append("UNSUPPORTED_MANUAL_EXECUTION")
    return {
        "candidate_id": candidate_id,
        "sleeve_id": _text(raw.get("sleeve_id") or raw.get("sleeve_ownership") or "UNKNOWN"),
        "symbol": _text(raw.get("symbol") or raw.get("symbol_or_pair")).upper(),
        "direction": direction,
        "instrument_type": instrument_type,
        "trade_class": trade_class,
        "entry_reference_price": _text(raw.get("entry_reference_price")),
        "suggested_quantity": suggested_quantity,
        "sizing_guidance": _text(raw.get("sizing_guidance")),
        "stop_price": _text(raw.get("stop_price")),
        "stop_logic": _text(raw.get("stop_logic")),
        "risk_per_trade": _text(raw.get("risk_per_trade")),
        "sleeve_ownership": _text(raw.get("sleeve_ownership") or raw.get("sleeve_id") or "UNKNOWN"),
        "confidence": _text(raw.get("confidence") or raw.get("conviction") or "UNKNOWN"),
        "conviction": _text(raw.get("conviction") or raw.get("confidence") or "UNKNOWN"),
        "reason_codes": _dedupe([*reason_codes, *blockers]),
        "governance_notes": _string_list(raw.get("governance_notes")),
        "edge_family": _text(raw.get("edge_family") or "UNKNOWN"),
        "thesis_id": _text(raw.get("thesis_id") or candidate_id),
        "shared_risk_tags": _string_list(raw.get("shared_risk_tags")),
        "correlated_symbols": sorted({item.upper() for item in _string_list(raw.get("correlated_symbols"))}),
        "regime_dependency": _text(raw.get("regime_dependency") or "UNKNOWN"),
        "macro_sensitivity": _text(raw.get("macro_sensitivity") or "UNKNOWN"),
        "volatility_liquidity_dependency": _text(raw.get("volatility_liquidity_dependency") or "UNKNOWN"),
        "source_artifact_refs": _artifact_refs(raw.get("source_artifact_refs") or raw.get("source_artifact_lineage")),
        "executable_status": "NON_EXECUTABLE" if blockers else "EXECUTABLE",
        "blockers": blockers,
    }


def build_sleeve_edge_overlap_review_v1(
    *,
    day_utc: str,
    run_id: str,
    generated_at_utc: str,
    candidates: list[dict[str, Any]],
    source_artifact_lineage: list[dict[str, Any]] | None = None,
    concentration_symbol_limit: int = 1,
) -> dict[str, Any]:
    normalized = [normalize_trade_candidate_v1(candidate, ordinal=idx + 1) for idx, candidate in enumerate(candidates)]
    normalized.sort(key=lambda row: (row["sleeve_id"], row["symbol"], row["candidate_id"]))
    edge_groups = _edge_groups(normalized)
    duplicate_groups = _duplicate_thesis_groups(normalized)
    concentration_warnings = _concentration_warnings(normalized, concentration_symbol_limit=concentration_symbol_limit)
    notes = _candidate_overlap_notes(
        normalized,
        duplicate_groups=duplicate_groups,
        concentration_warnings=concentration_warnings,
        edge_groups=edge_groups,
    )
    reason_codes: list[str] = []
    if any(note["governance_recommendation"] in BLOCKING_RECOMMENDATIONS for note in notes):
        reason_codes.append("CANDIDATE_MANUAL_REVIEW_REQUIRED")
    if any(note["concentration_warning_flag"] for note in notes):
        reason_codes.append("PORTFOLIO_CONCENTRATION_WARNING")
    if any(note["duplicate_thesis_flag"] for note in notes):
        reason_codes.append("DUPLICATE_THESIS_DETECTED")
    payload = {
        "schema_id": "sleeve_edge_overlap_review",
        "schema_version": "v1",
        "artifact_id": "sleeve_edge_overlap_review_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "status": "REVIEW_REQUIRED" if "CANDIDATE_MANUAL_REVIEW_REQUIRED" in reason_codes else "PASS",
        "distinct_edge_count": len(edge_groups),
        "edge_groups": edge_groups,
        "shared_risk_groups": _shared_risk_groups(normalized),
        "duplicate_thesis_groups": duplicate_groups,
        "correlated_exposure_groups": _correlated_exposure_groups(normalized),
        "portfolio_concentration_warnings": concentration_warnings,
        "governance_adjustments": [
            {
                "candidate_id": note["candidate_id"],
                "recommendation": note["governance_recommendation"],
                "reason_codes": note["reason_codes"],
            }
            for note in notes
        ],
        "candidate_level_overlap_notes": notes,
        "source_artifact_lineage": source_artifact_lineage or [],
        "reason_codes": sorted(set(reason_codes)),
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_aegis_lite_eod_report_v1(
    *,
    day_utc: str,
    run_id: str,
    generated_at_utc: str,
    truth_root: Path,
    candidates: list[dict[str, Any]],
    overlap_review: dict[str, Any],
    market_session: dict[str, Any] | None = None,
    market_regime_state: dict[str, Any] | None = None,
    data_freshness_status: dict[str, Any] | None = None,
    governance_status: dict[str, Any] | None = None,
    sleeve_outputs: list[dict[str, Any]] | None = None,
    sleeve_performance_summary: list[dict[str, Any]] | None = None,
    sandbox_research_notes: list[dict[str, Any]] | None = None,
    operator_notes: str = "",
    source_artifact_lineage: list[dict[str, Any]] | None = None,
    manual_operator_decisions: list[dict[str, Any]] | None = None,
    manual_execution_events: list[dict[str, Any]] | None = None,
    portfolio_position_snapshot: dict[str, Any] | None = None,
    protective_order_snapshot: dict[str, Any] | None = None,
    trade_outcome_attribution: dict[str, Any] | None = None,
    edge_cluster: dict[str, Any] | None = None,
    operator_execution_queue: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = [normalize_trade_candidate_v1(candidate, ordinal=idx + 1) for idx, candidate in enumerate(candidates)]
    notes_by_id = {
        str(note.get("candidate_id")): note
        for note in overlap_review.get("candidate_level_overlap_notes", [])
        if isinstance(note, dict)
    }
    report_candidates = [_report_candidate(row, notes_by_id.get(row["candidate_id"], {})) for row in normalized]
    gates = [
        _data_integrity_gate(data_freshness_status or {}),
        _governance_integrity_gate(report_candidates, governance_status or {}, overlap_review),
        _report_completeness_gate(report_candidates, overlap_review),
    ]
    feedback = _feedback_summary(
        manual_operator_decisions=manual_operator_decisions or [],
        manual_execution_events=manual_execution_events or [],
        portfolio_position_snapshot=portfolio_position_snapshot or {},
        protective_order_snapshot=protective_order_snapshot or {},
        trade_outcome_attribution=trade_outcome_attribution or {},
        edge_cluster=edge_cluster or {},
        operator_execution_queue=operator_execution_queue or {},
    )
    feedback["warnings"] = _dedupe([*feedback["warnings"], *_position_concentration_warnings(report_candidates, feedback["open_manual_positions"])])
    warnings = _dedupe([*_report_warnings(report_candidates, overlap_review, gates), *feedback["warnings"]])
    blockers = _dedupe([*_report_blockers(report_candidates, gates), *feedback["blockers"]])
    manual_ready = (
        not blockers
        and all(gate["status"] == "PASS" for gate in gates)
        and feedback["operator_execution_queue_ready"]
        and any(
            row["executable_status"] == "EXECUTABLE" and row["governance_adjusted_status"] in EXECUTABLE_RECOMMENDATIONS
            for row in report_candidates
        )
    )
    out_path = aegis_lite_eod_report_path_v1(truth_root=truth_root, day_utc=day_utc, run_id=run_id)
    overlap_path = sleeve_edge_overlap_review_path_v1(truth_root=truth_root, day_utc=day_utc, run_id=run_id)
    payload = {
        "schema_id": "aegis_lite_eod_report",
        "schema_version": "v1",
        "artifact_id": "aegis_lite_eod_report_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "truth_root": str(Path(truth_root).resolve()),
        "artifact_path": str(out_path),
        "operating_model": {
            "phase": "AEGIS_LITE",
            "near_term_product": "EOD_MANUAL_TRADE_REPORT",
            "manual_execution_only": True,
            "ib_automation_status": "DEFERRED",
            "broker_submit_required": False,
            "autonomous_order_routing_allowed": False,
        },
        "market_session": market_session or {"session_label": "REGULAR", "target_operating_window_et": "15:30-15:45"},
        "market_regime_state": market_regime_state or {"status": "UNKNOWN", "reason_codes": ["REGIME_INPUT_MISSING"]},
        "data_freshness_status": data_freshness_status or {"status": "BLOCKED", "reason_codes": ["DATA_FRESHNESS_MISSING"]},
        "governance_status": governance_status or {"status": "BLOCKED", "reason_codes": ["GOVERNANCE_STATUS_MISSING"]},
        "gates": gates,
        "sleeve_outputs": sleeve_outputs or [],
        "sleeve_performance_summary": sleeve_performance_summary or [],
        "sandbox_research_notes": sandbox_research_notes or [],
        "selected_trade_candidates": report_candidates,
        "edge_overlap_review": {
            "artifact_type": "sleeve_edge_overlap_review_v1",
            "path": str(overlap_path),
            "sha256": sha256_file_v1(overlap_path),
            "status": str(overlap_review.get("status") or "UNKNOWN"),
            "distinct_edge_count": int(overlap_review.get("distinct_edge_count") or 0),
        },
        "edge_overlap_summary": {
            "distinct_edge_count": int(overlap_review.get("distinct_edge_count") or 0),
            "edge_groups": overlap_review.get("edge_groups") or [],
            "shared_risk_groups": overlap_review.get("shared_risk_groups") or [],
            "duplicate_thesis_groups": overlap_review.get("duplicate_thesis_groups") or [],
            "correlated_exposure_groups": overlap_review.get("correlated_exposure_groups") or [],
            "portfolio_concentration_warnings": overlap_review.get("portfolio_concentration_warnings") or [],
        },
        "open_manual_positions": feedback["open_manual_positions"],
        "missing_stop_warnings": feedback["missing_stop_warnings"],
        "prior_day_manual_decisions": feedback["prior_day_manual_decisions"],
        "manual_execution_events": feedback["manual_execution_events"],
        "current_exposure_by_edge_cluster": feedback["current_exposure_by_edge_cluster"],
        "current_exposure_by_sleeve": feedback["current_exposure_by_sleeve"],
        "manual_execution_queue": feedback["manual_execution_queue"],
        "skipped_candidate_tracking": feedback["skipped_candidate_tracking"],
        "unsupported_manual_execution_warnings": feedback["unsupported_manual_execution_warnings"],
        "performance_summary": feedback["performance_summary"],
        "edge_clusters": feedback["edge_clusters"],
        "manual_execution_checklist": _manual_execution_checklist(),
        "do_not_trade_blockers": blockers,
        "warnings": warnings,
        "operator_notes": operator_notes,
        "source_artifact_lineage": source_artifact_lineage or [],
        "run_receipt": {
            "pipeline": "aegis_lite_eod_pipeline_v1",
            "deterministic": True,
            "replayable": True,
            "audit_archive_path": str(out_path.parent),
            "ib_submit_automation_invoked": False,
            "broker_transmit_control_touched": False,
        },
        "report_status": "BLOCKED" if blockers else ("READY_WITH_WARNINGS" if warnings else "READY"),
        "manual_execution_status": "READY_FOR_MANUAL_ENTRY" if manual_ready else "NOT_READY",
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def read_candidate_input_v1(path: Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, list):
        return {"candidates": payload}
    if not isinstance(payload, dict):
        raise ValueError("AEGIS_LITE_INPUT_NOT_OBJECT_OR_LIST")
    return payload


def _safe_run_id(run_id: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(run_id or "").strip())
    return cleaned or "aegis_lite_eod_v1"


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _trade_class_from_candidate(*, instrument_type: str, direction: str) -> str:
    instrument = str(instrument_type or "").upper()
    side = str(direction or "").upper()
    if instrument in {"LONG_EQUITY", "EQUITY_SPOT"} and side == "SHORT":
        return "SHORT_EQUITY"
    if instrument in {"LONG_EQUITY", "EQUITY_SPOT"}:
        return "LONG_EQUITY"
    if instrument in SUPPORTED_MANUAL_TRADE_CLASSES:
        return instrument
    return instrument or "UNKNOWN"


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [_text(value)] if _text(value) else []
    if isinstance(value, list):
        return [_text(item) for item in value if _text(item)]
    return [_text(value)] if _text(value) else []


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _text(value)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _artifact_refs(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    refs: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            refs.append(
                {
                    "artifact_type": _text(item.get("artifact_type") or item.get("logical_name") or "source"),
                    "path": _text(item.get("path")),
                    "exists": bool(item.get("exists", False)),
                    "sha256": _text(item.get("sha256")),
                }
            )
    return refs


def _edge_key(candidate: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return (
        candidate["edge_family"],
        candidate["direction"],
        ",".join(candidate["shared_risk_tags"]),
        candidate["regime_dependency"],
        candidate["macro_sensitivity"],
        candidate["volatility_liquidity_dependency"],
    )


def _edge_groups(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = {}
    for candidate in candidates:
        grouped.setdefault(_edge_key(candidate), []).append(candidate)
    groups: list[dict[str, Any]] = []
    for idx, (key, rows) in enumerate(sorted(grouped.items(), key=lambda item: item[0]), start=1):
        groups.append(
            {
                "edge_group_id": f"EDGE_{idx:03d}",
                "edge_family": key[0],
                "direction": key[1],
                "shared_risk_tags": key[2].split(",") if key[2] else [],
                "regime_dependency": key[3],
                "macro_sensitivity": key[4],
                "volatility_liquidity_dependency": key[5],
                "candidate_ids": [row["candidate_id"] for row in rows],
            }
        )
    return groups


def _shared_risk_groups(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_tag: dict[str, list[str]] = {}
    for candidate in candidates:
        for tag in candidate["shared_risk_tags"]:
            by_tag.setdefault(tag, []).append(candidate["candidate_id"])
    return [
        {"shared_risk_group_id": f"RISK_{idx:03d}", "risk_tag": tag, "candidate_ids": sorted(set(ids))}
        for idx, (tag, ids) in enumerate(sorted(by_tag.items()), start=1)
        if len(set(ids)) > 1
    ]


def _duplicate_thesis_groups(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_thesis: dict[str, list[str]] = {}
    for candidate in candidates:
        thesis = candidate["thesis_id"]
        if thesis:
            by_thesis.setdefault(thesis, []).append(candidate["candidate_id"])
    return [
        {"duplicate_thesis_group_id": f"THESIS_{idx:03d}", "thesis_id": thesis, "candidate_ids": sorted(ids)}
        for idx, (thesis, ids) in enumerate(sorted(by_thesis.items()), start=1)
        if len(set(ids)) > 1
    ]


def _correlated_exposure_groups(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_symbol: dict[str, set[str]] = {}
    for candidate in candidates:
        for symbol in {candidate["symbol"], *candidate["correlated_symbols"]}:
            if symbol:
                by_symbol.setdefault(symbol, set()).add(candidate["candidate_id"])
    return [
        {"correlated_exposure_group_id": f"CORR_{idx:03d}", "symbol": symbol, "candidate_ids": sorted(ids)}
        for idx, (symbol, ids) in enumerate(sorted(by_symbol.items()), start=1)
        if len(ids) > 1
    ]


def _concentration_warnings(candidates: list[dict[str, Any]], *, concentration_symbol_limit: int) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[str]] = {}
    for candidate in candidates:
        by_symbol.setdefault(candidate["symbol"], []).append(candidate["candidate_id"])
    return [
        {
            "warning_id": f"CONCENTRATION_{idx:03d}",
            "symbol": symbol,
            "candidate_ids": sorted(ids),
            "reason_codes": ["SYMBOL_CONCENTRATION_WARNING"],
        }
        for idx, (symbol, ids) in enumerate(sorted(by_symbol.items()), start=1)
        if symbol and len(set(ids)) > concentration_symbol_limit
    ]


def _candidate_overlap_notes(
    candidates: list[dict[str, Any]],
    *,
    duplicate_groups: list[dict[str, Any]],
    concentration_warnings: list[dict[str, Any]],
    edge_groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    duplicate_ids = {candidate_id for group in duplicate_groups for candidate_id in group["candidate_ids"]}
    concentration_ids = {candidate_id for warning in concentration_warnings for candidate_id in warning["candidate_ids"]}
    group_by_candidate = {
        candidate_id: group["edge_group_id"]
        for group in edge_groups
        for candidate_id in group["candidate_ids"]
    }
    notes: list[dict[str, Any]] = []
    for candidate in candidates:
        reason_codes = list(candidate["reason_codes"])
        duplicate = candidate["candidate_id"] in duplicate_ids
        concentration = candidate["candidate_id"] in concentration_ids
        if candidate["blockers"]:
            recommendation = "manual_review_required"
        elif duplicate:
            recommendation = "prefer_best_candidate_in_group"
            reason_codes.append("DUPLICATE_THESIS_DETECTED")
        elif concentration:
            recommendation = "approve_reduced_size"
            reason_codes.append("SYMBOL_CONCENTRATION_WARNING")
        else:
            recommendation = "approve"
        notes.append(
            {
                "candidate_id": candidate["candidate_id"],
                "sleeve_id": candidate["sleeve_id"],
                "symbol": candidate["symbol"],
                "direction": candidate["direction"],
                "edge_family": candidate["edge_family"],
                "thesis_id": candidate["thesis_id"],
                "shared_risk_tags": candidate["shared_risk_tags"],
                "correlated_symbols": candidate["correlated_symbols"],
                "overlap_group_id": group_by_candidate.get(candidate["candidate_id"], ""),
                "duplicate_thesis_flag": duplicate,
                "concentration_warning_flag": concentration,
                "governance_recommendation": recommendation,
                "reason_codes": _dedupe(reason_codes),
            }
        )
    return notes


def _report_candidate(candidate: dict[str, Any], note: dict[str, Any]) -> dict[str, Any]:
    recommendation = _text(note.get("governance_recommendation")) or "manual_review_required"
    return {
        "candidate_id": candidate["candidate_id"],
        "sleeve_id": candidate["sleeve_id"],
        "symbol": candidate["symbol"],
        "direction": candidate["direction"],
        "instrument_type": candidate["instrument_type"],
        "entry_reference_price": candidate["entry_reference_price"],
        "suggested_quantity": candidate["suggested_quantity"],
        "sizing_guidance": candidate["sizing_guidance"],
        "stop_price": candidate["stop_price"],
        "stop_logic": candidate["stop_logic"],
        "risk_per_trade": candidate["risk_per_trade"],
        "sleeve_ownership": candidate["sleeve_ownership"],
        "confidence": candidate["confidence"],
        "conviction": candidate["conviction"],
        "reason_codes": _dedupe([*candidate["reason_codes"], *_string_list(note.get("reason_codes"))]),
        "governance_notes": candidate["governance_notes"],
        "edge_family": candidate["edge_family"],
        "thesis_id": candidate["thesis_id"],
        "shared_risk_tags": candidate["shared_risk_tags"],
        "correlated_symbols": candidate["correlated_symbols"],
        "edge_overlap": {
            "overlap_group_id": _text(note.get("overlap_group_id")),
            "duplicate_thesis_flag": bool(note.get("duplicate_thesis_flag", False)),
            "concentration_warning_flag": bool(note.get("concentration_warning_flag", False)),
            "governance_recommendation": recommendation,
        },
        "shared_risk_classification": candidate["shared_risk_tags"],
        "portfolio_concentration_warnings": ["SYMBOL_CONCENTRATION_WARNING"]
        if bool(note.get("concentration_warning_flag", False))
        else [],
        "governance_adjusted_status": recommendation,
        "manual_execution": {
            "operator_must_enter_in_ib": True,
            "aegis_broker_submit_required": False,
            "checklist_required": True,
        },
        "executable_status": "NON_EXECUTABLE"
        if candidate["blockers"] or recommendation in BLOCKING_RECOMMENDATIONS
        else "EXECUTABLE",
        "blockers": candidate["blockers"],
        "source_artifact_refs": candidate["source_artifact_refs"],
    }


def _data_integrity_gate(status: dict[str, Any]) -> dict[str, Any]:
    raw_status = _text(status.get("status")).upper()
    passed = raw_status == "PASS"
    return {
        "gate_id": "DATA_INTEGRITY",
        "status": "PASS" if passed else "BLOCKED",
        "reason_codes": _string_list(status.get("reason_codes")) or ([] if passed else [f"DATA_STATUS_NOT_PASS:{raw_status or 'MISSING'}"]),
    }


def _governance_integrity_gate(
    candidates: list[dict[str, Any]],
    status: dict[str, Any],
    overlap_review: dict[str, Any],
) -> dict[str, Any]:
    reason_codes = _string_list(status.get("reason_codes"))
    raw_status = _text(status.get("status")).upper()
    blocked = raw_status != "PASS"
    if blocked and not reason_codes:
        reason_codes.append(f"GOVERNANCE_STATUS_NOT_PASS:{raw_status or 'MISSING'}")
    if any(candidate["executable_status"] != "EXECUTABLE" for candidate in candidates):
        blocked = True
        reason_codes.append("CANDIDATE_NOT_EXECUTABLE")
    if str(overlap_review.get("status") or "").upper() == "REVIEW_REQUIRED":
        reason_codes.append("EDGE_OVERLAP_REVIEW_REQUIRED")
    return {"gate_id": "GOVERNANCE_INTEGRITY", "status": "BLOCKED" if blocked else "PASS", "reason_codes": _dedupe(reason_codes)}


def _report_completeness_gate(candidates: list[dict[str, Any]], overlap_review: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    if not candidates:
        reason_codes.append("NO_TRADE_CANDIDATES")
    if int(overlap_review.get("distinct_edge_count") or 0) < 1 and candidates:
        reason_codes.append("EDGE_OVERLAP_REVIEW_INCOMPLETE")
    for candidate in candidates:
        if any(not _text(candidate.get(field)) for field in CRITICAL_CANDIDATE_FIELDS):
            reason_codes.append("CANDIDATE_REPORT_FIELD_MISSING")
            break
    return {"gate_id": "REPORT_COMPLETENESS", "status": "BLOCKED" if reason_codes else "PASS", "reason_codes": _dedupe(reason_codes)}


def _manual_execution_checklist() -> list[dict[str, Any]]:
    items = [
        ("CHECK_SYMBOL", "Confirm symbol and sleeve ownership in IB before entry."),
        ("CHECK_DIRECTION", "Confirm direction and instrument type."),
        ("CHECK_ENTRY", "Use the report entry reference price as the manual entry reference."),
        ("CHECK_SIZE", "Use suggested quantity or sizing guidance; do not exceed governance adjustment."),
        ("CHECK_STOP", "Enter or record the stop price and stop logic before trade capture."),
        ("CHECK_WARNINGS", "Review duplicate, correlation, concentration, and do-not-trade warnings."),
        ("CAPTURE_MANUAL_FILL", "Capture any manual fill back into governed trade records."),
        ("NO_AEGIS_TRANSMIT", "Do not use Aegis broker transmit automation in Lite phase."),
    ]
    return [{"check_id": check_id, "label": label, "status": "REQUIRED"} for check_id, label in items]


def _report_warnings(candidates: list[dict[str, Any]], overlap_review: dict[str, Any], gates: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    warnings.extend(_string_list(overlap_review.get("reason_codes")))
    for candidate in candidates:
        if candidate["edge_overlap"]["duplicate_thesis_flag"]:
            warnings.append(f"{candidate['candidate_id']}:DUPLICATE_THESIS_DETECTED")
        if candidate["edge_overlap"]["concentration_warning_flag"]:
            warnings.append(f"{candidate['candidate_id']}:SYMBOL_CONCENTRATION_WARNING")
    for gate in gates:
        if gate["status"] != "BLOCKED":
            warnings.extend([str(code) for code in gate["reason_codes"]])
    return _dedupe(warnings)


def _report_blockers(candidates: list[dict[str, Any]], gates: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    for gate in gates:
        if gate["status"] == "BLOCKED":
            blockers.extend([f"{gate['gate_id']}:{code}" for code in gate["reason_codes"]])
    for candidate in candidates:
        for code in candidate["blockers"]:
            blockers.append(f"{candidate['candidate_id']}:{code}")
    return _dedupe(blockers)


def _feedback_summary(
    *,
    manual_operator_decisions: list[dict[str, Any]],
    manual_execution_events: list[dict[str, Any]],
    portfolio_position_snapshot: dict[str, Any],
    protective_order_snapshot: dict[str, Any],
    trade_outcome_attribution: dict[str, Any],
    edge_cluster: dict[str, Any],
    operator_execution_queue: dict[str, Any],
) -> dict[str, Any]:
    positions = _objects(portfolio_position_snapshot.get("open_positions"))
    missing_stop_warnings = _string_list(protective_order_snapshot.get("missing_stop_warnings"))
    unsupported = _string_list(operator_execution_queue.get("unsupported_manual_execution_warnings"))
    decisions = _objects(manual_operator_decisions)
    attributions = _objects(trade_outcome_attribution.get("attributions"))
    queue_rows = _objects(operator_execution_queue.get("execution_queue"))
    protective_rows = _objects(protective_order_snapshot.get("protective_orders"))
    skipped = [
        {
            "candidate_id": str(row.get("candidate_id") or ""),
            "decision": str(row.get("decision") or ""),
            "decision_reason_codes": _string_list(row.get("decision_reason_codes")),
        }
        for row in decisions
        if str(row.get("decision") or "").upper() in {"SKIPPED", "WATCHLIST", "REJECTED"}
    ]
    warnings = [
        *[f"MISSING_PROTECTIVE_STOP:{item}" for item in missing_stop_warnings],
        *[f"UNSUPPORTED_MANUAL_EXECUTION:{item}" for item in unsupported],
    ]
    blockers = _feedback_blockers(
        positions=positions,
        protective_rows=protective_rows,
        missing_stop_warnings=missing_stop_warnings,
        unsupported_warnings=unsupported,
        queue_rows=queue_rows,
    )
    return {
        "open_manual_positions": positions,
        "missing_stop_warnings": missing_stop_warnings,
        "prior_day_manual_decisions": decisions,
        "manual_execution_events": _objects(manual_execution_events),
        "current_exposure_by_edge_cluster": _objects(portfolio_position_snapshot.get("exposure_by_edge_cluster")),
        "current_exposure_by_sleeve": _objects(portfolio_position_snapshot.get("exposure_by_sleeve")),
        "manual_execution_queue": queue_rows,
        "skipped_candidate_tracking": skipped,
        "unsupported_manual_execution_warnings": unsupported,
        "performance_summary": _performance_summary(attributions),
        "edge_clusters": _objects(edge_cluster.get("edge_clusters")),
        "warnings": _dedupe(warnings),
        "blockers": blockers,
        "operator_execution_queue_ready": bool(queue_rows) and not any(blocker.startswith("OPERATOR_QUEUE") for blocker in blockers),
    }


def _feedback_blockers(
    *,
    positions: list[dict[str, Any]],
    protective_rows: list[dict[str, Any]],
    missing_stop_warnings: list[str],
    unsupported_warnings: list[str],
    queue_rows: list[dict[str, Any]],
) -> list[str]:
    blockers: list[str] = []
    blockers.extend([f"MISSING_PROTECTIVE_STOP:{item}" for item in missing_stop_warnings])
    blockers.extend([f"UNSUPPORTED_MANUAL_EXECUTION:{item}" for item in unsupported_warnings])
    if positions and not protective_rows:
        blockers.append("PROTECTIVE_ORDER_SNAPSHOT_MISSING_FOR_OPEN_POSITIONS")
    protected_keys = {
        str(row.get("position_id") or row.get("candidate_id") or "").strip()
        for row in protective_rows
        if str(row.get("position_id") or row.get("candidate_id") or "").strip()
    }
    for row in positions:
        position_key = str(row.get("position_id") or row.get("candidate_id") or "").strip()
        if position_key and protective_rows and position_key not in protected_keys:
            blockers.append(f"PROTECTIVE_ORDER_MISSING_FOR_OPEN_POSITION:{position_key}")
    for row in protective_rows:
        status = str(row.get("protection_status") or "").upper()
        if status != "PROTECTED" or bool(row.get("operator_action_required")):
            blockers.append(f"UNPROTECTED_OPEN_POSITION:{row.get('candidate_id') or row.get('position_id') or 'UNKNOWN'}:{status or 'UNKNOWN'}")
    if not queue_rows:
        blockers.append("OPERATOR_QUEUE_MISSING")
    for row in queue_rows:
        candidate_id = str(row.get("candidate_id") or "UNKNOWN")
        status = str(row.get("queue_status") or "").upper()
        recipe = str(row.get("manual_execution_recipe") or "").strip()
        required_orders = [str(item).strip() for item in row.get("required_orders", []) if str(item).strip()]
        confirmations = [str(item).strip() for item in row.get("operator_confirmations", []) if str(item).strip()]
        if status != "READY_FOR_MANUAL_ENTRY":
            blockers.append(f"OPERATOR_QUEUE_ITEM_NOT_READY:{candidate_id}:{status or 'MISSING'}")
        if not recipe or recipe == "UNSUPPORTED_MANUAL_EXECUTION":
            blockers.append(f"OPERATOR_QUEUE_ITEM_MISSING_RECIPE:{candidate_id}")
        if "PROTECTIVE_STOP" not in required_orders or not bool(row.get("stop_required")):
            blockers.append(f"OPERATOR_QUEUE_ITEM_MISSING_STOP_ORDER:{candidate_id}")
        if not confirmations:
            blockers.append(f"OPERATOR_QUEUE_ITEM_MISSING_CONFIRMATIONS:{candidate_id}")
    return _dedupe(blockers)


def _position_concentration_warnings(candidates: list[dict[str, Any]], positions: list[dict[str, Any]]) -> list[str]:
    open_symbols = {str(row.get("symbol") or "").upper() for row in positions if str(row.get("symbol") or "")}
    warnings: list[str] = []
    for candidate in candidates:
        symbol = str(candidate.get("symbol") or "").upper()
        if symbol and symbol in open_symbols:
            warnings.append(f"{candidate['candidate_id']}:{symbol}:OPEN_POSITION_CONCENTRATION")
    return warnings


def _performance_summary(attributions: list[dict[str, Any]]) -> dict[str, Any]:
    evidence_rows = [
        row
        for row in attributions
        if any(str(item).strip() for item in row.get("model_forward_returns", []) if str(item).strip() != "PENDING_FORWARD_ANALYSIS")
    ]
    if not evidence_rows:
        return {
            "status": "UNAVAILABLE",
            "candidate_count": len(attributions),
            "skipped_candidate_count": 0,
            "reason_codes": ["TRADE_OUTCOME_ATTRIBUTION_MISSING"],
        }
    skipped = [
        row
        for row in attributions
        if str(row.get("operator_execution_quality") or "").upper() in {"SKIPPED", "NOT_IMPLEMENTED"}
        or str(row.get("skipped_trade_outcome") or "")
    ]
    return {
        "status": "AVAILABLE",
        "candidate_count": len(evidence_rows),
        "skipped_candidate_count": len(skipped),
        "reason_codes": [],
        "sleeve_signal_quality": _quality_counts(attributions, "sleeve_signal_quality"),
        "implementation_quality": _quality_counts(attributions, "implementation_quality"),
        "operator_execution_quality": _quality_counts(attributions, "operator_execution_quality"),
    }


def _quality_counts(rows: list[dict[str, Any]], field_name: str) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field_name) or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return [{"status": key, "count": value} for key, value in sorted(counts.items())]


def _objects(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
