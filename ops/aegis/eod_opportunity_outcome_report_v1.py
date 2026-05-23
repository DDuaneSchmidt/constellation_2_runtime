from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional


SCHEMA_VERSION = "eod_opportunity_outcome_report.v1"
CLASSIFIER_VERSION = "eod_opportunity_outcome_classifier.v1"
BUCKET_PRECEDENCE_VERSION = "eod_opportunity_bucket_taxonomy.v1"

PRIMARY_BUCKETS = [
    "BLOCKED_DATA_OR_INPUT",
    "SLEEVE_NOT_RUN",
    "NO_RAW_SIGNAL",
    "RAW_SIGNAL_REJECTED",
    "CANDIDATE_REJECTED",
    "UNSUPPORTED_RESEARCH_OBSERVATION",
    "OPERATOR_ELIGIBLE_OPPORTUNITY",
    "OPERATOR_REVIEWED",
    "MANUAL_EXTERNAL_CAPTURE_RECORDED",
]

STRATEGY_FAILURE_BUCKETS = {
    "NO_RAW_SIGNAL",
    "RAW_SIGNAL_REJECTED",
    "CANDIDATE_REJECTED",
    "UNSUPPORTED_RESEARCH_OBSERVATION",
}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _candidate_id(row: Mapping[str, Any]) -> str:
    return str(row.get("candidate_id") or row.get("id") or "").strip()


def _sleeve_id(row: Mapping[str, Any]) -> str:
    return str(row.get("sleeve_id") or row.get("sleeve") or row.get("sleeve_name") or "").strip()


def _all_sleeve_ids(cockpit: Mapping[str, Any]) -> List[str]:
    ids: set[str] = set()
    opportunities = _safe_dict(cockpit.get("opportunities"))
    for row in _safe_list(opportunities.get("sleeve_run_summary")):
        if isinstance(row, dict) and _sleeve_id(row):
            ids.add(_sleeve_id(row))
    sleeves = _safe_dict(cockpit.get("sleeve_warnings"))
    for bucket in sleeves.values():
        for row in _safe_list(bucket):
            if isinstance(row, dict) and _sleeve_id(row):
                ids.add(_sleeve_id(row))
    for key in sleeves.keys():
        if isinstance(key, str) and key.startswith("C2_"):
            ids.add(key)
    for row in _safe_list(cockpit.get("top_candidates")):
        if isinstance(row, dict) and _sleeve_id(row):
            ids.add(_sleeve_id(row))
    active = _safe_dict(cockpit.get("active_opportunity_projection"))
    manual = _safe_dict(active.get("manual_capture_candidate") or active.get("selected_exposure"))
    if _sleeve_id(manual):
        ids.add(_sleeve_id(manual))
    watchlist = _safe_dict(active.get("suppressed_candidate_watchlist"))
    for row in _safe_list(watchlist.get("candidates")):
        if isinstance(row, dict) and _sleeve_id(row):
            ids.add(_sleeve_id(row))
    for bucket in ["candidates", "research_observations", "needs_evidence_candidates"]:
        for row in _safe_list(active.get(bucket)):
            if isinstance(row, dict) and _sleeve_id(row):
                ids.add(_sleeve_id(row))
    return sorted(ids)


def _run_rows_by_sleeve(cockpit: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    rows: Dict[str, Dict[str, Any]] = {}
    opportunities = _safe_dict(cockpit.get("opportunities"))
    for row in _safe_list(opportunities.get("sleeve_run_summary")):
        if isinstance(row, dict) and _sleeve_id(row):
            rows[_sleeve_id(row)] = row
    return rows


def _candidates_by_sleeve(cockpit: Mapping[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    rows: Dict[str, List[Dict[str, Any]]] = {}
    active = _safe_dict(cockpit.get("active_opportunity_projection"))
    manual = _safe_dict(active.get("manual_capture_candidate") or active.get("selected_exposure"))
    if manual.get("candidate_available") and _sleeve_id(manual):
        rows.setdefault(_sleeve_id(manual), []).append(
            {
                **manual,
                "candidate_id": str(manual.get("selected_exposure_intent_id") or ""),
                "symbol": str(manual.get("symbol") or "").upper(),
                "_projection_bucket": "selected_exposure",
                "blocked_conversion": bool(manual.get("blocker_code")),
            }
        )
    watchlist = _safe_dict(active.get("suppressed_candidate_watchlist"))
    for row in _safe_list(watchlist.get("candidates")):
        if isinstance(row, dict) and _sleeve_id(row):
            rows.setdefault(_sleeve_id(row), []).append({**row, "_projection_bucket": "suppressed_watchlist"})
    for bucket in ["candidates", "research_observations", "needs_evidence_candidates"]:
        for row in _safe_list(active.get(bucket)):
            if isinstance(row, dict) and _sleeve_id(row):
                rows.setdefault(_sleeve_id(row), []).append({**row, "_projection_bucket": bucket})
    for row in _safe_list(cockpit.get("top_candidates")):
        if isinstance(row, dict) and _sleeve_id(row):
            candidate_id = _candidate_id(row)
            existing = {_candidate_id(existing_row) for existing_row in rows.get(_sleeve_id(row), [])}
            if candidate_id not in existing:
                rows.setdefault(_sleeve_id(row), []).append({**row, "_projection_bucket": "top_candidates"})
    return rows


def _manual_capture_count(rows: Iterable[Mapping[str, Any]]) -> int:
    count = 0
    for row in rows:
        state = str(row.get("review_state") or row.get("operator_review_status") or row.get("candidate_status") or "").upper()
        if row.get("manual_capture_recorded") is True or "MANUAL_EXTERNAL_CAPTURE" in state or "CAPTURE" in state:
            count += 1
    return count


def _reviewed_count(rows: Iterable[Mapping[str, Any]]) -> int:
    count = 0
    for row in rows:
        state = str(row.get("review_state") or row.get("operator_review_status") or row.get("candidate_status") or "").upper()
        if any(token in state for token in ["REVIEWED", "WATCHLIST", "DISMISSED", "NEEDS_MORE_EVIDENCE"]):
            count += 1
    return count


def _secondary_flags(*, run_row: Mapping[str, Any], sleeve_candidates: List[Mapping[str, Any]], missing_symbols: List[str], stale_symbols: List[str]) -> List[str]:
    flags: set[str] = set()
    if "VIX" in missing_symbols:
        flags.add("VIX_MISSING")
    if "VIX" in stale_symbols:
        flags.add("VIX_STALE")
    if missing_symbols or stale_symbols:
        flags.add("MARKET_DATA_PARTIAL")
    for row in sleeve_candidates:
        decision = _safe_dict(row.get("candidate_decision_projection"))
        missing = _safe_list(decision.get("missing_evidence_with_reason"))
        if missing:
            flags.add("EVIDENCE_INCOMPLETE")
        missing_text = json.dumps(missing, sort_keys=True).lower()
        if "expectancy" in missing_text:
            flags.add("EXPECTANCY_MISSING")
        if "drift" in missing_text:
            flags.add("DRIFT_CONTEXT_MISSING")
        if "fragility" in missing_text:
            flags.add("FRAGILITY_CONTEXT_MISSING")
        state = str(row.get("review_state") or row.get("operator_review_status") or "").upper()
        if "EXPIRED" in state:
            flags.add("CANDIDATE_EXPIRED")
        if "PENDING" in state or "REVIEW_REQUIRED" in state:
            flags.add("OPERATOR_ACTION_PENDING")
    if str(run_row.get("canonical_blocker") or "").upper().find("GOVERNANCE") >= 0:
        flags.add("GOVERNANCE_REJECTED")
    return sorted(flags)


def _highest_bucket(applicable: Iterable[str]) -> str:
    order = {bucket: idx for idx, bucket in enumerate(PRIMARY_BUCKETS)}
    values = [bucket for bucket in applicable if bucket in order]
    if not values:
        return "SLEEVE_NOT_RUN"
    return sorted(values, key=lambda bucket: order[bucket])[-1]


def classify_sleeve_eod_outcome_v1(sleeve_id: str, cockpit: Mapping[str, Any]) -> Dict[str, Any]:
    run_rows = _run_rows_by_sleeve(cockpit)
    candidates_by_sleeve = _candidates_by_sleeve(cockpit)
    run_row = run_rows.get(sleeve_id, {})
    sleeve_candidates = candidates_by_sleeve.get(sleeve_id, [])
    active = _safe_dict(cockpit.get("active_opportunity_projection"))
    eligible_ids = {_candidate_id(row) for row in _safe_list(active.get("candidates")) if isinstance(row, dict)}
    unsupported_ids = {_candidate_id(row) for row in _safe_list(active.get("research_observations") or active.get("needs_evidence_candidates")) if isinstance(row, dict)}
    selected_rows = [row for row in sleeve_candidates if str(row.get("_projection_bucket") or "") == "selected_exposure"]
    suppressed_rows = [row for row in sleeve_candidates if str(row.get("_projection_bucket") or "") == "suppressed_watchlist"]
    rejected_reason_codes: List[str] = []
    rejection_reason_codes: List[str] = []
    missing_symbols = [str(symbol).upper() for symbol in _safe_list(run_row.get("missing_symbols") or run_row.get("blocking_inputs"))]
    stale_symbols = [str(symbol).upper() for symbol in _safe_list(run_row.get("stale_symbols"))]
    if not missing_symbols:
        market = _safe_dict(_safe_dict(cockpit.get("opportunities")).get("market_data_summary"))
        missing_symbols = [str(symbol).upper() for symbol in _safe_list(market.get("missing_symbols")) if str(symbol).upper() in {str(v).upper() for v in _safe_list(run_row.get("blocking_inputs"))}]
        stale_symbols = [str(symbol).upper() for symbol in _safe_list(market.get("stale_symbols")) if str(symbol).upper() in {str(v).upper() for v in _safe_list(run_row.get("blocking_inputs"))}]
    raw_signal_count = int(run_row.get("raw_signal_count") or run_row.get("raw_signals") or len(_safe_list(run_row.get("raw_signal_ids"))) or 0)
    candidate_count = len(sleeve_candidates)
    unsupported_count = len([row for row in sleeve_candidates if _candidate_id(row) in unsupported_ids])
    eligible_count = len([row for row in sleeve_candidates if _candidate_id(row) in eligible_ids])
    manual_count = _manual_capture_count(sleeve_candidates)
    reviewed_count = _reviewed_count(sleeve_candidates)
    rejected_count = int(run_row.get("rejected_candidate_count") or run_row.get("rejected_signal_count") or len(_safe_list(run_row.get("rejections"))) or 0)
    run_status = str(run_row.get("run_status") or "").upper()
    ran = bool(run_row) and run_status not in {"BLOCKED", "NOT_RUN", "SKIPPED"}
    blocked_reason_codes = [str(run_row.get("canonical_blocker") or run_row.get("blocked_reason") or "").strip()]
    blocked_reason_codes = [code for code in blocked_reason_codes if code]
    rejection_reason_codes.extend(str(code) for code in _safe_list(run_row.get("rejection_reason_codes")))
    applicable: List[str] = []
    if run_status == "BLOCKED" or blocked_reason_codes or missing_symbols or stale_symbols:
        applicable.append("BLOCKED_DATA_OR_INPUT")
    if not run_row:
        applicable.append("SLEEVE_NOT_RUN")
    if ran and raw_signal_count == 0 and candidate_count == 0 and rejected_count == 0:
        applicable.append("NO_RAW_SIGNAL")
    if ran and raw_signal_count > 0 and candidate_count == 0:
        applicable.append("RAW_SIGNAL_REJECTED")
    if rejected_count > 0 and candidate_count == 0:
        applicable.append("CANDIDATE_REJECTED")
    if selected_rows and any(row.get("blocked_conversion") for row in selected_rows):
        applicable.append("BLOCKED_DATA_OR_INPUT")
        blocked_reason_codes.append("STALE_MARKET_DATA_BLOCKS_CONVERSION")
        stale_symbol = str(selected_rows[0].get("symbol") or "").upper()
        if stale_symbol and stale_symbol not in stale_symbols:
            stale_symbols.append(stale_symbol)
    if suppressed_rows and not selected_rows and not unsupported_count and not eligible_count:
        applicable.append("CANDIDATE_REJECTED")
    if unsupported_count > 0:
        applicable.append("UNSUPPORTED_RESEARCH_OBSERVATION")
    if eligible_count > 0:
        applicable.append("OPERATOR_ELIGIBLE_OPPORTUNITY")
    if reviewed_count > 0:
        applicable.append("OPERATOR_REVIEWED")
    if manual_count > 0:
        applicable.append("MANUAL_EXTERNAL_CAPTURE_RECORDED")
    if "BLOCKED_DATA_OR_INPUT" in applicable and not ran:
        bucket = "BLOCKED_DATA_OR_INPUT"
    else:
        bucket = _highest_bucket(applicable)
    flags = _secondary_flags(run_row=run_row, sleeve_candidates=sleeve_candidates, missing_symbols=missing_symbols, stale_symbols=stale_symbols)
    if unsupported_count:
        flags.append("PAPER_OBSERVATION_ONLY")
    flags = sorted(set(flags))
    evidence_completeness = "incomplete" if "EVIDENCE_INCOMPLETE" in flags or unsupported_count else "complete" if eligible_count else "not_applicable"
    decision_support_status = "operator_eligible" if eligible_count else "needs_evidence" if unsupported_count else "none"
    explanation = _explain_outcome(bucket, sleeve_id=sleeve_id, missing_symbols=missing_symbols, unsupported_count=unsupported_count, eligible_count=eligible_count)
    source_artifacts = [str(path) for path in _safe_dict(cockpit.get("source_paths")).values() if path]
    outcome = {
        "sleeve_outcome_id": f"eod-sleeve:{sleeve_id}:{_stable_hash({'sleeve_id': sleeve_id, 'bucket': bucket, 'candidates': [_candidate_id(row) for row in sleeve_candidates]})[:16]}",
        "sleeve_id": sleeve_id,
        "sleeve_version_id": str(run_row.get("sleeve_version_id") or ""),
        "primary_outcome_bucket": bucket,
        "secondary_flags": flags,
        "ran": ran,
        "raw_signal_count": raw_signal_count,
        "candidate_count": candidate_count,
        "rejected_candidate_count": rejected_count,
        "unsupported_observation_count": unsupported_count,
        "operator_eligible_count": eligible_count,
        "manual_capture_count": manual_count,
        "blocked_reason_codes": blocked_reason_codes,
        "rejection_reason_codes": rejection_reason_codes,
        "missing_symbols": missing_symbols,
        "stale_symbols": stale_symbols,
        "evidence_completeness": evidence_completeness,
        "decision_support_status": decision_support_status,
        "source_artifacts": source_artifacts,
        "explanation": explanation,
        "strategy_failure_counted": bucket in STRATEGY_FAILURE_BUCKETS,
    }
    outcome["determinism_fingerprint"] = _stable_hash(outcome)
    return outcome


def _explain_outcome(bucket: str, *, sleeve_id: str, missing_symbols: List[str], unsupported_count: int, eligible_count: int) -> str:
    if bucket == "BLOCKED_DATA_OR_INPUT":
        return f"{sleeve_id} was blocked by required data/input availability: {', '.join(missing_symbols) if missing_symbols else 'input requirement blocked'}."
    if bucket == "SLEEVE_NOT_RUN":
        return f"{sleeve_id} did not run in the source sleeve summary."
    if bucket == "NO_RAW_SIGNAL":
        return f"{sleeve_id} ran and produced no raw signal."
    if bucket == "RAW_SIGNAL_REJECTED":
        return f"{sleeve_id} produced raw signals, but they were rejected before candidate generation."
    if bucket == "CANDIDATE_REJECTED":
        return f"{sleeve_id} produced candidate material that was rejected by deterministic filters."
    if bucket == "UNSUPPORTED_RESEARCH_OBSERVATION":
        return f"{sleeve_id} generated {unsupported_count} research observation(s), but supporting evidence was incomplete."
    if bucket == "OPERATOR_ELIGIBLE_OPPORTUNITY":
        return f"{sleeve_id} generated {eligible_count} operator-eligible opportunity candidate(s)."
    if bucket == "OPERATOR_REVIEWED":
        return f"{sleeve_id} had an operator-reviewed candidate."
    if bucket == "MANUAL_EXTERNAL_CAPTURE_RECORDED":
        return f"{sleeve_id} had a manual external capture recorded. Aegis did not execute anything."
    return f"{sleeve_id} outcome classified as {bucket}."


def build_eod_opportunity_outcome_report_v1(cockpit: Mapping[str, Any], *, trading_session: str, generated_at: Optional[str] = None) -> Dict[str, Any]:
    sleeve_ids = _all_sleeve_ids(cockpit)
    outcomes = [classify_sleeve_eod_outcome_v1(sleeve_id, cockpit) for sleeve_id in sleeve_ids]
    outcomes = sorted(outcomes, key=lambda row: row["sleeve_id"])
    counts = {bucket: len([row for row in outcomes if row["primary_outcome_bucket"] == bucket]) for bucket in PRIMARY_BUCKETS}
    active = _safe_dict(cockpit.get("active_opportunity_projection"))
    selected_candidate_count = int(active.get("selected_candidate_count") or (1 if _safe_dict(active.get("manual_capture_candidate")).get("candidate_available") else 0))
    suppressed_candidate_count = int(active.get("suppressed_count") or _safe_dict(active.get("suppressed_candidate_watchlist")).get("suppressed_count") or 0)
    blocked_conversion_count = int(active.get("blocked_conversion_count") or (1 if _safe_dict(active.get("manual_capture_candidate")).get("blocker_code") else 0))
    latest_run_summary = _safe_dict(cockpit.get("latest_operator_run_summary"))
    projected_sleeves_ran = int(latest_run_summary.get("sleeves_ran") or latest_run_summary.get("sleeves_run") or 0)
    classified_sleeves_ran = len([row for row in outcomes if row["ran"]])
    report = {
        "eod_outcome_report_id": f"eodopp_{str(trading_session).replace('-', '')}_{_stable_hash({'session': trading_session, 'outcomes': outcomes})[:12]}",
        "trading_session": trading_session,
        "generated_at": generated_at or _now(),
        "schema_version": SCHEMA_VERSION,
        "classifier_version": CLASSIFIER_VERSION,
        "bucket_precedence_version": BUCKET_PRECEDENCE_VERSION,
        "research_label": "RESEARCH_ONLY",
        "advisory_label": "ADVISORY_ONLY",
        "source_run_ids": [str(row.get("run_id")) for row in _safe_list(_safe_dict(cockpit.get("opportunities")).get("sleeve_run_summary")) if isinstance(row, dict) and row.get("run_id")],
        "source_candidate_batch_ids": [str(row.get("candidate_batch_id")) for row in _safe_list(cockpit.get("top_candidates")) if isinstance(row, dict) and row.get("candidate_batch_id")],
        "source_diagnostic_ids": [str(row.get("diagnostic_id")) for row in _safe_list(_safe_dict(cockpit.get("system_diagnostic_projection")).get("diagnostics")) if isinstance(row, dict)],
        "source_market_data_snapshot_ids": [str(_safe_dict(_safe_dict(cockpit.get("opportunities")).get("market_data_summary")).get("snapshot_id") or "")],
        "sleeve_outcomes": outcomes,
        "aggregate_summary": {
            "sleeves_expected": len(outcomes),
            "sleeves_ran": max(classified_sleeves_ran, projected_sleeves_ran),
            "bucket_counts": counts,
            "strategy_failure_count": len([row for row in outcomes if row["strategy_failure_counted"]]),
            "data_or_input_block_count": counts["BLOCKED_DATA_OR_INPUT"],
            "selected_candidate_count": selected_candidate_count,
            "suppressed_candidate_count": suppressed_candidate_count,
            "blocked_conversion_count": blocked_conversion_count,
        },
        "selected_candidate_count": selected_candidate_count,
        "suppressed_candidate_count": suppressed_candidate_count,
        "blocked_conversion_count": blocked_conversion_count,
        "sleeves_ran": max(classified_sleeves_ran, projected_sleeves_ran),
        "sleeves_run": max(classified_sleeves_ran, projected_sleeves_ran),
        "operator_eligible_count": sum(row["operator_eligible_count"] for row in outcomes),
        "unsupported_observation_count": sum(row["unsupported_observation_count"] for row in outcomes),
        "blocked_sleeve_count": counts["BLOCKED_DATA_OR_INPUT"],
        "manual_capture_count": sum(row["manual_capture_count"] for row in outcomes),
        "no_signal_count": counts["NO_RAW_SIGNAL"],
        "rejected_count": counts["RAW_SIGNAL_REJECTED"] + counts["CANDIDATE_REJECTED"],
        "audit_refs": ["EOD_OPPORTUNITY_OUTCOME_RECORDED"],
        "operator_eligible_candidate_ids": [_candidate_id(row) for row in _safe_list(active.get("candidates")) if isinstance(row, dict)],
        "unsupported_candidate_ids": [_candidate_id(row) for row in _safe_list(active.get("research_observations") or active.get("needs_evidence_candidates")) if isinstance(row, dict)],
        "governance": {
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "autonomous_execution_allowed": False,
            "order_routing_allowed": False,
            "automatic_approval_allowed": False,
            "automatic_promotion_allowed": False,
            "sleeve_mutation_allowed": False,
        },
    }
    report["determinism_fingerprint"] = _stable_hash({k: v for k, v in report.items() if k not in {"generated_at", "determinism_fingerprint"}})
    return report


def eod_outcome_report_path_v1(*, truth_root: Path, trading_session: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_eod_opportunity_outcome_report_v1" / trading_session / "eod_opportunity_outcome_report.v1.json"


def eod_outcome_registry_path_v1(*, truth_root: Path) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_eod_opportunity_outcome_report_v1" / "registry.jsonl"


def write_eod_opportunity_outcome_report_v1(*, truth_root: Path, report: Mapping[str, Any]) -> Dict[str, Any]:
    path = eod_outcome_report_path_v1(truth_root=truth_root, trading_session=str(report["trading_session"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(report)
    payload["output_report_hash"] = _stable_hash(payload)
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing.get("determinism_fingerprint") != payload.get("determinism_fingerprint"):
            raise FileExistsError(f"EOD outcome report already exists with different fingerprint: {path}")
        return {**existing, "path": str(path), "already_exists": True}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    registry = eod_outcome_registry_path_v1(truth_root=truth_root)
    registry.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "event_type": "EOD_OPPORTUNITY_OUTCOME_RECORDED",
        "eod_outcome_report_id": payload["eod_outcome_report_id"],
        "trading_session": payload["trading_session"],
        "determinism_fingerprint": payload["determinism_fingerprint"],
        "output_report_hash": payload["output_report_hash"],
        "path": str(path),
        "recorded_at": _now(),
    }
    with registry.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")
    return {**payload, "path": str(path), "already_exists": False}


def read_eod_opportunity_outcome_report_v1(*, truth_root: Path, trading_session: str) -> Optional[Dict[str, Any]]:
    path = eod_outcome_report_path_v1(truth_root=truth_root, trading_session=trading_session)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["path"] = str(path)
    return payload


def read_latest_eod_opportunity_outcome_report_v1(*, truth_root: Path) -> Optional[Dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve() / "reports" / "aegis_eod_opportunity_outcome_report_v1"
    if not root.exists():
        return None
    candidates = sorted(root.glob("*/eod_opportunity_outcome_report.v1.json"))
    if not candidates:
        return None
    payload = json.loads(candidates[-1].read_text(encoding="utf-8"))
    payload["path"] = str(candidates[-1])
    return payload


def read_eod_opportunity_outcome_report_by_id_v1(*, truth_root: Path, eod_outcome_report_id: str) -> Optional[Dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve() / "reports" / "aegis_eod_opportunity_outcome_report_v1"
    if not root.exists():
        return None
    for path in sorted(root.glob("*/eod_opportunity_outcome_report.v1.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("eod_outcome_report_id") == eod_outcome_report_id:
            payload["path"] = str(path)
            return payload
    return None


def eod_sleeve_history_v1(*, truth_root: Path, sleeve_id: str) -> Dict[str, Any]:
    root = Path(truth_root).expanduser().resolve() / "reports" / "aegis_eod_opportunity_outcome_report_v1"
    rows = []
    for path in sorted(root.glob("*/eod_opportunity_outcome_report.v1.json")) if root.exists() else []:
        report = json.loads(path.read_text(encoding="utf-8"))
        for outcome in _safe_list(report.get("sleeve_outcomes")):
            if isinstance(outcome, dict) and outcome.get("sleeve_id") == sleeve_id:
                rows.append({
                    "trading_session": report.get("trading_session"),
                    "eod_outcome_report_id": report.get("eod_outcome_report_id"),
                    **outcome,
                })
    return {"sleeve_id": sleeve_id, "history": rows, "history_count": len(rows)}


def longitudinal_eod_accumulation_v1(reports: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    by_sleeve: Dict[str, Dict[str, Any]] = {}
    for report in sorted(reports, key=lambda row: str(row.get("trading_session"))):
        for outcome in _safe_list(report.get("sleeve_outcomes")):
            if not isinstance(outcome, dict):
                continue
            sleeve_id = str(outcome.get("sleeve_id") or "")
            bucket = str(outcome.get("primary_outcome_bucket") or "")
            row = by_sleeve.setdefault(sleeve_id, {
                "sleeve_id": sleeve_id,
                "sessions": 0,
                "no_signal_streak": 0,
                "blocked_streak": 0,
                "unsupported_observation_streak": 0,
                "operator_eligible_count": 0,
                "rejection_count": 0,
                "data_block_count": 0,
                "evidence_incomplete_count": 0,
                "manual_capture_count": 0,
                "strategy_failure_count": 0,
            })
            row["sessions"] += 1
            row["no_signal_streak"] = row["no_signal_streak"] + 1 if bucket == "NO_RAW_SIGNAL" else 0
            row["blocked_streak"] = row["blocked_streak"] + 1 if bucket == "BLOCKED_DATA_OR_INPUT" else 0
            row["unsupported_observation_streak"] = row["unsupported_observation_streak"] + 1 if bucket == "UNSUPPORTED_RESEARCH_OBSERVATION" else 0
            row["operator_eligible_count"] += int(outcome.get("operator_eligible_count") or 0)
            row["rejection_count"] += 1 if bucket in {"RAW_SIGNAL_REJECTED", "CANDIDATE_REJECTED"} else 0
            row["data_block_count"] += 1 if bucket == "BLOCKED_DATA_OR_INPUT" else 0
            row["evidence_incomplete_count"] += 1 if "EVIDENCE_INCOMPLETE" in _safe_list(outcome.get("secondary_flags")) else 0
            row["manual_capture_count"] += int(outcome.get("manual_capture_count") or 0)
            row["strategy_failure_count"] += 1 if outcome.get("strategy_failure_counted") is True else 0
    for row in by_sleeve.values():
        sessions = max(int(row["sessions"]), 1)
        row["operator_eligible_rate"] = row["operator_eligible_count"] / sessions
        row["rejection_rate"] = row["rejection_count"] / sessions
        row["data_block_rate"] = row["data_block_count"] / sessions
        row["evidence_incomplete_rate"] = row["evidence_incomplete_count"] / sessions
        row["manual_capture_rate"] = row["manual_capture_count"] / sessions
    result = {"schema_version": "eod_longitudinal_accumulation.v1", "sleeves": sorted(by_sleeve.values(), key=lambda row: row["sleeve_id"])}
    result["determinism_fingerprint"] = _stable_hash(result)
    return result
