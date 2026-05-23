from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional


SCHEMA_VERSION = "portfolio_gate_candidate_report.v1"
REPORT_KIND = "DIAGNOSTICS_ONLY"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def portfolio_gate_candidate_report_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "portfolio_gate_candidate_report_v1" / day_utc / "portfolio_gate_candidate_report.v1.json"


def _candidate_direction(intent_payload: Mapping[str, Any], ranking: Mapping[str, Any]) -> str:
    exposure = str(intent_payload.get("exposure_type") or ranking.get("exposure_type") or "").upper()
    if exposure.startswith("LONG"):
        return "LONG"
    if exposure.startswith("SHORT"):
        return "SHORT"
    if exposure:
        return exposure
    return "UNKNOWN"


def _intent_payload_from_ranking(ranking: Mapping[str, Any]) -> Dict[str, Any]:
    for path_text in _safe_list(ranking.get("evidence_paths")):
        path = Path(str(path_text))
        if path.name.endswith(".exposure_intent.v1.json") and path.exists():
            return _read_json(path)
    return {}


def _selected_by_bucket(gate: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    selected: Dict[str, Dict[str, Any]] = {}
    for row in _safe_list(gate.get("decisions")):
        if not isinstance(row, dict) or row.get("portfolio_gate_decision") != "ALLOW":
            continue
        selected[str(row.get("regime_bucket") or "UNKNOWN")] = row
    return selected


def _suppression_flags(row: Mapping[str, Any]) -> Dict[str, bool]:
    reasons = set(str(code) for code in _safe_list(row.get("reason_codes")))
    lifecycle_reasons = set(str(code) for code in _safe_list(row.get("lifecycle_reason_codes")))
    return {
        "lower_rank": "ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED" in reasons,
        "duplicate_sleeve_exposure": False,
        "duplicate_symbol_exposure": False,
        "portfolio_concentration": "ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED" in reasons,
        "risk_budget": any("RISK" in code for code in reasons),
        "max_one_intent_per_cycle": "ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED" in reasons,
        "stale_lifecycle": any("STALE" in code for code in lifecycle_reasons) or str(row.get("position_match_status") or "").endswith("STALE"),
        "missing_state": any("UNKNOWN" in code or "MISSING" in code for code in reasons),
        "incompatible_exposure": any("UNFAVORABLE" in code or "SUPPRESSES" in code for code in reasons),
        "submit_boundary_precheck": "SCORING_NOT_EXECUTABLE_SUPPRESS" in reasons or row.get("portfolio_gate_decision") == "SIGNAL_ONLY",
    }


def _suppression_code(row: Mapping[str, Any]) -> str:
    reasons = set(str(code) for code in _safe_list(row.get("reason_codes")))
    if row.get("portfolio_gate_decision") == "ALLOW":
        return "SELECTED"
    if "ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED" in reasons:
        return "one_primary_per_regime_bucket_suppressed"
    if row.get("portfolio_gate_decision") == "SIGNAL_ONLY":
        return "signal_only_submit_boundary"
    if any("LIFECYCLE" in code or "POSITION" in code or "ORDER" in code for code in reasons):
        return "lifecycle_suppression"
    if row.get("portfolio_gate_decision") == "SUPPRESS":
        return "portfolio_gate_suppressed"
    return str(row.get("portfolio_gate_decision") or "unknown").lower()


def _suppression_reason(row: Mapping[str, Any]) -> str:
    code = _suppression_code(row)
    if code == "SELECTED":
        return "Selected by portfolio activation gate."
    if code == "one_primary_per_regime_bucket_suppressed":
        return "Suppressed by ONE_PRIMARY_PER_REGIME_BUCKET after another candidate in the same regime bucket was allowed."
    if code == "signal_only_submit_boundary":
        return "Kept signal-only because paired/submission boundary support is not available."
    if code == "lifecycle_suppression":
        return "Suppressed by lifecycle position/order state."
    return "Suppressed by portfolio activation gate policy."


def build_portfolio_gate_candidate_report_v1(
    *,
    day_utc: str,
    truth_root: Path,
    gate_path: Optional[Path] = None,
    scoring_path: Optional[Path] = None,
) -> Dict[str, Any]:
    truth_root = Path(truth_root).expanduser().resolve()
    gate_path = Path(gate_path).resolve() if gate_path else truth_root / "reports" / "portfolio_activation_gate_v1" / day_utc / "portfolio_activation_gate.v1.json"
    scoring_path = Path(scoring_path).resolve() if scoring_path else truth_root / "reports" / "portfolio_scoring_v1" / day_utc / "portfolio_scoring.v1.json"
    gate = _read_json(gate_path)
    scoring = _read_json(scoring_path)
    selected_by_bucket = _selected_by_bucket(gate)
    selected_gate_rows = [row for row in _safe_list(gate.get("decisions")) if isinstance(row, dict) and row.get("portfolio_gate_decision") == "ALLOW"]
    rows: List[Dict[str, Any]] = []
    for ranking in _safe_list(scoring.get("rankings")):
        if not isinstance(ranking, dict):
            continue
        intent_payload = _intent_payload_from_ranking(ranking)
        selected = bool(ranking.get("allowed_by_portfolio_gate") is True and ranking.get("portfolio_gate_decision") == "ALLOW")
        bucket = str(ranking.get("regime_bucket") or "UNKNOWN")
        competing = selected_by_bucket.get(bucket, {})
        if not competing and "ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED" in set(str(code) for code in _safe_list(ranking.get("reason_codes"))) and len(selected_gate_rows) == 1:
            competing = selected_gate_rows[0]
        flags = _suppression_flags(ranking)
        row = {
            "candidate_id": str(ranking.get("intent_id") or intent_payload.get("intent_id") or ""),
            "sleeve_id": str(ranking.get("sleeve_id") or ""),
            "engine_id": str(ranking.get("sleeve_id") or intent_payload.get("engine", {}).get("engine_id") if isinstance(intent_payload.get("engine"), dict) else ranking.get("sleeve_id") or ""),
            "symbol": str(ranking.get("symbol") or intent_payload.get("underlying", {}).get("symbol") if isinstance(intent_payload.get("underlying"), dict) else ranking.get("symbol") or "").upper(),
            "direction": _candidate_direction(intent_payload, ranking),
            "score": ranking.get("score_total"),
            "confidence": ranking.get("regime_confidence_level") or "UNKNOWN",
            "rank_before_arbitration": ranking.get("rank"),
            "selected_by_gate": "YES" if selected else "NO",
            "suppression_code": "SELECTED" if selected else _suppression_code(ranking),
            "suppression_reason": _suppression_reason(ranking),
            "competing_selected_candidate_id": "" if selected else str(competing.get("raw_intent_id") or ""),
            **flags,
            "portfolio_gate_decision": ranking.get("portfolio_gate_decision"),
            "reason_codes": _safe_list(ranking.get("reason_codes")),
            "lifecycle_decision": ranking.get("lifecycle_decision"),
            "lifecycle_reason_codes": _safe_list(ranking.get("lifecycle_reason_codes")),
            "regime_bucket": bucket,
            "overlap_group": ranking.get("overlap_group", ""),
            "evidence_paths": _safe_list(ranking.get("evidence_paths")),
        }
        rows.append(row)
    rows = sorted(rows, key=lambda row: (0 if row["selected_by_gate"] == "YES" else 1, int(row.get("rank_before_arbitration") or 999999), row["candidate_id"]))
    counts = Counter(row["suppression_code"] for row in rows if row["selected_by_gate"] != "YES")
    selected_rows = [row for row in rows if row["selected_by_gate"] == "YES"]
    report = {
        "schema_id": "portfolio_gate_candidate_report",
        "schema_version": SCHEMA_VERSION,
        "report_kind": REPORT_KIND,
        "day_utc": day_utc,
        "generated_at_utc": _now(),
        "source_gate_path": str(gate_path),
        "source_scoring_path": str(scoring_path),
        "selected_candidate_id": selected_rows[0]["candidate_id"] if selected_rows else "",
        "candidate_count": len(rows),
        "selected_count": len(selected_rows),
        "suppressed_count": len(rows) - len(selected_rows),
        "suppression_code_counts": dict(sorted(counts.items())),
        "candidate_rows": rows,
        "top_10_suppressed": [row for row in rows if row["selected_by_gate"] != "YES"][:10],
        "gate_policy_answers": {
            "designed_to_select_only_one_candidate_total": False,
            "designed_to_select_one_per_sleeve": False,
            "designed_to_select_one_per_symbol": False,
            "designed_to_select_one_per_regime_bucket": True,
            "all_current_suppressed_scored_candidates_are_lower_ranked_alternatives": all(row.get("lower_rank") for row in rows if row["selected_by_gate"] != "YES"),
            "configurable_max_selected_intents_setting": "No explicit max_selected_intents setting found; effective limit is one ALLOW per regime_bucket via ONE_PRIMARY_PER_REGIME_BUCKET.",
            "setting_intentional": True,
        },
        "watchlist_visibility_recommendation": {
            "selected_candidate": "show_as_selected_candidate",
            "suppressed_candidates": "show_as_watchlist_only_candidates",
            "do_not_convert_to_trades": True,
        },
        "diagnosis": "expected_governance_behavior" if len(selected_rows) == 1 and counts.get("one_primary_per_regime_bucket_suppressed", 0) else "review_required",
        "fix_implemented": "diagnostics_only",
        "safety": {
            "trades_created": False,
            "broker_used": False,
            "orders_created": False,
            "capital_allocated": False,
            "governance_bypass": False,
            "paper_submit_created": False,
        },
    }
    report["determinism_fingerprint"] = _stable_hash({k: v for k, v in report.items() if k != "generated_at_utc"})
    return report


def write_portfolio_gate_candidate_report_v1(*, truth_root: Path, report: Mapping[str, Any]) -> Dict[str, Any]:
    path = portfolio_gate_candidate_report_path(truth_root=truth_root, day_utc=str(report["day_utc"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(report)
    payload["artifact_path"] = str(path)
    payload["artifact_hash"] = _stable_hash(payload)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
