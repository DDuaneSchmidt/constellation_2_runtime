from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1, paper_position_ledger_path_v1
from ops.aegis.trade_lifecycle.exit_policy_registry_v1 import exit_policy_for_sleeve_v1, exit_policy_registry_v1

REPORT_FAMILY = "aegis_exit_recommendations_v1"
REVIEW_FAMILY = "aegis_exit_logic_review_v1"
RECOMMENDATIONS_FILENAME = "exit_recommendations.v1.json"
REVIEW_FILENAME = "exit_logic_review.v1.json"

SAFETY = {
    "paper_only": True,
    "human_review_required": True,
    "operator_action_required": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "automatic_exit_allowed": False,
}


def exit_recommendations_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / RECOMMENDATIONS_FILENAME


def exit_logic_review_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REVIEW_FAMILY / str(day_utc) / REVIEW_FILENAME


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def file_hash_v1(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def read_json_v1(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def build_exit_logic_review_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    ledger_path = paper_position_ledger_path_v1(truth_root=root, day_utc=day_utc)
    rec_path = exit_recommendations_path_v1(truth_root=root, day_utc=day_utc)
    mechanisms = [
        {
            "mechanism_id": "canonical_paper_position_ledger",
            "implemented_in": "ops/aegis/paper_position_ledger_v1.py",
            "artifact": str(ledger_path),
            "description": "Canonical OPEN/CLOSED/LEGACY simulated paper position source of truth.",
            "deterministic": True,
            "sleeve_specific": False,
            "outcome_attribution_exists": True,
            "limitations": ["Position closure still requires explicit operator Record Exit."],
        },
        {
            "mechanism_id": "record_paper_exit_command",
            "implemented_in": "ops/aegis/human_reviewed_paper_mode_v1.py and ops/aegis/operator_action_command_contracts_v1.py",
            "artifact": "aegis_paper_trade_receipts_v1 + aegis_paper_position_events_v1",
            "description": "Operator-confirmed simulated paper exit receipt closes a ledger position.",
            "deterministic": True,
            "sleeve_specific": False,
            "outcome_attribution_exists": True,
            "limitations": ["Does not itself decide whether the exit was good."],
        },
        {
            "mechanism_id": "position_management_stop_events",
            "implemented_in": "ops/aegis/position_management_v1.py",
            "artifact": "aegis_position_management_v1",
            "description": "Manual risk plans and stop events support hard/soft/trailing/time stop evidence.",
            "deterministic": True,
            "sleeve_specific": False,
            "outcome_attribution_exists": True,
            "limitations": ["Not yet the canonical exit recommendation source for paper_position_ledger rows."],
        },
        {
            "mechanism_id": "trade_lifecycle_exit_review_projection",
            "implemented_in": "ops/aegis/trade_lifecycle/exit_review_projection_v1.py",
            "artifact": "exit_review_projection_v1",
            "description": "Existing trade lifecycle exit review can derive HOLD/UPDATE_STOP/TAKE_PARTIAL/EXIT_FULL/BLOCK for lifecycle rows.",
            "deterministic": True,
            "sleeve_specific": True,
            "outcome_attribution_exists": True,
            "limitations": ["Projection is not yet centered on canonical paper_position_ledger OPEN rows."],
        },
        {
            "mechanism_id": "paper_exit_recommendations_v1",
            "implemented_in": "ops/aegis/exit_recommendations_v1.py",
            "artifact": str(rec_path),
            "description": "Human-reviewed recommendation layer for OPEN simulated paper positions.",
            "deterministic": True,
            "sleeve_specific": True,
            "outcome_attribution_exists": True,
            "limitations": ["Policy values are explicit stubs; they need outcome calibration before being treated as a backtested policy."],
        },
    ]
    support = {
        "stop_loss": True,
        "take_profit": True,
        "trailing_stop": True,
        "time_stop": True,
        "signal_invalidation": True,
        "regime_invalidation": True,
        "volatility_based_stop": False,
        "sleeve_specific_exit_rules": True,
        "max_hold_period": True,
        "exit_reason_attribution": True,
        "actual_vs_recommended_exit_comparison": True,
    }
    missing_inputs = [
        "Certified intraday/current mark for every open position when market is open.",
        "High-water mark / MFE evidence for robust trailing stop evaluation.",
        "ATR or volatility feature if volatility-based stops are enabled.",
        "Current signal invalidation and regime invalidation evidence by candidate/sleeve.",
        "Backtest calibration for sleeve-specific policy values.",
    ]
    payload = {
        "schema_id": "aegis_exit_logic_review",
        "schema_version": "v1",
        "artifact_id": REVIEW_FAMILY,
        "day_utc": str(day_utc),
        "generated_at_utc": utc_now_v1(),
        "current_maturity": "BASIC_RULES",
        "prior_maturity": "MANUAL_EXIT_ONLY",
        "target_next_maturity": "SLEEVE_SPECIFIC_RULES",
        "current_exit_mechanisms": mechanisms,
        "sleeve_coverage": _sleeve_coverage(root, str(day_utc)),
        "known_limitations": missing_inputs,
        "missing_inputs": missing_inputs,
        "capability_support": support,
        "recommendation_layer_status": "AVAILABLE" if rec_path.exists() else "BUILD_REQUIRED",
        "next_repair_action": f"TARGET_DAY={day_utc} npm run aegis:exit-recommendations",
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def build_exit_recommendations_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    ledger = build_paper_position_ledger_v1(truth_root=root, day_utc=str(day_utc))
    ledger_path = paper_position_ledger_path_v1(truth_root=root, day_utc=str(day_utc))
    rows = []
    for position in ledger.get("open_positions") or []:
        if isinstance(position, dict):
            rows.append(_recommendation_for_position(position, truth_root=root, day_utc=str(day_utc), ledger_path=ledger_path))
    summary = _pnl_summary(ledger, rows)
    payload = {
        "schema_id": "aegis_exit_recommendations",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": str(day_utc),
        "generated_at_utc": utc_now_v1(),
        "operating_mode": "HUMAN_REVIEWED_PAPER_MODE",
        "maturity_state": "BASIC_RULES",
        "recommendation_count": len(rows),
        "open_position_count": len(rows),
        "recommendations": rows,
        "rows": rows,
        "pnl_report_hooks": summary,
        "exit_policy_registry": exit_policy_registry_v1(),
        "source_artifacts": [str(ledger_path)],
        "source_hashes": {str(ledger_path): file_hash_v1(ledger_path)},
        "operator_action_required": True,
        "automatic_exit_allowed": False,
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_exit_recommendations_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    payload = payload or build_exit_recommendations_v1(truth_root=root, day_utc=day_utc)
    path = exit_recommendations_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"exit_recommendations": str(path), "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload))}


def write_exit_logic_review_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    payload = payload or build_exit_logic_review_v1(truth_root=root, day_utc=day_utc)
    path = exit_logic_review_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"exit_logic_review": str(path), "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload))}


def latest_exit_recommendation_for_candidate_v1(*, truth_root: Path | str, day_utc: str, candidate_id: str) -> dict[str, Any]:
    path = exit_recommendations_path_v1(truth_root=truth_root, day_utc=day_utc)
    payload = read_json_v1(path)
    if not payload:
        payload = build_exit_recommendations_v1(truth_root=truth_root, day_utc=day_utc)
    for row in payload.get("recommendations") or []:
        if isinstance(row, dict) and str(row.get("candidate_id") or "") == str(candidate_id or ""):
            return row
    return {}


def _recommendation_for_position(position: dict[str, Any], *, truth_root: Path, day_utc: str, ledger_path: Path) -> dict[str, Any]:
    lineage = position.get("candidate_lineage") if isinstance(position.get("candidate_lineage"), dict) else {}
    candidate_context = _candidate_context(truth_root, day_utc, str(position.get("candidate_id") or lineage.get("candidate_id") or ""))
    sleeve_id = str(position.get("sleeve_id") or lineage.get("sleeve_id") or candidate_context.get("sleeve_id") or "DEFAULT").upper() or "DEFAULT"
    policy = exit_policy_for_sleeve_v1(sleeve_id)
    entry = _float(position.get("entry_price"))
    mark = _float(position.get("mark_price") or position.get("current_mark"))
    side = str(position.get("side") or "BUY").upper()
    holding_days = _holding_days(position.get("entry_time"), day_utc)
    reason_codes: list[str] = []
    recommendation = "HOLD"
    confidence = 0.55
    return_pct = None
    if entry is None or entry <= 0:
        reason_codes.append("MISSING_ENTRY_PRICE")
        confidence = 0.2
    if mark is None or mark <= 0:
        reason_codes.append("MISSING_CURRENT_MARK")
        confidence = min(confidence, 0.2)
    if entry and mark:
        sign = -1 if side in {"SELL", "SHORT"} else 1
        return_pct = ((mark - entry) / entry) * sign
        stop_loss_pct = _float(policy.get("stop_loss_pct"))
        take_profit_pct = _float(policy.get("take_profit_pct"))
        trailing_stop_pct = _float(policy.get("trailing_stop_pct"))
        high_water = _float(position.get("highest_mark") or position.get("max_mark") or position.get("MFE_mark"))
        if bool(position.get("signal_invalidated")):
            recommendation = "EXIT_SIGNAL_INVALIDATED"
            reason_codes.append("SIGNAL_INVALIDATED")
            confidence = 0.8
        elif bool(position.get("regime_invalidated")):
            recommendation = "EXIT_REGIME_INVALIDATED"
            reason_codes.append("REGIME_INVALIDATED")
            confidence = 0.8
        elif stop_loss_pct is not None and return_pct <= -abs(stop_loss_pct):
            recommendation = "EXIT_STOP_LOSS"
            reason_codes.append("STOP_LOSS_THRESHOLD_REACHED")
            confidence = 0.85
        elif take_profit_pct is not None and return_pct >= abs(take_profit_pct):
            recommendation = "EXIT_TAKE_PROFIT"
            reason_codes.append("TAKE_PROFIT_THRESHOLD_REACHED")
            confidence = 0.8
        elif high_water and trailing_stop_pct is not None and high_water > entry:
            trailing_drawdown = ((high_water - mark) / high_water) if side not in {"SELL", "SHORT"} else ((mark - high_water) / high_water)
            if trailing_drawdown >= abs(trailing_stop_pct):
                recommendation = "EXIT_TRAILING_STOP"
                reason_codes.append("TRAILING_STOP_THRESHOLD_REACHED")
                confidence = 0.75
    max_hold_days = _int(policy.get("max_hold_days") or policy.get("max_holding_days"))
    if recommendation == "HOLD" and max_hold_days is not None and holding_days is not None and holding_days >= max_hold_days:
        recommendation = "EXIT_TIME_STOP"
        reason_codes.append("MAX_HOLD_PERIOD_REACHED")
        confidence = 0.7
    if recommendation == "HOLD" and not reason_codes:
        reason_codes.append("NO_EXIT_RULE_TRIGGERED")
    evidence_paths = [str(ledger_path)] + [str(item) for item in lineage.get("evidence_paths") or candidate_context.get("evidence_paths") or [] if str(item)]
    row = {
        "position_id": str(position.get("position_id") or ""),
        "candidate_id": str(position.get("candidate_id") or ""),
        "symbol": str(position.get("symbol") or "").upper(),
        "sleeve_id": sleeve_id,
        "entry_price": position.get("entry_price"),
        "current_mark": position.get("mark_price") or position.get("current_mark") or "",
        "unrealized_pnl": position.get("unrealized_pnl"),
        "return_pct": round(return_pct, 6) if return_pct is not None else None,
        "holding_period": {"days": holding_days, "entry_time": str(position.get("entry_time") or ""), "as_of_day": day_utc},
        "exit_recommendation": recommendation,
        "confidence": confidence,
        "reason_codes": reason_codes,
        "policy": policy,
        "candidate_context": candidate_context,
        "evidence_paths": evidence_paths,
        "source_hashes": {path: file_hash_v1(Path(path)) for path in evidence_paths if Path(path).exists()},
        "operator_action_required": True,
        "automatic_exit_allowed": False,
        "record_exit_command": "RECORD_PAPER_EXIT",
        **SAFETY,
    }
    row["recommendation_id"] = f"exit-rec:{stable_hash_v1(row)[:24]}"
    row["content_hash"] = stable_hash_v1(row)
    return row



def _candidate_context(root: Path, day_utc: str, candidate_id: str) -> dict[str, Any]:
    if not candidate_id:
        return {}
    sources = [
        ("aegis_candidate_review_packet_v1", "candidate_review_packet.v1.json", ("review_candidates", "rows")),
        ("aegis_paper_review_queue_v1", "paper_review_queue.v1.json", ("rows", "review_candidates")),
        ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json", ("candidate_contracts", "candidates", "rows")),
    ]
    for family, filename, row_keys in sources:
        path = root / "reports" / family / day_utc / filename
        payload = read_json_v1(path)
        for key in row_keys:
            rows = payload.get(key) if isinstance(payload.get(key), list) else []
            for row in rows:
                if isinstance(row, dict) and str(row.get("candidate_id") or "") == candidate_id:
                    out = dict(row)
                    out["source_path"] = str(path)
                    return out
    return {}

def _pnl_summary(ledger: dict[str, Any], recommendations: list[dict[str, Any]]) -> dict[str, Any]:
    open_positions = [row for row in ledger.get("open_positions") or [] if isinstance(row, dict)]
    closed_positions = [row for row in ledger.get("closed_positions") or [] if isinstance(row, dict)]
    by_sleeve: dict[str, dict[str, Any]] = {}
    for row in open_positions + closed_positions:
        lineage = row.get("candidate_lineage") if isinstance(row.get("candidate_lineage"), dict) else {}
        sleeve = str(row.get("sleeve_id") or lineage.get("sleeve_id") or "UNKNOWN").upper()
        bucket = by_sleeve.setdefault(sleeve, {"sleeve_id": sleeve, "open_count": 0, "closed_count": 0, "unrealized_pnl": 0.0, "realized_pnl": 0.0, "wins": 0, "losses": 0})
        if str(row.get("current_status") or "").upper() == "OPEN":
            bucket["open_count"] += 1
            bucket["unrealized_pnl"] += _float(row.get("unrealized_pnl")) or 0.0
        elif str(row.get("current_status") or "").upper() == "CLOSED":
            realized = _float(row.get("realized_pnl")) or 0.0
            bucket["closed_count"] += 1
            bucket["realized_pnl"] += realized
            if realized > 0:
                bucket["wins"] += 1
            elif realized < 0:
                bucket["losses"] += 1
    return {
        "open_position_pnl": round(sum((_float(row.get("unrealized_pnl")) or 0.0) for row in open_positions), 6),
        "realized_pnl": round(sum((_float(row.get("realized_pnl")) or 0.0) for row in closed_positions), 6),
        "pnl_by_sleeve": list(by_sleeve.values()),
        "win_loss_by_sleeve": [{"sleeve_id": row["sleeve_id"], "wins": row["wins"], "losses": row["losses"]} for row in by_sleeve.values()],
        "average_hold_time_days": _average([_holding_days(row.get("entry_time"), str(row.get("exit_time") or "")[:10]) for row in closed_positions]),
        "recommendation_hit_rate": "INSUFFICIENT_CLOSED_ATTRIBUTION",
        "recommendation_counts": _counts(recommendations, "exit_recommendation"),
    }


def _sleeve_coverage(root: Path, day_utc: str) -> list[dict[str, Any]]:
    ledger = build_paper_position_ledger_v1(truth_root=root, day_utc=day_utc)
    policies = {str(row.get("sleeve_id") or "").upper(): row for row in exit_policy_registry_v1().get("policies") or [] if isinstance(row, dict)}
    sleeves = sorted({str(((row.get("candidate_lineage") or {}) if isinstance(row.get("candidate_lineage"), dict) else {}).get("sleeve_id") or row.get("sleeve_id") or "UNKNOWN").upper() for row in ledger.get("open_positions") or [] if isinstance(row, dict)})
    return [{"sleeve_id": sleeve, "policy_status": "EXPLICIT" if sleeve in policies else "DEFAULT", "open_position_count": sum(1 for row in ledger.get("open_positions") or [] if isinstance(row, dict) and str(((row.get("candidate_lineage") or {}) if isinstance(row.get("candidate_lineage"), dict) else {}).get("sleeve_id") or row.get("sleeve_id") or "UNKNOWN").upper() == sleeve)} for sleeve in sleeves]


def _holding_days(entry_time: Any, day_utc: str) -> int | None:
    text = str(entry_time or "").strip()
    if not text:
        return None
    try:
        start = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC).date()
        end = datetime.fromisoformat(str(day_utc)[:10] + "T00:00:00+00:00").date()
    except ValueError:
        return None
    return max((end - start).days, 0)


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _average(values: list[int | None]) -> float | None:
    clean = [value for value in values if value is not None]
    return round(sum(clean) / len(clean), 4) if clean else None


def _counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "UNKNOWN")
        out[value] = out.get(value, 0) + 1
    return out
