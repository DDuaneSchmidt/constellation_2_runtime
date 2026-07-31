from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.paper_pnl_report_v1 import paper_pnl_report_path_v1
from ops.aegis.paper_position_ledger_v1 import paper_position_ledger_path_v1
from ops.aegis.sleeve_evidence_certification_v1 import sleeve_evidence_certification_path_v1
from ops.aegis.sleeve_performance_truth_v1 import sleeve_performance_truth_path_v1


REPORT_FAMILY = "aegis_outcome_flow_audit_v1"
JSON_FILENAME = "outcome_flow_audit.v1.json"
SUMMARY_FILENAME = "outcome_flow_audit_summary.md"

MINIMUM_SAMPLE_THRESHOLD = 5
EARLY_OBSERVATION_DAYS = 5
DEFAULT_EXIT_REVIEW_AGE_DAYS = 5
SIX_MONTH_DAYS = 183
TWELVE_MONTH_DAYS = 365

SAFETY = {
    "review_only": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_policy_changed": False,
    "runtime_policy_changed": False,
    "no_trade_recommendations": True,
    "no_investable_edge_claims": True,
}


def outcome_flow_audit_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / JSON_FILENAME


def outcome_flow_audit_summary_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / SUMMARY_FILENAME


def build_outcome_flow_audit_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    source_paths = _source_paths(root, day)
    payloads = {name: _read_json(path) for name, path in source_paths.items()}
    missing_required = [
        name
        for name in ("paper_position_ledger", "paper_pnl_report", "sleeve_performance_truth", "sleeve_evidence_certification")
        if not source_paths[name].exists()
    ]

    ledger = payloads["paper_position_ledger"]
    sleeve_truth = payloads["sleeve_performance_truth"]
    certification = payloads["sleeve_evidence_certification"]
    exit_recommendations = payloads["exit_recommendations"]
    auto_closure = payloads["paper_outcome_auto_closure"]
    outcome_validation = payloads["outcome_validation"]

    sleeve_ids = sorted(
        {
            str(row.get("sleeve_id") or "")
            for row in (
                _safe_rows(sleeve_truth, "sleeves")
                + _safe_rows(certification, "sleeves")
                + _safe_rows(ledger, "positions")
                + _safe_rows(ledger, "open_positions")
                + _safe_rows(ledger, "closed_positions")
                + _safe_rows(exit_recommendations, "rows", "recommendations")
                + _safe_rows(auto_closure, "rows")
            )
            if str(row.get("sleeve_id") or "")
        }
    )

    rows = [
        _audit_sleeve(
            sleeve_id=sleeve_id,
            day_utc=day,
            missing_required=missing_required,
            sleeve_truth=sleeve_truth,
            certification=certification,
            ledger=ledger,
            exit_recommendations=exit_recommendations,
            auto_closure=auto_closure,
            outcome_validation=outcome_validation,
        )
        for sleeve_id in sleeve_ids
    ]
    rows.sort(key=lambda row: str(row.get("sleeve_id") or ""))

    payload = {
        "schema_id": "aegis_outcome_flow_audit",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": _now(),
        "truth_model": "CANONICAL_DERIVED_READ_MODEL_FROM_EXISTING_AUTHORITIES",
        "purpose": "Determine whether paper sleeve outcomes are accumulating closed evidence samples fast enough to certify or falsify sleeves.",
        "rules": {
            "read_model_only_no_signal_logic_change": True,
            "read_model_only_no_sleeve_logic_change": True,
            "no_trade_recommendations": True,
            "no_investable_edge_claims": True,
            "zero_closed_samples_are_not_working_evidence": True,
        },
        "thresholds": {
            "minimum_sample_threshold": MINIMUM_SAMPLE_THRESHOLD,
            "early_observation_days": EARLY_OBSERVATION_DAYS,
            "default_exit_review_age_days": DEFAULT_EXIT_REVIEW_AGE_DAYS,
            "six_month_days": SIX_MONTH_DAYS,
            "twelve_month_days": TWELVE_MONTH_DAYS,
        },
        "sleeves": rows,
        "sleeve_count": len(rows),
        "summary": _summary(rows, outcome_validation),
        "source_artifact_paths": {name: str(path) for name, path in source_paths.items() if path.exists()},
        "source_hashes": {name: _sha256(path) for name, path in source_paths.items() if path.exists()},
        "missing_required_inputs": missing_required,
        "data_quality_status": "DATA_INCOMPLETE" if missing_required else "PASS",
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_outcome_flow_audit_v1(
    *, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None
) -> dict[str, Path]:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_outcome_flow_audit_v1(truth_root=root, day_utc=day_utc))
    json_path = outcome_flow_audit_path_v1(truth_root=root, day_utc=day_utc)
    summary_path = outcome_flow_audit_summary_path_v1(truth_root=root, day_utc=day_utc)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(json_path, json.dumps(body, indent=2, sort_keys=True) + "\n")
    _atomic_write(summary_path, render_outcome_flow_audit_summary_v1(body))
    return {"json": json_path, "markdown": summary_path}


def render_outcome_flow_audit_summary_v1(payload: Mapping[str, Any]) -> str:
    rows = _safe_rows(payload, "sleeves")
    lines = [
        "# AEGIS Outcome Flow Audit V1",
        "",
        f"Day UTC: {payload.get('day_utc', '')}",
        "",
        "Paper research positions are research observations, not investment recommendations. This artifact makes no trade recommendations and no investable-edge claims.",
        "",
        "## Outcome Flow Status",
    ]
    counts = payload.get("summary", {}).get("evidence_accumulation_status_counts", {}) if isinstance(payload.get("summary"), Mapping) else {}
    for status in sorted(counts):
        lines.append(f"- {status}: {counts[status]}")
    lines.extend(["", "## Sleeve Diagnostics", ""])
    for row in rows:
        lines.append(
            f"- {row.get('sleeve_id')}: status={row.get('evidence_accumulation_status')}, "
            f"opened={row.get('total_positions_opened')}, open={row.get('currently_open_positions')}, "
            f"closed={row.get('closed_positions')}, triggered_exits={row.get('exit_rules_triggered_count')}, "
            f"estimate={row.get('estimated_time_to_minimum_sample')}"
        )
        reasons = row.get("blocking_reasons") if isinstance(row.get("blocking_reasons"), list) else []
        if reasons:
            lines.append(f"  Blocking reasons: {', '.join(str(reason) for reason in reasons)}")
    lines.append("")
    return "\n".join(lines)


def _audit_sleeve(
    *,
    sleeve_id: str,
    day_utc: str,
    missing_required: list[str],
    sleeve_truth: Mapping[str, Any],
    certification: Mapping[str, Any],
    ledger: Mapping[str, Any],
    exit_recommendations: Mapping[str, Any],
    auto_closure: Mapping[str, Any],
    outcome_validation: Mapping[str, Any],
) -> dict[str, Any]:
    truth_row = _row_for_sleeve(sleeve_truth, sleeve_id, "sleeves")
    cert_row = _row_for_sleeve(certification, sleeve_id, "sleeves")
    open_positions = _rows_for_sleeve(ledger, sleeve_id, "open_positions")
    closed_positions = _rows_for_sleeve(ledger, sleeve_id, "closed_positions")
    all_positions = _dedupe_positions(_rows_for_sleeve(ledger, sleeve_id, "positions") + open_positions + closed_positions)
    exit_rows = _rows_for_sleeve(exit_recommendations, sleeve_id, "rows", "recommendations")
    closure_rows = _rows_for_sleeve(auto_closure, sleeve_id, "rows")
    triggered_exit_rows = [row for row in exit_rows if _exit_triggered(row)]
    auto_closed_rows = [row for row in closure_rows if str(row.get("auto_closure_state") or "") == "AUTO_CLOSED_PAPER_OUTCOME"]
    closure_signal_count = len(_dedupe_positions(triggered_exit_rows + auto_closed_rows))

    open_ages = [_age_days(row, day_utc) for row in open_positions]
    open_ages = [age for age in open_ages if age is not None]
    oldest_date = _oldest_position_date(open_positions)
    newest_date = _newest_position_date(open_positions)
    policies = [row.get("policy") for row in exit_rows if isinstance(row.get("policy"), Mapping)]
    configured_rules = _configured_exit_rules(policies)
    observed_rules = sorted(
        {
            str(row.get("exit_recommendation") or row.get("exit_trigger") or reason)
            for row in exit_rows + closure_rows
            for reason in ([None] + list(row.get("reason_codes") or row.get("auto_closure_reason_codes") or []))
            if str(row.get("exit_recommendation") or row.get("exit_trigger") or reason or "")
        }
    )
    review_min_age, explicit_minimum_holding_period = _minimum_review_age(policies)
    eligible_for_review = len([age for age in open_ages if age >= review_min_age])
    blocked_from_review = max(0, len(open_positions) - eligible_for_review)
    cert_closed = _int(cert_row.get("closed_position_count"))
    closure_rate = _closure_rate(closed_positions, all_positions, day_utc)
    status, reasons = _status_and_reasons(
        missing_required=missing_required,
        open_count=len(open_positions),
        closed_count=len(closed_positions),
        cert_closed=cert_closed,
        max_open_age=max(open_ages) if open_ages else None,
        review_min_age=review_min_age,
        explicit_minimum_holding_period=explicit_minimum_holding_period,
        configured_rules=configured_rules,
        triggered_exit_count=closure_signal_count,
        eligible_for_review=eligible_for_review,
        closure_records_not_in_evidence=max(0, closure_signal_count - max(len(closed_positions), cert_closed)),
    )

    return {
        "sleeve_id": sleeve_id,
        "factory_classification": _factory_classification(truth_row, cert_row),
        "total_positions_opened": len(all_positions),
        "currently_open_positions": len(open_positions),
        "closed_positions": len(closed_positions),
        "oldest_open_position_date": oldest_date or "NOT_EVALUABLE",
        "newest_open_position_date": newest_date or "NOT_EVALUABLE",
        "average_open_age_days": _optional_decimal_text(_average_decimal(open_ages)),
        "max_open_age_days": _optional_decimal_text(max(open_ages) if open_ages else None),
        "open_position_age_distribution": _age_distribution(open_ages),
        "configured_exit_rules": configured_rules,
        "exit_rules_observed": observed_rules,
        "exit_rules_triggered_count": closure_signal_count,
        "exit_rules_never_triggered": bool(configured_rules and closure_signal_count == 0),
        "positions_eligible_for_exit_review": eligible_for_review,
        "positions_blocked_from_exit_review": blocked_from_review,
        "closure_rate": _optional_decimal_text(closure_rate),
        "closed_positions_seen_by_evidence_certification": cert_closed,
        "closure_records_not_in_evidence_certification": max(0, closure_signal_count - cert_closed),
        "evidence_accumulation_status": status,
        "estimated_time_to_minimum_sample": _estimated_time_to_minimum_sample(
            status=status,
            closed_count=len(closed_positions),
            closure_rate=closure_rate,
            closure_records_not_in_evidence=max(0, closure_signal_count - cert_closed),
        ),
        "six_to_twelve_month_outlook": _six_to_twelve_month_outlook(closure_rate, status),
        "blocking_reasons": sorted(set(reasons)) or ["NONE"],
        "recommended_non_trading_action": _recommended_non_trading_action(status),
        "diagnostic_counts": {
            "exit_recommendation_rows": len(exit_rows),
            "exit_recommendation_triggered_rows": len(triggered_exit_rows),
            "auto_closure_rows": len(closure_rows),
            "auto_closed_rows": len(auto_closed_rows),
            "outcome_validation_closed_outcome_count": _int((outcome_validation.get("summary") or {}).get("closed_outcome_count"))
            if isinstance(outcome_validation.get("summary"), Mapping)
            else 0,
        },
    }


def _status_and_reasons(
    *,
    missing_required: list[str],
    open_count: int,
    closed_count: int,
    cert_closed: int,
    max_open_age: Decimal | None,
    review_min_age: Decimal,
    explicit_minimum_holding_period: bool,
    configured_rules: list[str],
    triggered_exit_count: int,
    eligible_for_review: int,
    closure_records_not_in_evidence: int,
) -> tuple[str, list[str]]:
    reasons = [f"MISSING_REQUIRED_INPUT:{name}" for name in missing_required]
    if missing_required:
        return "NOT_EVALUABLE", reasons
    if triggered_exit_count > 0 and (closed_count == 0 or cert_closed == 0 or closure_records_not_in_evidence > 0):
        reasons.append("EXIT_RECORDS_EXIST_BUT_LEDGER_OR_EVIDENCE_HAS_ZERO_CLOSED_SAMPLES")
        return "EXIT_PIPELINE_BROKEN", reasons
    if closed_count >= MINIMUM_SAMPLE_THRESHOLD:
        return "SUFFICIENT_FLOW", reasons
    if closed_count > 0:
        reasons.append("CLOSED_SAMPLE_BELOW_MINIMUM_THRESHOLD")
        return "BUILDING_SAMPLE", reasons
    if open_count == 0:
        reasons.append("NO_OPEN_OR_CLOSED_PAPER_POSITIONS")
        return "NOT_EVALUABLE", reasons
    if not configured_rules:
        reasons.append("MISSING_EXIT_LOGIC")
        return "NO_OUTCOME_FLOW", reasons
    if max_open_age is None:
        reasons.append("OPEN_POSITION_AGE_NOT_EVALUABLE")
        return "NOT_EVALUABLE", reasons
    if explicit_minimum_holding_period and max_open_age < review_min_age:
        reasons.append("HOLDING_PERIOD_POLICY_EXCEEDS_CURRENT_OBSERVATION_AGE")
        return "LONG_HOLDING_PERIOD_EXPECTED", reasons
    if max_open_age < Decimal(EARLY_OBSERVATION_DAYS):
        reasons.append("OPEN_POSITIONS_TOO_RECENT_FOR_OUTCOME_FLOW")
        return "EARLY_OBSERVATION", reasons
    if max_open_age < review_min_age:
        reasons.append("HOLDING_PERIOD_POLICY_EXCEEDS_CURRENT_OBSERVATION_AGE")
        return "LONG_HOLDING_PERIOD_EXPECTED", reasons
    if eligible_for_review > 0 and triggered_exit_count == 0:
        reasons.append("EXIT_RULES_EXIST_BUT_HAVE_NOT_TRIGGERED")
        return "EXIT_RULES_NOT_TRIGGERING", reasons
    reasons.append("NO_CLOSED_EVIDENCE_SAMPLES")
    return "NO_OUTCOME_FLOW", reasons


def _source_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "paper_position_ledger": paper_position_ledger_path_v1(truth_root=root, day_utc=day),
        "paper_pnl_report": paper_pnl_report_path_v1(truth_root=root, day_utc=day),
        "sleeve_performance_truth": sleeve_performance_truth_path_v1(truth_root=root, day_utc=day),
        "sleeve_evidence_certification": sleeve_evidence_certification_path_v1(truth_root=root, day_utc=day),
        "exit_recommendations": root / "reports" / "aegis_exit_recommendations_v1" / day / "exit_recommendations.v1.json",
        "paper_outcome_auto_closure": root / "reports" / "aegis_paper_outcome_auto_closure_v1" / day / "paper_outcome_auto_closure.v1.json",
        "outcome_validation": root / "reports" / "aegis_outcome_validation_v1" / day / "outcome_validation.v1.json",
    }


def _summary(rows: list[Mapping[str, Any]], outcome_validation: Mapping[str, Any]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("evidence_accumulation_status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    return {
        "evidence_accumulation_status_counts": counts,
        "total_positions_opened": sum(_int(row.get("total_positions_opened")) for row in rows),
        "currently_open_positions": sum(_int(row.get("currently_open_positions")) for row in rows),
        "closed_positions_in_paper_ledger": sum(_int(row.get("closed_positions")) for row in rows),
        "closed_positions_seen_by_evidence_certification": sum(_int(row.get("closed_positions_seen_by_evidence_certification")) for row in rows),
        "exit_rules_triggered_count": sum(_int(row.get("exit_rules_triggered_count")) for row in rows),
        "outcome_validation_closed_outcome_count": _int((outcome_validation.get("summary") or {}).get("closed_outcome_count"))
        if isinstance(outcome_validation.get("summary"), Mapping)
        else 0,
        "flow_health": _flow_health(counts),
    }


def _flow_health(counts: Mapping[str, int]) -> str:
    if counts.get("EXIT_PIPELINE_BROKEN", 0):
        return "BROKEN"
    if counts.get("NO_OUTCOME_FLOW", 0) or counts.get("EXIT_RULES_NOT_TRIGGERING", 0):
        return "BLOCKED"
    if counts.get("EARLY_OBSERVATION", 0) or counts.get("LONG_HOLDING_PERIOD_EXPECTED", 0):
        return "EARLY"
    if counts.get("SUFFICIENT_FLOW", 0) or counts.get("BUILDING_SAMPLE", 0):
        return "HEALTHY"
    return "NOT_EVALUABLE"


def _configured_exit_rules(policies: list[Any]) -> list[str]:
    rules: set[str] = set()
    for policy in policies:
        if not isinstance(policy, Mapping):
            continue
        for key in (
            "stop_loss_pct",
            "take_profit_pct",
            "trailing_stop_pct",
            "target_r_multiple",
            "max_hold_days",
            "max_holding_days",
            "invalidation_rule",
            "regime_invalidation_policy",
            "review_frequency",
        ):
            if policy.get(key) not in (None, "", []):
                rules.add(key)
    return sorted(rules)


def _minimum_review_age(policies: list[Any]) -> tuple[Decimal, bool]:
    candidates: list[Decimal] = []
    for policy in policies:
        if not isinstance(policy, Mapping):
            continue
        value = _number(policy.get("minimum_holding_period") or policy.get("min_hold_days") or policy.get("expected_holding_days_default"))
        if value is not None:
            candidates.append(value)
    return (min(candidates), True) if candidates else (Decimal(DEFAULT_EXIT_REVIEW_AGE_DAYS), False)


def _exit_triggered(row: Mapping[str, Any]) -> bool:
    rec = str(row.get("exit_recommendation") or "").upper()
    trigger = str(row.get("exit_trigger") or "").upper()
    return rec.startswith("EXIT_") or bool(trigger and trigger != "NO_EXIT_RULE_TRIGGERED")


def _estimated_time_to_minimum_sample(
    *, status: str, closed_count: int, closure_rate: Decimal | None, closure_records_not_in_evidence: int
) -> str:
    remaining = max(0, MINIMUM_SAMPLE_THRESHOLD - closed_count)
    if remaining == 0:
        return "MINIMUM_SAMPLE_REACHED"
    if status == "EXIT_PIPELINE_BROKEN":
        return f"PIPELINE_REPAIR_REQUIRED_BEFORE_ESTIMATE; {closure_records_not_in_evidence} closure records are not certified evidence"
    if closure_rate is None or closure_rate <= 0:
        return "NOT_ESTIMABLE_WITH_CURRENT_ZERO_CLOSURE_RATE"
    return f"{_decimal_text(Decimal(remaining) / closure_rate)} days at observed closure rate"


def _six_to_twelve_month_outlook(closure_rate: Decimal | None, status: str) -> dict[str, Any]:
    if status == "EXIT_PIPELINE_BROKEN":
        return {"can_reach_minimum_sample_within_6_months": False, "can_reach_minimum_sample_within_12_months": False, "basis": "pipeline repair required"}
    if closure_rate is None or closure_rate <= 0:
        return {"can_reach_minimum_sample_within_6_months": False, "can_reach_minimum_sample_within_12_months": False, "basis": "observed closure rate is zero or not evaluable"}
    return {
        "can_reach_minimum_sample_within_6_months": closure_rate * Decimal(SIX_MONTH_DAYS) >= Decimal(MINIMUM_SAMPLE_THRESHOLD),
        "can_reach_minimum_sample_within_12_months": closure_rate * Decimal(TWELVE_MONTH_DAYS) >= Decimal(MINIMUM_SAMPLE_THRESHOLD),
        "basis": "observed ledger closure rate",
    }


def _recommended_non_trading_action(status: str) -> str:
    return {
        "EXIT_PIPELINE_BROKEN": "AUDIT_CLOSURE_TO_EVIDENCE_PIPELINE",
        "NO_OUTCOME_FLOW": "AUDIT_EXIT_LOGIC_COVERAGE",
        "EXIT_RULES_NOT_TRIGGERING": "REVIEW_EXIT_RULE_TRIGGER_DIAGNOSTICS",
        "LONG_HOLDING_PERIOD_EXPECTED": "CONTINUE_OUTCOME_FLOW_MONITORING",
        "EARLY_OBSERVATION": "CONTINUE_OUTCOME_FLOW_MONITORING",
        "BUILDING_SAMPLE": "CONTINUE_OUTCOME_FLOW_MONITORING",
        "SUFFICIENT_FLOW": "CONTINUE_OUTCOME_FLOW_MONITORING",
        "NOT_EVALUABLE": "REPAIR_AUDIT_INPUTS",
    }.get(status, "CONTINUE_OUTCOME_FLOW_MONITORING")


def _closure_rate(closed_positions: list[Mapping[str, Any]], all_positions: list[Mapping[str, Any]], day_utc: str) -> Decimal | None:
    if not closed_positions:
        return Decimal("0")
    dates = [_position_date(row) for row in all_positions]
    dates = [value for value in dates if value is not None]
    if not dates:
        return None
    span_days = max(Decimal("1"), Decimal((date.fromisoformat(day_utc) - min(dates)).days + 1))
    return Decimal(len(closed_positions)) / span_days


def _age_days(row: Mapping[str, Any], day_utc: str) -> Decimal | None:
    position_date = _position_date(row)
    if position_date is None:
        return None
    return Decimal((date.fromisoformat(day_utc) - position_date).days)


def _position_date(row: Mapping[str, Any]) -> date | None:
    for key in ("originating_day", "entry_time", "timestamp_utc", "timestamp", "promotion_timestamp"):
        value = str(row.get(key) or "").strip()
        if not value:
            continue
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        except ValueError:
            try:
                return date.fromisoformat(value[:10])
            except ValueError:
                continue
    return None


def _oldest_position_date(rows: list[Mapping[str, Any]]) -> str:
    values = [_position_date(row) for row in rows]
    values = [value for value in values if value is not None]
    return min(values).isoformat() if values else ""


def _newest_position_date(rows: list[Mapping[str, Any]]) -> str:
    values = [_position_date(row) for row in rows]
    values = [value for value in values if value is not None]
    return max(values).isoformat() if values else ""


def _age_distribution(ages: list[Decimal]) -> dict[str, int]:
    return {
        "0_to_4_days": len([age for age in ages if age < 5]),
        "5_to_20_days": len([age for age in ages if Decimal(5) <= age <= Decimal(20)]),
        "21_to_60_days": len([age for age in ages if Decimal(21) <= age <= Decimal(60)]),
        "over_60_days": len([age for age in ages if age > Decimal(60)]),
    }


def _factory_classification(*rows: Mapping[str, Any]) -> str:
    for row in rows:
        raw = str(row.get("factory_classification") or row.get("factory") or row.get("sleeve_factory") or "").strip().upper()
        if raw:
            return raw
    return "UNCLASSIFIED"


def _row_for_sleeve(payload: Mapping[str, Any], sleeve_id: str, *keys: str) -> dict[str, Any]:
    for row in _safe_rows(payload, *keys):
        if str(row.get("sleeve_id") or "") == sleeve_id:
            return row
    return {}


def _rows_for_sleeve(payload: Mapping[str, Any], sleeve_id: str, *keys: str) -> list[dict[str, Any]]:
    return [row for row in _safe_rows(payload, *keys) if str(row.get("sleeve_id") or "") == sleeve_id]


def _dedupe_positions(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for idx, row in enumerate(rows):
        key = str(row.get("position_id") or row.get("paper_position_id") or row.get("candidate_id") or idx)
        out[key] = dict(row)
    return list(out.values())


def _safe_rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        rows = payload.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _number(value: Any) -> Decimal | None:
    try:
        if value in (None, "") or isinstance(value, bool):
            return None
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _average_decimal(values: list[Decimal]) -> Decimal | None:
    return sum(values, Decimal("0")) / Decimal(len(values)) if values else None


def _decimal_text(value: Decimal | int | float | str) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value or "0"))
    normalized = value.quantize(Decimal("0.000001"))
    return format(normalized.normalize(), "f")


def _optional_decimal_text(value: Decimal | int | float | str | None) -> str:
    if value is None:
        return "NOT_EVALUABLE"
    return _decimal_text(value)


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def _atomic_write(path: Path, body: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(body, encoding="utf-8")
    os.replace(tmp, path)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
