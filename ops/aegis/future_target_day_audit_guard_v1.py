from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1

FAMILY = "aegis_future_target_day_audit_guard_v1"
FILENAME = "future_target_day_audit_guard.v1.json"
POLICY_VERSION = "AEGIS_FUTURE_TARGET_DAY_AUDIT_GUARD_V1"

SAFETY = {
    "research_only": True,
    "read_only": True,
    "no_mark_certification": True,
    "no_outcome_creation": True,
    "no_validation_sample_creation": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "safety_gates_changed": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def future_target_day_audit_guard_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_future_target_day_audit_guard_v1(
    *, truth_root: Path | str, day_utc: str, actual_runtime_date: str | None = None, computed_at_utc: str | None = None
) -> dict[str, Any]:
    target = _parse_day(day_utc)
    actual = _parse_day(actual_runtime_date or datetime.now(UTC).strftime("%Y-%m-%d"))
    is_future = target > actual
    delta = (target - actual).days
    if is_future:
        audit_mode = "FUTURE_TARGET_DAY_PROVISIONAL"
        guard_status = "TARGET_DAY_IN_FUTURE_GUARDED"
        blocker_code = "TARGET_DAY_IN_FUTURE"
        blocker_reason = "Target day is later than the actual runtime date, so final target-day marks, deterministic outcome closure, and validation sample creation are not available yet."
        remaining = "TARGET_DAY_IN_FUTURE"
        guarded_checks = ["final_mark_certification", "outcome_closure", "validation_sample_creation", "evidence_lineage_final_mark_coverage", "future_target_day_sleeve_execution"]
        skipped_checks = ["aegis:run-sleeves-now"]
        downgraded_checks = ["evidence_lineage_mark_coverage_integrity"]
        strict_checks = ["runtime_truth_kernel", "candidate_lineage", "sleeve_attribution", "safety_gates", "broker_execution_policy"]
        final_allowed = False
        outcome_allowed = False
        lineage_allowed = False
    else:
        audit_mode = "STRICT_FINAL"
        guard_status = "NOT_APPLICABLE_TARGET_DAY_CURRENT_OR_PAST"
        blocker_code = "NONE"
        blocker_reason = "Target day is current or historical; strict final audit checks remain enforced."
        remaining = "NONE"
        guarded_checks = []
        skipped_checks = []
        downgraded_checks = []
        strict_checks = ["final_mark_certification", "outcome_closure", "validation_sample_creation", "evidence_lineage_final_mark_coverage", "runtime_truth_kernel", "candidate_lineage", "sleeve_attribution", "safety_gates"]
        final_allowed = True
        outcome_allowed = True
        lineage_allowed = True
    body: dict[str, Any] = {
        "schema_id": "aegis_future_target_day_audit_guard",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "target_day": target.isoformat(),
        "day_utc": target.isoformat(),
        "actual_runtime_date": actual.isoformat(),
        "computed_at_utc": computed_at_utc or _now(),
        "is_future_target_day": is_future,
        "future_day_delta": max(delta, 0),
        "final_mark_certification_allowed": final_allowed,
        "outcome_closure_allowed": outcome_allowed,
        "validation_sample_creation_allowed": outcome_allowed,
        "evidence_lineage_final_mark_check_allowed": lineage_allowed,
        "audit_mode": audit_mode,
        "guard_status": guard_status,
        "guarded_checks": guarded_checks,
        "skipped_checks": skipped_checks,
        "downgraded_checks": downgraded_checks,
        "strict_checks_remaining": strict_checks,
        "remaining_blocker": remaining,
        "blocker_code": blocker_code,
        "blocker_reason": blocker_reason,
        "owner": "AEGIS_SYSTEM" if is_future else "NONE",
        "david_action_required": False,
        **SAFETY,
        "safety": dict(SAFETY),
    }
    body["summary"] = {key: body[key] for key in (
        "target_day", "actual_runtime_date", "is_future_target_day", "future_day_delta",
        "final_mark_certification_allowed", "outcome_closure_allowed", "evidence_lineage_final_mark_check_allowed",
        "audit_mode", "guard_status", "guarded_checks", "skipped_checks", "downgraded_checks",
        "strict_checks_remaining", "remaining_blocker", "blocker_code", "blocker_reason", "owner", "david_action_required"
    )}
    body["content_hash"] = stable_hash_v1(_without_time(body))
    return body


def write_future_target_day_audit_guard_v1(*, truth_root: Path | str, day_utc: str, actual_runtime_date: str | None = None, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_future_target_day_audit_guard_v1(truth_root=truth_root, day_utc=day_utc, actual_runtime_date=actual_runtime_date)
    return write_json_v1(future_target_day_audit_guard_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def read_future_target_day_audit_guard_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    return read_json_v1(future_target_day_audit_guard_path_v1(truth_root=truth_root, day_utc=day_utc))


def is_future_target_day_guarded_v1(*, truth_root: Path | str, day_utc: str) -> bool:
    payload = read_future_target_day_audit_guard_v1(truth_root=truth_root, day_utc=day_utc)
    return payload.get("guard_status") == "TARGET_DAY_IN_FUTURE_GUARDED" and payload.get("is_future_target_day") is True


def _parse_day(value: str) -> date:
    return date.fromisoformat(str(value)[:10])


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _without_time(payload: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    out.pop("computed_at_utc", None)
    out.pop("content_hash", None)
    return out
