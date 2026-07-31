from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.candidate_to_paper_lifecycle_v1 import (
    build_candidate_to_paper_lifecycle_v1,
    normalized_candidate_to_paper_lifecycle_v1,
)
from ops.aegis.future_target_day_audit_guard_v1 import future_target_day_audit_guard_path_v1, read_future_target_day_audit_guard_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, write_json_v1

REPORT_FAMILY = "aegis_candidate_to_paper_self_check_v1"


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def build_candidate_to_paper_self_check_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    lifecycle_path, lifecycle = latest_json_v1(root, "aegis_candidate_to_paper_lifecycle_v1", day_utc, "candidate_to_paper_lifecycle.v1.json")
    ledger_path, ledger = latest_json_v1(root, "aegis_paper_position_ledger_v1", day_utc, "paper_position_ledger.v1.json")
    outcomes_path, outcomes = latest_json_v1(root, "aegis_outcome_registry_v1", day_utc, "outcome_registry.v1.json")
    samples_path, samples = latest_json_v1(root, "aegis_validation_samples_v1", day_utc, "validation_samples.v1.json")
    guard = read_future_target_day_audit_guard_v1(truth_root=root, day_utc=day_utc)
    future_target_day_guarded = guard.get("is_future_target_day") is True and guard.get("guard_status") == "TARGET_DAY_IN_FUTURE_GUARDED"
    failures: list[dict[str, Any]] = []
    guarded_skips: list[dict[str, Any]] = []
    rows = [row for row in _safe_list(lifecycle.get("rows")) if isinstance(row, dict)]
    summary = lifecycle.get("summary") if isinstance(lifecycle.get("summary"), dict) else {}
    if int(summary.get("valid_candidate_contract_count") or 0) != len(rows):
        failures.append({"failure_code": "VALID_CANDIDATE_LIFECYCLE_COUNT_MISMATCH", "expected": summary.get("valid_candidate_contract_count"), "actual": len(rows)})
    for row in rows:
        candidate_id = _text(row.get("candidate_id"))
        if not candidate_id:
            failures.append({"failure_code": "LIFECYCLE_ROW_MISSING_CANDIDATE_ID"})
        if not _text(row.get("paper_position_id")) and not _safe_list(row.get("blocker_reason_codes")):
            failures.append({"failure_code": "CANDIDATE_BLOCKED_FROM_PAPER_MISSING_REASON_CODES", "candidate_id": candidate_id})
        for key in ("sleeve_id", "hypothesis_id", "thesis_id"):
            if not _text(row.get(key)):
                failures.append({"failure_code": "CANDIDATE_LIFECYCLE_MISSING_LINEAGE", "candidate_id": candidate_id, "field": key})
    seen_positions: set[str] = set()
    for position in _safe_list(ledger.get("positions")) or [*_safe_list(ledger.get("open_positions")), *_safe_list(ledger.get("closed_positions"))]:
        if not isinstance(position, dict):
            continue
        position_id = _text(position.get("position_id"))
        candidate_id = _text(position.get("candidate_id") or position.get("linked_candidate_id"))
        if position_id in seen_positions:
            failures.append({"failure_code": "DUPLICATE_PAPER_POSITION_ID", "position_id": position_id})
        seen_positions.add(position_id)
        for key in ("candidate_id", "sleeve_id", "hypothesis_id", "thesis_id"):
            value = candidate_id if key == "candidate_id" else _text(position.get(key))
            if not value:
                failures.append({"failure_code": "PAPER_POSITION_MISSING_LINEAGE", "position_id": position_id, "field": key})
    outcome_by_position = {_text(row.get("position_id")): row for row in _safe_list(outcomes.get("outcomes")) if isinstance(row, dict)}
    sample_by_position = {_text(row.get("position_id")): row for row in _safe_list(samples.get("samples")) if isinstance(row, dict)}
    for position in _safe_list(ledger.get("positions")) or [*_safe_list(ledger.get("open_positions")), *_safe_list(ledger.get("closed_positions"))]:
        if not isinstance(position, dict):
            continue
        position_id = _text(position.get("position_id"))
        if position_id and position_id not in outcome_by_position:
            failures.append({"failure_code": "PAPER_POSITION_MISSING_OUTCOME_REGISTRY_ROW", "position_id": position_id})
        auto_promoted = str(position.get("current_state") or "").upper() == "AUTO_PROMOTED_TO_PAPER_TRACKING" or str(position.get("paper_tracking_mode") or "").upper() == "AUTO_PROMOTED_RESEARCH_OBSERVATION"
        if auto_promoted:
            for key in ("candidate_id", "hypothesis_id", "thesis_id", "sleeve_id"):
                value = candidate_id if key == "candidate_id" else _text(position.get(key))
                if not value:
                    failures.append({"failure_code": "AUTO_PROMOTED_POSITION_MISSING_LINEAGE", "position_id": position_id, "field": key})
            if not _safe_list(position.get("auto_promotion_reason_codes")):
                failures.append({"failure_code": "AUTO_PROMOTED_POSITION_MISSING_REASON_CODES", "position_id": position_id})
            for key, code in (("trade_advice_allowed", "AUTO_PROMOTION_ENABLED_TRADE_ADVICE"), ("manual_capture_allowed", "AUTO_PROMOTION_ENABLED_MANUAL_CAPTURE"), ("broker_execution_allowed", "AUTO_PROMOTION_ENABLED_BROKER_EXECUTION"), ("autonomous_execution_allowed", "AUTO_PROMOTION_ENABLED_AUTONOMOUS_EXECUTION")):
                if position.get(key) is True:
                    failures.append({"failure_code": code, "position_id": position_id})
            if position_id and position_id not in sample_by_position:
                if future_target_day_guarded:
                    guarded_skips.append({"check_code": "AUTO_PROMOTED_POSITION_MISSING_VALIDATION_SAMPLE_ROW", "position_id": position_id, "guard_status": "TARGET_DAY_IN_FUTURE_GUARDED", "blocker_code": "TARGET_DAY_IN_FUTURE"})
                else:
                    failures.append({"failure_code": "AUTO_PROMOTED_POSITION_MISSING_VALIDATION_SAMPLE_ROW", "position_id": position_id})
    if lifecycle:
        rebuilt = build_candidate_to_paper_lifecycle_v1(truth_root=root, day_utc=day_utc)
        if normalized_candidate_to_paper_lifecycle_v1(rebuilt) != normalized_candidate_to_paper_lifecycle_v1(lifecycle):
            failures.append({"failure_code": "NON_DETERMINISTIC_OUTPUT"})
    return {
        "schema_id": "aegis_candidate_to_paper_self_check",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "ok": not failures,
        "failure_count": len(failures),
        "failures": failures,
        "future_target_day_guarded": future_target_day_guarded,
        "future_target_day_guard_status": guard.get("guard_status") or "",
        "guarded_skip_count": len(guarded_skips),
        "guarded_skips": guarded_skips,
        "source_artifacts": {"lifecycle": str(lifecycle_path or ""), "paper_position_ledger": str(ledger_path or ""), "outcome_registry": str(outcomes_path or ""), "validation_samples": str(samples_path or ""), "future_target_day_guard": str(future_target_day_audit_guard_path_v1(truth_root=root, day_utc=day_utc))},
        "safety": {"trade_advice_allowed": False, "manual_capture_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False},
    }


def write_candidate_to_paper_self_check_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "self_check.v1.json", payload)
    return {"json": str(path)}
