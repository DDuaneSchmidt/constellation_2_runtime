from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.future_target_day_audit_guard_v1 import future_target_day_audit_guard_path_v1, read_future_target_day_audit_guard_v1
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.outcome_registry_v1 import build_outcome_registry_v1, outcome_registry_path_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1

REPORT_FAMILY = "aegis_validation_samples_v1"
REPORT_FILENAME = "validation_samples.v1.json"
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False}
RESOLVED = {"CLOSED_WIN", "CLOSED_LOSS", "CLOSED_FLAT", "EXPIRED", "INVALIDATED"}


def validation_samples_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def build_validation_samples_v1(*, truth_root: Path | str, day_utc: str, outcome_registry: Mapping[str, Any] | None = None) -> dict[str, Any]:
    outcomes_payload = dict(outcome_registry or build_outcome_registry_v1(truth_root=truth_root, day_utc=day_utc))
    guard = read_future_target_day_audit_guard_v1(truth_root=truth_root, day_utc=str(day_utc))
    future_guarded = guard.get("guard_status") == "TARGET_DAY_IN_FUTURE_GUARDED" and guard.get("is_future_target_day") is True
    samples = [] if future_guarded else [_sample_from_outcome(row) for row in outcomes_payload.get("outcomes") or [] if isinstance(row, Mapping)]
    payload = {"schema_id": "aegis_validation_samples", "schema_version": "v1", "artifact_id": REPORT_FAMILY, "day_utc": str(day_utc), "generated_at": _now(), "samples": samples, "summary": {"total_samples": len(samples), "included_samples": sum(1 for row in samples if row["sample_state"] == "INCLUDED"), "excluded_samples": sum(1 for row in samples if row["sample_state"] != "INCLUDED"), "closed_or_resolved_outcomes": sum(1 for row in outcomes_payload.get("outcomes") or [] if isinstance(row, Mapping) and row.get("outcome_state") in RESOLVED), "future_target_day_guarded": future_guarded, "future_target_day_guard_status": text_v1(guard.get("guard_status")), "remaining_blocker": "TARGET_DAY_IN_FUTURE" if future_guarded else "NONE"}, "source_artifact_paths": {"outcome_registry": str(outcome_registry_path_v1(truth_root=truth_root, day_utc=day_utc)), "future_target_day_guard": str(future_target_day_audit_guard_path_v1(truth_root=truth_root, day_utc=str(day_utc)))}, "safety": dict(SAFETY), **SAFETY}
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_validation_samples_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(validation_samples_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_validation_samples_v1(truth_root=truth_root, day_utc=day_utc))


def _sample_from_outcome(row: Mapping[str, Any]) -> dict[str, Any]:
    state, inclusion, inclusion_reason, exclusion = _sample_state(row)
    return_value = row.get("realized_return") if state == "INCLUDED" else None
    sample_id = "sample_" + stable_hash_v1({"outcome_id": row.get("outcome_id"), "position_id": row.get("position_id")})[:24]
    return {"sample_id": sample_id, "thesis_id": text_v1(row.get("thesis_id")), "hypothesis_id": text_v1(row.get("hypothesis_id")), "sleeve_id": text_v1(row.get("sleeve_id")), "candidate_id": text_v1(row.get("candidate_id")), "position_id": text_v1(row.get("position_id")), "outcome_id": text_v1(row.get("outcome_id")), "sample_state": state, "inclusion_status": inclusion, "inclusion_reason": inclusion_reason, "exclusion_reason": exclusion, "return_value": return_value, "benchmark_return": 0.0 if state == "INCLUDED" else None, "excess_return": return_value if state == "INCLUDED" and return_value is not None else None, "holding_period_days": row.get("holding_period_days"), "market_regime_tag": "UNAVAILABLE", "source_artifacts": row.get("source_artifacts") or [], "source_hashes": row.get("source_hashes") or {}}


def _sample_state(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    if not all(text_v1(row.get(k)) for k in ("hypothesis_id", "sleeve_id", "candidate_id", "position_id", "outcome_id")):
        return "EXCLUDED_BAD_LINEAGE", "EXCLUDED", "", "BAD_LINEAGE"
    outcome_state = text_v1(row.get("outcome_state"))
    if outcome_state == "OPEN":
        return "EXCLUDED_OPEN_POSITION", "EXCLUDED", "", "OPEN_POSITION_NOT_RESOLVED"
    if outcome_state == "INVALIDATED":
        return "EXCLUDED_INVALIDATED", "EXCLUDED", "", "OUTCOME_INVALIDATED"
    if outcome_state == "UNKNOWN_BLOCKED":
        return "EXCLUDED_OTHER", "EXCLUDED", "", ",".join(row.get("blocker_reasons") or ["UNKNOWN_BLOCKED"])
    if outcome_state in RESOLVED:
        if row.get("realized_return") is None and outcome_state.startswith("CLOSED_"):
            return "EXCLUDED_MISSING_EXIT", "EXCLUDED", "", "MISSING_REALIZED_RETURN"
        return "INCLUDED", "INCLUDED", "RESOLVED_OUTCOME_WITH_RETURN", ""
    return "EXCLUDED_OTHER", "EXCLUDED", "", "UNHANDLED_OUTCOME_STATE"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
