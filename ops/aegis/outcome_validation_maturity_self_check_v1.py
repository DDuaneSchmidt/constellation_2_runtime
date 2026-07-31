from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.hypothesis_outcome_ledger_v1 import build_hypothesis_outcome_ledger_v1
from ops.aegis.hypothesis_state_machine_v1 import build_hypothesis_state_machine_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.outcome_registry_v1 import build_outcome_registry_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1
from ops.aegis.statistical_sufficiency_engine_v1 import build_statistical_sufficiency_v1
from ops.aegis.validation_sample_generator_v1 import build_validation_samples_v1

REPORT_FAMILY = "aegis_outcome_validation_maturity_self_check_v1"
REPORT_FILENAME = "self_check.v1.json"
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False}
RESOLVED = {"CLOSED_WIN", "CLOSED_LOSS", "CLOSED_FLAT", "EXPIRED", "INVALIDATED"}


def outcome_validation_maturity_self_check_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def outcome_validation_maturity_failures_v1(outcomes: Mapping[str, Any], samples: Mapping[str, Any], ledger: Mapping[str, Any], sufficiency: Mapping[str, Any], states: Mapping[str, Any]) -> list[dict[str, Any]]:
    failures=[]
    outcome_rows=[r for r in outcomes.get("outcomes") or [] if isinstance(r, Mapping)]
    sample_rows=[r for r in samples.get("samples") or [] if isinstance(r, Mapping)]
    sample_by_outcome={text_v1(r.get("outcome_id")): r for r in sample_rows}
    for row in outcome_rows:
        if not text_v1(row.get("position_id")):
            failures.append(_failure("outcome_missing_position", "outcome row lacks position_id", row))
        if row.get("outcome_state") == "UNKNOWN_BLOCKED" and not row.get("blocker_reasons"):
            failures.append(_failure("unknown_blocked_without_reason", "UNKNOWN_BLOCKED lacks blocker reasons", row))
        if row.get("outcome_state") in RESOLVED and text_v1(row.get("outcome_id")) not in sample_by_outcome:
            failures.append(_failure("resolved_outcome_missing_sample", "resolved outcome lacks validation sample", row))
        if text_v1(row.get("outcome_state")).startswith("CLOSED_") and row.get("realized_return") is None:
            failures.append(_failure("closed_position_missing_realized_return", "closed outcome lacks realized return", row))
    for row in sample_rows:
        if not all(text_v1(row.get(k)) for k in ("hypothesis_id", "sleeve_id", "candidate_id", "position_id", "outcome_id")):
            failures.append(_failure("validation_sample_broken_lineage", "validation sample missing required linkage", row))
        if text_v1(row.get("sample_state")) == "INCLUDED" and row.get("return_value") is None:
            failures.append(_failure("included_sample_missing_return", "included sample lacks return value", row))
    ledger_h={text_v1(r.get("hypothesis_id")) for r in ledger.get("hypotheses") or [] if isinstance(r, Mapping)}
    suff_h={text_v1(r.get("hypothesis_id")) for r in sufficiency.get("hypotheses") or [] if isinstance(r, Mapping)}
    for hid in suff_h:
        if hid not in ledger_h:
            failures.append(_failure("sufficiency_without_ledger", f"{hid} has sufficiency row but no ledger row", {"hypothesis_id": hid}))
    for row in sufficiency.get("hypotheses") or []:
        if isinstance(row, Mapping) and not row.get("state_reason_codes"):
            failures.append(_failure("sufficiency_missing_reason_codes", "sufficiency row lacks reason codes", row))
    for row in states.get("transitions") or []:
        if isinstance(row, Mapping) and text_v1(row.get("transition_reason")).startswith("STATISTICAL_SUFFICIENCY") and not (row.get("evidence_artifacts") or row.get("evidence_artifact_paths")):
            failures.append(_failure("transition_missing_evidence", "hypothesis transition lacks evidence artifacts", row))
    return failures


def build_outcome_validation_maturity_self_check_v1(*, truth_root: Path | str, day_utc: str, outcomes: Mapping[str, Any] | None = None, samples: Mapping[str, Any] | None = None, ledger: Mapping[str, Any] | None = None, sufficiency: Mapping[str, Any] | None = None, states: Mapping[str, Any] | None = None) -> dict[str, Any]:
    outcomes_payload=dict(outcomes or build_outcome_registry_v1(truth_root=truth_root, day_utc=day_utc))
    samples_payload=dict(samples or build_validation_samples_v1(truth_root=truth_root, day_utc=day_utc, outcome_registry=outcomes_payload))
    ledger_payload=dict(ledger or build_hypothesis_outcome_ledger_v1(truth_root=truth_root, day_utc=day_utc, outcome_registry=outcomes_payload, validation_samples=samples_payload))
    suff_payload=dict(sufficiency or build_statistical_sufficiency_v1(truth_root=truth_root, day_utc=day_utc, ledger=ledger_payload))
    states_payload=dict(states or build_hypothesis_state_machine_v1(truth_root=truth_root, day_utc=day_utc, sufficiency=suff_payload))
    failures=outcome_validation_maturity_failures_v1(outcomes_payload, samples_payload, ledger_payload, suff_payload, states_payload)
    payload={"schema_id":"aegis_outcome_validation_maturity_self_check","schema_version":"v1","artifact_id":REPORT_FAMILY,"day_utc":str(day_utc),"generated_at":_now(),"ok":not failures,"failure_count":len(failures),"failures":failures,"checks":["paper_position_has_outcome","resolved_outcome_has_validation_sample","validation_sample_lineage","closed_position_return_data","unknown_blocked_has_reasons","hypothesis_has_ledger_row","sufficiency_has_reason_codes","hypothesis_transition_has_evidence"],"safety":dict(SAFETY),**SAFETY}
    payload["content_hash"]=stable_hash_v1({**payload,"generated_at":"","content_hash":""})
    return payload


def write_outcome_validation_maturity_self_check_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(outcome_validation_maturity_self_check_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_outcome_validation_maturity_self_check_v1(truth_root=truth_root, day_utc=day_utc))


def _failure(code: str, message: str, row: Mapping[str, Any]) -> dict[str, Any]:
    return {"check_id": code, "message": message, "row_ref": {k: row.get(k) for k in ("hypothesis_id", "sleeve_id", "candidate_id", "position_id", "outcome_id") if row.get(k)}}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
