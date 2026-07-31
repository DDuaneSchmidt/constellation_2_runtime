from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.hypothesis_registry_v1 import build_hypothesis_registry_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1

REPORT_FAMILY = "aegis_hypothesis_state_v1"
REPORT_FILENAME = "hypothesis_states.v1.json"
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False}


def hypothesis_state_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def transition_hypothesis_state_v1(row: Mapping[str, Any], sufficiency: Mapping[str, Any] | None = None, sufficiency_artifact: str = "") -> dict[str, Any]:
    prior = text_v1(row.get("status") or "PROPOSED") or "PROPOSED"
    candidates = len(row.get("linked_candidates") or [])
    positions = len(row.get("linked_paper_positions") or [])
    samples = len(row.get("linked_validation_samples") or []) + len(row.get("linked_validation_results") or [])
    outcomes = len(row.get("linked_outcomes") or [])
    confidence = float(row.get("confidence_score") or 0.0)
    validation_state = text_v1(row.get("validation_state"))
    suff = sufficiency if isinstance(sufficiency, Mapping) else {}
    suff_state = text_v1(suff.get("sufficiency_state"))
    if suff_state in {"VALIDATION_READY", "VALIDATED", "DISPROVEN", "DEGRADED"}:
        new = {"VALIDATION_READY": "VALIDATION_READY", "VALIDATED": "VALIDATED", "DISPROVEN": "DISPROVEN", "DEGRADED": "DEGRADED"}[suff_state]
        reason = f"STATISTICAL_SUFFICIENCY_{suff_state}"
    elif validation_state in {"SUPPORTED", "VALIDATED"} and samples >= 6:
        new = "VALIDATED"
        reason = "SUPPORTED_VALIDATION_WITH_SAMPLE_THRESHOLD"
    elif validation_state in {"FAILED", "DISPROVEN"}:
        new = "DISPROVEN"
        reason = "VALIDATION_FAILURE"
    elif samples >= 6:
        new = "VALIDATION_READY"
        reason = "VALIDATION_SAMPLE_THRESHOLD_MET"
    elif positions or samples:
        new = "ACCUMULATING_EVIDENCE"
        reason = "PAPER_OR_VALIDATION_EVIDENCE_PRESENT"
    elif candidates:
        new = "ACCUMULATING_EVIDENCE"
        reason = "CANDIDATE_EVIDENCE_PRESENT"
    else:
        new = "INVESTIGATING"
        reason = "NO_CANDIDATE_OR_SAMPLE_EVIDENCE_YET"
    if confidence < 5 and candidates == 0 and positions == 0 and prior not in {"PROPOSED", "INVESTIGATING"}:
        new = "DEGRADED"
        reason = "STALLED_EVIDENCE_ACCUMULATION"
    return _transition(prior, new, reason, ["hypothesis_registry.v1.json", "statistical_sufficiency.v1.json"] if sufficiency_artifact else ["hypothesis_registry.v1.json"], row, sufficiency_artifact=sufficiency_artifact)


def sleeve_relationship_state_v1(row: Mapping[str, Any], hypothesis_state: str, total_pnl: float | None = None) -> str:
    if hypothesis_state in {"DISPROVEN", "RETIRED"}:
        return "RETIRE_CANDIDATE"
    if hypothesis_state == "DEGRADED":
        return "WATCH"
    if hypothesis_state == "VALIDATED" and (total_pnl or 0.0) > 0:
        return "SCALE_CANDIDATE"
    if hypothesis_state in {"VALIDATION_READY", "ACCUMULATING_EVIDENCE"}:
        return "ACTIVE_VALIDATION" if len(row.get("linked_validation_results") or []) else "PAPER_TESTING"
    return "CANDIDATE_IMPLEMENTATION"


def build_hypothesis_state_machine_v1(*, truth_root: Path | str, day_utc: str, registry: Mapping[str, Any] | None = None, sufficiency: Mapping[str, Any] | None = None) -> dict[str, Any]:
    registry_payload = dict(registry or build_hypothesis_registry_v1(truth_root=truth_root, day_utc=day_utc))
    suff_path = report_path_v1(truth_root, "aegis_statistical_sufficiency_v1", day_utc, "statistical_sufficiency.v1.json")
    suff_payload = dict(sufficiency or read_json_v1(suff_path))
    suff_by_h = {text_v1(row.get("hypothesis_id")): row for row in suff_payload.get("hypotheses") or [] if isinstance(row, Mapping)}
    transitions = []
    states = []
    for row in registry_payload.get("hypotheses") or []:
        if not isinstance(row, Mapping):
            continue
        transition = transition_hypothesis_state_v1(row, suff_by_h.get(text_v1(row.get("hypothesis_id"))), str(suff_path) if suff_by_h else "")
        transitions.append(transition)
        states.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "thesis_id": row.get("thesis_id"),
            "prior_state": transition["prior_state"],
            "state": transition["new_state"],
            "validation_state": row.get("validation_state"),
            "linked_sleeves": row.get("linked_sleeves") or [],
            "relationship_states": [{"sleeve_id": sleeve, "relationship_state": sleeve_relationship_state_v1(row, transition["new_state"])} for sleeve in row.get("linked_sleeves") or []],
        })
    payload = {
        "schema_id": "aegis_hypothesis_state",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": str(day_utc),
        "generated_at": _now(),
        "hypothesis_states": states,
        "transitions": transitions,
        "summary": {"hypothesis_count": len(states), "transition_count": len(transitions), "validation_ready": sum(1 for r in states if r["state"] == "VALIDATION_READY"), "validated": sum(1 for r in states if r["state"] == "VALIDATED"), "degraded": sum(1 for r in states if r["state"] == "DEGRADED"), "retired": sum(1 for r in states if r["state"] == "RETIRED")},
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_hypothesis_state_machine_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(hypothesis_state_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_hypothesis_state_machine_v1(truth_root=truth_root, day_utc=day_utc))


def _transition(prior: str, new: str, reason: str, required_evidence: list[str], row: Mapping[str, Any], sufficiency_artifact: str = "") -> dict[str, Any]:
    source_paths = sorted({str(e.get("source_artifact")) for e in row.get("evidence_objects") or [] if isinstance(e, Mapping) and e.get("source_artifact")})
    if sufficiency_artifact:
        source_paths = sorted(set(source_paths + [sufficiency_artifact]))
    return {"hypothesis_id": row.get("hypothesis_id"), "prior_state": prior, "new_state": new, "transition_reason": reason, "required_evidence": required_evidence, "evidence_artifacts": source_paths, "evidence_artifact_paths": source_paths, "sufficiency_artifact": sufficiency_artifact, "timestamp": _now(), "generated_at": _now(), "source_hashes": {path: "" for path in source_paths}}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
