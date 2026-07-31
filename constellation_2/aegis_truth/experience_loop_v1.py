from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
from typing import Any, Literal

from constellation_2.aegis_truth.evidence_event_v1 import canonical_json, sha256_text, utc_now_iso

SCHEMA_VERSION = "aegis_experience_loop.v1"
DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
LEDGER_RELATIVE = Path("events/aegis_experience_loop_v1")
CERTIFICATION_RELATIVE = Path("reports/aegis_experience_loop_v1")

VALID_RECORD_TYPES = {
    "AttentionDecision",
    "Prediction",
    "Outcome",
    "Regret",
    "CalibrationRecord",
    "BehaviorChange",
    "ExperienceEvent",
}


@dataclass(frozen=True)
class AttentionDecision:
    schema_version: str
    record_type: Literal["AttentionDecision"]
    decision_id: str
    subject_id: str
    target_day: str
    decided_at_utc: str
    attention_scope: str
    selected_action: str
    rationale: str
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True)
class Prediction:
    schema_version: str
    record_type: Literal["Prediction"]
    prediction_id: str
    decision_id: str
    target_day: str
    predicted_at_utc: str
    prediction_statement: str
    expected_outcome: str
    confidence: float
    horizon_days: int


@dataclass(frozen=True)
class Outcome:
    schema_version: str
    record_type: Literal["Outcome"]
    outcome_id: str
    prediction_id: str
    target_day: str
    observed_at_utc: str
    outcome_statement: str
    outcome_status: Literal["UNKNOWN", "MATCHED", "MISSED", "PARTIAL"]
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True)
class Regret:
    schema_version: str
    record_type: Literal["Regret"]
    regret_id: str
    outcome_id: str
    target_day: str
    assessed_at_utc: str
    regret_status: Literal["NONE", "LOW", "MEDIUM", "HIGH"]
    regret_statement: str


@dataclass(frozen=True)
class CalibrationRecord:
    schema_version: str
    record_type: Literal["CalibrationRecord"]
    calibration_record_id: str
    regret_id: str
    target_day: str
    calibrated_at_utc: str
    prior_confidence: float
    calibrated_confidence: float
    calibration_note: str


@dataclass(frozen=True)
class BehaviorChange:
    schema_version: str
    record_type: Literal["BehaviorChange"]
    behavior_change_id: str
    calibration_record_id: str
    target_day: str
    changed_at_utc: str
    change_statement: str
    activation_rule: str


@dataclass(frozen=True)
class ExperienceEvent:
    schema_version: str
    record_type: Literal["ExperienceEvent"]
    experience_event_id: str
    behavior_change_id: str
    target_day: str
    recorded_at_utc: str
    event_statement: str
    loop_status: Literal["COMPLETE"]


def create_attention_decision(
    *,
    subject_id: str,
    target_day: str,
    attention_scope: str,
    selected_action: str,
    rationale: str,
    evidence_refs: tuple[str, ...] = (),
    decided_at_utc: str | None = None,
) -> AttentionDecision:
    decided = decided_at_utc or utc_now_iso()
    payload = {
        "subject_id": subject_id,
        "target_day": target_day,
        "decided_at_utc": decided,
        "attention_scope": attention_scope,
        "selected_action": selected_action,
        "rationale": rationale,
        "evidence_refs": list(evidence_refs),
    }
    return AttentionDecision(
        schema_version=SCHEMA_VERSION,
        record_type="AttentionDecision",
        decision_id=_record_id("AttentionDecision", payload),
        subject_id=subject_id,
        target_day=target_day,
        decided_at_utc=decided,
        attention_scope=attention_scope,
        selected_action=selected_action,
        rationale=rationale,
        evidence_refs=tuple(evidence_refs),
    )


def predict_from_decision(
    decision: AttentionDecision,
    *,
    prediction_statement: str,
    expected_outcome: str,
    confidence: float,
    horizon_days: int,
    predicted_at_utc: str | None = None,
) -> Prediction:
    _validate_record(asdict(decision))
    predicted = predicted_at_utc or utc_now_iso()
    payload = {
        "decision_id": decision.decision_id,
        "target_day": decision.target_day,
        "predicted_at_utc": predicted,
        "prediction_statement": prediction_statement,
        "expected_outcome": expected_outcome,
        "confidence": confidence,
        "horizon_days": horizon_days,
    }
    return Prediction(schema_version=SCHEMA_VERSION, record_type="Prediction", prediction_id=_record_id("Prediction", payload), **payload)


def observe_outcome(
    prediction: Prediction,
    *,
    outcome_statement: str,
    outcome_status: Literal["UNKNOWN", "MATCHED", "MISSED", "PARTIAL"],
    evidence_refs: tuple[str, ...] = (),
    observed_at_utc: str | None = None,
) -> Outcome:
    _validate_record(asdict(prediction))
    observed = observed_at_utc or utc_now_iso()
    payload = {
        "prediction_id": prediction.prediction_id,
        "target_day": prediction.target_day,
        "observed_at_utc": observed,
        "outcome_statement": outcome_statement,
        "outcome_status": outcome_status,
        "evidence_refs": list(evidence_refs),
    }
    return Outcome(
        schema_version=SCHEMA_VERSION,
        record_type="Outcome",
        outcome_id=_record_id("Outcome", payload),
        prediction_id=prediction.prediction_id,
        target_day=prediction.target_day,
        observed_at_utc=observed,
        outcome_statement=outcome_statement,
        outcome_status=outcome_status,
        evidence_refs=tuple(evidence_refs),
    )


def assess_regret(
    outcome: Outcome,
    *,
    regret_status: Literal["NONE", "LOW", "MEDIUM", "HIGH"],
    regret_statement: str,
    assessed_at_utc: str | None = None,
) -> Regret:
    _validate_record(asdict(outcome))
    assessed = assessed_at_utc or utc_now_iso()
    payload = {
        "outcome_id": outcome.outcome_id,
        "target_day": outcome.target_day,
        "assessed_at_utc": assessed,
        "regret_status": regret_status,
        "regret_statement": regret_statement,
    }
    return Regret(schema_version=SCHEMA_VERSION, record_type="Regret", regret_id=_record_id("Regret", payload), **payload)


def calibrate_from_regret(
    regret: Regret,
    *,
    prior_confidence: float,
    calibrated_confidence: float,
    calibration_note: str,
    calibrated_at_utc: str | None = None,
) -> CalibrationRecord:
    _validate_record(asdict(regret))
    calibrated = calibrated_at_utc or utc_now_iso()
    payload = {
        "regret_id": regret.regret_id,
        "target_day": regret.target_day,
        "calibrated_at_utc": calibrated,
        "prior_confidence": prior_confidence,
        "calibrated_confidence": calibrated_confidence,
        "calibration_note": calibration_note,
    }
    return CalibrationRecord(
        schema_version=SCHEMA_VERSION,
        record_type="CalibrationRecord",
        calibration_record_id=_record_id("CalibrationRecord", payload),
        **payload,
    )


def change_behavior(
    calibration_record: CalibrationRecord,
    *,
    change_statement: str,
    activation_rule: str,
    changed_at_utc: str | None = None,
) -> BehaviorChange:
    _validate_record(asdict(calibration_record))
    changed = changed_at_utc or utc_now_iso()
    payload = {
        "calibration_record_id": calibration_record.calibration_record_id,
        "target_day": calibration_record.target_day,
        "changed_at_utc": changed,
        "change_statement": change_statement,
        "activation_rule": activation_rule,
    }
    return BehaviorChange(
        schema_version=SCHEMA_VERSION,
        record_type="BehaviorChange",
        behavior_change_id=_record_id("BehaviorChange", payload),
        **payload,
    )


def record_experience_event(
    behavior_change: BehaviorChange,
    *,
    event_statement: str,
    recorded_at_utc: str | None = None,
) -> ExperienceEvent:
    _validate_record(asdict(behavior_change))
    recorded = recorded_at_utc or utc_now_iso()
    payload = {
        "behavior_change_id": behavior_change.behavior_change_id,
        "target_day": behavior_change.target_day,
        "recorded_at_utc": recorded,
        "event_statement": event_statement,
        "loop_status": "COMPLETE",
    }
    return ExperienceEvent(
        schema_version=SCHEMA_VERSION,
        record_type="ExperienceEvent",
        experience_event_id=_record_id("ExperienceEvent", payload),
        **payload,
    )


def append_record(record: Any, *, truth_root: str | Path = DEFAULT_TRUTH_ROOT) -> Path:
    payload = _record_to_dict(record)
    _validate_record(payload)
    path = ledger_path(truth_root, payload["target_day"])
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    fd = os.open(path, flags, 0o644)
    try:
        os.write(fd, (canonical_json(payload) + "\n").encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    _fsync_dir(path.parent)
    return path


def certification_path(truth_root: str | Path, target_day: str) -> Path:
    return Path(truth_root) / CERTIFICATION_RELATIVE / target_day / "experience_loop_certification.v1.json"


def build_certification(*, records: list[dict[str, Any]], target_day: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    audit = audit_loop(records)
    return {
        "schema_version": "aegis_experience_loop_certification.v1",
        "day_utc": target_day,
        "generated_at_utc": generated_at_utc or utc_now_iso(),
        "artifact_type": "append_only_experience_loop_certification",
        "source_record_count": len(records),
        "complete_loop": bool(audit["complete_loop"]),
        "audit": audit,
        "policy": {
            "read_only": True,
            "audit_only": True,
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "sleeve_creation_allowed": False,
            "candidate_creation_allowed": False,
            "paper_position_creation_allowed": False,
            "capital_allocation_allowed": False,
            "hypothesis_generation_allowed": False,
            "broad_discovery_allowed": False,
        },
    }


def write_certification(
    *,
    truth_root: str | Path = DEFAULT_TRUTH_ROOT,
    target_day: str,
    generated_at_utc: str | None = None,
) -> Path:
    records = read_records(truth_root=truth_root, target_day=target_day)
    payload = build_certification(records=records, target_day=target_day, generated_at_utc=generated_at_utc)
    path = certification_path(truth_root, target_day)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(canonical_json(payload) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_dir(path.parent)
    return path


def read_records(*, truth_root: str | Path = DEFAULT_TRUTH_ROOT, target_day: str) -> list[dict[str, Any]]:
    path = ledger_path(truth_root, target_day)
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            record = json.loads(stripped)
            _validate_record(record)
            records.append(record)
    return records


def audit_loop(records: list[dict[str, Any]]) -> dict[str, Any]:
    by_type = {record["record_type"]: record for record in records}
    missing = [record_type for record_type in VALID_RECORD_TYPES if record_type not in by_type]
    failures: list[str] = []
    if missing:
        failures.append("MISSING_RECORDS:" + ",".join(sorted(missing)))
    if "AttentionDecision" in by_type and "Prediction" in by_type:
        _require_link(failures, by_type["Prediction"], "decision_id", by_type["AttentionDecision"], "decision_id")
    if "Prediction" in by_type and "Outcome" in by_type:
        _require_link(failures, by_type["Outcome"], "prediction_id", by_type["Prediction"], "prediction_id")
    if "Outcome" in by_type and "Regret" in by_type:
        _require_link(failures, by_type["Regret"], "outcome_id", by_type["Outcome"], "outcome_id")
    if "Regret" in by_type and "CalibrationRecord" in by_type:
        _require_link(failures, by_type["CalibrationRecord"], "regret_id", by_type["Regret"], "regret_id")
    if "CalibrationRecord" in by_type and "BehaviorChange" in by_type:
        _require_link(failures, by_type["BehaviorChange"], "calibration_record_id", by_type["CalibrationRecord"], "calibration_record_id")
    if "BehaviorChange" in by_type and "ExperienceEvent" in by_type:
        _require_link(failures, by_type["ExperienceEvent"], "behavior_change_id", by_type["BehaviorChange"], "behavior_change_id")
    return {
        "schema_version": SCHEMA_VERSION,
        "ok": not failures,
        "record_count": len(records),
        "complete_loop": not failures,
        "failures": failures,
    }


def ledger_path(truth_root: str | Path, target_day: str) -> Path:
    return Path(truth_root) / LEDGER_RELATIVE / target_day / "experience_events.jsonl"


def _record_id(record_type: str, payload: dict[str, Any]) -> str:
    return sha256_text(record_type + "|" + canonical_json(payload))


def _record_to_dict(record: Any) -> dict[str, Any]:
    payload = asdict(record) if hasattr(record, "__dataclass_fields__") else dict(record)
    if isinstance(payload.get("evidence_refs"), tuple):
        payload["evidence_refs"] = list(payload["evidence_refs"])
    return payload


def _validate_record(record: dict[str, Any]) -> None:
    if isinstance(record.get("evidence_refs"), tuple):
        record["evidence_refs"] = list(record["evidence_refs"])
    if record.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("invalid schema_version")
    record_type = record.get("record_type")
    if record_type not in VALID_RECORD_TYPES:
        raise ValueError("invalid record_type")
    for key, value in record.items():
        if key.endswith("_id") and (not isinstance(value, str) or not value.strip()):
            raise ValueError(f"{key} must be a non-empty string")
    if not isinstance(record.get("target_day"), str) or not record["target_day"].strip():
        raise ValueError("target_day must be a non-empty string")
    for key in ("confidence", "prior_confidence", "calibrated_confidence"):
        if key in record and not 0.0 <= float(record[key]) <= 1.0:
            raise ValueError(f"{key} must be between 0 and 1")
    if "horizon_days" in record and int(record["horizon_days"]) < 0:
        raise ValueError("horizon_days must be non-negative")
    if record_type == "Outcome" and record.get("outcome_status") not in {"UNKNOWN", "MATCHED", "MISSED", "PARTIAL"}:
        raise ValueError("invalid outcome_status")
    if record_type == "Regret" and record.get("regret_status") not in {"NONE", "LOW", "MEDIUM", "HIGH"}:
        raise ValueError("invalid regret_status")
    if record_type == "ExperienceEvent" and record.get("loop_status") != "COMPLETE":
        raise ValueError("invalid loop_status")


def _require_link(failures: list[str], child: dict[str, Any], child_key: str, parent: dict[str, Any], parent_key: str) -> None:
    if child.get(child_key) != parent.get(parent_key):
        failures.append(f"BROKEN_LINK:{child['record_type']}.{child_key}->{parent['record_type']}.{parent_key}")


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
