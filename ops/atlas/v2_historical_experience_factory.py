from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_core import HISTORICAL_EXPERIENCE_SOURCE_TYPES, PROHIBITED_AUTHORITY_FIELDS, AtlasV2Ledger, AtlasV2ValidationError
from ops.atlas.v2_historical_experience_parsers import parse_historical_experience_source

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None

FACTORY_VERSION = "atlas_v2_historical_experience_factory_v1"
DEFAULT_CREATED_AT = "2026-06-04T00:00:00Z"
DEFAULT_PERIOD = "2026-06-04"
UNKNOWN_VALUE = "UNKNOWN"
NONE_VALUE = "NONE"
DEFAULT_MIN_QUALITY_TO_CONVERT = 0.5
DEFAULT_SOURCE_LOCATIONS = (
    Path("research_journal/failures"),
    Path("research_journal/observations"),
    Path("research_journal/knowledge"),
    Path("research_journal/reports"),
    Path("docs/aegis"),
    Path("reports"),
)


@dataclass(frozen=True)
class HistoricalExperienceFactoryResult:
    historical_records: list[dict[str, Any]]
    decisions: list[dict[str, Any]]
    predictions: list[dict[str, Any]]
    outcomes: list[dict[str, Any]]
    regrets: list[dict[str, Any]]
    calibrations: list[dict[str, Any]]
    experience_events: list[dict[str, Any]]
    rejected_records: list[dict[str, Any]]
    source_locations_scanned: list[str]
    ledger_root: Path

    @property
    def records_discovered(self) -> int:
        return len(self.historical_records) + len(self.rejected_records)

    @property
    def records_converted(self) -> int:
        return sum(1 for record in self.historical_records if record.get("status") == "CONVERTED")

    def report(self) -> dict[str, Any]:
        return {
            "factory_version": FACTORY_VERSION,
            "source_locations_scanned": self.source_locations_scanned,
            "historical_records_discovered": self.records_discovered,
            "records_converted": self.records_converted,
            "records_rejected_or_incomplete": self.records_discovered - self.records_converted,
            "experience_events_created": len(self.experience_events),
            "status_distribution": _status_distribution(self.historical_records, self.rejected_records),
            "source_type_distribution": _source_type_distribution(self.historical_records),
            "parser_contribution_by_source_family": _parser_contribution_by_source_family(self.historical_records),
            "quality_score_distribution": _quality_distribution(self.historical_records),
            "provenance_coverage": _provenance_coverage(self.experience_events),
            "learning_label_source_fields": _learning_label_source_fields(self.historical_records),
            "circularity_audit": _circularity_audit(self.historical_records),
            "authority_audit_status": "PASS" if AtlasV2Ledger(self.ledger_root).audit_all().ok else "FAIL",
            "rejected_records": self.rejected_records,
        }


class HistoricalExperienceFactory:
    def __init__(self, ledger: AtlasV2Ledger, *, repo_root: Path | None = None) -> None:
        self.ledger = ledger
        self.repo_root = Path(repo_root or Path.cwd())

    def scan_sources(self, source_locations: list[Path] | None = None, *, max_records: int | None = None) -> tuple[list[dict[str, Any]], list[str]]:
        records: list[dict[str, Any]] = []
        scanned: list[str] = []
        for location in source_locations or list(DEFAULT_SOURCE_LOCATIONS):
            scanned.append(str(location))
            root = location if location.is_absolute() else self.repo_root / location
            if not root.exists():
                continue
            files = [root] if root.is_file() else sorted(path for path in root.rglob("*") if path.is_file())
            for path in files:
                source = self._source_from_path(path)
                if source is None:
                    continue
                records.append(source)
                if max_records is not None and len(records) >= max_records:
                    return records, scanned
        return records, scanned

    def run_scan(self, source_locations: list[Path] | None = None, *, max_records: int | None = None, batch_id: str = FACTORY_VERSION, created_at: str = DEFAULT_CREATED_AT, min_quality_to_convert: float = DEFAULT_MIN_QUALITY_TO_CONVERT) -> HistoricalExperienceFactoryResult:
        records, scanned = self.scan_sources(source_locations, max_records=max_records)
        return self.convert_records(records, batch_id=batch_id, created_at=created_at, min_quality_to_convert=min_quality_to_convert, source_locations_scanned=scanned)

    def convert_records(self, records: list[dict[str, Any]], *, batch_id: str = FACTORY_VERSION, created_at: str = DEFAULT_CREATED_AT, min_quality_to_convert: float = DEFAULT_MIN_QUALITY_TO_CONVERT, source_locations_scanned: list[str] | None = None) -> HistoricalExperienceFactoryResult:
        historical_records: list[dict[str, Any]] = []
        decisions: list[dict[str, Any]] = []
        predictions: list[dict[str, Any]] = []
        outcomes: list[dict[str, Any]] = []
        regrets: list[dict[str, Any]] = []
        calibrations: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        seen_sources = {str(record.get("source_artifact")) for record in self.ledger.records("HistoricalExperienceRecord")}
        for index, source in enumerate(records, start=1):
            normalized = self._normalize_source(source, batch_id=batch_id, index=index)
            if normalized["source_artifact"] in seen_sources:
                normalized["status"] = "DUPLICATE_SOURCE"
                normalized["conversion_reason"] = "Source artifact already has a HistoricalExperienceRecord in this ledger."
            seen_sources.add(normalized["source_artifact"])
            try:
                cycle = self._convert_one(normalized, batch_id=batch_id, created_at=created_at, min_quality_to_convert=min_quality_to_convert)
            except AtlasV2ValidationError as exc:
                rejected.append({"record_id": str(normalized.get("record_id") or f"historical-{index:04d}"), "reason": str(exc)})
                continue
            historical_records.append(cycle["historical_record"])
            if cycle.get("experience_event"):
                decisions.append(cycle["decision"])
                predictions.append(cycle["prediction"])
                outcomes.append(cycle["outcome"])
                regrets.append(cycle["regret"])
                calibrations.append(cycle["calibration"])
                events.append(cycle["experience_event"])
        return HistoricalExperienceFactoryResult(historical_records, decisions, predictions, outcomes, regrets, calibrations, events, rejected, source_locations_scanned or [], self.ledger.root)

    def _convert_one(self, source: dict[str, Any], *, batch_id: str, created_at: str, min_quality_to_convert: float) -> dict[str, Any]:
        self._assert_source_safe(source)
        record_id = str(source["record_id"])
        source_type = str(source["source_type"]).upper()
        source_artifact = str(source["source_artifact"])
        expected = str(source["expected_outcome"])
        actual = str(source["actual_outcome"])
        confidence = _bounded_float(source["confidence"], "confidence")
        regret_score = _bounded_float(source["regret_score"], "regret_score")
        quality = _bounded_float(source["experience_quality_score"], "experience_quality_score")
        expected_learning = _bounded_float(source["expected_learning_value_pre_outcome"], "expected_learning_value_pre_outcome")
        actual_learning = _bounded_float(source["actual_learning_value_post_outcome"], "actual_learning_value_post_outcome")
        expected_fields = _string_list(source["expected_learning_source_fields"], "expected_learning_source_fields")
        actual_fields = _string_list(source["actual_learning_source_fields"], "actual_learning_source_fields")
        status = str(source.get("status") or "CONVERTED")
        if status == "CONVERTED" and _is_incomplete(source):
            status = "INCOMPLETE_PROVENANCE"
        if status == "CONVERTED" and quality < min_quality_to_convert:
            status = "REJECTED_LOW_QUALITY"
        matched = _matches_expected(expected, actual)
        calibration_error = round(abs((1.0 if matched else 0.0) - confidence), 6)
        decision_id = str(source.get("decision_id") or f"{record_id}-decision")
        prediction_id = str(source.get("prediction_id") or f"{record_id}-prediction")
        outcome_id = str(source.get("outcome_id") or f"{record_id}-outcome")
        regret_id = str(source.get("regret_id") or f"{record_id}-regret")
        calibration_id = str(source.get("calibration_id") or f"{record_id}-calibration")
        experience_id = str(source.get("experience_id") or f"{record_id}-experience")
        historical_record = self.ledger.create_record("HistoricalExperienceRecord", {
            "record_id": record_id,
            "created_at": created_at,
            "source_artifact": source_artifact,
            "source_type": source_type,
            **_optional_parser_metadata(source),
            "historical_date": str(source["historical_date"]),
            "decision_summary": str(source["decision_summary"]),
            "expected_outcome": expected,
            "actual_outcome": actual,
            "confidence": confidence,
            "regret_score": regret_score,
            "calibration_error": calibration_error,
            "experience_quality_score": quality,
            "expected_learning_value_pre_outcome": expected_learning,
            "actual_learning_value_post_outcome": actual_learning,
            "expected_learning_source_fields": expected_fields,
            "actual_learning_source_fields": actual_fields,
            "conversion_reason": str(source["conversion_reason"]),
            "provenance_reference": str(source["provenance_reference"]),
            "converted_experience_event_id": experience_id if status == "CONVERTED" else NONE_VALUE,
            "status": status,
        }, reason="historical source record preserved before conversion", triggering_object=f"{FACTORY_VERSION}:{batch_id}")
        if status != "CONVERTED":
            return {"historical_record": historical_record}
        decision = self.ledger.create_record("AttentionDecision", {"decision_id": decision_id, "created_at": created_at, "decision_type": f"historical_{source_type.lower()}", "uncertainty_target": str(source["decision_summary"]), "importance_score": quality, "expected_learning_value": expected_learning, "expected_learning_source_fields": expected_fields, "expected_regret_if_ignored": regret_score, "attention_cost": float(source.get("attention_cost", 0.0)), "selected_action": "Convert historical artifact into read-only experience evidence", "rejected_alternatives": ["Create candidate", "Create sleeve", "Create paper position", "Allocate capital"], "decision_reason": str(source["conversion_reason"]), "status": "recorded", "historical_record_id": record_id, "source_artifact": source_artifact, "provenance_reference": str(source["provenance_reference"])}, reason="historical experience attention decision recorded", triggering_object=f"HistoricalExperienceRecord:{record_id}")
        prediction = self.ledger.create_record("Prediction", {"prediction_id": prediction_id, "decision_id": decision_id, "created_at": created_at, "prediction_statement": str(source.get("prediction_statement") or source["decision_summary"]), "confidence": confidence, "expected_outcome": expected, "evaluation_date": str(source["historical_date"]), "status": "evaluated", "historical_record_id": record_id, "source_artifact": source_artifact, "provenance_reference": str(source["provenance_reference"])}, reason="historical experience prediction recorded", triggering_object=f"AttentionDecision:{decision_id}")
        outcome = self.ledger.create_record("Outcome", {"outcome_id": outcome_id, "prediction_id": prediction_id, "created_at": created_at, "observed_outcome": actual, "outcome_date": str(source["historical_date"]), "matched_expected_outcome": matched, "outcome_confidence": _bounded_float(source.get("outcome_confidence", confidence), "outcome_confidence"), "evidence_reference": source_artifact, "historical_record_id": record_id, "source_artifact": source_artifact, "provenance_reference": str(source["provenance_reference"])}, reason="historical experience outcome recorded from source artifact", triggering_object=f"Prediction:{prediction_id}")
        regret = self.ledger.create_record("Regret", {"regret_id": regret_id, "decision_id": decision_id, "outcome_id": outcome_id, "created_at": created_at, "regret_score": regret_score, "missed_alternative": str(source.get("missed_alternative", "Earlier preservation of outcome-linked historical evidence")), "regret_reason": str(source.get("regret_reason", source["conversion_reason"])), "importance_weighted_regret": round(quality * regret_score, 6), "historical_record_id": record_id, "source_artifact": source_artifact, "provenance_reference": str(source["provenance_reference"])}, reason="historical experience regret scored", triggering_object=f"Outcome:{outcome_id}")
        calibration = self.ledger.create_record("CalibrationRecord", {"calibration_id": calibration_id, "prediction_id": prediction_id, "created_at": created_at, "confidence": confidence, "actual_result": matched, "calibration_error": calibration_error, "calibration_bucket": _calibration_bucket(confidence), "historical_record_id": record_id, "source_artifact": source_artifact, "provenance_reference": str(source["provenance_reference"])}, reason="historical experience calibration scored", triggering_object=f"Outcome:{outcome_id}")
        event = self.ledger.create_record("ExperienceEvent", {"experience_id": experience_id, "created_at": created_at, "source_decision_id": decision_id, "prediction_id": prediction_id, "outcome_id": outcome_id, "regret_id": regret_id, "calibration_id": calibration_id, "lesson": str(source.get("lesson") or _lesson_for(source_type, expected, actual)), "experience_quality_score": quality, "actual_learning_value_post_outcome": actual_learning, "actual_learning_source_fields": actual_fields, "historical_record_id": record_id, "source_artifact": source_artifact, "source_type": source_type, "provenance_reference": str(source["provenance_reference"]), "batch_id": batch_id}, reason="historical experience event linked", triggering_object=f"HistoricalExperienceRecord:{record_id}")
        return {"historical_record": historical_record, "decision": decision, "prediction": prediction, "outcome": outcome, "regret": regret, "calibration": calibration, "experience_event": event}

    def _assert_source_safe(self, source: dict[str, Any]) -> None:
        for field in PROHIBITED_AUTHORITY_FIELDS:
            if source.get(field) not in (None, "", False, [], {}):
                raise AtlasV2ValidationError(f"historical source cannot set prohibited authority field: {field}")
        if str(source["source_type"]).upper() not in HISTORICAL_EXPERIENCE_SOURCE_TYPES:
            raise AtlasV2ValidationError(f"unsupported historical source_type: {source['source_type']}")
        if str(source["actual_outcome"]).strip().lower() in {"failed", "failure"} and str(source["expected_outcome"]).strip().lower() in {"unknown", "outcome_unknown", "pending_outcome"}:
            raise AtlasV2ValidationError("historical source cannot collapse unknown into failed")

    def _normalize_source(self, source: dict[str, Any], *, batch_id: str, index: int) -> dict[str, Any]:
        record = dict(source)
        record.setdefault("record_id", f"{batch_id}-historical-{index:04d}-{_stable_id(str(record.get('source_artifact') or index))}")
        record.setdefault("source_artifact", f"operator_supplied:{record['record_id']}")
        record["source_type"] = str(record.get("source_type") or "OBSERVATION").upper()
        if record["source_type"] not in HISTORICAL_EXPERIENCE_SOURCE_TYPES:
            record["source_type"] = "OBSERVATION"
            record["status"] = "UNSUPPORTED_SOURCE"
        record.setdefault("historical_date", DEFAULT_PERIOD)
        record.setdefault("decision_summary", UNKNOWN_VALUE)
        record.setdefault("expected_outcome", UNKNOWN_VALUE)
        record.setdefault("actual_outcome", UNKNOWN_VALUE)
        record.setdefault("confidence", 0.0)
        record.setdefault("regret_score", 0.0)
        record.setdefault("experience_quality_score", _quality_score(record))
        record.setdefault("expected_learning_value_pre_outcome", _expected_learning_value_pre_outcome(record))
        record.setdefault("actual_learning_value_post_outcome", _actual_learning_value_post_outcome(record))
        record.setdefault("expected_learning_source_fields", _expected_learning_source_fields(record))
        record.setdefault("actual_learning_source_fields", _actual_learning_source_fields(record))
        record.setdefault("conversion_reason", "Historical source normalized for read-only Atlas V2 experience conversion.")
        record.setdefault("provenance_reference", f"{record['source_artifact']}#{record['record_id']}")
        return record


    def records_from_inventory(self, inventory: dict[str, Any], *, max_records: int | None = None) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for item in inventory.get("candidate_artifacts", []):
            if not isinstance(item, dict):
                continue
            source_path = item.get("source_path")
            if not source_path:
                continue
            source = self._source_from_inventory_candidate(item)
            if source is None:
                continue
            records.append(source)
            if max_records is not None and len(records) >= max_records:
                break
        return records

    def _source_from_inventory_candidate(self, candidate: dict[str, Any]) -> dict[str, Any] | None:
        rel = str(candidate["source_path"])
        path = self.repo_root / rel
        if not path.exists() or not path.is_file():
            return None
        parsed = self._source_from_path(path)
        text = path.read_text(encoding="utf-8", errors="ignore")
        source_type = str(candidate.get("source_type") or "OBSERVATION").upper()
        expected = _extract_expected_from_text(text) if candidate.get("has_expectation") else UNKNOWN_VALUE
        actual = _extract_actual_from_text(text) if candidate.get("has_outcome") else UNKNOWN_VALUE
        status = "CONVERTED" if str(candidate.get("conversion_eligibility") or "") == "ELIGIBLE" else "INCOMPLETE_PROVENANCE"
        if str(candidate.get("conversion_eligibility") or "") == "UNSUPPORTED":
            status = "UNSUPPORTED_SOURCE"
        fallback = {
            "record_id": f"hist-{_stable_id(rel)}",
            "source_artifact": rel,
            "source_type": source_type,
            "historical_date": DEFAULT_PERIOD,
            "decision_summary": _first_heading(text) or _compact(expected, limit=160),
            "expected_outcome": expected,
            "actual_outcome": actual,
            "confidence": _inventory_confidence(source_type, text),
            "regret_score": _inventory_regret(source_type, text),
            "experience_quality_score": float(candidate.get("estimated_experience_quality_score", 0.5) or 0.5),
            "conversion_reason": str(candidate.get("reason") or "Controlled conversion from historical inventory artifact."),
            "provenance_reference": f"{rel}#historical-inventory-2026-06-04",
            "status": status,
        }
        if parsed is None:
            return fallback
        merged = {**fallback, **parsed}
        for field in ("expected_outcome", "actual_outcome", "decision_summary"):
            if str(merged.get(field) or "").strip().upper() in {"", UNKNOWN_VALUE, NONE_VALUE}:
                merged[field] = fallback[field]
        merged["experience_quality_score"] = fallback["experience_quality_score"]
        if fallback["status"] == "CONVERTED" and not _is_incomplete(merged):
            merged["status"] = "CONVERTED"
        else:
            merged.setdefault("status", fallback["status"])
        return merged

    def _source_from_path(self, path: Path) -> dict[str, Any] | None:
        rel = path.relative_to(self.repo_root).as_posix() if path.is_absolute() and path.is_relative_to(self.repo_root) else path.as_posix()
        parsed = parse_historical_experience_source(path, repo_root=self.repo_root)
        if parsed is not None:
            return parsed
        if "research_journal/failures" in rel and path.suffix.lower() in {".yaml", ".yml"}:
            data = _load_yaml(path)
            if not data:
                return None
            return {"record_id": f"hist-{str(data.get('id') or path.stem).lower().replace('_', '-')}", "source_artifact": rel, "source_type": "FAILURE", "historical_date": str(data.get("date") or DEFAULT_PERIOD), "decision_summary": str(data.get("what_we_expected") or UNKNOWN_VALUE), "expected_outcome": str(data.get("what_we_expected") or UNKNOWN_VALUE), "actual_outcome": str(data.get("what_failed") or UNKNOWN_VALUE), "confidence": 0.65, "regret_score": 0.75, "experience_quality_score": _quality_score({"source_artifact": rel, "expected_outcome": data.get("what_we_expected"), "actual_outcome": data.get("what_failed"), "decision_summary": data.get("what_we_expected"), "regret_score": 0.75}), "conversion_reason": str(data.get("why_failed") or "Failure journal entry has expected and failed outcome fields."), "provenance_reference": f"{rel}#{data.get('id') or path.stem}"}
        if "research_journal/observations" in rel and path.suffix.lower() in {".yaml", ".yml"}:
            data = _load_yaml(path)
            if not data:
                return None
            return {"record_id": f"hist-{str(data.get('id') or path.stem).lower().replace('_', '-')}", "source_artifact": rel, "source_type": "OBSERVATION", "historical_date": str(data.get("date") or DEFAULT_PERIOD), "decision_summary": str(data.get("observation") or UNKNOWN_VALUE), "expected_outcome": UNKNOWN_VALUE, "actual_outcome": str(data.get("observation") or UNKNOWN_VALUE), "confidence": 0.45, "regret_score": 0.35, "experience_quality_score": _quality_score({"source_artifact": rel, "actual_outcome": data.get("observation"), "decision_summary": data.get("why_interesting")}), "conversion_reason": "Observation lacks explicit expected outcome; preserved as incomplete provenance.", "provenance_reference": f"{rel}#{data.get('id') or path.stem}"}
        if "research_journal/knowledge" in rel and path.suffix.lower() in {".yaml", ".yml"}:
            data = _load_yaml(path)
            if not data:
                return None
            evidence = data.get("evidence") or []
            return {"record_id": f"hist-{str(data.get('id') or path.stem).lower().replace('_', '-')}", "source_artifact": rel, "source_type": "KNOWLEDGE", "historical_date": str(data.get("date") or DEFAULT_PERIOD), "decision_summary": str(data.get("statement") or UNKNOWN_VALUE), "expected_outcome": str(data.get("statement") or UNKNOWN_VALUE), "actual_outcome": "; ".join(str(item) for item in evidence) if evidence else UNKNOWN_VALUE, "confidence": 0.6 if evidence else 0.3, "regret_score": 0.45, "experience_quality_score": _quality_score({"source_artifact": rel, "expected_outcome": data.get("statement"), "actual_outcome": evidence, "decision_summary": data.get("why_it_matters"), "regret_score": 0.45}), "conversion_reason": "Knowledge journal entry preserves statement and supporting evidence as historical learning context.", "provenance_reference": f"{rel}#{data.get('id') or path.stem}"}
        if "research_journal/reports" in rel and path.suffix.lower() == ".md":
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "Strongest contradictory evidence" not in text:
                return None
            return {"record_id": f"hist-{_stable_id(rel)}", "source_artifact": rel, "source_type": "CONTRADICTION", "historical_date": DEFAULT_PERIOD, "decision_summary": _first_heading(text) or path.stem, "expected_outcome": _compact(_section_after(text, "Strongest supporting evidence") or UNKNOWN_VALUE), "actual_outcome": _compact(_section_after(text, "Strongest contradictory evidence") or UNKNOWN_VALUE), "confidence": 0.55, "regret_score": 0.6, "experience_quality_score": 0.8, "conversion_reason": "Historical review contains supporting and contradictory evidence sections suitable for read-only conversion.", "provenance_reference": f"{rel}#historical-review"}
        return None



def _extract_expected_from_text(text: str) -> str:
    for heading in ("Expected", "Expectation", "Decision", "Claim", "Thesis", "Supporting evidence", "Standard"):
        section = _section_after(text, heading)
        if section:
            return _compact(section, limit=240)
    return _compact(_first_meaningful_line(text), limit=240) or UNKNOWN_VALUE


def _extract_actual_from_text(text: str) -> str:
    for heading in ("Outcome", "Actual", "Result", "Contradictory evidence", "Conclusion", "Review result"):
        section = _section_after(text, heading)
        if section:
            return _compact(section, limit=240)
    lowered = text.lower()
    for token in ("outcome", "result", "failed", "contradict", "conclusion", "evidence"):
        idx = lowered.find(token)
        if idx >= 0:
            return _compact(text[idx: idx + 500], limit=240)
    return UNKNOWN_VALUE


def _first_meaningful_line(text: str) -> str:
    for line in text.splitlines():
        cleaned = line.strip().strip("# -*`)>")
        if len(cleaned) >= 16:
            return cleaned
    return ""


def _inventory_confidence(source_type: str, text: str) -> float:
    base = {"FAILURE": 0.62, "CONTRADICTION": 0.55, "DECISION": 0.58, "KNOWLEDGE": 0.6, "RESEARCH_OUTCOME": 0.57}.get(source_type.upper(), 0.5)
    return round(min(0.8, base + (0.05 if len(text) > 1000 else 0.0)), 6)


def _inventory_regret(source_type: str, text: str) -> float:
    lowered = text.lower()
    base = 0.7 if source_type.upper() in {"FAILURE", "CONTRADICTION"} else 0.55 if source_type.upper() == "DECISION" else 0.45
    if any(token in lowered for token in ("failed", "failure", "contradict", "risk", "blocked")):
        base += 0.1
    return round(min(0.9, base), 6)

def _load_yaml(path: Path) -> dict[str, Any]:
    if yaml is None:
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _is_incomplete(source: dict[str, Any]) -> bool:
    return any(str(source.get(field) or "").strip().upper() in {"", UNKNOWN_VALUE, NONE_VALUE} for field in ("source_artifact", "decision_summary", "expected_outcome", "actual_outcome", "provenance_reference"))


def _matches_expected(expected: str, actual: str) -> bool:
    expected_norm = " ".join(expected.lower().split())
    actual_norm = " ".join(actual.lower().split())
    return expected_norm not in {"unknown", "outcome unknown", "pending outcome"} and expected_norm == actual_norm


def _bounded_float(value: Any, label: str) -> float:
    result = float(value)
    if result < 0 or result > 1:
        raise AtlasV2ValidationError(f"{label} must be between 0 and 1")
    return result


def _calibration_bucket(confidence: float) -> str:
    return f"{int(confidence * 10) / 10:.1f}"



def _string_list(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise AtlasV2ValidationError(f"{label} must be a non-empty list")
    result = [str(item) for item in value]
    if any(not item.strip() for item in result):
        raise AtlasV2ValidationError(f"{label} must contain non-empty strings")
    return result


def _known(value: Any) -> bool:
    return str(value or "").strip().upper() not in {"", UNKNOWN_VALUE, NONE_VALUE}


def _text_clarity(value: Any) -> float:
    text = str(value or "").strip()
    if not text or text.upper() in {UNKNOWN_VALUE, NONE_VALUE}:
        return 0.0
    if len(text) >= 120:
        return 1.0
    if len(text) >= 60:
        return 0.8
    if len(text) >= 24:
        return 0.6
    return 0.4


def _source_type_prior(source_type: str) -> float:
    return {
        "FAILURE": 0.7,
        "CONTRADICTION": 0.65,
        "RESEARCH_OUTCOME": 0.62,
        "DECISION": 0.58,
        "KNOWLEDGE": 0.55,
        "OBSERVATION": 0.45,
    }.get(source_type.upper(), 0.5)


def _expected_learning_value_pre_outcome(source: dict[str, Any]) -> float:
    source_type = str(source.get("source_type") or "").upper()
    attention_cost = min(max(float(source.get("attention_cost", 0.0) or 0.0), 0.0), 1.0)
    components = [
        _source_type_prior(source_type),
        _text_clarity(source.get("decision_summary")),
        1.0 if _known(source.get("expected_outcome")) else 0.0,
        1.0 - min(max(float(source.get("confidence", 0.5) or 0.5), 0.0), 1.0),
        0.75 if source_type in {"FAILURE", "CONTRADICTION", "RESEARCH_OUTCOME"} else 0.55,
        1.0 - attention_cost,
    ]
    return round(sum(components) / len(components), 6)


def _actual_learning_value_post_outcome(source: dict[str, Any]) -> float:
    source_type = str(source.get("source_type") or "").upper()
    regret = min(max(float(source.get("regret_score", 0.0) or 0.0), 0.0), 1.0)
    confidence = min(max(float(source.get("confidence", 0.0) or 0.0), 0.0), 1.0)
    calibration_usefulness = 1.0 - abs((1.0 if _matches_expected(str(source.get("expected_outcome") or ""), str(source.get("actual_outcome") or "")) else 0.0) - confidence)
    components = [
        1.0 if _known(source.get("actual_outcome")) else 0.0,
        regret,
        calibration_usefulness,
        0.9 if source_type in {"FAILURE", "CONTRADICTION"} else 0.65 if source_type == "RESEARCH_OUTCOME" else 0.45,
        1.0 if _known(source.get("provenance_reference")) and _known(source.get("source_artifact")) else 0.0,
        0.85 if regret >= 0.6 or source_type in {"FAILURE", "CONTRADICTION"} else 0.55,
    ]
    return round(sum(components) / len(components), 6)


def _expected_learning_source_fields(source: dict[str, Any]) -> list[str]:
    return [
        "source_type_prior",
        "expected_outcome",
        "expectation_clarity",
        "decision_summary_clarity",
        "prior_uncertainty",
        "future_decision_relevance",
        "attention_cost",
    ]


def _actual_learning_source_fields(source: dict[str, Any]) -> list[str]:
    return [
        "actual_outcome",
        "outcome_clarity",
        "regret_informativeness",
        "calibration_usefulness",
        "failure_contradiction_information",
        "behavior_change_potential",
        "provenance_strength",
    ]


def _learning_label_source_fields(records: list[dict[str, Any]]) -> dict[str, list[str]]:
    converted = [record for record in records if record.get("status") == "CONVERTED"]
    expected = sorted({field for record in converted for field in record.get("expected_learning_source_fields", [])})
    actual = sorted({field for record in converted for field in record.get("actual_learning_source_fields", [])})
    return {"expected_learning_value_pre_outcome": expected, "actual_learning_value_post_outcome": actual}


def _circularity_audit(records: list[dict[str, Any]]) -> dict[str, Any]:
    converted = [record for record in records if record.get("status") == "CONVERTED"]
    pairs = [
        (float(record["expected_learning_value_pre_outcome"]), float(record["actual_learning_value_post_outcome"]))
        for record in converted
    ]
    all_equal = bool(pairs) and all(abs(expected - actual) <= 1e-9 for expected, actual in pairs)
    correlation = None
    if len(pairs) >= 2:
        from ops.atlas.v2_learning_estimator import _correlation
        correlation = _correlation([left for left, _ in pairs], [right for _, right in pairs])
    warnings: list[str] = []
    status = "PASS"
    if all_equal:
        status = "FAIL"
        warnings.append("expected_learning_value_pre_outcome equals actual_learning_value_post_outcome for all converted records")
    if correlation is not None and correlation == 1.0 and len(pairs) > 10:
        if status != "FAIL":
            status = "WARN"
        warnings.append("correlation_expected_actual is exactly 1.0 with n > 10")
    return {
        "status": status,
        "record_count": len(pairs),
        "all_expected_actual_equal": all_equal,
        "correlation_expected_actual": correlation,
        "warnings": warnings,
        "expected_learning_source_fields": _learning_label_source_fields(records)["expected_learning_value_pre_outcome"],
        "actual_learning_source_fields": _learning_label_source_fields(records)["actual_learning_value_post_outcome"],
    }

def _lesson_for(source_type: str, expected: str, actual: str) -> str:
    return f"Historical {source_type.lower()} preserved expected outcome [{expected}] versus actual outcome [{actual}]."


def _quality_score(source: dict[str, Any]) -> float:
    components = [1.0 if source.get("source_artifact") else 0.0, 1.0 if str(source.get("expected_outcome") or "").strip().upper() not in {"", UNKNOWN_VALUE} else 0.0, 1.0 if str(source.get("actual_outcome") or "").strip().upper() not in {"", UNKNOWN_VALUE} else 0.0, 1.0 if str(source.get("decision_summary") or "").strip().upper() not in {"", UNKNOWN_VALUE} else 0.0, float(source.get("regret_score", 0.5) or 0.0), 0.75]
    return round(sum(components) / len(components), 6)


def _optional_parser_metadata(source: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for field in ("source_family", "parser_name", "parser_extracted_fields"):
        value = source.get(field)
        if value not in (None, "", [], {}):
            metadata[field] = value
    return metadata


def _source_type_distribution(records: list[dict[str, Any]]) -> dict[str, int]:
    distribution: dict[str, int] = {}
    for record in records:
        source_type = str(record.get("source_type") or "UNKNOWN")
        distribution[source_type] = distribution.get(source_type, 0) + 1
    return dict(sorted(distribution.items()))


def _parser_contribution_by_source_family(records: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    contribution: dict[str, dict[str, int]] = {}
    for record in records:
        family = str(record.get("source_family") or "legacy_or_inventory_fallback")
        bucket = contribution.setdefault(family, {"records": 0, "converted": 0, "incomplete": 0})
        bucket["records"] += 1
        if record.get("status") == "CONVERTED":
            bucket["converted"] += 1
        else:
            bucket["incomplete"] += 1
    return dict(sorted(contribution.items()))


def _quality_distribution(records: list[dict[str, Any]]) -> dict[str, int]:
    distribution = {"0.0-0.25": 0, "0.25-0.5": 0, "0.5-0.75": 0, "0.75-1.0": 0}
    for record in records:
        score = float(record.get("experience_quality_score", 0))
        if score < 0.25:
            distribution["0.0-0.25"] += 1
        elif score < 0.5:
            distribution["0.25-0.5"] += 1
        elif score < 0.75:
            distribution["0.5-0.75"] += 1
        else:
            distribution["0.75-1.0"] += 1
    return distribution


def _status_distribution(records: list[dict[str, Any]], rejected: list[dict[str, Any]]) -> dict[str, int]:
    distribution = {"CONVERTED": 0, "INCOMPLETE_PROVENANCE": 0, "UNSUPPORTED_SOURCE": 0, "DUPLICATE_SOURCE": 0, "REJECTED_LOW_QUALITY": 0}
    for record in records:
        distribution[str(record.get("status"))] = distribution.get(str(record.get("status")), 0) + 1
    if rejected:
        distribution["VALIDATION_REJECTED"] = len(rejected)
    return distribution


def _provenance_coverage(events: list[dict[str, Any]]) -> float:
    if not events:
        return 0.0
    covered = sum(1 for event in events if event.get("historical_record_id") and event.get("source_artifact") and event.get("provenance_reference"))
    return round(covered / len(events), 6)


def _stable_id(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _first_heading(text: str) -> str | None:
    for line in text.splitlines():
        if line.startswith("#"):
            return line.lstrip("#").strip()
    return None


def _section_after(text: str, heading: str) -> str | None:
    marker = heading.lower()
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if marker in line.lower():
            body = []
            for next_line in lines[i + 1:]:
                if next_line.startswith("##"):
                    break
                if next_line.strip():
                    body.append(next_line.strip(" -"))
                if len(body) >= 5:
                    break
            return " ".join(body) if body else None
    return None


def _compact(value: Any, limit: int = 500) -> str:
    text = " ".join(str(value).split())
    return text[:limit] if len(text) > limit else text


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert Atlas/AEGIS historical artifacts into append-only Atlas V2 experience events.")
    parser.add_argument("--ledger-root", type=Path, required=True)
    parser.add_argument("--input-json", type=Path)
    parser.add_argument("--scan-root", action="append", type=Path)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--batch-id", default=FACTORY_VERSION)
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--min-quality-to-convert", type=float, default=DEFAULT_MIN_QUALITY_TO_CONVERT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    factory = HistoricalExperienceFactory(AtlasV2Ledger(args.ledger_root), repo_root=args.repo_root)
    if args.input_json:
        payload = json.loads(args.input_json.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and isinstance(payload.get("candidate_artifacts"), list):
            records = factory.records_from_inventory(payload, max_records=args.max_records)
        elif isinstance(payload, list):
            records = payload
        else:
            raise SystemExit("--input-json must contain a JSON list or historical_inventory.v1 object")
        result = factory.convert_records(records, batch_id=args.batch_id, created_at=args.created_at, min_quality_to_convert=args.min_quality_to_convert)
    else:
        result = factory.run_scan(args.scan_root, max_records=args.max_records, batch_id=args.batch_id, created_at=args.created_at, min_quality_to_convert=args.min_quality_to_convert)
    print(json.dumps(result.report(), sort_keys=True))
    return 0 if result.report()["authority_audit_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
