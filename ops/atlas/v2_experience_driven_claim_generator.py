from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, PROHIBITED_AUTHORITY_FIELDS
from ops.atlas.v2_external_strategy_claim_extractor import DEFAULT_CREATED_AT, classify_mechanism_text, short_hash
from ops.atlas.v2_claim_idea_generator import generated_claim_to_external_strategy_claim_payload, rule_fields_for_family

GENERATOR_VERSION = "atlas_v2_experience_driven_claim_generator_v1"
DEFAULT_LEDGER_ROOT = Path("/tmp/atlas_v2_experience_driven_claim_generator_v1_ledgers")
DEFAULT_MAX_CLAIMS = 10
MAX_CLAIMS_HARD_CAP = 100
ALLOWED_SOURCE_TYPES = {
    "HISTORICAL_FAILURE",
    "HISTORICAL_CONTRADICTION",
    "REGRET_SIGNAL",
    "CALIBRATION_ERROR",
    "MECHANISM_GAP",
    "CONTRARIAN_GAP",
    "PRIOR_EXPERIENCE_EVENT",
}
AUTHORITY_TEXT_TERMS = (
    "buy ",
    "sell ",
    "profitable",
    "profitability",
    "validated",
    "proven",
    "guaranteed",
    "recommendation",
    "trade advice",
    "allocate",
    "allocation",
    "broker",
    "candidate",
    "sleeve",
    "paper position",
    "order",
)


@dataclass(frozen=True)
class ExperienceDrivenClaimGenerationResult:
    generated_claims: list[dict[str, Any]]
    run: dict[str, Any]

    def summary(self) -> dict[str, Any]:
        return {
            "run_id": self.run["run_id"],
            "max_claims": self.run["max_claims"],
            "claims_generated": self.run["claims_generated"],
            "duplicates_detected": self.run["duplicates_detected"],
            "insufficient_basis_count": self.run["insufficient_basis_count"],
            "mechanism_distribution": self.run["mechanism_distribution"],
            "status": self.run["status"],
        }


class AtlasV2ExperienceDrivenClaimGenerator:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def generate(
        self,
        *,
        max_claims: int = DEFAULT_MAX_CLAIMS,
        run_id: str | None = None,
        created_at: str = DEFAULT_CREATED_AT,
    ) -> ExperienceDrivenClaimGenerationResult:
        if max_claims < 0 or max_claims > MAX_CLAIMS_HARD_CAP:
            raise AtlasV2ValidationError("Experience-Driven Claim Generator max_claims must be between 0 and 100")

        sources = collect_experience_sources(self.ledger)[:max_claims]
        existing = existing_generated_fingerprints(self.ledger)
        generated_claims: list[dict[str, Any]] = []
        duplicate_count = 0
        insufficient_count = 0

        for source in sources:
            assert_source_safe(source)
            payload = build_generated_claim_payload(source, created_at=created_at)
            fingerprint = generated_claim_fingerprint(payload)
            if fingerprint in existing and payload["status"] == "GENERATED":
                payload = {
                    **payload,
                    "generated_claim_id": f"grc-{short_hash(source['source_reference'] + ':duplicate:' + fingerprint)}",
                    "expected_learning_value": min(float(payload["expected_learning_value"]), 0.2),
                    "status": "DUPLICATE",
                }
            existing.add(fingerprint)
            claim = self.ledger.create_record(
                "GeneratedResearchClaim",
                payload,
                reason="experience-driven Atlas V2 research claim generated from existing evidence only",
                triggering_object=source["source_reference"],
            )
            generated_claims.append(claim)
            if claim["status"] == "DUPLICATE":
                duplicate_count += 1
            if claim["status"] in {"INSUFFICIENT_BASIS", "UNSUPPORTED_MECHANISM", "REJECTED_LOW_TESTABILITY"}:
                insufficient_count += 1

        generated_count = sum(1 for claim in generated_claims if claim["status"] == "GENERATED")
        mechanism_distribution = dict(Counter(claim["mechanism_family"] for claim in generated_claims if claim["status"] == "GENERATED"))
        status = "NO_ELIGIBLE_SOURCES" if not sources else ("COMPLETED_WITH_REJECTIONS" if duplicate_count or insufficient_count else "COMPLETED")
        run_record_id = run_id or f"cgr-{short_hash(':'.join(source['source_reference'] for source in sources) + ':' + str(max_claims) + ':' + GENERATOR_VERSION)}"
        run = self.ledger.create_record(
            "ClaimGenerationRun",
            {
                "run_id": run_record_id,
                "created_at": created_at,
                "max_claims": max_claims,
                "claims_generated": generated_count,
                "duplicates_detected": duplicate_count,
                "insufficient_basis_count": insufficient_count,
                "mechanism_distribution": mechanism_distribution,
                "status": status,
            },
            reason="experience-driven claim generation run summarized without hypothesis, experiment, or execution creation",
            triggering_object=GENERATOR_VERSION,
        )
        return ExperienceDrivenClaimGenerationResult(generated_claims=generated_claims, run=run)

    def external_strategy_claim_payloads(self, generated_claims: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        claims = generated_claims if generated_claims is not None else self.ledger.records("GeneratedResearchClaim")
        return [generated_claim_to_external_strategy_claim_payload(claim) for claim in claims if claim.get("status") == "GENERATED"]


def collect_experience_sources(ledger: AtlasV2Ledger) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    for record in ledger.records("HistoricalExperienceRecord"):
        source_type = historical_source_type(record)
        if source_type:
            sources.append(source_from_record(source_type, "HistoricalExperienceRecord", record, historical_text(record)))
    for record in ledger.records("Regret"):
        sources.append(source_from_record("REGRET_SIGNAL", "Regret", record, regret_text(record)))
    for record in ledger.records("CalibrationRecord"):
        sources.append(source_from_record("CALIBRATION_ERROR", "CalibrationRecord", record, calibration_text(record)))
    for record in ledger.records("MechanismRegistryEntry"):
        sources.append(source_from_record("MECHANISM_GAP", "MechanismRegistryEntry", record, registry_text(record)))
    for record in ledger.records("ExternalStrategyContrarianTheory"):
        sources.append(source_from_record("CONTRARIAN_GAP", "ExternalStrategyContrarianTheory", record, contrarian_text(record)))
    for record in ledger.records("ExperienceEvent"):
        sources.append(source_from_record("PRIOR_EXPERIENCE_EVENT", "ExperienceEvent", record, experience_text(record)))
    for record in ledger.records("LabelIntegrityReport"):
        if is_label_failure_report(record):
            sources.append(source_from_record("HISTORICAL_FAILURE", "LabelIntegrityReport", record, label_integrity_text(record)))
    for record in ledger.records("EstimatorPerformanceReport"):
        sources.append(source_from_record("MECHANISM_GAP", "EstimatorPerformanceReport", record, estimator_report_text(record)))
    return sources


def source_from_record(source_type: str, object_type: str, record: dict[str, Any], text: str) -> dict[str, Any]:
    return {
        "source_type": source_type,
        "source_object_type": object_type,
        "source_reference": f"{object_type}:{object_id_for_record(record)}",
        "source_event_ids": source_event_ids_for_record(object_type, record),
        "source_mechanism_ids": source_mechanism_ids_for_record(record),
        "source_record": record,
        "source_text": text,
    }


def object_id_for_record(record: dict[str, Any]) -> str:
    for field in ("experience_id", "regret_id", "calibration_id", "record_id", "mechanism_id", "contrarian_id", "report_id", "estimate_id"):
        if record.get(field):
            return str(record[field])
    return short_hash(json.dumps(record, sort_keys=True))


def source_event_ids_for_record(object_type: str, record: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for field in ("experience_id", "regret_id", "calibration_id", "record_id", "report_id", "estimate_id"):
        if record.get(field):
            ids.append(f"{object_type}:{record[field]}")
            break
    for linked in ("experience_id", "regret_id", "calibration_id", "historical_record_id"):
        if record.get(linked):
            marker = f"{linked}:{record[linked]}"
            if marker not in ids:
                ids.append(marker)
    return ids


def source_mechanism_ids_for_record(record: dict[str, Any]) -> list[str]:
    ids = []
    for field in ("mechanism_id", "parent_mechanism_id"):
        if record.get(field):
            ids.append(str(record[field]))
    return ids


def historical_source_type(record: dict[str, Any]) -> str | None:
    source_type = str(record.get("source_type") or "")
    if source_type == "FAILURE":
        return "HISTORICAL_FAILURE"
    if source_type == "CONTRADICTION":
        return "HISTORICAL_CONTRADICTION"
    actual = str(record.get("actual_outcome") or record.get("conversion_reason") or "").lower()
    if "fail" in actual or "miss" in actual or "contradict" in actual:
        return "HISTORICAL_FAILURE"
    return None


def is_label_failure_report(record: dict[str, Any]) -> bool:
    status = str(record.get("label_independence_status") or "")
    text = label_integrity_text(record).lower()
    return status in {"CIRCULAR", "HIGHLY_SHARED", "FAIL"} or "label" in text and ("circular" in text or "shared" in text)


def mechanism_family_for_source(source: dict[str, Any], text: str) -> str:
    record = source["source_record"]
    if source["source_object_type"] == "LabelIntegrityReport":
        return "SESSION_TIMING"
    if record.get("mechanism_family"):
        return str(record["mechanism_family"])
    family, _, _ = classify_mechanism_text(text)
    return family


def build_generated_claim_payload(source: dict[str, Any], *, created_at: str) -> dict[str, Any]:
    text = str(source["source_text"] or "")
    family = mechanism_family_for_source(source, text)
    source_type = str(source["source_type"])
    status = status_for_source(source, family, text)
    claim_text = claim_text_for_source(source_type, family, status)
    payload = {
        "generated_claim_id": f"grc-{short_hash(source['source_reference'] + ':' + family + ':' + claim_text)}",
        "created_at": created_at,
        "source_event_ids": source["source_event_ids"],
        "source_mechanism_ids": source["source_mechanism_ids"],
        "source_type": source_type,
        "source_reference": source["source_reference"],
        "mechanism_family": family,
        "claim_text": claim_text,
        "rationale": rationale_for_source(source, family, status),
        "novelty_basis": novelty_basis_for_source(source, family, status),
        "expected_learning_value": expected_learning_value_for_source(source, status),
        "uncertainty_score": uncertainty_score_for_source(source, status),
        "contrarian_prompt": contrarian_prompt_for_family(family, status),
        "intended_testability": intended_testability_for_family(family, status),
        "authority_boundary_acknowledged": True,
        "status": status,
        **rule_fields_for_family(family),
    }
    assert_no_forbidden_claim_text(payload)
    return payload


def status_for_source(source: dict[str, Any], family: str, text: str) -> str:
    if source["source_object_type"] == "LabelIntegrityReport" and not is_label_failure_report(source["source_record"]):
        return "INSUFFICIENT_BASIS"
    if family == "UNKNOWN":
        return "UNSUPPORTED_MECHANISM"
    if not has_testable_basis(source, text):
        return "REJECTED_LOW_TESTABILITY"
    return "GENERATED"


def has_testable_basis(source: dict[str, Any], text: str) -> bool:
    record = source["source_record"]
    object_type = source["source_object_type"]
    if object_type == "HistoricalExperienceRecord":
        return float(record.get("experience_quality_score") or 0.0) >= 0.25
    if object_type == "Regret":
        return float(record.get("regret_score") or record.get("importance_weighted_regret") or 0.0) > 0
    if object_type == "CalibrationRecord":
        return float(record.get("calibration_error") or 0.0) > 0
    if object_type == "MechanismRegistryEntry":
        return int(record.get("claim_count") or 0) > 0 and str(record.get("status") or "") not in {"RETAINED_NOT_PROMOTED", "UNKNOWN_RETAINED"}
    if object_type == "ExternalStrategyContrarianTheory":
        return bool(record.get("required_falsification_tests") or record.get("primary_failure_modes"))
    if object_type == "ExperienceEvent":
        return float(record.get("experience_quality_score") or 0.0) >= 0.25
    if object_type == "LabelIntegrityReport":
        return is_label_failure_report(record)
    if object_type == "EstimatorPerformanceReport":
        return int(record.get("estimate_count") or 0) > 0 or bool(text.strip())
    return bool(text.strip())


def claim_text_for_source(source_type: str, family: str, status: str) -> str:
    phrase = family_phrase(family)
    if status == "UNSUPPORTED_MECHANISM":
        return "Unknown-mechanism evidence may require mechanism classification before bounded claim generation."
    if source_type == "HISTORICAL_FAILURE" and family == "SESSION_TIMING":
        return "Label-failure evidence may require source-field separation before normal mechanism research claims are generated."
    if status == "REJECTED_LOW_TESTABILITY":
        return f"{phrase} evidence may require clearer observable rules before it can become a bounded research claim."
    if status == "INSUFFICIENT_BASIS":
        return "Label-integrity evidence may require label-failure isolation before normal mechanism claims are generated."
    if source_type == "HISTORICAL_FAILURE":
        return f"{phrase} mechanisms may require failure-mode segmentation before cheap testing."
    if source_type == "HISTORICAL_CONTRADICTION":
        return f"{phrase} claims may become more testable when contradictory outcomes are separated by regime or rule detail."
    if source_type == "REGRET_SIGNAL":
        return f"{phrase} claims with prior regret may require simpler baseline comparisons before additional research steps."
    if source_type == "CALIBRATION_ERROR":
        return f"{phrase} claims may need confidence calibration checks before downstream hypothesis generation."
    if source_type == "MECHANISM_GAP":
        return f"{phrase} mechanisms may require claim-count and source-diversity checks before cheap testing."
    if source_type == "CONTRARIAN_GAP":
        return f"{phrase} claims may be more falsifiable when contrarian failure prompts are converted into explicit baseline checks."
    return f"{phrase} prior experience events may reveal more learning value when repeated with matched baselines and uncertainty scoring."


def rationale_for_source(source: dict[str, Any], family: str, status: str) -> str:
    if status != "GENERATED":
        return "Source was retained but did not meet the safe basis for a normal generated research claim."
    return f"Existing {source['source_object_type']} evidence supports a bounded {family} research claim without creating hypotheses or experiments."


def novelty_basis_for_source(source: dict[str, Any], family: str, status: str) -> str:
    if status != "GENERATED":
        return "No normal novelty basis recorded because source basis was insufficient, duplicate, or unsupported."
    return f"Novelty comes from combining {source['source_type']} evidence with mechanism-level uncertainty and a baseline-oriented testability prompt."


def contrarian_prompt_for_family(family: str, status: str) -> str:
    if status != "GENERATED":
        return "What missing mechanism, source-quality, or rule-detail issue blocks this claim from becoming testable?"
    return f"What baseline observation would show that {family} adds no learning signal beyond a simpler comparison condition?"


def intended_testability_for_family(family: str, status: str) -> str:
    if status != "GENERATED":
        return "Testability requires resolving the missing basis, then comparing observable rules against a baseline before falsification."
    return f"Compare {family} observations against matched baseline observations; falsify if separation is absent or rule coverage is insufficient."


def expected_learning_value_for_source(source: dict[str, Any], status: str) -> float:
    if status != "GENERATED":
        return 0.12
    record = source["source_record"]
    values = [0.42]
    for field in ("expected_learning_value", "expected_learning_value_pre_outcome", "actual_learning_value_post_outcome", "importance_weighted_regret", "calibration_error"):
        if isinstance(record.get(field), (int, float)):
            values.append(min(max(float(record[field]), 0.0), 0.85))
    return round(max(values), 4)


def uncertainty_score_for_source(source: dict[str, Any], status: str) -> float:
    if status != "GENERATED":
        return 0.82
    record = source["source_record"]
    if isinstance(record.get("confidence"), (int, float)):
        return round(max(0.15, min(0.95, 1.0 - float(record["confidence"]))), 4)
    if isinstance(record.get("calibration_error"), (int, float)):
        return round(max(0.2, min(0.95, float(record["calibration_error"]))), 4)
    return 0.58


def generated_claim_fingerprint(record: dict[str, Any]) -> str:
    material = re.sub(r"[^a-z0-9]+", " ", f"{record.get('mechanism_family')} {record.get('claim_text')}".lower()).strip()
    return short_hash(material)


def existing_generated_fingerprints(ledger: AtlasV2Ledger) -> set[str]:
    return {generated_claim_fingerprint(record) for record in ledger.records("GeneratedResearchClaim")}


def assert_source_safe(source: dict[str, Any]) -> None:
    if source["source_type"] not in ALLOWED_SOURCE_TYPES:
        raise AtlasV2ValidationError(f"Experience-Driven Claim Generator unsupported source_type: {source['source_type']}")
    for field in PROHIBITED_AUTHORITY_FIELDS:
        if source["source_record"].get(field) not in (None, "", False, [], {}):
            raise AtlasV2ValidationError(f"Experience-Driven Claim Generator stopped on prohibited authority field: {field}")


def assert_no_forbidden_claim_text(payload: dict[str, Any]) -> None:
    combined = " ".join(str(payload.get(field) or "") for field in ("claim_text", "rationale", "novelty_basis", "contrarian_prompt", "intended_testability")).lower()
    if any(term in combined for term in AUTHORITY_TEXT_TERMS):
        raise AtlasV2ValidationError("GeneratedResearchClaim cannot imply trading, profitability, recommendation, validation, broker, sleeve, candidate, paper-position, order, or allocation authority")


def historical_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("decision_summary", "expected_outcome", "actual_outcome", "conversion_reason", "lesson", "provenance_reference"))


def regret_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("regret_reason", "missed_alternative", "decision_id", "outcome_id"))


def calibration_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("actual_result", "calibration_bucket", "prediction_id"))


def registry_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("mechanism_family", "status", "mechanism_id"))


def contrarian_text(record: dict[str, Any]) -> str:
    return " ".join([str(record.get("opposite_hypothesis") or ""), " ".join(str(item) for item in record.get("fragility_conditions", [])), " ".join(str(item) for item in record.get("required_falsification_tests", []))])


def experience_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("lesson", "source_artifact", "source_type", "provenance_reference"))


def label_integrity_text(record: dict[str, Any]) -> str:
    return " ".join([str(record.get("dataset_name") or ""), str(record.get("label_independence_status") or ""), " ".join(str(item) for item in record.get("warnings", [])), " ".join(str(item) for item in record.get("failures", []))])


def estimator_report_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("summary", "estimator_run_id", "period_start", "period_end"))


def family_phrase(family: str) -> str:
    return family.replace("_", " ").title().replace("Or", "or")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate bounded Atlas V2 research claims from existing experience evidence.")
    parser.add_argument("--ledger-root", type=Path, default=DEFAULT_LEDGER_ROOT)
    parser.add_argument("--max-claims", type=int, default=DEFAULT_MAX_CLAIMS)
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--run-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = AtlasV2ExperienceDrivenClaimGenerator(AtlasV2Ledger(args.ledger_root)).generate(
        max_claims=args.max_claims,
        run_id=args.run_id,
        created_at=args.created_at,
    )
    json.dump(result.summary(), sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
