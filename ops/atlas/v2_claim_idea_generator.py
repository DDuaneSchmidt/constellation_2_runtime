from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, PROHIBITED_AUTHORITY_FIELDS
from ops.atlas.v2_external_strategy_claim_extractor import DEFAULT_CREATED_AT, classify_mechanism_text, short_hash

GENERATOR_VERSION = "atlas_v2_claim_idea_generator_v1"
DEFAULT_LEDGER_ROOT = Path("/tmp/atlas_v2_claim_idea_generator_v1_ledgers")
DEFAULT_MAX_CLAIMS = 10
MAX_CLAIMS_HARD_CAP = 100
SUPPORTED_SOURCE_TYPES = {
    "HISTORICAL_FAILURE",
    "HISTORICAL_CONTRADICTION",
    "HISTORICAL_DECISION",
    "HISTORICAL_RESEARCH_OUTCOME",
    "MECHANISM_REGISTRY",
    "CONTRARIAN_GAP",
    "TRANSCRIPT_CLAIM",
    "PRIOR_EXPERIENCE_EVENT",
}
GENERATED_STATUSES = {"GENERATED", "DUPLICATE", "INSUFFICIENT_BASIS", "UNSUPPORTED_MECHANISM", "REJECTED_LOW_TESTABILITY"}
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
)


@dataclass(frozen=True)
class ClaimIdeaGenerationResult:
    generated_claims: list[dict[str, Any]]
    run: dict[str, Any]
    source_summaries: list[dict[str, Any]]

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


class AtlasV2ClaimIdeaGenerator:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def generate(
        self,
        *,
        max_claims: int = DEFAULT_MAX_CLAIMS,
        run_id: str | None = None,
        created_at: str = DEFAULT_CREATED_AT,
    ) -> ClaimIdeaGenerationResult:
        if max_claims < 0 or max_claims > MAX_CLAIMS_HARD_CAP:
            raise AtlasV2ValidationError("Claim Idea Generator max_claims must be between 0 and 100")

        sources = collect_generation_sources(self.ledger)
        selected_sources = sources[:max_claims]
        generated_claims: list[dict[str, Any]] = []
        source_counts: dict[str, Counter[str]] = defaultdict(Counter)
        seen_fingerprints = existing_generated_fingerprints(self.ledger)
        source_inputs = [source["source_reference"] for source in selected_sources]

        for source in selected_sources:
            source_type = str(source["source_type"])
            source_counts[source_type]["source_count"] += 1
            self._assert_no_authority_violation(source)
            draft = build_generated_claim_payload(source, created_at=created_at)
            fingerprint = generated_claim_fingerprint(draft)
            if fingerprint in seen_fingerprints and draft["status"] == "GENERATED":
                draft = {
                    **draft,
                    "generated_claim_id": f"grc-{short_hash(source['source_reference'] + ':duplicate:' + fingerprint)}",
                    "expected_learning_value": min(float(draft["expected_learning_value"]), 0.2),
                    "status": "DUPLICATE",
                }
            seen_fingerprints.add(fingerprint)

            claim = self.ledger.create_record(
                "GeneratedResearchClaim",
                draft,
                reason="bounded Atlas V2 research claim generated from existing safe source only",
                triggering_object=source["source_reference"],
            )
            generated_claims.append(claim)
            if claim["status"] == "GENERATED":
                source_counts[source_type]["generated_count"] += 1
            elif claim["status"] == "DUPLICATE":
                source_counts[source_type]["duplicate_count"] += 1
            else:
                source_counts[source_type]["rejected_count"] += 1

        run_record_id = run_id or f"cgr-{short_hash(':'.join(source_inputs) + ':' + str(max_claims) + ':' + GENERATOR_VERSION)}"
        generated_count = sum(1 for claim in generated_claims if claim["status"] == "GENERATED")
        duplicate_count = sum(1 for claim in generated_claims if claim["status"] == "DUPLICATE")
        insufficient_count = sum(1 for claim in generated_claims if claim["status"] in {"INSUFFICIENT_BASIS", "UNSUPPORTED_MECHANISM", "REJECTED_LOW_TESTABILITY"})
        mechanism_distribution = dict(Counter(claim["mechanism_family"] for claim in generated_claims if claim["status"] == "GENERATED"))
        run_status = "NO_ELIGIBLE_SOURCES" if not selected_sources else ("COMPLETED_WITH_REJECTIONS" if insufficient_count or duplicate_count else "COMPLETED")
        run = self.ledger.create_record(
            "ClaimGenerationRun",
            {
                "run_id": run_record_id,
                "created_at": created_at,
                "source_inputs": source_inputs,
                "max_claims": max_claims,
                "claims_generated": generated_count,
                "duplicates_detected": duplicate_count,
                "insufficient_basis_count": insufficient_count,
                "mechanism_distribution": mechanism_distribution,
                "status": run_status,
            },
            reason="claim idea generation run summarized without hypothesis or experiment creation",
            triggering_object=GENERATOR_VERSION,
        )

        summaries: list[dict[str, Any]] = []
        for source_type in sorted(source_counts):
            counts = source_counts[source_type]
            summaries.append(
                self.ledger.create_record(
                    "ClaimGenerationSourceSummary",
                    {
                        "summary_id": f"cgs-{short_hash(run_record_id + ':' + source_type)}",
                        "run_id": run_record_id,
                        "source_type": source_type,
                        "source_count": int(counts["source_count"]),
                        "generated_count": int(counts["generated_count"]),
                        "duplicate_count": int(counts["duplicate_count"]),
                        "rejected_count": int(counts["rejected_count"]),
                        "created_at": created_at,
                        "status": "SUMMARIZED",
                    },
                    reason="claim idea generation source type summarized",
                    triggering_object=f"ClaimGenerationRun:{run_record_id}",
                )
            )
        return ClaimIdeaGenerationResult(generated_claims=generated_claims, run=run, source_summaries=summaries)

    def external_strategy_claim_payloads(self, generated_claims: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        claims = generated_claims if generated_claims is not None else self.ledger.records("GeneratedResearchClaim")
        return [generated_claim_to_external_strategy_claim_payload(claim) for claim in claims if claim.get("status") == "GENERATED"]

    def _assert_no_authority_violation(self, record: dict[str, Any]) -> None:
        for field in PROHIBITED_AUTHORITY_FIELDS:
            if record.get(field) not in (None, "", False, [], {}):
                raise AtlasV2ValidationError(f"Claim Idea Generator stopped on prohibited authority field: {field}")
        text = json.dumps(record, sort_keys=True).lower()
        if any(term in text for term in ("broker_order", "live_market_access", "capital_allocation")):
            raise AtlasV2ValidationError("Claim Idea Generator source contains prohibited authority text")


def collect_generation_sources(ledger: AtlasV2Ledger) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    for record in ledger.records("HistoricalExperienceRecord"):
        source_type = historical_source_type(record)
        if source_type:
            sources.append(source_from_record(source_type, "HistoricalExperienceRecord", record, historical_text(record)))
    for record in ledger.records("MechanismRegistryEntry"):
        sources.append(source_from_record("MECHANISM_REGISTRY", "MechanismRegistryEntry", record, registry_text(record)))
    for record in ledger.records("ExternalStrategyContrarianTheory"):
        sources.append(source_from_record("CONTRARIAN_GAP", "ExternalStrategyContrarianTheory", record, contrarian_text(record)))
    for record in ledger.records("ExternalStrategyClaim"):
        if str(record.get("extraction_status") or "") == "EXTRACTED":
            sources.append(source_from_record("TRANSCRIPT_CLAIM", "ExternalStrategyClaim", record, external_claim_text(record)))
    for record in ledger.records("ExperienceEvent"):
        sources.append(source_from_record("PRIOR_EXPERIENCE_EVENT", "ExperienceEvent", record, experience_text(record)))
    for record in ledger.records("LabelIntegrityReport"):
        sources.append(source_from_record("HISTORICAL_RESEARCH_OUTCOME", "LabelIntegrityReport", record, label_integrity_text(record)))
    for record in ledger.records("EstimatorPerformanceReport"):
        sources.append(source_from_record("HISTORICAL_RESEARCH_OUTCOME", "EstimatorPerformanceReport", record, estimator_report_text(record)))
    return sources


def source_from_record(source_type: str, object_type: str, record: dict[str, Any], text: str) -> dict[str, Any]:
    return {
        "source_type": source_type,
        "source_object_type": object_type,
        "source_reference": f"{object_type}:{object_id_for_record(record)}",
        "source_record": record,
        "source_text": text,
    }


def object_id_for_record(record: dict[str, Any]) -> str:
    for field in ("record_id", "experience_id", "mechanism_id", "contrarian_id", "claim_id", "report_id"):
        if record.get(field):
            return str(record[field])
    return short_hash(json.dumps(record, sort_keys=True))


def historical_source_type(record: dict[str, Any]) -> str | None:
    mapping = {
        "FAILURE": "HISTORICAL_FAILURE",
        "CONTRADICTION": "HISTORICAL_CONTRADICTION",
        "DECISION": "HISTORICAL_DECISION",
        "RESEARCH_OUTCOME": "HISTORICAL_RESEARCH_OUTCOME",
    }
    return mapping.get(str(record.get("source_type") or ""))


def historical_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("decision_summary", "expected_outcome", "actual_outcome", "conversion_reason", "provenance_reference"))


def registry_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("mechanism_family", "status", "mechanism_id"))


def contrarian_text(record: dict[str, Any]) -> str:
    return " ".join([
        str(record.get("opposite_hypothesis") or ""),
        " ".join(str(item) for item in record.get("fragility_conditions", [])),
        " ".join(str(item) for item in record.get("required_falsification_tests", [])),
    ])


def external_claim_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("claim_text", "entry_rule", "exit_rule", "filter_rule", "risk_rule", "timeframe", "instrument"))


def experience_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("lesson", "source_artifact", "historical_record_id", "experience_id"))


def label_integrity_text(record: dict[str, Any]) -> str:
    return " ".join([
        str(record.get("dataset_name") or ""),
        str(record.get("label_independence_status") or ""),
        " ".join(str(item) for item in record.get("warnings", [])),
    ])


def estimator_report_text(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in ("summary", "estimator_run_id", "period_start", "period_end"))


def build_generated_claim_payload(source: dict[str, Any], *, created_at: str) -> dict[str, Any]:
    source_type = str(source["source_type"])
    record = source["source_record"]
    text = str(source["source_text"])
    family = mechanism_family_for_source(source, text)
    status = status_for_source(source, family, text)
    claim_text = claim_text_for_source(source, family, status)
    expected_learning_value = expected_learning_for_status(source, status)
    uncertainty_score = uncertainty_for_source(source, status)
    payload = {
        "generated_claim_id": f"grc-{short_hash(source['source_reference'] + ':' + family + ':' + claim_text)}",
        "created_at": created_at,
        "source_type": source_type,
        "source_reference": source["source_reference"],
        "source_event_ids": source_event_ids_for_source(source),
        "source_mechanism_ids": source_mechanism_ids_for_source(source, family),
        "mechanism_family": family,
        "claim_text": claim_text,
        "rationale": rationale_for_source(source, family, status),
        "novelty_basis": novelty_basis_for_source(source, family, status),
        "expected_learning_value": expected_learning_value,
        "uncertainty_score": uncertainty_score,
        "contrarian_prompt": contrarian_prompt_for_family(family, status),
        "intended_testability": intended_testability_for_family(family, status),
        "authority_boundary_acknowledged": True,
        "status": status,
        **rule_fields_for_family(family),
    }
    assert_no_forbidden_claim_text(payload)
    return payload



def source_event_ids_for_record(object_type: str, record: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for field in ("experience_id", "regret_id", "calibration_id", "record_id", "report_id", "claim_id", "contrarian_id", "mechanism_id"):
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

def mechanism_family_for_source(source: dict[str, Any], text: str) -> str:
    record = source["source_record"]
    if record.get("mechanism_family"):
        return str(record["mechanism_family"])
    family, _, _ = classify_mechanism_text(text)
    return family


def status_for_source(source: dict[str, Any], family: str, text: str) -> str:
    record = source["source_record"]
    if source["source_object_type"] == "LabelIntegrityReport":
        label_status = str(record.get("label_independence_status") or "")
        if label_status in {"CIRCULAR", "HIGHLY_SHARED"}:
            return "INSUFFICIENT_BASIS"
    if family == "UNKNOWN":
        return "UNSUPPORTED_MECHANISM"
    if not has_testable_basis(source, text):
        return "REJECTED_LOW_TESTABILITY"
    return "GENERATED"


def has_testable_basis(source: dict[str, Any], text: str) -> bool:
    record = source["source_record"]
    if source["source_object_type"] == "MechanismRegistryEntry":
        return int(record.get("claim_count") or 0) > 0 or int(record.get("learning_event_count") or 0) > 0
    if source["source_object_type"] == "ExternalStrategyContrarianTheory":
        return bool(record.get("required_falsification_tests")) and str(record.get("status") or "") == "GENERATED"
    if source["source_object_type"] == "ExternalStrategyClaim":
        return bool(record.get("entry_rule") and (record.get("exit_rule") or record.get("risk_rule") or record.get("filter_rule")))
    if source["source_object_type"] == "HistoricalExperienceRecord":
        return str(record.get("status") or "") in {"CONVERTED", "INCOMPLETE_PROVENANCE"} and float(record.get("experience_quality_score") or 0) >= 0.35
    if source["source_object_type"] == "ExperienceEvent":
        return float(record.get("experience_quality_score") or 0) >= 0.35
    if source["source_object_type"] == "EstimatorPerformanceReport":
        return int(record.get("estimate_count") or 0) > 0
    return bool(text.strip())


def source_event_ids_for_source(source: dict[str, Any]) -> list[str]:
    record = source["source_record"]
    if source["source_object_type"] in {"HistoricalExperienceRecord", "ExperienceEvent"}:
        return [object_id_for_record(record)]
    return []


def source_mechanism_ids_for_source(source: dict[str, Any], family: str) -> list[str]:
    record = source["source_record"]
    if record.get("mechanism_id"):
        return [str(record["mechanism_id"])]
    if family != "UNKNOWN" and source["source_object_type"] == "MechanismRegistryEntry":
        return [object_id_for_record(record)]
    return []


def claim_text_for_source(source: dict[str, Any], family: str, status: str) -> str:
    phrase = family_phrase(family)
    if status == "INSUFFICIENT_BASIS":
        return "Label-integrity concerns may require source-field separation tests before normal mechanism research claims are generated."
    if status == "UNSUPPORTED_MECHANISM":
        return "Unknown-mechanism source material needs mechanism classification before a bounded research claim can be tested."
    if status == "REJECTED_LOW_TESTABILITY":
        return f"{phrase} source material needs clearer observable rules before a bounded research claim can be tested."
    if source["source_type"] == "HISTORICAL_FAILURE":
        return f"{phrase} claims may produce clearer learning records when failure modes are paired with explicit baseline comparisons and rule-clarification checks."
    if source["source_type"] == "MECHANISM_REGISTRY":
        return f"{phrase} claims may be more falsifiable when registry-level mechanism counts are tested against simple matched baseline observations."
    if source["source_type"] == "CONTRARIAN_GAP":
        return f"{phrase} claims may be more testable when contrarian failure prompts are converted into explicit baseline and falsification checks."
    if source["source_type"] == "TRANSCRIPT_CLAIM":
        return f"{phrase} transcript claims may produce higher learning value when vague rule components are separated from outcome replay checks."
    if source["source_type"] == "PRIOR_EXPERIENCE_EVENT":
        return f"{phrase} prior experience events may reveal more learning value when repeated with a matched baseline and explicit uncertainty scoring."
    return f"{phrase} research outcomes may become more falsifiable when the next claim includes a simple baseline and explicit uncertainty score."


def rationale_for_source(source: dict[str, Any], family: str, status: str) -> str:
    if status != "GENERATED":
        return "Source was retained as claim-generation evidence but did not meet the safe basis for a normal generated research claim."
    return f"Existing {source['source_object_type']} evidence references {family}; the generated claim is framed only as a bounded research question."


def novelty_basis_for_source(source: dict[str, Any], family: str, status: str) -> str:
    if status != "GENERATED":
        return "No normal novelty basis recorded because source basis was insufficient, duplicate, or unsupported."
    return f"Novelty comes from recombining {source['source_type']} evidence with a mechanism-linked baseline design, not from asserting outcome truth."


def contrarian_prompt_for_family(family: str, status: str) -> str:
    if status != "GENERATED":
        return "What missing rule detail or source-quality issue prevents this from becoming a bounded testable claim?"
    return f"What baseline observation would show that {family} adds no learning signal beyond the simpler comparison condition?"


def intended_testability_for_family(family: str, status: str) -> str:
    if status == "INSUFFICIENT_BASIS":
        return "Test label failure by comparing expected and actual label source fields before any normal mechanism replay."
    if status == "UNSUPPORTED_MECHANISM":
        return "Testability requires mechanism classification before baseline or falsification design."
    if status == "REJECTED_LOW_TESTABILITY":
        return "Testability requires explicit observable entry, stop, baseline, and falsification rule fields."
    return f"Compare {family} condition observations against matched baseline observations; falsify if separation is absent or rule coverage is insufficient."


def expected_learning_for_status(source: dict[str, Any], status: str) -> float:
    if status != "GENERATED":
        return 0.12
    record = source["source_record"]
    base = 0.42
    for field in ("expected_learning_value_pre_outcome", "actual_learning_value_post_outcome", "expected_learning_value"):
        if isinstance(record.get(field), (int, float)):
            base = max(base, min(float(record[field]), 0.82))
    return round(base, 4)


def uncertainty_for_source(source: dict[str, Any], status: str) -> float:
    if status != "GENERATED":
        return 0.82
    record = source["source_record"]
    confidence = record.get("confidence")
    if isinstance(confidence, (int, float)):
        return round(max(0.15, min(0.95, 1.0 - float(confidence))), 4)
    return 0.58


def rule_fields_for_family(family: str) -> dict[str, Any]:
    rules = {
        "OPENING_RANGE": {
            "entry_rule": "observe a candle close beyond the opening range boundary after the range is complete",
            "exit_rule": "end the observation at a fixed window close or explicit target condition",
            "risk_rule": "stop the observation if price closes back inside the opening range boundary",
            "filter_rule": "record whether a regime filter is present before baseline comparison",
            "timeframe": "opening range fixture window",
        },
        "VWAP_OR_AVERAGE_RECLAIM": {
            "entry_rule": "observe a candle close back above VWAP or the specified average after trading below it",
            "exit_rule": "end the observation after the bounded replay window or a prior swing reference",
            "risk_rule": "stop the observation if price closes back below the reclaim candle low",
            "filter_rule": "compare against a simple threshold crossing without VWAP context",
            "timeframe": "mock historical VWAP replay window",
        },
        "MEAN_REVERSION": {
            "entry_rule": "observe rejection or bounce at a predefined level after a failed extension",
            "exit_rule": "end the observation at the midpoint reference or bounded replay window",
            "risk_rule": "stop the observation beyond the failed extension wick",
            "filter_rule": "record whether reversal confirmation is present before baseline comparison",
            "timeframe": "mock historical mean reversion replay window",
        },
        "BREAKOUT": {
            "entry_rule": "observe a candle close beyond the prior range high or low",
            "exit_rule": "end the observation at the measured midpoint or bounded replay window",
            "risk_rule": "stop the observation if price closes back inside the prior range",
            "filter_rule": "compare against range observations without breakout confirmation",
            "timeframe": "mock historical breakout replay window",
        },
        "MOMENTUM": {
            "entry_rule": "observe follow through after a strong candle or impulse sequence closes",
            "exit_rule": "end the observation after a fixed number of bars or loss of impulse midpoint",
            "risk_rule": "stop the observation below the impulse sequence low",
            "filter_rule": "compare against observations without strong candle impulse context",
            "timeframe": "mock historical momentum replay window",
        },
    }
    default = {
        "entry_rule": "observe the mechanism condition after an explicit candle close or threshold event",
        "exit_rule": "end the observation after the bounded replay window",
        "risk_rule": "stop the observation if the mechanism condition is negated",
        "filter_rule": "compare against observations where the mechanism condition is absent",
        "timeframe": "mock historical fixture replay window",
    }
    return {**rules.get(family, default), "instrument": "Atlas V2 mock historical fixture observations"}


def family_phrase(family: str) -> str:
    return family.replace("_", " ").title().replace("Or", "or")


def generated_claim_fingerprint(record: dict[str, Any]) -> str:
    material = re.sub(r"[^a-z0-9]+", " ", f"{record.get('mechanism_family')} {record.get('claim_text')}".lower()).strip()
    return short_hash(material)


def existing_generated_fingerprints(ledger: AtlasV2Ledger) -> set[str]:
    return {generated_claim_fingerprint(record) for record in ledger.records("GeneratedResearchClaim")}


def assert_no_forbidden_claim_text(payload: dict[str, Any]) -> None:
    combined = " ".join(str(payload.get(field) or "") for field in ("claim_text", "rationale", "novelty_basis", "contrarian_prompt", "intended_testability")).lower()
    if any(term in combined for term in AUTHORITY_TEXT_TERMS):
        raise AtlasV2ValidationError("GeneratedResearchClaim cannot imply trading, profitability, recommendation, validation, broker, sleeve, candidate, paper-position, or allocation authority")


def generated_claim_to_external_strategy_claim_payload(claim: dict[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    if str(claim.get("status") or "") != "GENERATED":
        raise AtlasV2ValidationError("Only GENERATED research claims can be adapted to ExternalStrategyClaim payloads")
    return {
        "claim_id": f"generated-external-{claim['generated_claim_id']}",
        "source_id": f"generated-research-claim:{claim['generated_claim_id']}",
        "claim_text": claim["claim_text"],
        "entry_rule": str(claim.get("entry_rule") or rule_fields_for_family(str(claim["mechanism_family"]))["entry_rule"]),
        "exit_rule": str(claim.get("exit_rule") or rule_fields_for_family(str(claim["mechanism_family"]))["exit_rule"]),
        "risk_rule": str(claim.get("risk_rule") or rule_fields_for_family(str(claim["mechanism_family"]))["risk_rule"]),
        "filter_rule": str(claim.get("filter_rule") or rule_fields_for_family(str(claim["mechanism_family"]))["filter_rule"]),
        "timeframe": str(claim.get("timeframe") or rule_fields_for_family(str(claim["mechanism_family"]))["timeframe"]),
        "instrument": str(claim.get("instrument") or "Atlas V2 mock historical fixture observations"),
        "confidence": round(max(0.1, min(0.9, 1.0 - float(claim["uncertainty_score"]))), 4),
        "extraction_status": "EXTRACTED",
        "created_at": created_at or str(claim.get("created_at") or DEFAULT_CREATED_AT),
    }


def _read_sources(path: str | None) -> list[dict[str, Any]] | None:
    if not path:
        return None
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise AtlasV2ValidationError("source input must be an array of Atlas V2 records")
    return data


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate bounded Atlas V2 research claims from existing safe ledger artifacts.")
    parser.add_argument("--ledger-root", type=Path, default=DEFAULT_LEDGER_ROOT)
    parser.add_argument("--max-claims", type=int, default=DEFAULT_MAX_CLAIMS)
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--run-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = AtlasV2ClaimIdeaGenerator(AtlasV2Ledger(args.ledger_root)).generate(
        max_claims=args.max_claims,
        run_id=args.run_id,
        created_at=args.created_at,
    )
    print(json.dumps(result.summary(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
