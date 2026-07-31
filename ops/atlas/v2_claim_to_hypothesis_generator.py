from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError
from ops.atlas.v2_external_strategy_claim_extractor import DEFAULT_CREATED_AT, short_hash

GENERATOR_VERSION = "atlas_v2_claim_to_hypothesis_generator_v1"
DEFAULT_LEDGER_ROOT = Path("/tmp/atlas_v2_claim_to_hypothesis_generator_v1_ledgers")
GENERATED_STATUS = "GENERATED"
INSUFFICIENT_STATUS = "INSUFFICIENT_DETAIL"
UNSUPPORTED_STATUS = "UNSUPPORTED_MECHANISM"
REJECTED_STATUS = "REJECTED_LOW_SPECIFICITY"


@dataclass(frozen=True)
class HypothesisGenerationResult:
    hypotheses: list[dict[str, Any]]
    falsification_plans: list[dict[str, Any]]
    run: dict[str, Any]

    def summary(self) -> dict[str, Any]:
        return {
            "run_id": self.run["run_id"],
            "claims_processed": self.run["claims_processed"],
            "hypotheses_created": self.run["hypotheses_created"],
            "insufficient_detail_count": self.run["insufficient_detail_count"],
            "duplicate_hypothesis_count": self.run["duplicate_hypothesis_count"],
            "status": self.run["status"],
        }


class AtlasV2ClaimToHypothesisGenerator:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def generate(
        self,
        *,
        claim_ids: list[str] | None = None,
        source_batch_id: str | None = None,
        source_mechanism_registry_id: str | None = None,
        run_id: str | None = None,
        created_at: str = DEFAULT_CREATED_AT,
    ) -> HypothesisGenerationResult:
        claims = select_claims(self.ledger, claim_ids, source_batch_id=source_batch_id)
        mechanisms = latest_by_field(self.ledger.records("ExternalStrategyMechanism"), "claim_id")
        contrarians = latest_contrarian_by_claim_and_mechanism(self.ledger.records("ExternalStrategyContrarianTheory"))
        dedupe = latest_by_field(self.ledger.records("ExternalStrategyDeduplicationResult"), "claim_id")
        registries = latest_registry_by_family(self.ledger.records("MechanismRegistryEntry"))

        hypotheses: list[dict[str, Any]] = []
        falsification_plans: list[dict[str, Any]] = []
        insufficient_detail_count = 0
        duplicate_hypothesis_count = 0
        seen_hypothesis_keys: set[str] = set()
        run_record_id = run_id or f"hgr-{short_hash(':'.join(sorted(str(claim['claim_id']) for claim in claims)) + ':' + GENERATOR_VERSION)}"

        for claim in claims:
            claim_id = str(claim["claim_id"])
            mechanism = mechanisms.get(claim_id)
            mechanism_id = str(mechanism.get("mechanism_id") if mechanism else "NO_MECHANISM")
            family = str(mechanism.get("mechanism_family") if mechanism else "UNKNOWN")
            contrarian = contrarians.get((claim_id, mechanism_id))
            registry = registries.get(family)
            status = hypothesis_status(claim, mechanism, contrarian)
            hypothesis_key = dedupe_key(claim, mechanism, contrarian)
            dedupe_record = dedupe.get(claim_id)
            if bool(dedupe_record and dedupe_record.get("is_duplicate")) or hypothesis_key in seen_hypothesis_keys:
                duplicate_hypothesis_count += 1
                continue
            seen_hypothesis_keys.add(hypothesis_key)

            if status in {INSUFFICIENT_STATUS, UNSUPPORTED_STATUS, REJECTED_STATUS}:
                insufficient_detail_count += 1

            hypothesis_payload = build_hypothesis_payload(
                claim,
                mechanism,
                contrarian,
                registry,
                status=status,
                created_at=created_at,
            )
            hypothesis = self.ledger.create_record(
                "ResearchHypothesis",
                hypothesis_payload,
                reason="claim converted into Atlas V2 research hypothesis without experiment execution",
                triggering_object=f"ExternalStrategyClaim:{claim_id}",
            )
            hypotheses.append(hypothesis)

            plan_payload = build_falsification_plan_payload(
                hypothesis,
                claim,
                mechanism,
                contrarian,
                created_at=created_at,
            )
            plan = self.ledger.create_record(
                "HypothesisFalsificationPlan",
                plan_payload,
                reason="contrarian theory converted into falsification plan without execution or validation authority",
                triggering_object=f"ResearchHypothesis:{hypothesis['hypothesis_id']}",
            )
            falsification_plans.append(plan)

        if hypotheses:
            run_status = "COMPLETED_WITH_INSUFFICIENT_DETAIL" if insufficient_detail_count else "COMPLETED"
        else:
            run_status = "NO_TESTABLE_HYPOTHESES"

        run_payload: dict[str, Any] = {
            "run_id": run_record_id,
            "created_at": created_at,
            "claims_processed": len(claims),
            "hypotheses_created": len(hypotheses),
            "insufficient_detail_count": insufficient_detail_count,
            "duplicate_hypothesis_count": duplicate_hypothesis_count,
            "status": run_status,
        }
        if source_batch_id:
            run_payload["source_batch_id"] = source_batch_id
        if source_mechanism_registry_id:
            run_payload["source_mechanism_registry_id"] = source_mechanism_registry_id
        run = self.ledger.create_record(
            "HypothesisGenerationRun",
            run_payload,
            reason="hypothesis generation run recorded without experiment execution or authority expansion",
            triggering_object=GENERATOR_VERSION,
        )
        return HypothesisGenerationResult(hypotheses=hypotheses, falsification_plans=falsification_plans, run=run)


def select_claims(ledger: AtlasV2Ledger, claim_ids: list[str] | None, *, source_batch_id: str | None = None) -> list[dict[str, Any]]:
    claims = ledger.records("ExternalStrategyClaim")
    if source_batch_id and claim_ids is None:
        batch = next((item for item in reversed(ledger.records("ClaimBatch")) if item.get("batch_id") == source_batch_id), None)
        if batch is None:
            raise AtlasV2ValidationError(f"ClaimBatch not found for hypothesis generation: {source_batch_id}")
        claim_ids = [str(item) for item in batch.get("claim_ids", [])]
    if claim_ids is None:
        return claims
    wanted = {str(claim_id) for claim_id in claim_ids}
    selected = [claim for claim in claims if str(claim.get("claim_id")) in wanted]
    found = {str(claim.get("claim_id")) for claim in selected}
    missing = sorted(wanted - found)
    if missing:
        raise AtlasV2ValidationError(f"ExternalStrategyClaim not found for hypothesis generation: {missing}")
    return selected


def latest_by_field(records: list[dict[str, Any]], field: str) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        latest[str(record.get(field) or "")] = record
    return {key: value for key, value in latest.items() if key}


def latest_contrarian_by_claim_and_mechanism(records: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        latest[(str(record.get("claim_id") or ""), str(record.get("mechanism_id") or ""))] = record
    return {key: value for key, value in latest.items() if all(key)}


def latest_registry_by_family(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        latest[str(record.get("mechanism_family") or "")] = record
    return {key: value for key, value in latest.items() if key}


def hypothesis_status(claim: dict[str, Any], mechanism: dict[str, Any] | None, contrarian: dict[str, Any] | None) -> str:
    if str(claim.get("extraction_status") or "") != "EXTRACTED":
        return INSUFFICIENT_STATUS
    family = str(mechanism.get("mechanism_family") if mechanism else "UNKNOWN")
    if family == "UNKNOWN":
        return UNSUPPORTED_STATUS
    if not has_specific_rule_detail(claim):
        return REJECTED_STATUS
    if not contrarian or str(contrarian.get("status") or "") in {"UNKNOWN", "INSUFFICIENT_DETAIL", "REQUIRES_CLARIFICATION"}:
        return INSUFFICIENT_STATUS
    return GENERATED_STATUS


def has_specific_rule_detail(claim: dict[str, Any]) -> bool:
    text = " ".join(str(claim.get(field) or "") for field in ("claim_text", "entry_rule", "exit_rule", "filter_rule", "risk_rule", "timeframe", "instrument"))
    has_rule = bool(claim.get("entry_rule") and (claim.get("exit_rule") or claim.get("risk_rule") or claim.get("filter_rule")))
    specific_markers = ("opening", "range", "vwap", "atr", "candle", "close", "break", "stop", "target", "2r", "15")
    return has_rule and any(marker in text.lower() for marker in specific_markers)


def dedupe_key(claim: dict[str, Any], mechanism: dict[str, Any] | None, contrarian: dict[str, Any] | None) -> str:
    family = str(mechanism.get("mechanism_family") if mechanism else "UNKNOWN")
    text = " ".join(str(claim.get(field) or "") for field in ("claim_text", "entry_rule", "exit_rule", "filter_rule", "risk_rule"))
    failures = ":".join(sorted(str(item) for item in (contrarian or {}).get("primary_failure_modes", [])))
    normalized = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
    return short_hash(f"{family}:{normalized}:{failures}")


def build_hypothesis_payload(
    claim: dict[str, Any],
    mechanism: dict[str, Any] | None,
    contrarian: dict[str, Any] | None,
    registry: dict[str, Any] | None,
    *,
    status: str,
    created_at: str,
) -> dict[str, Any]:
    claim_id = str(claim["claim_id"])
    mechanism_id = str(mechanism.get("mechanism_id") if mechanism else "NO_MECHANISM")
    family = str(mechanism.get("mechanism_family") if mechanism else "UNKNOWN")
    hypothesis_id = f"rhy-{short_hash(claim_id + ':' + mechanism_id + ':' + GENERATOR_VERSION)}"
    required_data = required_data_for_claim(claim, family)
    criteria = falsification_criteria(claim, mechanism, contrarian, status=status)
    contrarian_inputs = contrarian_inputs_for_hypothesis(contrarian, status=status)

    if status == GENERATED_STATUS:
        entry_rule = clean_sentence(str(claim.get("entry_rule") or claim.get("claim_text") or "source-defined setup"))
        exit_rule = clean_sentence(str(claim.get("exit_rule") or "source-defined follow-through condition"))
        risk_rule = clean_sentence(str(claim.get("risk_rule") or "source-defined invalidation condition"))
        hypothesis_text = (
            f"For {family} claims, when {entry_rule}, subsequent movement or state change toward the source-defined "
            f"objective ({exit_rule}) exceeds the matched baseline condition before the source-defined invalidation ({risk_rule})."
        )
        testable_condition = (
            f"Replay timestamped observations where the {family} setup is present and compare the post-condition movement "
            "against predeclared matched baseline observations using only information available at each timestamp."
        )
        expected_direction = "EFFECT_SIZE_ABOVE_BASELINE"
        baseline = baseline_for_family(family)
        confidence = round(min(float(claim.get("confidence", 0.0) or 0.0), float(mechanism.get("classification_confidence", 0.0) or 0.0)), 4)
        if registry:
            confidence = round(min(0.95, confidence + min(int(registry.get("claim_count", 0)), 10) * 0.005), 4)
    elif status == UNSUPPORTED_STATUS:
        hypothesis_text = f"Claim {claim_id} uses an UNKNOWN mechanism and cannot be converted into a supported research hypothesis."
        testable_condition = "A supported mechanism family must be identified before a replayable hypothesis can be specified."
        expected_direction = "UNSUPPORTED_UNTIL_MECHANISM_CLASSIFIED"
        baseline = "No baseline comparison is defined for UNKNOWN mechanisms until mechanism classification is resolved."
        confidence = min(float(claim.get("confidence", 0.0) or 0.0), 0.2)
    elif status == REJECTED_STATUS:
        hypothesis_text = f"Claim {claim_id} is rejected for low specificity and cannot form a replayable research hypothesis."
        testable_condition = "The claim must specify concrete setup, observation window, and comparison target before hypothesis generation."
        expected_direction = "REJECTED_UNTIL_SPECIFIC_RULES_EXIST"
        baseline = "No baseline comparison can be defined until the claim contains enough replayable specificity."
        confidence = min(float(claim.get("confidence", 0.0) or 0.0), 0.25)
    else:
        hypothesis_text = f"Claim {claim_id} does not contain enough structured detail to form a testable {family} research hypothesis."
        testable_condition = "Explicit setup, observation window, required data, and invalidation conditions must be supplied first."
        expected_direction = "UNSPECIFIED_UNTIL_RULE_DETAIL_EXISTS"
        baseline = "No baseline comparison can be defined until the source claim is replayable."
        confidence = min(float(claim.get("confidence", 0.0) or 0.0), 0.3)

    return {
        "hypothesis_id": hypothesis_id,
        "created_at": created_at,
        "source_claim_id": claim_id,
        "mechanism_id": mechanism_id,
        "mechanism_family": family,
        "hypothesis_text": hypothesis_text,
        "testable_condition": testable_condition,
        "expected_direction": expected_direction,
        "baseline_comparison": baseline,
        "required_data": required_data,
        "falsification_criteria": criteria,
        "contrarian_inputs": contrarian_inputs,
        "confidence": confidence,
        "authority_boundary_acknowledged": True,
        "status": status,
    }


def build_falsification_plan_payload(
    hypothesis: dict[str, Any],
    claim: dict[str, Any],
    mechanism: dict[str, Any] | None,
    contrarian: dict[str, Any] | None,
    *,
    created_at: str,
) -> dict[str, Any]:
    failure_modes = list(contrarian.get("primary_failure_modes") if contrarian else ["VAGUE_RULES"])
    tests = list(contrarian.get("required_falsification_tests") if contrarian else ["Request explicit source rules before defining any replay design."])
    generated = hypothesis["status"] == GENERATED_STATUS
    return {
        "plan_id": f"hfp-{short_hash(hypothesis['hypothesis_id'] + ':' + str((contrarian or {}).get('contrarian_id') or 'NO_CONTRARIAN'))}",
        "created_at": created_at,
        "hypothesis_id": hypothesis["hypothesis_id"],
        "failure_modes": failure_modes,
        "required_tests": tests + [
            "Compare against the declared baseline condition before any experiment execution is considered.",
            "Check whether the effect remains across basic time, instrument, and regime partitions.",
        ],
        "minimum_sample_requirement": 30 if generated else 0,
        "baseline_requirement": hypothesis["baseline_comparison"],
        "falsification_threshold": "; ".join(hypothesis["falsification_criteria"]),
        "status": "CREATED" if generated else ("REQUIRES_DATA" if hypothesis["status"] == UNSUPPORTED_STATUS else "INSUFFICIENT_DETAIL"),
    }


def required_data_for_claim(claim: dict[str, Any], family: str) -> list[str]:
    data = ["timestamped_ohlcv_or_bar_data", "source_defined_setup_flags", "matched_baseline_observations"]
    if claim.get("instrument"):
        data.append(f"instrument:{claim['instrument']}")
    if claim.get("timeframe"):
        data.append(f"timeframe:{claim['timeframe']}")
    if family == "OPENING_RANGE":
        data.extend(["session_open_timestamp", "opening_range_high_low", "prior_day_range_or_swing_boundary"] )
    return data


def contrarian_inputs_for_hypothesis(contrarian: dict[str, Any] | None, *, status: str) -> list[str]:
    if not contrarian:
        return ["No contrarian theory available; hypothesis remains detail-limited."]
    inputs: list[str] = []
    for mode in contrarian.get("primary_failure_modes", []):
        inputs.append(f"failure_mode:{mode}")
    for condition in contrarian.get("fragility_conditions", [])[:4]:
        inputs.append(f"fragility:{condition}")
    if status != GENERATED_STATUS:
        inputs.append("contrarian_status_blocks_generated_hypothesis")
    return inputs or ["Contrarian theory supplied no usable falsification inputs."]


def falsification_criteria(
    claim: dict[str, Any],
    mechanism: dict[str, Any] | None,
    contrarian: dict[str, Any] | None,
    *,
    status: str,
) -> list[str]:
    if status == UNSUPPORTED_STATUS:
        return [
            "Fail hypothesis generation until the mechanism family is no longer UNKNOWN.",
            "Do not infer a supported mechanism from name similarity alone.",
        ]
    if status in {INSUFFICIENT_STATUS, REJECTED_STATUS}:
        return [
            "Mark the claim INSUFFICIENT_DETAIL if setup, observation window, or invalidation remains unspecified.",
            "Do not infer a replay design until the missing source rules are supplied.",
        ]
    tests = list(contrarian.get("required_falsification_tests", []) if contrarian else [])
    criteria = [
        "Fail if timestamp-available replay cannot identify the source-defined setup condition.",
        "Fail if the measured post-condition movement or state change does not exceed the declared baseline condition.",
        "Fail if the observed separation disappears across basic regime partitions.",
    ]
    criteria.extend(f"Contrarian check: {test}" for test in tests[:4])
    return criteria


def baseline_for_family(family: str) -> str:
    mapping = {
        "OPENING_RANGE": "Compare against baseline boundary touches during the same session without the source-defined confirmation pattern.",
        "BREAKOUT": "Compare against matched range-boundary touches without the source-defined breakout confirmation.",
        "MEAN_REVERSION": "Compare against matched level touches without the source-defined reversal confirmation.",
        "VWAP_OR_AVERAGE_RECLAIM": "Compare against matched VWAP or average touches without the source-defined reclaim confirmation.",
    }
    return mapping.get(family, "Compare against a neutral matched observation baseline defined before replay.")


def clean_sentence(value: str) -> str:
    return " ".join(value.strip().rstrip(".").split())[:260] or "source-defined condition"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Atlas V2 research hypotheses from structured claims.")
    parser.add_argument("--ledger-root", default=str(DEFAULT_LEDGER_ROOT))
    parser.add_argument("--claim-id", action="append", dest="claim_ids")
    parser.add_argument("--source-batch-id")
    parser.add_argument("--source-mechanism-registry-id")
    parser.add_argument("--run-id")
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    args = parser.parse_args(argv)
    result = AtlasV2ClaimToHypothesisGenerator(AtlasV2Ledger(Path(args.ledger_root))).generate(
        claim_ids=args.claim_ids,
        source_batch_id=args.source_batch_id,
        source_mechanism_registry_id=args.source_mechanism_registry_id,
        run_id=args.run_id,
        created_at=args.created_at,
    )
    json.dump(result.summary(), sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
