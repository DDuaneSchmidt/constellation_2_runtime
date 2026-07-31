from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_core import (
    AtlasV2Ledger,
    AtlasV2ValidationError,
    CHEAP_EXPERIMENT_SPEC_TIERS,
    FORBIDDEN_AUTHORITY_KEYS,
)
from ops.atlas.v2_external_strategy_claim_extractor import DEFAULT_CREATED_AT, short_hash

GENERATOR_VERSION = "atlas_v2_cheap_experiment_generator_v1"
DEFAULT_LEDGER_ROOT = Path("/tmp/atlas_v2_cheap_experiment_generator_v1_ledgers")
ALLOWED_TIERS = ("TIER_0_DEDUPE", "TIER_1_SANITY", "TIER_2_LIGHTWEIGHT_VALIDATION")
FORBIDDEN_TIERS = ("TIER_3_ROBUST_VALIDATION", "TIER_4_MATURITY_TRACKING")


@dataclass(frozen=True)
class CheapExperimentGeneratorResult:
    run: dict[str, Any]
    specs: list[dict[str, Any]]
    data_requirements: list[dict[str, Any]]
    evaluation_plans: list[dict[str, Any]]

    def summary(self) -> dict[str, Any]:
        return {
            "generation_run_id": self.run["generation_run_id"],
            "generator_version": self.run["generator_version"],
            "input_hypothesis_count": self.run["input_hypothesis_count"],
            "generated_spec_count": self.run["generated_spec_count"],
            "tiers": [record["tier"] for record in self.specs],
            "status": self.run["status"],
        }


class AtlasV2CheapExperimentGenerator:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def generate(
        self,
        hypotheses: list[dict[str, Any]] | None = None,
        *,
        generation_run_id: str | None = None,
        created_at: str = DEFAULT_CREATED_AT,
    ) -> CheapExperimentGeneratorResult:
        inputs = hypotheses if hypotheses is not None else self.ledger.records("ResearchHypothesis")
        normalized = [normalize_research_hypothesis(item) for item in inputs]
        specs: list[dict[str, Any]] = []
        data_requirements: list[dict[str, Any]] = []
        evaluation_plans: list[dict[str, Any]] = []

        for index, hypothesis in enumerate(normalized, start=1):
            spec_payload = build_spec_payload(hypothesis, index=index, created_at=created_at)
            spec = self.ledger.create_record(
                "CheapExperimentSpec",
                spec_payload,
                reason="cheap experiment spec generated from ResearchHypothesis without execution",
                triggering_object=f"ResearchHypothesis:{spec_payload['hypothesis_id']}",
            )
            specs.append(spec)

            for requirement_payload in build_data_requirements(spec, hypothesis, created_at=created_at):
                data_requirement = self.ledger.create_record(
                    "ExperimentDataRequirement",
                    requirement_payload,
                    reason="cheap experiment data requirement recorded as spec metadata only",
                    triggering_object=f"CheapExperimentSpec:{spec['experiment_spec_id']}",
                )
                data_requirements.append(data_requirement)

            evaluation_plan = self.ledger.create_record(
                "ExperimentEvaluationPlan",
                build_evaluation_plan(spec, hypothesis, created_at=created_at),
                reason="cheap experiment evaluation plan recorded without result or execution authority",
                triggering_object=f"CheapExperimentSpec:{spec['experiment_spec_id']}",
            )
            evaluation_plans.append(evaluation_plan)

        run_id = generation_run_id or f"xgr-{short_hash(GENERATOR_VERSION + ':' + ':'.join(spec['experiment_spec_id'] for spec in specs))}"
        run = self.ledger.create_record(
            "ExperimentGenerationRun",
            {
                "generation_run_id": run_id,
                "created_at": created_at,
                "generator_version": GENERATOR_VERSION,
                "input_hypothesis_count": len(normalized),
                "generated_spec_count": len(specs),
                "experiment_spec_ids": [record["experiment_spec_id"] for record in specs],
                "data_requirement_ids": [record["data_requirement_id"] for record in data_requirements],
                "evaluation_plan_ids": [record["evaluation_plan_id"] for record in evaluation_plans],
                "allowed_tiers": list(ALLOWED_TIERS),
                "forbidden_tiers": list(FORBIDDEN_TIERS),
                "authority_boundary_acknowledged": True,
                "status": "COMPLETED_SPEC_GENERATION",
            },
            reason="cheap experiment generation run completed with specs only",
            triggering_object=GENERATOR_VERSION,
        )
        return CheapExperimentGeneratorResult(run=run, specs=specs, data_requirements=data_requirements, evaluation_plans=evaluation_plans)

    def generate_from_latest_hypotheses(
        self,
        *,
        generation_run_id: str | None = None,
        created_at: str = DEFAULT_CREATED_AT,
    ) -> CheapExperimentGeneratorResult:
        return self.generate(None, generation_run_id=generation_run_id, created_at=created_at)


def normalize_research_hypothesis(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise AtlasV2ValidationError("Cheap Experiment Generator requires ResearchHypothesis objects")
    assert_no_forbidden_authority(payload, "ResearchHypothesis input")
    hypothesis_id = str(payload.get("hypothesis_id") or payload.get("id") or "").strip()
    if not hypothesis_id:
        text = str(payload.get("hypothesis_text") or payload.get("hypothesis") or payload.get("claim_text") or "")
        hypothesis_id = f"rh-{short_hash(text or json.dumps(payload, sort_keys=True))}"
    mechanism_id = str(payload.get("mechanism_id") or "mechanism-unassigned")
    tier = str(payload.get("tier") or payload.get("recommended_tier") or inferred_tier(payload))
    if tier in FORBIDDEN_TIERS or tier not in CHEAP_EXPERIMENT_SPEC_TIERS:
        raise AtlasV2ValidationError(f"Cheap Experiment Generator V1 cannot generate {tier} specs")
    required_data = payload.get("required_data")
    if not isinstance(required_data, list) or not required_data:
        required_data = default_required_data(payload)
    return {
        **payload,
        "hypothesis_id": hypothesis_id,
        "mechanism_id": mechanism_id,
        "tier": tier,
        "entry_condition": text_field(payload, "entry_condition", "testable_condition", default="Observe the supplied hypothesis condition in existing records."),
        "exit_condition": text_field(payload, "exit_condition", default="End the cheap check after the bounded observation window or required sample count."),
        "stop_condition": text_field(payload, "stop_condition", default="Stop if required data is missing or the hypothesis cannot be evaluated from existing records."),
        "target_condition": text_field(payload, "target_condition", "expected_direction", default="Observe whether the hypothesis condition separates from the baseline comparison."),
        "baseline_condition": text_field(payload, "baseline_condition", "baseline_comparison", default="Compare against records where the hypothesis condition is absent."),
        "required_data": [str(item) for item in required_data if str(item).strip()],
        "evaluation_metric": text_field(payload, "evaluation_metric", default="difference_vs_baseline_rate"),
        "falsification_threshold": text_field(payload, "falsification_threshold", "falsification_criteria", default="Falsify if the condition does not outperform the baseline metric or if data coverage is insufficient."),
    }


def build_spec_payload(hypothesis: dict[str, Any], *, index: int, created_at: str) -> dict[str, Any]:
    material = ":".join([hypothesis["hypothesis_id"], hypothesis["mechanism_id"], hypothesis["tier"], str(index)])
    return {
        "experiment_spec_id": str(hypothesis.get("experiment_spec_id") or f"xes-{short_hash(material)}"),
        "created_at": created_at,
        "hypothesis_id": hypothesis["hypothesis_id"],
        "mechanism_id": hypothesis["mechanism_id"],
        "tier": hypothesis["tier"],
        "entry_condition": hypothesis["entry_condition"],
        "exit_condition": hypothesis["exit_condition"],
        "stop_condition": hypothesis["stop_condition"],
        "target_condition": hypothesis["target_condition"],
        "baseline_condition": hypothesis["baseline_condition"],
        "required_data": hypothesis["required_data"],
        "evaluation_metric": hypothesis["evaluation_metric"],
        "falsification_threshold": hypothesis["falsification_threshold"],
        "authority_boundary_acknowledged": True,
        "status": "GENERATED_SPEC",
    }


def build_data_requirements(spec: dict[str, Any], hypothesis: dict[str, Any], *, created_at: str) -> list[dict[str, Any]]:
    requirements: list[dict[str, Any]] = []
    for index, item in enumerate(spec["required_data"], start=1):
        requirements.append({
            "data_requirement_id": f"xdr-{short_hash(spec['experiment_spec_id'] + ':' + str(index) + ':' + item)}",
            "experiment_spec_id": spec["experiment_spec_id"],
            "hypothesis_id": spec["hypothesis_id"],
            "created_at": created_at,
            "data_name": item,
            "data_purpose": "Support baseline comparison and falsification check; no result is computed by this generator.",
            "minimum_observation_count": int(hypothesis.get("minimum_observation_count") or 20),
            "source_mode": str(hypothesis.get("source_mode") or "EXISTING_LEDGER"),
            "authority_boundary_acknowledged": True,
            "status": "REQUIRED",
        })
    return requirements


def build_evaluation_plan(spec: dict[str, Any], hypothesis: dict[str, Any], *, created_at: str) -> dict[str, Any]:
    return {
        "evaluation_plan_id": str(
            hypothesis.get("evaluation_plan_id") or f"xep-{short_hash(spec['experiment_spec_id'] + ':evaluation')}"
        ),
        "experiment_spec_id": spec["experiment_spec_id"],
        "hypothesis_id": spec["hypothesis_id"],
        "created_at": created_at,
        "baseline_condition": spec["baseline_condition"],
        "evaluation_metric": spec["evaluation_metric"],
        "falsification_threshold": spec["falsification_threshold"],
        "comparison_method": str(hypothesis.get("comparison_method") or "Compare hypothesis-condition observations against baseline-condition observations after later execution."),
        "result_recording_allowed": False,
        "execution_allowed": False,
        "authority_boundary_acknowledged": True,
        "status": "PLANNED_SPEC_ONLY",
    }


def inferred_tier(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "").upper()
    if status in {"INSUFFICIENT_DETAIL", "DUPLICATE_CLAIM", "DEDUPE_ONLY"}:
        return "TIER_0_DEDUPE"
    if payload.get("tier") == "TIER_2_LIGHTWEIGHT_VALIDATION":
        return "TIER_2_LIGHTWEIGHT_VALIDATION"
    return "TIER_1_SANITY"


def default_required_data(payload: dict[str, Any]) -> list[str]:
    fields = ["hypothesis_condition_observations", "baseline_condition_observations"]
    mechanism_id = str(payload.get("mechanism_id") or "")
    if mechanism_id:
        fields.append(f"mechanism_record:{mechanism_id}")
    return fields


def text_field(payload: dict[str, Any], *names: str, default: str) -> str:
    for name in names:
        value = payload.get(name)
        if isinstance(value, list) and value:
            return "; ".join(str(item) for item in value)
        if str(value or "").strip():
            return str(value).strip()
    return default


def assert_no_forbidden_authority(payload: dict[str, Any], label: str) -> None:
    for key, value in payload.items():
        if str(key).lower() in FORBIDDEN_AUTHORITY_KEYS and value not in (None, "", False, [], {}):
            raise AtlasV2ValidationError(f"{label} cannot set prohibited authority field: {key}")


def _read_hypotheses(args: argparse.Namespace) -> list[dict[str, Any]] | None:
    if args.input_file:
        data = json.loads(Path(args.input_file).read_text(encoding="utf-8"))
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        if not raw:
            return None
        data = json.loads(raw)
    else:
        return None
    if isinstance(data, dict) and data.get("object_type") == "ResearchHypothesis":
        return [data]
    if not isinstance(data, list):
        raise AtlasV2ValidationError("Cheap Experiment Generator input must be a ResearchHypothesis object or array")
    return data


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atlas V2 Cheap Experiment Generator V1")
    parser.add_argument("--input-file")
    parser.add_argument("--ledger-root", default=str(DEFAULT_LEDGER_ROOT))
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--generation-run-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    ledger = AtlasV2Ledger(Path(args.ledger_root))
    result = AtlasV2CheapExperimentGenerator(ledger).generate(
        _read_hypotheses(args),
        generation_run_id=args.generation_run_id,
        created_at=args.created_at,
    )
    print(json.dumps(result.summary(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
