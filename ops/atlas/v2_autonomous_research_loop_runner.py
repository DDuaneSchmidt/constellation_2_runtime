from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ops.atlas.v2_cheap_experiment_generator import AtlasV2CheapExperimentGenerator
from ops.atlas.v2_cheap_experiment_executor import AtlasV2CheapExperimentExecutor
from ops.atlas.v2_claim_to_hypothesis_generator import AtlasV2ClaimToHypothesisGenerator
from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, PROHIBITED_AUTHORITY_FIELDS
from ops.atlas.v2_experiment_experience_writer import AtlasV2ExperimentExperienceWriter
from ops.atlas.v2_external_strategy_claim_extractor import DEFAULT_CREATED_AT, classify_mechanism_text, claim_fingerprint, short_hash
from ops.atlas.v2_label_integrity import AtlasV2LabelIntegrityAuditor
from ops.atlas.v2_learning_estimator import AtlasV2LearningValueEstimator

RUNNER_VERSION = "atlas_v2_autonomous_research_loop_runner_v1"
DEFAULT_LEDGER_ROOT = Path("/tmp/atlas_v2_autonomous_research_loop_runner_v1_ledgers")
DEFAULT_PERIOD = "2026-06-04"
ALLOWED_MODES = {"mock", "historical"}
DEFAULT_CLAIM_TEXT = (
    "Mark the first 15 minute opening range on NQ. Enter only when price breaks above the opening range high "
    "after a candle close. Use VWAP as a filter. Exit the observation at 2R or stop the observation below the range low."
)


@dataclass(frozen=True)
class AutonomousResearchLoopResult:
    run: dict[str, Any]
    summary_record: dict[str, Any]
    hypotheses: list[dict[str, Any]]
    specs: list[dict[str, Any]]
    results: list[dict[str, Any]]
    experience_events: list[dict[str, Any]]
    label_integrity_report: dict[str, Any] | None
    stopped_on_authority_violation: bool

    def summary(self) -> dict[str, Any]:
        return {
            "loop_run_id": self.run["loop_run_id"],
            "status": self.run["status"],
            "mode": self.run["mode"],
            "claims_seen": self.run["claims_seen"],
            "hypotheses_created": self.run["hypotheses_created"],
            "specs_created": self.run["specs_created"],
            "results_created": self.run["results_created"],
            "experience_events_created": self.run["experience_events_created"],
            "learning_estimates_created": self.run["learning_estimates_created"],
            "label_integrity_report_id": self.run["label_integrity_report_id"],
            "stopped_on_authority_violation": self.stopped_on_authority_violation,
        }


class AtlasV2AutonomousResearchLoopRunner:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def run(
        self,
        *,
        mode: str = "mock",
        max_loop_count: int = 10,
        claims: list[dict[str, Any]] | None = None,
        created_at: str = DEFAULT_CREATED_AT,
        period_start: str = DEFAULT_PERIOD,
        period_end: str = DEFAULT_PERIOD,
        loop_run_id: str | None = None,
    ) -> AutonomousResearchLoopResult:
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in ALLOWED_MODES:
            raise AtlasV2ValidationError("Autonomous Research Loop requires mock or historical mode")
        if max_loop_count < 0 or max_loop_count > 10:
            raise AtlasV2ValidationError("max_loop_count must be between 0 and 10")

        run_id = loop_run_id or f"arl-{short_hash(normalized_mode + ':' + str(max_loop_count) + ':' + created_at)}"
        try:
            selected_claims = self._prepare_claims(claims, max_loop_count=max_loop_count, created_at=created_at)
            if not selected_claims:
                return self._finalize(
                    loop_run_id=run_id,
                    created_at=created_at,
                    mode=normalized_mode,
                    max_loop_count=max_loop_count,
                    claims_seen=0,
                    hypotheses=[],
                    specs=[],
                    results=[],
                    experience_events=[],
                    estimator_counts={},
                    label_report=None,
                    stopped_on_authority_violation=False,
                    status="NO_CLAIMS",
                )

            hypothesis_result = AtlasV2ClaimToHypothesisGenerator(self.ledger).generate(
                claim_ids=[str(record["claim_id"]) for record in selected_claims],
                run_id=f"{run_id}-hypothesis-generation",
                created_at=created_at,
            )
            hypotheses = [record for record in hypothesis_result.hypotheses if str(record.get("status")) in {"GENERATED", "TESTABLE"}]
            spec_result = AtlasV2CheapExperimentGenerator(self.ledger).generate(
                hypotheses,
                generation_run_id=f"{run_id}-spec-generation",
                created_at=created_at,
            )
            observations = observations_from_specs(spec_result.specs, mode=normalized_mode)
            self._assert_no_authority_violation(list(observations.values()))
            execution_result = AtlasV2CheapExperimentExecutor(self.ledger).execute(
                spec_result.specs,
                observations=observations,
                execution_run_id=f"{run_id}-experiment-execution",
                data_mode="MOCK_HISTORICAL" if normalized_mode == "mock" else "HISTORICAL_READONLY",
                created_at=created_at,
            )
            hypotheses_by_id = {str(record["hypothesis_id"]): record for record in hypothesis_result.hypotheses}
            specs_by_id = {str(record["experiment_spec_id"]): record for record in spec_result.specs}
            experience_events = []
            for index, experiment_result in enumerate(execution_result.results, start=1):
                spec = specs_by_id[str(experiment_result["experiment_spec_id"])]
                hypothesis = hypotheses_by_id[str(experiment_result["hypothesis_id"])]
                writer_result = AtlasV2ExperimentExperienceWriter(self.ledger).write(
                    hypothesis=hypothesis,
                    experiment_spec=spec,
                    experiment_result=writer_input_from_executor_result(experiment_result, index=index),
                    created_at=created_at,
                    period_start=period_start,
                    period_end=period_end,
                )
                experience_events.append(writer_result.experience_event)
            stopped = False
            estimator_result = AtlasV2LearningValueEstimator(self.ledger).run_fixture_batch(
                report_id=f"{run_id}-learning-estimator-report",
                period_start=period_start,
                period_end=period_end,
                created_at=created_at,
                estimator_run_id=f"{run_id}-learning-estimator",
                force=True,
            )
            label_report = AtlasV2LabelIntegrityAuditor(self.ledger).build_report(
                dataset_name="atlas_v2_autonomous_research_loop_learning_labels",
                report_id=f"{run_id}-label-integrity-report",
                created_at=created_at,
            )
            estimator_counts = {
                "learning_estimates_created": len(estimator_result.estimates),
                "learning_evaluations_created": len(estimator_result.evaluations),
                "attention_signals_created": len(estimator_result.attention_signals),
            }

            return self._finalize(
                loop_run_id=run_id,
                created_at=created_at,
                mode=normalized_mode,
                max_loop_count=max_loop_count,
                claims_seen=len(selected_claims),
                hypotheses=hypothesis_result.hypotheses,
                specs=spec_result.specs,
                results=execution_result.results,
                experience_events=experience_events,
                estimator_counts=estimator_counts,
                label_report=label_report,
                stopped_on_authority_violation=stopped,
                status="STOPPED_AUTHORITY_VIOLATION" if stopped else "COMPLETED_SAFE_RESEARCH_LOOP",
            )
        except AtlasV2ValidationError as exc:
            if "prohibited authority" not in str(exc) and "authority" not in str(exc).lower():
                raise
            return self._finalize(
                loop_run_id=run_id,
                created_at=created_at,
                mode=normalized_mode,
                max_loop_count=max_loop_count,
                claims_seen=0,
                hypotheses=[],
                specs=[],
                results=[],
                experience_events=[],
                estimator_counts={},
                label_report=None,
                stopped_on_authority_violation=True,
                status="STOPPED_AUTHORITY_VIOLATION",
            )

    def _prepare_claims(self, claims: list[dict[str, Any]] | None, *, max_loop_count: int, created_at: str) -> list[dict[str, Any]]:
        if claims is None:
            claims = [default_claim_payload(created_at=created_at)]
        selected: list[dict[str, Any]] = []
        seen_fingerprints: dict[str, str] = {}
        for index, payload in enumerate(claims[:max_loop_count], start=1):
            self._assert_no_authority_violation([payload])
            claim_payload = normalize_claim_payload(payload, index=index, created_at=created_at)
            claim = self.ledger.create_record(
                "ExternalStrategyClaim",
                claim_payload,
                reason="autonomous research loop claim recorded from explicit mock/historical input",
                triggering_object=RUNNER_VERSION,
            )
            fingerprint = claim_fingerprint(claim)
            duplicate_of_claim_id = seen_fingerprints.get(fingerprint)
            if duplicate_of_claim_id:
                self.ledger.create_record(
                    "ExternalStrategyDeduplicationResult",
                    {
                        "dedupe_id": f"arl-dedupe-{short_hash(str(claim['claim_id']) + fingerprint)}",
                        "claim_id": claim["claim_id"],
                        "claim_fingerprint": fingerprint,
                        "mechanism_fingerprint": fingerprint,
                        "is_duplicate": True,
                        "duplicate_of_claim_id": duplicate_of_claim_id,
                        "duplicate_reason": "AUTONOMOUS_LOOP_CLAIM_FINGERPRINT_MATCH",
                        "status": "DUPLICATE_CLAIM",
                        "created_at": created_at,
                    },
                    reason="autonomous research loop duplicate claim compressed before mechanism generation",
                    triggering_object=f"ExternalStrategyClaim:{claim['claim_id']}",
                )
                continue
            seen_fingerprints[fingerprint] = str(claim["claim_id"])
            if str(claim.get("extraction_status") or "") != "EXTRACTED":
                selected.append(claim)
                continue
            mechanism = self.ledger.create_record(
                "ExternalStrategyMechanism",
                mechanism_payload_for_claim(claim, created_at=created_at),
                reason="autonomous research loop mechanism classified from supplied claim only",
                triggering_object=f"ExternalStrategyClaim:{claim['claim_id']}",
            )
            self.ledger.create_record(
                "ExternalStrategyContrarianTheory",
                contrarian_payload_for_claim(claim, mechanism, created_at=created_at),
                reason="autonomous research loop contrarian theory recorded before hypothesis generation",
                triggering_object=f"ExternalStrategyMechanism:{mechanism['mechanism_id']}",
            )
            selected.append(claim)
        return selected

    def _assert_no_authority_violation(self, records: list[dict[str, Any]]) -> None:
        for record in records:
            for field in PROHIBITED_AUTHORITY_FIELDS:
                if record.get(field) not in (None, "", False, [], {}):
                    raise AtlasV2ValidationError(f"autonomous research loop stopped on prohibited authority field: {field}")

    def _finalize(
        self,
        *,
        loop_run_id: str,
        created_at: str,
        mode: str,
        max_loop_count: int,
        claims_seen: int,
        hypotheses: list[dict[str, Any]],
        specs: list[dict[str, Any]],
        results: list[dict[str, Any]],
        experience_events: list[dict[str, Any]],
        estimator_counts: dict[str, int],
        label_report: dict[str, Any] | None,
        stopped_on_authority_violation: bool,
        status: str,
    ) -> AutonomousResearchLoopResult:
        loop_status = "STOPPED_AUTHORITY_VIOLATION" if stopped_on_authority_violation else status
        summary_status = "STOPPED_AUTHORITY_VIOLATION" if stopped_on_authority_violation else (
            "NO_CLAIMS" if status == "NO_CLAIMS" else "SAFE_RESEARCH_LOOP_COMPLETED"
        )
        run = self.ledger.create_record(
            "AutonomousResearchLoopRun",
            {
                "loop_run_id": loop_run_id,
                "created_at": created_at,
                "runner_version": RUNNER_VERSION,
                "mode": mode,
                "max_loop_count": max_loop_count,
                "claims_seen": claims_seen,
                "hypotheses_created": len(hypotheses),
                "specs_created": len(specs),
                "results_created": len(results),
                "experience_events_created": len(experience_events),
                "learning_estimates_created": int(estimator_counts.get("learning_estimates_created", 0)),
                "label_integrity_report_id": str(label_report.get("report_id")) if label_report else "NOT_RUN",
                "authority_boundary_acknowledged": True,
                "stopped_on_authority_violation": stopped_on_authority_violation,
                "status": loop_status,
            },
            reason="safe autonomous research loop run summarized without authority expansion",
            triggering_object=RUNNER_VERSION,
        )
        summary = self.ledger.create_record(
            "AutonomousResearchLoopSummary",
            {
                "summary_id": f"{loop_run_id}-summary",
                "loop_run_id": loop_run_id,
                "created_at": created_at,
                "mode": mode,
                "claims_seen": claims_seen,
                "mechanisms_seen": len(self.ledger.records("ExternalStrategyMechanism")),
                "hypotheses_created": len(hypotheses),
                "specs_created": len(specs),
                "results_created": len(results),
                "experience_events_created": len(experience_events),
                "learning_estimates_created": int(estimator_counts.get("learning_estimates_created", 0)),
                "learning_evaluations_created": int(estimator_counts.get("learning_evaluations_created", 0)),
                "attention_signals_created": int(estimator_counts.get("attention_signals_created", 0)),
                "label_integrity_report_id": str(label_report.get("report_id")) if label_report else "NOT_RUN",
                "label_independence_status": str(label_report.get("label_independence_status")) if label_report else "NOT_RUN",
                "authority_boundary_acknowledged": True,
                "forbidden_authority_terms_absent": not stopped_on_authority_violation,
                "status": summary_status,
            },
            reason="safe autonomous research loop summary recorded as read-only evidence",
            triggering_object=f"AutonomousResearchLoopRun:{loop_run_id}",
        )
        return AutonomousResearchLoopResult(
            run=run,
            summary_record=summary,
            hypotheses=hypotheses,
            specs=specs,
            results=results,
            experience_events=experience_events,
            label_integrity_report=label_report,
            stopped_on_authority_violation=stopped_on_authority_violation,
        )


def default_claim_payload(*, created_at: str) -> dict[str, Any]:
    return {
        "claim_id": f"arl-claim-{short_hash(DEFAULT_CLAIM_TEXT)}",
        "source_id": "atlas-v2-autonomous-research-loop-fixture-source",
        "claim_text": "Opening range break with VWAP filter can be replayed as a bounded observation claim.",
        "entry_rule": "price breaks above the opening range high after a candle close",
        "exit_rule": "end the observation at 2R or the bounded observation window",
        "risk_rule": "stop the observation below the opening range low",
        "filter_rule": "VWAP filter is present",
        "timeframe": "first 15 minute opening range",
        "instrument": "NQ historical fixture observations",
        "confidence": 0.72,
        "extraction_status": "EXTRACTED",
        "created_at": created_at,
    }


def normalize_claim_payload(payload: dict[str, Any], *, index: int, created_at: str) -> dict[str, Any]:
    if not payload:
        return default_claim_payload(created_at=created_at)
    merged = {**payload}
    if not str(merged.get("claim_id") or "").strip():
        merged["claim_id"] = f"arl-claim-{index:04d}-{short_hash(json.dumps(payload, sort_keys=True))}"
    if not str(merged.get("source_id") or "").strip():
        merged["source_id"] = "atlas-v2-autonomous-research-loop-manual-source"
    if not str(merged.get("claim_text") or "").strip():
        merged["claim_text"] = "Manual claim did not include enough detail for autonomous research conversion."
    if merged.get("confidence") is None:
        merged["confidence"] = 0.25
    if not str(merged.get("extraction_status") or "").strip():
        has_rule = bool(merged.get("entry_rule") and (merged.get("exit_rule") or merged.get("risk_rule") or merged.get("filter_rule")))
        merged["extraction_status"] = "EXTRACTED" if has_rule else "INSUFFICIENT_RULE_DETAIL"
    merged["created_at"] = str(merged.get("created_at") or created_at)
    return merged


def mechanism_payload_for_claim(claim: dict[str, Any], *, created_at: str) -> dict[str, Any]:
    text = " ".join(str(claim.get(field) or "") for field in ("claim_text", "entry_rule", "exit_rule", "filter_rule", "risk_rule", "timeframe", "instrument"))
    family, confidence, description = classify_mechanism_text(text)
    return {
        "mechanism_id": f"arl-mechanism-{short_hash(str(claim['claim_id']) + ':' + family)}",
        "claim_id": claim["claim_id"],
        "mechanism_family": family,
        "mechanism_description": description,
        "classification_confidence": confidence,
        "created_at": created_at,
    }


def contrarian_payload_for_claim(claim: dict[str, Any], mechanism: dict[str, Any], *, created_at: str) -> dict[str, Any]:
    return {
        "contrarian_id": f"arl-contrarian-{short_hash(str(claim['claim_id']) + str(mechanism['mechanism_id']))}",
        "claim_id": claim["claim_id"],
        "mechanism_id": mechanism["mechanism_id"],
        "primary_failure_modes": ["OVERFITTING", "REGIME_DEPENDENCE", "LOW_SAMPLE_SIZE"],
        "opposite_hypothesis": f"{mechanism['mechanism_family']} observations do not separate from matched baseline observations.",
        "fragility_conditions": ["small historical sample", "session regime change", "baseline mismatch"],
        "required_falsification_tests": ["matched baseline replay", "sample sufficiency check", "lookahead exclusion check"],
        "prior_failure_matches": [],
        "contrarian_confidence": 0.68,
        "status": "GENERATED",
        "created_at": created_at,
    }


def observations_from_specs(specs: list[dict[str, Any]], *, mode: str) -> dict[str, dict[str, Any]]:
    observations: dict[str, dict[str, Any]] = {}
    for index, spec in enumerate(specs, start=1):
        matched = index % 5 != 0
        baseline_value = round(0.45 + (index % 3) * 0.02, 4)
        experiment_value = round(baseline_value + (0.08 if matched else -0.06), 4)
        observations[str(spec["experiment_spec_id"])] = {
            "baseline_observation_count": 40 + index,
            "experiment_observation_count": 35 + index,
            "baseline_metric_value": baseline_value,
            "experiment_metric_value": experiment_value,
            "outcome": "PASS" if matched else "FAIL",
            "falsification_result": "NOT_FALSIFIED" if matched else "FALSIFIED",
            "baseline_comparison": f"{mode} fixture comparison for {spec['experiment_spec_id']}",
            "result_summary": "Mock/historical cheap experiment result recorded as learning evidence only.",
        }
    return observations


def writer_input_from_executor_result(result: dict[str, Any], *, index: int) -> dict[str, Any]:
    status_map = {"PASS": "PASSED", "FAIL": "FAILED", "INCONCLUSIVE": "INCONCLUSIVE"}
    return {
        **result,
        "experiment_result_id": result["result_id"],
        "outcome_status": status_map[str(result["outcome"])],
        "observed_outcome": result["result_summary"],
        "evidence_reference": f"atlas/v2/mock_historical_experiment_result/{result['result_id']}",
        "confidence": 0.62,
        "outcome_confidence": 0.74 if result["outcome"] == "PASS" else 0.58,
        "expected_learning_value": round(0.2 + (index % 4) * 0.03, 4),
        "actual_learning_value": round(0.24 + (index % 5) * 0.025, 4),
        "importance_score": round(0.18 + (index % 5) * 0.03, 4),
        "attention_cost_estimate": 0.01,
        "experience_quality_score": 0.68 if result["outcome"] == "PASS" else 0.58,
    }


def fixture_from_spec(spec: dict[str, Any], *, index: int, mode: str) -> dict[str, Any]:
    matched = index % 5 != 0
    return {
        "fixture_id": f"{spec['experiment_spec_id']}-fixture",
        "experiment_id": f"{spec['experiment_spec_id']}-result",
        "tier": "TIER_1_SANITY" if spec["tier"] == "TIER_2_LIGHTWEIGHT_VALIDATION" else spec["tier"],
        "prediction_statement": f"Spec {spec['experiment_spec_id']} will produce bounded mock historical learning evidence.",
        "expected_learning_value": round(0.2 + (index % 4) * 0.03, 4),
        "attention_cost_estimate": 0.01,
        "data_scope": f"{mode} Atlas V2 fixture observations only; no live market data",
        "method_summary": "Read-only replay-style comparison of spec condition against baseline fixture observations.",
        "expected_outcome": "bounded learning evidence recorded",
        "observed_outcome": "bounded learning evidence recorded" if matched else "bounded learning evidence contradicted expectation",
        "outcome_summary": "Mock historical result recorded as learning evidence only, not validation.",
        "expected_result": True,
        "actual_result": matched,
        "matched_expected_outcome": matched,
        "confidence": 0.62,
        "outcome_confidence": 0.75 if matched else 0.58,
        "actual_learning_value": round(0.24 + (index % 5) * 0.025, 4),
        "importance_score": round(0.18 + (index % 5) * 0.03, 4),
        "regret_score": 0.05 if matched else 0.32,
        "lesson": "Spec-level mock historical cycle produced bounded learning evidence without authority expansion.",
        "experience_quality_score": 0.68 if matched else 0.58,
    }


def _read_claims(path: str | None) -> list[dict[str, Any]] | None:
    if not path:
        return None
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return [data]
    if not isinstance(data, list):
        raise AtlasV2ValidationError("claim input must be an object or array")
    return data


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Atlas V2 Autonomous Research Loop V1 in mock/historical mode.")
    parser.add_argument("--ledger-root", type=Path, default=DEFAULT_LEDGER_ROOT)
    parser.add_argument("--mode", default="mock", choices=sorted(ALLOWED_MODES))
    parser.add_argument("--max-loop-count", type=int, default=10)
    parser.add_argument("--claims-file")
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--period-start", default=DEFAULT_PERIOD)
    parser.add_argument("--period-end", default=DEFAULT_PERIOD)
    parser.add_argument("--loop-run-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = AtlasV2AutonomousResearchLoopRunner(AtlasV2Ledger(args.ledger_root)).run(
        mode=args.mode,
        max_loop_count=args.max_loop_count,
        claims=_read_claims(args.claims_file),
        created_at=args.created_at,
        period_start=args.period_start,
        period_end=args.period_end,
        loop_run_id=args.loop_run_id,
    )
    print(json.dumps(result.summary(), indent=2, sort_keys=True))
    return 2 if result.stopped_on_authority_violation else 0


if __name__ == "__main__":
    raise SystemExit(main())
