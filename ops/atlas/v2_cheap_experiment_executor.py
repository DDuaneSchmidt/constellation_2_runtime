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
    CHEAP_EXPERIMENT_EXECUTOR_DATA_MODES,
    FORBIDDEN_AUTHORITY_KEYS,
)
from ops.atlas.v2_external_strategy_claim_extractor import DEFAULT_CREATED_AT, short_hash

EXECUTOR_VERSION = "atlas_v2_cheap_experiment_executor_v1"
DEFAULT_LEDGER_ROOT = Path("/tmp/atlas_v2_cheap_experiment_executor_v1_ledgers")
ALLOWED_DATA_MODES = ("FIXTURE", "MOCK_HISTORICAL", "HISTORICAL_READONLY")
ALLOWED_REQUIREMENT_SOURCE_MODES = {"EXISTING_LEDGER", "EXISTING_HISTORICAL_FIXTURE"}


@dataclass(frozen=True)
class CheapExperimentExecutionResult:
    run: dict[str, Any]
    results: list[dict[str, Any]]
    outcome_summary: dict[str, Any]

    def summary(self) -> dict[str, Any]:
        return {
            "execution_run_id": self.run["execution_run_id"],
            "executor_version": self.run["executor_version"],
            "input_spec_count": self.run["input_spec_count"],
            "result_count": self.run["result_count"],
            "outcome_counts": {
                "pass": self.outcome_summary["pass_count"],
                "fail": self.outcome_summary["fail_count"],
                "inconclusive": self.outcome_summary["inconclusive_count"],
            },
            "status": self.run["status"],
        }


class AtlasV2CheapExperimentExecutor:
    def __init__(self, ledger: AtlasV2Ledger) -> None:
        self.ledger = ledger

    def execute(
        self,
        specs: list[dict[str, Any]] | None = None,
        *,
        observations: dict[str, dict[str, Any]] | None = None,
        execution_run_id: str | None = None,
        data_mode: str = "MOCK_HISTORICAL",
        created_at: str = DEFAULT_CREATED_AT,
    ) -> CheapExperimentExecutionResult:
        mode = normalize_data_mode(data_mode)
        input_specs = specs if specs is not None else self.ledger.records("CheapExperimentSpec")
        normalized_specs = [self._normalize_spec(spec) for spec in input_specs]
        observation_map = observations or {}
        run_id = execution_run_id or f"xer-{short_hash(EXECUTOR_VERSION + ':' + ':'.join(spec['experiment_spec_id'] for spec in normalized_specs))}"

        results: list[dict[str, Any]] = []
        for index, spec in enumerate(normalized_specs, start=1):
            self._assert_readonly_data_requirements(spec)
            payload = build_result_payload(
                spec,
                observation_map.get(spec["experiment_spec_id"], {}),
                execution_run_id=run_id,
                data_mode=mode,
                index=index,
                created_at=created_at,
            )
            result = self.ledger.create_record(
                "ExperimentResult",
                payload,
                reason="cheap experiment spec evaluated from fixture/mock/historical-readonly data",
                triggering_object=f"CheapExperimentSpec:{spec['experiment_spec_id']}",
            )
            results.append(result)

        summary_payload = build_outcome_summary_payload(results, execution_run_id=run_id, created_at=created_at)
        outcome_summary = self.ledger.create_record(
            "ExperimentOutcomeSummary",
            summary_payload,
            reason="cheap experiment executor summarized read-only results",
            triggering_object=f"ExperimentExecutionRun:{run_id}",
        )
        run = self.ledger.create_record(
            "ExperimentExecutionRun",
            {
                "execution_run_id": run_id,
                "created_at": created_at,
                "executor_version": EXECUTOR_VERSION,
                "input_spec_count": len(normalized_specs),
                "result_count": len(results),
                "experiment_spec_ids": [spec["experiment_spec_id"] for spec in normalized_specs],
                "result_ids": [result["result_id"] for result in results],
                "outcome_summary_id": outcome_summary["summary_id"],
                "allowed_data_modes": list(ALLOWED_DATA_MODES),
                "forbidden_authority_acknowledged": True,
                "status": "COMPLETED_READ_ONLY_EXECUTION",
            },
            reason="cheap experiment executor completed without trading or validation authority",
            triggering_object=EXECUTOR_VERSION,
        )
        return CheapExperimentExecutionResult(run=run, results=results, outcome_summary=outcome_summary)

    def execute_latest_specs(
        self,
        *,
        observations: dict[str, dict[str, Any]] | None = None,
        execution_run_id: str | None = None,
        data_mode: str = "MOCK_HISTORICAL",
        created_at: str = DEFAULT_CREATED_AT,
    ) -> CheapExperimentExecutionResult:
        return self.execute(
            None,
            observations=observations,
            execution_run_id=execution_run_id,
            data_mode=data_mode,
            created_at=created_at,
        )

    def _normalize_spec(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict) or payload.get("object_type") != "CheapExperimentSpec":
            raise AtlasV2ValidationError("Cheap Experiment Executor requires CheapExperimentSpec objects")
        assert_no_forbidden_authority(payload, "CheapExperimentSpec input")
        return payload

    def _assert_readonly_data_requirements(self, spec: dict[str, Any]) -> None:
        for requirement in self.ledger.records("ExperimentDataRequirement"):
            if requirement.get("experiment_spec_id") != spec["experiment_spec_id"]:
                continue
            source_mode = str(requirement.get("source_mode") or "")
            if source_mode not in ALLOWED_REQUIREMENT_SOURCE_MODES:
                raise AtlasV2ValidationError(
                    f"Cheap Experiment Executor cannot use non-read-only data requirement source_mode: {source_mode}"
                )
            assert_no_forbidden_authority(requirement, "ExperimentDataRequirement input")


def build_result_payload(
    spec: dict[str, Any],
    observation: dict[str, Any],
    *,
    execution_run_id: str,
    data_mode: str,
    index: int,
    created_at: str,
) -> dict[str, Any]:
    assert_no_forbidden_authority(observation, "ExperimentResult observation input")
    baseline_count = non_negative_int(observation.get("baseline_observation_count"), default=0)
    experiment_count = non_negative_int(observation.get("experiment_observation_count"), default=0)
    baseline_value = number(observation.get("baseline_metric_value"), default=0.0)
    experiment_value = number(observation.get("experiment_metric_value"), default=0.0)
    outcome = derive_outcome(
        observation.get("outcome"),
        baseline_count=baseline_count,
        experiment_count=experiment_count,
        baseline_value=baseline_value,
        experiment_value=experiment_value,
    )
    falsification_result = derive_falsification_result(observation.get("falsification_result"), outcome=outcome)
    delta = experiment_value - baseline_value
    baseline_comparison = str(
        observation.get("baseline_comparison")
        or f"{spec['evaluation_metric']}: experiment={experiment_value:.6g}, baseline={baseline_value:.6g}, delta={delta:.6g}"
    )
    return {
        "result_id": str(observation.get("result_id") or f"xrs-{short_hash(execution_run_id + ':' + spec['experiment_spec_id'] + ':' + str(index))}"),
        "execution_run_id": execution_run_id,
        "experiment_spec_id": spec["experiment_spec_id"],
        "hypothesis_id": spec["hypothesis_id"],
        "mechanism_id": spec["mechanism_id"],
        "tier": spec["tier"],
        "created_at": created_at,
        "data_mode": data_mode,
        "baseline_condition": spec["baseline_condition"],
        "baseline_observation_count": baseline_count,
        "experiment_observation_count": experiment_count,
        "baseline_metric_value": baseline_value,
        "experiment_metric_value": experiment_value,
        "baseline_comparison": baseline_comparison,
        "evaluation_metric": spec["evaluation_metric"],
        "falsification_threshold": spec["falsification_threshold"],
        "falsification_result": falsification_result,
        "outcome": outcome,
        "result_summary": str(
            observation.get("result_summary")
            or f"Read-only cheap experiment result recorded as {outcome}; falsification={falsification_result}."
        ),
        "authority_boundary_acknowledged": True,
        "status": "RECORDED_READ_ONLY_RESULT",
    }


def build_outcome_summary_payload(results: list[dict[str, Any]], *, execution_run_id: str, created_at: str) -> dict[str, Any]:
    return {
        "summary_id": f"xos-{short_hash(execution_run_id + ':outcome-summary')}",
        "execution_run_id": execution_run_id,
        "created_at": created_at,
        "result_count": len(results),
        "pass_count": sum(1 for result in results if result["outcome"] == "PASS"),
        "fail_count": sum(1 for result in results if result["outcome"] == "FAIL"),
        "inconclusive_count": sum(1 for result in results if result["outcome"] == "INCONCLUSIVE"),
        "falsified_count": sum(1 for result in results if result["falsification_result"] == "FALSIFIED"),
        "not_falsified_count": sum(1 for result in results if result["falsification_result"] == "NOT_FALSIFIED"),
        "baseline_comparison_recorded_count": sum(1 for result in results if bool(result.get("baseline_comparison"))),
        "authority_boundary_acknowledged": True,
        "status": "SUMMARIZED_READ_ONLY_RESULTS",
    }


def normalize_data_mode(data_mode: str) -> str:
    mode = str(data_mode or "").upper()
    if mode not in CHEAP_EXPERIMENT_EXECUTOR_DATA_MODES:
        raise AtlasV2ValidationError(f"Cheap Experiment Executor data_mode must be fixture/mock/historical-readonly: {data_mode}")
    return mode


def derive_outcome(
    supplied: Any,
    *,
    baseline_count: int,
    experiment_count: int,
    baseline_value: float,
    experiment_value: float,
) -> str:
    normalized = str(supplied or "").upper()
    if normalized in {"PASS", "FAIL", "INCONCLUSIVE"}:
        return normalized
    if baseline_count <= 0 or experiment_count <= 0:
        return "INCONCLUSIVE"
    if experiment_value > baseline_value:
        return "PASS"
    if experiment_value < baseline_value:
        return "FAIL"
    return "INCONCLUSIVE"


def derive_falsification_result(supplied: Any, *, outcome: str) -> str:
    normalized = str(supplied or "").upper()
    if normalized in {"FALSIFIED", "NOT_FALSIFIED", "INCONCLUSIVE"}:
        return normalized
    if outcome == "PASS":
        return "NOT_FALSIFIED"
    if outcome == "FAIL":
        return "FALSIFIED"
    return "INCONCLUSIVE"


def non_negative_int(value: Any, *, default: int) -> int:
    if value is None or value == "":
        return default
    if not isinstance(value, int) or value < 0:
        raise AtlasV2ValidationError("Cheap Experiment Executor observation counts must be non-negative integers")
    return value


def number(value: Any, *, default: float) -> float:
    if value is None or value == "":
        return default
    if not isinstance(value, (int, float)):
        raise AtlasV2ValidationError("Cheap Experiment Executor metric values must be numbers")
    return float(value)


def assert_no_forbidden_authority(payload: dict[str, Any], label: str) -> None:
    for key, value in payload.items():
        if str(key).lower() in FORBIDDEN_AUTHORITY_KEYS and value not in (None, "", False, [], {}):
            raise AtlasV2ValidationError(f"{label} cannot set prohibited authority field: {key}")


def _read_specs(args: argparse.Namespace) -> list[dict[str, Any]] | None:
    if args.input_file:
        data = json.loads(Path(args.input_file).read_text(encoding="utf-8"))
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        if not raw:
            return None
        data = json.loads(raw)
    else:
        return None
    if isinstance(data, dict) and data.get("object_type") == "CheapExperimentSpec":
        return [data]
    if not isinstance(data, list):
        raise AtlasV2ValidationError("Cheap Experiment Executor input must be a CheapExperimentSpec object or array")
    return data


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atlas V2 Cheap Experiment Executor V1")
    parser.add_argument("--input-file")
    parser.add_argument("--ledger-root", default=str(DEFAULT_LEDGER_ROOT))
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--execution-run-id")
    parser.add_argument("--data-mode", default="MOCK_HISTORICAL")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    ledger = AtlasV2Ledger(Path(args.ledger_root))
    result = AtlasV2CheapExperimentExecutor(ledger).execute(
        _read_specs(args),
        execution_run_id=args.execution_run_id,
        data_mode=args.data_mode,
        created_at=args.created_at,
    )
    print(json.dumps(result.summary(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
