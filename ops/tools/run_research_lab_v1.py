#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    apply_experiment_result_v1,
    build_research_experiment_result_v1,
    build_research_lab_awareness_report_v1,
    validate_research_lab_artifact_v1,
    write_research_lab_artifact_v1,
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _day() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _path(truth_root: Path, day_utc: str, artifact_id: str, identifier: str, filename: str) -> Path:
    return truth_root / "research_lab" / artifact_id / day_utc / identifier / filename


def _plan_path(truth_root: Path, day_utc: str, hypothesis_id: str) -> Path:
    return _path(truth_root, day_utc, "hypothesis_test_plan_v1", hypothesis_id, "hypothesis_test_plan.v1.json")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_v1")
    parser.add_argument("--truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--max_tasks", type=int, default=5)
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = args.day_utc or _day()
    generated_at = _now()
    registry_path = _path(truth_root, day_utc, "hypothesis_registry_v1", "index", "hypothesis_registry.v1.json")
    queue_path = _path(truth_root, day_utc, "research_task_queue_v1", "index", "research_task_queue.v1.json")
    registry = _read(registry_path)
    queue = _read(queue_path)
    completed_results = []
    processed = 0
    for task in list(queue.get("tasks") or []):
        if processed >= args.max_tasks:
            break
        if task.get("status") != "open":
            continue
        hypothesis_id = str(task["hypothesis_id"])
        plan = _read(_plan_path(truth_root, day_utc, hypothesis_id))
        result = build_research_experiment_result_v1(
            experiment_id=f"exp:{task['task_id']}",
            hypothesis_id=hypothesis_id,
            task_id=str(task["task_id"]),
            dataset_used="offline_placeholder_dataset",
            test_window="not_run_live_market",
            trigger_definition="Defined by hypothesis registry trigger_conditions.",
            outcome_definition="Defined by hypothesis registry expected_outcome.",
            sample_count=0,
            expectancy="not_computed",
            win_rate="not_computed",
            drawdown="not_computed",
            regime_dependency="not_computed",
            robustness_notes="Offline runner produced protocol placeholder; no executable trade emitted.",
            overlap_with_existing_sleeves="not_computed",
            result_status="insufficient_data",
            recommendation="continue_test_plan_when_dataset_available",
            next_action="collect_or_bind offline dataset",
            completed_stage=str(plan.get("current_stage") or "definition_check"),
            completed_at_utc=generated_at,
        )
        validate_research_lab_artifact_v1(result)
        write_research_lab_artifact_v1(truth_root=truth_root, day_utc=day_utc, payload=result)
        applied = apply_experiment_result_v1(
            registry=registry,
            task_queue=queue,
            test_plan=plan,
            result=result,
            updated_at_utc=generated_at,
        )
        registry = applied["registry"]
        queue = applied["task_queue"]
        plan = applied["test_plan"]
        for payload in (registry, queue, plan):
            validate_research_lab_artifact_v1(payload)
            write_research_lab_artifact_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
        completed_results.append(result)
        processed += 1
    awareness = build_research_lab_awareness_report_v1(
        generated_at_utc=generated_at,
        registry=registry,
        task_queue=queue,
        experiment_results=completed_results,
    )
    validate_research_lab_artifact_v1(awareness)
    write_research_lab_artifact_v1(truth_root=truth_root, day_utc=day_utc, payload=awareness)
    print(json.dumps({"processed_tasks": processed, "research_lab_only": True, "executable_trade_created": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
