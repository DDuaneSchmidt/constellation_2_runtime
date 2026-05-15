#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    apply_research_result_to_hypothesis_v1,
    build_research_evidence_packet_v1,
    build_research_queue_task_v1,
    build_research_result_ledger_v1,
    build_research_result_v1,
    build_research_task_queue_v1,
    validate_research_lab_artifact_v1,
    write_research_lab_artifact_v1,
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _day(value: str) -> str:
    return value or datetime.now(UTC).strftime("%Y-%m-%d")


def _queue_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "research_lab" / "research_task_queue_v1" / day_utc / "index" / "research_task_queue.v1.json"


def _ledger_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "research_lab" / "research_result_ledger_v1" / day_utc / "index" / "research_result_ledger.v1.json"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _existing_results(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [row for row in _read_json(path).get("results", []) if isinstance(row, dict)]


def _find_hypothesis_path(truth_root: Path, day_utc: str, hypothesis_id: str, required_inputs: list[Any]) -> Path:
    for raw in required_inputs:
        text = str(raw)
        if text.startswith("research_hypothesis.v1:"):
            ref = text.split(":", 1)[1]
            candidate = Path(ref).expanduser()
            if candidate.exists() and candidate.is_file():
                return candidate.resolve()
    root = truth_root / "research_lab" / "research_hypothesis_v1" / day_utc
    for path in sorted(root.rglob("research_hypothesis.v1.json")) if root.exists() else []:
        payload = _read_json(path)
        if str(payload.get("hypothesis_id") or "") == hypothesis_id:
            return path
    raise ValueError(f"RESEARCH_HYPOTHESIS_NOT_FOUND:{hypothesis_id}")


def _fixture_metrics(path: str) -> dict[str, Any]:
    if not path:
        return {}
    fixture = Path(path).expanduser().resolve()
    if not fixture.exists():
        raise ValueError(f"RESEARCH_FIXTURE_NOT_FOUND:{fixture}")
    payload = _read_json(fixture)
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else payload
    return dict(metrics) if isinstance(metrics, dict) else {}


def _numeric_metric(metrics: dict[str, Any], key: str, default: float) -> float:
    try:
        return float(metrics.get(key, default))
    except (TypeError, ValueError):
        return default


def _task_evaluation(*, task: dict[str, Any], hypothesis: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    task_type = str(task.get("task_type") or "")
    instruments = hypothesis.get("instruments") if isinstance(hypothesis.get("instruments"), list) else []
    expectancy_value = _numeric_metric(metrics, "expectancy", 0.0)
    sample_size_value = int(_numeric_metric(metrics, "sample_size", 0))
    evidence_metrics = {
        "expectancy": str(expectancy_value),
        "sample_size": sample_size_value,
        "hit_rate": str(_numeric_metric(metrics, "hit_rate", 0.0)),
        "drawdown": str(_numeric_metric(metrics, "drawdown", 0.0)),
        "MAE": str(_numeric_metric(metrics, "MAE", 0.0)),
        "MFE": str(_numeric_metric(metrics, "MFE", 0.0)),
        "regime_expectancy": metrics.get("regime_expectancy", {}),
        "forward_return_windows": metrics.get("forward_return_windows", {}),
        "false_positive_rate": str(_numeric_metric(metrics, "false_positive_rate", 0.0)),
        "edge_decay": str(_numeric_metric(metrics, "edge_decay", 0.0)),
    }
    if task_type == "DEFINITION_CHECK":
        has_definition = bool(str(hypothesis.get("hypothesis_summary") or "").strip()) and bool(str(hypothesis.get("title") or "").strip())
        has_falsification = bool(hypothesis.get("failure_conditions")) and bool(hypothesis.get("invalidation_conditions"))
        status = "SUPPORTS_HYPOTHESIS" if has_definition and has_falsification else "NEEDS_MORE_RESEARCH"
        next_task = "DATA_AVAILABILITY_CHECK"
        summary = "Definition is explicit and falsifiable." if status == "SUPPORTS_HYPOTHESIS" else "Definition needs failure and invalidation conditions."
    elif task_type == "DATA_AVAILABILITY_CHECK":
        status = "SUPPORTS_HYPOTHESIS" if instruments else "INSUFFICIENT_DATA"
        next_task = "REPLAY_ANALYSIS" if instruments else ""
        summary = "Required instrument universe is present." if instruments else "No instrument universe available for offline testing."
    elif task_type in {"REPLAY_ANALYSIS", "BACKTEST", "EXPECTANCY_REVIEW"}:
        expectancy = expectancy_value
        sample_size = sample_size_value
        if sample_size <= 0:
            status = "INSUFFICIENT_DATA"
        elif expectancy > 0:
            status = "SUPPORTS_HYPOTHESIS"
        elif expectancy == 0:
            status = "WEAK_SUPPORT"
        else:
            status = "CONTRADICTS_HYPOTHESIS"
        next_task = "EXPECTANCY_REVIEW" if task_type in {"REPLAY_ANALYSIS", "BACKTEST"} else "PROMOTION_REVIEW"
        summary = f"{task_type} deterministic metrics: expectancy={expectancy}, sample_size={sample_size}."
    elif task_type == "FAILURE_MODE_REVIEW":
        status = "NEEDS_MORE_RESEARCH" if hypothesis.get("failure_conditions") else "CONTRADICTS_HYPOTHESIS"
        next_task = "EXPECTANCY_REVIEW" if status == "NEEDS_MORE_RESEARCH" else ""
        summary = "Failure modes retained for further validation." if status == "NEEDS_MORE_RESEARCH" else "Failure modes are undefined."
    else:
        status = "NEEDS_MORE_RESEARCH"
        next_task = ""
        summary = f"Task type {task_type} is recognized but not yet evidence-complete."
    promotion = "PROMOTION_CANDIDATE" if task_type == "PROMOTION_REVIEW" and status == "SUPPORTS_HYPOTHESIS" else "CONTINUE_RESEARCH"
    if status in {"CONTRADICTS_HYPOTHESIS", "INVALIDATED"}:
        promotion = "REJECT"
    return {
        "result_status": status,
        "next_task": next_task,
        "summary": summary,
        "metrics": evidence_metrics,
        "promotion_recommendation": promotion,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_task_queue_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--max_tasks", type=int, default=5)
    parser.add_argument("--fixture_json", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = _day(args.day_utc)
    generated_at = _now()
    queue = _read_json(_queue_path(truth_root, day_utc))
    fixture_metrics = _fixture_metrics(args.fixture_json)
    completed_results = []
    tasks = []
    new_tasks: list[dict[str, Any]] = []
    processed = 0
    for task in queue.get("tasks", []):
        if not isinstance(task, dict):
            continue
        updated = dict(task)
        if processed < args.max_tasks and str(task.get("status")) == "QUEUED":
            hypothesis_path = _find_hypothesis_path(
                truth_root=truth_root,
                day_utc=day_utc,
                hypothesis_id=str(task["hypothesis_id"]),
                required_inputs=list(task.get("required_inputs") or []),
            )
            hypothesis = _read_json(hypothesis_path)
            evaluation = _task_evaluation(task=task, hypothesis=hypothesis, metrics=fixture_metrics)
            evidence = build_research_evidence_packet_v1(
                research_id=f"evidence:{task['task_id']}",
                hypothesis_id=str(task["hypothesis_id"]),
                task_id=str(task["task_id"]),
                title=str(hypothesis.get("title") or task["task_id"]),
                hypothesis_description=str(hypothesis.get("hypothesis_summary") or ""),
                research_type="EDGE",
                created_at_utc=generated_at,
                source_data_summary="Deterministic offline Research Lab executor. Fixture data used when supplied.",
                replay_window=str(fixture_metrics.get("replay_window") or "fixture_or_definition_scope"),
                instruments_tested=[str(item) for item in hypothesis.get("instruments", []) if str(item).strip()],
                regimes_tested=[str(hypothesis.get("expected_regime") or "UNKNOWN")],
                edge_family=str(hypothesis.get("edge_family") or ""),
                expected_holding_period=str(hypothesis.get("expected_holding_period") or ""),
                methodology_summary=f"Offline task evaluation for {task.get('task_type')}.",
                metrics_summary=evaluation["metrics"],
                expectancy_summary=str(evaluation["metrics"].get("expectancy")),
                drawdown_summary=str(evaluation["metrics"].get("drawdown")),
                MAE_MFE_summary=f"MAE={evaluation['metrics'].get('MAE')}; MFE={evaluation['metrics'].get('MFE')}",
                failure_modes=[str(item) for item in hypothesis.get("failure_conditions", []) if str(item).strip()],
                known_limitations=["Fixture/sample data may be synthetic unless source lineage states otherwise."],
                reproducibility_notes="Inputs are explicit task required_inputs plus optional fixture_json.",
                artifact_lineage=[{"artifact_type": "research_hypothesis_v1", "path": str(hypothesis_path)}],
                data_snapshot_refs=[str(Path(args.fixture_json).expanduser().resolve())] if args.fixture_json else [],
                reason_codes=[f"TASK_TYPE:{task.get('task_type')}", f"RESULT_STATUS:{evaluation['result_status']}"],
                research_status="VALIDATED_RESEARCH" if evaluation["promotion_recommendation"] == "PROMOTION_CANDIDATE" else "UNDER_REVIEW",
            )
            evidence_path = write_research_lab_artifact_v1(truth_root=truth_root, day_utc=day_utc, payload=evidence)
            result = build_research_result_v1(
                result_id=f"result:{task['task_id']}",
                hypothesis_id=str(task["hypothesis_id"]),
                task_id=str(task["task_id"]),
                result_status=str(evaluation["result_status"]),
                evidence_refs=[str(evidence_path)],
                conclusion_summary=str(evaluation["summary"]),
                confidence_before=str(hypothesis.get("confidence_level") or "LOW"),
                confidence_after="MEDIUM" if evaluation["result_status"] == "SUPPORTS_HYPOTHESIS" else "LOW",
                confidence_change="EXPLICIT_BY_TRANSITION_ENGINE",
                metrics_summary=dict(evaluation["metrics"]),
                failure_mode_notes="; ".join(str(item) for item in hypothesis.get("failure_conditions", [])),
                invalidation_notes="; ".join(str(item) for item in hypothesis.get("invalidation_conditions", [])) if evaluation["result_status"] in {"CONTRADICTS_HYPOTHESIS", "INVALIDATED"} else "",
                next_recommended_task=str(evaluation["next_task"]),
                promotion_recommendation=str(evaluation["promotion_recommendation"]),
                created_at_utc=generated_at,
                data_snapshot_refs=[str(Path(args.fixture_json).expanduser().resolve())] if args.fixture_json else [],
                artifact_lineage=[
                    {"artifact_type": "research_hypothesis_v1", "path": str(hypothesis_path)},
                    {"artifact_type": "research_evidence_packet_v1", "path": str(evidence_path)},
                ],
                reason_codes=[f"TASK_TYPE:{task.get('task_type')}", f"RESULT_STATUS:{evaluation['result_status']}"],
                reproducibility_notes="Deterministic offline executor; inputs are task required_inputs plus optional fixture_json.",
            )
            transition = apply_research_result_to_hypothesis_v1(
                hypothesis=hypothesis,
                result=result,
                evidence_packet_refs=[str(evidence_path)],
            )
            write_research_lab_artifact_v1(truth_root=truth_root, day_utc=day_utc, payload=transition["updated_hypothesis"])
            completed_results.append(result)
            updated["status"] = "COMPLETED"
            updated["updated_at_utc"] = generated_at
            updated["output_artifact_refs"] = [str(evidence_path), f"research_result_ledger.v1:{result['result_id']}"]
            if transition["next_recommended_task"]:
                new_tasks.append(
                    build_research_queue_task_v1(
                        task_id=f"task:{task['hypothesis_id']}:{transition['next_recommended_task']}:{processed + 1}",
                        hypothesis_id=str(task["hypothesis_id"]),
                        task_type=str(transition["next_recommended_task"]),
                        priority=str(task.get("priority") or "NORMAL"),
                        status="QUEUED",
                        required_inputs=[f"research_hypothesis.v1:{hypothesis_path}", f"research_evidence_packet.v1:{evidence_path}"],
                        output_artifact_refs=[],
                        blocker_reason_codes=[],
                        created_at_utc=generated_at,
                    )
                )
            processed += 1
        tasks.append(updated)
    new_queue = build_research_task_queue_v1(generated_at_utc=generated_at, tasks=[*tasks, *new_tasks])
    ledger = build_research_result_ledger_v1(
        generated_at_utc=generated_at,
        results=[*_existing_results(_ledger_path(truth_root, day_utc)), *completed_results],
    )
    for payload in (new_queue, ledger):
        validate_research_lab_artifact_v1(payload)
        write_research_lab_artifact_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    print(
        json.dumps(
            {
                "processed_tasks": processed,
                "result_count": len(ledger["results"]),
                "research_lab_only": True,
                "runtime_mutation_allowed": False,
                "broker_submit_required": False,
                "trade_authorization_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
