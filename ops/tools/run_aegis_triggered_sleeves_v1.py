#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.event_regime_trigger_evaluator_v1 import (  # noqa: E402
    build_event_regime_trigger_evaluation_v1,
    write_event_regime_trigger_evaluation_v1,
)
from ops.aegis.candidate_lifecycle_v1 import write_candidate_lifecycle_reports_v1  # noqa: E402
from ops.aegis.candidate_ranking_explanation_v1 import build_candidate_ranking_v1, write_candidate_ranking_reports_v1  # noqa: E402
from ops.aegis.intelligence_common_v1 import latest_json_v1, read_json_v1, write_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


REPORT_FAMILY = "aegis_triggered_sleeve_runs_v1"
CANDIDATE_FAMILY = "aegis_triggered_advisory_candidates_v1"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_triggered_sleeves_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day-utc", "--day", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--trigger-id", default="")
    parser.add_argument("--sleeve-ids", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reason", default="")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    payload = build_triggered_sleeve_runs_v1(
        truth_root=root,
        repo_root=REPO_ROOT,
        day_utc=str(args.day_utc),
        trigger_id=str(args.trigger_id or ""),
        sleeve_ids=_split_csv(args.sleeve_ids),
        dry_run=bool(args.dry_run),
        reason=str(args.reason or ""),
    )
    paths = write_triggered_sleeve_runs_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(
        json.dumps(
            {
                **paths,
                "run_count": len(payload["runs"]),
                "candidate_count": sum(int(row.get("candidate_count") or 0) for row in payload["runs"]),
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


def build_triggered_sleeve_runs_v1(
    *,
    truth_root: Path,
    repo_root: Path,
    day_utc: str,
    trigger_id: str = "",
    sleeve_ids: list[str] | None = None,
    dry_run: bool = False,
    reason: str = "",
) -> dict[str, Any]:
    truth_root = Path(truth_root).expanduser().resolve()
    eval_path, evaluation = latest_json_v1(truth_root, "aegis_event_regime_trigger_evaluator_v1", day_utc, "trigger_evaluation.v1.json")
    if not evaluation:
        evaluation = build_event_regime_trigger_evaluation_v1(truth_root=truth_root, repo_root=repo_root, day_utc=day_utc)
        paths = write_event_regime_trigger_evaluation_v1(truth_root=truth_root, day_utc=day_utc, payload=evaluation)
        eval_path = Path(paths["json"])
    selected_filter = {item.upper() for item in (sleeve_ids or [])}
    decisions = evaluation.get("decisions") if isinstance(evaluation.get("decisions"), list) else []
    run_decisions = [
        row
        for row in decisions
        if isinstance(row, dict)
        and row.get("decision") == "RUN_SLEEVES"
        and (not trigger_id or row.get("trigger_id") == trigger_id)
    ]
    started = _now()
    runs: list[dict[str, Any]] = []
    for decision in run_decisions:
        selected = [str(item).upper() for item in decision.get("selected_sleeve_ids") or []]
        if selected_filter:
            selected = [item for item in selected if item in selected_filter]
        if not selected:
            runs.append(_skipped_run(decision=decision, reason="NO_SELECTED_SLEEVES_AFTER_FILTER", started=started, dry_run=dry_run))
            continue
        missing_generators = [sleeve_id for sleeve_id in selected if _generator_status(sleeve_id) != "SLEEVE_ADVISORY_GENERATOR_AVAILABLE"]
        candidates, candidate_path = _write_advisory_candidates(
            truth_root=truth_root,
            day_utc=day_utc,
            decision=decision,
            sleeve_ids=selected,
            dry_run=dry_run,
        )
        lifecycle_paths = write_candidate_lifecycle_reports_v1(truth_root=truth_root, day_utc=day_utc)
        ranking = build_candidate_ranking_v1(truth_root=truth_root, day_utc=day_utc)
        ranking_paths = write_candidate_ranking_reports_v1(truth_root=truth_root, day_utc=day_utc, payload=ranking)
        completed = _now()
        runs.append(
            {
                "triggered_run_id": f"triggered-run:{decision.get('trigger_id')}:{day_utc}",
                "trigger_id": decision.get("trigger_id"),
                "trigger_type": decision.get("trigger_type"),
                "detected_condition": decision.get("detected_condition"),
                "selected_sleeve_ids": selected,
                "command_executed": _command_string(trigger_id=str(decision.get("trigger_id") or ""), sleeve_ids=selected, dry_run=dry_run),
                "started_at": started,
                "completed_at": completed,
                "status": "SUCCESS",
                "dry_run": dry_run,
                "advisory_candidate_artifacts": [str(candidate_path)],
                "candidate_count": len(candidates),
                "candidate_ids": [candidate["candidate_id"] for candidate in candidates],
                "output_artifacts": [str(candidate_path), lifecycle_paths["lifecycle"], ranking_paths["ranking"]],
                "evidence_hashes": decision.get("evidence_hashes") or [],
                "safety": _safety(),
                "errors": [],
                "warnings": [f"{sleeve_id}:SLEEVE_NO_ADVISORY_GENERATOR" for sleeve_id in missing_generators],
                "reason": reason or decision.get("reason_selected") or "",
            }
        )
    if not runs:
        runs.append(
            {
                "triggered_run_id": f"triggered-run:skipped:{day_utc}",
                "trigger_id": trigger_id or "",
                "trigger_type": "",
                "detected_condition": "",
                "selected_sleeve_ids": [],
                "command_executed": _command_string(trigger_id=trigger_id, sleeve_ids=sorted(selected_filter), dry_run=dry_run),
                "started_at": started,
                "completed_at": _now(),
                "status": "SKIPPED",
                "dry_run": dry_run,
                "advisory_candidate_artifacts": [],
                "candidate_count": 0,
                "candidate_ids": [],
                "output_artifacts": [str(eval_path)] if eval_path else [],
                "evidence_hashes": [_sha256(eval_path)] if eval_path else [],
                "safety": _safety(),
                "errors": [],
                "warnings": ["NO_RUN_SLEEVES_DECISIONS"],
                "reason": reason or "No trigger evaluation decisions were marked RUN_SLEEVES.",
            }
        )
    return {
        "schema_id": "aegis_triggered_sleeve_runs",
        "schema_version": "v1",
        "artifact_id": "aegis_triggered_sleeve_runs_v1",
        "generated_at": _now(),
        "day_utc": day_utc,
        "truth_root": str(truth_root),
        "trigger_evaluation_path": str(eval_path) if eval_path else "",
        "runs": runs,
        "run_count": len(runs),
        "candidate_count": sum(int(row.get("candidate_count") or 0) for row in runs),
        "safety": _safety(),
    }


def write_triggered_sleeve_runs_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "triggered_sleeve_runs.v1.json", payload)
    summary_path = out_dir / "triggered_sleeve_runs.summary.txt"
    matrix_path = out_dir / "triggered_sleeve_runs.matrix.csv"
    summary_path.write_text(render_triggered_sleeve_runs_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_triggered_sleeve_runs_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_triggered_sleeve_runs_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS TRIGGERED SLEEVE RUNS v1",
        f"day_utc: {payload.get('day_utc')}",
        f"run_count: {payload.get('run_count')}",
        f"candidate_count: {payload.get('candidate_count')}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "runs:",
    ]
    for row in payload.get("runs") or []:
        lines.append(
            f"- {row.get('triggered_run_id')}: status={row.get('status')} trigger={row.get('trigger_id') or 'NONE'} sleeves={','.join(row.get('selected_sleeve_ids') or []) or 'NONE'} candidates={row.get('candidate_count')}"
        )
    lines.append("")
    return "\n".join(lines)


def render_triggered_sleeve_runs_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=["triggered_run_id", "trigger_id", "status", "selected_sleeve_ids", "candidate_count", "broker_execution_allowed", "autonomous_execution_allowed"],
    )
    writer.writeheader()
    for row in payload.get("runs") or []:
        safety = row.get("safety") if isinstance(row.get("safety"), dict) else {}
        writer.writerow(
            {
                "triggered_run_id": row.get("triggered_run_id") or "",
                "trigger_id": row.get("trigger_id") or "",
                "status": row.get("status") or "",
                "selected_sleeve_ids": ",".join(row.get("selected_sleeve_ids") or []),
                "candidate_count": row.get("candidate_count") or 0,
                "broker_execution_allowed": safety.get("broker_execution_allowed") is True,
                "autonomous_execution_allowed": safety.get("autonomous_execution_allowed") is True,
            }
        )
    return out.getvalue()


def _write_advisory_candidates(*, truth_root: Path, day_utc: str, decision: dict[str, Any], sleeve_ids: list[str], dry_run: bool) -> tuple[list[dict[str, Any]], Path]:
    candidates = []
    for idx, sleeve_id in enumerate(sleeve_ids, start=1):
        if _generator_status(sleeve_id) != "SLEEVE_ADVISORY_GENERATOR_AVAILABLE":
            continue
        candidates.append(
            {
                "candidate_id": f"triggered-advisory:{decision.get('trigger_id')}:{sleeve_id}:{idx}",
                "trigger_id": decision.get("trigger_id"),
                "sleeve_id": sleeve_id,
                "symbol": "REVIEW_REQUIRED",
                "side": "REVIEW_REQUIRED",
                "advisory_only": True,
                "human_review_required": True,
                "manual_execution_only": True,
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
                "broker_submit_transmit_called": False,
                "reason": decision.get("reason_selected") or "",
                "run_reason": decision.get("reason_selected") or "",
                "trigger_type": decision.get("trigger_type"),
                "detected_condition": decision.get("detected_condition"),
                "evidence_artifacts": decision.get("evidence_artifacts") or [],
                "evidence_hashes": decision.get("evidence_hashes") or [],
                "generator_status": _generator_status(sleeve_id),
                "dry_run": dry_run,
            }
        )
    payload = {
        "schema_id": "aegis_triggered_advisory_candidates",
        "schema_version": "v1",
        "artifact_id": "aegis_triggered_advisory_candidates_v1",
        "generated_at": _now(),
        "day_utc": day_utc,
        "trigger_id": decision.get("trigger_id"),
        "candidate_count": len(candidates),
        "candidates": candidates,
        "safety": _safety(),
    }
    safe_trigger = _safe(str(decision.get("trigger_id") or "trigger"))
    path = Path(truth_root).expanduser().resolve() / "reports" / CANDIDATE_FAMILY / day_utc / safe_trigger / "triggered_advisory_candidates.v1.json"
    write_json_v1(path, payload)
    return candidates, path


def _skipped_run(*, decision: dict[str, Any], reason: str, started: str, dry_run: bool) -> dict[str, Any]:
    return {
        "triggered_run_id": f"triggered-run:{decision.get('trigger_id')}:skipped",
        "trigger_id": decision.get("trigger_id"),
        "trigger_type": decision.get("trigger_type"),
        "detected_condition": decision.get("detected_condition"),
        "selected_sleeve_ids": [],
        "command_executed": _command_string(trigger_id=str(decision.get("trigger_id") or ""), sleeve_ids=[], dry_run=dry_run),
        "started_at": started,
        "completed_at": _now(),
        "status": "SKIPPED",
        "dry_run": dry_run,
        "advisory_candidate_artifacts": [],
        "candidate_count": 0,
        "candidate_ids": [],
        "output_artifacts": [],
        "evidence_hashes": decision.get("evidence_hashes") or [],
        "safety": _safety(),
        "errors": [],
        "warnings": [reason],
        "reason": reason,
    }


def _command_string(*, trigger_id: str, sleeve_ids: list[str], dry_run: bool) -> str:
    parts = ["npm run aegis:triggered-sleeves", "--"]
    if trigger_id:
        parts.extend(["--trigger-id", trigger_id])
    if sleeve_ids:
        parts.extend(["--sleeve-ids", ",".join(sleeve_ids)])
    if dry_run:
        parts.append("--dry-run")
    return " ".join(parts)


def _safety() -> dict[str, bool]:
    return {
        "advisory_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "broker_submit_transmit_called": False,
    }


def _generator_status(sleeve_id: str) -> str:
    return "SLEEVE_ADVISORY_GENERATOR_AVAILABLE" if sleeve_id == "PRIMARY" else "SLEEVE_NO_ADVISORY_GENERATOR"


def _split_csv(value: str) -> list[str]:
    return [item.strip().upper() for item in str(value or "").split(",") if item.strip()]


def _safe(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value) or "trigger"


def _sha256(path: Path | None) -> str:
    if not path:
        return ""
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return ""


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
