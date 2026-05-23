#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.canonical_operator_state_v1 import build_canonical_operator_state_v1, write_canonical_operator_state_v1  # noqa: E402
from ops.aegis.intelligence_common_v1 import latest_json_v1, write_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


REPORT_FAMILY = "aegis_operator_brief_v1"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_operator_brief_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    canonical_path, canonical = latest_json_v1(root, "aegis_canonical_operator_state_v1", str(args.day_utc), "canonical_operator_state.v1.json")
    if not canonical:
        canonical = build_canonical_operator_state_v1(truth_root=root, repo_root=REPO_ROOT, day_utc=str(args.day_utc))
        paths = write_canonical_operator_state_v1(truth_root=root, day_utc=str(args.day_utc), payload=canonical)
        canonical_path = Path(paths["json"])
    payload = build_operator_brief_v1(canonical=canonical, canonical_path=Path(canonical_path))
    paths = write_operator_brief_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({**paths, "action_count": len(payload.get("action_required") or []), "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


def build_operator_brief_v1(*, canonical: dict[str, Any], canonical_path: Path) -> dict[str, Any]:
    runtime = canonical.get("runtime") if isinstance(canonical.get("runtime"), dict) else {}
    candidates = canonical.get("candidates") if isinstance(canonical.get("candidates"), dict) else {}
    sleeves = canonical.get("sleeves") if isinstance(canonical.get("sleeves"), dict) else {}
    research = canonical.get("research") if isinstance(canonical.get("research"), dict) else {}
    governance = canonical.get("governance") if isinstance(canonical.get("governance"), dict) else {}
    return {
        "schema_id": "aegis_operator_brief",
        "schema_version": "v1",
        "artifact_id": "aegis_operator_brief_v1",
        "day_utc": canonical.get("day_utc"),
        "generated_at_utc": canonical.get("generated_at_utc"),
        "canonical_operator_state_path": str(canonical_path),
        "reads_only_canonical_operator_state": True,
        "today": {
            "runtime_truth_classification": runtime.get("runtime_truth_classification", "UNKNOWN"),
            "highest_readiness_layer": runtime.get("highest_readiness_layer", "UNKNOWN"),
            "target_operating_mode": runtime.get("target_operating_mode", "UNKNOWN"),
            "candidate_count": sum(len(candidates.get(key) or []) for key in candidates),
            "warning_count": len(canonical.get("warnings") or []),
        },
        "action_required": canonical.get("actions_required") or [],
        "top_candidates": canonical.get("top_candidates") or [],
        "what_changed": "SEE_CANONICAL_STATE_DRILLDOWN",
        "sleeve_warnings": (sleeves.get("watch") or []) + (sleeves.get("challenged") or []) + (sleeves.get("insufficient_data") or []),
        "research_priorities": research.get("priority_tasks") or [],
        "governance_approvals": governance.get("awaiting_approval") or [],
        "no_action_now": canonical.get("no_action_now") or [],
        "drilldown_links": canonical.get("drilldown_index") or [],
        "safety": {
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "automatic_approval_allowed": False,
        },
    }


def write_operator_brief_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "operator_brief.v1.json", payload)
    summary_path = out_dir / "operator_brief.summary.txt"
    matrix_path = out_dir / "operator_brief.matrix.csv"
    summary_path.write_text(render_operator_brief_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_operator_brief_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_operator_brief_summary_v1(payload: dict[str, Any]) -> str:
    today = payload.get("today") if isinstance(payload.get("today"), dict) else {}
    lines = [
        "AEGIS OPERATOR BRIEF v1",
        f"day_utc: {payload.get('day_utc')}",
        "source: canonical_operator_state_only",
        f"runtime_truth_classification: {today.get('runtime_truth_classification', 'UNKNOWN')}",
        f"highest_readiness_layer: {today.get('highest_readiness_layer', 'UNKNOWN')}",
        f"actions_required: {len(payload.get('action_required') or [])}",
        f"top_candidates: {len(payload.get('top_candidates') or [])}",
        f"sleeve_warnings: {len(payload.get('sleeve_warnings') or [])}",
        f"governance_approvals: {len(payload.get('governance_approvals') or [])}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "action_required:",
    ]
    for action in payload.get("action_required") or [{"priority": "INFO", "title": "No operator action required."}]:
        lines.append(f"- {action.get('priority')}: {action.get('title')}")
    lines.append("")
    return "\n".join(lines)


def render_operator_brief_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["section", "id", "priority", "title"])
    writer.writeheader()
    for action in payload.get("action_required") or []:
        writer.writerow({"section": "action_required", "id": action.get("action_id", ""), "priority": action.get("priority", ""), "title": action.get("title", "")})
    for candidate in payload.get("top_candidates") or []:
        writer.writerow({"section": "top_candidate", "id": candidate.get("candidate_id", ""), "priority": candidate.get("priority", ""), "title": candidate.get("why_this_trade", "")})
    for warning in payload.get("sleeve_warnings") or []:
        writer.writerow({"section": "sleeve_warning", "id": warning.get("sleeve_id", ""), "priority": warning.get("recommendation", ""), "title": warning.get("source", "")})
    return out.getvalue()


if __name__ == "__main__":
    raise SystemExit(main())
