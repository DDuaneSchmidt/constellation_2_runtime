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

from constellation_2.common.aegis_chatgpt_control_packet_v1 import (  # noqa: E402
    aegis_chatgpt_control_packet_path_v1,
    render_aegis_chatgpt_control_packet_summary_v1,
)
from ops.aegis.intelligence_common_v1 import intelligence_summaries_v1, latest_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import (  # noqa: E402
    build_runtime_truth_kernel_v1,
    render_recovery_plan_v1,
    write_runtime_truth_kernel_reports_v1,
)
from ops.aegis.paper_session_ledger_v1 import paper_session_ledger_path_v1, read_paper_session_ledger_v1  # noqa: E402
from ops.aegis.paper_operator_projection_v1 import paper_operator_projection_path_v1  # noqa: E402


AUDIT_PROMPT = """Use this Aegis ChatGPT Control Packet as the source of truth.

Goal:
Audit Aegis Lite for remaining functionality gaps, missing operational proof, stale assumptions, unsafe coupling, incomplete UI/operator workflows, missing data, and anything that is designed but not operational.

Classify each item as:
- READY
- PARTIAL
- MISSING
- UNPROVEN
- DEFERRED
- LEGACY

Focus on:
- Aegis Lite EOD
- Noon preflight
- Event Monitoring
- Market Context
- Trade Sizing
- Manual Packets
- Receipts / Outcomes
- Sleeve Performance
- AI Feedback / Evidence Gate
- Research Lab
- Operator Inbox
- ChatGPT Control Packet
- UI / Operator Status
- Governance / Safety

Give me:
1. what is complete
2. what is incomplete
3. what could fail in production
4. what should be tested next
5. what should not be claimed yet
6. top 10 next hardening tasks"""


def audit_handoff_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).expanduser().resolve()
        / "reports"
        / "aegis_audit_handoff_v1"
        / day_utc
        / "aegis_audit_handoff.txt"
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"FAIL: JSON read failed path={path} err={type(exc).__name__}:{exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"FAIL: JSON object required path={path}")
    return payload


def _current_release(root: Path) -> dict[str, Any]:
    path = root / "releases" / "current_release.v1.json"
    if not path.exists():
        return {"status": "MISSING", "path": str(path)}
    payload = _read_json(path)
    return {
        "status": "PRESENT",
        "path": str(path),
        "release_id": str(payload.get("release_id") or ""),
        "release_path": str(payload.get("release_path") or ""),
        "commit": str(payload.get("commit") or ""),
        "activated_at_utc": str(payload.get("activated_at_utc") or ""),
    }


def _json_block(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True)


def _lines(title: str, values: list[str]) -> list[str]:
    if not values:
        values = ["NONE"]
    return [title, *[f"- {item}" for item in values]]


def _known_blockers_from_kernel(kernel: dict[str, Any]) -> list[str]:
    evaluation = kernel.get("runtime_evaluation") if isinstance(kernel.get("runtime_evaluation"), dict) else {}
    graph = evaluation.get("capabilities") if isinstance(evaluation.get("capabilities"), dict) else (kernel.get("dependency_graph") if isinstance(kernel.get("dependency_graph"), dict) else {})
    blockers: list[str] = []
    for capability, row in graph.items():
        if not isinstance(row, dict) or bool(row.get("allowed", False)):
            continue
        reason = str(row.get("reason") or "").strip()
        blockers.append(f"{capability}: {reason or 'BLOCKED_BY_KERNEL'}")
    return sorted(set(blockers))


def build_handoff_text_v1(
    *,
    packet: dict[str, Any],
    packet_path: Path,
    truth_root: Path,
    day_utc: str,
    kernel: dict[str, Any] | None = None,
    kernel_paths: dict[str, str] | None = None,
) -> str:
    kernel = kernel or build_runtime_truth_kernel_v1(truth_root=truth_root, day_utc=day_utc)
    kernel_paths = kernel_paths or write_runtime_truth_kernel_reports_v1(truth_root=truth_root, payload=kernel)
    runtime_truth = str(kernel.get("runtime_truth_classification") or "UNKNOWN")
    if runtime_truth not in {"REAL_RUNTIME", "PARTIAL_CONTEXT", "DRY_RUN_ONLY", "DEMO_ONLY"}:
        raise SystemExit(f"FAIL: aegis:audit cannot produce a handoff from runtime_truth_classification={runtime_truth}")
    current_release = _current_release(truth_root)
    actionable = packet.get("current_actionable_items") if isinstance(packet.get("current_actionable_items"), list) else []
    blocked = packet.get("blocked_items") if isinstance(packet.get("blocked_items"), list) else []
    do_not_claim = [str(item) for item in kernel.get("do_not_claim", []) if str(item)] if isinstance(kernel.get("do_not_claim"), list) else []
    next_actions = [str(item) for item in packet.get("next_operator_actions", []) if str(item)] if isinstance(packet.get("next_operator_actions"), list) else []
    known_blockers = _known_blockers_from_kernel(kernel)
    evaluation = kernel.get("runtime_evaluation") if isinstance(kernel.get("runtime_evaluation"), dict) else {}
    graph = evaluation.get("capabilities") if isinstance(evaluation.get("capabilities"), dict) else (kernel.get("dependency_graph") if isinstance(kernel.get("dependency_graph"), dict) else {})
    trade = graph.get("TRADE_ADVICE_ALLOWED") if isinstance(graph.get("TRADE_ADVICE_ALLOWED"), dict) else {}
    manual = graph.get("MANUAL_TRADE_CAPTURE_ALLOWED") if isinstance(graph.get("MANUAL_TRADE_CAPTURE_ALLOWED"), dict) else {}
    packet_summary = {
        **packet,
        "runtime_truth_classification": runtime_truth,
        "readiness_state": {
            "classification": kernel.get("highest_readiness_layer"),
            "layers": kernel.get("layers"),
            "kernel_authority": "aegis_runtime_truth_kernel_v1",
            "missing_or_stale_source_count": kernel.get("missing_or_stale_source_count"),
        },
        "do_not_claim": do_not_claim,
        "trade_advice_allowed": bool(trade.get("allowed", False)),
        "manual_trade_capture_allowed": bool(manual.get("allowed", False)),
    }
    transition_payload = _read_json(Path(kernel_paths["runtime_state_transitions"])) if Path(kernel_paths["runtime_state_transitions"]).exists() else {}
    invalidation_payload = _read_json(Path(kernel_paths["runtime_invalidations"])) if Path(kernel_paths["runtime_invalidations"]).exists() else {}
    intelligence = intelligence_summaries_v1(truth_root, day_utc)
    candidate_path, candidate_lifecycle = latest_json_v1(truth_root, "aegis_candidate_lifecycle_v1", day_utc, "candidate_lifecycle.v1.json")
    paper_golden_path, paper_golden = latest_json_v1(truth_root, "aegis_paper_trade_golden_path_v1", day_utc, "paper_trade_golden_path.v1.json")
    paper_session_ledger = read_paper_session_ledger_v1(truth_root=truth_root, day_utc=day_utc)
    paper_session = (paper_session_ledger.get("sessions") or [{}])[0] if isinstance(paper_session_ledger.get("sessions"), list) else {}
    canonical_path, canonical = latest_json_v1(truth_root, "aegis_canonical_operator_state_v1", day_utc, "canonical_operator_state.v1.json")
    brief_path, brief = latest_json_v1(truth_root, "aegis_operator_brief_v1", day_utc, "operator_brief.v1.json")
    candidate_rows = candidate_lifecycle.get("candidates") if isinstance(candidate_lifecycle.get("candidates"), list) else []
    candidate_decision_summary = {
        "candidate_lifecycle_path": str(candidate_path or ""),
        "candidate_count": len(candidate_rows),
        "awaiting_decision": sum(1 for row in candidate_rows if row.get("current_operator_decision") in {None, "", "GENERATED", "REVIEWED"}),
        "candidates_with_decisions": sum(1 for row in candidate_rows if row.get("current_operator_decision") not in {None, "", "GENERATED", "REVIEWED"}),
        "corrected_decisions": sum(1 for row in candidate_rows if int(row.get("correction_count") or 0) > 0),
        "pending_outcomes": sum(1 for row in candidate_rows if row.get("outcome_status") in {"OUTCOME_PENDING", "OUTCOME_UNKNOWN"}),
        "latest_state": [
            {
                "candidate_id": row.get("candidate_id"),
                "current_operator_decision": row.get("current_operator_decision"),
                "current_intended_shares": row.get("current_intended_shares"),
                "current_risk_bucket": row.get("current_risk_bucket"),
                "correction_count": row.get("correction_count"),
                "latest_correction_id": row.get("latest_correction_id"),
                "audit_history_count": len(row.get("audit_history") or []),
            }
            for row in candidate_rows[:10]
        ],
    }

    sections = [
        "AEGIS AUDIT HANDOFF v1",
        f"day_utc: {day_utc}",
        f"truth_root: {truth_root}",
        f"control_packet_path: {packet_path}",
        "",
        "CONTROL PACKET SUMMARY",
        render_aegis_chatgpt_control_packet_summary_v1(packet_summary),
        "",
        "AUDIT QUICK STATUS",
        f"runtime_truth_classification: {runtime_truth}",
        f"target_operating_mode: {kernel.get('target_operating_mode') or 'HUMAN_APPROVED_ADVISORY_RUNTIME'}",
        f"human_approved_advisory_runtime_ready: {str(bool(kernel.get('human_approved_advisory_runtime_ready'))).lower()}",
        f"advisory_status: {kernel.get('advisory_status') or 'ADVISORY_NOT_EVALUATED'}",
        f"operator_action_required: {str(bool(kernel.get('operator_action_required'))).lower()}",
        f"operator_action_reason: {kernel.get('operator_action_reason') or 'NONE'}",
        f"live_broker_trading_policy: {kernel.get('live_broker_trading_policy') or 'DISABLED_BY_DESIGN'}",
        f"autonomous_execution_policy: {kernel.get('autonomous_execution_policy') or 'DISABLED_BY_DESIGN'}",
        f"broker_submit_transmit_policy: {kernel.get('broker_submit_transmit_policy') or 'DISABLED_BY_DESIGN'}",
        f"readiness_state: {_json_block({'classification': kernel.get('highest_readiness_layer'), 'layers': kernel.get('layers'), 'kernel_authority': 'aegis_runtime_truth_kernel_v1'})}",
        f"runtime_truth_kernel_path: {kernel_paths['runtime_truth_kernel']}",
        f"runtime_truth_kernel_classification: {kernel.get('runtime_truth_classification')}",
        f"runtime_truth_kernel_highest_readiness_layer: {kernel.get('highest_readiness_layer')}",
        f"runtime_truth_kernel_missing_or_stale_source_count: {kernel.get('missing_or_stale_source_count')}",
        f"runtime_state_snapshot_path: {kernel_paths['runtime_state_snapshot']}",
        f"runtime_state_transition_path: {kernel_paths['runtime_state_transitions']}",
        f"runtime_invalidations_path: {kernel_paths['runtime_invalidations']}",
        f"actionable_items_count: {len(actionable)}",
        f"blocked_items_count: {len(blocked)}",
        "",
        "CURRENT RELEASE",
        _json_block(current_release),
        "",
        "MARKET CONTEXT STATUS",
        _json_block(packet.get("market_context_status") or {}),
        "",
        "EVENT MONITOR STATUS",
        _json_block(packet.get("event_monitoring_status") or {}),
        "",
        "AI FEEDBACK STATUS",
        _json_block(packet.get("ai_feedback_status") or {}),
        "",
        "DATASET GAPS",
        _json_block(packet.get("dataset_gaps") or {}),
        "",
        *_lines("KNOWN BLOCKERS", known_blockers),
        "",
        "RUNTIME TRUTH KERNEL",
        _json_block(
            {
                "paths": kernel_paths,
                "blocked_capabilities": kernel.get("blocked_capabilities") or [],
                "policy_disabled_capabilities": kernel.get("policy_disabled_capabilities") or [],
                "optional_not_required_capabilities": kernel.get("optional_not_required_capabilities") or [],
                "out_of_scope_capabilities": kernel.get("out_of_scope_capabilities") or {},
                "layers": kernel.get("layers") or {},
                "claim_violation_count": kernel.get("claim_violation_count"),
                "runtime_state_snapshot": kernel_paths.get("runtime_state_snapshot"),
                "runtime_state_transitions": kernel_paths.get("runtime_state_transitions"),
                "runtime_invalidations": kernel_paths.get("runtime_invalidations"),
            }
        ),
        "",
        "RUNTIME STATE HISTORY",
        _json_block(
            {
                "snapshot_path": kernel_paths.get("runtime_state_snapshot"),
                "transition_summary": transition_payload.get("summary") or {},
                "transition_count": transition_payload.get("transition_count", 0),
                "invalidation_summary": {
                    "path": kernel_paths.get("runtime_invalidations"),
                    "invalidation_count": invalidation_payload.get("invalidation_count", 0),
                },
                "replay_command": f"npm run aegis:replay-state -- --truth_root {truth_root} --day {day_utc}",
                "state_diff_command": f"npm run aegis:state-diff -- --truth_root {truth_root} --from-day {day_utc} --to-day {day_utc}",
            }
        ),
        "",
        "ADAPTIVE INTELLIGENCE REPORTS",
        _json_block(intelligence),
        "",
        "OPERATOR COMPRESSION",
        _json_block(
            {
                "canonical_operator_state_path": str(canonical_path or ""),
                "operator_brief_path": str(brief_path or ""),
                "canonical_action_count": len(canonical.get("actions_required") or []),
                "brief_action_count": len(brief.get("action_required") or []),
                "canonical_is_read_only_projection": bool((canonical.get("projection_semantics") or {}).get("read_only", False)),
                "operator_brief_reads_only_canonical_state": bool(brief.get("reads_only_canonical_operator_state", False)),
            }
        ),
        "",
        "CANDIDATE DECISION / CORRECTION HISTORY",
        _json_block(candidate_decision_summary),
        "",
        "PAPER SESSION LEDGER",
        _json_block({
            "artifact_path": str(paper_session_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)),
            "paper_session_id": paper_session.get("paper_session_id"),
            "scheduled_run_time": paper_session.get("scheduled_run_time"),
            "execution_started_at": paper_session.get("execution_started_at"),
            "execution_completed_at": paper_session.get("execution_completed_at"),
            "canonicalized_at": paper_session.get("canonicalized_at"),
            "status": paper_session.get("status"),
            "reconstruction_count": paper_session.get("reconstruction_count"),
            "projection_path": str(paper_operator_projection_path_v1(truth_root=truth_root, day_utc=day_utc)),
        }),
        "",
        "PAPER REHEARSAL GOLDEN PATH",
        _json_block({
            "artifact_path": str(paper_golden_path or ""),
            "mode": paper_golden.get("mode") if isinstance(paper_golden, dict) else "",
            "execution": paper_golden.get("execution") if isinstance(paper_golden, dict) else "",
            "paper_rehearsal_lifecycle_proven": bool(paper_golden.get("paper_rehearsal_lifecycle_proven")) if isinstance(paper_golden, dict) else False,
            "receipt_type": paper_golden.get("receipt_type") if isinstance(paper_golden, dict) else "",
            "safety": paper_golden.get("safety") if isinstance(paper_golden, dict) else {},
            "chain": paper_golden.get("chain") if isinstance(paper_golden, dict) else [],
        }),
        "",
        "MISSING / STALE / INVALID SOURCES",
        _json_block(kernel.get("missing_or_stale_sources") or []),
        "",
        "RECOVERY PLAN",
        render_recovery_plan_v1(kernel),
        "",
        *_lines("DO NOT CLAIM", do_not_claim),
        "",
        *_lines("NEXT OPERATOR ACTIONS", next_actions),
        "",
        "RECOMMENDED CHATGPT AUDIT PROMPT",
        AUDIT_PROMPT,
        "",
    ]
    return "\n".join(sections)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_audit_handoff_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = str(args.day)
    packet_path = aegis_chatgpt_control_packet_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not packet_path.exists():
        raise SystemExit(f"FAIL: control packet missing; run aegis:chatgpt:control-packet first path={packet_path}")
    packet = _read_json(packet_path)
    kernel = build_runtime_truth_kernel_v1(truth_root=truth_root, day_utc=day_utc)
    kernel_paths = write_runtime_truth_kernel_reports_v1(truth_root=truth_root, payload=kernel)
    text = build_handoff_text_v1(
        packet=packet,
        packet_path=packet_path,
        truth_root=truth_root,
        day_utc=day_utc,
        kernel=kernel,
        kernel_paths=kernel_paths,
    )
    out_path = audit_handoff_path_v1(truth_root=truth_root, day_utc=day_utc)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")

    actionable = packet.get("current_actionable_items") if isinstance(packet.get("current_actionable_items"), list) else []
    blocked = packet.get("blocked_items") if isinstance(packet.get("blocked_items"), list) else []
    do_not_claim = [str(item) for item in kernel.get("do_not_claim", []) if str(item)] if isinstance(kernel.get("do_not_claim"), list) else []
    next_actions = [str(item) for item in packet.get("next_operator_actions", []) if str(item)] if isinstance(packet.get("next_operator_actions"), list) else []
    print("AEGIS AUDIT HANDOFF")
    print(f"runtime_truth_classification: {kernel.get('runtime_truth_classification') or 'UNKNOWN'}")
    print(f"target_operating_mode: {kernel.get('target_operating_mode') or 'HUMAN_APPROVED_ADVISORY_RUNTIME'}")
    print(f"human_approved_advisory_runtime_ready: {str(bool(kernel.get('human_approved_advisory_runtime_ready'))).lower()}")
    print(f"advisory_status: {kernel.get('advisory_status') or 'ADVISORY_NOT_EVALUATED'}")
    print(f"operator_action_required: {str(bool(kernel.get('operator_action_required'))).lower()}")
    print(f"live_broker_trading_policy: {kernel.get('live_broker_trading_policy') or 'DISABLED_BY_DESIGN'}")
    print(f"autonomous_execution_policy: {kernel.get('autonomous_execution_policy') or 'DISABLED_BY_DESIGN'}")
    print(f"broker_submit_transmit_policy: {kernel.get('broker_submit_transmit_policy') or 'DISABLED_BY_DESIGN'}")
    print(f"runtime_truth_kernel_path: {kernel_paths['runtime_truth_kernel']}")
    print(f"runtime_truth_kernel_classification: {kernel.get('runtime_truth_classification') or 'UNKNOWN'}")
    print(f"runtime_truth_kernel_highest_readiness_layer: {kernel.get('highest_readiness_layer') or 'UNKNOWN'}")
    print(f"runtime_truth_kernel_missing_or_stale_source_count: {kernel.get('missing_or_stale_source_count')}")
    print(f"runtime_state_snapshot_path: {kernel_paths['runtime_state_snapshot']}")
    print(f"runtime_state_transition_path: {kernel_paths['runtime_state_transitions']}")
    print(f"runtime_invalidations_path: {kernel_paths['runtime_invalidations']}")
    paper_golden_path, paper_golden = latest_json_v1(truth_root, "aegis_paper_trade_golden_path_v1", day_utc, "paper_trade_golden_path.v1.json")
    paper_session_ledger = read_paper_session_ledger_v1(truth_root=truth_root, day_utc=day_utc)
    paper_session = (paper_session_ledger.get("sessions") or [{}])[0] if isinstance(paper_session_ledger.get("sessions"), list) else {}
    print(f"paper_session_id: {paper_session.get('paper_session_id') or ''}")
    print(f"paper_session_scheduled_run_time: {paper_session.get('scheduled_run_time') or ''}")
    print(f"paper_session_canonicalized_at: {paper_session.get('canonicalized_at') or ''}")
    print(f"paper_session_ledger_path: {paper_session_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)}")
    print(f"readiness_state: {_json_block({'classification': kernel.get('highest_readiness_layer'), 'layers': kernel.get('layers'), 'kernel_authority': 'aegis_runtime_truth_kernel_v1'})}")
    print(f"paper_rehearsal_golden_path_proven: {str(bool(paper_golden.get('paper_rehearsal_lifecycle_proven')) if isinstance(paper_golden, dict) else False).lower()}")
    print(f"paper_rehearsal_golden_path_path: {paper_golden_path or ''}")
    print(f"paper_rehearsal_receipt_type: {paper_golden.get('receipt_type') if isinstance(paper_golden, dict) else ''}")
    print(f"actionable_items_count: {len(actionable)}")
    print(f"blocked_items_count: {len(blocked)}")
    print("do_not_claim:")
    for item in do_not_claim or ["NONE"]:
        print(f"- {item}")
    print("next_operator_actions:")
    for item in next_actions or ["NONE"]:
        print(f"- {item}")
    print(
        json.dumps(
            {
                "handoff_path": str(out_path),
                "runtime_truth_kernel_path": kernel_paths["runtime_truth_kernel"],
                "missing_stale_sources_path": kernel_paths["missing_stale_sources"],
                "recovery_plan_path": kernel_paths["recovery_plan"],
                "readiness_dependencies_path": kernel_paths["readiness_dependencies"],
                "runtime_state_snapshot_path": kernel_paths["runtime_state_snapshot"],
                "runtime_state_transition_path": kernel_paths["runtime_state_transitions"],
                "runtime_invalidations_path": kernel_paths["runtime_invalidations"],
                "replay_command": f"npm run aegis:replay-state -- --truth_root {truth_root} --day {day_utc}",
                "state_diff_command": f"npm run aegis:state-diff -- --truth_root {truth_root} --from-day {day_utc} --to-day {day_utc}",
                "broker_submit_required": False,
                "ib_automation_required": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
