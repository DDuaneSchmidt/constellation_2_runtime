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


def _known_blockers(packet: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    readiness = packet.get("readiness_state") if isinstance(packet.get("readiness_state"), dict) else {}
    primary_blocker = str(readiness.get("primary_blocker") or packet.get("reason_if_blocked") or "").strip()
    if primary_blocker:
        blockers.append(primary_blocker)
    for item in readiness.get("reason_codes", []) if isinstance(readiness.get("reason_codes"), list) else []:
        if str(item):
            blockers.append(str(item))
    for row in packet.get("blocked_items", []) if isinstance(packet.get("blocked_items"), list) else []:
        if not isinstance(row, dict):
            continue
        for item in row.get("blockers", []) if isinstance(row.get("blockers"), list) else []:
            if str(item):
                blockers.append(str(item))
        reason = str(row.get("reason_if_blocked") or "").strip()
        if reason:
            blockers.append(reason)
    return sorted(set(blockers))


def build_handoff_text_v1(*, packet: dict[str, Any], packet_path: Path, truth_root: Path, day_utc: str) -> str:
    runtime_truth = str(packet.get("runtime_truth_classification") or "UNKNOWN")
    if runtime_truth == "REAL_RUNTIME":
        raise SystemExit("FAIL: aegis:audit refuses to produce a handoff from a REAL_RUNTIME control packet")
    current_release = _current_release(truth_root)
    actionable = packet.get("current_actionable_items") if isinstance(packet.get("current_actionable_items"), list) else []
    blocked = packet.get("blocked_items") if isinstance(packet.get("blocked_items"), list) else []
    do_not_claim = [str(item) for item in packet.get("do_not_claim", []) if str(item)] if isinstance(packet.get("do_not_claim"), list) else []
    next_actions = [str(item) for item in packet.get("next_operator_actions", []) if str(item)] if isinstance(packet.get("next_operator_actions"), list) else []
    known_blockers = _known_blockers(packet)

    sections = [
        "AEGIS AUDIT HANDOFF v1",
        f"day_utc: {day_utc}",
        f"truth_root: {truth_root}",
        f"control_packet_path: {packet_path}",
        "",
        "CONTROL PACKET SUMMARY",
        render_aegis_chatgpt_control_packet_summary_v1(packet),
        "",
        "AUDIT QUICK STATUS",
        f"runtime_truth_classification: {runtime_truth}",
        f"readiness_state: {_json_block(packet.get('readiness_state') or {})}",
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
    text = build_handoff_text_v1(packet=packet, packet_path=packet_path, truth_root=truth_root, day_utc=day_utc)
    out_path = audit_handoff_path_v1(truth_root=truth_root, day_utc=day_utc)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")

    actionable = packet.get("current_actionable_items") if isinstance(packet.get("current_actionable_items"), list) else []
    blocked = packet.get("blocked_items") if isinstance(packet.get("blocked_items"), list) else []
    do_not_claim = [str(item) for item in packet.get("do_not_claim", []) if str(item)] if isinstance(packet.get("do_not_claim"), list) else []
    next_actions = [str(item) for item in packet.get("next_operator_actions", []) if str(item)] if isinstance(packet.get("next_operator_actions"), list) else []
    print("AEGIS AUDIT HANDOFF")
    print(f"runtime_truth_classification: {packet.get('runtime_truth_classification') or 'UNKNOWN'}")
    print(f"readiness_state: {_json_block(packet.get('readiness_state') or {})}")
    print(f"actionable_items_count: {len(actionable)}")
    print(f"blocked_items_count: {len(blocked)}")
    print("do_not_claim:")
    for item in do_not_claim or ["NONE"]:
        print(f"- {item}")
    print("next_operator_actions:")
    for item in next_actions or ["NONE"]:
        print(f"- {item}")
    print(json.dumps({"handoff_path": str(out_path), "broker_submit_required": False, "ib_automation_required": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
