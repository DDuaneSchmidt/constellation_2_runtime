#!/usr/bin/env python3
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path("/home/node/constellation_2_runtime")
TRUTH_ROOT = REPO_ROOT / "constellation_2" / "runtime" / "truth"
SYSTEM_SNAPSHOT_ROOT = TRUTH_ROOT / "system_snapshot"

RUNTIME_STATE_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_runtime_state.v1.json"
ROOT_CAUSE_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_root_cause_report.v1.json"
OUTPUT_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_repair_plan.v1.json"


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"FAIL_CLOSED: cannot parse {path}: {exc}")


def ensure_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise SystemExit(f"FAIL_CLOSED: missing required {label}: {path}")


def action_for_root_cause(rc: dict[str, Any]) -> dict[str, Any]:
    code = rc.get("root_cause_code", "UNKNOWN")
    severity = rc.get("severity", "WARN")
    evidence = rc.get("evidence", [])

    if code == "DAY_ROLLOVER_MISMATCH":
        return {
            "root_cause_code": code,
            "priority": "P1",
            "blocking": True,
            "owner_surface": "paper-day orchestration / broker-event day binding",
            "repair_goal": "Make broker-event day advancement consistent with canonical operating day and upstream lifecycle advancement.",
            "recommended_actions": [
                "Prove how operating day is resolved in paper-day orchestration and broker-event capture.",
                "Verify whether broker-event capture is writing 2026-03-08 while intents/preflight remain on 2026-03-07.",
                "Stop any writer that advances broker-event day independently of canonical day resolution.",
                "Re-run lifecycle generation only after day binding is proven correct."
            ],
            "evidence": evidence,
            "stop_gate": "Do not treat the system as paper-trading ready while latest broker-event day exceeds upstream lifecycle day.",
            "severity": severity
        }

    if code == "EXECUTION_AUTHORITY_FAIL":
        return {
            "root_cause_code": code,
            "priority": "P1",
            "blocking": True,
            "owner_surface": "sleeve execution authority",
            "repair_goal": "Restore sleeve-scoped execution authority to PASS/DEGRADED with valid verdict lineage.",
            "recommended_actions": [
                "Inspect latest sleeve orchestrator verdict and stage failures in sleeve truth partition.",
                "Repair sleeve execution path first; do not rely on global monitoring status to override execution failure.",
                "Re-run sleeve orchestrator after correcting blocking stage failures."
            ],
            "evidence": evidence,
            "stop_gate": "Do not promote to paper-ready while sleeve execution authority is FAIL.",
            "severity": severity
        }

    if code == "BROKER_ONLY_ACTIVITY_WITHOUT_INTENT_LINEAGE":
        return {
            "root_cause_code": code,
            "priority": "P1",
            "blocking": True,
            "owner_surface": "execution evidence lineage / submission linkage",
            "repair_goal": "Restore governed submission lineage for the latest broker activity day.",
            "recommended_actions": [
                "Prove whether the broker events are real new events or passive observer carry-forward.",
                "Verify that submissions/submission_index/manifests exist for the same day as broker events when those events are trading-day relevant.",
                "If broker events are observer-only carry-forward, classify them separately so they do not become the latest operating day.",
                "If broker events are true execution-day evidence, regenerate or recover missing governed submission lineage."
            ],
            "evidence": evidence,
            "stop_gate": "Do not accept broker-day execution evidence as operationally complete without same-day governed lineage or explicit governed carry-forward classification.",
            "severity": severity
        }

    if code == "STALE_MONITORING":
        return {
            "root_cause_code": code,
            "priority": "P1",
            "blocking": False,
            "owner_surface": "monitoring / readiness pipeline",
            "repair_goal": "Bring monitoring and readiness up to the latest proven operating day.",
            "recommended_actions": [
                "Re-run lifecycle monitor for the latest valid operating day.",
                "Re-run paper readiness after lifecycle and reconciliation surfaces are current.",
                "Add a freshness guard so readiness cannot report OK while trailing latest lifecycle evidence."
            ],
            "evidence": evidence,
            "stop_gate": "Do not rely on paper_readiness OK when readiness day materially trails latest lifecycle evidence.",
            "severity": severity
        }

    if code == "INVALID_ARTIFACT_PREFERRED":
        return {
            "root_cause_code": code,
            "priority": "P1",
            "blocking": False,
            "owner_surface": "latest-file selection / artifact readers",
            "repair_goal": "Ensure INVALID artifacts never win latest-selection logic.",
            "recommended_actions": [
                "Patch stream readers to exclude filenames containing .INVALID_.",
                "Recompute latest pointers and latest-day inference after filtering invalid files.",
                "Add a fail-closed check in state-building tools whenever an INVALID artifact would be selected as latest."
            ],
            "evidence": evidence,
            "stop_gate": "Do not trust AI state inference until INVALID artifacts are excluded from latest selection.",
            "severity": severity
        }

    if code == "NON_AUTHORITATIVE_SURFACE_COMPETITION":
        return {
            "root_cause_code": code,
            "priority": "P2",
            "blocking": False,
            "owner_surface": "truth authority enforcement",
            "repair_goal": "Prevent non-authoritative execution surfaces from competing with authoritative truth.",
            "recommended_actions": [
                "Verify all readers are pinned to authoritative execution_evidence_v1/broker_events.",
                "Add or strengthen a gate that flags live use of execution_evidence_v2/broker_events.",
                "Confirm non-authoritative roots are not contributing to latest-day calculations."
            ],
            "evidence": evidence,
            "stop_gate": "Do not merge execution evidence from authoritative and non-authoritative roots.",
            "severity": severity
        }

    if code == "QUARANTINE_ACTIVITY_PRESENT":
        return {
            "root_cause_code": code,
            "priority": "P3",
            "blocking": False,
            "owner_surface": "runtime hygiene / quarantine governance",
            "repair_goal": "Determine whether quarantine roots reflect active unresolved issues or historical residue.",
            "recommended_actions": [
                "Review recent quarantine roots for unresolved same-day incidents.",
                "Confirm no AI/state reader consumes any quarantine path.",
                "Document whether each quarantine root is historical residue or active operational concern."
            ],
            "evidence": evidence,
            "stop_gate": "Do not use quarantine roots as canonical evidence.",
            "severity": severity
        }

    if code == "DOWNSTREAM_LEDGER_STALE":
        return {
            "root_cause_code": code,
            "priority": "P2",
            "blocking": False,
            "owner_surface": "fill ledger pipeline",
            "repair_goal": "Bring fill ledger up to latest valid lifecycle day.",
            "recommended_actions": [
                "Verify whether fill ledger day should advance to match latest valid execution day.",
                "Repair upstream lineage before regenerating the fill ledger if same-day execution lineage is broken.",
                "Add freshness checks linking fill ledger recency to authoritative broker/submission evidence."
            ],
            "evidence": evidence,
            "stop_gate": "Do not treat fills as current when fill ledger materially trails execution evidence.",
            "severity": severity
        }

    if code == "POSITION_STATE_STALE":
        return {
            "root_cause_code": code,
            "priority": "P2",
            "blocking": False,
            "owner_surface": "positions / position lifecycle pipeline",
            "repair_goal": "Bring positions and lifecycle truth up to the latest valid day.",
            "recommended_actions": [
                "Recompute position lifecycle after fill ledger and execution reconciliation are current.",
                "Regenerate positions snapshot from authoritative upstream truth only.",
                "Add freshness checks tying positions recency to fill ledger and lifecycle day."
            ],
            "evidence": evidence,
            "stop_gate": "Do not treat position state as current when it trails latest valid execution lifecycle.",
            "severity": severity
        }

    if code == "CAPITAL_AUTHORITY_STALE":
        return {
            "root_cause_code": code,
            "priority": "P3",
            "blocking": False,
            "owner_surface": "capital authority / allocation pipeline",
            "repair_goal": "Refresh capital authority surfaces to current valid operating context.",
            "recommended_actions": [
                "Verify whether capital authority should be same-day or previous-day by contract.",
                "If same-day required, regenerate capital authority and capital risk envelope.",
                "If previous-day is expected, encode that rule explicitly so AI diagnostics do not overstate the issue."
            ],
            "evidence": evidence,
            "stop_gate": "Do not assume capital authority is current without explicit freshness semantics.",
            "severity": severity
        }

    return {
        "root_cause_code": code,
        "priority": "P4",
        "blocking": False,
        "owner_surface": "UNKNOWN",
        "repair_goal": "Manual review required.",
        "recommended_actions": [
            "Review root cause evidence manually.",
            "Classify subsystem owner and freshness semantics.",
            "Add a dedicated classifier rule if this root cause recurs."
        ],
        "evidence": evidence,
        "stop_gate": "Unknown root cause remains unresolved.",
        "severity": severity
    }


def overall_status(actions: list[dict[str, Any]]) -> str:
    if any(a.get("blocking") for a in actions):
        return "BLOCKING"
    if any(a.get("severity") in {"ERROR", "WARN"} for a in actions):
        return "DEGRADED"
    return "OK"


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def main() -> None:
    ensure_exists(RUNTIME_STATE_PATH, "runtime state artifact")
    ensure_exists(ROOT_CAUSE_PATH, "root cause artifact")

    runtime_state = read_json(RUNTIME_STATE_PATH)
    root_cause_report = read_json(ROOT_CAUSE_PATH)

    root_causes = root_cause_report.get("root_causes", [])
    if not isinstance(root_causes, list):
        raise SystemExit("FAIL_CLOSED: root_causes is not a list")

    actions = [action_for_root_cause(rc) for rc in root_causes]

    blocking_actions = [a for a in actions if a.get("blocking")]
    priority_buckets = {
        "P1": [a["root_cause_code"] for a in actions if a["priority"] == "P1"],
        "P2": [a["root_cause_code"] for a in actions if a["priority"] == "P2"],
        "P3": [a["root_cause_code"] for a in actions if a["priority"] == "P3"],
        "P4": [a["root_cause_code"] for a in actions if a["priority"] == "P4"],
    }

    report = {
        "artifact_id": "constellation_repair_plan",
        "schema_version": "1.0",
        "generated_utc": now_utc(),
        "inputs": {
            "runtime_state": str(RUNTIME_STATE_PATH.relative_to(REPO_ROOT)),
            "root_cause_report": str(ROOT_CAUSE_PATH.relative_to(REPO_ROOT)),
        },
        "latest_operating_day": runtime_state.get("latest_operating_day"),
        "overall_status": overall_status(actions),
        "blocking_action_count": len(blocking_actions),
        "priority_buckets": priority_buckets,
        "recommended_execution_order": [
            "P1_blocking_integrity_repairs",
            "P1_state_reader_repairs",
            "P2_downstream_truth_repairs",
            "P3_hygiene_and_freshness_repairs",
            "P4_manual_followups"
        ],
        "actions": actions,
        "summary": {
            "highest_priority": "P1" if priority_buckets["P1"] else "P2" if priority_buckets["P2"] else "P3" if priority_buckets["P3"] else "P4",
            "top_blockers": [a["root_cause_code"] for a in blocking_actions],
            "paper_trade_ready_after_repairs": False if blocking_actions else None
        }
    }

    write_json(OUTPUT_PATH, report)
    print(f"WROTE: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
