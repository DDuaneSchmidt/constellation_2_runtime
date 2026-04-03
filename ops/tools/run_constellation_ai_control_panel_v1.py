#!/usr/bin/env python3
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path("/home/node/constellation_2_runtime")
SYSTEM_SNAPSHOT_ROOT = REPO_ROOT / "constellation_2" / "runtime" / "truth" / "system_snapshot"

SYSTEM_SNAPSHOT_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_system_snapshot.v1.json"
ARCH_INDEX_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_ai_architecture_index.v1.json"
CAPABILITY_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_capability_manifest.v1.json"
RUNTIME_STATE_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_runtime_state.v1.json"
ROOT_CAUSE_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_root_cause_report.v1.json"
REPAIR_PLAN_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_repair_plan.v1.json"
DIAGNOSTICS_MEMO_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_diagnostics_memo.v1.md"
BUG_METRICS_PATH = REPO_ROOT / "constellation_2" / "runtime" / "truth" / "readiness_v1" / "constellation_bug_metrics_v1" / "latest_pointer.v1.json"
PLATFORM_READINESS_PATH = REPO_ROOT / "constellation_2" / "runtime" / "truth" / "readiness_v1" / "constellation_platform_readiness_v1" / "latest_pointer.v1.json"

AI_REASONING_CONTRACT_PATH = REPO_ROOT / "governance" / "contracts" / "constellation_ai_reasoning_contract.v1.md"
SYSTEM_INVARIANTS_PATH = REPO_ROOT / "governance" / "contracts" / "constellation_system_invariants.v1.md"

OUTPUT_PATH = SYSTEM_SNAPSHOT_ROOT / "constellation_ai_control_panel.v1.json"


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise SystemExit(f"FAIL_CLOSED: missing required {label}: {path}")


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"FAIL_CLOSED: cannot parse json {path}: {exc}")


def rel(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


def main() -> None:
    required = [
        (SYSTEM_SNAPSHOT_PATH, "system snapshot"),
        (ARCH_INDEX_PATH, "architecture index"),
        (CAPABILITY_PATH, "capability manifest"),
        (RUNTIME_STATE_PATH, "runtime state"),
        (ROOT_CAUSE_PATH, "root cause report"),
        (REPAIR_PLAN_PATH, "repair plan"),
        (DIAGNOSTICS_MEMO_PATH, "diagnostics memo"),
        (BUG_METRICS_PATH, "platform bug metrics pointer"),
        (PLATFORM_READINESS_PATH, "platform readiness pointer"),
        (AI_REASONING_CONTRACT_PATH, "AI reasoning contract"),
        (SYSTEM_INVARIANTS_PATH, "system invariants contract"),
    ]
    for path, label in required:
        ensure_exists(path, label)

    system_snapshot = read_json(SYSTEM_SNAPSHOT_PATH)
    arch_index = read_json(ARCH_INDEX_PATH)
    capability_manifest = read_json(CAPABILITY_PATH)
    runtime_state = read_json(RUNTIME_STATE_PATH)
    root_cause_report = read_json(ROOT_CAUSE_PATH)
    repair_plan = read_json(REPAIR_PLAN_PATH)

    repair_summary = repair_plan.get("summary", {})
    top_blockers = repair_summary.get("top_blockers", [])
    if not isinstance(top_blockers, list):
        top_blockers = []
    top_blockers_detail = repair_summary.get("top_blockers_detail", [])
    if not isinstance(top_blockers_detail, list):
        top_blockers_detail = []
    blocker_counts_by_class = repair_plan.get("blocker_counts_by_class", {})
    if not isinstance(blocker_counts_by_class, dict):
        blocker_counts_by_class = {}

    data = {
        "artifact_id": "constellation_ai_control_panel",
        "schema_version": "1.0",
        "generated_utc": now_utc(),
        "repo_root": str(REPO_ROOT),
        "canonical_truth_root": str(REPO_ROOT / "constellation_2" / "runtime" / "truth"),
        "control_panel_purpose": "Single AI entrypoint for loading Constellation architecture, runtime state, diagnostics, and repair context.",
        "load_order": [
            {
                "order": 1,
                "artifact_id": "constellation_system_snapshot",
                "path": rel(SYSTEM_SNAPSHOT_PATH),
                "purpose": "Canonical architecture snapshot"
            },
            {
                "order": 2,
                "artifact_id": "constellation_ai_architecture_index",
                "path": rel(ARCH_INDEX_PATH),
                "purpose": "Machine-readable architecture location map"
            },
            {
                "order": 3,
                "artifact_id": "constellation_capability_manifest",
                "path": rel(CAPABILITY_PATH),
                "purpose": "Proven system capabilities"
            },
            {
                "order": 4,
                "artifact_id": "constellation_runtime_state",
                "path": rel(RUNTIME_STATE_PATH),
                "purpose": "Current system operating state"
            },
            {
                "order": 5,
                "artifact_id": "constellation_root_cause_report",
                "path": rel(ROOT_CAUSE_PATH),
                "purpose": "Current root-cause classifications"
            },
            {
                "order": 6,
                "artifact_id": "constellation_repair_plan",
                "path": rel(REPAIR_PLAN_PATH),
                "purpose": "Prioritized repair actions"
            },
            {
                "order": 7,
                "artifact_id": "constellation_diagnostics_memo",
                "path": rel(DIAGNOSTICS_MEMO_PATH),
                "purpose": "Human-readable summary"
            }
        ],
        "governance_inputs": [
            {
                "artifact_id": "constellation_ai_reasoning_contract",
                "path": rel(AI_REASONING_CONTRACT_PATH),
                "purpose": "AI reasoning rules"
            },
            {
                "artifact_id": "constellation_system_invariants",
                "path": rel(SYSTEM_INVARIANTS_PATH),
                "purpose": "Core system invariants"
            }
        ],
        "current_status": {
            "overall_health": runtime_state.get("system_status", {}).get("overall_health", "UNKNOWN"),
            "overall_status": runtime_state.get("system_status", {}).get("overall_status", "UNKNOWN"),
            "paper_trading_ready_status": runtime_state.get("system_status", {}).get("paper_trading_ready_status", "UNKNOWN"),
            "operator_daily_gate_status": runtime_state.get("system_status", {}).get("operator_daily_gate_status", "UNKNOWN"),
            "gate_stack_verdict_status": runtime_state.get("system_status", {}).get("gate_stack_verdict_status", "UNKNOWN"),
            "latest_operating_day": runtime_state.get("latest_operating_day", "UNKNOWN"),
            "latest_execution_day": runtime_state.get("latest_execution_day", "UNKNOWN"),
            "scope_health": runtime_state.get("scope_health", {}),
            "top_blockers": top_blockers,
            "top_blockers_detail": top_blockers_detail,
            "blocker_counts_by_class": blocker_counts_by_class,
            "root_cause_count": root_cause_report.get("root_cause_count", "UNKNOWN"),
            "blocking_action_count": repair_plan.get("blocking_action_count", "UNKNOWN"),
            "execution_blocking_action_count": repair_plan.get("execution_blocking_action_count", "UNKNOWN"),
            "platform_bug_metrics_pointer": rel(BUG_METRICS_PATH),
            "platform_readiness_pointer": rel(PLATFORM_READINESS_PATH),
        },
        "system_identity": system_snapshot.get("system_identity", {}),
        "known_sleeves": system_snapshot.get("sleeves", []),
        "known_engines": system_snapshot.get("engines", []),
        "entry_instructions": {
            "ai_must_load_in_order": True,
            "ai_must_fail_closed_if_missing": True,
            "ai_must_consult_governance_inputs": True,
            "ai_must_distinguish_proven_vs_unknown": True
        },
        "recommended_prompt_prefix": (
            "Read constellation_ai_control_panel.v1.json first, follow its load_order exactly, "
            "then assess Constellation using the referenced governance inputs."
        ),
        "source_artifact_generated_utc": {
            "system_snapshot": system_snapshot.get("generated_utc"),
            "architecture_index": arch_index.get("generated_utc"),
            "capability_manifest": capability_manifest.get("generated_utc"),
            "runtime_state": runtime_state.get("generated_utc"),
            "root_cause_report": root_cause_report.get("generated_utc"),
            "repair_plan": repair_plan.get("generated_utc"),
        }
    }

    OUTPUT_PATH.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    print(f"WROTE: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
