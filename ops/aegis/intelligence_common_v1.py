from __future__ import annotations

import json
import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


REPORTS = "reports"
NO_LIVE_AI_MODEL = "DETERMINISTIC_FALLBACK_NO_LLM_RUNTIME"
LIVE_AI_PATTERNS = (
    "import openai",
    "from openai",
    "OpenAI(",
    "openai.",
    "import anthropic",
    "from anthropic",
    "Anthropic(",
    "anthropic.",
    "chat.completions",
    "responses.create",
    "completions.create",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
)


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json_v1(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def write_json_v1(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    from ops.aegis.producer_event_bridge_v1 import emit_evidence_events_for_artifact_v1

    emit_evidence_events_for_artifact_v1(
        artifact_path=path,
        payload=payload,
        producer="ops.aegis.intelligence_common_v1.write_json_v1",
        producer_version="write_json_v1",
    )
    return path


def latest_json_v1(truth_root: Path, family: str, day_utc: str, filename: str) -> tuple[Path | None, dict[str, Any]]:
    day_root = Path(truth_root).resolve() / REPORTS / family / day_utc
    if not day_root.exists():
        return None, {}
    candidates = sorted(day_root.rglob(filename), key=lambda path: path.stat().st_mtime if path.exists() else 0, reverse=True)
    if not candidates:
        return None, {}
    return candidates[0], read_json_v1(candidates[0])


def all_json_v1(truth_root: Path, filename: str, day_utc: str | None = None) -> list[tuple[Path, dict[str, Any]]]:
    root = Path(truth_root).resolve()
    out: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(root.rglob(filename)):
        if day_utc and day_utc not in path.parts:
            continue
        payload = read_json_v1(path)
        if payload:
            out.append((path, payload))
    return out


def report_ref_v1(truth_root: Path, family: str, day_utc: str, filename: str) -> dict[str, Any]:
    path, payload = latest_json_v1(truth_root, family, day_utc, filename)
    if not path:
        return {"status": "NOT_FOUND", "path": "", "summary": {}}
    summary = {
        key: payload.get(key)
        for key in (
            "status",
            "risk_state",
            "advisory_status",
            "target_mode_ready",
            "runtime_truth_classification",
            "highest_readiness_layer",
            "recommendation_count",
            "performance_attribution_engine_status",
            "opportunity_count",
            "sleeve_count",
            "attention_count",
            "evidence_quality",
            "confidence",
            "event_importance",
            "concentration_risk",
        )
        if key in payload
    }
    return {"status": "AVAILABLE", "path": str(path), "summary": summary}


def intelligence_summaries_v1(truth_root: Path, day_utc: str) -> dict[str, Any]:
    specs = {
        "evolution_engine": ("aegis_evolution_engine_v1", "evolution_engine.v1.json"),
        "research_priorities": ("aegis_research_priorities_v1", "prioritized_research_queue.v1.json"),
        "sleeve_attribution": ("aegis_sleeve_attribution_v1", "sleeve_attribution.v1.json"),
        "event_sleeve_activation_audit": ("aegis_event_sleeve_activation_audit_v1", "event_sleeve_activation_audit.v1.json"),
        "event_regime_trigger_evaluator": ("aegis_event_regime_trigger_evaluator_v1", "trigger_evaluation.v1.json"),
        "triggered_sleeve_runs": ("aegis_triggered_sleeve_runs_v1", "triggered_sleeve_runs.v1.json"),
        "event_regime_triggered_sleeve_run_audit": ("aegis_event_regime_triggered_sleeve_run_audit_v1", "event_regime_triggered_sleeve_run_audit.v1.json"),
        "candidate_lifecycle": ("aegis_candidate_lifecycle_v1", "candidate_lifecycle.v1.json"),
        "candidate_ranking": ("aegis_candidate_ranking_v1", "candidate_ranking.v1.json"),
        "regime_outcome_memory": ("aegis_regime_outcome_memory_v1", "regime_outcome_memory.v1.json"),
        "research_lab_execution_loop": ("aegis_research_lab_execution_loop_v1", "research_lab_execution_loop.v1.json"),
        "sleeve_challenger": ("aegis_sleeve_challenger_v1", "sleeve_challenger.v1.json"),
        "canonical_operator_state": ("aegis_canonical_operator_state_v1", "canonical_operator_state.v1.json"),
        "operator_brief": ("aegis_operator_brief_v1", "operator_brief.v1.json"),
        "operator_inbox": ("aegis_operator_inbox_v1", "operator_inbox.v1.json"),
        "risk_governance": ("aegis_risk_governance_v1", "risk_governance.v1.json"),
        "eod_intelligence": ("aegis_eod_intelligence_v1", "eod_intelligence.v1.json"),
        "eow_intelligence": ("aegis_eow_intelligence_v1", "eow_intelligence.v1.json"),
        "regime_detection": ("regime_detection_v1", "regime_detection.v1.json"),
        "capital_allocation_intelligence": ("capital_allocation_intelligence_v1", "capital_allocation_intelligence.v1.json"),
        "failure_analysis": ("failure_analysis_v1", "failure_analysis.v1.json"),
        "research_memory_graph": ("research_memory_graph_v1", "research_memory_graph.v1.json"),
        "cross_sleeve_interaction": ("cross_sleeve_interaction_v1", "cross_sleeve_interaction.v1.json"),
        "research_queue_optimizer": ("research_queue_optimizer_v1", "research_queue_optimizer.v1.json"),
        "event_interpretation": ("event_interpretation_v1", "event_interpretation.v1.json"),
        "adaptive_governance": ("adaptive_governance_v1", "adaptive_governance.v1.json"),
        "regime_context": ("regime_context_v1", "regime_context.v1.json"),
        "sleeve_performance_analytics": ("aegis_sleeve_performance_analytics_v1", "sleeve_performance_analytics.v1.json"),
        "cross_sleeve_analysis": ("cross_sleeve_analysis_v1", "cross_sleeve_analysis.v1.json"),
        "intelligence_governance_kernel": ("aegis_intelligence_governance_kernel_v1", "intelligence_governance_kernel.v1.json"),
    }
    return {key: report_ref_v1(truth_root, family, day_utc, filename) for key, (family, filename) in specs.items()}


def live_ai_call_path_found_v1(repo_root: Path) -> bool:
    roots = [Path(repo_root) / "ops", Path(repo_root) / "constellation_2"]
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts or path.name == "write_aegis_automation_ai_inventory_v1.py":
                continue
            text = _read_text(path)
            for line in text.splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or stripped[0] in {"'", '"'}:
                    continue
                if any(pattern in stripped for pattern in LIVE_AI_PATTERNS):
                    return True
    return False


def ai_evidence_v1(repo_root: Path) -> dict[str, Any]:
    live = live_ai_call_path_found_v1(repo_root)
    return {
        "ai_used": False if not live else False,
        "deterministic_fallback": True if not live else True,
        "model_used": NO_LIVE_AI_MODEL if not live else "UNKNOWN_LIVE_AI_PROVIDER_NOT_ENABLED_BY_THIS_REPORT",
        "live_ai_call_path_found": live,
    }


def systemd_timer_inventory_v1(repo_root: Path) -> dict[str, Any]:
    root = Path(repo_root) / "ops" / "systemd" / "user"
    timers: list[dict[str, Any]] = []
    if root.exists():
        for path in sorted(root.glob("*.timer")):
            text = _read_text(path)
            unit = path.name
            timers.append(
                {
                    "unit": unit,
                    "path": str(path.relative_to(repo_root)),
                    "calendar": re.findall(r"(?m)^OnCalendar=(.+)$", text),
                    "relative_schedule": [f"{key}={value}" for key, value in re.findall(r"(?m)^(OnBootSec|OnUnitActiveSec)=(.+)$", text)],
                    "service_unit": _first_match(text, r"(?m)^Unit=(.+)$") or unit.replace(".timer", ".service"),
                }
            )
    active = active_user_timers_v1()
    return {"repo_defined_timers": timers, "active_user_timers": active}


def active_user_timers_v1() -> list[dict[str, str]]:
    try:
        proc = subprocess.run(["systemctl", "--user", "list-timers", "--all", "--no-pager"], capture_output=True, text=True, check=False, timeout=5)
    except Exception:
        return []
    if proc.returncode != 0:
        return []
    rows: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        if ".timer" not in line or "ACTIVATES" in line:
            continue
        match = re.search(r"(?P<unit>\S+\.timer)\s+(?P<activates>\S+\.service)", line)
        if match:
            rows.append({"unit": match.group("unit"), "activates": match.group("activates"), "raw": line.strip()})
    return rows


def week_days_ending_v1(day_utc: str) -> set[str]:
    end = datetime.fromisoformat(day_utc).date()
    return {(end - timedelta(days=offset)).isoformat() for offset in range(0, 5)}


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _first_match(text: str, pattern: str) -> str:
    match = re.search(pattern, text)
    return match.group(1).strip() if match else ""
