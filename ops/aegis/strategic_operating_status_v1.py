from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1

STRATEGIC_SYSTEM_OF_RECORD = "Paper Trading + Hypothesis Validation Architecture"
LEGACY_COMPATIBILITY_LAYER = "Aegis Lite"


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def strategic_operating_status_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_strategic_operating_status_v1" / day_utc / "strategic_operating_status.v1.json"


def _latest(root: Path, family: str, day_utc: str, filename: str) -> Path | None:
    base = root / "reports" / family / day_utc
    matches = sorted(base.rglob(filename)) if base.exists() else []
    return matches[-1] if matches else None


def _read(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha(path: Path | None) -> str:
    if not path or not path.exists():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_ref(label: str, path: Path | None) -> dict[str, str]:
    return {"source_id": label, "path": str(path or ""), "sha256": _sha(path)}


def _summary(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("summary")
    return value if isinstance(value, dict) else {}


def _policy_state(operator_action: dict[str, Any], runtime_truth: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_advice_allowed": bool(operator_action.get("trade_advice_allowed", False) or runtime_truth.get("trade_advice_allowed", False)),
        "manual_trade_capture_allowed": bool(runtime_truth.get("manual_trade_capture_allowed", False)),
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
        "manual_execution_only": True,
        "broker_submit_required": False,
    }


def build_strategic_operating_status_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or now_utc_v1()
    paths = {
        "operator_action_model": _latest(root, "aegis_operator_action_model_v1", day_utc, "operator_action_model.v1.json"),
        "scheduled_run_registry": _latest(root, "aegis_scheduled_run_registry_v1", day_utc, "scheduled_run_registry.v1.json"),
        "scheduled_run_readiness_certificate": _latest(root, "aegis_scheduled_run_readiness_certificate_v1", day_utc, "readiness_certificate.v1.json"),
        "candidate_state": _latest(root, "aegis_candidate_state_v1", day_utc, "candidate_state.v1.json"),
        "paper_session_ledger": _latest(root, "aegis_paper_session_ledger_v1", day_utc, "paper_session_ledger.v1.json"),
        "outcome_registry": _latest(root, "aegis_outcome_registry_v1", day_utc, "outcome_registry.v1.json"),
        "validation_samples": _latest(root, "aegis_validation_samples_v1", day_utc, "validation_samples.v1.json"),
        "statistical_sufficiency": _latest(root, "aegis_statistical_sufficiency_v1", day_utc, "statistical_sufficiency.v1.json"),
        "research_portfolio": _latest(root, "aegis_research_portfolio_v1", day_utc, "research_portfolio.v1.json"),
        "research_capital_allocation": _latest(root, "aegis_research_capital_allocation_v1", day_utc, "research_capital_allocation.v1.json"),
        "verified_runtime_graph": _latest(root, "aegis_verified_runtime_graph_v1", day_utc, "verified_runtime_graph.v1.json"),
        "runtime_truth_kernel": _latest(root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json"),
    }
    data = {key: _read(path) for key, path in paths.items()}
    op_summary = _summary(data["operator_action_model"])
    sched_summary = _summary(data["scheduled_run_readiness_certificate"])
    candidate_status_counts = data["candidate_state"].get("status_counts") if isinstance(data["candidate_state"].get("status_counts"), dict) else {}
    queue_counts = data["candidate_state"].get("queue_status_counts") if isinstance(data["candidate_state"].get("queue_status_counts"), dict) else {}
    runtime_truth = data["runtime_truth_kernel"]
    payload: dict[str, Any] = {
        "schema_id": "aegis_strategic_operating_status",
        "schema_version": "v1",
        "artifact_id": "aegis_strategic_operating_status_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "strategic_system_of_record": STRATEGIC_SYSTEM_OF_RECORD,
        "legacy_compatibility_layer": LEGACY_COMPATIBILITY_LAYER,
        "operating_status": {
            "status": "MONITORING_ONLY" if not bool(op_summary.get("david_action_required", False)) else "NEEDS_OPERATOR_ACTION",
            "operator_summary": str(op_summary.get("top_level_summary") or "Monitoring only."),
            "explanation": str(op_summary.get("explanatory_sentence") or "Aegis is monitoring with execution disabled."),
            "aegis_can_operate": bool(op_summary.get("aegis_can_operate", False)),
            "runtime_truth_classification": str(runtime_truth.get("runtime_truth_classification") or "UNKNOWN"),
            "highest_readiness_layer": str(runtime_truth.get("highest_readiness_layer") or "UNKNOWN"),
        },
        "run_summary": {
            "last_completed_run": str((data["scheduled_run_registry"].get("last_completed_run") or {}).get("run_id") or ""),
            "next_scheduled_run_id": str(sched_summary.get("next_scheduled_run_id") or ""),
            "next_readiness_status": str(sched_summary.get("next_readiness_status") or "UNKNOWN"),
            "blocked_scheduled_run_count": int(sched_summary.get("blocked_count") or 0),
        },
        "candidate_generation_result": {
            "candidate_count": int(data["candidate_state"].get("candidate_count") or 0),
            "active_candidate_count": int(data["candidate_state"].get("active_candidate_count") or 0),
            "current_day_candidate_count": int(op_summary.get("current_day_candidate_count") or 0),
            "status_counts": candidate_status_counts,
            "queue_status_counts": queue_counts,
            "trade_advice_allowed": False,
        },
        "paper_monitoring_result": {
            "open_paper_position_count": int(op_summary.get("open_paper_position_count") or 0),
            "closed_paper_position_count": int(op_summary.get("closed_paper_position_count") or 0),
            "paper_session_id": str(data["paper_session_ledger"].get("paper_session_id") or ""),
            "paper_only": True,
        },
        "outcome_validation_state": {
            "open_outcome_count": int(_summary(data["outcome_registry"]).get("open_outcome_count") or _summary(data.get("outcome_validation", {})).get("open_outcome_count") or 0),
            "validation_sample_count": int(op_summary.get("validation_sample_count") or 0),
            "statistical_sufficiency_status": str(data["statistical_sufficiency"].get("status") or data["statistical_sufficiency"].get("sufficiency_status") or "UNKNOWN"),
        },
        "research_allocation_state": {
            "research_summary": _summary(data["research_portfolio"]),
            "capital_allocation_summary": _summary(data["research_capital_allocation"]),
        },
        "operator_action_state": {
            "david_action_required": bool(op_summary.get("david_action_required", False)),
            "top_level_summary": str(op_summary.get("top_level_summary") or "Monitoring only."),
            "current_actionable_items": int(runtime_truth.get("actionable_items_count") or 0),
            "blocked_items": int(runtime_truth.get("blocked_items_count") or 0),
        },
        "policy_state": _policy_state(data["operator_action_model"], runtime_truth),
        "source_artifacts": [_source_ref(key, path) for key, path in sorted(paths.items())],
        "source_artifact_hashes": {key: _sha(path) for key, path in sorted(paths.items()) if path},
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def write_strategic_operating_status_v1(*, truth_root: Path | str, payload: dict[str, Any]) -> Path:
    path = strategic_operating_status_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_and_write_strategic_operating_status_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> tuple[dict[str, Any], Path]:
    payload = build_strategic_operating_status_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    return payload, write_strategic_operating_status_v1(truth_root=truth_root, payload=payload)
