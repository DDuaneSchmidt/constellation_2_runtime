from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1

REPORT_FAMILY = "aegis_mode_readiness_v1"
REPORT_FILENAME = "mode_readiness.v1.json"
ACTIVE_MODE = "HUMAN_REVIEWED_PAPER_MODE"
MODES = [
    "HUMAN_REVIEWED_PAPER_MODE",
    "CANDIDATE_VISIBILITY_ONLY",
    "EOD_ADVISORY_MODE",
    "STRICT_CURRENT_SESSION_MODE",
    "FULL_PLATFORM_MODE",
]
POLICY_DISABLED = ["TRADE_ADVICE_ALLOWED", "BROKER_SUBMIT_TRANSMIT", "AUTONOMOUS_EXECUTION_ALLOWED", "LIVE_TRADING"]


@dataclass(frozen=True)
class Requirement:
    artifact_id: str
    expected_path: str
    required: bool = True
    conditional: str = ""
    warning_only: bool = False


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def mode_readiness_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_mode_readiness_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    generated_at_utc: str | None = None,
    verified_graph: dict[str, Any] | None = None,
    global_missing_or_stale_sources: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or now_utc_v1()
    graph = verified_graph if isinstance(verified_graph, dict) else _read_latest_verified_graph(root, day_utc)
    graph_status = str(graph.get("graph_status") or "UNKNOWN")
    global_blockers = global_missing_or_stale_sources if isinstance(global_missing_or_stale_sources, list) else []
    mode_rows = []
    for mode in MODES:
        mode_rows.append(_build_mode_row(root=root, day_utc=day_utc, mode=mode, graph_status=graph_status, global_blockers=global_blockers))
    by_mode = {row["mode"]: row for row in mode_rows}
    active = by_mode[ACTIVE_MODE]
    return {
        "schema_id": "aegis_mode_readiness",
        "schema_version": "v1",
        "artifact_id": "aegis_mode_readiness_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "truth_root": str(root),
        "verified_graph_status": graph_status,
        "active_mode": ACTIVE_MODE,
        "active_mode_readiness_status": active["readiness_status"],
        "modes": mode_rows,
        "modes_by_id": by_mode,
        "policy_disabled_capabilities": POLICY_DISABLED,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "live_trading_allowed": False,
    }


def write_mode_readiness_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    path = write_json_v1(mode_readiness_path_v1(truth_root=truth_root, day_utc=day_utc), payload)
    return {"json": str(path)}


def build_and_write_mode_readiness_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> tuple[dict[str, Any], dict[str, str]]:
    payload = build_mode_readiness_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    return payload, write_mode_readiness_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)


def _build_mode_row(*, root: Path, day_utc: str, mode: str, graph_status: str, global_blockers: list[dict[str, Any]]) -> dict[str, Any]:
    requirements = _requirements_for_mode(mode, day_utc)
    evaluations = [_evaluate_requirement(root, day_utc, req) for req in requirements]
    if mode == "FULL_PLATFORM_MODE":
        blocking = [_global_blocker_row(row) for row in global_blockers]
        warning = []
    else:
        blocking = [row for row in evaluations if row["requirement_class"] == "BLOCKING" and row["status"] not in {"OK", "SATISFIED_BY_CARRIED_FORWARD_CANDIDATES"}]
        warning = [row for row in evaluations if row["requirement_class"] == "WARNING" and row["status"] != "OK"]
    graph_blocked = graph_status != "READY"
    if graph_blocked:
        blocking.append({"artifact_id": "verified_runtime_graph", "status": graph_status or "UNKNOWN", "reason": "Verified evidence graph is not READY.", "path": "reports/aegis_verified_runtime_graph_v1/{day}/verified_runtime_graph.v1.json".format(day=day_utc)})
    readiness_status = "READY"
    if blocking:
        readiness_status = "BLOCKED"
    elif warning:
        readiness_status = "DEGRADED_READY"
    return {
        "mode": mode,
        "required_artifacts": [req.artifact_id for req in requirements if req.required and not req.warning_only],
        "optional_artifacts": [req.artifact_id for req in requirements if not req.required or req.warning_only],
        "forbidden_capabilities": POLICY_DISABLED,
        "allowed_capabilities": _allowed_capabilities_for_mode(mode) if not blocking else [],
        "blocking_requirements": blocking,
        "warning_requirements": warning,
        "artifact_evaluations": evaluations,
        "readiness_status": readiness_status,
        "policy_gates": {
            "trade_advice_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "live_trading_allowed": False,
        },
    }


def _requirements_for_mode(mode: str, day_utc: str) -> list[Requirement]:
    paper = [
        Requirement("candidate_contracts", f"reports/aegis_candidate_contracts_v1/{day_utc}/candidate_contracts.v1.json", conditional="required unless carried-forward reviewable candidates exist"),
        Requirement("paper_review_queue", f"reports/aegis_paper_review_queue_v1/{day_utc}/paper_review_queue.v1.json"),
        Requirement("paper_position_ledger", f"reports/aegis_paper_position_ledger_v1/{day_utc}/paper_position_ledger.v1.json"),
        Requirement("paper_position_events", f"reports/aegis_paper_position_events_v1/{day_utc}/paper_position_events.v1.jsonl"),
        Requirement("paper_trade_outcomes", f"reports/aegis_paper_trade_outcomes_v1/{day_utc}/paper_trade_outcomes.v1.json"),
        Requirement("exit_recommendations", f"reports/aegis_exit_recommendations_v1/{day_utc}/exit_recommendations.v1.json"),
        Requirement("context_requirement_profile", f"reports/aegis_context_requirement_profile_v1/{day_utc}/context_requirement_profile.v1.json"),
        Requirement("market_data_coverage", f"reports/aegis_market_data_coverage_v1/{day_utc}/market_data_coverage.v1.json"),
        Requirement("canonical_operator_state", f"reports/aegis_canonical_operator_state_v1/{day_utc}/canonical_operator_state.v1.json"),
        Requirement("sleeve_performance_truth", f"reports/aegis_sleeve_performance_truth_v1/{day_utc}/sleeve_performance_truth.v1.json", required=False, warning_only=True),
    ]
    if mode == "HUMAN_REVIEWED_PAPER_MODE":
        return paper
    if mode == "CANDIDATE_VISIBILITY_ONLY":
        return [
            Requirement("candidate_contracts", f"reports/aegis_candidate_contracts_v1/{day_utc}/candidate_contracts.v1.json", conditional="required unless carried-forward reviewable candidates exist"),
            Requirement("market_data_coverage", f"reports/aegis_market_data_coverage_v1/{day_utc}/market_data_coverage.v1.json"),
            Requirement("canonical_operator_state", f"reports/aegis_canonical_operator_state_v1/{day_utc}/canonical_operator_state.v1.json"),
        ]
    if mode == "EOD_ADVISORY_MODE":
        return paper + [Requirement("aegis_research_eod_summary", f"reports/aegis_research_eod_summary_v1/{day_utc}/research_eod_summary.v1.json")]
    if mode == "STRICT_CURRENT_SESSION_MODE":
        return paper + [Requirement("event_market_snapshot", f"reports/event_market_snapshot_v1/{day_utc}/event_market_snapshot.v1.json")]
    return []


def _allowed_capabilities_for_mode(mode: str) -> list[str]:
    if mode == "HUMAN_REVIEWED_PAPER_MODE":
        return ["CANDIDATE_VISIBILITY", "PAPER_REVIEW_QUEUE", "PAPER_POSITION_TRACKING", "EXIT_REVIEW"]
    if mode == "CANDIDATE_VISIBILITY_ONLY":
        return ["CANDIDATE_VISIBILITY"]
    if mode == "EOD_ADVISORY_MODE":
        return ["CANDIDATE_VISIBILITY", "EOD_ADVISORY_REVIEW"]
    if mode == "STRICT_CURRENT_SESSION_MODE":
        return ["CANDIDATE_VISIBILITY", "CURRENT_SESSION_CONTEXT_REVIEW"]
    if mode == "FULL_PLATFORM_MODE":
        return ["FULL_PLATFORM_READINESS"]
    return []


def _evaluate_requirement(root: Path, day_utc: str, req: Requirement) -> dict[str, Any]:
    path = _resolve_path(root, req.expected_path)
    base = {
        "artifact_id": req.artifact_id,
        "expected_path": req.expected_path,
        "path": str(path) if path else "",
        "required": req.required,
        "conditional": req.conditional,
        "requirement_class": "WARNING" if req.warning_only or not req.required else "BLOCKING",
    }
    if req.artifact_id == "candidate_contracts" and not path and _has_carried_forward_reviewable_candidates(root=root, day_utc=day_utc):
        return {**base, "status": "SATISFIED_BY_CARRIED_FORWARD_CANDIDATES", "reason": "Candidate contracts are absent, but carried-forward reviewable candidates exist."}
    if not path:
        return {**base, "status": "MISSING", "reason": "Expected artifact was not found."}
    if path.suffix == ".jsonl":
        return {**base, "status": "OK", "reason": "Artifact exists."}
    payload = read_json_v1(path)
    if not payload:
        return {**base, "status": "INVALID", "reason": "Artifact is not parseable JSON."}
    if str(payload.get("day_utc") or payload.get("date") or day_utc)[:10] not in {"", day_utc}:
        return {**base, "status": "INVALID", "reason": "Artifact day does not match target day."}
    if req.artifact_id == "market_data_coverage" and str(payload.get("status") or "").upper() != "READY":
        return {**base, "status": "INVALID", "reason": "Market data coverage is not READY."}
    return {**base, "status": "OK", "reason": "Artifact exists and satisfies mode requirement.", "artifact_hash": _sha256(path)}


def _resolve_path(root: Path, expected_path: str) -> Path | None:
    pattern = str(root / expected_path)
    matches = sorted(Path(item) for item in glob_paths(pattern))
    return matches[-1] if matches else None


def glob_paths(pattern: str) -> list[str]:
    import glob

    return glob.glob(pattern, recursive=True)


def _has_carried_forward_reviewable_candidates(*, root: Path, day_utc: str) -> bool:
    queue_path = _resolve_path(root, f"reports/aegis_paper_review_queue_v1/{day_utc}/paper_review_queue.v1.json")
    queue = read_json_v1(queue_path) if queue_path else {}
    for key in ("candidate_count", "reviewable_candidate_count"):
        try:
            if int(queue.get(key) or 0) > 0:
                return True
        except Exception:
            pass
    counts = queue.get("status_counts") if isinstance(queue.get("status_counts"), dict) else {}
    return any(int(counts.get(key) or 0) > 0 for key in ("AWAITING_REVIEW", "PAPER_POSITION_OPEN", "REVIEWABLE", "CARRIED_FORWARD"))


def _global_blocker_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": str(row.get("artifact_id") or ""),
        "status": str(row.get("status") or ""),
        "path": str(row.get("path") or row.get("expected_path") or ""),
        "expected_path": str(row.get("expected_path") or ""),
        "reason": str(row.get("reason") or ""),
    }


def _read_latest_verified_graph(root: Path, day_utc: str) -> dict[str, Any]:
    return read_json_v1(root / "reports" / "aegis_verified_runtime_graph_v1" / day_utc / "verified_runtime_graph.v1.json")


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except Exception:
        return ""
