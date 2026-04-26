from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .research_event_bus_v1 import resolve_runtime_root
from .research_event_v1 import (
    build_research_event_v1,
    utc_now_iso,
    validate_ai_result_review_v1,
    validate_sandbox_result_v1,
)


DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth").resolve()
DEFAULT_EXECUTION_ROOT = Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER").resolve()
DEFAULT_AEGIS_PACKET_PATH = Path(
    "/home/node/constellation_runtime_data/exports/aegis_state/latest/chatgpt_aegis_packet.md"
).resolve()
TRUTH_ROOT_ENV = "CONSTELLATION_TRUTH_ROOT"
EXECUTION_ROOT_ENV = "CONSTELLATION_EXECUTION_ROOT"


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _canonical_write_json(path: Path, payload: Any) -> Path:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return target


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON_NOT_OBJECT:{path}")
    return payload


def resolve_truth_root(explicit_truth_root: str | Path | None = None) -> Path:
    import os

    override = str(explicit_truth_root or "").strip() or str(os.getenv(TRUTH_ROOT_ENV, "")).strip()
    return Path(override).expanduser().resolve() if override else DEFAULT_TRUTH_ROOT


def resolve_execution_root(explicit_execution_root: str | Path | None = None) -> Path:
    import os

    override = str(explicit_execution_root or "").strip() or str(os.getenv(EXECUTION_ROOT_ENV, "")).strip()
    return Path(override).expanduser().resolve() if override else DEFAULT_EXECUTION_ROOT


def _market_calendar_path(truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "market_calendar_v1" / "NYSE" / f"{day_utc[:4]}.jsonl").resolve()


def is_trading_day_v1(*, day_utc: str, truth_root: Path) -> tuple[bool, str]:
    normalized_day = str(day_utc).strip()
    try:
        parsed = date.fromisoformat(normalized_day)
    except ValueError as exc:
        raise ValueError(f"INVALID_DAY_UTC:{normalized_day}") from exc

    calendar_path = _market_calendar_path(truth_root, normalized_day)
    if calendar_path.exists() and calendar_path.is_file():
        for raw in calendar_path.read_text(encoding="utf-8").splitlines():
            text = str(raw).strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            if str(row.get("day_utc") or "").strip() != normalized_day:
                continue
            value = row.get("is_trading_session")
            if isinstance(value, bool):
                return value, str(calendar_path)
    return parsed.weekday() < 5, str(calendar_path)


def _default_day_paths(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path,
    aegis_packet_path: Path,
) -> dict[str, Path]:
    return {
        "aegis_packet": aegis_packet_path,
        "closure_authority": (
            truth_root / "reports" / "aegis_day_closure_authority_v1" / day_utc / "aegis_day_closure_authority.v1.json"
        ).resolve(),
        "submit_boundary": (
            truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"
        ).resolve(),
        "current_head": (
            execution_root / "execution_evidence_v1" / "current_head" / day_utc / "current_head.v1.json"
        ).resolve(),
        "submission_index": (
            execution_root / "submission_index_v1" / day_utc / "submission_index.v1.json"
        ).resolve(),
    }


def _extract_surface_path(closure_payload: dict[str, Any], key: str) -> Path | None:
    surfaces = closure_payload.get("source_surfaces") if isinstance(closure_payload.get("source_surfaces"), dict) else {}
    row = surfaces.get(key) if isinstance(surfaces, dict) else None
    if not isinstance(row, dict):
        return None
    path_text = str(row.get("path") or "").strip()
    if not path_text:
        return None
    return Path(path_text).expanduser().resolve()


def _execution_evidence_exists(*, day_utc: str, execution_root: Path, paths: dict[str, Path]) -> bool:
    latest_pointer = (execution_root / "execution_evidence_v1" / "latest_pointer.v1.json").resolve()
    if latest_pointer.exists() and latest_pointer.is_file():
        return True
    if paths["current_head"].exists() and paths["current_head"].is_file():
        return True
    if paths["submission_index"].exists() and paths["submission_index"].is_file():
        return True
    stream_day = (execution_root / "execution_stream_v1").resolve()
    if stream_day.exists() and stream_day.is_dir():
        for candidate in stream_day.rglob(f"*/{day_utc}"):
            if candidate.exists():
                return True
    return False


def _optional_ib_report_path(day_utc: str, truth_root: Path) -> Path | None:
    candidates = [
        truth_root / "reports" / "broker_reconciliation_v1" / day_utc / "broker_reconciliation.v1.json",
        truth_root / "reports" / "ib_api_handshake_spine_v1" / day_utc / "ib_api_handshake_spine.v1.json",
        truth_root / "reports" / "operator_trade_health_v1" / day_utc / "operator_trade_health.v1.json",
    ]
    for path in candidates:
        resolved = path.resolve()
        if resolved.exists() and resolved.is_file():
            return resolved
    return None


def evaluate_trading_day_closed_trigger_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path,
    aegis_packet_path: Path,
) -> dict[str, Any]:
    trading_day, calendar_path = is_trading_day_v1(day_utc=day_utc, truth_root=truth_root)
    if not trading_day:
        return {
            "status": "SKIPPED",
            "reason": "NON_TRADING_DAY",
            "day_utc": day_utc,
            "calendar_path": calendar_path,
        }

    paths = _default_day_paths(
        day_utc=day_utc,
        truth_root=truth_root,
        execution_root=execution_root,
        aegis_packet_path=aegis_packet_path,
    )

    missing: list[str] = []
    source_artifacts: list[str] = []

    for logical in ("aegis_packet", "closure_authority", "submit_boundary"):
        path = paths[logical]
        if not path.exists() or not path.is_file():
            missing.append(f"MISSING_ARTIFACT:{logical}:{path}")
        else:
            source_artifacts.append(str(path))

    closure_payload: dict[str, Any] = {}
    if paths["closure_authority"].exists() and paths["closure_authority"].is_file():
        try:
            closure_payload = _read_json(paths["closure_authority"])
        except Exception:
            closure_payload = {}

    surfaced_current_head = _extract_surface_path(closure_payload, "execution_current_head")
    surfaced_submission_index = _extract_surface_path(closure_payload, "submission_index")
    if surfaced_current_head is not None:
        paths["current_head"] = surfaced_current_head
    if surfaced_submission_index is not None:
        paths["submission_index"] = surfaced_submission_index

    evidence_exists = _execution_evidence_exists(day_utc=day_utc, execution_root=execution_root, paths=paths)
    if evidence_exists:
        for logical in ("current_head", "submission_index"):
            path = paths[logical]
            if not path.exists() or not path.is_file():
                missing.append(f"MISSING_ARTIFACT:{logical}:{path}")
            else:
                source_artifacts.append(str(path))

    ib_path = _optional_ib_report_path(day_utc, truth_root)
    if ib_path is not None:
        source_artifacts.append(str(ib_path))

    if missing:
        return {
            "status": "FAILED",
            "reason": missing[0],
            "missing": missing,
            "day_utc": day_utc,
            "source_artifacts": sorted(set(source_artifacts)),
        }

    event = build_research_event_v1(
        event_type="TRADING_DAY_CLOSED",
        day_utc=day_utc,
        source="AEGIS_CORE",
        source_artifacts=sorted(set(source_artifacts)),
        requires_ai_review=True,
    )
    return {
        "status": "READY",
        "day_utc": day_utc,
        "event": event,
        "source_artifacts": sorted(set(source_artifacts)),
    }


def _research_runtime_root() -> Path:
    root = resolve_runtime_root()
    if _is_under(root, Path(__file__).resolve().parents[2]):
        raise ValueError(f"RESEARCH_RUNTIME_ROOT_UNDER_REPO_FORBIDDEN:{root}")
    return root


def _find_optional_idea_artifact(idea_id: str) -> Path | None:
    root = _research_runtime_root()
    patterns = [
        root / "ideas" / "validated" / f"{idea_id}.edge_hypothesis.v1.json",
        root / "ideas" / "rejected" / f"{idea_id}.edge_hypothesis.v1.json",
        root / "ideas" / "archived" / f"{idea_id}.edge_hypothesis.v1.json",
        root / "ideas" / "proposed" / f"{idea_id}.edge_hypothesis.v1.json",
    ]
    for path in patterns:
        if path.exists() and path.is_file():
            return path.resolve()
    return None


def _find_optional_test_plan_artifact(test_id: str) -> Path | None:
    root = _research_runtime_root()
    for bucket in ("queued", "running", "completed", "failed"):
        candidate = root / "tests" / bucket / f"{test_id}.sandbox_test_plan.v1.json"
        if candidate.exists() and candidate.is_file():
            return candidate.resolve()
    return None


def build_sandbox_result_completed_event_v1(
    *,
    result_payload: dict[str, Any],
    result_json_path: Path,
) -> dict[str, Any]:
    result = validate_sandbox_result_v1(result_payload)
    source_artifacts = [str(result_json_path.expanduser().resolve())]

    idea_path = _find_optional_idea_artifact(str(result["idea_id"]))
    if idea_path is not None:
        source_artifacts.append(str(idea_path))

    test_plan_path = _find_optional_test_plan_artifact(str(result["test_id"]))
    if test_plan_path is not None:
        source_artifacts.append(str(test_plan_path))

    return build_research_event_v1(
        event_type="SANDBOX_RESULT_COMPLETED",
        day_utc=str(result.get("generated_utc") or "")[:10],
        source="RESEARCH_LAB",
        source_artifacts=sorted(set(source_artifacts)),
        requires_ai_review=True,
    )


def evaluate_sandbox_result_for_ai_review_v1(result_payload: dict[str, Any]) -> dict[str, Any]:
    result = validate_sandbox_result_v1(result_payload)
    metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}

    sample_size = int(result.get("sample_size") or 0)
    cost_status = str(result.get("cost_model_status") or "").strip().upper()
    slippage_status = str(result.get("slippage_model_status") or "").strip().upper()
    overfitting_risk = str(metrics.get("overfitting_risk") or "LOW").strip().upper()
    data_quality_risk = str(metrics.get("data_quality_risk") or "LOW").strip().upper()
    walk_forward_passed = metrics.get("walk_forward_passed")
    walk_forward_inconclusive = bool(metrics.get("walk_forward_inconclusive") is True or walk_forward_passed is None)

    findings: list[str] = []
    if sample_size < 50:
        findings.append("SAMPLE_SIZE_BELOW_THRESHOLD")
    if cost_status not in {"PRESENT", "PASS", "OK", "INCLUDED"}:
        findings.append("COST_MODEL_MISSING")
    if slippage_status not in {"PRESENT", "PASS", "OK", "INCLUDED"}:
        findings.append("SLIPPAGE_MODEL_MISSING")
    if overfitting_risk == "HIGH":
        findings.append("OVERFITTING_RISK_HIGH")

    status = str(result.get("status") or "").strip().upper()
    if findings:
        review_status = "AUTO_REJECTED"
        primary_reason = findings[0]
        action_required_for_human = False
        recommended_next_action = "Reject result and retain evidence"
    elif status == "INCONCLUSIVE" or walk_forward_inconclusive:
        review_status = "RETEST"
        primary_reason = "WALK_FORWARD_INCONCLUSIVE"
        action_required_for_human = False
        recommended_next_action = "Queue retest with expanded sample"
    elif status == "PASS" and walk_forward_passed is True and sample_size >= 100:
        review_status = "PAPER_CANDIDATE_RECOMMENDED"
        primary_reason = "SANDBOX_PASS_WITH_WALK_FORWARD"
        action_required_for_human = True
        recommended_next_action = "Human decision required before paper testing"
    else:
        review_status = "AUTO_ARCHIVED"
        primary_reason = "WEAK_OR_NON_GENERALIZING_RESULT"
        action_required_for_human = False
        recommended_next_action = "Archive and retain result lineage"

    review = {
        "schema_version": "ai_result_review.v1",
        "idea_id": str(result["idea_id"]),
        "test_id": str(result["test_id"]),
        "review_status": review_status,
        "action_required_for_human": action_required_for_human,
        "primary_reason": primary_reason,
        "findings": sorted(set(findings if findings else [primary_reason])),
        "overfitting_risk": overfitting_risk if overfitting_risk in {"LOW", "MEDIUM", "HIGH"} else "LOW",
        "data_quality_risk": data_quality_risk if data_quality_risk in {"LOW", "MEDIUM", "HIGH"} else "LOW",
        "recommended_next_action": recommended_next_action,
        "codex_task_recommended": "run_research_event_sweep_v1",
        "generated_utc": utc_now_iso(),
    }
    return validate_ai_result_review_v1(review)


def write_ai_review_artifact_v1(review_payload: dict[str, Any]) -> Path:
    review = validate_ai_result_review_v1(review_payload)
    path = (_research_runtime_root() / "reviews" / "ai_result_reviews" / f"{review['test_id']}.ai_result_review.v1.json").resolve()
    return _canonical_write_json(path, review)


def build_trading_day_ai_packet_v1(
    *,
    day_utc: str,
    source_artifacts: list[str],
) -> dict[str, Any]:
    closure_payload: dict[str, Any] = {}
    submit_boundary_payload: dict[str, Any] = {}
    submission_index_payload: dict[str, Any] = {}
    ib_report_ref = ""

    for path_text in source_artifacts:
        path = Path(path_text).expanduser().resolve()
        if path.name == "aegis_day_closure_authority.v1.json" and path.exists():
            closure_payload = _read_json(path)
        elif path.name == "submit_boundary_status.v1.json" and path.exists():
            submit_boundary_payload = _read_json(path)
        elif path.name == "submission_index.v1.json" and path.exists():
            submission_index_payload = _read_json(path)
        elif "broker_reconciliation" in str(path):
            ib_report_ref = str(path)

    blocking_evidence = closure_payload.get("blocking_evidence") if isinstance(closure_payload.get("blocking_evidence"), list) else []
    submit_blockers = submit_boundary_payload.get("blocking_codes") if isinstance(submit_boundary_payload.get("blocking_codes"), list) else []

    packet = {
        "schema_version": "research_ai_packet.v1",
        "day_utc": day_utc,
        "generated_utc": utc_now_iso(),
        "aegis_day_status": {
            "closure_status": str(closure_payload.get("status") or "UNKNOWN"),
            "canonical_blocker": str(closure_payload.get("canonical_blocker") or ""),
        },
        "trades_if_any": {
            "submission_index_status": str(submission_index_payload.get("status") or "UNKNOWN"),
            "submission_count": int(submission_index_payload.get("submission_count") or 0)
            if isinstance(submission_index_payload.get("submission_count"), int)
            else 0,
        },
        "blocked_signals": [str(code) for code in submit_blockers],
        "execution_fill_summary": {
            "submission_index_path": next(
                (path for path in source_artifacts if path.endswith("submission_index.v1.json")),
                "",
            ),
            "closure_blocking_evidence_count": len(blocking_evidence),
        },
        "ib_report_reference": ib_report_ref,
        "recurring_blockers": [
            str(item.get("code") or "")
            for item in blocking_evidence
            if isinstance(item, dict) and str(item.get("code") or "")
        ],
        "candidate_areas_for_edge_mining": [
            "post-blocker mean reversion",
            "regime-aware filter discovery",
            "execution-friction resilient signal variants",
            "loss-pattern-conditioned rejection filters",
        ],
        "strict_instruction": "Output only sandbox-testable hypotheses. Do not recommend live trades.",
        "ai_requests": [
            "new edge hypotheses",
            "filters for losing conditions",
            "failure pattern analysis",
            "sandbox test designs",
            "rejection criteria",
        ],
        "source_artifacts": sorted(set([str(path) for path in source_artifacts])),
        "requires_ai_output_json_only": True,
        "forbidden_actions": [
            "place_live_trade",
            "enable_paper_trading",
            "promote_live",
            "change_risk_limits",
            "modify_aegis_core",
        ],
    }
    return packet


def build_trading_day_ai_packet_markdown_v1(packet: dict[str, Any]) -> str:
    status = packet.get("aegis_day_status") if isinstance(packet.get("aegis_day_status"), dict) else {}
    lines = [
        "# Research AI Packet V1",
        "",
        f"- day_utc: {packet.get('day_utc')}",
        f"- closure_status: {status.get('closure_status', 'UNKNOWN')}",
        f"- canonical_blocker: {status.get('canonical_blocker', '')}",
        f"- ib_report_reference: {packet.get('ib_report_reference', '')}",
        "",
        "## Required AI Output",
        "- Return JSON-only hypotheses that are sandbox-testable.",
        "- Include: new edge hypotheses, losing-condition filters, failure-pattern analysis, sandbox test designs, rejection criteria.",
        "",
        "## Forbidden",
        "- No live trade recommendation.",
        "- No paper/live promotion recommendation.",
        "- No risk/capital/Aegis-Core modifications.",
    ]
    return "\n".join(lines) + "\n"


def write_trading_day_ai_packet_v1(*, day_utc: str, packet: dict[str, Any], packet_markdown: str) -> dict[str, str]:
    root = _research_runtime_root()
    base = (root / "reviews" / "ai_edge_reviews" / day_utc).resolve()
    json_path = _canonical_write_json((base / "research_ai_packet.v1.json").resolve(), packet)
    md_path = (base / "research_ai_packet.md").resolve()
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(packet_markdown, encoding="utf-8")
    return {
        "packet_json_path": str(json_path),
        "packet_markdown_path": str(md_path),
    }
