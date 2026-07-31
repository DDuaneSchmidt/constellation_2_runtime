from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def lite_migration_equivalence_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_lite_migration_equivalence_v1" / day_utc / "lite_migration_equivalence.v1.json"


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


def _row(semantic_id: str, lite_source: str, strategic_source: str, equivalent: bool, mismatch_reason: str, blocking_capability: str) -> dict[str, Any]:
    return {
        "semantic_id": semantic_id,
        "lite_source": lite_source,
        "strategic_source": strategic_source,
        "equivalent": bool(equivalent),
        "mismatch_reason": "" if equivalent else mismatch_reason,
        "blocking_capability": blocking_capability,
        "migration_safe": bool(equivalent),
    }


def _field(payload: dict[str, Any], dotted: str) -> Any:
    value: Any = payload
    for part in dotted.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _false(payload: dict[str, Any], dotted: str) -> bool:
    return _field(payload, dotted) is False


def build_lite_migration_equivalence_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or now_utc_v1()
    paths = {
        "aegis_lite_operating_status": _latest(root, "aegis_lite_operating_status_v1", day_utc, "aegis_lite_operating_status.v1.json"),
        "aegis_lite_eod_report": _latest(root, "aegis_lite_eod_report_v1", day_utc, "aegis_lite_eod_report.v1.json"),
        "aegis_strategic_operating_status": _latest(root, "aegis_strategic_operating_status_v1", day_utc, "strategic_operating_status.v1.json"),
        "aegis_research_eod_summary": _latest(root, "aegis_research_eod_summary_v1", day_utc, "research_eod_summary.v1.json"),
    }
    data = {key: _read(path) for key, path in paths.items()}
    lite_status = data["aegis_lite_operating_status"]
    lite_eod = data["aegis_lite_eod_report"]
    strategic_status = data["aegis_strategic_operating_status"]
    research_eod = data["aegis_research_eod_summary"]
    lite_selected_count = len(lite_eod.get("selected_trade_candidates") if isinstance(lite_eod.get("selected_trade_candidates"), list) else [])
    strategic_current_day_candidates = int(_field(research_eod, "candidate_summary.current_day_candidate_count") or 0)
    strategic_source_count = len(strategic_status.get("source_artifacts") if isinstance(strategic_status.get("source_artifacts"), list) else [])
    research_source_count = len(research_eod.get("source_artifacts") if isinstance(research_eod.get("source_artifacts"), list) else [])
    lite_blocked = str(lite_eod.get("manual_execution_status") or "").upper() != "READY_FOR_MANUAL_ENTRY" or bool(lite_eod.get("do_not_trade_blockers"))
    strategic_non_action = not bool(_field(strategic_status, "operator_action_state.david_action_required"))
    rows = [
        _row(
            "operating_status_current_day",
            "aegis_lite_operating_status.day_utc",
            "aegis_strategic_operating_status.day_utc",
            bool(lite_status) and bool(strategic_status) and str(lite_status.get("day_utc")) == day_utc and str(strategic_status.get("day_utc")) == day_utc,
            "Lite or strategic operating status is missing or wrong-day.",
            "DATA_READY",
        ),
        _row(
            "operating_policy_safety",
            "aegis_lite_operating_status policy fields",
            "aegis_strategic_operating_status.policy_state",
            bool(strategic_status) and _false(strategic_status, "policy_state.trade_advice_allowed") and _false(strategic_status, "policy_state.manual_trade_capture_allowed") and _false(strategic_status, "policy_state.broker_submit_transmit_allowed") and _false(strategic_status, "policy_state.autonomous_execution_allowed"),
            "Strategic operating status does not preserve disabled safety policy fields.",
            "TRADE_ADVICE_ALLOWED",
        ),
        _row(
            "operating_source_coverage",
            "aegis_lite_operating_status.source_artifact_lineage",
            "aegis_strategic_operating_status.source_artifacts",
            strategic_source_count >= 8,
            "Strategic operating status does not include enough source evidence references.",
            "DATA_READY",
        ),
        _row(
            "eod_summary_current_day",
            "aegis_lite_eod_report.day_utc",
            "aegis_research_eod_summary.day_utc",
            bool(lite_eod) and bool(research_eod) and str(lite_eod.get("day_utc")) == day_utc and str(research_eod.get("day_utc")) == day_utc,
            "Lite EOD report or research EOD summary is missing or wrong-day.",
            "DATA_READY",
        ),
        _row(
            "eod_candidate_action_equivalence",
            "aegis_lite_eod_report.selected_trade_candidates/manual_execution_status",
            "aegis_research_eod_summary.candidate_summary/operator_action_summary",
            (lite_selected_count == 0 and strategic_current_day_candidates == 0 and strategic_non_action) or (lite_selected_count == strategic_current_day_candidates),
            "Strategic EOD candidate/action summary does not match Lite no-action semantics.",
            "DATA_READY",
        ),
        _row(
            "eod_blocker_semantics",
            "aegis_lite_eod_report.do_not_trade_blockers/report_status",
            "aegis_research_eod_summary.operator_action_summary.blockers/eod_status",
            (not lite_blocked) or str(research_eod.get("eod_status") or "").upper() in {"READY_WITH_NO_ACTION", "NEEDS_OPERATOR_ACTION"},
            "Strategic EOD summary does not preserve blocked/no-action semantics.",
            "DATA_READY",
        ),
        _row(
            "eod_source_coverage",
            "aegis_lite_eod_report.source_artifact_lineage",
            "aegis_research_eod_summary.source_artifacts",
            research_source_count >= 8,
            "Research EOD summary does not include enough source evidence references.",
            "DATA_READY",
        ),
        _row(
            "manual_packet_not_migrated",
            "manual_trade_packet remains out of scope",
            "runtime truth keeps manual_trade_packet dependency",
            True,
            "",
            "TRADE_ADVICE_ALLOWED",
        ),
    ]
    blocking_rows = [row for row in rows if not row["migration_safe"]]
    payload: dict[str, Any] = {
        "schema_id": "aegis_lite_migration_equivalence",
        "schema_version": "v1",
        "artifact_id": "aegis_lite_migration_equivalence_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "migration_scope": ["aegis_lite_operating_status", "aegis_lite_eod_report"],
        "out_of_scope": ["operator_execution_queue", "manual_trade_packet", "manual_execution_receipt"],
        "replacement_mapping": {
            "aegis_lite_operating_status": "aegis_strategic_operating_status",
            "aegis_lite_eod_report": "aegis_research_eod_summary",
        },
        "comparisons": rows,
        "overall_equivalent": not blocking_rows,
        "runtime_truth_migration_safe": not blocking_rows,
        "blocking_comparison_count": len(blocking_rows),
        "blocking_comparisons": blocking_rows,
        "source_artifacts": [{"source_id": key, "path": str(path or ""), "sha256": _sha(path)} for key, path in sorted(paths.items())],
        "source_artifact_hashes": {key: _sha(path) for key, path in sorted(paths.items()) if path},
        "safety_assertions": {
            "trade_advice_allowed": False,
            "manual_trade_capture_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_execution_allowed": False,
            "manual_trade_packet_migrated": False,
        },
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def write_lite_migration_equivalence_v1(*, truth_root: Path | str, payload: dict[str, Any]) -> Path:
    path = lite_migration_equivalence_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_and_write_lite_migration_equivalence_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> tuple[dict[str, Any], Path]:
    payload = build_lite_migration_equivalence_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    return payload, write_lite_migration_equivalence_v1(truth_root=truth_root, payload=payload)


def validate_lite_migration_equivalence_v1(payload: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if str(payload.get("schema_id") or "") != "aegis_lite_migration_equivalence":
        failures.append("schema_id must be aegis_lite_migration_equivalence")
    if not isinstance(payload.get("comparisons"), list) or not payload.get("comparisons"):
        failures.append("comparisons are required")
    for row in payload.get("comparisons") or []:
        if not isinstance(row, dict):
            failures.append("comparison row must be an object")
            continue
        for field in ("lite_source", "strategic_source", "equivalent", "mismatch_reason", "blocking_capability", "migration_safe"):
            if field not in row:
                failures.append(f"comparison {row.get('semantic_id')} missing {field}")
    safety = payload.get("safety_assertions") if isinstance(payload.get("safety_assertions"), dict) else {}
    for key in ("trade_advice_allowed", "manual_trade_capture_allowed", "broker_execution_allowed", "broker_submit_transmit_allowed", "live_trading_allowed", "autonomous_execution_allowed", "manual_trade_packet_migrated"):
        if safety.get(key) is not False:
            failures.append(f"safety assertion must remain false: {key}")
    return failures
