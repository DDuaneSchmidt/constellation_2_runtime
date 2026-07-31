from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.dormant_sleeve_signal_generation_diagnostics_v1 import (
    build_dormant_sleeve_signal_generation_diagnostics_v1,
    write_dormant_sleeve_signal_generation_diagnostics_v1,
)
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.missing_market_data_repair_v1 import build_missing_market_data_repair_v1, write_missing_market_data_repair_v1
from ops.aegis.missing_market_data_requirement_resolver_v1 import (
    build_missing_market_data_requirement_resolver_v1,
    write_missing_market_data_requirement_resolver_v1,
)
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1
from ops.aegis.sleeve_throughput_diagnostics_v1 import (
    build_sleeve_throughput_diagnostics_v1,
    write_sleeve_throughput_diagnostics_v1,
)
from ops.tools.run_intent_lifecycle_state_v1 import build_intent_lifecycle_state_v1
from ops.tools.run_position_lifecycle_state_v1 import build_position_lifecycle_state_v1

FAMILY = "aegis_event_dislocation_position_state_freshness_repair_v1"
FILENAME = "event_dislocation_position_state_freshness_repair.v1.json"
POLICY_VERSION = "AEGIS_EVENT_DISLOCATION_POSITION_STATE_FRESHNESS_REPAIR_V1"
SLEEVE_ID = "C2_EVENT_DISLOCATION_V1"
SLEEVE_NAME = "Event Dislocation Repricing"

SAFETY = {
    "read_only": False,
    "diagnostics_only": False,
    "repair_scope": "position_state_routing_and_freshness_only",
    "no_strategy_logic_mutation": True,
    "no_threshold_mutation": True,
    "no_candidate_scoring_mutation": True,
    "no_risk_policy_mutation": True,
    "no_quality_mutation": True,
    "no_allocation_mutation": True,
    "no_position_state_fabrication": True,
    "no_signal_fabrication": True,
    "no_candidate_fabrication": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def event_dislocation_position_state_freshness_repair_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_event_dislocation_position_state_freshness_repair_v1(
    *, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    computed_at = computed_at_utc or f"{day}T00:00:00Z"
    sleeve_root = _sleeve_truth_root(root)
    source_path = sleeve_root / "positions_snapshot_v2" / "snapshots" / day / "positions_snapshot.v2.json"
    routed_path = sleeve_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json"
    source_before = _state_check(source_path, day)
    routed_before = _state_check(routed_path, day)
    t04_row = _event_t04_row(root, day)

    repair_status = "NO_REPAIR_REQUIRED"
    repair_type = "NO_REPAIR"
    repair_attempted = False
    blocker_reason = "NONE"
    created_paths: list[str] = []

    if not source_before["found"]:
        repair_status = "NOT_REPAIRED_SOURCE_TRULY_MISSING"
        blocker_reason = f"Authoritative governed source state is missing: {source_path}"
    elif not source_before["fresh"]:
        repair_status = "NOT_REPAIRED_SOURCE_STALE"
        blocker_reason = source_before["reason"]
    elif not source_before["lineage_ok"]:
        repair_status = "NOT_REPAIRED_LINEAGE_MISSING"
        blocker_reason = source_before["reason"]
    elif routed_before["fresh"]:
        repair_status = "NO_REPAIR_REQUIRED"
    else:
        repair_attempted = True
        repair_status = "REPAIRED"
        repair_type = "POSITION_STATE_ROUTING_REPAIR"
        _route_state(source_path, routed_path)
        created_paths.append(str(routed_path))

    if repair_status in {"REPAIRED", "NO_REPAIR_REQUIRED"}:
        post_lifecycle = _regenerate_lifecycle(root, day)
        post_payloads = _post_diagnostics(root, day, computed_at)
    else:
        post_lifecycle = {}
        post_payloads = _read_post_diagnostics(root, day)

    source_after = _state_check(source_path, day)
    routed_after = _state_check(routed_path, day)
    t01_row = _find_sleeve(_rows(post_payloads.get("t01"), "sleeves"), SLEEVE_ID)
    t02_row = _find_sleeve(_rows(post_payloads.get("t02"), "dormant_sleeves"), SLEEVE_ID)

    post_t01_status = _text(t01_row.get("throughput_status")) or "UNKNOWN"
    post_t01_blocker = _text(t01_row.get("blocker_code"))
    post_t02_reason = _text(t02_row.get("dormant_reason_code")) or ("NO_T02_ROW" if not t02_row else "UNKNOWN")
    position_status = "FRESH" if routed_after["fresh"] else "STALE_OR_MISSING"
    remaining_blocker, owner, david_action = _remaining_blocker(
        repair_status=repair_status,
        post_t01_status=post_t01_status,
        post_t01_blocker=post_t01_blocker,
        post_t02_reason=post_t02_reason,
    )
    if blocker_reason == "NONE":
        blocker_reason = _blocker_reason(remaining_blocker, post_t01_blocker, post_t02_reason, routed_after)

    payload: dict[str, Any] = {
        "schema_id": "aegis_event_dislocation_position_state_freshness_repair",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": computed_at,
        "sleeve_id": SLEEVE_ID,
        "sleeve_name": SLEEVE_NAME,
        "original_t04_status": _text(t04_row.get("repair_status")) or "UNKNOWN",
        "original_blocker": _text(t04_row.get("remaining_blocker") or t04_row.get("post_repair_t02_reason_code")) or "UNKNOWN",
        "required_position_state_items": ["paper.positions_snapshot.v2", "intent_lifecycle.positions_snapshot"],
        "required_fields": ["day_utc", "status", "positions.items", "input_manifest"],
        "expected_position_state_source": "truth_sleeves/PRIMARY/PAPER/positions_snapshot_v2/snapshots/<TARGET_DAY>/positions_snapshot.v2.json",
        "source_state_found": bool(source_after["found"]),
        "source_state_path": str(source_path),
        "source_state_fresh_before_repair": bool(source_before["fresh"]),
        "source_state_fresh_after_repair": bool(source_after["fresh"]),
        "source_state_timestamp": source_after["timestamp"],
        "freshness_rule": "position state day_utc/as_of_day_utc must equal target_day and status must not be BLOCKED, ERROR, FAILED, or STALE",
        "state_lineage_status": "LINEAGE_PRESENT" if source_after["lineage_ok"] and routed_after["lineage_ok"] else "LINEAGE_MISSING",
        "state_routing_status": "ROUTED" if routed_after["fresh"] else "NOT_ROUTED",
        "repair_attempted": repair_attempted,
        "repair_status": repair_status,
        "repair_type": repair_type,
        "post_repair_position_state_status": position_status,
        "post_repair_signal_generation_status": _signal_generation_status(t01_row, t02_row),
        "post_repair_candidate_status": "CANDIDATES_PRESENT" if _int(t01_row.get("candidate_count")) > 0 else "NO_CANDIDATES_PRESENT",
        "post_repair_t01_status": post_t01_status,
        "post_repair_t02_reason_code": post_t02_reason,
        "remaining_blocker": remaining_blocker,
        "blocker_reason": blocker_reason,
        "owner": owner,
        "david_action_required": david_action,
        "created_paths": created_paths,
        "routed_state_path": str(routed_path),
        "source_artifact_paths": _source_paths(root, day, source_path, routed_path, post_lifecycle),
        "source_artifact_hashes": {
            name: file_hash_v1(Path(path))
            for name, path in _source_paths(root, day, source_path, routed_path, post_lifecycle).items()
            if path and Path(path).exists()
        },
        "safety_statement": "T05 repairs only AEGIS-owned position-state routing/freshness from governed source evidence. It does not change strategy logic, thresholds, candidate scoring, risk policy, allocation, broker/live trading, or fabricate position state/signals/candidates.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1({**payload, "content_hash": ""})
    return payload


def write_event_dislocation_position_state_freshness_repair_v1(
    *, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_event_dislocation_position_state_freshness_repair_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(event_dislocation_position_state_freshness_repair_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _sleeve_truth_root(root: Path) -> Path:
    return root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"


def _state_check(path: Path, day: str) -> dict[str, Any]:
    payload = read_json_v1(path)
    if not payload:
        return {"found": False, "fresh": False, "lineage_ok": False, "timestamp": "", "reason": f"missing:{path}"}
    status = _upper(payload.get("status") or payload.get("snapshot_status") or "OK")
    payload_day = _text(payload.get("day_utc") or payload.get("as_of_day_utc") or day)
    positions = payload.get("positions") if isinstance(payload.get("positions"), dict) else {}
    items = payload.get("items")
    if items is None:
        items = positions.get("items") if isinstance(positions, dict) else None
    lineage_ok = isinstance(payload.get("input_manifest"), list) and bool(payload.get("input_manifest"))
    timestamp = _text(payload.get("produced_utc") or payload.get("asof_utc") or positions.get("asof_utc") if isinstance(positions, dict) else "")
    if not isinstance(items, list):
        return {"found": True, "fresh": False, "lineage_ok": lineage_ok, "timestamp": timestamp, "reason": "positions.items missing or malformed"}
    if payload_day != day:
        return {"found": True, "fresh": False, "lineage_ok": lineage_ok, "timestamp": timestamp, "reason": f"state day {payload_day} != target day {day}"}
    if status in {"BLOCKED", "ERROR", "FAILED", "STALE"}:
        return {"found": True, "fresh": False, "lineage_ok": lineage_ok, "timestamp": timestamp, "reason": f"state status {status} is not fresh"}
    if not lineage_ok:
        return {"found": True, "fresh": False, "lineage_ok": False, "timestamp": timestamp, "reason": "input_manifest missing"}
    return {"found": True, "fresh": True, "lineage_ok": True, "timestamp": timestamp, "reason": "fresh"}


def _route_state(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.read_bytes() == source.read_bytes():
        return
    shutil.copyfile(source, target)


def _regenerate_lifecycle(root: Path, day: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        out["position_lifecycle"] = build_position_lifecycle_state_v1(day_utc=day, truth_root=root)
    except Exception as exc:  # pragma: no cover - defensive runtime reporting
        out["position_lifecycle_error"] = repr(exc)
    try:
        out["intent_lifecycle"] = build_intent_lifecycle_state_v1(day_utc=day, truth_root=root)
    except Exception as exc:  # pragma: no cover - defensive runtime reporting
        out["intent_lifecycle_error"] = repr(exc)
    return out


def _post_diagnostics(root: Path, day: str, computed_at: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    t01_path = report_path_v1(root, "aegis_sleeve_throughput_diagnostics_v1", day, "sleeve_throughput_diagnostics.v1.json")
    t02_path = report_path_v1(root, "aegis_dormant_sleeve_signal_generation_diagnostics_v1", day, "dormant_sleeve_signal_generation_diagnostics.v1.json")
    t03_path = report_path_v1(root, "aegis_missing_market_data_requirement_resolver_v1", day, "missing_market_data_requirement_resolver.v1.json")
    t04_path = report_path_v1(root, "aegis_missing_market_data_repair_v1", day, "missing_market_data_repair.v1.json")
    can_rebuild_signal_diagnostics = (
        report_path_v1(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json").exists()
        and report_path_v1(root, "sleeve_evaluation_kernel_v1", day, "sleeve_evaluation_rollup.v1.json").exists()
    )
    if can_rebuild_signal_diagnostics:
        try:
            payloads["t01"] = build_sleeve_throughput_diagnostics_v1(truth_root=root, day_utc=day, computed_at_utc=computed_at)
            write_sleeve_throughput_diagnostics_v1(truth_root=root, day_utc=day, payload=payloads["t01"])
        except Exception:
            payloads["t01"] = read_json_v1(t01_path)
    else:
        payloads["t01"] = read_json_v1(t01_path)
    if can_rebuild_signal_diagnostics:
        try:
            payloads["t02"] = build_dormant_sleeve_signal_generation_diagnostics_v1(truth_root=root, day_utc=day, computed_at_utc=computed_at)
            write_dormant_sleeve_signal_generation_diagnostics_v1(truth_root=root, day_utc=day, payload=payloads["t02"])
        except Exception:
            payloads["t02"] = read_json_v1(t02_path)
    else:
        payloads["t02"] = read_json_v1(t02_path)
    try:
        payloads["t03"] = build_missing_market_data_requirement_resolver_v1(truth_root=root, day_utc=day, computed_at_utc=computed_at)
        write_missing_market_data_requirement_resolver_v1(truth_root=root, day_utc=day, payload=payloads["t03"])
    except Exception:
        payloads["t03"] = read_json_v1(t03_path)
    try:
        payloads["t04"] = build_missing_market_data_repair_v1(truth_root=root, day_utc=day, computed_at_utc=computed_at)
        write_missing_market_data_repair_v1(truth_root=root, day_utc=day, payload=payloads["t04"])
    except Exception:
        payloads["t04"] = read_json_v1(t04_path)
    return payloads

def _read_post_diagnostics(root: Path, day: str) -> dict[str, Any]:
    return {
        "t01": read_json_v1(report_path_v1(root, "aegis_sleeve_throughput_diagnostics_v1", day, "sleeve_throughput_diagnostics.v1.json")),
        "t02": read_json_v1(report_path_v1(root, "aegis_dormant_sleeve_signal_generation_diagnostics_v1", day, "dormant_sleeve_signal_generation_diagnostics.v1.json")),
        "t03": read_json_v1(report_path_v1(root, "aegis_missing_market_data_requirement_resolver_v1", day, "missing_market_data_requirement_resolver.v1.json")),
        "t04": read_json_v1(report_path_v1(root, "aegis_missing_market_data_repair_v1", day, "missing_market_data_repair.v1.json")),
    }


def _event_t04_row(root: Path, day: str) -> dict[str, Any]:
    payload = read_json_v1(report_path_v1(root, "aegis_missing_market_data_repair_v1", day, "missing_market_data_repair.v1.json"))
    return _find_sleeve(_rows(payload, "sleeves"), SLEEVE_ID)


def _source_paths(root: Path, day: str, source: Path, routed: Path, lifecycle: Mapping[str, Any]) -> dict[str, str]:
    paths = {
        "t04": str(report_path_v1(root, "aegis_missing_market_data_repair_v1", day, "missing_market_data_repair.v1.json")),
        "source_position_state": str(source),
        "routed_position_state": str(routed),
        "intent_lifecycle_state": str(root / "reports" / "intent_lifecycle_state_v1" / day / "intent_lifecycle_state.v1.json"),
        "position_lifecycle_state": str(root / "reports" / "position_lifecycle_state_v1" / day / "position_lifecycle_state.v1.json"),
        "t01": str(report_path_v1(root, "aegis_sleeve_throughput_diagnostics_v1", day, "sleeve_throughput_diagnostics.v1.json")),
        "t02": str(report_path_v1(root, "aegis_dormant_sleeve_signal_generation_diagnostics_v1", day, "dormant_sleeve_signal_generation_diagnostics.v1.json")),
        "t03": str(report_path_v1(root, "aegis_missing_market_data_requirement_resolver_v1", day, "missing_market_data_requirement_resolver.v1.json")),
    }
    for key in ("position_lifecycle", "intent_lifecycle"):
        value = lifecycle.get(key)
        if isinstance(value, dict) and value.get("artifact_path"):
            paths[f"{key}_artifact"] = _text(value.get("artifact_path"))
    return paths


def _remaining_blocker(*, repair_status: str, post_t01_status: str, post_t01_blocker: str, post_t02_reason: str) -> tuple[str, str, bool]:
    if repair_status.startswith("NOT_REPAIRED"):
        return repair_status.replace("NOT_REPAIRED_", ""), "AEGIS_SYSTEM", False
    if post_t01_status == "FLOWING":
        return "NONE", "NONE", False
    if post_t02_reason not in {"", "NO_T02_ROW", "UNKNOWN"}:
        return post_t02_reason, "AEGIS_SYSTEM", False
    if post_t01_blocker:
        return post_t01_blocker, "AEGIS_SYSTEM", False
    return "NONE", "NONE", False


def _blocker_reason(blocker: str, t01_blocker: str, t02_reason: str, routed_after: Mapping[str, Any]) -> str:
    if blocker == "NONE":
        return "NONE"
    if blocker == "SIGNALS_PRESENT_BUT_NO_CANDIDATES":
        return "Position-state freshness blocker cleared; Event Dislocation produced raw signals but no candidate contracts."
    if blocker == "POSITION_STATE_STALE":
        return routed_after.get("reason") or "Position state remains stale or unrouted."
    if t02_reason not in {"", "NO_T02_ROW"}:
        return f"Post-repair T02 reports {t02_reason}."
    if t01_blocker:
        return f"Post-repair T01 reports {t01_blocker}."
    return f"Post-repair blocker: {blocker}."


def _signal_generation_status(t01_row: Mapping[str, Any], t02_row: Mapping[str, Any]) -> str:
    status = _text(t01_row.get("throughput_status"))
    if status == "FLOWING":
        return "FLOWING"
    reason = _text(t02_row.get("dormant_reason_code"))
    if reason in {"VALID_NO_SIGNAL_CONDITIONS", "TRIGGER_THRESHOLDS_NOT_MET"}:
        return reason
    if status == "BLOCKED":
        return "BLOCKED"
    if status == "DORMANT":
        return "DORMANT"
    return "UNKNOWN"


def _rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key) if isinstance(payload, Mapping) else None
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _find_sleeve(rows: list[dict[str, Any]], sleeve_id: str) -> dict[str, Any]:
    for row in rows:
        if _text(row.get("sleeve_id") or row.get("engine_id")) == sleeve_id:
            return row
    return {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0
