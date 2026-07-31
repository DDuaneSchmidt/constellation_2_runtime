from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.operator_action_model_v1 import (
    ALLOWED_STATUSES,
    REQUIRED_CAPABILITIES,
    build_operator_action_model_v1,
    operator_action_model_path_v1,
)

REPORT_FAMILY = "aegis_operator_action_model_self_check_v1"
REPORT_FILENAME = "self_check.v1.json"


def operator_action_model_self_check_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_operator_action_model_self_check_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    model_path = operator_action_model_path_v1(truth_root=root, day_utc=day_utc)
    model = _read_json(model_path)
    if not model:
        model = build_operator_action_model_v1(truth_root=root, day_utc=day_utc)
    rebuilt = build_operator_action_model_v1(truth_root=root, day_utc=day_utc)
    failures: list[dict[str, Any]] = []
    rows = model.get("capability_matrix") if isinstance(model.get("capability_matrix"), list) else []
    by_id = {str(row.get("capability_id") or ""): row for row in rows if isinstance(row, Mapping)}
    for capability_id in REQUIRED_CAPABILITIES:
        if capability_id not in by_id:
            failures.append(_failure("MISSING_CAPABILITY", capability_id, "Required capability is absent."))
    for capability_id, row in sorted(by_id.items()):
        status = str(row.get("status") or "")
        if status not in ALLOWED_STATUSES:
            failures.append(_failure("INVALID_STATUS", capability_id, status))
        if "david_action_required" not in row or not isinstance(row.get("david_action_required"), bool):
            failures.append(_failure("MISSING_DAVID_ACTION_FLAG", capability_id, "david_action_required must be boolean."))
        if status in {"BLOCKED", "WAITING", "DISABLED_BY_POLICY"} and not row.get("reason_codes"):
            failures.append(_failure("MISSING_REASON_CODES", capability_id, status))
        reason = str(row.get("human_readable_reason") or "")
        if status in {"BLOCKED", "WAITING", "DISABLED_BY_POLICY"} and not reason:
            failures.append(_failure("MISSING_HUMAN_REASON", capability_id, status))
        blocker_text = f"{reason} {' '.join(str(x) for x in row.get('reason_codes') or [])}".lower()
        if "n/a" in blocker_text and any(code for code in row.get("reason_codes") or []):
            failures.append(_failure("NA_BLOCKER_WITH_SOURCE_REASON", capability_id, blocker_text))
    broker = by_id.get("BROKER_EXECUTION", {})
    if broker.get("status") != "DISABLED_BY_POLICY":
        failures.append(_failure("BROKER_EXECUTION_NOT_DISABLED_BY_POLICY", "BROKER_EXECUTION", str(broker.get("status"))))
    trade = by_id.get("TRADE_RECOMMENDATION", {})
    safety = model.get("safety") if isinstance(model.get("safety"), Mapping) else {}
    if safety.get("trade_advice_allowed") is False and trade.get("status") == "READY":
        failures.append(_failure("TRADE_RECOMMENDATION_READY_WHEN_TRADE_ADVICE_FALSE", "TRADE_RECOMMENDATION", "trade_advice_allowed=false"))
    manual = by_id.get("MANUAL_TRADE_CAPTURE", {})
    manual_codes = set(str(x) for x in manual.get("reason_codes") or [])
    if manual.get("status") == "READY" and "ELIGIBLE_MANUAL_PACKET_PRESENT" not in manual_codes:
        failures.append(_failure("MANUAL_CAPTURE_READY_WITHOUT_ELIGIBLE_PACKET", "MANUAL_TRADE_CAPTURE", "missing packet reason code"))
    david = by_id.get("DAVID_ACTION", {})
    if david.get("david_action_required") is True and david.get("status") not in {"ACTIVE", "READY"}:
        failures.append(_failure("DAVID_ACTION_TRUE_WITHOUT_REAL_TASK_STATE", "DAVID_ACTION", str(david.get("status"))))
    byte_stable_match = _normalized(model) == _normalized(rebuilt)
    semantic_stable_match = _normalized(_semantic_model(model)) == _normalized(_semantic_model(rebuilt))
    provenance_only_drift = (not byte_stable_match) and semantic_stable_match
    if not semantic_stable_match:
        failures.append(_failure("NON_DETERMINISTIC_OUTPUT", "operator_action_model", "rebuilt semantic model differs from artifact"))
    return {
        "schema_id": "aegis_operator_action_model_self_check",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": model.get("generated_at") or rebuilt.get("generated_at"),
        "ok": not failures,
        "byte_stable_match": byte_stable_match,
        "semantic_stable_match": semantic_stable_match,
        "provenance_only_drift": provenance_only_drift,
        "failure_count": len(failures),
        "failures": failures,
        "model_path": str(model_path),
        "required_capability_count": len(REQUIRED_CAPABILITIES),
        "capability_count": len(rows),
    }


def write_operator_action_model_self_check_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    path = operator_action_model_self_check_path_v1(truth_root=truth_root, day_utc=day_utc)
    body = dict(payload or build_operator_action_model_self_check_v1(truth_root=truth_root, day_utc=day_utc))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _semantic_model(payload: Mapping[str, Any]) -> dict[str, Any]:
    clone = json.loads(json.dumps(payload, sort_keys=True))
    if isinstance(clone, dict):
        clone.pop("generated_at", None)
        clone.pop("source_hashes", None)
        for row in clone.get("capability_matrix") or []:
            if isinstance(row, dict):
                row.pop("source_hashes", None)
        for row in (clone.get("capabilities_by_id") or {}).values():
            if isinstance(row, dict):
                row.pop("source_hashes", None)
    return clone if isinstance(clone, dict) else {}


def _normalized(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _failure(code: str, item: str, detail: str) -> dict[str, str]:
    return {"failure_code": code, "item": item, "detail": detail}
