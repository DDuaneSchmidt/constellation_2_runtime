from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping


def build_step_result_envelope_v1(
    *,
    step_id: str,
    status: str,
    artifact_refs: Iterable[str] = (),
    counts: Mapping[str, int] | None = None,
    warnings: Iterable[str] = (),
    blocking_defects: Iterable[Mapping[str, str]] = (),
    diagnostics: Mapping[str, Any] | None = None,
    schema_refs: Iterable[str] = (),
) -> Dict[str, Any]:
    normalized_status = str(status or "").strip().upper()
    if normalized_status not in {"OK", "OK_WITH_WARNINGS", "BLOCKED"}:
        raise ValueError(f"STEP_RESULT_STATUS_INVALID:status={status!r}")
    step_name = str(step_id or "").strip()
    if not step_name:
        raise ValueError("STEP_RESULT_STEP_ID_REQUIRED")

    refs = [str(item).strip() for item in artifact_refs if str(item).strip()]
    warning_rows = [str(item).strip() for item in warnings if str(item).strip()]
    schema_rows = [str(item).strip() for item in schema_refs if str(item).strip()]
    defects = []
    for row in blocking_defects:
        defect = {
            "blocking_class": str(row.get("blocking_class") or "").strip(),
            "reason_code": str(row.get("reason_code") or "").strip(),
            "evidence_ref": str(row.get("evidence_ref") or "").strip(),
        }
        if not defect["blocking_class"] or not defect["reason_code"] or not defect["evidence_ref"]:
            raise ValueError(f"STEP_RESULT_BLOCKING_DEFECT_INVALID:defect={row!r}")
        defects.append(defect)

    counter_map: Dict[str, int] = {}
    for key, value in (counts or {}).items():
        counter_key = str(key).strip()
        if not counter_key:
            continue
        counter_value = int(value)
        if counter_value < 0:
            raise ValueError(f"STEP_RESULT_COUNT_NEGATIVE:key={counter_key}:value={counter_value}")
        counter_map[counter_key] = counter_value

    return {
        "schema_id": "step_result_envelope",
        "schema_version": "v1",
        "step_id": step_name,
        "status": normalized_status,
        "artifact_refs": refs,
        "counts": counter_map,
        "warnings": warning_rows,
        "blocking_defects": defects,
        "diagnostics": dict(diagnostics or {}),
        "schema_refs": schema_rows,
    }
