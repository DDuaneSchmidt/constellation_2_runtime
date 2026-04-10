from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
)


EXECUTION_OUTCOME_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_outcome.v1.schema.json"
SELF_HEAL_MARKERS = ("QUARANTINED_STALE_", "REFRESHED_STALE_", "self_heal=1")
DEFERRED_MARKERS = ("DEFERRED_",)


def resolve_execution_outcome_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "execution_outcome_v1"
        / str(day_utc).strip()
        / "execution_outcome.v1.json"
    ).resolve()


def _text_lines(value: Any) -> List[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]


def _extract_items(*, stage_id: str, text: str, item_kind: str, markers: Iterable[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for line in _text_lines(text):
        if any(marker in line for marker in markers):
            items.append({"stage_id": stage_id, "item_kind": item_kind, "message": line})
    return items


def derive_execution_outcome_payload(
    *,
    truth_root: Path,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    day_utc = str(context.get("day_utc") or "").strip()
    runs = context.get("runs") if isinstance(context.get("runs"), dict) else {}
    stage_rows: List[Dict[str, Any]] = []
    nonfatal_items: List[Dict[str, Any]] = []
    self_heal_items: List[Dict[str, Any]] = []
    deferred_items: List[Dict[str, Any]] = []

    for stage_id, raw in runs.items():
        if not isinstance(raw, dict):
            continue
        returncode = int(raw.get("returncode") or 0)
        stdout_text = str(raw.get("stdout") or "")
        stderr_text = str(raw.get("stderr") or "")
        stage_self_heal = _extract_items(
            stage_id=str(stage_id),
            text=f"{stdout_text}\n{stderr_text}",
            item_kind="SELF_HEAL",
            markers=SELF_HEAL_MARKERS,
        )
        stage_deferred = _extract_items(
            stage_id=str(stage_id),
            text=f"{stdout_text}\n{stderr_text}",
            item_kind="DEFERRED",
            markers=DEFERRED_MARKERS,
        )
        self_heal_items.extend(stage_self_heal)
        deferred_items.extend(stage_deferred)
        nonfatal_items.extend(stage_self_heal)
        nonfatal_items.extend(stage_deferred)
        stage_rows.append(
            {
                "stage_id": str(stage_id),
                "status": "PASS" if returncode == 0 else "FAIL",
                "returncode": returncode,
                "command": list(raw.get("cmd") or []),
                "self_heal_count": len(stage_self_heal),
                "deferred_count": len(stage_deferred),
            }
        )

    overall_exit_code = int(context.get("overall_exit_code") or 0)
    if overall_exit_code != 0 or any(row["status"] == "FAIL" for row in stage_rows):
        execution_status = "FAIL"
        clean_run_status = "DIRTY"
    elif self_heal_items:
        execution_status = "PASS_WITH_SELF_HEAL"
        clean_run_status = "CLEAN_WITH_SELF_HEAL"
    elif deferred_items:
        execution_status = "PASS_WITH_DEFERRED"
        clean_run_status = "CLEAN_WITH_DEFERRED"
    else:
        execution_status = "PASS"
        clean_run_status = "CLEAN"

    source_artifacts = []
    for row in context.get("source_artifacts") or []:
        if isinstance(row, dict):
            source_artifacts.append(dict(row))

    return {
        "schema_id": "execution_outcome",
        "schema_version": "v1",
        "day_utc": day_utc,
        "release_id": str(context.get("release_id") or "").strip(),
        "git_sha": str(context.get("git_sha") or "").strip(),
        "entrypoint": str(context.get("entrypoint") or "").strip(),
        "overall_exit_code": overall_exit_code,
        "execution_status": execution_status,
        "stages": stage_rows,
        "nonfatal_items": nonfatal_items,
        "self_heal_items": self_heal_items,
        "deferred_items": deferred_items,
        "clean_run_status": clean_run_status,
        "source_artifacts": source_artifacts,
        "generated_at_utc": str(context.get("generated_at_utc") or ""),
    }


def write_execution_outcome_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_execution_outcome_path(
            truth_root=truth_root,
            day_utc=str(payload.get("day_utc") or "").strip(),
        ),
        payload=payload,
        schema_relpath=EXECUTION_OUTCOME_SCHEMA_RELPATH,
        volatile_field_names=("generated_at_utc",),
    )


def load_execution_outcome_context(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"EXECUTION_OUTCOME_CONTEXT_TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj
