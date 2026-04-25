from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from constellation_2.common.constitutional_runtime_v1 import (
    CLOSURE_STATE_BLOCKED,
    CLOSURE_STATE_COMPLETE,
    CLOSURE_STATE_DEGRADED,
    FINALITY_FINALIZED,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_machine_blocker_envelope_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
)


EXECUTION_OUTCOME_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_outcome.v1.schema.json"
SELF_HEAL_MARKERS = ("QUARANTINED_STALE_", "REFRESHED_STALE_", "self_heal=1")
DEFERRED_MARKERS = ("DEFERRED_",)
FATAL_MARKERS = ("Traceback", "FAIL:", "SchemaValidationError", "ImmutableWriteError", "FileNotFoundError")


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


def _has_fatal_signal(text: str) -> bool:
    return any(marker in text for marker in FATAL_MARKERS)


def derive_execution_outcome_payload(
    *,
    truth_root: Path,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(
        Path(__file__).resolve().parents[2],
        "execution_outcome_v1",
        "constellation_2.common.execution_outcome_v1",
    )
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
        combined_text = f"{stdout_text}\n{stderr_text}"
        nonfatal_override = returncode != 0 and bool(stage_self_heal or stage_deferred) and not _has_fatal_signal(combined_text)
        self_heal_items.extend(stage_self_heal)
        deferred_items.extend(stage_deferred)
        nonfatal_items.extend(stage_self_heal)
        nonfatal_items.extend(stage_deferred)
        stage_rows.append(
            {
                "stage_id": str(stage_id),
                "status": "PASS" if returncode == 0 or nonfatal_override else "FAIL",
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
    source_artifact_map = {
        str(row.get("artifact_family") or "").strip(): row
        for row in source_artifacts
        if str(row.get("artifact_family") or "").strip()
    }
    constitutional_dependency_refs: List[Dict[str, Any]] = []
    missing_dependency_artifacts: List[str] = []
    for artifact_id, artifact_class in (
        ("submit_boundary_status_v1", "admission_result"),
        ("execution_journal_v1", "outcome_record"),
    ):
        source_row = source_artifact_map.get(artifact_id)
        if not isinstance(source_row, dict):
            missing_dependency_artifacts.append(artifact_id)
            continue
        path_text = str(source_row.get("artifact_path") or "").strip()
        sha256 = str(source_row.get("artifact_sha256") or "").strip()
        if not path_text or not sha256:
            missing_dependency_artifacts.append(artifact_id)
            continue
        constitutional_dependency_refs.append(
            {
                "artifact_id": artifact_id,
                "path": path_text,
                "sha256": sha256,
                "artifact_class": artifact_class,
                "finality_state": FINALITY_FINALIZED,
            }
        )
    closure_state = CLOSURE_STATE_COMPLETE
    if execution_status in {"PASS_WITH_SELF_HEAL", "PASS_WITH_DEFERRED"}:
        closure_state = CLOSURE_STATE_DEGRADED
    if execution_status == "FAIL" or missing_dependency_artifacts:
        closure_state = CLOSURE_STATE_BLOCKED
    blocking_codes: List[str] = []
    if execution_status == "FAIL":
        blocking_codes.append("EXECUTION_OUTCOME_FAILED")
    if execution_status == "PASS_WITH_SELF_HEAL":
        blocking_codes.append("EXECUTION_OUTCOME_SELF_HEAL")
    if execution_status == "PASS_WITH_DEFERRED":
        blocking_codes.append("EXECUTION_OUTCOME_DEFERRED")
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state=closure_state,
        reason_codes=blocking_codes,
        missing_dependency_artifacts=missing_dependency_artifacts,
    )
    constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type="execution_outcome_v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="execution_outcome_v1",
        declared_dependency_artifacts=[
            str(item).strip()
            for item in (contract.get("required_upstream_dependencies") or [])
            if str(item).strip()
        ],
        dependency_refs=constitutional_dependency_refs,
    )
    constitutional_lineage = build_governed_artifact_lineage_v1(
        artifact_type="execution_outcome_v1",
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="execution_outcome_v1",
        producer_id="constellation_2.common.execution_outcome_v1",
        generated_at_utc=str(context.get("generated_at_utc") or ""),
        effective_at_utc=str(context.get("generated_at_utc") or ""),
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=constitutional_dependency_refs,
        policy_snapshot_refs=[],
        code_version=str(context.get("git_sha") or "").strip(),
        run_id=f"execution_outcome:{day_utc}:{str(context.get('release_id') or '').strip()}",
    )

    return {
        "schema_id": "execution_outcome",
        "schema_version": "v1",
        "day_utc": day_utc,
        "release_id": str(context.get("release_id") or "").strip(),
        "git_sha": str(context.get("git_sha") or "").strip(),
        "entrypoint": str(context.get("entrypoint") or "").strip(),
        "overall_exit_code": overall_exit_code,
        "execution_status": execution_status,
        "closure_state": str(blocker_envelope["closure_state"]),
        "first_blocker_code": str(blocker_envelope["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker_envelope["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": constitutional_dependency_declaration,
        "constitutional_lineage": constitutional_lineage,
        "stages": stage_rows,
        "nonfatal_items": nonfatal_items,
        "self_heal_items": self_heal_items,
        "deferred_items": deferred_items,
        "clean_run_status": clean_run_status,
        "source_artifacts": source_artifacts,
        "generated_at_utc": str(context.get("generated_at_utc") or ""),
    }


def write_execution_outcome_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    validate_governed_artifact_payload_v1(
        repo_root=Path(__file__).resolve().parents[2],
        artifact_id="execution_outcome_v1",
        payload=payload,
        required_finality_states=["finalized", "corrected"],
    )
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
