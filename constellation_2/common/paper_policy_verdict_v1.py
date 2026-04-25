from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.common.capability_state_v1 import (
    CAPABILITY_POLICY_REGISTRY_RELPATH,
    resolve_capability_state_path,
    resolve_paper_policy_verdict_path,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
)


PAPER_POLICY_VERDICT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_policy_verdict.v1.schema.json"

_PAPER_BOOTSTRAP_STARTUP_MATERIALIZATION_TOLERATED_REASONS = frozenset(
    {
        "STARTUP_MATERIALIZATION_FAIL:NO_SUPPORTED_IDENTITY_OUTPUTS",
        "STARTUP_MATERIALIZATION_UNKNOWN:LATEST_ACTIVE_ATTEMPT_POINTER_MISSING",
    }
)
_PAPER_BOOTSTRAP_STARTUP_MATERIALIZATION_REASON_CODE = (
    "PAPER_BOOTSTRAP_TOLERATED:STARTUP_MATERIALIZATION_NO_ACTIVE_ATTEMPT"
)
_STARTUP_PHASEC_VETO_FAIL_CLOSED_CODE = "STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"
_OPTIONS_SNAPSHOT_ROOT_MISSING_PREFIX = "OPTIONS_SNAPSHOT_ROOT_MISSING"
_PAPER_STARTUP_OPTIONS_PENDING_REASON_CODE = "PAPER_START_ALLOWED_OPTIONS_SNAPSHOT_PENDING"


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def _safe_read_json(path: Path) -> Dict[str, Any] | None:
    try:
        return _read_json(path)
    except Exception:
        return None


def _registry_rows(repo_root: Path) -> tuple[Dict[str, Any], Path]:
    path = (Path(repo_root).resolve() / CAPABILITY_POLICY_REGISTRY_RELPATH).resolve()
    return _read_json(path), path


def _extract_veto_paths_from_startup_payload(startup_payload: Dict[str, Any]) -> List[Path]:
    paths: List[Path] = []
    evidence = startup_payload.get("path_resolution_evidence")
    if isinstance(evidence, dict):
        pointer_raw = str(evidence.get("latest_active_attempt_path") or "").strip()
        if pointer_raw:
            pointer_payload = _safe_read_json(Path(pointer_raw))
            if isinstance(pointer_payload, dict):
                attempt_dir_raw = str(pointer_payload.get("attempt_dir") or "").strip()
                if attempt_dir_raw:
                    attempt_dir = Path(attempt_dir_raw)
                    if attempt_dir.exists() and attempt_dir.is_dir():
                        paths.extend(sorted(attempt_dir.glob("*.veto_record.v1.json"), key=lambda p: p.name))
    phasec_result = startup_payload.get("phasec_materializer_result")
    stdout = str(phasec_result.get("stdout") or "") if isinstance(phasec_result, dict) else ""
    for match in re.finditer(r"path=(\S+\.veto_record\.v1\.json)", stdout):
        paths.append(Path(match.group(1)))
    deduped: List[Path] = []
    seen: set[str] = set()
    for candidate in paths:
        resolved = candidate.resolve()
        resolved_key = str(resolved)
        if resolved_key in seen:
            continue
        seen.add(resolved_key)
        deduped.append(resolved)
    return deduped


def _startup_materialization_options_snapshot_pending(*, startup_payload: Dict[str, Any]) -> bool:
    startup_codes = [
        str(code).strip()
        for code in (startup_payload.get("blocking_codes") or [])
        if str(code).strip()
    ]
    if _STARTUP_PHASEC_VETO_FAIL_CLOSED_CODE not in startup_codes:
        return False
    for code in startup_codes:
        if code == _STARTUP_PHASEC_VETO_FAIL_CLOSED_CODE:
            continue
        if code.startswith("STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:OPTIONS_SNAPSHOT_ROOT_MISSING"):
            continue
        return False
    veto_paths = _extract_veto_paths_from_startup_payload(startup_payload)
    if not veto_paths:
        return False
    for veto_path in veto_paths:
        veto_payload = _safe_read_json(veto_path)
        if not isinstance(veto_payload, dict):
            return False
        reason_code = str(veto_payload.get("reason_code") or "").strip()
        reason_detail = str(veto_payload.get("reason_detail") or "").strip()
        if reason_code != "C2_SUBMIT_FAIL_CLOSED_REQUIRED":
            return False
        if not reason_detail.startswith(_OPTIONS_SNAPSHOT_ROOT_MISSING_PREFIX):
            return False
    return True


def _paper_startup_tolerance_reason_codes(
    *,
    environment: str,
    item: Dict[str, Any],
    capability_state_row: Dict[str, Any],
    truth_root: Path,
    day_utc: str,
) -> List[str]:
    if str(environment).strip().upper() != "PAPER":
        return []
    if str(item.get("capability_id") or "").strip() != "startup_materialization_ready":
        return []
    if str(item.get("status") or "").strip().upper() != "FAIL":
        return []
    reason_codes = frozenset(str(code).strip() for code in (item.get("reason_codes") or []) if str(code).strip())
    if reason_codes == _PAPER_BOOTSTRAP_STARTUP_MATERIALIZATION_TOLERATED_REASONS:
        return [_PAPER_BOOTSTRAP_STARTUP_MATERIALIZATION_REASON_CODE]

    startup_path = None
    for source in capability_state_row.get("source_artifacts") or []:
        if not isinstance(source, dict):
            continue
        if str(source.get("artifact_family") or "").strip() != "startup_materialization_v1":
            continue
        candidate = str(source.get("artifact_path") or "").strip()
        if candidate:
            startup_path = Path(candidate)
            break
    if startup_path is None:
        startup_path = (Path(truth_root).resolve() / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").resolve()
    startup_payload = _safe_read_json(startup_path)
    if not isinstance(startup_payload, dict):
        return []
    if _startup_materialization_options_snapshot_pending(startup_payload=startup_payload):
        return [_PAPER_STARTUP_OPTIONS_PENDING_REASON_CODE]
    return []


def derive_paper_policy_verdict_payload(
    *,
    repo_root: Path,
    truth_root: Path,
    capability_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    capability_payload = capability_ref.payload
    registry_payload, registry_path = _registry_rows(repo_root)
    capability_map = {
        str(row.get("capability_id") or "").strip(): row
        for row in (capability_payload.get("capabilities") or [])
        if isinstance(row, dict)
    }
    blocking_items: List[Dict[str, Any]] = []
    advisory_items: List[Dict[str, Any]] = []
    production_only_open_items: List[Dict[str, Any]] = []

    environment = str(capability_payload.get("environment") or "").strip().upper()
    day_utc = str(capability_payload.get("day_utc") or "").strip()
    for row in registry_payload.get("capabilities") or []:
        if not isinstance(row, dict):
            continue
        capability_id = str(row.get("capability_id") or "").strip()
        capability_state = capability_map.get(capability_id) or {}
        status = str(capability_state.get("status") or "UNKNOWN").strip().upper()
        item = {
            "capability_id": capability_id,
            "status": status,
            "paper_role": str(row.get("paper_role") or "").strip().upper(),
            "production_role": str(row.get("production_role") or "").strip().upper(),
            "reason_codes": list(capability_state.get("reason_codes") or []),
            "rationale": str(row.get("rationale") or "").strip(),
        }
        if status == "PASS":
            continue
        tolerance_reason_codes = _paper_startup_tolerance_reason_codes(
            environment=environment,
            item=item,
            capability_state_row=capability_state,
            truth_root=truth_root,
            day_utc=day_utc,
        )
        if tolerance_reason_codes:
            tolerated_item = dict(item)
            tolerated_item["paper_role"] = "ADVISORY"
            tolerated_reason_codes = list(tolerated_item.get("reason_codes") or [])
            tolerated_reason_codes.extend(tolerance_reason_codes)
            tolerated_item["reason_codes"] = sorted({str(code).strip() for code in tolerated_reason_codes if str(code).strip()})
            tolerated_item["rationale"] = (
                f"{tolerated_item['rationale']} "
                "PAPER startup mode allows open-state readiness while options snapshot capture is pending; submit boundary remains fail-closed."
            ).strip()
            advisory_items.append(tolerated_item)
            if tolerated_item["production_role"] == "BLOCKING":
                production_only_open_items.append(tolerated_item)
            continue
        if item["paper_role"] == "BLOCKING":
            blocking_items.append(item)
        else:
            advisory_items.append(item)
        if item["paper_role"] != item["production_role"] and item["production_role"] == "BLOCKING":
            production_only_open_items.append(item)

    source_artifacts = list(capability_payload.get("source_artifacts") or [])
    source_artifacts.extend(
        [
            {
                "artifact_family": "capability_state_v1",
                "artifact_path": str(capability_ref.path),
                "artifact_sha256": capability_ref.sha256,
            },
            {
                "artifact_family": "capability_policy_registry_v1",
                "artifact_path": str(registry_path),
                "artifact_sha256": capability_payload.get("registry_ref", {}).get("artifact_sha256") or "",
            },
        ]
    )
    source_artifacts = sorted(
        {
            (row["artifact_family"], row["artifact_path"], row["artifact_sha256"]): row
            for row in source_artifacts
            if isinstance(row, dict)
        }.values(),
        key=lambda row: (row["artifact_family"], row["artifact_path"]),
    )

    return {
        "schema_id": "paper_policy_verdict",
        "schema_version": "v1",
        "day_utc": str(capability_payload.get("day_utc") or "").strip(),
        "environment": str(capability_payload.get("environment") or "").strip(),
        "ib_account": str(capability_payload.get("ib_account") or "").strip(),
        "sleeve_id": str(capability_payload.get("sleeve_id") or "").strip(),
        "overall_status": "PASS" if not blocking_items else "FAIL",
        "paper_allowed": not blocking_items,
        "blocking_items": blocking_items,
        "advisory_items": advisory_items,
        "production_only_open_items": production_only_open_items,
        "capability_state_ref": {
            "artifact_path": str(capability_ref.path),
            "artifact_sha256": capability_ref.sha256,
        },
        "source_artifacts": source_artifacts,
        "release_id": str(capability_payload.get("release_id") or "").strip(),
        "git_sha": str(capability_payload.get("git_sha") or "").strip(),
        "generated_at_utc": str(capability_payload.get("generated_at_utc") or ""),
    }


def read_capability_state_ref(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_capability_state_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/capability_state.v1.schema.json",
    )


def write_paper_policy_verdict_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_paper_policy_verdict_path(truth_root=truth_root, day_utc=str(payload.get("day_utc") or "").strip()),
        payload=payload,
        schema_relpath=PAPER_POLICY_VERDICT_SCHEMA_RELPATH,
        volatile_field_names=("generated_at_utc",),
    )
