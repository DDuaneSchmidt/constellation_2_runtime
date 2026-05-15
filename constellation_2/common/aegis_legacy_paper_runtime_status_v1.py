from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_lite_operating_status_v1 import LEGACY_DEFERRED_MARKER, LEGACY_PAPER_TIMER_NAMES
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/legacy_paper_runtime_status.v1.schema.json"


def now_utc_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def legacy_paper_runtime_status_path_v1(*, truth_root: Path) -> Path:
    return Path(truth_root).resolve() / "reports/legacy_paper_runtime_status_v1/current/legacy_paper_runtime_status.v1.json"


def build_legacy_paper_runtime_status_v1(*, generated_at_utc: str, repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    disabled_timers: list[str] = []
    deferred_services: list[str] = []
    remaining: list[str] = []
    for timer_name in LEGACY_PAPER_TIMER_NAMES:
        timer_path = Path(repo_root) / "ops/systemd/user" / timer_name
        service_name = timer_name.replace(".timer", ".service")
        service_path = Path(repo_root) / "ops/systemd/user" / service_name
        timer_text = timer_path.read_text(encoding="utf-8") if timer_path.exists() else ""
        service_text = service_path.read_text(encoding="utf-8") if service_path.exists() else ""
        if LEGACY_DEFERRED_MARKER in timer_text:
            disabled_timers.append(timer_name)
        else:
            remaining.append(timer_name)
        if LEGACY_DEFERRED_MARKER in service_text or "ConditionEnvironment=AEGIS_ENABLE_LEGACY_PAPER_AUTOMATION=1" in service_text:
            deferred_services.append(service_name)
        else:
            remaining.append(service_name)
    payload = {
        "schema_id": "legacy_paper_runtime_status",
        "schema_version": "v1",
        "artifact_id": "legacy_paper_runtime_status_v1",
        "generated_at_utc": generated_at_utc,
        "disabled_timers": sorted(set(disabled_timers)),
        "deferred_services": sorted(set(deferred_services)),
        "remaining_legacy_components": sorted(set(remaining)),
        "legacy_runtime_active": bool(remaining),
        "lite_operational_spine_active": not bool(remaining),
        "reason_codes": [] if not remaining else ["LEGACY_PAPER_RUNTIME_COMPONENTS_REMAIN_ACTIVE"],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def validate_legacy_paper_runtime_status_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)


def write_legacy_paper_runtime_status_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_legacy_paper_runtime_status_v1(payload)
    path = legacy_paper_runtime_status_path_v1(truth_root=truth_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path
