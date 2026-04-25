from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from constellation_2.common.control_plane_read_gateway_v1 import (
    ControlPlaneReadCollectionV1,
    ControlPlaneReadRefV1,
    read_control_plane_collection_v1,
    read_control_plane_surface_v1,
)
from constellation_2.common.runtime_base_v1 import advisor_runtime_root
from constellation_2.common.truth_root_v1 import resolve_runtime_root, resolve_truth_root


REPO_ROOT = Path(__file__).resolve().parents[3]
GLOBAL_TRUTH_ROOT = (resolve_runtime_root() / "truth").resolve()
SLEEVE_TRUTH_ROOT = resolve_truth_root(repo_root=REPO_ROOT).resolve()
ADVISORY_RUNTIME_ROOT = advisor_runtime_root().resolve()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def is_day_str(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except Exception:
        return False


def read_json(path: Path) -> Tuple[Optional[Any], Optional[str]]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle), None
    except FileNotFoundError:
        return None, "FILE_NOT_FOUND"
    except json.JSONDecodeError:
        return None, "JSON_DECODE_ERROR"
    except Exception:
        return None, "READ_ERROR"


def read_json_dict(path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    obj, err = read_json(path)
    if obj is None:
        return None, err
    if not isinstance(obj, dict):
        return None, "TOP_LEVEL_NOT_OBJECT"
    return obj, None


def read_control_plane_surface_dict(
    *,
    domain: str,
    surface: str,
    truth_root: Optional[Path] = None,
    truth_sleeves_root: Optional[Path] = None,
    day: Optional[str] = None,
    sleeve_id: str = "PRIMARY",
    environment: str = "PAPER",
) -> Tuple[Optional[ControlPlaneReadRefV1], Optional[str]]:
    try:
        ref = read_control_plane_surface_v1(
            domain=domain,
            surface=surface,
            truth_root=truth_root,
            truth_sleeves_root=truth_sleeves_root,
            day_utc=day,
            sleeve_id=sleeve_id,
            environment=environment,
        )
    except Exception as exc:
        return None, f"{type(exc).__name__}:{exc}"
    return ref, None


def read_control_plane_collection(
    *,
    domain: str,
    surface: str,
    truth_root: Path,
    day: str,
) -> Tuple[Optional[ControlPlaneReadCollectionV1], Optional[str]]:
    try:
        collection = read_control_plane_collection_v1(
            domain=domain,
            surface=surface,
            truth_root=truth_root,
            day_utc=day,
        )
    except Exception as exc:
        return None, f"{type(exc).__name__}:{exc}"
    return collection, None


def path_mtime(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except Exception:
        return None


def iso_from_mtime(path: Path) -> Optional[str]:
    mt = path_mtime(path)
    if mt is None:
        return None
    return datetime.fromtimestamp(mt, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def list_day_dirs(root: Path) -> List[str]:
    if not root.exists() or not root.is_dir():
        return []
    days = [path.name for path in root.iterdir() if path.is_dir() and is_day_str(path.name)]
    days.sort()
    return days


def latest_day_from_roots(roots: Sequence[Path]) -> Optional[str]:
    candidates: List[str] = []
    for root in roots:
        candidates.extend(list_day_dirs(root))
    if not candidates:
        return None
    candidates = sorted(set(candidates))
    return candidates[-1]


def resolve_ui_day(day: Optional[str]) -> Optional[str]:
    if is_day_str(day):
        return str(day)

    active_ref, _ = read_control_plane_surface_dict(
        domain="session",
        surface="active_session_current",
        truth_root=GLOBAL_TRUTH_ROOT,
    )
    active_doc = active_ref.payload if active_ref is not None else {}
    active_day = str(active_doc.get("active_day") or "").strip() if isinstance(active_doc, dict) else ""
    if is_day_str(active_day):
        return active_day

    return latest_day_from_roots(
        [
            GLOBAL_TRUTH_ROOT / "reports" / "current_system_projection_v1",
            SLEEVE_TRUTH_ROOT / "execution_evidence_v1" / "submissions",
            SLEEVE_TRUTH_ROOT / "positions_v1" / "snapshots",
            SLEEVE_TRUTH_ROOT / "reports" / "reconciliation_report_v3",
        ]
    )


def first_existing(paths: Sequence[Path]) -> Optional[Path]:
    for path in paths:
        if path.exists():
            return path
    return None


def latest_existing_file(paths: Iterable[Path]) -> Optional[Path]:
    existing = [path for path in paths if path.exists() and path.is_file()]
    if not existing:
        return None
    existing.sort(key=lambda item: str(item))
    return existing[-1]


def normalize_symbol(item: Dict[str, Any]) -> Optional[str]:
    instrument = item.get("instrument")
    if isinstance(instrument, dict):
        for key in ("symbol", "underlying"):
            value = instrument.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    value = item.get("symbol")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def latest_timestamp(*values: Any) -> Optional[str]:
    candidates = [str(value).strip() for value in values if isinstance(value, str) and str(value).strip()]
    if not candidates:
        return None
    return max(candidates)


def freshness_state(*timestamps: Optional[str], stale_after_hours: int = 6) -> str:
    present = [value for value in timestamps if isinstance(value, str) and value]
    if not present:
        return "unknown"
    latest = max(present)
    try:
        normalized = latest.replace("Z", "+00:00")
        age = datetime.now(timezone.utc) - datetime.fromisoformat(normalized)
    except Exception:
        return "unknown"
    return "stale" if age.total_seconds() > stale_after_hours * 3600 else "healthy"


def provenance_markers(*markers: str) -> List[str]:
    out: List[str] = []
    for marker in markers:
        normalized = str(marker or "").strip()
        if normalized and normalized not in out:
            out.append(normalized)
    return out or ["unknown"]


def evidence_ref(path: Optional[Path], *, label: Optional[str] = None, artifact_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if path is None:
        return None
    return {
        "label": label or path.name,
        "path": str(path.resolve()),
        "artifact_type": artifact_type or path.name,
        "last_update_utc": iso_from_mtime(path),
    }


def read_pointer_target(path: Path) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    doc, _ = read_json_dict(path)
    if not isinstance(doc, dict):
        return None, None
    raw = doc
    target = None
    if "artifact_path" in raw:
        value = raw.get("artifact_path")
        if isinstance(value, str) and value.strip():
            target = Path(value).resolve()
    elif "pointers" in raw and isinstance(raw["pointers"], dict):
        value = raw["pointers"].get("snapshot_path")
        if isinstance(value, str) and value.strip():
            target = Path(value).resolve()
    return target, doc
