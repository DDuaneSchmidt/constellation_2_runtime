from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "execution_evidence_current_head.v1"


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"INVALID_JSON_OBJECT:{path}")
    return payload


def _parse_ts(value: str, fallback: datetime) -> datetime:
    text = str(value or "").strip()
    if not text:
        return fallback
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).astimezone(UTC)
    except ValueError:
        return fallback


def _candidate_paths(execution_root: Path, day_utc: str) -> list[Path]:
    evidence_root = (execution_root / "execution_evidence_v1").resolve()
    latest = (evidence_root / "latest_pointer.v1.json").resolve()
    day_pointer = (evidence_root / "submissions" / day_utc / "latest_pointer.v1.json").resolve()
    out: list[Path] = []
    if latest.exists() and latest.is_file():
        out.append(latest)
    if day_pointer.exists() and day_pointer.is_file():
        out.append(day_pointer)
    return out


def _replay_snapshot_paths(execution_root: Path, day_utc: str) -> list[Path]:
    replay_day_root = (execution_root / "execution_stream_v1" / "replays" / day_utc).resolve()
    if not replay_day_root.exists() or not replay_day_root.is_dir():
        return []
    snapshots: list[Path] = []
    for replay_dir in sorted([p for p in replay_day_root.iterdir() if p.is_dir() and not p.name.startswith("_")], reverse=True):
        snapshot = (replay_dir / "execution_stream_snapshot.v1.json").resolve()
        if snapshot.exists() and snapshot.is_file():
            snapshots.append(snapshot)
    return snapshots


def _target_path_from_pointer(pointer_obj: dict[str, Any]) -> str:
    pointers = pointer_obj.get("pointers")
    if isinstance(pointers, dict):
        target = str(pointers.get("submissions_day_dir") or "").strip()
        if target:
            return target
    return str(pointer_obj.get("points_to") or pointer_obj.get("target_path") or "").strip()


def _target_day_mismatch(target_path: Path, day_utc: str) -> bool:
    parts = list(target_path.parts)
    for token in parts:
        if len(token) == 10 and token[4:5] == "-" and token[7:8] == "-":
            return token != day_utc
    return False


def _selected_attempt_id(target: Path) -> str:
    if target.is_dir():
        candidates = sorted(
            [p for p in target.iterdir() if p.is_dir() and not p.name.startswith("_")],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return candidates[0].name
    return ""


def _attempt_id_from_submission_record_path(*, source_submission_record_path: str, day_utc: str) -> str:
    path_text = str(source_submission_record_path or "").strip()
    if not path_text:
        return ""
    record_path = Path(path_text).resolve()
    if not record_path.exists() or not record_path.is_file():
        return ""
    if record_path.name != "broker_submission_record.v2.json":
        return ""
    try:
        record_obj = _read_json(record_path)
    except Exception:
        return ""

    attempt_id = str(record_obj.get("attempt_id") or "").strip()
    if attempt_id:
        return attempt_id

    submission_id = str(record_obj.get("submission_id") or "").strip()
    if not submission_id:
        return ""

    parts = list(record_path.parts)
    try:
        submissions_idx = parts.index("submissions")
    except ValueError:
        return ""
    if submissions_idx + 2 >= len(parts):
        return ""
    path_day = str(parts[submissions_idx + 1]).strip()
    path_submission_id = str(parts[submissions_idx + 2]).strip()
    if path_day != str(day_utc).strip():
        return ""
    if path_submission_id != submission_id:
        return ""
    if record_path.parent.name != submission_id:
        return ""
    return submission_id


def _selected_attempt_id_from_replay(*, replay_obj: dict[str, Any], day_utc: str) -> str:
    direct = str(replay_obj.get("selected_attempt_id") or replay_obj.get("attempt_id") or "").strip()
    if direct:
        return direct
    return _attempt_id_from_submission_record_path(
        source_submission_record_path=str(replay_obj.get("source_submission_record_path") or ""),
        day_utc=day_utc,
    )


def evaluate_execution_evidence_current_head_v1(
    *,
    day_utc: str,
    execution_root: Path,
    sleeve: str = "PRIMARY",
    environment: str = "PAPER",
) -> dict[str, Any]:
    execution_root = Path(execution_root).resolve()
    rejected_candidates: list[dict[str, str]] = []
    valid: list[tuple[datetime, Path, Path, str]] = []

    for pointer_path in _candidate_paths(execution_root, day_utc):
        try:
            pointer_obj = _read_json(pointer_path)
        except Exception:
            rejected_candidates.append({"path": str(pointer_path), "reason": "FAILED_STATUS"})
            continue

        pointer_text = str(pointer_path)
        if "QUARANTINED" in pointer_text.upper() or "INVALID" in pointer_text.upper():
            rejected_candidates.append({"path": str(pointer_path), "reason": "QUARANTINED"})
            continue

        pointer_day = str(pointer_obj.get("day_utc") or pointer_obj.get("asof_day_utc") or "").strip()
        if pointer_day and pointer_day != day_utc:
            rejected_candidates.append({"path": str(pointer_path), "reason": "STALE_DAY"})
            continue

        status = str(pointer_obj.get("status") or "").strip().upper()
        if status and status not in {"OK", "PASS"}:
            rejected_candidates.append({"path": str(pointer_path), "reason": "FAILED_STATUS"})
            continue

        target_text = _target_path_from_pointer(pointer_obj)
        if not target_text:
            rejected_candidates.append({"path": str(pointer_path), "reason": "TARGET_MISSING"})
            continue

        target_path = Path(target_text).resolve()
        if not target_path.exists():
            rejected_candidates.append({"path": str(pointer_path), "reason": "TARGET_MISSING"})
            continue

        if _target_day_mismatch(target_path, day_utc):
            rejected_candidates.append({"path": str(pointer_path), "reason": "DAY_MISMATCH"})
            continue

        fallback_ts = datetime.fromtimestamp(pointer_path.stat().st_mtime, tz=UTC)
        produced = _parse_ts(
            str(pointer_obj.get("produced_utc") or pointer_obj.get("generated_at_utc") or pointer_obj.get("as_of_utc") or ""),
            fallback=fallback_ts,
        )
        valid.append((produced, pointer_path, target_path, ""))

    for replay_path in _replay_snapshot_paths(execution_root, day_utc):
        try:
            replay_obj = _read_json(replay_path)
        except Exception:
            rejected_candidates.append({"path": str(replay_path), "reason": "FAILED_STATUS"})
            continue

        replay_day = str(replay_obj.get("day") or replay_obj.get("day_utc") or "").strip()
        if replay_day and replay_day != day_utc:
            rejected_candidates.append({"path": str(replay_path), "reason": "STALE_DAY"})
            continue

        replay_status = str(replay_obj.get("status") or "").strip().upper()
        if replay_status and replay_status not in {"OK", "PASS"}:
            rejected_candidates.append({"path": str(replay_path), "reason": "FAILED_STATUS"})
            continue

        fallback_ts = datetime.fromtimestamp(replay_path.stat().st_mtime, tz=UTC)
        produced = _parse_ts(
            str(replay_obj.get("generated_at_utc") or replay_obj.get("produced_utc") or ""),
            fallback=fallback_ts,
        )
        selected_attempt = _selected_attempt_id_from_replay(replay_obj=replay_obj, day_utc=day_utc)
        valid.append((produced, replay_path, replay_path, selected_attempt))

    valid.sort(key=lambda item: item[0], reverse=True)
    if not valid:
        return {
            "schema_version": SCHEMA_VERSION,
            "day": day_utc,
            "sleeve": sleeve,
            "environment": environment,
            "status": "FAIL",
            "selected_attempt_id": "",
            "selected_artifact_path": "",
            "rejected_candidates": rejected_candidates,
            "generated_at_utc": _utc_now_iso(),
        }

    _, selected_pointer, selected_target, selected_attempt_id = valid[0]
    for _, pointer_path, _, _ in valid[1:]:
        rejected_candidates.append({"path": str(pointer_path), "reason": "STALE_DAY"})

    resolved_selected_attempt_id = selected_attempt_id or _selected_attempt_id(selected_target)
    if not resolved_selected_attempt_id:
        rejected_candidates.append({"path": str(selected_pointer), "reason": "ATTEMPT_ID_MISSING"})
        return {
            "schema_version": SCHEMA_VERSION,
            "day": day_utc,
            "sleeve": sleeve,
            "environment": environment,
            "status": "FAIL",
            "selected_attempt_id": "",
            "selected_artifact_path": str(selected_target),
            "selected_pointer_path": str(selected_pointer),
            "rejected_candidates": rejected_candidates,
            "generated_at_utc": _utc_now_iso(),
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "day": day_utc,
        "sleeve": sleeve,
        "environment": environment,
        "status": "PASS",
        "selected_attempt_id": resolved_selected_attempt_id,
        "selected_artifact_path": str(selected_target),
        "selected_pointer_path": str(selected_pointer),
        "rejected_candidates": rejected_candidates,
        "generated_at_utc": _utc_now_iso(),
    }


def current_head_output_path(*, execution_root: Path, day_utc: str) -> Path:
    return (Path(execution_root).resolve() / "execution_evidence_v1" / "current_head" / day_utc / "current_head.v1.json").resolve()


def write_execution_evidence_current_head_v1(*, execution_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = current_head_output_path(execution_root=execution_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return output_path
