from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Mapping

PERIOD_TYPES = {"QTD", "YTD", "Annual"}
ARTIFACT_ID = "advisor_benchmark_v1"
FILENAME = "advisor_benchmark.v1.json"


class AdvisorBenchmarkValidationError(ValueError):
    def __init__(self, field_errors: Mapping[str, str]):
        super().__init__("Advisor benchmark validation failed.")
        self.field_errors = dict(field_errors)


def advisor_benchmark_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / ARTIFACT_ID / str(day_utc) / FILENAME


def write_advisor_benchmark_snapshot_v1(*, truth_root: Path | str, payload: Mapping[str, Any], now_utc: datetime | None = None) -> dict[str, Any]:
    snapshot = validate_advisor_benchmark_snapshot_v1(payload, now_utc=now_utc)
    path = advisor_benchmark_path_v1(truth_root=truth_root, day_utc=snapshot["as_of_date"])
    existing = _read_artifact(path)
    snapshots = [row for row in existing.get("snapshots", []) if isinstance(row, dict)]
    created_at = _now(now_utc)
    replaced = False
    for idx, row in enumerate(snapshots):
        if row.get("as_of_date") == snapshot["as_of_date"] and row.get("period_type") == snapshot["period_type"]:
            snapshot["created_at"] = str(row.get("created_at") or created_at)
            snapshot["updated_at"] = created_at
            snapshots[idx] = snapshot
            replaced = True
            break
    if not replaced:
        snapshot["created_at"] = created_at
        snapshot["updated_at"] = created_at
        snapshots.append(snapshot)
    snapshots = sorted(snapshots, key=lambda row: (str(row.get("as_of_date") or ""), _period_sort(str(row.get("period_type") or ""))))
    artifact = {
        "schema_id": "advisor_benchmark",
        "schema_version": "v1",
        "artifact_id": f"advisor_benchmark:{snapshot['as_of_date']}",
        "day_utc": snapshot["as_of_date"],
        "generated_at_utc": created_at,
        "snapshots": snapshots,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"ok": True, "snapshot": snapshot, "artifact_path": str(path)}


def validate_advisor_benchmark_snapshot_v1(payload: Mapping[str, Any], *, now_utc: datetime | None = None) -> dict[str, Any]:
    errors: dict[str, str] = {}
    as_of = str(payload.get("as_of_date") or "").strip()
    if not as_of:
        errors["as_of_date"] = "as_of_date is required."
    else:
        try:
            date.fromisoformat(as_of)
        except ValueError:
            errors["as_of_date"] = "as_of_date must be a valid YYYY-MM-DD date."
    period_type = str(payload.get("period_type") or "").strip()
    if not period_type:
        errors["period_type"] = "period_type is required."
    elif period_type not in PERIOD_TYPES:
        errors["period_type"] = "period_type must be one of QTD, YTD, Annual."
    raw_return = payload.get("return_pct")
    if raw_return in (None, ""):
        errors["return_pct"] = "return_pct is required."
        return_pct = None
    else:
        try:
            return_pct = float(str(raw_return).replace(",", ""))
        except (TypeError, ValueError):
            errors["return_pct"] = "return_pct must be numeric."
            return_pct = None
    if return_pct is not None and abs(return_pct) > 1000:
        errors["return_pct"] = "return_pct is outside the supported manual benchmark range."
    if errors:
        raise AdvisorBenchmarkValidationError(errors)
    return {
        "as_of_date": as_of,
        "period_type": period_type,
        "return_pct": return_pct,
        "source": str(payload.get("source") or "").strip(),
        "notes": str(payload.get("notes") or "").strip(),
    }


def latest_advisor_benchmark_snapshot_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    base = root / "reports" / ARTIFACT_ID
    rows: list[tuple[str, str, dict[str, Any], Path]] = []
    if not base.exists():
        return {}
    for path in base.glob(f"**/{FILENAME}"):
        payload = _read_artifact(path)
        for snapshot in payload.get("snapshots", []):
            if not isinstance(snapshot, dict):
                continue
            as_of = str(snapshot.get("as_of_date") or "")
            if _valid_day(as_of) and as_of <= str(day_utc):
                row = dict(snapshot)
                row["artifact_path"] = str(path)
                rows.append((as_of, str(snapshot.get("updated_at") or snapshot.get("created_at") or ""), row, path))
    if not rows:
        return {}
    return sorted(rows, key=lambda item: (item[0], item[1], str(item[3])))[-1][2]


def advisor_benchmark_is_stale_v1(snapshot: Mapping[str, Any], *, day_utc: str) -> bool:
    as_of = str(snapshot.get("as_of_date") or "")
    if not _valid_day(as_of):
        return True
    expected = most_recent_expected_quarter_end_v1(day_utc)
    return bool(expected and as_of < expected)


def most_recent_expected_quarter_end_v1(day_utc: str) -> str:
    try:
        current = date.fromisoformat(str(day_utc))
    except ValueError:
        current = datetime.now(UTC).date()
    quarter_ends = [date(current.year, 3, 31), date(current.year, 6, 30), date(current.year, 9, 30), date(current.year, 12, 31)]
    eligible = [day for day in quarter_ends if day <= current]
    if eligible:
        return eligible[-1].isoformat()
    return date(current.year - 1, 12, 31).isoformat()


def _read_artifact(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _valid_day(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _period_sort(value: str) -> int:
    return {"QTD": 0, "YTD": 1, "Annual": 2}.get(value, 9)


def _now(value: datetime | None = None) -> str:
    current = value or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
