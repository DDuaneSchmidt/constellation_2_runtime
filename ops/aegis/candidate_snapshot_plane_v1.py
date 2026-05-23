from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA_ID = "immutable_candidate_snapshot"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "immutable_candidate_snapshot_v1"

CERTIFIED = "CERTIFIED"
PROVISIONAL = "PROVISIONAL"
NON_EXECUTION_STATES = {"PROVISIONAL_INTRADAY", "PARTIAL_DATA_AVAILABLE", "STALE", "CERTIFICATION_PENDING", "INVALID"}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")


def stable_hash_v1(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_bytes(payload)).hexdigest()


def _strings(value: Any) -> list[str]:
    return sorted({str(item or "").strip() for item in (value if isinstance(value, list) else []) if str(item or "").strip()})


def _manifest_certification_state(manifest: Mapping[str, Any]) -> str:
    return str(manifest.get("certification_state") or manifest.get("final_eod_certification_status") or "").strip().upper()


def _manifest_lane(manifest: Mapping[str, Any]) -> str:
    lane = str(manifest.get("candidate_lane") or manifest.get("candidate_visibility_lane") or "").strip().upper()
    if lane in {CERTIFIED, PROVISIONAL}:
        return lane
    return CERTIFIED if _manifest_certification_state(manifest) == CERTIFIED else PROVISIONAL


def annotate_candidate_rows_v1(*, rows: list[dict[str, Any]], lane: str, certification_state: str, input_snapshot_ids: list[str]) -> list[dict[str, Any]]:
    execution_eligible = lane == CERTIFIED and certification_state == CERTIFIED
    out: list[dict[str, Any]] = []
    for row in rows:
        next_row = dict(row)
        next_row["candidate_lane"] = lane
        next_row["certification_state"] = certification_state
        next_row["certification_label"] = "CERTIFIED" if execution_eligible else "NON_CERTIFIED"
        next_row["execution_eligible"] = execution_eligible
        next_row["read_only"] = not execution_eligible
        next_row["input_market_data_snapshot_ids"] = list(input_snapshot_ids)
        out.append(next_row)
    return out


def build_candidate_snapshot_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    candidate_manifest: Mapping[str, Any],
    source_manifest_path: str = "",
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    lane = _manifest_lane(candidate_manifest)
    certification_state = _manifest_certification_state(candidate_manifest) or ("CERTIFIED" if lane == CERTIFIED else "CERTIFICATION_PENDING")
    input_snapshot_ids = _strings(candidate_manifest.get("input_market_data_snapshot_ids") or candidate_manifest.get("input_snapshot_ids"))
    rows = candidate_manifest.get("candidate_rows") if isinstance(candidate_manifest.get("candidate_rows"), list) else []
    annotated_rows = annotate_candidate_rows_v1(rows=[dict(row) for row in rows if isinstance(row, dict)], lane=lane, certification_state=certification_state, input_snapshot_ids=input_snapshot_ids)
    content_basis = {
        "day_utc": day_utc,
        "lane": lane,
        "certification_state": certification_state,
        "input_market_data_snapshot_ids": input_snapshot_ids,
        "candidate_rows": annotated_rows,
        "source_manifest_hash": stable_hash_v1(candidate_manifest),
    }
    content_hash = stable_hash_v1(content_basis)
    snapshot_id = f"candidate-snapshot:{day_utc}:{lane}:{content_hash[:16]}"
    path = root / "reports" / REPORT_FAMILY / day_utc / lane.lower() / f"{snapshot_id}.json"
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "trading_day": day_utc,
        "created_at_utc": _now(),
        "lane": lane,
        "certification_state": certification_state,
        "certification_label": "CERTIFIED" if lane == CERTIFIED and certification_state == CERTIFIED else "NON_CERTIFIED",
        "read_only": not (lane == CERTIFIED and certification_state == CERTIFIED),
        "execution_eligible": lane == CERTIFIED and certification_state == CERTIFIED,
        "input_market_data_snapshot_ids": input_snapshot_ids,
        "candidate_count": len(annotated_rows),
        "candidate_rows": annotated_rows,
        "content_hash": content_hash,
        "lineage": {
            "source_candidate_manifest_path": source_manifest_path,
            "source_candidate_manifest_hash": stable_hash_v1(candidate_manifest),
            "promotion_source_snapshot_id": "",
            "certified_market_data_snapshot_id": "",
        },
        "safety": {
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_enabled": False,
        },
        "artifact_path": str(path),
    }
    payload["artifact_hash"] = stable_hash_v1(payload)
    return payload


def write_candidate_snapshot_v1(*, truth_root: Path | str, snapshot: Mapping[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    day = str(snapshot["trading_day"])
    lane = str(snapshot["lane"]).lower()
    snapshot_id = str(snapshot["snapshot_id"])
    path = root / "reports" / REPORT_FAMILY / day / lane / f"{snapshot_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(snapshot)
    payload["artifact_path"] = str(path)
    data = _json_bytes(payload) + b"\n"
    if path.exists() and path.read_bytes() != data:
        raise ValueError(f"immutable candidate snapshot collision: {path}")
    if not path.exists():
        path.write_bytes(data)
    return {"json": str(path), "snapshot_id": snapshot_id, "hash": stable_hash_v1(payload)}


def promote_certified_candidate_snapshot_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    provisional_snapshot: Mapping[str, Any],
    certified_market_data_snapshot_id: str,
) -> dict[str, Any]:
    if str(certified_market_data_snapshot_id or "").strip() == "":
        raise ValueError("certified_market_data_snapshot_id is required for promotion")
    rows = provisional_snapshot.get("candidate_rows") if isinstance(provisional_snapshot.get("candidate_rows"), list) else []
    manifest_like = {
        "candidate_rows": rows,
        "candidate_lane": CERTIFIED,
        "certification_state": CERTIFIED,
        "input_market_data_snapshot_ids": [certified_market_data_snapshot_id],
    }
    promoted = build_candidate_snapshot_v1(truth_root=truth_root, day_utc=day_utc, candidate_manifest=manifest_like, source_manifest_path=str(provisional_snapshot.get("artifact_path") or ""))
    promoted["lineage"]["promotion_source_snapshot_id"] = str(provisional_snapshot.get("snapshot_id") or "")
    promoted["lineage"]["certified_market_data_snapshot_id"] = certified_market_data_snapshot_id
    promoted["promotion_audit"] = {
        "promoted_at_utc": _now(),
        "source_lane": str(provisional_snapshot.get("lane") or ""),
        "source_snapshot_id": str(provisional_snapshot.get("snapshot_id") or ""),
        "certified_market_data_snapshot_id": certified_market_data_snapshot_id,
        "overwrite_performed": False,
    }
    promoted["artifact_hash"] = stable_hash_v1(promoted)
    return promoted


def execution_firewall_validate_candidate_snapshot_v1(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    lane = str(snapshot.get("lane") or "").upper()
    certification_state = str(snapshot.get("certification_state") or "").upper()
    if lane != CERTIFIED or certification_state != CERTIFIED:
        return {
            "status": "REJECTED",
            "reason": "NON_CERTIFIED_CANDIDATE_SNAPSHOT",
            "lane": lane,
            "certification_state": certification_state,
            "execution_allowed": False,
        }
    rows = snapshot.get("candidate_rows") if isinstance(snapshot.get("candidate_rows"), list) else []
    if any(not isinstance(row, dict) or row.get("execution_eligible") is not True for row in rows):
        return {
            "status": "REJECTED",
            "reason": "CANDIDATE_ROW_NOT_EXECUTION_ELIGIBLE",
            "lane": lane,
            "certification_state": certification_state,
            "execution_allowed": False,
        }
    return {
        "status": "PASS",
        "reason": "",
        "lane": lane,
        "certification_state": certification_state,
        "execution_allowed": True,
    }


def replay_candidate_snapshot_v1(*, truth_root: Path | str, day_utc: str, snapshot_ids: list[str]) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    snapshots: list[dict[str, Any]] = []
    for snapshot_id in snapshot_ids:
        matches = sorted((root / "reports" / REPORT_FAMILY / day_utc).glob(f"*/{snapshot_id}.json"))
        if not matches:
            raise FileNotFoundError(snapshot_id)
        payload = json.loads(matches[0].read_text(encoding="utf-8"))
        snapshots.append(
            {
                "snapshot_id": str(payload.get("snapshot_id") or ""),
                "lane": str(payload.get("lane") or ""),
                "certification_state": str(payload.get("certification_state") or ""),
                "candidate_count": int(payload.get("candidate_count") or 0),
                "content_hash": str(payload.get("content_hash") or ""),
                "input_market_data_snapshot_ids": _strings(payload.get("input_market_data_snapshot_ids")),
            }
        )
    return {
        "schema_id": "candidate_snapshot_replay",
        "schema_version": "v1",
        "day_utc": day_utc,
        "snapshot_ids": snapshot_ids,
        "snapshot_count": len(snapshots),
        "snapshots": snapshots,
        "replay_status": "PASS",
        "safety": {
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_enabled": False,
        },
    }
