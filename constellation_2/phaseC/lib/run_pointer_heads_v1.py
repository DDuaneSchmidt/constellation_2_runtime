from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class PointerEntry:
    pointer_seq: int
    day_utc: str
    attempt_id: str
    attempt_seq: int
    mode: str
    status: str
    authoritative: bool
    produced_utc: str
    producer_git_sha: str
    points_to: str


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise RuntimeError(f"FAIL: pointer index missing: {path}")
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        obj = json.loads(s)
        if isinstance(obj, dict):
            rows.append(obj)
    if not rows:
        raise RuntimeError(f"FAIL: pointer index empty: {path}")
    return rows


def _as_entry(obj: Dict[str, Any]) -> PointerEntry:
    return PointerEntry(
        pointer_seq=int(obj["pointer_seq"]),
        day_utc=str(obj["day_utc"]),
        attempt_id=str(obj["attempt_id"]),
        attempt_seq=int(obj["attempt_seq"]),
        mode=str(obj["mode"]),
        status=str(obj["status"]),
        authoritative=bool(obj["authoritative"]),
        produced_utc=str(obj["produced_utc"]),
        producer_git_sha=str(obj["producer_git_sha"]),
        points_to=str(obj["points_to"]),
    )


def resolve_display_head_from_index(idx_path: Path) -> PointerEntry:
    rows = _read_jsonl(idx_path)
    best: Optional[PointerEntry] = None
    for obj in rows:
        if str(obj.get("schema_id") or "") != "C2_RUN_POINTER_CANONICAL_POINTER_INDEX_V1":
            continue
        e = _as_entry(obj)
        if best is None or e.pointer_seq > best.pointer_seq:
            best = e
    if best is None:
        raise RuntimeError(f"FAIL: no valid pointer entries in index: {idx_path}")
    return best


def resolve_authority_head_from_index(idx_path: Path) -> PointerEntry:
    rows = _read_jsonl(idx_path)
    best: Optional[PointerEntry] = None
    for obj in rows:
        if str(obj.get("schema_id") or "") != "C2_RUN_POINTER_CANONICAL_POINTER_INDEX_V1":
            continue
        st = str(obj.get("status") or "").upper()
        auth = bool(obj.get("authoritative"))
        if (st != "PASS") or (not auth):
            continue
        e = _as_entry(obj)
        if best is None or e.pointer_seq > best.pointer_seq:
            best = e
    if best is None:
        raise RuntimeError(f"FAIL: no authority head (authoritative=true AND status=PASS) in index: {idx_path}")
    return best


def head_payload(kind: str, entry: PointerEntry) -> Dict[str, Any]:
    # Deterministic JSON shape
    return {
        "schema_id": f"c2_run_pointer_{kind}_head",
        "schema_version": "v1",
        "pointer_seq": entry.pointer_seq,
        "day_utc": entry.day_utc,
        "attempt_id": entry.attempt_id,
        "attempt_seq": entry.attempt_seq,
        "mode": entry.mode,
        "status": entry.status,
        "authoritative": entry.authoritative,
        "produced_utc": entry.produced_utc,
        "producer_git_sha": entry.producer_git_sha,
        "points_to": entry.points_to,
    }
