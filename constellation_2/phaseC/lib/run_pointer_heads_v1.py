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


def _read_jsonl_strict(path: Path) -> List[Dict[str, Any]]:
    """
    FAIL-CLOSED JSONL reader for canonical_pointer_index.v1.jsonl.

    Integrity rules:
    - File must exist
    - File must be newline-terminated (atomic append invariant for JSONL)
    - Every non-empty line must be valid JSON object
    - No duplicate pointer_seq
    - No duplicate attempt_id
    - No duplicate (day_utc, attempt_seq)
    """
    if not path.exists():
        raise RuntimeError(f"FAIL: pointer index missing: {path}")

    raw = path.read_bytes()
    if raw == b"":
        raise RuntimeError(f"FAIL: pointer index empty: {path}")

    # Critical: newline-termination required. If missing, treat as truncation / partial write.
    if not raw.endswith(b"\n"):
        raise RuntimeError(f"FAIL: pointer index not newline-terminated (possible truncation): {path}")

    try:
        text = raw.decode("utf-8")
    except Exception as e:
        raise RuntimeError(f"FAIL: pointer index not utf-8 decodable: {path} err={e!r}")

    rows: List[Dict[str, Any]] = []
    seen_pointer_seq: set[int] = set()
    seen_attempt_id: set[str] = set()
    seen_day_attempt: set[Tuple[str, int]] = set()

    # splitlines() is safe here because we already enforced newline termination above.
    for i, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception as e:
            raise RuntimeError(f"FAIL: pointer index has invalid JSONL line={i}: {path} err={e!r}")

        if not isinstance(obj, dict):
            raise RuntimeError(f"FAIL: pointer index line={i} not JSON object: {path}")

        # Duplicate detection (tamper / bypass)
        try:
            ps = int(obj.get("pointer_seq"))
        except Exception:
            ps = None  # validated later by consumers
        if isinstance(ps, int):
            if ps in seen_pointer_seq:
                raise RuntimeError(f"FAIL: pointer index duplicate pointer_seq={ps}: {path}")
            seen_pointer_seq.add(ps)

        aid = str(obj.get("attempt_id") or "").strip()
        if aid:
            if aid in seen_attempt_id:
                raise RuntimeError(f"FAIL: pointer index duplicate attempt_id={aid}: {path}")
            seen_attempt_id.add(aid)

        day = str(obj.get("day_utc") or "").strip()
        try:
            aseq = int(obj.get("attempt_seq"))
        except Exception:
            aseq = None
        if day and isinstance(aseq, int):
            key = (day, aseq)
            if key in seen_day_attempt:
                raise RuntimeError(f"FAIL: pointer index duplicate (day_utc,attempt_seq)={key}: {path}")
            seen_day_attempt.add(key)

        rows.append(obj)

    if not rows:
        raise RuntimeError(f"FAIL: pointer index empty (no JSON objects): {path}")
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
    rows = _read_jsonl_strict(idx_path)
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
    rows = _read_jsonl_strict(idx_path)
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
