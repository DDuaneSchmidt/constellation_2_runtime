from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Tuple


STATUS_PASS = "PASS"
STATUS_FAIL_CLOSED = "FAIL_CLOSED"

RC_KILL_SWITCH_AUTHORITY_MISMATCH = "KILL_SWITCH_AUTHORITY_MISMATCH"
RC_KILL_SWITCH_CANONICAL_MISSING = "KILL_SWITCH_CANONICAL_MISSING"


@dataclass(frozen=True)
class KillSwitchAuthorityResultV1:
    status: str
    canonical_path: Path
    canonical_sha256: str
    payload: Dict[str, Any] | None
    state: str
    allow_entries: bool
    allow_exits: bool | None
    reason_codes: Tuple[str, ...]
    reason_code: str | None
    reason_detail: str | None
    sleeve_present: bool
    sleeve_paths: Tuple[Path, ...]
    mismatch_paths: Tuple[Path, ...]


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonical_json_bytes(payload: Dict[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _read_json_dict(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"KILL_SWITCH_TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _normalize_truth_sleeves_root(
    *,
    canonical_truth_root: Path,
    truth_sleeves_root: Path | None,
) -> Path | None:
    if truth_sleeves_root is not None:
        candidate = Path(truth_sleeves_root).resolve()
        return candidate if candidate.exists() and candidate.is_dir() else None

    candidate = (canonical_truth_root.resolve().parent / "truth_sleeves").resolve()
    return candidate if candidate.exists() and candidate.is_dir() else None


def _discover_sleeve_paths(*, truth_sleeves_root: Path | None, day_utc: str) -> Tuple[Path, ...]:
    if truth_sleeves_root is None:
        return ()
    pattern = f"*/*/risk_v1/kill_switch_v1/{day_utc}/global_kill_switch_state.v1.json"
    return tuple(sorted(path.resolve() for path in truth_sleeves_root.glob(pattern) if path.is_file()))


def resolve_kill_switch_authority_v1(
    *,
    canonical_truth_root: str | Path,
    day_utc: str,
    truth_sleeves_root: str | Path | None = None,
) -> KillSwitchAuthorityResultV1:
    canonical_root = Path(canonical_truth_root).resolve()
    canonical_path = (canonical_root / "risk_v1" / "kill_switch_v1" / day_utc / "global_kill_switch_state.v1.json").resolve()
    sleeves_root = _normalize_truth_sleeves_root(
        canonical_truth_root=canonical_root,
        truth_sleeves_root=Path(truth_sleeves_root).resolve() if truth_sleeves_root is not None else None,
    )
    sleeve_paths = _discover_sleeve_paths(truth_sleeves_root=sleeves_root, day_utc=day_utc)
    sleeve_present = bool(sleeve_paths)

    if not canonical_path.exists() or not canonical_path.is_file():
        return KillSwitchAuthorityResultV1(
            status=STATUS_FAIL_CLOSED,
            canonical_path=canonical_path,
            canonical_sha256=_sha256_bytes(b""),
            payload=None,
            state="UNKNOWN",
            allow_entries=False,
            allow_exits=None,
            reason_codes=(RC_KILL_SWITCH_CANONICAL_MISSING,),
            reason_code=RC_KILL_SWITCH_CANONICAL_MISSING,
            reason_detail=f"canonical_path={canonical_path}",
            sleeve_present=sleeve_present,
            sleeve_paths=sleeve_paths,
            mismatch_paths=(),
        )

    canonical_payload = _read_json_dict(canonical_path)
    canonical_sha256 = _sha256_file(canonical_path)
    canonical_bytes = _canonical_json_bytes(canonical_payload)

    mismatch_paths = []
    for sleeve_path in sleeve_paths:
        try:
            sleeve_payload = _read_json_dict(sleeve_path)
            sleeve_bytes = _canonical_json_bytes(sleeve_payload)
        except Exception:
            mismatch_paths.append(sleeve_path)
            continue
        if sleeve_bytes != canonical_bytes:
            mismatch_paths.append(sleeve_path)

    state = str(canonical_payload.get("state") or "UNKNOWN").strip().upper()
    allow_entries = bool(canonical_payload.get("allow_entries") is True)
    allow_exits_raw = canonical_payload.get("allow_exits")
    allow_exits = allow_exits_raw if isinstance(allow_exits_raw, bool) else None
    reason_codes = tuple(
        str(code).strip()
        for code in (canonical_payload.get("reason_codes") or [])
        if str(code).strip()
    )

    if mismatch_paths:
        mismatch_detail = ",".join(str(path) for path in mismatch_paths)
        return KillSwitchAuthorityResultV1(
            status=STATUS_FAIL_CLOSED,
            canonical_path=canonical_path,
            canonical_sha256=canonical_sha256,
            payload=canonical_payload,
            state=state,
            allow_entries=allow_entries,
            allow_exits=allow_exits,
            reason_codes=(RC_KILL_SWITCH_AUTHORITY_MISMATCH,),
            reason_code=RC_KILL_SWITCH_AUTHORITY_MISMATCH,
            reason_detail=f"canonical_path={canonical_path};mismatch_paths={mismatch_detail}",
            sleeve_present=sleeve_present,
            sleeve_paths=sleeve_paths,
            mismatch_paths=tuple(mismatch_paths),
        )

    return KillSwitchAuthorityResultV1(
        status=STATUS_PASS,
        canonical_path=canonical_path,
        canonical_sha256=canonical_sha256,
        payload=canonical_payload,
        state=state,
        allow_entries=allow_entries,
        allow_exits=allow_exits,
        reason_codes=reason_codes,
        reason_code=None,
        reason_detail=None,
        sleeve_present=sleeve_present,
        sleeve_paths=sleeve_paths,
        mismatch_paths=(),
    )
