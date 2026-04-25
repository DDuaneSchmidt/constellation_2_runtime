#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Optional

# ---- BOOTSTRAP REPO ROOT INTO PYTHONPATH ----
_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parents[2]  # ops/tools/ -> repo root
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

if not (_REPO_ROOT / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT}")

from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root  # noqa: E402


def _resolve_truth_root(truth_root_arg: str) -> Path:
    arg = (truth_root_arg or "").strip()
    if arg:
        p = Path(arg).expanduser().resolve()
        if not p.is_absolute():
            raise SystemExit(f"FAIL: --truth_root must be absolute: {p}")
        if not p.exists() or not p.is_dir():
            raise SystemExit(f"FAIL: --truth_root must exist and be a directory: {p}")
        return p
    return resolve_truth_root(repo_root=_REPO_ROOT).resolve()


def _pointer_paths(truth_root: Path) -> Dict[str, Path]:
    out_dir = (truth_root / "run_pointer_v2").resolve()
    return {
        "idx_path": (truth_root / "run_pointer_v1" / "canonical_pointer_index.v1.jsonl").resolve(),
        "out_dir": out_dir,
        "display_path": (out_dir / "canonical_display_head.v1.json").resolve(),
        "authority_path": (out_dir / "canonical_authority_head.v1.json").resolve(),
        "lock_path": (out_dir / ".heads_materialize_v1.lock").resolve(),
    }


def _atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    b = (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    with open(tmp, "wb") as f:
        f.write(b)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def _lock_acquire(out_dir: Path, lock_path: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise SystemExit(f"FAIL: lock busy (heads materializer): {lock_path}")
    os.write(fd, f"pid={os.getpid()}\n".encode("utf-8"))
    os.fsync(fd)
    return fd


def _lock_release(fd: int, lock_path: Path) -> None:
    try:
        os.close(fd)
    finally:
        try:
            os.unlink(str(lock_path))
        except FileNotFoundError:
            pass


def _has_any_submission_lineage(truth_root: Path) -> bool:
    submissions_root = (truth_root / "execution_evidence_v1" / "submissions").resolve()
    if not submissions_root.exists() or not submissions_root.is_dir():
        return False
    for day_dir in submissions_root.iterdir():
        if not day_dir.is_dir():
            continue
        for lineage_dir in day_dir.iterdir():
            if lineage_dir.is_dir():
                return True
    return False


def _failure_head_payload(*, schema_id: str, error: str, reason_codes: list[str]) -> Dict[str, Any]:
    return {
        "schema_id": schema_id,
        "schema_version": "v1",
        "ok": False,
        "status": "UNAVAILABLE",
        "reason_codes": list(reason_codes),
        "error": str(error),
    }


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def _authorization_verdict_path_for_day(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "authorization_gate_verdict_v1"
        / str(day_utc).strip()
        / "authorization_gate_verdict.v1.json"
    ).resolve()


def _active_session_payload(truth_root: Path) -> Dict[str, Any]:
    path = (truth_root / "active_session_v1" / "current.json").resolve()
    if not path.exists() or not path.is_file():
        return {}
    try:
        return _read_json_obj(path)
    except Exception:
        return {}


def _fallback_authority_head_for_expected_day(
    *,
    truth_root: Path,
    expected_day_utc: str,
    current_head_obj: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    expected_day = str(expected_day_utc or "").strip()
    if not expected_day:
        return None

    candidate_roots = [truth_root]
    try:
        canonical_truth_root = resolve_canonical_truth_root().resolve()
    except Exception:
        canonical_truth_root = resolve_truth_root(repo_root=_REPO_ROOT).resolve()
    if canonical_truth_root not in candidate_roots:
        candidate_roots.append(canonical_truth_root)

    active_payload: Dict[str, Any] = {}
    for root in candidate_roots:
        active_payload = _active_session_payload(root)
        if active_payload:
            break
    if str(active_payload.get("active_day") or "").strip() != expected_day:
        return None
    if str(active_payload.get("target_day_admission_status") or "").strip().upper() != "ADMIT":
        return None

    verdict_path: Optional[Path] = None
    verdict_payload: Dict[str, Any] = {}
    for root in candidate_roots:
        candidate_verdict_path = _authorization_verdict_path_for_day(truth_root=root, day_utc=expected_day)
        if not candidate_verdict_path.exists() or not candidate_verdict_path.is_file():
            continue
        try:
            candidate_verdict_payload = _read_json_obj(candidate_verdict_path)
        except Exception:
            continue
        verdict_status = str(candidate_verdict_payload.get("status") or "").strip().upper()
        verdict_day = str(candidate_verdict_payload.get("day_utc") or "").strip()
        if verdict_day == expected_day and verdict_status in {"PASS", "BOOTSTRAP_PASS"}:
            verdict_path = candidate_verdict_path
            verdict_payload = candidate_verdict_payload
            break
    if verdict_path is None:
        return None

    base_head = dict(current_head_obj or {})
    pointer_seq = int(base_head.get("pointer_seq") or 0)
    attempt_seq = int(base_head.get("attempt_seq") or 0)
    mode = str(base_head.get("mode") or "PAPER").strip() or "PAPER"
    producer_git_sha = str(base_head.get("producer_git_sha") or "").strip()
    produced_utc = (
        str(verdict_payload.get("produced_utc") or "").strip()
        or str(verdict_payload.get("generated_utc") or "").strip()
        or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    reason_codes = sorted(
        {
            *[str(code).strip() for code in (base_head.get("reason_codes") or []) if str(code).strip()],
            "ACTIVE_SESSION_PROMOTED_AUTHORITY_HEAD_FALLBACK",
        }
    )
    return {
        "schema_id": "c2_run_pointer_canonical_authority_head",
        "schema_version": "v1",
        "pointer_seq": pointer_seq,
        "day_utc": expected_day,
        "attempt_id": str(base_head.get("attempt_id") or f"{expected_day}__AUTH_VERDICT_FALLBACK"),
        "attempt_seq": attempt_seq,
        "mode": mode,
        "status": "PASS",
        "authoritative": True,
        "produced_utc": produced_utc,
        "producer_git_sha": producer_git_sha,
        "points_to": str(verdict_path.resolve()),
        "reason_codes": reason_codes,
    }


def _verify_materialized_heads(*, paths: Dict[str, Path], expected_day_utc: str) -> Dict[str, Any]:
    display_obj = _read_json_obj(paths["display_path"])
    if str(display_obj.get("schema_id") or "").strip() != "c2_run_pointer_canonical_display_head":
        raise SystemExit(f"FAIL: POINTER_HEADS_DISPLAY_VERIFY_SCHEMA:path={paths['display_path']}")

    authority_obj = _read_json_obj(paths["authority_path"])
    if str(authority_obj.get("schema_id") or "").strip() != "c2_run_pointer_canonical_authority_head":
        raise SystemExit(f"FAIL: POINTER_HEADS_AUTHORITY_VERIFY_SCHEMA:path={paths['authority_path']}")

    authority_status = str(authority_obj.get("status") or "").strip().upper()
    authority_ok = bool(authority_obj.get("ok") is True) or authority_status == "PASS"
    authority_day_utc = str(authority_obj.get("day_utc") or "").strip()
    reason_codes = [str(code).strip() for code in (authority_obj.get("reason_codes") or []) if str(code).strip()]
    if authority_ok and expected_day_utc and authority_day_utc and authority_day_utc != expected_day_utc:
        result_state = "MISMATCH"
        reason_codes = sorted(set(reason_codes + ["TARGET_DAY_DATE_MISMATCH"]))
    elif authority_ok:
        result_state = "PASS"
    else:
        result_state = "UNAVAILABLE"
        if not reason_codes:
            reason_codes = ["CANONICAL_AUTHORITY_HEAD_UNAVAILABLE"]

    return {
        "authority_ok": authority_ok,
        "authority_msg": "OK" if authority_ok else str(authority_obj.get("error") or "UNAVAILABLE"),
        "authority_day_utc": authority_day_utc,
        "result_state": result_state,
        "reason_codes": reason_codes,
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_pointer_heads_materialize_v1")
    ap.add_argument("--fail_if_no_authority_head", required=True, choices=["YES", "NO"])
    ap.add_argument("--expected_day_utc", default="", help="Optional target-day expectation for result classification.")
    ap.add_argument("--truth_root", default="", help="Absolute truth root; defaults to C2_TRUTH_ROOT or repo resolver")
    args = ap.parse_args()

    from constellation_2.phaseC.lib.run_pointer_heads_v1 import (  # noqa: E402
        head_payload,
        resolve_authority_head_from_index,
        resolve_display_head_from_index,
    )

    fail_if_no_auth = str(args.fail_if_no_authority_head).strip().upper() == "YES"
    expected_day_utc = str(args.expected_day_utc or "").strip()
    truth_root = _resolve_truth_root(str(args.truth_root))
    paths = _pointer_paths(truth_root)

    lock_fd = _lock_acquire(paths["out_dir"], paths["lock_path"])
    try:
        try:
            display_entry = resolve_display_head_from_index(paths["idx_path"])
        except Exception as e:
            # No pointer index is expected when there has been no governed submission lineage yet.
            if _has_any_submission_lineage(truth_root):
                raise
            missing_msg = str(e)
            reason_codes = ["NO_POINTER_INDEX_NO_SUBMISSIONS"]
            display_obj = _failure_head_payload(
                schema_id="c2_run_pointer_canonical_display_head",
                error=missing_msg,
                reason_codes=reason_codes,
            )
            authority_obj = _failure_head_payload(
                schema_id="c2_run_pointer_canonical_authority_head",
                error=missing_msg,
                reason_codes=reason_codes,
            )
            _atomic_write_json(paths["display_path"], display_obj)
            _atomic_write_json(paths["authority_path"], authority_obj)
            out = {
                "ok": True,
                "index_path": str(paths["idx_path"]),
                "display_head_path": str(paths["display_path"]),
                "authority_head_path": str(paths["authority_path"]),
                "authority_ok": False,
                "authority_msg": missing_msg,
                "result_state": "UNAVAILABLE",
                "reason_codes": reason_codes,
                "authority_day_utc": "",
                "expected_day_utc": expected_day_utc,
            }
            print(json.dumps(out, sort_keys=True))
            return 0

        display_obj = head_payload("canonical_display", display_entry)
        if not str(display_obj.get("status") or "").strip():
            display_obj["status"] = "PASS" if bool(display_obj.get("ok")) else "FAIL"
        display_obj["reason_codes"] = list(display_obj.get("reason_codes") or [])
        _atomic_write_json(paths["display_path"], display_obj)

        try:
            auth_entry = resolve_authority_head_from_index(paths["idx_path"])
            auth_obj = head_payload("canonical_authority", auth_entry)
            if not str(auth_obj.get("status") or "").strip():
                auth_obj["status"] = "PASS" if bool(auth_obj.get("ok")) else "FAIL"
            auth_obj["reason_codes"] = list(auth_obj.get("reason_codes") or [])
            if expected_day_utc and str(auth_obj.get("day_utc") or "").strip() != expected_day_utc:
                fallback_head = _fallback_authority_head_for_expected_day(
                    truth_root=truth_root,
                    expected_day_utc=expected_day_utc,
                    current_head_obj=auth_obj,
                )
                if fallback_head is not None:
                    auth_obj = fallback_head
            _atomic_write_json(paths["authority_path"], auth_obj)
            authority_ok = True
            authority_msg = "OK"
        except Exception as e:
            authority_ok = False
            authority_msg = str(e)
            fallback_head = _fallback_authority_head_for_expected_day(
                truth_root=truth_root,
                expected_day_utc=expected_day_utc,
            )
            if fallback_head is not None:
                _atomic_write_json(paths["authority_path"], fallback_head)
                auth_obj = dict(fallback_head)
                authority_ok = True
                authority_msg = "OK"
            else:
                missing = _failure_head_payload(
                    schema_id="c2_run_pointer_canonical_authority_head",
                    error=authority_msg,
                    reason_codes=["CANONICAL_AUTHORITY_HEAD_UNAVAILABLE"],
                )
                _atomic_write_json(paths["authority_path"], missing)
                auth_obj = dict(missing)
                if fail_if_no_auth:
                    raise SystemExit(f"FAIL: no authority head: {authority_msg}")
    finally:
        _lock_release(lock_fd, paths["lock_path"])

    verification = _verify_materialized_heads(paths=paths, expected_day_utc=expected_day_utc)
    authority_day_utc = str(verification["authority_day_utc"] or "")
    reason_codes = list(verification["reason_codes"])
    result_state = str(verification["result_state"] or "UNAVAILABLE")
    authority_ok = bool(verification["authority_ok"])
    authority_msg = str(verification["authority_msg"] or "")

    out = {
        "ok": True,
        "index_path": str(paths["idx_path"]),
        "display_head_path": str(paths["display_path"]),
        "authority_head_path": str(paths["authority_path"]),
        "authority_ok": bool(authority_ok),
        "authority_msg": authority_msg,
        "result_state": result_state,
        "reason_codes": reason_codes,
        "authority_day_utc": authority_day_utc,
        "expected_day_utc": expected_day_utc,
    }
    print(json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
