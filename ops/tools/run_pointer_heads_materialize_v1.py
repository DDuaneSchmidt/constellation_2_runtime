#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

# ---- BOOTSTRAP REPO ROOT INTO PYTHONPATH ----
_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parents[2]  # ops/tools/ -> repo root
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

if not (_REPO_ROOT / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT}")

TRUTH_ROOT = (_REPO_ROOT / "constellation_2/runtime/truth").resolve()
IDX_PATH = (TRUTH_ROOT / "run_pointer_v1" / "canonical_pointer_index.v1.jsonl").resolve()
OUT_DIR = (TRUTH_ROOT / "run_pointer_v2").resolve()

DISPLAY_PATH = (OUT_DIR / "canonical_display_head.v1.json").resolve()
AUTHORITY_PATH = (OUT_DIR / "canonical_authority_head.v1.json").resolve()
LOCK_PATH = (OUT_DIR / ".heads_materialize_v1.lock").resolve()


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


def _lock_acquire() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise SystemExit(f"FAIL: lock busy (heads materializer): {LOCK_PATH}")
    os.write(fd, f"pid={os.getpid()}\n".encode("utf-8"))
    os.fsync(fd)
    return fd


def _lock_release(fd: int) -> None:
    try:
        os.close(fd)
    finally:
        try:
            os.unlink(str(LOCK_PATH))
        except FileNotFoundError:
            pass


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_pointer_heads_materialize_v1")
    ap.add_argument("--fail_if_no_authority_head", required=True, choices=["YES", "NO"])
    args = ap.parse_args()

    from constellation_2.phaseC.lib.run_pointer_heads_v1 import (  # noqa: E402
        head_payload,
        resolve_authority_head_from_index,
        resolve_display_head_from_index,
    )

    fail_if_no_auth = str(args.fail_if_no_authority_head).strip().upper() == "YES"

    lock_fd = _lock_acquire()
    try:
        display_entry = resolve_display_head_from_index(IDX_PATH)
        display_obj = head_payload("canonical_display", display_entry)
        _atomic_write_json(DISPLAY_PATH, display_obj)

        try:
            auth_entry = resolve_authority_head_from_index(IDX_PATH)
            auth_obj = head_payload("canonical_authority", auth_entry)
            _atomic_write_json(AUTHORITY_PATH, auth_obj)
            authority_ok = True
            authority_msg = "OK"
        except Exception as e:
            authority_ok = False
            authority_msg = str(e)
            missing = {
                "schema_id": "c2_run_pointer_canonical_authority_head",
                "schema_version": "v1",
                "ok": False,
                "error": authority_msg,
            }
            _atomic_write_json(AUTHORITY_PATH, missing)
            if fail_if_no_auth:
                raise SystemExit(f"FAIL: no authority head: {authority_msg}")
    finally:
        _lock_release(lock_fd)

    out = {
        "ok": True,
        "index_path": str(IDX_PATH),
        "display_head_path": str(DISPLAY_PATH),
        "authority_head_path": str(AUTHORITY_PATH),
        "authority_ok": bool(authority_ok),
        "authority_msg": authority_msg,
    }
    print(json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
