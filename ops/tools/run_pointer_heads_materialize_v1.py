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

from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402


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


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_pointer_heads_materialize_v1")
    ap.add_argument("--fail_if_no_authority_head", required=True, choices=["YES", "NO"])
    ap.add_argument("--truth_root", default="", help="Absolute truth root; defaults to C2_TRUTH_ROOT or repo resolver")
    args = ap.parse_args()

    from constellation_2.phaseC.lib.run_pointer_heads_v1 import (  # noqa: E402
        head_payload,
        resolve_authority_head_from_index,
        resolve_display_head_from_index,
    )

    fail_if_no_auth = str(args.fail_if_no_authority_head).strip().upper() == "YES"
    truth_root = _resolve_truth_root(str(args.truth_root))
    paths = _pointer_paths(truth_root)

    lock_fd = _lock_acquire(paths["out_dir"], paths["lock_path"])
    try:
        display_entry = resolve_display_head_from_index(paths["idx_path"])
        display_obj = head_payload("canonical_display", display_entry)
        _atomic_write_json(paths["display_path"], display_obj)

        try:
            auth_entry = resolve_authority_head_from_index(paths["idx_path"])
            auth_obj = head_payload("canonical_authority", auth_entry)
            _atomic_write_json(paths["authority_path"], auth_obj)
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
            _atomic_write_json(paths["authority_path"], missing)
            if fail_if_no_auth:
                raise SystemExit(f"FAIL: no authority head: {authority_msg}")
    finally:
        _lock_release(lock_fd, paths["lock_path"])

    out = {
        "ok": True,
        "index_path": str(paths["idx_path"]),
        "display_head_path": str(paths["display_path"]),
        "authority_head_path": str(paths["authority_path"]),
        "authority_ok": bool(authority_ok),
        "authority_msg": authority_msg,
    }
    print(json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
