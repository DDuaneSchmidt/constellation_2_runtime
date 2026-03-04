#!/usr/bin/env python3
"""
c2_truth_root_resolver_v1.py

Governed sleeve truth root resolver (fail-closed).

Inputs:
  --sleeve_id <ID>
  --mode PAPER|LIVE
Output:
  absolute path to the sleeve truth root directory

Fail-closed on:
  - registry missing / invalid JSON
  - sleeve_id not found or disabled
  - mode mismatch (registry is authoritative)
  - truth_partition not canonical
  - resolved path does not exist on disk
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List


REPO_ROOT = "/home/node/constellation_2_runtime"
REGISTRY_PATH = os.path.join(REPO_ROOT, "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json")
RUNTIME_ROOT = os.path.join(REPO_ROOT, "constellation_2/runtime")


def die(msg: str, code: int = 2) -> None:
    print(f"ABORT: {msg}", file=sys.stderr)
    sys.exit(code)


def load_registry(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        die(f"registry_missing path={path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        die(f"registry_json_parse_failed path={path} err={type(e).__name__}:{e}")


def require_str(obj: Dict[str, Any], key: str) -> str:
    v = obj.get(key, None)
    if not isinstance(v, str) or not v.strip():
        die(f"registry_invalid_field key={key} expected=nonempty_string got={repr(v)}")
    return v


def require_bool(obj: Dict[str, Any], key: str) -> bool:
    v = obj.get(key, None)
    if not isinstance(v, bool):
        die(f"registry_invalid_field key={key} expected=bool got={repr(v)}")
    return v


def require_list_str(obj: Dict[str, Any], key: str) -> List[str]:
    v = obj.get(key, None)
    if not isinstance(v, list) or any((not isinstance(x, str) or not x.strip()) for x in v):
        die(f"registry_invalid_field key={key} expected=list[str] got={repr(v)}")
    return v


def canonical_partition(sleeve_id: str, mode: str) -> str:
    return f"truth_sleeves/{sleeve_id}/{mode}"


def main() -> None:
    p = argparse.ArgumentParser(description="Resolve absolute sleeve truth root (fail-closed).")
    p.add_argument("--sleeve_id", required=True, help="Sleeve identifier (e.g. PRIMARY)")
    p.add_argument("--mode", required=True, choices=["PAPER", "LIVE"], help="Sleeve mode")
    args = p.parse_args()

    sleeve_id = args.sleeve_id.strip()
    mode = args.mode.strip()

    reg = load_registry(REGISTRY_PATH)

    schema_id = reg.get("schema_id")
    schema_version = reg.get("schema_version")
    if schema_id != "c2_sleeve_registry" or schema_version != "v1":
        die(f"registry_schema_mismatch expected=(c2_sleeve_registry,v1) got=({schema_id},{schema_version})")

    sleeves = reg.get("sleeves", None)
    if not isinstance(sleeves, list):
        die("registry_invalid_field key=sleeves expected=list")

    match = None
    for s in sleeves:
        if isinstance(s, dict) and s.get("sleeve_id") == sleeve_id:
            match = s
            break

    if match is None:
        die(f"sleeve_not_found sleeve_id={sleeve_id}")

    enabled = require_bool(match, "enabled")
    if not enabled:
        die(f"sleeve_disabled sleeve_id={sleeve_id}")

    reg_mode = require_str(match, "mode")
    if reg_mode != mode:
        die(f"mode_mismatch sleeve_id={sleeve_id} requested={mode} registry={reg_mode}")

    _ = require_str(match, "ib_account")
    _ = require_list_str(match, "symbols")  # explicit list per contract (may be empty, but must be list[str])

    truth_partition = require_str(match, "truth_partition")
    expected_partition = canonical_partition(sleeve_id, mode)
    if truth_partition != expected_partition:
        die(f"truth_partition_mismatch sleeve_id={sleeve_id} expected={expected_partition} got={truth_partition}")

    abs_root = os.path.join(RUNTIME_ROOT, truth_partition)

    # Fail-closed: partition path must exist.
    if not os.path.isdir(abs_root):
        die(f"truth_partition_path_missing path={abs_root}")

    # Print only the absolute path (machine-friendly).
    print(abs_root)


if __name__ == "__main__":
    main()
