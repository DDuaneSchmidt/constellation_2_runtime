#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.release_current_v1 import reduce_release_current_v1


def _default_truth_root_from_active_runtime_contract() -> Path:
    contract_ref = read_control_plane_surface_v1(domain="release", surface="active_runtime_contract")
    canonical_truth_root = Path(str(contract_ref.payload.get("canonical_truth_root") or "")).expanduser().resolve()
    if not canonical_truth_root.is_absolute():
        raise SystemExit(f"FAIL: canonical_truth_root_not_absolute:{canonical_truth_root}")
    return canonical_truth_root


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_release_current_reducer_v1")
    ap.add_argument(
        "--truth_root",
        default="",
    )
    args = ap.parse_args(argv)
    truth_root = (
        Path(str(args.truth_root).strip()).expanduser().resolve()
        if str(args.truth_root).strip()
        else _default_truth_root_from_active_runtime_contract()
    )

    ref = reduce_release_current_v1(
        truth_root=truth_root,
        producer_module="ops/tools/run_release_current_reducer_v1.py",
    )
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "release_current_id": str(ref.payload.get("release_current_id") or ""),
                "activation_state": str(ref.payload.get("activation_state") or ""),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
