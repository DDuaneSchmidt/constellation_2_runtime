#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_release_integrity_status_v1 import (  # noqa: E402
    build_aegis_release_integrity_status_v1,
    now_utc_iso_v1,
    write_aegis_release_integrity_status_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import resolve_fact_plane_truth_root_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="check_aegis_release_integrity_v1")
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--current_runtime_mode", default="PAPER")
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--allow-mismatch-exit-zero", action="store_true")
    args = parser.parse_args(argv)

    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    status = build_aegis_release_integrity_status_v1(
        generated_at_utc=str(args.generated_at_utc or now_utc_iso_v1()),
        current_runtime_mode=str(args.current_runtime_mode or "PAPER").upper(),
    )
    path = write_aegis_release_integrity_status_v1(truth_root=truth_root, payload=status)
    print(json.dumps({**status, "path": str(path)}, sort_keys=True))
    if args.allow_mismatch_exit_zero:
        return 0
    return 0 if status["release_match_status"] == "MATCH" else 2


if __name__ == "__main__":
    raise SystemExit(main())
