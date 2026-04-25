#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.evaluation_authority_foundation_v1 import materialize_evaluation_authority_slice_v1
from constellation_2.common.truth_root_v1 import resolve_truth_root


def _parse_day(day_utc: str) -> str:
    value = str(day_utc or "").strip()
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {value!r}")
    return value


def _resolve_truth_root(raw: str) -> Path:
    if str(raw or "").strip():
        path = Path(raw).expanduser().resolve()
    else:
        path = resolve_truth_root(repo_root=REPO_ROOT).resolve()
    if not path.exists() or not path.is_dir():
        raise SystemExit(f"FAIL: INVALID_TRUTH_ROOT: {path}")
    return path


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_evaluation_authority_foundation_day_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day in YYYY-MM-DD")
    ap.add_argument("--truth_root", default="", help="Optional canonical truth root override")
    ap.add_argument("--execution_sleeve_id", default="PRIMARY", help="Execution sleeve partition id")
    ap.add_argument("--mode", default="PAPER", help="Execution mode")
    ap.add_argument("--sleeve_id", required=True, help="Logical sleeve id for the authority slice")
    ap.add_argument(
        "--operator_intervention_state_path",
        default="",
        help="Optional operator_intervention_state_v1 path to bind as an override reference",
    )
    args = ap.parse_args()

    result = materialize_evaluation_authority_slice_v1(
        truth_root=_resolve_truth_root(args.truth_root),
        day_utc=_parse_day(args.day_utc),
        execution_sleeve_id=str(args.execution_sleeve_id or "").strip().upper() or "PRIMARY",
        mode=str(args.mode or "").strip().upper() or "PAPER",
        sleeve_id=str(args.sleeve_id or "").strip(),
        operator_intervention_state_path=str(args.operator_intervention_state_path or "").strip() or None,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
