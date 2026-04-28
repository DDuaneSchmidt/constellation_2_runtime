#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_operating_contract_v1 import (
    build_aegis_operating_contract_v1,
    write_aegis_operating_contract_v1,
)
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_operating_contract.v1.schema.json"


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    parser = argparse.ArgumentParser(prog="run_aegis_operating_contract_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--mode", default="DRY_RUN", choices=["DRY_RUN", "PAPER_TRANSMIT", "LIVE"])
    parser.add_argument("--run_style", default="MANUAL", choices=["MANUAL", "AUTO"])
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_decision_truth_root_bridge_v1(args.truth_root or "", repo_root=REPO_ROOT, caller="ops/tools/run_aegis_operating_contract_v1.py")
    payload = build_aegis_operating_contract_v1(day_utc=day_utc, mode=args.mode, run_style=args.run_style)
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA)
    path = write_aegis_operating_contract_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    print(json.dumps({"path": str(path), "mode": payload["mode"], "run_style": payload["run_style"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
