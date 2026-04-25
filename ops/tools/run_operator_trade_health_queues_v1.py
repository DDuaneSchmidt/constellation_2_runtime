#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.operator_trade_health_v1 import materialize_operator_trade_health_queues_v1
from constellation_2.common.execution_identity_binding_v1 import resolve_governed_execution_identity_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_operator_trade_health_queues_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--execution_root", default="")
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--sleeve_id", default="PRIMARY")
    args = parser.parse_args(argv)

    if str(args.execution_root or "").strip():
        execution_root = Path(str(args.execution_root).strip()).expanduser().resolve()
    else:
        identity = resolve_governed_execution_identity_v1(repo_root=REPO_ROOT, environment=str(args.environment or "").strip().upper(), sleeve_id=str(args.sleeve_id or "").strip().upper())
        execution_root = resolve_sleeve_execution_root_v1(repo_root=REPO_ROOT, environment=identity.environment, ib_account=identity.account_id, sleeve_id=identity.sleeve_id).execution_root_path
    result = materialize_operator_trade_health_queues_v1(repo_root=REPO_ROOT, execution_root_path=Path(execution_root), day_utc=str(args.day_utc))
    output = dict(result.summary)
    output["report_path"] = str(result.report_path)
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
