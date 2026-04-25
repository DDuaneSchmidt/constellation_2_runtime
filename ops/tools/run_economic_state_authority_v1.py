#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.economic_state_authority_v1 import run_economic_state_authority_v1



def main() -> int:
    ap = argparse.ArgumentParser(prog='run_economic_state_authority_v1')
    ap.add_argument('--operation_type', required=True)
    ap.add_argument('--day_utc', required=True)
    ap.add_argument('--sleeve_id', required=True)
    ap.add_argument('--environment', required=True)
    ap.add_argument('--ib_account', required=True)
    ap.add_argument('--truth_root', default=None)
    ap.add_argument('--materialize', default='YES', choices=['YES', 'NO'])
    ap.add_argument('--emit_package', default='YES', choices=['YES', 'NO'])
    args = ap.parse_args()

    result = run_economic_state_authority_v1(
        repo_root=Path(__file__).resolve().parents[2],
        operation_type=str(args.operation_type).strip(),
        day_utc=str(args.day_utc).strip(),
        sleeve_id=str(args.sleeve_id).strip(),
        environment=str(args.environment).strip(),
        ib_account=str(args.ib_account).strip(),
        materialize=(str(args.materialize).strip().upper() == 'YES'),
        emit_package=(str(args.emit_package).strip().upper() == 'YES'),
    )
    print(json.dumps({
        'build_path': str(result['build_path']),
        'package_path': (str(result['package_path']) if result['package_path'] is not None else None),
        'closure_status': result['build_obj']['closure_status'],
        'first_real_blocker': result['build_obj']['first_real_blocker'],
        'materialized_nodes': result['build_obj']['materialized_nodes'],
    }, sort_keys=True))
    return 0 if result['build_obj']['closure_status'] == 'COMPLETE' else 2


if __name__ == '__main__':
    raise SystemExit(main())
