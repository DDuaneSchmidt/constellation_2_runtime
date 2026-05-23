#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.domain_repair_orchestrator_v1 import run_domain_repair_orchestration_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='run_domain_repair_orchestrator_v1')
    parser.add_argument('--truth-root', '--truth_root', dest='truth_root', default='/home/node/constellation_runtime_data/truth')
    parser.add_argument('--day-utc', '--day', dest='day_utc', required=True)
    parser.add_argument('--domain-id', '--domain_id', dest='domain_id', default='')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args(argv)
    result = run_domain_repair_orchestration_v1(
        truth_root=Path(args.truth_root),
        repo_root=REPO_ROOT,
        day_utc=str(args.day_utc),
        domain_id=str(args.domain_id or '').upper() or None,
        execute=bool(args.execute),
    )
    print(json.dumps({
        'ok': True,
        'execute': result.get('execute'),
        'results': result.get('results'),
        'before_summary': result.get('before_summary'),
        'after_summary': result.get('after_summary'),
        'lifecycle_paths': result.get('lifecycle_paths'),
        'domain_certification_hash': result.get('domain_certification_hash'),
        'broker_submit_transmit_allowed': False,
        'autonomous_execution_allowed': False,
        'trade_advice_allowed': False,
    }, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
