#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.exception_state_v1 import materialize_exception_state_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='run_exception_state_v1')
    parser.add_argument('--core2_trade_dir', required=True)
    parser.add_argument('--execution_root', required=True)
    parser.add_argument('--evaluated_at_utc', default='')
    parser.add_argument('--core1_health_path', default='')
    parser.add_argument('--core3_authority_path', default='')
    parser.add_argument('--core4_boundary_path', default='')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args(argv)

    result = materialize_exception_state_v1(
        core2_trade_dir=Path(args.core2_trade_dir).resolve(),
        execution_root=Path(args.execution_root).resolve(),
        evaluated_at_utc=str(args.evaluated_at_utc or '').strip(),
        core1_health_path=str(args.core1_health_path or '').strip() or None,
        core3_authority_path=str(args.core3_authority_path or '').strip() or None,
        core4_boundary_path=str(args.core4_boundary_path or '').strip() or None,
    )
    output = {
        'exception_path': str(result.exception_path),
        'provenance_path': str(result.provenance_path),
        'exception_id': str(result.payload.get('exception_id') or ''),
        'exception_class': str(result.payload.get('exception_class') or ''),
        'classification_status': str(result.payload.get('classification_status') or ''),
        'constraint_posture': str(result.payload.get('constraint_posture') or ''),
        'reason_codes': list(result.payload.get('reason_codes') or []),
    }
    if args.json:
        print(json.dumps(output, sort_keys=True))
    else:
        print(f"OK: EXCEPTION_STATE_V1 exception_id={output['exception_id']} path={output['exception_path']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
