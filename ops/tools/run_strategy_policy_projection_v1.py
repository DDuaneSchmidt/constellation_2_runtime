#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.strategy_policy_projection_v1 import materialize_strategy_policy_projection_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='run_strategy_policy_projection_v1')
    parser.add_argument('--core2_trade_dir', required=True)
    parser.add_argument('--execution_root', required=True)
    parser.add_argument('--evaluated_at_utc', default='')
    parser.add_argument('--exception_state_path', default='')
    parser.add_argument('--operator_intervention_state_path', default='')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args(argv)

    result = materialize_strategy_policy_projection_v1(
        core2_trade_dir=Path(args.core2_trade_dir).resolve(),
        execution_root=Path(args.execution_root).resolve(),
        evaluated_at_utc=str(args.evaluated_at_utc or '').strip(),
        exception_state_path=str(args.exception_state_path or '').strip() or None,
        operator_intervention_state_path=str(args.operator_intervention_state_path or '').strip() or None,
    )
    output = {
        'policy_projection_path': str(result.policy_path),
        'provenance_path': str(result.provenance_path),
        'policy_projection_id': str(result.payload.get('policy_projection_id') or ''),
        'policy_input_posture': str(result.payload.get('policy_input_posture') or ''),
        'blocker_codes': list(result.payload.get('blocker_codes') or []),
        'degraded_codes': list(result.payload.get('degraded_codes') or []),
        'downstream_execution_posture': str(result.payload.get('downstream_execution_posture') or ''),
    }
    if args.json:
        print(json.dumps(output, sort_keys=True))
    else:
        print(f"OK: STRATEGY_POLICY_PROJECTION_V1 policy_projection_id={output['policy_projection_id']} path={output['policy_projection_path']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
