#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.orchestration_plane_v1 import materialize_orchestration_plane_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='run_orchestration_plane_v1')
    parser.add_argument('--core2_trade_dir', required=True)
    parser.add_argument('--execution_root', required=True)
    parser.add_argument('--policy_projection_path', required=True)
    parser.add_argument('--evaluated_at_utc', default='')
    parser.add_argument('--cadence_seconds', default='60')
    parser.add_argument('--session_phase', default='REGULAR')
    parser.add_argument('--exception_state_path', default='')
    parser.add_argument('--operator_intervention_state_path', default='')
    parser.add_argument('--prior_state_path', default='')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args(argv)

    result = materialize_orchestration_plane_v1(
        core2_trade_dir=Path(args.core2_trade_dir).resolve(),
        execution_root=Path(args.execution_root).resolve(),
        policy_projection_path=Path(args.policy_projection_path).resolve(),
        evaluated_at_utc=str(args.evaluated_at_utc or '').strip(),
        cadence_seconds=int(str(args.cadence_seconds or '60')),
        session_phase=str(args.session_phase or '').strip(),
        exception_state_path=str(args.exception_state_path or '').strip() or None,
        operator_intervention_state_path=str(args.operator_intervention_state_path or '').strip() or None,
        prior_state_path=str(args.prior_state_path or '').strip() or None,
    )
    output = {
        'state_path': str(result.state_path),
        'trigger_path': None if result.trigger_path is None else str(result.trigger_path),
        'orchestration_state_id': str(result.state_payload.get('orchestration_state_id') or ''),
        'orchestration_posture': str(result.state_payload.get('orchestration_posture') or ''),
        'reason_codes': list(result.state_payload.get('reason_codes') or []),
        'trigger_class': None if result.trigger_payload is None else str(result.trigger_payload.get('trigger_class') or ''),
        'trigger_target_type': None if result.trigger_payload is None else str(result.trigger_payload.get('trigger_target_type') or ''),
    }
    if args.json:
        print(json.dumps(output, sort_keys=True))
    else:
        print(f"OK: ORCHESTRATION_PLANE_V1 state_id={output['orchestration_state_id']} state_path={output['state_path']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
