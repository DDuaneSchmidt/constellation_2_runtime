#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.operator_intervention_state_v1 import materialize_operator_intervention_state_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='run_operator_intervention_state_v1')
    parser.add_argument('--core2_trade_dir', required=True)
    parser.add_argument('--execution_root', required=True)
    parser.add_argument('--emitted_at_utc', default='')
    parser.add_argument('--review_status', default='NOT_REQUIRED')
    parser.add_argument('--override_status', default='NONE')
    parser.add_argument('--override_scope_class', default='NONE')
    parser.add_argument('--override_expiry_utc', default='')
    parser.add_argument('--acknowledgement_status', default='NOT_REQUIRED')
    parser.add_argument('--human_decision_class', default='NO_HUMAN_DECISION')
    parser.add_argument('--allowed_action_codes', default='')
    parser.add_argument('--one_shot', default='true')
    parser.add_argument('--durable', default='false')
    parser.add_argument('--exception_state_path', default='')
    parser.add_argument('--core3_authority_path', default='')
    parser.add_argument('--core4_boundary_path', default='')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args(argv)

    allowed_action_codes = [part.strip() for part in str(args.allowed_action_codes or '').split(',') if part.strip()]
    result = materialize_operator_intervention_state_v1(
        core2_trade_dir=Path(args.core2_trade_dir).resolve(),
        execution_root=Path(args.execution_root).resolve(),
        emitted_at_utc=str(args.emitted_at_utc or '').strip(),
        review_status=str(args.review_status or '').strip(),
        override_status=str(args.override_status or '').strip(),
        override_scope_class=str(args.override_scope_class or '').strip(),
        override_expiry_utc=str(args.override_expiry_utc or '').strip(),
        acknowledgement_status=str(args.acknowledgement_status or '').strip(),
        human_decision_class=str(args.human_decision_class or '').strip(),
        allowed_action_codes=allowed_action_codes,
        one_shot=str(args.one_shot).strip().lower() == 'true',
        durable=str(args.durable).strip().lower() == 'true',
        exception_state_path=str(args.exception_state_path or '').strip() or None,
        core3_authority_path=str(args.core3_authority_path or '').strip() or None,
        core4_boundary_path=str(args.core4_boundary_path or '').strip() or None,
    )
    output = {
        'intervention_path': str(result.intervention_path),
        'provenance_path': str(result.provenance_path),
        'intervention_id': str(result.payload.get('intervention_id') or ''),
        'override_status': str(result.payload.get('override_status') or ''),
        'review_status': str(result.payload.get('review_status') or ''),
        'reason_codes': list(result.payload.get('reason_codes') or []),
    }
    if args.json:
        print(json.dumps(output, sort_keys=True))
    else:
        print(f"OK: OPERATOR_INTERVENTION_STATE_V1 intervention_id={output['intervention_id']} path={output['intervention_path']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
