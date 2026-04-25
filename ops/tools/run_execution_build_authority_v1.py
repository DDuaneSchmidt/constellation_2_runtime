#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.execution_build_authority_v1 import run_execution_build_authority_v1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog='run_execution_build_authority_v1')
    ap.add_argument('--operation_type', required=True)
    ap.add_argument('--candidate_path', required=True)
    ap.add_argument('--materialize', choices=['YES', 'NO'], default='YES')
    ap.add_argument('--emit_package', choices=['YES', 'NO'], default='YES')
    args = ap.parse_args(argv)

    result = run_execution_build_authority_v1(
        repo_root=REPO_ROOT,
        operation_type=str(args.operation_type).strip(),
        candidate_path=Path(str(args.candidate_path).strip()).resolve(),
        materialize=(str(args.materialize).strip().upper() == 'YES'),
        emit_package=(str(args.emit_package).strip().upper() == 'YES'),
    )
    summary = {
        'build_path': str(result['build_path']),
        'package_path': (str(result['package_path']) if result['package_path'] is not None else None),
        'closure_status': result['build_obj']['closure_status'],
        'first_real_blocker': result['build_obj']['first_real_blocker'],
        'unowned_dependencies': result['build_obj']['unowned_dependencies'],
    }
    print(json.dumps(summary, sort_keys=True, separators=(',', ':'), ensure_ascii=False))
    return 0 if result['build_obj']['closure_status'] == 'COMPLETE' else 2


if __name__ == '__main__':
    raise SystemExit(main())
