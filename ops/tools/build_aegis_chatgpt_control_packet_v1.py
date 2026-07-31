#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_portfolio_control_packet_v1 import build_control_packet, render_markdown, write_control_packet


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='build_aegis_chatgpt_control_packet_v1')
    parser.add_argument('--truth_root', default='')
    parser.add_argument('--day', default='')
    parser.add_argument('--generated_at_utc', default='')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args(argv)

    packet = build_control_packet()
    paths = write_control_packet(packet)
    result = {
        'terminal_status': 'CONTROL_PACKET_GENERATED',
        'path': str(paths['json']),
        'markdown_path': str(paths['markdown']),
        'packet_version': packet['packet_metadata']['packet_version'],
        'repo_path': packet['packet_metadata']['repo_path'],
        'broker_submit_required': False,
        'ib_automation_required': False,
        'autonomous_execution_allowed': False,
    }
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(render_markdown(packet))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
