#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_portfolio_control_packet_v1 import CONTROL_PACKET_JSON, CONTROL_PACKET_MD, render_markdown, write_control_packet


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='show_aegis_chatgpt_control_packet_v1')
    parser.add_argument('--truth_root', default='')
    parser.add_argument('--day', default='')
    parser.add_argument('--full-json', action='store_true')
    args = parser.parse_args(argv)

    if not CONTROL_PACKET_JSON.exists() or not CONTROL_PACKET_MD.exists():
        write_control_packet()
    if args.full_json:
        print(json.dumps(json.loads(CONTROL_PACKET_JSON.read_text(encoding='utf-8')), indent=2, sort_keys=True))
    else:
        print(CONTROL_PACKET_MD.read_text(encoding='utf-8'), end='')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
