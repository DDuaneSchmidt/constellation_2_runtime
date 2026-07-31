#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_portfolio_control_packet_v1 import TERMINAL_PASS, audit_control_packet, write_control_packet


def main() -> int:
    write_control_packet()
    result = audit_control_packet()
    print(result['terminal_status'])
    print(json.dumps(result, sort_keys=True))
    return 0 if result['terminal_status'] == TERMINAL_PASS else 1


if __name__ == '__main__':
    raise SystemExit(main())
