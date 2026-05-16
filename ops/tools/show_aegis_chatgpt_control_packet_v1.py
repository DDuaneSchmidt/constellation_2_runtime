#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_chatgpt_control_packet_v1 import (  # noqa: E402
    aegis_chatgpt_control_packet_path_v1,
    render_aegis_chatgpt_control_packet_summary_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="show_aegis_chatgpt_control_packet_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--full-json", action="store_true")
    args = parser.parse_args(argv)

    path = aegis_chatgpt_control_packet_path_v1(truth_root=Path(args.truth_root), day_utc=str(args.day))
    if not path.exists():
        raise SystemExit(f"FAIL: Aegis ChatGPT control packet not found: {path}")
    packet = json.loads(path.read_text(encoding="utf-8"))
    if args.full_json:
        print(json.dumps(packet, indent=2, sort_keys=True))
    else:
        print(render_aegis_chatgpt_control_packet_summary_v1(packet))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
