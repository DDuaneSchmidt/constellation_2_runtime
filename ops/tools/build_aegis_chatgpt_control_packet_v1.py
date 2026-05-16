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
    build_aegis_chatgpt_control_packet_v1,
    render_aegis_chatgpt_control_packet_summary_v1,
    write_aegis_chatgpt_control_packet_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_chatgpt_control_packet_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--json", action="store_true", help="Print compact machine-readable command result.")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    packet = build_aegis_chatgpt_control_packet_v1(
        truth_root=truth_root,
        day_utc=str(args.day),
        generated_at_utc=args.generated_at_utc or None,
    )
    out_path = write_aegis_chatgpt_control_packet_v1(truth_root=truth_root, payload=packet)
    result = {
        "path": str(out_path),
        "day_utc": packet["day_utc"],
        "runtime_truth_classification": packet["runtime_truth_classification"],
        "trade_advice_allowed": packet["trade_advice_allowed"],
        "manual_trade_capture_allowed": packet["manual_trade_capture_allowed"],
        "reason_if_blocked": packet["reason_if_blocked"],
        "do_not_claim_count": len(packet["do_not_claim"]),
        "broker_submit_required": False,
        "ib_automation_required": False,
    }
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(render_aegis_chatgpt_control_packet_summary_v1(packet))
        print("")
        print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
