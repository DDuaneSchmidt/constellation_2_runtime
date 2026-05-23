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

from ops.aegis.adaptive_intelligence.common_v1 import render_standard_summary_v1
from ops.aegis.adaptive_intelligence.event_interpretation_v1 import build_event_interpretation_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_event_interpretation_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_event_interpretation_v1(truth_root=root, repo_root=REPO_ROOT, day_utc=str(args.day))
    out_dir = root / "reports" / "event_interpretation_v1" / str(args.day)
    json_path = write_json_v1(out_dir / "event_interpretation.v1.json", payload)
    summary_path = out_dir / "event_interpretation.summary.txt"
    summary_path.write_text(render_standard_summary_v1(payload, title="AEGIS EVENT INTERPRETATION v1"), encoding="utf-8")
    print(json.dumps({"path": str(json_path), "summary_path": str(summary_path), "event_importance": payload.get("event_importance"), "trade_advice_generated": payload.get("trade_advice_generated"), "execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
