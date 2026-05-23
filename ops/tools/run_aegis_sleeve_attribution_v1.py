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

from ops.aegis.intelligence_common_v1 import write_json_v1  # noqa: E402
from ops.aegis.sleeve_attribution_engine_v1 import build_sleeve_attribution_v1, render_sleeve_feedback_summary_v1, sleeve_health_scores_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_sleeve_attribution_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    payload = build_sleeve_attribution_v1(truth_root=root, day_utc=day)
    out_dir = root / "reports" / "aegis_sleeve_attribution_v1" / day
    attribution_path = write_json_v1(out_dir / "sleeve_attribution.v1.json", payload)
    health_path = write_json_v1(out_dir / "sleeve_health_scores.v1.json", sleeve_health_scores_v1(payload))
    summary_path = out_dir / "sleeve_feedback.summary.txt"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(render_sleeve_feedback_summary_v1(payload), encoding="utf-8")
    print(json.dumps({"path": str(attribution_path), "health_path": str(health_path), "summary_path": str(summary_path), "sleeve_count": payload["sleeve_count"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
