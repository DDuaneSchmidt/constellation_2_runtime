#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.sleeve_throughput_evidence_classification_scorecard_v1 import (  # noqa: E402
    build_sleeve_throughput_evidence_classification_scorecard_v1,
    write_sleeve_throughput_evidence_classification_scorecard_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Aegis sleeve throughput evidence classification scorecard V1.")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    payload = build_sleeve_throughput_evidence_classification_scorecard_v1(truth_root=root, day_utc=day)
    artifact = write_sleeve_throughput_evidence_classification_scorecard_v1(truth_root=root, day_utc=day, payload=payload)
    print("AEGIS SLEEVE THROUGHPUT EVIDENCE CLASSIFICATION SCORECARD v1")
    print(f"day_utc: {day}")
    print(f"artifact: {artifact}")
    print(f"summary: {json.dumps(payload.get('summary') or {}, sort_keys=True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
