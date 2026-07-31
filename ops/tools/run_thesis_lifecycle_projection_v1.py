#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.trade_lifecycle.thesis_lifecycle_v1 import (
    build_thesis_evidence_ledger_v1,
    build_thesis_outcome_feedback_v1,
    build_thesis_state_projection_v1,
    write_thesis_evidence_ledger_v1,
    write_thesis_outcome_feedback_v1,
    write_thesis_state_projection_v1,
)

DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build deterministic Aegis thesis lifecycle evidence, state projection, and outcome feedback artifacts.")
    parser.add_argument("--day_utc", "--day-utc", dest="day_utc", default=datetime.now(UTC).date().isoformat())
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    truth_root = Path(args.truth_root).expanduser().resolve()
    evidence = build_thesis_evidence_ledger_v1(truth_root=truth_root, day_utc=args.day_utc)
    evidence_paths = write_thesis_evidence_ledger_v1(truth_root=truth_root, day_utc=args.day_utc, payload=evidence)
    projection = build_thesis_state_projection_v1(truth_root=truth_root, day_utc=args.day_utc, evidence_ledger=evidence)
    projection_paths = write_thesis_state_projection_v1(truth_root=truth_root, day_utc=args.day_utc, payload=projection)
    feedback = build_thesis_outcome_feedback_v1(truth_root=truth_root, day_utc=args.day_utc, thesis_projection=projection)
    feedback_paths = write_thesis_outcome_feedback_v1(truth_root=truth_root, day_utc=args.day_utc, payload=feedback)

    payload = {
        "status": "OK",
        "day_utc": args.day_utc,
        "thesis_evidence_row_count": evidence.get("row_count", 0),
        "thesis_projection_row_count": projection.get("row_count", 0),
        "thesis_feedback_count": feedback.get("feedback_count", 0),
        **evidence_paths,
        **projection_paths,
        **feedback_paths,
    }
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(
            " ".join(
                [
                    "THESIS_LIFECYCLE_OK",
                    f"day_utc={args.day_utc}",
                    f"evidence_rows={payload['thesis_evidence_row_count']}",
                    f"projection_rows={payload['thesis_projection_row_count']}",
                    f"projection_path={payload['thesis_state_projection']}",
                ]
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
