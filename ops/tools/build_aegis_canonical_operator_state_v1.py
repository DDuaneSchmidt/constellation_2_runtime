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

from ops.aegis.canonical_operator_state_v1 import build_canonical_operator_state_v1, write_canonical_operator_state_v1  # noqa: E402
from ops.aegis.candidate_review_ledger_v1 import build_candidate_review_ledger_v1, write_candidate_review_ledger_v1  # noqa: E402
from ops.aegis.research_hypothesis_classification_v1 import (  # noqa: E402
    build_research_hypothesis_classification_v1,
    write_research_hypothesis_classification_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.operator_state.canonical_operator_state_builder_v1 import build_and_write_operator_state_snapshot_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_canonical_operator_state_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    classification_payload = build_research_hypothesis_classification_v1(truth_root=root, day_utc=str(args.day_utc))
    classification_paths = write_research_hypothesis_classification_v1(
        truth_root=root,
        day_utc=str(args.day_utc),
        payload=classification_payload,
    )
    ledger_payload = build_candidate_review_ledger_v1(truth_root=root, day_utc=str(args.day_utc), filter_name="all")
    ledger_paths = write_candidate_review_ledger_v1(truth_root=root, day_utc=str(args.day_utc), payload=ledger_payload)
    payload = build_canonical_operator_state_v1(truth_root=root, repo_root=REPO_ROOT, day_utc=str(args.day_utc))
    paths = write_canonical_operator_state_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    operator_state_snapshot, operator_state_snapshot_path = build_and_write_operator_state_snapshot_v1(truth_root=root, day_utc=str(args.day_utc))
    print(
        json.dumps(
            {
                **paths,
                "action_count": len(payload.get("actions_required") or []),
                "missing_input_count": len(payload.get("missing_inputs") or []),
                "research_hypothesis_classification": classification_paths["json"],
                "candidate_review_ledger": ledger_paths["json"],
                "operator_state_snapshot_v1": str(operator_state_snapshot_path),
                "operator_state_snapshot_id": operator_state_snapshot.get("snapshot_id", ""),
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
