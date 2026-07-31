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

from ops.aegis.research_lab.research_pipeline_v1 import ETF_DROP_MEAN_REVERSION_HYPOTHESIS_ID  # noqa: E402
from ops.aegis.research_lab.research_validation_samples_v1 import build_research_validation_samples_v1, write_research_validation_samples_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_research_validation_samples_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--hypothesis-id", default=ETF_DROP_MEAN_REVERSION_HYPOTHESIS_ID)
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_research_validation_samples_v1(truth_root=root, day_utc=str(args.day_utc), hypothesis_id=str(args.hypothesis_id))
    path = write_research_validation_samples_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload, hypothesis_id=str(args.hypothesis_id))
    print("AEGIS RESEARCH VALIDATION SAMPLES v1")
    print(f"hypothesis_id: {payload['hypothesis_id']}")
    print(f"required_samples: {payload['required_samples']}")
    print(f"current_samples: {payload['current_samples']}")
    print(f"missing_samples: {payload['missing_samples']}")
    print(f"last_sample_at: {payload['last_sample_at'] or 'none'}")
    print(f"next_sample_expected_at: {payload['next_sample_expected_at']}")
    print(f"excluded_samples: {len(payload['excluded_samples'])}")
    print(f"exclusion_reasons: {json.dumps(payload['exclusion_reasons'], sort_keys=True)}")
    print(f"should_rerun_qualification: {str(payload['should_rerun_qualification']).lower()}")
    print(f"path: {path}")
    print(json.dumps({k: payload[k] for k in ['hypothesis_id','required_samples','current_samples','missing_samples','last_sample_at','next_sample_expected_at','exclusion_reasons','should_rerun_qualification','status','safety']}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
