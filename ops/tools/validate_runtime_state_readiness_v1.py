#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from constellation_2.common.runtime_state_readiness_validator_v1 import (
    validate_runtime_state_readiness_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the active runtime-state/readiness family.")
    parser.add_argument("--repo-root", default="")
    parser.add_argument("--canonical-truth-root", default="")
    args = parser.parse_args()
    report = validate_runtime_state_readiness_v1(
        repo_root=args.repo_root or None,
        canonical_truth_root=args.canonical_truth_root or None,
    )
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    return 0 if bool(report.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
