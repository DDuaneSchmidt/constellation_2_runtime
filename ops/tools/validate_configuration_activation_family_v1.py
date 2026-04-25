#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from constellation_2.common.configuration_activation_family_validator_v1 import (
    validate_configuration_activation_family_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the configuration activation artifact family.")
    parser.add_argument("--truth-root", default="", help="Canonical truth root to validate. Defaults to the runtime contract root.")
    args = parser.parse_args()
    report = validate_configuration_activation_family_v1(
        truth_root=args.truth_root or None,
    )
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    return 0 if bool(report.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
