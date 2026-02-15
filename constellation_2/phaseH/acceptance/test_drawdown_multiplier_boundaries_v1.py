#!/usr/bin/env python3
from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

# Fail-closed import root (match tool pattern)
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseH.tools.c2_risk_transformer_offline_v1 import drawdown_multiplier_v1  # noqa: E402


def main() -> int:
    cases = [
        ("0.000000", "1.00"),
        ("-0.049000", "1.00"),
        ("-0.050000", "0.75"),
        ("-0.100000", "0.50"),
        ("-0.150000", "0.25"),
        ("-0.200000", "0.25"),
    ]

    for dd_s, exp_s in cases:
        got = drawdown_multiplier_v1(Decimal(dd_s))
        if got != Decimal(exp_s):
            raise SystemExit(f"FAIL: dd={dd_s} expected={exp_s} got={str(got)}")

    print("OK: drawdown multiplier boundary cases")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
