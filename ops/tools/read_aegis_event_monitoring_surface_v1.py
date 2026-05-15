#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_event_monitoring_v1 import build_event_monitoring_operator_surface_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="read_aegis_event_monitoring_surface_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--event_rules_registry", default="")
    args = parser.parse_args(argv)

    payload = build_event_monitoring_operator_surface_v1(
        truth_root=Path(args.truth_root),
        day_utc=args.day_utc,
        event_rules_registry_path=Path(args.event_rules_registry) if args.event_rules_registry else None,
    )
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
