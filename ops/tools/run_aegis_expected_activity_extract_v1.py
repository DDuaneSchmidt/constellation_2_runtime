#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.ib_reconciliation.aegis_expected_extractor_v1 import (  # noqa: E402
    AegisExpectedExtractorError,
    extract_aegis_expected_activity_v1,
)
from constellation_2.ib_reconciliation.paths_v1 import (  # noqa: E402
    ensure_runtime_layout_v1,
    runtime_artifact_path_v1,
    validate_day_utc_v1,
)
from constellation_2.ib_reconciliation.schema_v1 import write_deterministic_json_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="run_aegis_expected_activity_extract_v1",
        description="Extract deterministic Aegis expected activity for IB reconciliation.",
    )
    parser.add_argument("--day_utc", required=True, help="UTC reconciliation day (YYYY-MM-DD).")
    args = parser.parse_args()

    day_utc = validate_day_utc_v1(args.day_utc)
    ensure_runtime_layout_v1(day_utc)

    try:
        payload = extract_aegis_expected_activity_v1(day_utc)
    except AegisExpectedExtractorError as exc:
        raise SystemExit(f"FAIL: {exc}") from exc

    output_path = runtime_artifact_path_v1("aegis_expected", day_utc, "aegis_expected_activity.v1.json")
    write_deterministic_json_v1(output_path, payload)

    print(
        json.dumps(
            {
                "status": "OK",
                "day_utc": day_utc,
                "output_path": str(output_path),
                "expected_orders": len(payload.get("expected_orders", [])),
                "expected_fills": len(payload.get("expected_fills", [])),
                "source_artifacts": len(payload.get("source_artifacts", [])),
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
