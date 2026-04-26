#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.ib_reconciliation.ib_flex_normalizer_v1 import (  # noqa: E402
    IBFlexNormalizationError,
    normalize_ib_flex_xml_v1,
)
from constellation_2.ib_reconciliation.paths_v1 import (  # noqa: E402
    ensure_runtime_layout_v1,
    runtime_artifact_path_v1,
    validate_day_utc_v1,
)
from constellation_2.ib_reconciliation.schema_v1 import write_deterministic_json_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="run_ib_flex_normalize_v1",
        description="Normalize downloaded IB Flex XML into deterministic IB reconciliation JSON artifacts.",
    )
    parser.add_argument("--day_utc", required=True, help="UTC reconciliation day (YYYY-MM-DD).")
    parser.add_argument("--ib_flex_xml", required=True, help="Path to downloaded IB Flex XML report.")
    args = parser.parse_args()

    day_utc = validate_day_utc_v1(args.day_utc)
    xml_path = Path(args.ib_flex_xml).expanduser().resolve()
    if not xml_path.exists() or not xml_path.is_file():
        raise SystemExit(f"FAIL: IB flex XML file missing: {xml_path}")

    ensure_runtime_layout_v1(day_utc)
    raw_copy_path = runtime_artifact_path_v1("ib_raw", day_utc, xml_path.name)
    shutil.copy2(xml_path, raw_copy_path)

    try:
        normalized = normalize_ib_flex_xml_v1(day_utc, xml_path)
    except IBFlexNormalizationError as exc:
        raise SystemExit(f"FAIL: {exc}") from exc

    trades_path = runtime_artifact_path_v1("normalized_ib", day_utc, "normalized_ib_trades.v1.json")
    write_deterministic_json_v1(trades_path, normalized["normalized_trades"])

    positions_path = None
    if normalized["normalized_positions"] is not None:
        positions_path = runtime_artifact_path_v1("normalized_ib", day_utc, "normalized_ib_positions.v1.json")
        write_deterministic_json_v1(positions_path, normalized["normalized_positions"])

    cash_path = None
    if normalized["normalized_cash"] is not None:
        cash_path = runtime_artifact_path_v1("normalized_ib", day_utc, "normalized_ib_cash.v1.json")
        write_deterministic_json_v1(cash_path, normalized["normalized_cash"])

    print(
        json.dumps(
            {
                "status": "OK",
                "day_utc": day_utc,
                "raw_copy_path": str(raw_copy_path),
                "normalized_trades_path": str(trades_path),
                "normalized_positions_path": str(positions_path) if positions_path else None,
                "normalized_cash_path": str(cash_path) if cash_path else None,
                "warnings": normalized["warnings"],
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
