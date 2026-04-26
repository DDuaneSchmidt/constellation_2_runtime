#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.ib_reconciliation.mismatch_classifier_v1 import classify_mismatches_v1  # noqa: E402
from constellation_2.ib_reconciliation.paths_v1 import (  # noqa: E402
    ensure_runtime_layout_v1,
    runtime_artifact_path_v1,
    truth_surface_report_path_v1,
    validate_day_utc_v1,
)
from constellation_2.ib_reconciliation.reconciliation_matcher_v1 import match_expected_to_ib_v1  # noqa: E402
from constellation_2.ib_reconciliation.schema_v1 import (  # noqa: E402
    IB_RECONCILIATION_SCHEMA_VERSION,
    read_json_object_v1,
    utc_now_z_v1,
    validate_reconciliation_v1,
    write_deterministic_json_v1,
)


def _load_optional_json(path: Path) -> dict | None:
    if not path.exists() or not path.is_file():
        return None
    return read_json_object_v1(path)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="run_ib_reconciliation_v1",
        description="Run deterministic IB reconciliation between Aegis expected and normalized IB actuals.",
    )
    parser.add_argument("--day_utc", required=True, help="UTC reconciliation day (YYYY-MM-DD).")
    args = parser.parse_args()

    day_utc = validate_day_utc_v1(args.day_utc)
    ensure_runtime_layout_v1(day_utc)

    expected_path = runtime_artifact_path_v1("aegis_expected", day_utc, "aegis_expected_activity.v1.json")
    trades_path = runtime_artifact_path_v1("normalized_ib", day_utc, "normalized_ib_trades.v1.json")
    positions_path = runtime_artifact_path_v1("normalized_ib", day_utc, "normalized_ib_positions.v1.json")
    cash_path = runtime_artifact_path_v1("normalized_ib", day_utc, "normalized_ib_cash.v1.json")
    output_path = runtime_artifact_path_v1("reconciliations", day_utc, "ib_reconciliation.v1.json")
    truth_surface_path = truth_surface_report_path_v1(day_utc)

    if not expected_path.exists():
        raise SystemExit(f"FAIL: missing expected activity artifact: {expected_path}")
    if not trades_path.exists():
        raise SystemExit(f"FAIL: missing normalized IB trades artifact: {trades_path}")

    expected_payload = read_json_object_v1(expected_path)
    normalized_trades_payload = read_json_object_v1(trades_path)
    normalized_positions_payload = _load_optional_json(positions_path)
    normalized_cash_payload = _load_optional_json(cash_path)

    match_result = match_expected_to_ib_v1(expected_payload, normalized_trades_payload)
    classified = classify_mismatches_v1(
        day_utc=day_utc,
        expected_activity=expected_payload,
        normalized_ib_trades=normalized_trades_payload,
        normalized_ib_positions=normalized_positions_payload,
        normalized_ib_cash=normalized_cash_payload,
        match_result=match_result,
    )

    payload = {
        "schema_version": IB_RECONCILIATION_SCHEMA_VERSION,
        "day_utc": day_utc,
        "status": classified["status"],
        "aegis_expected_path": str(expected_path),
        "ib_actual_paths": {
            "normalized_trades": str(trades_path),
            "normalized_positions": str(positions_path) if positions_path.exists() else "",
            "normalized_cash": str(cash_path) if cash_path.exists() else "",
        },
        "matched_items": match_result["matched_items"],
        "mismatches": classified["mismatches"],
        "summary": classified["summary"],
        "generated_utc": utc_now_z_v1(),
    }
    validate_reconciliation_v1(payload)

    write_deterministic_json_v1(output_path, payload)
    write_deterministic_json_v1(truth_surface_path, payload)

    print(
        json.dumps(
            {
                "status": "OK",
                "day_utc": day_utc,
                "reconciliation_status": payload["status"],
                "output_path": str(output_path),
                "truth_surface_path": str(truth_surface_path),
                "high_mismatch_count": payload["summary"]["high_mismatch_count"],
                "medium_mismatch_count": payload["summary"]["medium_mismatch_count"],
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
