from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.run_sleeve_evaluation_kernel_v1 import _nearest_miss_telemetry_from_stdout  # noqa: E402


def test_event_dislocation_extracts_complete_full_universe_nearest_miss() -> None:
    stdout = "\n".join(
        [
            'OK: ED_NO_INTENT {"symbol":"AAA","status":"NO_INTENT","gap_abs_pct":"0.0195","range_pct":"0.010","gap_abs_enter":"0.02","range_enter":"0.03","rule":"NOT_TRIGGERED"}',
            'OK: ED_NO_INTENT {"symbol":"BBB","status":"NO_INTENT","gap_abs_pct":"0.001","range_pct":"0.029","gap_abs_enter":"0.02","range_enter":"0.03","rule":"NOT_TRIGGERED"}',
        ]
    )

    payload = _nearest_miss_telemetry_from_stdout(
        engine_id="C2_EVENT_DISLOCATION_V1",
        stdout=stdout,
        requested_symbols=["AAA", "BBB"],
    )

    assert payload["status"] == "COMPLETE"
    assert payload["evaluated_symbol_count"] == 2
    assert payload["missing_telemetry_symbols"] == []
    assert payload["top_nearest_misses"][0]["symbol"] == "AAA"
    assert payload["top_nearest_misses"][0]["nearest_rule"] == "gap_abs"


def test_mean_reversion_extracts_sorted_z_nearest_miss() -> None:
    stdout = "\n".join(
        [
            'OK: MR_NO_INTENT {"symbol":"AAA","status":"NO_INTENT","z":"-1.1","z_enter":"-2.0","rule":"z > -2.0"}',
            'OK: MR_NO_INTENT {"symbol":"BBB","status":"NO_INTENT","z":"-1.9","z_enter":"-2.0","rule":"z > -2.0"}',
        ]
    )

    payload = _nearest_miss_telemetry_from_stdout(
        engine_id="C2_MEAN_REVERSION_EQ_V1",
        stdout=stdout,
        requested_symbols=["AAA", "BBB"],
    )

    assert payload["status"] == "COMPLETE"
    assert payload["top_nearest_misses"][0]["symbol"] == "BBB"
    assert payload["top_nearest_misses"][0]["distance_to_threshold"] < payload["top_nearest_misses"][1]["distance_to_threshold"]


def test_vol_income_reports_missing_symbol_when_runner_output_is_incomplete() -> None:
    stdout = '{"symbol":"SPY","status":"NO_INTENT","pct_rank":"0.14285714285714285","enter_percentile":"0.75","trend_ok":true}'

    payload = _nearest_miss_telemetry_from_stdout(
        engine_id="C2_VOL_INCOME_DEFINED_RISK_V1",
        stdout=stdout,
        requested_symbols=["GLD", "SPY"],
    )

    assert payload["status"] == "PARTIAL"
    assert payload["evaluated_symbol_count"] == 1
    assert payload["missing_telemetry_symbols"] == ["GLD"]
    assert payload["top_nearest_misses"][0]["symbol"] == "SPY"


def test_batch_failure_rows_are_reported_as_blocked_symbols_not_silent_omissions() -> None:
    stdout = 'OK: MR_NO_INTENT {"symbol":"AAA","status":"NO_INTENT","reason_codes":["BATCH_SYMBOL_EVALUATION_FAILED","MISSING_BAR_FOR_DAY"]}'

    payload = _nearest_miss_telemetry_from_stdout(
        engine_id="C2_MEAN_REVERSION_EQ_V1",
        stdout=stdout,
        requested_symbols=["AAA"],
    )

    assert payload["status"] == "COMPLETE"
    assert payload["evaluated_symbol_count"] == 0
    assert payload["blocked_symbol_count"] == 1
    assert payload["blocked_symbols"][0]["symbol"] == "AAA"
