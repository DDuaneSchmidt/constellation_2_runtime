from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.sleeve_condition_audit_v1 import build_sleeve_condition_audit_v1, write_sleeve_condition_audit_v1  # noqa: E402

DAY = "2026-05-29"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path) -> None:
    _write(
        root / "reports" / "aegis_sleeve_evaluation_v1" / DAY / "sleeve_evaluation.v1.json",
        {
            "sleeves": [
                {"sleeve_id": "C2_DEFENSIVE_TAIL_V1", "classification": "SILENT_MARKET_CONDITION_NOT_MET", "market_data_status": "OK", "missing_input_artifacts": []},
                {"sleeve_id": "C2_EVENT_DISLOCATION_V1", "classification": "SILENT_MARKET_CONDITION_NOT_MET", "market_data_status": "OK", "missing_input_artifacts": []},
                {"sleeve_id": "C2_MARKET_NEUTRAL_SPREAD_V1", "classification": "SILENT_MARKET_CONDITION_NOT_MET", "market_data_status": "OK", "missing_input_artifacts": []},
                {"sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "classification": "SILENT_MARKET_CONDITION_NOT_MET", "market_data_status": "OK", "missing_input_artifacts": []},
                {"sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "classification": "SILENT_MARKET_CONDITION_NOT_MET", "market_data_status": "OK", "missing_input_artifacts": []},
            ]
        },
    )
    _write(
        root / "reports" / "sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json",
        {
            "outcomes": [
                {"sleeve_id": "C2_DEFENSIVE_TAIL_V1", "status": "NO_INTENT", "output_intents": 0, "reason_codes": ["NO_INTENT_DECLARED"], "stdout_summary": "OK: DEF_TAIL_NO_INTENT {\"max_pairwise_corr\":\"0\",\"status\":\"NO_INTENT\"}"},
                {"sleeve_id": "C2_EVENT_DISLOCATION_V1", "status": "NO_INTENT", "output_intents": 0, "reason_codes": ["NO_INTENT_DECLARED"], "producer_requested_symbols": ["AAA", "BBB"], "stdout_summary": "MISSING_BAR_FOR_DAY {\"status\":\"MISSING_BAR_FOR_DAY\",\"symbol\":\"AAA\"}"},
                {"sleeve_id": "C2_MARKET_NEUTRAL_SPREAD_V1", "status": "NO_INTENT", "output_intents": 0, "reason_codes": ["NO_INTENT_DECLARED"], "stdout_summary": "{\"status\":\"NO_INTENT\",\"z_enter\":2.0,\"evaluations\":[{\"pair\":\"SPY:QQQ\",\"z\":\"-1.91\"},{\"pair\":\"HYG:LQD\",\"z\":\"0.2\"}]}"},
                {"sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "status": "NO_INTENT", "output_intents": 0, "reason_codes": ["NO_INTENT_DECLARED"], "producer_requested_symbols": ["AAA", "BBB"], "stdout_summary": "NO_INTENT {\"status\":\"NO_INTENT\",\"symbol\":\"AAA\",\"z\":-1.1}"},
                {"sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "status": "NO_INTENT", "output_intents": 0, "reason_codes": ["NO_INTENT_DECLARED"], "producer_requested_symbols": ["GLD", "SPY"], "stdout_summary": "NO_INTENT {\"status\":\"NO_INTENT\",\"symbol\":\"SPY\",\"stdev_percentile\":0.14,\"trend_ok\":true}"},
            ]
        },
    )
    _write(
        root / "truth_sleeves" / "PRIMARY" / "PAPER" / "accounting_v1" / "nav" / DAY / "nav_snapshot.v1.json",
        {"history": {"drawdown_pct": "0.000000"}},
    )
    _write(
        root / "truth_sleeves" / "PRIMARY" / "PAPER" / "monitoring_v1" / "regime_snapshot_v2" / DAY / "regime_snapshot.v2.json",
        {"regime": "NORMAL"},
    )


def _rows(payload: dict) -> dict[str, dict]:
    return {row["sleeve_id"]: row for row in payload["sleeves"]}


def test_condition_audit_includes_all_requested_silent_sleeves(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_sleeve_condition_audit_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary"]["audited_sleeve_count"] == 5
    assert set(_rows(payload)) == {
        "C2_DEFENSIVE_TAIL_V1",
        "C2_EVENT_DISLOCATION_V1",
        "C2_MARKET_NEUTRAL_SPREAD_V1",
        "C2_MEAN_REVERSION_EQ_V1",
        "C2_VOL_INCOME_DEFINED_RISK_V1",
    }


def test_defensive_tail_shows_exact_failed_conditions_and_keep_as_is(tmp_path: Path) -> None:
    _seed(tmp_path)
    row = _rows(build_sleeve_condition_audit_v1(truth_root=tmp_path, day_utc=DAY))["C2_DEFENSIVE_TAIL_V1"]

    assert row["recommendation"] == "KEEP_AS_IS"
    assert row["latest_evaluation"]["silence_correct"] is True
    assert any("regime is NORMAL" in item for item in row["latest_evaluation"]["conditions_failed"])
    assert any("max_pairwise_corr" in item for item in row["latest_evaluation"]["conditions_failed"])
    assert row["logic_changed"] is False
    assert row["thresholds_changed"] is False


def test_market_neutral_near_threshold_recommends_review_threshold_without_changing_logic(tmp_path: Path) -> None:
    _seed(tmp_path)
    row = _rows(build_sleeve_condition_audit_v1(truth_root=tmp_path, day_utc=DAY))["C2_MARKET_NEUTRAL_SPREAD_V1"]

    assert row["recommendation"] == "REVIEW_THRESHOLD"
    assert row["modification_review_warranted"] is True
    assert row["near_miss_analysis"]["nearest_miss_data_available"] is True
    assert row["near_miss_analysis"]["top_nearest_misses"][0]["pair"] == "SPY:QQQ"
    assert row["thresholds_changed"] is False


def test_missing_full_nearest_miss_telemetry_is_reported_explicitly(tmp_path: Path) -> None:
    _seed(tmp_path)
    rows = _rows(build_sleeve_condition_audit_v1(truth_root=tmp_path, day_utc=DAY))

    for sleeve_id in ("C2_EVENT_DISLOCATION_V1", "C2_MEAN_REVERSION_EQ_V1", "C2_VOL_INCOME_DEFINED_RISK_V1"):
        row = rows[sleeve_id]
        assert row["recommendation"] == "NEEDS_NEAR_MISS_TELEMETRY"
        assert row["near_miss_analysis"]["nearest_miss_data_available"] is False
        assert row["near_miss_analysis"]["producer_must_emit"]


def test_write_condition_audit_uses_expected_path_and_preserves_safety_flags(tmp_path: Path) -> None:
    _seed(tmp_path)
    path = write_sleeve_condition_audit_v1(truth_root=tmp_path, day_utc=DAY)

    assert path == tmp_path / "reports" / "aegis_sleeve_condition_audit_v1" / DAY / "sleeve_condition_audit.v1.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["trade_advice_allowed"] is False
    assert payload["broker_execution_allowed"] is False
    assert payload["sleeve_logic_modification_allowed"] is False
    assert payload["threshold_modification_allowed"] is False
    assert payload["universe_modification_allowed"] is False
