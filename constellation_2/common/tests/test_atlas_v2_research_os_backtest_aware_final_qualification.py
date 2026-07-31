from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.backtest_aware_final_qualification import (
    build_backtest_aware_final_qualification_report,
    compute_backtest_aware_final_qualification,
    compute_backtest_evidence_score,
    compute_backtest_penalties,
    write_backtest_aware_final_qualification_report,
)


NOW = "2026-06-05T00:00:00Z"


def supported_backtest(*, warnings: list[str] | None = None, sample_size: int = 90) -> dict[str, object]:
    return {
        "classification": "BACKTEST_SUPPORTED",
        "metrics": {
            "sample_size": sample_size,
            "expectancy": 0.018,
            "profit_factor": 1.75,
            "max_drawdown": -0.08,
            "replay_backtest_consistency": 0.82,
            "regime_specific_performance": {
                "TREND": {"expectancy": 0.015, "win_rate": 0.58},
                "HIGH_VOL": {"expectancy": 0.011, "win_rate": 0.54},
            },
        },
        "warnings": warnings or [],
        "missing_data": [],
    }


def preliminary(score: float = 0.665) -> dict[str, object]:
    return {
        "score": {"edge_score": score},
        "disqualification_reasons": ["edge_score below 0.7"],
    }


def test_supported_backtest_evidence_can_lift_final_score_without_changing_threshold() -> None:
    result = compute_backtest_aware_final_qualification(preliminary(0.665), supported_backtest())

    assert result["threshold"] == 0.70
    assert result["final_score"] > result["preliminary_edge_score"]
    assert result["eligible"] is True
    assert "backtest classification BACKTEST_SUPPORTED" in result["qualification_reasons"]


def test_proxy_and_intraday_daily_mismatch_penalties_suppress_final_score() -> None:
    clean = compute_backtest_aware_final_qualification(preliminary(0.665), supported_backtest())
    penalized_backtest = supported_backtest(
        warnings=[
            "candidate plan has no symbol or universe; used SPY adjusted daily data as local proxy",
            "daily bars cannot fully test intraday entry/exit details for this mechanism",
        ],
        sample_size=20,
    )
    penalized = compute_backtest_aware_final_qualification(preliminary(0.665), penalized_backtest)

    assert penalized["penalties"]["proxy_data_penalty"] > 0
    assert penalized["penalties"]["intraday_daily_mismatch_penalty"] > 0
    assert penalized["penalties"]["sample_size_penalty"] > 0
    assert penalized["final_score"] < clean["final_score"]


def test_backtest_evidence_components_include_required_metrics() -> None:
    score = compute_backtest_evidence_score(supported_backtest())

    assert set(score["components"]) == {
        "expectancy",
        "profit_factor",
        "sample_size",
        "max_drawdown_inverse",
        "backtest_consistency",
        "mechanism_regime_consistency",
    }
    assert score["backtest_evidence_score"] > 0


def test_penalties_fail_closed_for_weak_backtest() -> None:
    backtest = supported_backtest()
    backtest["classification"] = "BACKTEST_WEAK"
    penalties = compute_backtest_penalties(backtest)

    assert penalties["negative_backtest_penalty"] > 0


def test_final_qualification_report_writes_research_only_latest(tmp_path: Path) -> None:
    report = build_backtest_aware_final_qualification_report(root=tmp_path, observation_count=200, created_at=NOW)
    paths = write_backtest_aware_final_qualification_report(report, root=tmp_path)

    assert paths["latest_json"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "BACKTEST_AWARE_FINAL_QUALIFICATION"
    assert payload["threshold"] == 0.70
    assert payload["guardrails"]["live_trading_authorized"] is False
    assert payload["guardrails"]["position_sizing_authorized"] is False
