from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.candidate_symbol_attribution import (
    METHOD_CLUSTER_SYMBOL_SET,
    METHOD_DIRECT_OBSERVATION_LINEAGE,
    attribute_candidate_symbols,
    build_candidate_symbol_attribution_report,
    write_candidate_symbol_attribution_report,
)
from constellation_2.common.atlas_v2_research_os.direct_candidate_data_validation import build_direct_candidate_data_validation_report


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_csv(path: Path, rows: int = 260) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["date,open,high,low,close,volume"]
    price = 100.0
    for index in range(rows):
        price *= 1.0 + (0.002 if index % 7 else -0.01)
        open_price = price * 1.001
        high = open_price * 1.01
        low = open_price * 0.99
        close = price
        lines.append(f"2025-01-{(index % 28) + 1:02d},{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},{1000000 + index}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_attribute_candidate_symbols_uses_direct_observation_lineage() -> None:
    row = attribute_candidate_symbols(
        {
            "candidate_id": "ptc-1",
            "mechanism": "BREAKOUT",
            "regime": "TRENDING",
            "source_lineage": {
                "symbols": ["abc"],
                "timeframes": ["1d"],
                "source_types": ["structured_fixture"],
                "source_observation_ids": ["obs-1", "obs-2"],
            },
        }
    )

    assert row["candidate_symbols"] == ["ABC"]
    assert row["candidate_timeframes"] == ["1d"]
    assert row["candidate_source_observation_ids"] == ["obs-1", "obs-2"]
    assert row["symbol_attribution_method"] == METHOD_DIRECT_OBSERVATION_LINEAGE
    assert row["authority_boundary"]["live_trading_authorized"] is False


def test_attribute_candidate_symbols_preserves_multi_symbol_universe() -> None:
    row = attribute_candidate_symbols(
        {
            "candidate_id": "ptc-2",
            "symbols": ["ABC", "XYZ"],
            "timeframes": ["1d"],
            "mechanism": "MEAN_REVERSION",
            "regime": "CHOP",
        }
    )

    assert row["candidate_symbols"] == ["ABC", "XYZ"]
    assert row["candidate_universe_symbols"] == ["ABC", "XYZ"]
    assert row["universe_level_candidate"] is True
    assert row["symbol_attribution_method"] == METHOD_CLUSTER_SYMBOL_SET


def test_candidate_symbol_attribution_report_writes_latest(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "focused_observation_campaign",
        {
            "campaign_candidates": [
                {
                    "candidate_id": "ptc-1",
                    "campaign_rank": 1,
                    "mechanism": "BREAKOUT",
                    "regime": "TRENDING",
                    "source_lineage": {
                        "symbols": ["ABC"],
                        "timeframes": ["1d"],
                        "source_observation_ids": ["obs-1"],
                    },
                }
            ]
        },
    )

    report = build_candidate_symbol_attribution_report(tmp_path, created_at="2026-06-05T00:00:00Z")
    paths = write_candidate_symbol_attribution_report(report, tmp_path)

    assert report["summary"]["symbols_attributed"] == 1
    assert paths["latest_json"].exists()
    assert json.loads(paths["latest_json"].read_text(encoding="utf-8"))["report_type"] == "CANDIDATE_SYMBOL_ATTRIBUTION"


def test_direct_validation_consumes_candidate_symbol_attribution_report(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "focused_observation_campaign",
        {
            "campaign_candidates": [
                {
                    "candidate_id": "ptc_direct",
                    "campaign_rank": 1,
                    "mechanism": "MEAN_REVERSION",
                    "regime": "TRENDING",
                    "final_score": 0.72,
                    "expectancy": 0.003,
                    "profit_factor": 1.4,
                    "sample_size": 100,
                    "max_drawdown": -0.12,
                    "backtest_classification": "BACKTEST_SUPPORTED",
                }
            ]
        },
    )
    _write_latest(tmp_path, "candidate_data_validation_plan", {"candidate_data_validation_plans": []})
    attribution = build_candidate_symbol_attribution_report(
        tmp_path,
        created_at="2026-06-05T00:00:00Z",
    )
    attribution["candidate_symbol_attributions"] = [
        {
            "candidate_id": "ptc_direct",
            "candidate_symbols": ["ABC"],
            "candidate_universe_symbols": [],
            "candidate_timeframes": ["1d"],
            "candidate_source_observation_ids": ["obs-1"],
            "symbol_attribution_confidence": 0.95,
            "symbol_attribution_method": METHOD_DIRECT_OBSERVATION_LINEAGE,
        }
    ]
    write_candidate_symbol_attribution_report(attribution, tmp_path)
    _write_csv(tmp_path / "data" / "cache" / "ABC.csv")

    report = build_direct_candidate_data_validation_report(tmp_path, created_at="2026-06-05T00:00:00Z")
    row = report["candidate_validations"][0]

    assert row["candidate_symbols"] == ["ABC"]
    assert row["data_exists_locally"] is True
    assert row["direct_replay_ran"] is True
    assert report["summary"]["symbols_attributed"] == 1
