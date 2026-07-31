from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os import regime_boundary_expansion as build157


NOW = "2026-06-06T00:00:00Z"


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    (root / "exact_coverage_import_validator").mkdir(parents=True)
    return root


def _write_validator(root: Path) -> None:
    payload = {
        "import_validation_matrix": [
            {
                "candidate_id": build157.TARGET_CANDIDATE_ID,
                "family_id": build157.TARGET_FAMILY_ID,
                "symbol": "TSLA",
                "timeframe": "30m",
                "validation_status": "VALID_WITH_WARNINGS",
                "normalized_file": "data/manual_intraday_import/TSLA_30m.csv",
            }
        ]
    }
    (root / "exact_coverage_import_validator" / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _patch_replay(monkeypatch, metrics_by_regime: dict[str, dict[str, object]] | None = None) -> None:
    rows = [
        {
            "timestamp": f"2023-01-{(i % 28) + 1:02d}T09:30:00Z",
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 1000,
        }
        for i in range(120)
    ]
    monkeypatch.setattr(build157, "normalize_market_data_csv", lambda path, symbol, timeframe: rows, raising=False)

    def fake_run_requirement(requirement, valid_file, *, created_at):
        regime = requirement["regime"]
        metrics = (metrics_by_regime or {}).get(regime, {"sample_size": 80, "expectancy": 0.002, "profit_factor": 1.4})
        return {
            "candidate_id": requirement["candidate_id"],
            "family_id": requirement["family_id"],
            "symbol": requirement["symbol"],
            "timeframe": requirement["timeframe"],
            "sample_size": metrics["sample_size"],
            "expectancy": metrics["expectancy"],
            "profit_factor": metrics["profit_factor"],
            "classification": "EXACT_CONFIRMED_STRONG",
            "fallback_used": False,
            "regime_bridged": build157.explain_regime_mapping(regime)["mapped_regime"],
            "notes": "",
        }

    monkeypatch.setattr(build157, "_run_exact_requirement", fake_run_requirement)


def test_regime_boundary_confirms_trending_when_only_trending_survives_net_of_cost(monkeypatch, tmp_path: Path) -> None:
    root = _root(tmp_path)
    _write_validator(root)
    _patch_replay(
        monkeypatch,
        {
            "TRENDING": {"sample_size": 80, "expectancy": 0.002, "profit_factor": 1.4},
            "HIGH_VOLATILITY": {"sample_size": 80, "expectancy": 0.0005, "profit_factor": 1.1},
            "LOW_VOLATILITY": {"sample_size": 80, "expectancy": 0.0005, "profit_factor": 1.1},
            "RANGE_BOUND": {"sample_size": 80, "expectancy": 0.0005, "profit_factor": 1.1},
            "UNKNOWN": {"sample_size": 80, "expectancy": 0.0005, "profit_factor": 1.1},
        },
    )

    report = build157.build_regime_boundary_expansion(root=root, created_at=NOW, repo_root=tmp_path)

    assert report["overall_classification"] == "REGIME_CONFIRMED"
    assert report["trending_necessary"] is True
    rows = {row["regime"]: row for row in report["regime_results"]}
    assert rows["TRENDING"]["net_classification_10bps"] == "NET_SURVIVES_STRONG"
    assert rows["HIGH_VOLATILITY"]["net_classification_10bps"] == "COST_ERODED"
    assert rows["RANGE_BOUND"]["research_regime"] == "CHOP"
    assert rows["RANGE_BOUND"]["replay_regime"] == "RANGE_BOUND"


def test_regime_boundary_is_weak_when_non_trending_regime_survives(monkeypatch, tmp_path: Path) -> None:
    root = _root(tmp_path)
    _write_validator(root)
    _patch_replay(
        monkeypatch,
        {
            "TRENDING": {"sample_size": 80, "expectancy": 0.002, "profit_factor": 1.4},
            "HIGH_VOLATILITY": {"sample_size": 80, "expectancy": 0.002, "profit_factor": 1.4},
        },
    )

    report = build157.build_regime_boundary_expansion(root=root, created_at=NOW, repo_root=tmp_path)

    assert report["overall_classification"] == "REGIME_WEAK"
    assert report["trending_necessary"] is False
    assert "HIGH_VOLATILITY" in report["summary"]["non_trending_net_survivors"]


def test_regime_boundary_control_only_when_unknown_is_only_survivor(monkeypatch, tmp_path: Path) -> None:
    root = _root(tmp_path)
    _write_validator(root)
    _patch_replay(
        monkeypatch,
        {
            "TRENDING": {"sample_size": 80, "expectancy": 0.0005, "profit_factor": 1.1},
            "HIGH_VOLATILITY": {"sample_size": 80, "expectancy": 0.0005, "profit_factor": 1.1},
            "LOW_VOLATILITY": {"sample_size": 80, "expectancy": 0.0005, "profit_factor": 1.1},
            "RANGE_BOUND": {"sample_size": 80, "expectancy": 0.0005, "profit_factor": 1.1},
            "UNKNOWN": {"sample_size": 80, "expectancy": 0.002, "profit_factor": 1.4},
        },
    )

    report = build157.build_regime_boundary_expansion(root=root, created_at=NOW, repo_root=tmp_path)

    assert report["overall_classification"] == "REGIME_CONTROL_ONLY"
    assert report["summary"]["control_net_survives"] is True


def test_regime_boundary_writes_required_outputs(monkeypatch, tmp_path: Path) -> None:
    root = _root(tmp_path)
    _write_validator(root)
    _patch_replay(monkeypatch)

    report = build157.run_regime_boundary_expansion(root=root, created_at=NOW, repo_root=tmp_path)
    out = root / build157.REPORT_DIRNAME

    assert report["authority_boundary"]["filter_loosening_authorized"] is False
    assert report["authority_boundary"]["alternate_timeframe_fallback_allowed"] is False
    assert (out / "latest.json").exists()
    assert (out / "latest_summary.md").exists()
    assert (out / "regime_boundary_expansion_results.csv").exists()
    with (out / "regime_boundary_expansion_results.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["regime"] for row in rows} == set(build157.TARGET_REGIMES)
