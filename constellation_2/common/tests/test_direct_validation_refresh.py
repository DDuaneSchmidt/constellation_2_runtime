from __future__ import annotations

import json
from pathlib import Path

from direct_validation_refresh import build_direct_validation_refresh


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


def test_direct_validation_refresh_detects_new_symbol_and_writes_delta(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    previous_path = root / "direct_candidate_data_validation" / "latest.json"
    previous_path.parent.mkdir(parents=True, exist_ok=True)
    previous_path.write_text(
        json.dumps(
            {
                "candidate_validations": [
                    {
                        "candidate_id": "ptc_refresh",
                        "resolved_symbols": ["ABC"],
                        "data_exists_locally": False,
                        "classification": "INSUFFICIENT_DATA",
                        "data_gap": "No local direct CSV data found for resolved symbols: ABC",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    _write_latest(
        root,
        "focused_observation_campaign",
        {
            "campaign_candidates": [
                {
                    "candidate_id": "ptc_refresh",
                    "campaign_rank": 1,
                    "mechanism": "BREAKOUT",
                    "regime": "CHOP",
                    "candidate_symbol": "ABC",
                    "final_score": 0.72,
                    "expectancy": 0.003,
                    "profit_factor": 1.4,
                    "sample_size": 100,
                    "max_drawdown": -0.12,
                }
            ]
        },
    )
    _write_latest(root, "candidate_data_validation_plan", {"candidate_data_validation_plans": [{"candidate_id": "ptc_refresh", "required_timeframe": "daily"}]})
    _write_latest(root, "candidate_symbol_attribution", {"candidate_symbol_attributions": []})
    _write_csv(root / "data" / "cache" / "ABC.csv")

    delta = build_direct_validation_refresh(root=root, out_dir=tmp_path / "out", created_at="2026-06-05T00:00:00Z")

    assert delta["newly_available_symbols"] == ["ABC"]
    assert delta["validation_rerun_performed"] is True
    assert delta["classification_counts_before"]["INSUFFICIENT_DATA"] == 1
    assert "INSUFFICIENT_DATA" in delta["classification_counts_after"]
    report_path = Path(delta["output_path"])
    assert report_path.name == "validation_delta_report.md"
    assert "No production pipeline integration" in report_path.read_text(encoding="utf-8")
