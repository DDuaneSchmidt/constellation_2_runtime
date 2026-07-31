from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_technical_conditional_pattern_pilot_v1 import (
    build_alpha_factory_technical_conditional_pattern_pilot_v1,
    write_alpha_factory_technical_conditional_pattern_pilot_v1,
)


DAY = "2026-06-03"
SYMBOLS = ("SPY", "QQQ", "GLD", "SLV", "TLT", "UUP", "VIX")
REPORT_FAMILY = "aegis_alpha_factory_cross_asset_etf_failure_attribution_v1"
REPORT_FILENAME = "aegis_alpha_factory_cross_asset_etf_failure_attribution_v1.json"


def _seed_market_history(root: Path, rows: int = 96) -> None:
    start = date(2026, 1, 2)
    for symbol_idx, symbol in enumerate(SYMBOLS):
        path = root / "market_data_snapshot_v1" / symbol / "2026.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        base = 50.0 + symbol_idx * 8.0
        for idx in range(rows):
            day = start + timedelta(days=idx)
            value = base + idx * (0.12 + symbol_idx * 0.015)
            cycle = (idx % 20) - 10
            value += cycle * (0.18 + symbol_idx * 0.01)
            if symbol == "VIX":
                value = 15.0 + abs(cycle) * 0.75
                if 45 <= idx <= 52:
                    value += 5.0
            if symbol == "SPY" and 30 <= idx <= 35:
                value -= 3.2
            if symbol == "QQQ" and 58 <= idx <= 65:
                value += 4.6
            lines.append(
                json.dumps(
                    {
                        "symbol": symbol,
                        "timestamp_utc": f"{day.isoformat()}T00:00:00Z",
                        "close": round(value, 4),
                        "source_name": "test_market_data_snapshot_v1",
                        "ingested_utc": f"{day.isoformat()}T21:00:00Z",
                    },
                    sort_keys=True,
                )
            )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _seed_failure_attribution(root: Path) -> None:
    artifact = {
        "schema_id": "aegis_alpha_factory_cross_asset_etf_failure_attribution",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": DAY,
        "content_hash": "seeded-cross-asset-failure-attribution",
        "verdicts": {
            "execution": "CROSS_ASSET_FAILURE_ATTRIBUTION_EXECUTION_VALID",
            "primary_failure_mode": "QUESTION_TOO_CLOSE_TO_BASELINE",
            "fault_class": "QUESTION_GENERATION_FAULT",
            "branch_decision": "PIVOT_RECOMMENDED",
            "minimum_next_action": "technical-analysis conditional pattern pilot",
        },
    }
    path = root / "reports" / REPORT_FAMILY / DAY / REPORT_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_technical_conditional_pattern_pilot_artifact_generation(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    _seed_failure_attribution(tmp_path)
    payload = build_alpha_factory_technical_conditional_pattern_pilot_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_technical_conditional_pattern_pilot"
    assert payload["verdicts"]["execution"] == "TECHNICAL_CONDITIONAL_PATTERN_PILOT_EXECUTION_VALID"
    assert payload["technical_features"]
    assert payload["pattern_evidence"]
    paths = write_alpha_factory_technical_conditional_pattern_pilot_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_technical_conditional_pattern_pilot_is_deterministic(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    _seed_failure_attribution(tmp_path)
    first = build_alpha_factory_technical_conditional_pattern_pilot_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_technical_conditional_pattern_pilot_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_technical_conditional_pattern_pilot_required_verdicts(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    _seed_failure_attribution(tmp_path)
    verdicts = build_alpha_factory_technical_conditional_pattern_pilot_v1(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "TECHNICAL_CONDITIONAL_PATTERN_PILOT_EXECUTION_VALID"
    assert verdicts["real_data_technical_pattern"] in {
        "REAL_DATA_TECHNICAL_PATTERN_FOUND",
        "NO_REAL_DATA_TECHNICAL_PATTERN",
        "INCONCLUSIVE",
    }
    assert verdicts["technical_conditional_advantage"] in {
        "TECHNICAL_CONDITIONAL_ADVANTAGE_PRESENT",
        "ABSENT",
        "INCONCLUSIVE",
    }
    assert verdicts["pipeline_vs_baseline"] in {
        "PIPELINE_OUTPERFORMS_BASELINE",
        "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE",
        "INCONCLUSIVE",
    }
    assert verdicts["real_market_technical_discovery_claim"] == "PROHIBITED"
    assert verdicts["minimum_next_action"]


def test_technical_conditional_pattern_pilot_hostile_checks(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    _seed_failure_attribution(tmp_path)
    payload = build_alpha_factory_technical_conditional_pattern_pilot_v1(truth_root=tmp_path, day_utc=DAY)
    supported = payload["supported_technical_evidence"]

    assert payload["hostile_checks"]["prior_failure_attribution_recommended_this_pilot"] is True
    assert payload["hostile_checks"]["technical_condition_lineage_complete"] is True
    assert payload["valid_research_asset_candidates"] == []
    assert all(row["sample_count"] > 0 for row in supported)
    assert all(row["technical_condition_non_pairwise"] for row in supported)
    assert all(not row["baseline_recoverable"] for row in supported)
    assert "relationship with" not in json.dumps(payload["technical_pattern_candidate_groups"], sort_keys=True)
