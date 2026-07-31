from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.bulk_observation_import import import_observations_from_csv, import_observations_from_json, seed_demo_observations

NOW = "2026-06-05T00:00:00Z"


def test_imports_csv_observations_and_creates_claim_backlog(tmp_path) -> None:
    csv_path = tmp_path / "observations.csv"
    csv_path.write_text(
        "timestamp,symbol,timeframe,mechanism,market_structure,observation,regime,source,confidence\n"
        "2026-06-05T09:45:00Z,SPY,5m,OPENING_RANGE,INSIDE_DAY,broke opening range high after compression,TRENDING,manual_seed,0.62\n"
        "2026-06-05T09:50:00Z,QQQ,5m,OPENING_RANGE,INSIDE_DAY,broke opening range high after compression,TRENDING,manual_seed,0.64\n",
        encoding="utf-8",
    )
    report = import_observations_from_csv(csv_path, root=tmp_path / "store", created_at=NOW, write_report=False)
    assert report["raw_observations"] == 2
    assert report["valid_observations"] == 2
    assert report["clusters_created"] >= 1
    assert report["claims_created"] >= 1
    assert report["backlog_items_created"] >= 1
    assert report["batch"]["records"][0]["market_structure"] == "INSIDE_DAY"
    assert report["clusters"][0]["market_structure"] == "INSIDE_DAY"
    assert report["claim_seeds"][0]["market_structure"] == "INSIDE_DAY"
    assert report["authority_boundary"]["trading_authorized"] is False


def test_imports_json_and_tracks_invalid_and_duplicate_rows(tmp_path) -> None:
    json_path = tmp_path / "observations.json"
    rows = [
        {"timestamp": "2026-06-05T09:45:00Z", "symbol": "SPY", "timeframe": "5m", "mechanism": "BREAKOUT", "market_structure": "COMPRESSION", "observation": "range breakout after compression", "regime": "TRENDING", "source": "manual", "confidence": 0.7},
        {"timestamp": "2026-06-05T09:46:00Z", "symbol": "SPY", "timeframe": "5m", "mechanism": "BREAKOUT", "market_structure": "COMPRESSION", "observation": "range breakout after compression", "regime": "TRENDING", "source": "manual", "confidence": 0.7},
        {"timestamp": "2026-06-05T09:46:00Z", "symbol": "SPY"},
    ]
    json_path.write_text(json.dumps({"observations": rows}), encoding="utf-8")
    report = import_observations_from_json(json_path, root=tmp_path / "store", created_at=NOW, write_report=False)
    assert report["raw_observations"] == 3
    assert report["invalid_observations"] == 1
    assert report["duplicates_skipped"] == 1
    assert report["valid_observations"] == 1


def test_seed_demo_observations_generates_100_across_mechanisms(tmp_path) -> None:
    result = seed_demo_observations(root=tmp_path / "store", output_path=tmp_path / "demo.json", created_at=NOW)
    report = result["import_report"]
    mechanisms = {record["mechanism"] for record in report["batch"]["records"]}
    structures = {record["market_structure"] for record in report["batch"]["records"]}
    assert result["observations_generated"] == 100
    assert len(mechanisms) == 10
    assert len(structures) == 10
    assert report["raw_observations"] == 100
