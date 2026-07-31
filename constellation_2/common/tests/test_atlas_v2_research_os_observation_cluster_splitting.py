from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.observation_cluster_splitting import (
    build_observation_cluster_split_experiment,
    coarse_cluster_key,
    generate_observation_records,
    split_cluster_key,
    split_over_merged_clusters,
    symbol_group,
    write_observation_cluster_split_experiment,
)

NOW = "2026-06-05T00:00:00Z"


def test_symbol_group_classification() -> None:
    assert symbol_group("SPY") == "index_etf"
    assert symbol_group("NVDA") == "mega_cap"
    assert symbol_group("XLE") == "sector_etf"
    assert symbol_group("XYZ") == "single_name"
    assert symbol_group("") == "unknown"


def test_split_key_preserves_timeframe_source_and_symbol_group() -> None:
    records = generate_observation_records(120, created_at=NOW)
    assert coarse_cluster_key(records[0]) == coarse_cluster_key(records[110])
    assert split_cluster_key(records[0]) != split_cluster_key(records[110])
    split_clusters = split_over_merged_clusters(records)
    assert len(split_clusters) > 50
    assert all((row["metadata"] or {}).get("split_cluster") is True for row in split_clusters)
    assert all((row["metadata"] or {}).get("split_dimensions", {}).get("symbol_group") for row in split_clusters)


def test_split_experiment_writes_research_only_report(tmp_path: Path) -> None:
    report = build_observation_cluster_split_experiment(root=tmp_path, observation_count=500, created_at=NOW)
    assert report["pre_split"]["clusters"] == 110
    assert report["post_split"]["clusters"] > report["pre_split"]["clusters"]
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["guardrails"]["position_sizing_authorized"] is False
    assert report["post_split"]["regime_metrics"]["claims_by_regime"]["HIGH_VOL"] >= 0
    paths = write_observation_cluster_split_experiment(report, root=tmp_path)
    assert paths["latest_json"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "OBSERVATION_CLUSTER_SPLIT_EXPERIMENT"
