from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.bulk_observation_import import REGIMES, observation_rows_for_profile
from constellation_2.common.atlas_v2_research_os.observation_expansion import (
    build_observation_expansion_report,
    write_observation_expansion_report,
)
from constellation_2.common.atlas_v2_research_os.observation_models import OBSERVATION_WORKLOAD_PROFILES

NOW = "2026-06-05T00:00:00Z"


def test_requested_observation_profiles_exist_and_generate_structured_rows() -> None:
    assert OBSERVATION_WORKLOAD_PROFILES["OBSERVATION_IMPORT_100"] == 100
    assert OBSERVATION_WORKLOAD_PROFILES["OBSERVATION_IMPORT_1000"] == 1000
    assert OBSERVATION_WORKLOAD_PROFILES["OBSERVATION_IMPORT_5000"] == 5000
    rows = observation_rows_for_profile("OBSERVATION_IMPORT_1000")
    assert len(rows) == 1000
    assert {"symbol_universe", "timeframe", "mechanism", "market_structure", "regime", "source_type", "confidence"}.issubset(rows[0])
    assert len({row["symbol"] for row in rows}) > 10
    assert len({row["source_type"] for row in rows}) >= 5
    assert len({row["market_structure"] for row in rows}) == 10
    assert {"HIGH_VOL", "LOW_VOL", "VOL_EXPANSION", "VOL_CONTRACTION", "BULL", "BEAR", "RISK_ON", "RISK_OFF"}.issubset(set(REGIMES))
    assert "HIGH_VOLATILITY" not in set(REGIMES)


def test_observation_expansion_imports_1000_and_runs_replay_dry_run(tmp_path: Path) -> None:
    report = build_observation_expansion_report(root=tmp_path, created_at=NOW, dry_run_limit=8)
    metrics = report["metrics"]
    assert report["selected_profile"] == "OBSERVATION_IMPORT_1000"
    assert metrics["observations_generated"] == 1000
    assert metrics["observations_imported"] == 1000
    assert metrics["clusters_created"] >= 50
    assert metrics["claims_generated"] >= 50
    assert metrics["dry_run_hypotheses_generated"] == 8
    assert metrics["dry_run_replays_run"] == 8
    assert report["coverage"]["symbols_covered"] >= 20
    assert report["coverage"]["timeframes_covered"] == 4
    assert report["coverage"]["mechanisms_covered"] == 10
    assert report["coverage"]["regimes_covered"] == 11
    assert set(report["regime_metrics"]["observations_by_regime"]) >= {"CHOP", "TRENDING", "HIGH_VOL", "LOW_VOL", "VOL_EXPANSION", "VOL_CONTRACTION", "BULL", "BEAR", "RISK_ON", "RISK_OFF"}
    assert report["regime_metrics"]["claims_by_regime"]["HIGH_VOL"] > 0
    assert report["regime_metrics"]["hypotheses_by_regime"]["CHOP"] >= 0
    assert report["coverage"]["source_types_covered"] == 5
    assert report["coverage"]["session_contexts_covered"] == 8
    assert set(metrics["observations_by_session"]) == {"OPEN", "MORNING", "MIDDAY", "POWER_HOUR", "CLOSE", "OVERNIGHT", "PREMARKET", "POSTMARKET"}
    assert sum(metrics["observations_by_session"].values()) == 1000
    assert sum(metrics["hypotheses_by_session"].values()) == 8
    assert report["coverage"]["market_structures_covered"] == 10
    assert set(metrics["observations_by_structure"]) >= {"INSIDE_DAY", "OUTSIDE_DAY", "FAILED_BREAKDOWN"}
    assert set(metrics["claims_by_structure"]) >= {"INSIDE_DAY", "OUTSIDE_DAY", "FAILED_BREAKDOWN"}
    assert report["dry_run_sample"][0]["market_structure"] in metrics["hypotheses_by_structure"]
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["capital_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False
    assert report["authority_boundary"]["candidate_promotion_authorized"] is False


def test_observation_expansion_writes_report_and_latest_pointers(tmp_path: Path) -> None:
    paths = write_observation_expansion_report(root=tmp_path, day="2026-06-05", created_at=NOW, dry_run_limit=3)
    for path in paths.values():
        assert path.exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["schema_id"] == "atlas_v2_research_os_observation_expansion_report_v1"
    assert payload["metrics"]["dry_run_hypotheses_generated"] == 3
    assert sum(payload["metrics"]["hypotheses_by_session"].values()) == 3
    assert "Observation Source Expansion" in paths["latest_summary"].read_text(encoding="utf-8")
