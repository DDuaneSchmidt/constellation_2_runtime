from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_historical_data_coverage_certification_v1 import (
    MINIMUM_NEXT_ACTION,
    build_alpha_factory_historical_data_coverage_certification_v1,
    write_alpha_factory_historical_data_coverage_certification_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1


DAY = "2026-06-03"
SOURCE_FAMILY = "aegis_alpha_factory_real_historical_data_pilot_v1"
SOURCE_FILENAME = "aegis_alpha_factory_real_historical_data_pilot_v1.json"


def _seed_real_pilot(root: Path) -> None:
    payload = {
        "schema_id": "aegis_alpha_factory_real_historical_data_pilot",
        "schema_version": "v1",
        "artifact_id": SOURCE_FAMILY,
        "content_hash": "seed-real-pilot-hash",
        "input_universe": {
            "loaded_symbols": ["GLD", "SLV", "SPY", "QQQ", "TLT", "UUP", "VIX"],
            "missing_symbols": ["DXY", "REAL_YIELD", "NOMINAL_YIELD", "INFLATION_EXPECTATIONS"],
            "observation_count": 1863,
        },
        "summary": {
            "valid_rac_count": 1,
            "baseline_cluster_count": 30,
            "lineage_complete": True,
        },
        "verdicts": {
            "execution": "REAL_HISTORICAL_PILOT_EXECUTION_VALID",
            "real_data_rac": "REAL_DATA_RAC_FOUND",
            "discovery_advantage": "DISCOVERY_ADVANTAGE_PRESENT",
            "pipeline_vs_baseline": "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE",
            "real_market_discovery_claim": "PROHIBITED",
        },
    }
    write_json_v1(root / "reports" / SOURCE_FAMILY / DAY / SOURCE_FILENAME, payload)


def test_historical_data_coverage_certification_artifact_generation(tmp_path: Path) -> None:
    _seed_real_pilot(tmp_path)
    payload = build_alpha_factory_historical_data_coverage_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_historical_data_coverage_certification"
    assert payload["verdicts"]["execution"] == "DATA_COVERAGE_CERTIFICATION_EXECUTION_VALID"
    paths = write_alpha_factory_historical_data_coverage_certification_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_historical_data_coverage_certification_is_deterministic(tmp_path: Path) -> None:
    _seed_real_pilot(tmp_path)
    first = build_alpha_factory_historical_data_coverage_certification_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_historical_data_coverage_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_historical_data_coverage_required_verdicts(tmp_path: Path) -> None:
    _seed_real_pilot(tmp_path)
    verdicts = build_alpha_factory_historical_data_coverage_certification_v1(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "DATA_COVERAGE_CERTIFICATION_EXECUTION_VALID"
    assert verdicts["cross_asset_etf_coverage"] in {"CROSS_ASSET_ETF_COVERAGE_SUFFICIENT", "INSUFFICIENT"}
    assert verdicts["macro_coverage"] in {"MACRO_COVERAGE_SUFFICIENT", "INSUFFICIENT"}
    assert verdicts["real_yield_discovery"] in {"REAL_YIELD_DISCOVERY_TESTABLE", "NOT_TESTABLE"}
    assert verdicts["real_market_discovery_claim"] in {"REAL_MARKET_DISCOVERY_CLAIM_ALLOWED", "PROHIBITED"}
    assert verdicts["minimum_next_action"] == MINIMUM_NEXT_ACTION


def test_historical_data_coverage_classifies_macro_limitations(tmp_path: Path) -> None:
    _seed_real_pilot(tmp_path)
    payload = build_alpha_factory_historical_data_coverage_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["verdicts"]["cross_asset_etf_coverage"] == "CROSS_ASSET_ETF_COVERAGE_SUFFICIENT"
    assert payload["verdicts"]["macro_coverage"] == "INSUFFICIENT"
    assert payload["verdicts"]["real_yield_discovery"] == "NOT_TESTABLE"
    assert "SUFFICIENT_FOR_CROSS_ASSET_ETF_PILOT" in payload["pilot_validity_classification"]
    assert "INSUFFICIENT_FOR_MACRO_RESEARCH_ASSET_DISCOVERY" in payload["pilot_validity_classification"]
    assert "INSUFFICIENT_FOR_REAL_MARKET_DISCOVERY_CLAIM" in payload["pilot_validity_classification"]


def test_historical_data_coverage_hostile_checks(tmp_path: Path) -> None:
    _seed_real_pilot(tmp_path)
    checks = build_alpha_factory_historical_data_coverage_certification_v1(truth_root=tmp_path, day_utc=DAY)["hostile_checks"]

    assert checks["missing_real_yield_blocks_real_yield_claims"] is True
    assert checks["missing_nominal_yield_blocks_nominal_rate_claims"] is True
    assert checks["missing_inflation_expectations_blocks_inflation_expectation_claims"] is True
    assert checks["missing_dxy_blocks_dollar_index_claims_unless_uup_limited_proxy"] is True
    assert checks["uup_proxy_explicitly_limited"] is True
    assert checks["baseline_matching_or_exceeding_pipeline_blocks_discovery_claim"] is True
