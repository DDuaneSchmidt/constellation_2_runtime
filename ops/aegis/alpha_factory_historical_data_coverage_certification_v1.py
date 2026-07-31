from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_historical_data_coverage_certification_v1"
SOURCE_FAMILY = "aegis_alpha_factory_real_historical_data_pilot_v1"
SOURCE_FILENAME = "aegis_alpha_factory_real_historical_data_pilot_v1.json"
SCHEMA_ID = "aegis_alpha_factory_historical_data_coverage_certification"
SCHEMA_VERSION = "v1"
REQUIRED_SERIES = (
    "GLD",
    "SLV",
    "SPY",
    "QQQ",
    "TLT",
    "UUP",
    "VIX",
    "DXY",
    "REAL_YIELD",
    "NOMINAL_YIELD",
    "INFLATION_EXPECTATIONS",
)
ETF_SERIES = ("GLD", "SLV", "SPY", "QQQ", "TLT", "UUP", "VIX")
MACRO_SERIES = ("DXY", "REAL_YIELD", "NOMINAL_YIELD", "INFLATION_EXPECTATIONS")
MINIMUM_NEXT_ACTION = (
    "Add real historical REAL_YIELD, NOMINAL_YIELD, INFLATION_EXPECTATIONS, and DXY series; keep UUP as a labeled limited dollar proxy only, then rerun the real historical pilot and hostile benchmark gates."
)


def build_alpha_factory_historical_data_coverage_certification_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    source_path = root / "reports" / SOURCE_FAMILY / day_utc / SOURCE_FILENAME
    pilot = read_json_v1(source_path)
    input_universe = pilot.get("input_universe", {}) if pilot else {}
    loaded = set(str(row) for row in input_universe.get("loaded_symbols", []) if row)
    missing = set(str(row) for row in input_universe.get("missing_symbols", []) if row)
    series = _series_coverage(loaded, missing)
    limitations = _macro_limitations(series)
    classifications = _pilot_classifications(series, pilot)
    hostile_checks = _hostile_checks(series, pilot)
    verdicts = _verdicts(series, pilot, hostile_checks)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_historical_data_coverage_certification_v1",
        "constraints": {
            "real_historical_data_pilot_v1_modified": False,
            "question_discovery_v2_modified": False,
            "evidence_test_v2_modified": False,
            "naive_baseline_modified": False,
            "candidate_qualification_rules_modified": False,
            "new_data_added": False,
            "trading_allowed": False,
        },
        "source_artifacts": {
            "real_historical_data_pilot_v1": {
                "status": "AVAILABLE" if pilot else "NOT_FOUND",
                "path": str(source_path),
                "content_hash": pilot.get("content_hash") if pilot else None,
            }
        },
        "series_coverage": series,
        "coverage_limitations": limitations,
        "pilot_validity_classification": classifications,
        "minimum_next_data_additions": [
            "REAL_YIELD daily historical series",
            "NOMINAL_YIELD daily historical series",
            "INFLATION_EXPECTATIONS daily historical series",
            "DXY daily historical series or formally governed UUP-as-limited-dollar-proxy policy",
        ],
        "hostile_checks": hostile_checks,
        "source_pilot_summary": {
            "loaded_symbols": sorted(loaded),
            "missing_symbols": sorted(missing),
            "observation_count": int(input_universe.get("observation_count") or 0),
            "pilot_verdicts": pilot.get("verdicts", {}) if pilot else {},
            "pilot_summary": pilot.get("summary", {}) if pilot else {},
        },
        "verdicts": verdicts,
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_historical_data_coverage_certification_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_historical_data_coverage_certification_v1.json", payload)
    return {"json": str(path)}


def _series_coverage(loaded: set[str], missing: set[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for series in REQUIRED_SERIES:
        available = series in loaded and series not in missing
        out[series] = {
            "available": available,
            "status": "AVAILABLE" if available else "MISSING",
            "role": "cross_asset_etf_or_proxy" if series in ETF_SERIES else "macro_required_series",
            "limited_proxy": series == "UUP",
            "claim_scope": "limited_dollar_proxy_only" if series == "UUP" and available else "direct_series",
        }
    return out


def _macro_limitations(series: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    real_yield_available = bool(series["REAL_YIELD"]["available"])
    nominal_available = bool(series["NOMINAL_YIELD"]["available"])
    inflation_available = bool(series["INFLATION_EXPECTATIONS"]["available"])
    dxy_available = bool(series["DXY"]["available"])
    uup_available = bool(series["UUP"]["available"])
    return {
        "real_yield_response_discovery": {
            "materially_limited": not real_yield_available,
            "testability": "NOT_TESTABLE" if not real_yield_available else "TESTABLE",
            "reason": "REAL_YIELD missing from pilot input." if not real_yield_available else "REAL_YIELD available.",
        },
        "inflation_expectation_discovery": {
            "materially_limited": not inflation_available,
            "testability": "NOT_TESTABLE" if not inflation_available else "TESTABLE",
            "reason": "INFLATION_EXPECTATIONS missing from pilot input." if not inflation_available else "INFLATION_EXPECTATIONS available.",
        },
        "nominal_rate_response_discovery": {
            "materially_limited": not nominal_available,
            "testability": "NOT_TESTABLE" if not nominal_available else "TESTABLE",
            "reason": "NOMINAL_YIELD missing from pilot input." if not nominal_available else "NOMINAL_YIELD available.",
        },
        "dollar_shock_discovery": {
            "materially_limited": not dxy_available,
            "testability": "LIMITED_PROXY_ONLY" if uup_available and not dxy_available else "TESTABLE" if dxy_available else "NOT_TESTABLE",
            "reason": "DXY missing; UUP is available only as a limited ETF proxy." if uup_available and not dxy_available else "DXY available." if dxy_available else "DXY and UUP unavailable.",
        },
        "macro_regime_discovery": {
            "materially_limited": not (real_yield_available and nominal_available and inflation_available and (dxy_available or uup_available)),
            "testability": "NOT_TESTABLE" if not (real_yield_available and nominal_available and inflation_available) else "PARTIAL" if not dxy_available and uup_available else "TESTABLE",
            "reason": "One or more required macro series are missing, so macro regime discovery is not sufficiently testable.",
        },
    }


def _pilot_classifications(series: dict[str, dict[str, Any]], pilot: dict[str, Any]) -> list[str]:
    classifications: list[str] = []
    if all(series[name]["available"] for name in ETF_SERIES):
        classifications.append("SUFFICIENT_FOR_CROSS_ASSET_ETF_PILOT")
    if not all(series[name]["available"] for name in MACRO_SERIES):
        classifications.append("INSUFFICIENT_FOR_MACRO_RESEARCH_ASSET_DISCOVERY")
    pilot_verdicts = pilot.get("verdicts", {}) if pilot else {}
    if (
        not all(series[name]["available"] for name in MACRO_SERIES)
        or pilot_verdicts.get("pipeline_vs_baseline") == "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE"
        or pilot_verdicts.get("real_market_discovery_claim") != "REAL_MARKET_DISCOVERY_CLAIM_ALLOWED"
    ):
        classifications.append("INSUFFICIENT_FOR_REAL_MARKET_DISCOVERY_CLAIM")
    return classifications


def _hostile_checks(series: dict[str, dict[str, Any]], pilot: dict[str, Any]) -> dict[str, Any]:
    pilot_verdicts = pilot.get("verdicts", {}) if pilot else {}
    dxy_missing = not series["DXY"]["available"]
    uup_available = bool(series["UUP"]["available"])
    return {
        "source_pilot_loaded": bool(pilot),
        "missing_real_yield_blocks_real_yield_claims": not series["REAL_YIELD"]["available"],
        "missing_nominal_yield_blocks_nominal_rate_claims": not series["NOMINAL_YIELD"]["available"],
        "missing_inflation_expectations_blocks_inflation_expectation_claims": not series["INFLATION_EXPECTATIONS"]["available"],
        "missing_dxy_blocks_dollar_index_claims_unless_uup_limited_proxy": dxy_missing and uup_available,
        "uup_proxy_explicitly_limited": uup_available,
        "baseline_matching_or_exceeding_pipeline_blocks_discovery_claim": pilot_verdicts.get("pipeline_vs_baseline") == "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE",
        "real_market_claim_already_prohibited_by_pilot": pilot_verdicts.get("real_market_discovery_claim") == "PROHIBITED",
    }


def _verdicts(series: dict[str, dict[str, Any]], pilot: dict[str, Any], hostile_checks: dict[str, Any]) -> dict[str, str]:
    etf_ok = all(series[name]["available"] for name in ETF_SERIES)
    macro_ok = all(series[name]["available"] for name in MACRO_SERIES)
    real_yield_ok = bool(series["REAL_YIELD"]["available"])
    pilot_verdicts = pilot.get("verdicts", {}) if pilot else {}
    claim_allowed = (
        macro_ok
        and real_yield_ok
        and pilot_verdicts.get("real_market_discovery_claim") == "REAL_MARKET_DISCOVERY_CLAIM_ALLOWED"
        and pilot_verdicts.get("pipeline_vs_baseline") == "PIPELINE_OUTPERFORMS_BASELINE"
        and not hostile_checks["baseline_matching_or_exceeding_pipeline_blocks_discovery_claim"]
    )
    return {
        "execution": "DATA_COVERAGE_CERTIFICATION_EXECUTION_VALID" if pilot else "DATA_COVERAGE_CERTIFICATION_EXECUTION_INVALID",
        "cross_asset_etf_coverage": "CROSS_ASSET_ETF_COVERAGE_SUFFICIENT" if etf_ok else "INSUFFICIENT",
        "macro_coverage": "MACRO_COVERAGE_SUFFICIENT" if macro_ok else "INSUFFICIENT",
        "real_yield_discovery": "REAL_YIELD_DISCOVERY_TESTABLE" if real_yield_ok else "NOT_TESTABLE",
        "real_market_discovery_claim": "REAL_MARKET_DISCOVERY_CLAIM_ALLOWED" if claim_allowed else "PROHIBITED",
        "minimum_next_action": MINIMUM_NEXT_ACTION,
    }


def _hash(payload: dict[str, Any]) -> str:
    clone = json.loads(json.dumps(payload, sort_keys=True))
    clone.pop("content_hash", None)
    encoded = json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
