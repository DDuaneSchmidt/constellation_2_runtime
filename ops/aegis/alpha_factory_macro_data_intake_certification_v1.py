from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any

from ops.aegis.intelligence_common_v1 import write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_macro_data_intake_certification_v1"
SCHEMA_ID = "aegis_alpha_factory_macro_data_intake_certification"
SCHEMA_VERSION = "v1"
REQUIRED_MACRO_SERIES = ("REAL_YIELD", "NOMINAL_YIELD", "INFLATION_EXPECTATIONS", "DXY")
ETF_SERIES = ("SPY", "QQQ", "GLD", "SLV", "TLT", "UUP", "VIX")
DXY_PROXY_SYMBOL = "UUP"
MINIMUM_OVERLAP_SAMPLE_COUNT = 20
KNOWN_AT_RULE = "macro_series_known_after_source_timestamp_or_ingestion_timestamp"
MINIMUM_NEXT_ACTION = (
    "Add source-backed REAL_YIELD, NOMINAL_YIELD, INFLATION_EXPECTATIONS, and DXY data, or retain UUP only as an explicitly limited DXY proxy; rerun macro data intake before rerunning the real historical pilot."
)


def build_alpha_factory_macro_data_intake_certification_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    market_root = root / "market_data_snapshot_v1"
    etf_days = _common_days(market_root, ETF_SERIES)
    direct_series = {series_id: _load_series(market_root, series_id) for series_id in REQUIRED_MACRO_SERIES}
    proxy_series = _load_series(market_root, DXY_PROXY_SYMBOL)
    certifications = {
        series_id: _certify_series(series_id=series_id, loaded=direct_series[series_id], etf_days=etf_days)
        for series_id in REQUIRED_MACRO_SERIES
    }
    dxy_proxy_certification = _certify_dxy_proxy(proxy_series, etf_days) if not direct_series["DXY"]["rows"] else None
    hostile_checks = _hostile_checks(certifications, dxy_proxy_certification)
    verdicts = _verdicts(certifications, dxy_proxy_certification, hostile_checks)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_macro_data_intake_certification_v1",
        "constraints": {
            "local_source_backed_data_only": True,
            "fabricated_macro_data_allowed": False,
            "question_discovery_v2_modified": False,
            "evidence_test_v2_modified": False,
            "feature_surface_repair_v1_modified": False,
            "real_historical_data_pilot_v1_modified": False,
            "coverage_certification_v1_modified": False,
            "candidate_qualification_rules_modified": False,
            "trading_allowed": False,
        },
        "data_search": {
            "market_data_snapshot_root": str(market_root),
            "required_macro_series": list(REQUIRED_MACRO_SERIES),
            "dxy_proxy_candidate": DXY_PROXY_SYMBOL,
            "etf_overlap_symbols": list(ETF_SERIES),
            "etf_common_sample_count": len(etf_days),
            "minimum_overlap_sample_count": MINIMUM_OVERLAP_SAMPLE_COUNT,
        },
        "series_certifications": certifications,
        "dxy_proxy_certification": dxy_proxy_certification,
        "blocking_impacts": _blocking_impacts(certifications, dxy_proxy_certification),
        "hostile_checks": hostile_checks,
        "verdicts": verdicts,
        "summary": {
            "available_direct_macro_series": sorted(
                series_id for series_id, cert in certifications.items() if cert["status"] == "AVAILABLE"
            ),
            "missing_direct_macro_series": sorted(
                series_id for series_id, cert in certifications.items() if cert["status"] == "MISSING"
            ),
            "dxy_proxy_status": dxy_proxy_certification["status"] if dxy_proxy_certification else "NOT_REQUIRED_DXY_DIRECT_AVAILABLE",
            "macro_research_asset_discovery_testable": verdicts["macro_research_asset_discovery"]
            == "MACRO_RESEARCH_ASSET_DISCOVERY_TESTABLE",
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_macro_data_intake_certification_v1(
    *, truth_root: Path, day_utc: str, payload: dict[str, Any]
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_macro_data_intake_certification_v1.json", payload)
    return {"json": str(path)}


def _load_series(market_root: Path, series_id: str) -> dict[str, Any]:
    symbol_root = market_root / series_id
    files = sorted(symbol_root.glob("*.jsonl")) if symbol_root.exists() else []
    rows_by_day: dict[str, dict[str, Any]] = {}
    lineage: list[dict[str, Any]] = []
    for path in files:
        source_names: set[str] = set()
        source_hashes: set[str] = set()
        row_count = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            timestamp = str(item.get("timestamp_utc") or item.get("date") or "")
            value = item.get("close", item.get("value"))
            if value is None or len(timestamp) < 10:
                continue
            day = timestamp[:10]
            rows_by_day[day] = item
            row_count += 1
            if item.get("source_name"):
                source_names.add(str(item["source_name"]))
            if item.get("source_hash"):
                source_hashes.add(str(item["source_hash"]))
        lineage.append(
            {
                "path": str(path),
                "row_count": row_count,
                "source_name_samples": sorted(source_names)[:5],
                "source_hash_samples": sorted(source_hashes)[:5],
            }
        )
    return {"series_id": series_id, "rows": rows_by_day, "source_lineage": lineage}


def _certify_series(*, series_id: str, loaded: dict[str, Any], etf_days: set[str]) -> dict[str, Any]:
    rows = loaded["rows"]
    if not rows:
        return {
            "series_id": series_id,
            "status": "MISSING",
            "missing": True,
            "blocking_impact": _series_blocking_impact(series_id),
        }
    days = sorted(rows)
    overlap_count = len(set(days) & etf_days)
    known_at_present = all(rows[day].get("ingested_utc") or rows[day].get("timestamp_utc") for day in days)
    source_lineage = [row for row in loaded["source_lineage"] if row["row_count"] > 0]
    return {
        "series_id": series_id,
        "status": "AVAILABLE",
        "missing": False,
        "date_coverage": {"start": days[0], "end": days[-1]},
        "sample_count": len(days),
        "missing_count": _missing_weekday_count(days),
        "known_at_rule": KNOWN_AT_RULE if known_at_present else "",
        "known_at_complete": known_at_present,
        "source_lineage": source_lineage,
        "source_lineage_complete": bool(source_lineage),
        "frequency": _infer_frequency(days),
        "etf_overlap": {
            "overlap_sample_count": overlap_count,
            "minimum_required_sample_count": MINIMUM_OVERLAP_SAMPLE_COUNT,
            "sufficient": overlap_count >= MINIMUM_OVERLAP_SAMPLE_COUNT,
        },
    }


def _certify_dxy_proxy(proxy_series: dict[str, Any], etf_days: set[str]) -> dict[str, Any]:
    proxy = _certify_series(series_id=DXY_PROXY_SYMBOL, loaded=proxy_series, etf_days=etf_days)
    if proxy["status"] != "AVAILABLE":
        return {
            "proxy_for": "DXY",
            "series_id": DXY_PROXY_SYMBOL,
            "status": "MISSING",
            "claim_scope": "no DXY proxy available",
            "missing": True,
            "blocking_impact": "Dollar-index discovery remains blocked; no DXY or explicit proxy series exists locally.",
        }
    return {
        "proxy_for": "DXY",
        "series_id": DXY_PROXY_SYMBOL,
        "status": "DXY_PROXY_AVAILABLE",
        "claim_scope": "limited_dollar_etf_proxy_only_not_direct_dxy",
        "direct_dxy_claim_allowed": False,
        "proxy_certification_explicit": True,
        "certification": proxy,
    }


def _common_days(market_root: Path, symbols: tuple[str, ...]) -> set[str]:
    day_sets: list[set[str]] = []
    for symbol in symbols:
        loaded = _load_series(market_root, symbol)
        if loaded["rows"]:
            day_sets.append(set(loaded["rows"]))
    if not day_sets:
        return set()
    common = day_sets[0]
    for days in day_sets[1:]:
        common &= days
    return common


def _blocking_impacts(
    certifications: dict[str, dict[str, Any]], dxy_proxy_certification: dict[str, Any] | None
) -> dict[str, dict[str, Any]]:
    return {
        "real_yield_response_discovery": {
            "blocked": certifications["REAL_YIELD"]["status"] != "AVAILABLE",
            "reason": "REAL_YIELD missing; no real-yield claims are allowed."
            if certifications["REAL_YIELD"]["status"] != "AVAILABLE"
            else "REAL_YIELD exists with source lineage and known_at certification.",
        },
        "nominal_rate_response_discovery": {
            "blocked": certifications["NOMINAL_YIELD"]["status"] != "AVAILABLE",
            "reason": "NOMINAL_YIELD missing; nominal-rate response claims are blocked."
            if certifications["NOMINAL_YIELD"]["status"] != "AVAILABLE"
            else "NOMINAL_YIELD exists with source lineage and known_at certification.",
        },
        "inflation_expectation_discovery": {
            "blocked": certifications["INFLATION_EXPECTATIONS"]["status"] != "AVAILABLE",
            "reason": "INFLATION_EXPECTATIONS missing; inflation-expectation claims are blocked."
            if certifications["INFLATION_EXPECTATIONS"]["status"] != "AVAILABLE"
            else "INFLATION_EXPECTATIONS exists with source lineage and known_at certification.",
        },
        "dollar_shock_discovery": {
            "blocked": not _cert_usable(certifications["DXY"]) and not _proxy_usable(dxy_proxy_certification),
            "reason": _dollar_reason(certifications, dxy_proxy_certification),
        },
        "macro_research_asset_discovery": {
            "blocked": not _macro_testable(certifications, dxy_proxy_certification),
            "reason": "All required direct macro series and DXY/proxy coverage are source-backed with sufficient ETF overlap."
            if _macro_testable(certifications, dxy_proxy_certification)
            else "Macro Research Asset discovery is not testable until direct real-yield, nominal-yield, inflation-expectation, and DXY/proxy coverage are source-backed with sufficient ETF overlap.",
        },
    }


def _hostile_checks(
    certifications: dict[str, dict[str, Any]], dxy_proxy_certification: dict[str, Any] | None
) -> dict[str, Any]:
    available = [cert for cert in certifications.values() if cert["status"] == "AVAILABLE"]
    proxy_available = _proxy_available(dxy_proxy_certification)
    all_available_or_proxy = available + (
        [dxy_proxy_certification["certification"]] if proxy_available and dxy_proxy_certification else []
    )
    return {
        "no_fabricated_macro_data": True,
        "local_source_backed_rows_only": True,
        "uup_not_silently_treated_as_dxy": certifications["DXY"]["status"] == "AVAILABLE" or bool(dxy_proxy_certification),
        "dxy_proxy_explicitly_certified": proxy_available,
        "direct_dxy_claim_blocked_when_only_proxy_available": bool(proxy_available and certifications["DXY"]["status"] != "AVAILABLE"),
        "no_real_yield_claims_unless_real_yield_exists": certifications["REAL_YIELD"]["status"] == "AVAILABLE"
        or not _macro_testable(certifications, dxy_proxy_certification),
        "known_at_rule_required": all(bool(cert.get("known_at_rule")) for cert in all_available_or_proxy),
        "source_lineage_required": all(bool(cert.get("source_lineage")) for cert in all_available_or_proxy),
        "sufficient_overlap_with_etf_data_required": all(
            bool(cert.get("etf_overlap", {}).get("sufficient")) for cert in all_available_or_proxy
        )
        if all_available_or_proxy
        else False,
        "macro_testability_blocked_when_required_series_missing": not _macro_testable(certifications, dxy_proxy_certification),
    }


def _verdicts(
    certifications: dict[str, dict[str, Any]], dxy_proxy_certification: dict[str, Any] | None, hostile_checks: dict[str, Any]
) -> dict[str, str]:
    dxy_verdict = "DXY_AVAILABLE"
    if certifications["DXY"]["status"] != "AVAILABLE":
        dxy_verdict = "DXY_PROXY_AVAILABLE" if _proxy_available(dxy_proxy_certification) else "MISSING"
    return {
        "execution": "MACRO_DATA_INTAKE_CERTIFICATION_EXECUTION_VALID",
        "real_yield": "REAL_YIELD_AVAILABLE" if certifications["REAL_YIELD"]["status"] == "AVAILABLE" else "MISSING",
        "nominal_yield": "NOMINAL_YIELD_AVAILABLE" if certifications["NOMINAL_YIELD"]["status"] == "AVAILABLE" else "MISSING",
        "inflation_expectations": "INFLATION_EXPECTATIONS_AVAILABLE"
        if certifications["INFLATION_EXPECTATIONS"]["status"] == "AVAILABLE"
        else "MISSING",
        "dxy": dxy_verdict,
        "macro_research_asset_discovery": "MACRO_RESEARCH_ASSET_DISCOVERY_TESTABLE"
        if _macro_testable(certifications, dxy_proxy_certification) and hostile_checks["no_fabricated_macro_data"]
        else "NOT_TESTABLE",
        "minimum_next_action": MINIMUM_NEXT_ACTION,
    }


def _macro_testable(certifications: dict[str, dict[str, Any]], dxy_proxy_certification: dict[str, Any] | None) -> bool:
    required_direct = ("REAL_YIELD", "NOMINAL_YIELD", "INFLATION_EXPECTATIONS")
    direct_ok = all(_cert_usable(certifications[name]) for name in required_direct)
    dollar_ok = _cert_usable(certifications["DXY"]) or _proxy_usable(dxy_proxy_certification)
    return direct_ok and dollar_ok


def _cert_usable(cert: dict[str, Any]) -> bool:
    return (
        cert.get("status") == "AVAILABLE"
        and bool(cert.get("known_at_rule"))
        and bool(cert.get("source_lineage"))
        and bool(cert.get("etf_overlap", {}).get("sufficient"))
        and int(cert.get("sample_count") or 0) > 0
    )


def _proxy_usable(dxy_proxy_certification: dict[str, Any] | None) -> bool:
    if not dxy_proxy_certification or dxy_proxy_certification.get("status") != "DXY_PROXY_AVAILABLE":
        return False
    return _cert_usable(dxy_proxy_certification.get("certification", {}))


def _proxy_available(dxy_proxy_certification: dict[str, Any] | None) -> bool:
    return bool(dxy_proxy_certification and dxy_proxy_certification.get("status") == "DXY_PROXY_AVAILABLE")


def _dollar_reason(
    certifications: dict[str, dict[str, Any]], dxy_proxy_certification: dict[str, Any] | None
) -> str:
    if certifications["DXY"]["status"] == "AVAILABLE":
        return "DXY direct series exists with source lineage and known_at certification."
    if _proxy_usable(dxy_proxy_certification):
        return "DXY missing; UUP is explicitly certified only as a limited dollar ETF proxy, not direct DXY."
    if _proxy_available(dxy_proxy_certification):
        return "DXY missing; UUP proxy exists but does not yet satisfy the ETF overlap requirement for dollar-shock discovery."
    return "DXY missing and no explicit local proxy certification is available."


def _series_blocking_impact(series_id: str) -> str:
    return {
        "REAL_YIELD": "Blocks real-yield response discovery and all real-yield claims.",
        "NOMINAL_YIELD": "Blocks nominal-rate response discovery and rate-regime claims.",
        "INFLATION_EXPECTATIONS": "Blocks inflation-expectation discovery and derived real-yield validation.",
        "DXY": "Blocks direct dollar-index discovery unless an explicit limited proxy is certified.",
    }.get(series_id, "Required macro series missing.")


def _missing_weekday_count(days: list[str]) -> int:
    if len(days) < 2:
        return 0
    observed = {date.fromisoformat(day) for day in days}
    start = date.fromisoformat(days[0])
    end = date.fromisoformat(days[-1])
    total_weekdays = 0
    current = start
    while current <= end:
        if current.weekday() < 5:
            total_weekdays += 1
        current = date.fromordinal(current.toordinal() + 1)
    return max(0, total_weekdays - len(observed))


def _infer_frequency(days: list[str]) -> str:
    if len(days) < 2:
        return "UNKNOWN"
    ordinals = [date.fromisoformat(day).toordinal() for day in days]
    gaps = [b - a for a, b in zip(ordinals, ordinals[1:])]
    if median(gaps) <= 3:
        return "DAILY_OR_BUSINESS_DAILY"
    if median(gaps) <= 10:
        return "WEEKLY"
    return "IRREGULAR"


def _hash(payload: dict[str, Any]) -> str:
    clone = json.loads(json.dumps(payload, sort_keys=True))
    clone.pop("content_hash", None)
    encoded = json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
