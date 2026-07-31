from __future__ import annotations

import json
from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_macro_data_intake_certification_v1 import (
    MINIMUM_NEXT_ACTION,
    build_alpha_factory_macro_data_intake_certification_v1,
    write_alpha_factory_macro_data_intake_certification_v1,
)


DAY = "2026-06-03"
ETF_SYMBOLS = ("SPY", "QQQ", "GLD", "SLV", "TLT", "UUP", "VIX")
MACRO_SYMBOLS = ("REAL_YIELD", "NOMINAL_YIELD", "INFLATION_EXPECTATIONS", "DXY")


def _seed_series(root: Path, symbol: str, *, start_value: float = 10.0, days: int = 30) -> None:
    path = root / "market_data_snapshot_v1" / symbol / "2026.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for idx in range(days):
        day = idx + 1
        rows.append(
            {
                "symbol": symbol,
                "timestamp_utc": f"2026-01-{day:02d}T00:00:00Z",
                "ingested_utc": f"2026-01-{day:02d}T21:00:00Z",
                "close": round(start_value + idx * 0.1, 6),
                "source_name": f"local_test_source:{symbol}",
                "source_hash": f"hash-{symbol}",
            }
        )
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _seed_etfs_with_uup(root: Path) -> None:
    for offset, symbol in enumerate(ETF_SYMBOLS):
        _seed_series(root, symbol, start_value=20.0 + offset)


def _seed_all_required_macro(root: Path) -> None:
    for offset, symbol in enumerate(MACRO_SYMBOLS):
        _seed_series(root, symbol, start_value=1.0 + offset)


def test_macro_data_intake_certification_artifact_generation(tmp_path: Path) -> None:
    _seed_etfs_with_uup(tmp_path)
    payload = build_alpha_factory_macro_data_intake_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_macro_data_intake_certification"
    assert payload["verdicts"]["execution"] == "MACRO_DATA_INTAKE_CERTIFICATION_EXECUTION_VALID"
    paths = write_alpha_factory_macro_data_intake_certification_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_macro_data_intake_certification_is_deterministic(tmp_path: Path) -> None:
    _seed_etfs_with_uup(tmp_path)
    first = build_alpha_factory_macro_data_intake_certification_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_macro_data_intake_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_macro_data_intake_required_verdicts(tmp_path: Path) -> None:
    _seed_etfs_with_uup(tmp_path)
    verdicts = build_alpha_factory_macro_data_intake_certification_v1(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "MACRO_DATA_INTAKE_CERTIFICATION_EXECUTION_VALID"
    assert verdicts["real_yield"] in {"REAL_YIELD_AVAILABLE", "MISSING"}
    assert verdicts["nominal_yield"] in {"NOMINAL_YIELD_AVAILABLE", "MISSING"}
    assert verdicts["inflation_expectations"] in {"INFLATION_EXPECTATIONS_AVAILABLE", "MISSING"}
    assert verdicts["dxy"] in {"DXY_AVAILABLE", "DXY_PROXY_AVAILABLE", "MISSING"}
    assert verdicts["macro_research_asset_discovery"] in {
        "MACRO_RESEARCH_ASSET_DISCOVERY_TESTABLE",
        "NOT_TESTABLE",
    }
    assert verdicts["minimum_next_action"] == MINIMUM_NEXT_ACTION


def test_macro_data_intake_blocks_missing_macro_claims_and_certifies_uup_proxy(tmp_path: Path) -> None:
    _seed_etfs_with_uup(tmp_path)
    payload = build_alpha_factory_macro_data_intake_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["verdicts"]["real_yield"] == "MISSING"
    assert payload["verdicts"]["nominal_yield"] == "MISSING"
    assert payload["verdicts"]["inflation_expectations"] == "MISSING"
    assert payload["verdicts"]["dxy"] == "DXY_PROXY_AVAILABLE"
    assert payload["verdicts"]["macro_research_asset_discovery"] == "NOT_TESTABLE"
    assert payload["dxy_proxy_certification"]["claim_scope"] == "limited_dollar_etf_proxy_only_not_direct_dxy"
    assert payload["hostile_checks"]["uup_not_silently_treated_as_dxy"] is True
    assert payload["hostile_checks"]["direct_dxy_claim_blocked_when_only_proxy_available"] is True
    assert payload["hostile_checks"]["no_real_yield_claims_unless_real_yield_exists"] is True


def test_macro_data_intake_can_certify_source_backed_macro_series(tmp_path: Path) -> None:
    _seed_etfs_with_uup(tmp_path)
    _seed_all_required_macro(tmp_path)
    payload = build_alpha_factory_macro_data_intake_certification_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["verdicts"]["real_yield"] == "REAL_YIELD_AVAILABLE"
    assert payload["verdicts"]["nominal_yield"] == "NOMINAL_YIELD_AVAILABLE"
    assert payload["verdicts"]["inflation_expectations"] == "INFLATION_EXPECTATIONS_AVAILABLE"
    assert payload["verdicts"]["dxy"] == "DXY_AVAILABLE"
    assert payload["verdicts"]["macro_research_asset_discovery"] == "MACRO_RESEARCH_ASSET_DISCOVERY_TESTABLE"
    for symbol in MACRO_SYMBOLS:
        cert = payload["series_certifications"][symbol]
        assert cert["sample_count"] == 30
        assert cert["known_at_rule"]
        assert cert["source_lineage"]
        assert cert["etf_overlap"]["sufficient"] is True


def test_macro_data_intake_hostile_checks(tmp_path: Path) -> None:
    _seed_etfs_with_uup(tmp_path)
    payload = build_alpha_factory_macro_data_intake_certification_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert checks["no_fabricated_macro_data"] is True
    assert checks["local_source_backed_rows_only"] is True
    assert checks["known_at_rule_required"] is True
    assert checks["source_lineage_required"] is True
    assert checks["macro_testability_blocked_when_required_series_missing"] is True
