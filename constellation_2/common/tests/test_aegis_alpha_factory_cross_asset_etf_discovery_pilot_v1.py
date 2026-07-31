from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_cross_asset_etf_discovery_pilot_v1 import (
    build_alpha_factory_cross_asset_etf_discovery_pilot_v1,
    write_alpha_factory_cross_asset_etf_discovery_pilot_v1,
)


DAY = "2026-06-03"
SYMBOLS = ("SPY", "QQQ", "GLD", "SLV", "TLT", "UUP", "VIX")
MACRO_TERMS = ("REAL_YIELD", "NOMINAL_YIELD", "INFLATION_EXPECTATIONS", "DXY", "real yield", "nominal yield")


def _seed_market_history(root: Path, rows: int = 72) -> None:
    start = date(2026, 1, 2)
    for symbol_idx, symbol in enumerate(SYMBOLS):
        path = root / "market_data_snapshot_v1" / symbol / "2026.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        base = 45.0 + symbol_idx * 9
        for idx in range(rows):
            day = start + timedelta(days=idx)
            value = base + idx * (0.18 + symbol_idx * 0.025)
            if symbol == "VIX":
                value = 15.0 + (idx % 11) * 0.65
            if symbol == "QQQ" and 25 <= idx <= 31:
                value += 3.8
            if symbol == "SLV" and 42 <= idx <= 46:
                value -= 2.2
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


def test_cross_asset_etf_pilot_artifact_generation(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    payload = build_alpha_factory_cross_asset_etf_discovery_pilot_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_cross_asset_etf_discovery_pilot"
    assert payload["verdicts"]["execution"] == "CROSS_ASSET_ETF_PILOT_EXECUTION_VALID"
    assert payload["relationship_features"]
    assert payload["input_universe"]["loaded_symbols"] == sorted(SYMBOLS)
    paths = write_alpha_factory_cross_asset_etf_discovery_pilot_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_cross_asset_etf_pilot_is_deterministic(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    first = build_alpha_factory_cross_asset_etf_discovery_pilot_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_cross_asset_etf_discovery_pilot_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_cross_asset_etf_pilot_required_verdicts(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    verdicts = build_alpha_factory_cross_asset_etf_discovery_pilot_v1(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "CROSS_ASSET_ETF_PILOT_EXECUTION_VALID"
    assert verdicts["real_data_rac"] in {"REAL_DATA_RAC_FOUND", "NO_REAL_DATA_RAC", "INCONCLUSIVE"}
    assert verdicts["discovery_advantage"] in {"DISCOVERY_ADVANTAGE_PRESENT", "ABSENT", "INCONCLUSIVE"}
    assert verdicts["pipeline_vs_baseline"] in {
        "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE",
        "PIPELINE_OUTPERFORMS_BASELINE",
        "INCONCLUSIVE",
    }
    assert verdicts["macro_claim_leakage"] in {"MACRO_CLAIM_LEAKAGE_ABSENT", "PRESENT"}
    assert verdicts["real_market_cross_asset_discovery_claim"] in {
        "REAL_MARKET_CROSS_ASSET_DISCOVERY_CLAIM_ALLOWED",
        "PROHIBITED",
    }
    assert verdicts["minimum_next_action"]


def test_cross_asset_etf_pilot_blocks_macro_claim_leakage(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    payload = build_alpha_factory_cross_asset_etf_discovery_pilot_v1(truth_root=tmp_path, day_utc=DAY)
    checked_surface = json.dumps(
        {
            "questions": payload["generated_questions"],
            "evidence": payload["evidence_support"],
            "candidate_groups": payload["candidate_groups"],
        },
        sort_keys=True,
    )

    assert payload["verdicts"]["macro_claim_leakage"] == "MACRO_CLAIM_LEAKAGE_ABSENT"
    assert payload["hostile_checks"]["no_macro_claim_leakage"] is True
    assert not any(term in checked_surface for term in MACRO_TERMS)
    assert all(symbol not in payload["input_universe"]["loaded_symbols"] for symbol in ("REAL_YIELD", "NOMINAL_YIELD", "DXY"))


def test_cross_asset_claim_requires_full_gate_success(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    payload = build_alpha_factory_cross_asset_etf_discovery_pilot_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]
    required = (
        verdicts["real_data_rac"] == "REAL_DATA_RAC_FOUND"
        and verdicts["discovery_advantage"] == "DISCOVERY_ADVANTAGE_PRESENT"
        and verdicts["pipeline_vs_baseline"] == "PIPELINE_OUTPERFORMS_BASELINE"
        and verdicts["macro_claim_leakage"] == "MACRO_CLAIM_LEAKAGE_ABSENT"
        and payload["hostile_checks"]["hostile_checks_all_pass"] is True
    )

    assert (verdicts["real_market_cross_asset_discovery_claim"] == "REAL_MARKET_CROSS_ASSET_DISCOVERY_CLAIM_ALLOWED") is required
