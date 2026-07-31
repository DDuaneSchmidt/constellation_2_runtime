from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_real_historical_data_pilot_v1 import (
    build_alpha_factory_real_historical_data_pilot_v1,
    write_alpha_factory_real_historical_data_pilot_v1,
)


DAY = "2026-06-03"
SYMBOLS = ("SPY", "QQQ", "GLD", "SLV", "TLT", "UUP", "VIX")


def _seed_market_history(root: Path, rows: int = 70) -> None:
    start = date(2026, 1, 2)
    for symbol_idx, symbol in enumerate(SYMBOLS):
        path = root / "market_data_snapshot_v1" / symbol / "2026.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        base = 50.0 + symbol_idx * 11
        for idx in range(rows):
            day = start + timedelta(days=idx)
            value = base + idx * (0.25 + symbol_idx * 0.03)
            if symbol == "VIX":
                value = 16.0 + (idx % 9) * 0.7
            if symbol == "QQQ" and idx in {28, 29, 30, 31}:
                value += 4.0
            if symbol == "GLD" and idx in {45, 46, 47}:
                value -= 2.5
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


def test_real_historical_pilot_artifact_generation(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    payload = build_alpha_factory_real_historical_data_pilot_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_real_historical_data_pilot"
    assert payload["verdicts"]["execution"] == "REAL_HISTORICAL_PILOT_EXECUTION_VALID"
    assert payload["relationship_features"]
    paths = write_alpha_factory_real_historical_data_pilot_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_real_historical_pilot_is_deterministic(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    first = build_alpha_factory_real_historical_data_pilot_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_real_historical_data_pilot_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_real_historical_pilot_required_verdicts(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    verdicts = build_alpha_factory_real_historical_data_pilot_v1(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "REAL_HISTORICAL_PILOT_EXECUTION_VALID"
    assert verdicts["real_data_rac"] in {"REAL_DATA_RAC_FOUND", "NO_REAL_DATA_RAC", "INCONCLUSIVE"}
    assert verdicts["discovery_advantage"] in {"DISCOVERY_ADVANTAGE_PRESENT", "ABSENT", "INCONCLUSIVE"}
    assert verdicts["pipeline_vs_baseline"] in {"BASELINE_MATCHES_OR_EXCEEDS_PIPELINE", "PIPELINE_OUTPERFORMS_BASELINE", "INCONCLUSIVE"}
    assert verdicts["real_market_discovery_claim"] in {"REAL_MARKET_DISCOVERY_CLAIM_ALLOWED", "PROHIBITED"}
    assert verdicts["minimum_next_action"]


def test_real_historical_pilot_hostile_checks(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    checks = build_alpha_factory_real_historical_data_pilot_v1(truth_root=tmp_path, day_utc=DAY)["hostile_checks"]

    assert checks["available_historical_data_loaded"] is True
    assert checks["no_synthetic_fixture_assumptions"] is True
    assert checks["relationship_specific_lineage_complete"] is True
    assert checks["deterministic_replay"] is True
    assert "no_zero_sample_support" in checks
    assert "no_baseline_recoverable_only_rac" in checks
    assert "evidence_support_not_just_naive_forward_return_correlation" in checks


def test_real_market_claim_requires_full_gate_success(tmp_path: Path) -> None:
    _seed_market_history(tmp_path)
    payload = build_alpha_factory_real_historical_data_pilot_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]
    required = (
        verdicts["real_data_rac"] == "REAL_DATA_RAC_FOUND"
        and verdicts["discovery_advantage"] == "DISCOVERY_ADVANTAGE_PRESENT"
        and verdicts["pipeline_vs_baseline"] == "PIPELINE_OUTPERFORMS_BASELINE"
        and payload["hostile_checks"]["hostile_checks_all_pass"] is True
    )

    assert (verdicts["real_market_discovery_claim"] == "REAL_MARKET_DISCOVERY_CLAIM_ALLOWED") is required
