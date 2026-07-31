from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.data_registry_v1 import build_data_registry_v1, write_data_registry_v1
from ops.aegis.market_data.market_data_provider_v1 import fetch_market_data_v1, provider_config_from_env_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import normalize_market_symbol_v1, provider_symbol_candidates_v1
from ops.aegis.market_data.symbol_map_v1 import build_runtime_symbol_universe_v1, build_symbol_map_v1, write_symbol_map_v1
from ops.aegis.sleeve_input_contracts_v1 import build_sleeve_input_contracts_v1, write_sleeve_input_contracts_v1
from ops.aegis.sleeve_readiness_v1 import build_sleeve_readiness_v1, write_sleeve_readiness_v1
from ops.tools.refresh_aegis_market_data_v1 import build_market_data_report_v1, write_market_data_report_v1


DAY = "2026-05-18"
SLEEVES = [
    ("C2_CROSS_ASSET_TREND_V1", ["DBC", "GLD", "HYG", "IEF", "IWM", "LQD", "QQQ", "SPY", "TLT", "UUP"]),
    ("C2_DEFENSIVE_TAIL_V1", ["TLT"]),
    ("C2_EVENT_DISLOCATION_V1", ["GLD"]),
    ("C2_MARKET_NEUTRAL_SPREAD_V1", ["HYG", "IWM", "LQD", "QQQ", "SPY"]),
    ("C2_MEAN_REVERSION_EQ_V1", ["QQQ"]),
    ("C2_TREND_EQ_PRIMARY_V1", ["SPY"]),
    ("C2_VOL_INCOME_DEFINED_RISK_V1", ["IWM"]),
]


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    _write_json(repo / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json", {"sleeves": []})
    _write_json(
        repo / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json",
        {"engines": [{"engine_id": sleeve, "activation_status": "ACTIVE", "engine_runner_path": f"{sleeve}.py", "allowed_symbols": symbols} for sleeve, symbols in SLEEVES]},
    )
    return repo


def _write_production_dataset(repo: Path, *, dataset_id: str = "ds_ohlcv_1d_liquid_etf_core_fixture", symbols: list[str] | None = None) -> None:
    _write_json(
        repo / "ops/config/aegis_runtime_universe.json",
        {
            "schema_id": "aegis_runtime_universe_config",
            "schema_version": "v1",
            "runtime_universe_mode": "production_scan_dataset",
            "production_scan_dataset_id": dataset_id,
        },
    )
    _write_json(
        repo / "research_lab/research_store/datasets" / dataset_id / "dataset_snapshot.json",
        {
            "dataset_snapshot_id": dataset_id,
            "dataset_id": "liquid_etf_core",
            "quality_status": "CURRENT",
            "created_at": "2026-05-18T00:00:00Z",
            "symbols": symbols or ["AAA", "BBB", "SPY"],
            "symbol_count": len(symbols or ["AAA", "BBB", "SPY"]),
        },
    )


def _market_data(root: Path, *, include_vix: bool = False, include_breadth: bool = False) -> None:
    symbols = {}
    for symbol in sorted({symbol for _, symbols_for_sleeve in SLEEVES for symbol in symbols_for_sleeve}):
        symbols[symbol] = {"last_price": "100", "close": "100", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T15:55:00Z", "source": "fixture", "freshness_status": "CURRENT"}
    if include_vix:
        symbols["VIX"] = {"last_price": "18", "close": "18", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T15:55:00Z", "source": "fixture", "freshness_status": "CURRENT"}
    breadth = {"freshness_status": "MISSING", "source": None}
    if include_breadth:
        breadth = {"breadth_down_pct": "45", "advance_decline_delta": "100", "freshness_status": "CURRENT", "source": "fixture"}
    _write_json(
        root / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json",
        {"day_utc": DAY, "market_session_date": DAY, "status": "CURRENT", "source": "fixture", "provider_results": [{"provider": "fixture", "request_status": "SUCCESS"}], "symbols": symbols, "breadth": breadth, "missing_fields": [], "stale_fields": [], "usable_for_candidate_generation": True},
    )


def test_data_registry_generates_market_context_items(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _market_data(root)
    payload = build_data_registry_v1(truth_root=root, day_utc=DAY, symbols=["SPY", "QQQ", "IWM"])
    paths = write_data_registry_v1(truth_root=root, day_utc=DAY, payload=payload)
    ids = {row["data_item_id"] for row in payload["data_items"]}

    assert Path(paths["json"]).exists()
    assert {"market.calendar.session", "provider.health", "runtime.freshness", "market.price.SPY", "market.price.QQQ", "market.volatility.VIX", "market.breadth.down_pct", "market.breadth.advance_decline_delta"} <= ids
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False


def test_refresh_market_data_missing_provider_fails_closed(tmp_path: Path, monkeypatch) -> None:
    empty_config = tmp_path / "empty_market_data.env"
    empty_config.write_text("", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(empty_config))
    monkeypatch.delenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", raising=False)
    monkeypatch.delenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", raising=False)
    payload = build_market_data_report_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    paths = write_market_data_report_v1(truth_root=tmp_path / "truth", day_utc=DAY, payload=payload)

    assert Path(paths["json"]).exists()
    assert payload["status"] == "FAILED"
    assert payload["failure_reason"] == "MARKET_DATA_PROVIDER_NOT_CONFIGURED"
    assert payload["usable_for_candidate_generation"] is False



def test_legacy_market_report_classifies_non_sleeve_gaps_as_warnings(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "truth"
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=LOCAL_CACHE\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    _write_json(
        root / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json",
        {
            "contracts": [
                {
                    "sleeve_id": "fixture",
                    "required_inputs": [
                        {"data_item_id": "market.price.SPY"},
                        {"data_item_id": "market.price.QQQ"},
                    ],
                    "optional_inputs": [],
                }
            ]
        },
    )
    for symbol in ("SPY", "QQQ"):
        path = root / "market_data_snapshot_v1" / symbol / f"{DAY[:4]}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"symbol": symbol, "timestamp_utc": f"{DAY}T21:00:00Z", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000}) + "\n", encoding="utf-8")
    symbol_map = {
        "required_symbols": ["SPY", "QQQ", "DIA"],
        "requested_symbols_source": "production_scan_dataset+sleeve_required_symbols",
        "symbols": {
            "SPY": {"providers": {"LOCAL_CACHE": "SPY"}},
            "QQQ": {"providers": {"LOCAL_CACHE": "QQQ"}},
            "DIA": {"providers": {"LOCAL_CACHE": "DIA"}},
        },
    }

    payload = build_market_data_report_v1(truth_root=root, day_utc=DAY, symbol_map_override=symbol_map)

    assert payload["legacy_market_report_status"] == "PARTIAL"
    assert payload["status"] == "CURRENT_WITH_WARNINGS"
    assert payload["current_sleeve_market_readiness_status"] == "READY"
    assert payload["blocking_missing_symbols"] == []
    assert payload["legacy_optional_missing_symbols"] == ["DIA"]
    assert payload["market_gap_classification"]["DIA"]["classification"] == "LEGACY_OPTIONAL"
    assert payload["usable_for_current_sleeve_candidate_generation"] is True
    assert payload["usable_for_candidate_generation"] is True

def test_provider_config_file_is_detected(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "market_data.env"
    config.write_text(
        "\n".join(
            [
                "AEGIS_MARKET_DATA_PROVIDER_PRIMARY=LOCAL_CACHE",
                "AEGIS_MARKET_DATA_PROVIDER_FALLBACK=STOOQ",
                "AEGIS_MARKET_DATA_ALLOW_DELAYED=true",
                "AEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true",
                "AEGIS_MARKET_DATA_REQUIRE_BREADTH=false",
                "AEGIS_MARKET_DATA_TIMEOUT_SECONDS=20",
                "AEGIS_MARKET_DATA_CACHE_TTL_SECONDS=900",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))

    payload = provider_config_from_env_v1()

    assert payload.primary == "LOCAL_CACHE"
    assert payload.fallback == "STOOQ"
    assert payload.allow_delayed is True
    assert payload.require_current_session is True
    assert payload.require_breadth is False
    assert payload.timeout_seconds == 20
    assert payload.cache_ttl_seconds == 900


def test_symbol_map_covers_all_sleeve_required_symbols(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AEGIS_RUNTIME_UNIVERSE_MODE", "sleeve_required_only")
    repo = _repo(tmp_path)
    payload = build_symbol_map_v1(repo_root=repo, day_utc=DAY)
    paths = write_symbol_map_v1(truth_root=tmp_path / "truth", day_utc=DAY, payload=payload)

    assert Path(paths["json"]).exists()
    assert payload["mapping_missing_symbols"] == []
    assert sorted(payload["required_symbols"]) == ["DBC", "DIA", "GLD", "HYG", "IEF", "IWM", "LQD", "QQQ", "SPY", "TLT", "UUP", "VIX"]
    assert payload["symbols"]["SPY"]["providers"]["STOOQ"] == "SPY.US"
    assert payload["symbols"]["QQQ"]["providers"]["STOOQ"] == "QQQ.US"
    assert payload["runtime_universe_mode"] == "sleeve_required_only"
    assert payload["requested_symbols_source"] == "ENGINE_MODEL_REGISTRY_V1.allowed_symbols+sleeve_required_context"
    assert payload["symbols"]["VIX"]["providers"]["STOOQ"] == "^VIX"
    assert payload["symbols"]["VIX"]["aliases"] == ["VIX", "^VIX", "$VIX", "vix", "VIXCLS"]
    assert provider_symbol_candidates_v1(payload, "VIX", "LOCAL_CACHE") == ("VIX", "^VIX", "vix", "VIXCLS", "$VIX")
    assert provider_symbol_candidates_v1(payload, "VIX", "STOOQ") == ("^VIX", "vix")
    assert normalize_market_symbol_v1("^VIX") == "VIX"
    assert normalize_market_symbol_v1("VIXCLS") == "VIX"


def test_production_scan_dataset_mode_uses_dataset_plus_sleeve_required_symbols(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("AEGIS_RUNTIME_UNIVERSE_MODE", raising=False)
    monkeypatch.delenv("AEGIS_PRODUCTION_SCAN_DATASET_ID", raising=False)
    repo = _repo(tmp_path)
    _write_production_dataset(repo, symbols=["AAA", "BBB", "SPY"])

    universe = build_runtime_symbol_universe_v1(repo_root=repo)
    payload = build_symbol_map_v1(repo_root=repo, day_utc=DAY)

    assert universe["runtime_universe_mode"] == "production_scan_dataset"
    assert universe["production_scan_dataset_id"] == "ds_ohlcv_1d_liquid_etf_core_fixture"
    assert universe["production_scan_universe_count"] == 3
    assert universe["sleeve_required_symbol_count"] == 12
    assert universe["total_requested_symbol_count"] == 14
    assert universe["requested_symbols_source"] == "production_scan_dataset+sleeve_required_symbols"
    assert universe["minimum_viable_runtime_reference_removed"] is True
    assert {"AAA", "BBB", "SPY", "UUP", "VIX"} <= set(universe["requested_symbols"])
    assert sorted(universe["requested_symbols"]) != ["DBC", "DIA", "GLD", "HYG", "IEF", "IWM", "LQD", "QQQ", "SPY", "TLT", "UUP", "VIX"]
    assert payload["symbols"]["AAA"]["providers"]["STOOQ"] == "AAA.US"
    assert payload["symbols"]["VIX"]["providers"]["STOOQ"] == "^VIX"
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False




def test_symbol_map_uses_canonical_dynamic_sleeve_universe_for_market_data_demand(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AEGIS_RUNTIME_UNIVERSE_MODE", "sleeve_required_only")
    repo = _repo(tmp_path)
    truth_root = tmp_path / "truth"
    dynamic_symbols = [f"ZZ{i:03d}" for i in range(120)]
    _write_json(
        repo / "governance/02_REGISTRIES/ENGINE_UNIVERSE_POLICY_V1.json",
        {
            "policies": [
                {
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "universe_mode": "LIQUIDITY_RANKED_SYMBOLS",
                    "symbol_source_class": "DYNAMIC_SAME_DAY",
                    "target_symbol_count": 100,
                    "curated_symbols": [],
                }
            ]
        },
    )
    ranked_path = truth_root / "reports" / "ranked_symbol_universe_v1" / DAY / "ranked_symbol_universe.v1.json"
    _write_json(ranked_path, {"status": "PASS", "day_utc": DAY, "symbols": dynamic_symbols})

    universe = build_runtime_symbol_universe_v1(repo_root=repo, truth_root=truth_root, day_utc=DAY)

    assert universe["canonical_sleeve_symbol_resolution_used"] is True
    assert universe["sleeve_required_symbol_source"] == "canonical_sleeve_universe_resolver_v1"
    assert universe["requested_symbols_source"] == "canonical_sleeve_required_symbols"
    assert set(dynamic_symbols[:100]) <= set(universe["requested_symbols"])
    assert "ZZ100" not in set(universe["requested_symbols"])
    assert {"SPY", "QQQ", "IWM", "DIA", "VIX"} <= set(universe["requested_symbols"])
    trend = next(row for row in universe["per_sleeve_symbol_resolution"] if row["sleeve_id"] == "C2_TREND_EQ_PRIMARY_V1")
    assert trend["resolution_status"] == "CANONICAL_RESOLVED"
    assert trend["source"] == "ranked_symbol_universe_v1"
    assert trend["symbol_count"] == 100


def test_runtime_universe_includes_real_raw_signal_symbols(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AEGIS_RUNTIME_UNIVERSE_MODE", "sleeve_required_only")
    repo = _repo(tmp_path)
    truth_root = tmp_path / "truth"
    sleeve_path = truth_root / "reports" / "sleeve_evaluation_kernel_v1" / DAY / "C2_TREND_EQ_PRIMARY_V1" / "sleeve_evaluation.v1.json"
    _write_json(
        sleeve_path,
        {
            "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "status": "BLOCKED",
            "current_status": "BLOCKED",
            "output_count": 2,
            "artifact_path": str(sleeve_path),
            "signal_state": {"state": "ACTIVE"},
            "exposure_intent_batch": {
                "output_intents": [
                    {"intent_id": "trend_crwd", "intent_path": str(truth_root / "signals/crwd.json"), "schema_id": "exposure_intent", "symbol": "CRWD"},
                    {"intent_id": "trend_hon", "intent_path": str(truth_root / "signals/hon.json"), "schema_id": "exposure_intent", "symbol": "HON"},
                ]
            },
        },
    )
    _write_json(truth_root / "reports" / "sleeve_evaluation_kernel_v1" / DAY / "sleeve_evaluation_rollup.v1.json", {"day_utc": DAY, "outcomes": [json.loads(sleeve_path.read_text(encoding="utf-8"))]})

    universe = build_runtime_symbol_universe_v1(repo_root=repo, truth_root=truth_root, day_utc=DAY)

    assert {"CRWD", "HON"} <= set(universe["requested_symbols"])
    assert universe["raw_signal_symbol_count"] == 2
    assert universe["requested_symbols_source"].endswith("real_raw_signal_registry")



def test_data_registry_unions_market_inputs_report_and_runtime_symbols(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write_json(
        root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json",
        {
            "day_utc": DAY,
            "market_session_date": DAY,
            "requested_symbols": ["AAPL", "CRWD"],
            "symbols": {
                "AAPL": {"last_price": "100", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T16:00:00Z", "source": "fixture", "freshness_status": "CURRENT"},
                "CRWD": {"last_price": "200", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T16:00:00Z", "source": "fixture", "freshness_status": "CURRENT"},
            },
            "provider_results": [{"provider": "fixture", "request_status": "SUCCESS"}],
        },
    )
    _write_json(
        root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json",
        {
            "day_utc": DAY,
            "input_records": [
                {"data_item_id": "market.price.AAPL", "symbol": "AAPL", "value": 100, "field_type": "last_price", "day_utc": DAY, "source_timestamp_utc": f"{DAY}T16:00:00Z", "source_vendor": "fixture", "validation_status": "VALID"},
            ],
        },
    )

    payload = build_data_registry_v1(truth_root=root, day_utc=DAY, symbols=["HON"])

    assert {"AAPL", "CRWD", "HON"} <= set(payload["requested_symbols"])

def test_data_registry_surfaces_runtime_universe_metadata(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _market_data(root)
    metadata = {
        "runtime_universe_mode": "production_scan_dataset",
        "production_scan_dataset_id": "ds_ohlcv_1d_liquid_etf_core_fixture",
        "dataset_snapshot_id": "ds_ohlcv_1d_liquid_etf_core_fixture",
        "production_scan_universe_count": 3,
        "sleeve_required_symbol_count": 12,
        "requested_symbols_source": "production_scan_dataset+sleeve_required_symbols",
        "minimum_viable_runtime_reference_removed": True,
    }

    payload = build_data_registry_v1(truth_root=root, day_utc=DAY, symbols=["SPY", "QQQ", "IWM"], universe_metadata=metadata)
    summary = write_data_registry_v1(truth_root=root, day_utc=DAY, payload=payload)

    assert payload["runtime_universe_mode"] == "production_scan_dataset"
    assert payload["production_scan_dataset_id"] == "ds_ohlcv_1d_liquid_etf_core_fixture"
    assert payload["production_scan_universe_count"] == 3
    assert payload["sleeve_required_symbol_count"] == 12
    assert payload["minimum_viable_runtime_reference_removed"] is True
    assert "universe_mode: production_scan_dataset" in Path(summary["summary"]).read_text(encoding="utf-8")


def test_runtime_commands_are_wired_to_shared_universe_builder() -> None:
    refresh = (ROOT / "ops/tools/refresh_aegis_market_data_v1.py").read_text(encoding="utf-8")
    registry = (ROOT / "ops/tools/build_aegis_data_registry_v1.py").read_text(encoding="utf-8")
    diagnostics = (ROOT / "ops/aegis/candidate_generation_diagnostics_v1.py").read_text(encoding="utf-8")
    ui = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")

    assert "build_symbol_map_v1(repo_root=REPO_ROOT" in refresh
    assert "_authoritative_sleeve_inventory" not in refresh
    assert 'requested.update({"SPY", "QQQ", "IWM", "DIA", "VIX"})' not in refresh
    assert "build_runtime_symbol_universe_v1(repo_root=REPO_ROOT)" in registry
    assert "runtime_universe_mode" in diagnostics
    assert "production_scan_universe_count" in diagnostics
    assert "Universe mode" in ui
    assert "Production scan universe" in ui


def test_vix_alias_resolution_works_from_local_cache(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "truth"
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=LOCAL_CACHE\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    cache = root / "market_data_snapshot_v1" / "^VIX" / f"{DAY[:4]}.jsonl"
    cache.parent.mkdir(parents=True)
    cache.write_text(json.dumps({"symbol": "^VIX", "timestamp_utc": f"{DAY}T21:00:00Z", "open": 18.0, "high": 19.0, "low": 17.5, "close": 18.25, "volume": 0}) + "\n", encoding="utf-8")
    symbol_map = {
        "required_symbols": ["VIX"],
        "symbols": {
            "VIX": {
                "canonical_symbol": "VIX",
                "providers": {"LOCAL_CACHE": "VIX"},
                "provider_aliases": {"LOCAL_CACHE": ["VIX", "^VIX", "vix", "VIXCLS", "$VIX"]},
            }
        },
    }
    write_symbol_map_v1(truth_root=root, day_utc=DAY, payload=symbol_map)

    payload = build_market_data_report_v1(truth_root=root, day_utc=DAY, symbol_map_override=symbol_map)
    registry = build_data_registry_v1(truth_root=root, day_utc=DAY, symbols=["VIX"])
    by_id = {row["data_item_id"]: row for row in registry["data_items"]}

    assert payload["status"] == "CURRENT"
    assert payload["missing_symbols"] == []
    assert payload["symbols"]["VIX"]["provider_symbol"] == "^VIX"
    assert payload["symbols"]["VIX"]["canonical_symbol"] == "VIX"
    assert by_id["market.volatility.VIX"]["status"] == "CURRENT"
    assert by_id["market.volatility.VIX"]["symbol"] == "VIX"


def test_missing_vix_has_stable_fail_closed_explanation(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "truth"
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=LOCAL_CACHE\nAEGIS_MARKET_DATA_PROVIDER_FALLBACK=DISABLED\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    symbol_map = {
        "required_symbols": ["VIX"],
        "symbols": {"VIX": {"providers": {"LOCAL_CACHE": "VIX", "DISABLED": "VIX"}, "provider_aliases": {"LOCAL_CACHE": ["VIX", "^VIX", "vix", "VIXCLS", "$VIX"]}}},
    }
    write_symbol_map_v1(truth_root=root, day_utc=DAY, payload=symbol_map)

    payload = build_market_data_report_v1(truth_root=root, day_utc=DAY, symbol_map_override=symbol_map)
    explanation = payload["missing_symbol_explanations"]["VIX"]

    assert payload["missing_symbols"] == ["VIX"]
    assert explanation["canonical_symbol"] == "VIX"
    assert "C2_VOL_INCOME_DEFINED_RISK_V1" in explanation["blocked_sleeves"]
    assert explanation["fail_closed"] is True
    assert explanation["synthetic_data_allowed"] is False
    assert "READY_PARTIAL" in explanation["operator_message"]
    assert "No execution is authorized" in explanation["operator_message"]


def test_manual_csv_drop_current_session_produces_current_status(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "truth"
    drop = tmp_path / "manual_drop"
    drop.mkdir(parents=True)
    drop.joinpath("latest_prices.csv").write_text(
        "symbol,session_date,last,close,source,timestamp_utc\n"
        "SPY,2026-05-18,520.10,520.10,fixture,2026-05-18T21:00:00Z\n"
        "QQQ,2026-05-18,450.20,450.20,fixture,2026-05-18T21:00:00Z\n",
        encoding="utf-8",
    )
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=MANUAL_CSV_DROP\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    monkeypatch.setattr("ops.aegis.market_data.market_data_provider_v1.MANUAL_DROP_ROOT", drop)
    symbol_map = {
        "required_symbols": ["SPY", "QQQ"],
        "symbols": {
            "SPY": {"providers": {"MANUAL_CSV_DROP": "SPY"}},
            "QQQ": {"providers": {"MANUAL_CSV_DROP": "QQQ"}},
        },
    }
    write_symbol_map_v1(truth_root=root, day_utc=DAY, payload=symbol_map)
    payload = build_market_data_report_v1(truth_root=root, day_utc=DAY, symbol_map_override=symbol_map)

    assert payload["status"] == "CURRENT"
    assert payload["fetched_symbols"] == ["QQQ", "SPY"]
    assert payload["symbols"]["SPY"]["freshness_status"] == "CURRENT"
    assert payload["usable_for_candidate_generation"] is True


def test_manual_csv_drop_stale_session_is_not_marked_current(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "truth"
    drop = tmp_path / "manual_drop"
    drop.mkdir(parents=True)
    drop.joinpath("latest_prices.csv").write_text(
        "symbol,session_date,last,close,source,timestamp_utc\n"
        "SPY,2026-05-15,518.10,518.10,fixture,2026-05-15T15:59:00Z\n",
        encoding="utf-8",
    )
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=MANUAL_CSV_DROP\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    monkeypatch.setattr("ops.aegis.market_data.market_data_provider_v1.MANUAL_DROP_ROOT", drop)
    symbol_map = {"required_symbols": ["SPY"], "symbols": {"SPY": {"providers": {"MANUAL_CSV_DROP": "SPY"}}}}
    write_symbol_map_v1(truth_root=root, day_utc=DAY, payload=symbol_map)
    payload = build_market_data_report_v1(truth_root=root, day_utc=DAY, symbol_map_override=symbol_map)

    assert payload["status"] == "STALE"
    assert payload["symbols"]["SPY"]["freshness_status"] == "STALE"
    assert payload["stale_symbols"] == ["SPY"]
    assert payload["usable_for_candidate_generation"] is False


def test_provider_fallback_attempted_after_primary_failure(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "truth"
    drop = tmp_path / "manual_drop"
    drop.mkdir(parents=True)
    drop.joinpath("latest_prices.csv").write_text(
        "symbol,session_date,last,close,source,timestamp_utc\n"
        "SPY,2026-05-18,520.10,520.10,fixture,2026-05-18T21:00:00Z\n",
        encoding="utf-8",
    )
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=DISABLED\nAEGIS_MARKET_DATA_PROVIDER_FALLBACK=MANUAL_CSV_DROP\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    monkeypatch.setattr("ops.aegis.market_data.market_data_provider_v1.MANUAL_DROP_ROOT", drop)
    symbol_map = {"required_symbols": ["SPY"], "symbols": {"SPY": {"providers": {"MANUAL_CSV_DROP": "SPY", "DISABLED": "SPY"}}}}
    write_symbol_map_v1(truth_root=root, day_utc=DAY, payload=symbol_map)
    payload = build_market_data_report_v1(truth_root=root, day_utc=DAY, symbol_map_override=symbol_map)

    providers = [row["provider"] for row in payload["provider_results"]]
    assert providers == ["DISABLED", "MANUAL_CSV_DROP"]
    assert payload["status"] == "CURRENT"
    assert payload["fetched_symbols"] == ["SPY"]


def test_stooq_quote_fallback_uses_real_rows_and_rejects_nd(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=STOOQ\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))

    class Response:
        def __init__(self, text: str) -> None:
            self.text = text

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            return self.text.encode("utf-8")

    def fake_urlopen(url: str, timeout: float):
        if "/q/d/l/" in url:
            return Response("Get your apikey:\nAppend the <apikey> variable with its value to your requests.")
        if "spy.us" in url:
            return Response("Symbol,Date,Time,Open,High,Low,Close,Volume\nSPY.US,2026-05-18,22:00:24,739.83,741.415,733.39,738.65,47641946\n")
        if "%5Evix" in url or "^vix" in url:
            return Response("Symbol,Date,Time,Open,High,Low,Close,Volume\n^VIX,N/D,N/D,N/D,N/D,N/D,N/D,N/D\n")
        return Response("Symbol,Date,Time,Open,High,Low,Close,Volume\n")

    monkeypatch.setattr("ops.aegis.market_data.market_data_provider_v1.urllib.request.urlopen", fake_urlopen)
    symbol_map = {
        "symbols": {
            "SPY": {"providers": {"STOOQ": "SPY.US"}},
            "VIX": {"providers": {"STOOQ": "^VIX"}},
        }
    }

    result = fetch_market_data_v1(truth_root=tmp_path / "truth", day_utc=DAY, symbols=["SPY", "VIX"], symbol_map=symbol_map)

    assert result.request_status == "SUCCESS"
    assert result.fetched_symbols == ("SPY",)
    assert result.missing_symbols == ("VIX",)
    assert result.symbols["SPY"]["freshness_status"] == "CURRENT"
    assert result.symbols["SPY"]["close"] == 738.65
    assert "VIX" not in result.symbols


def test_vix_fallback_provider_mapping_returns_canonical_symbol(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=STOOQ\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))

    class Response:
        def __init__(self, text: str) -> None:
            self.text = text

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            return self.text.encode("utf-8")

    def fake_urlopen(url: str, timeout: float):
        if "/q/d/l/" in url:
            return Response("Get your apikey:\nAppend the <apikey> variable with its value to your requests.")
        if "%5Evix" in url or "^vix" in url:
            return Response("Symbol,Date,Time,Open,High,Low,Close,Volume\n^VIX,2026-05-18,22:00:24,18.1,19.0,17.9,18.5,0\n")
        return Response("Symbol,Date,Time,Open,High,Low,Close,Volume\n")

    monkeypatch.setattr("ops.aegis.market_data.market_data_provider_v1.urllib.request.urlopen", fake_urlopen)
    symbol_map = {"symbols": {"VIX": {"providers": {"STOOQ": "^VIX"}, "provider_aliases": {"STOOQ": ["^VIX", "vix"]}}}}

    result = fetch_market_data_v1(truth_root=tmp_path / "truth", day_utc=DAY, symbols=["VIX"], symbol_map=symbol_map)

    assert result.request_status == "SUCCESS"
    assert result.fetched_symbols == ("VIX",)
    assert result.missing_symbols == ()
    assert result.symbols["VIX"]["provider_symbol"] == "^VIX"
    assert result.symbols["VIX"]["canonical_symbol"] == "VIX"


def test_all_enabled_sleeves_get_contracts_and_optional_breadth_warns_only(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _market_data(root)
    registry = build_data_registry_v1(truth_root=root, day_utc=DAY, symbols=sorted({symbol for _, symbols in SLEEVES for symbol in symbols}))
    write_data_registry_v1(truth_root=root, day_utc=DAY, payload=registry)
    contracts = build_sleeve_input_contracts_v1(repo_root=repo, day_utc=DAY)
    write_sleeve_input_contracts_v1(truth_root=root, day_utc=DAY, payload=contracts)
    readiness = build_sleeve_readiness_v1(truth_root=root, day_utc=DAY)

    assert contracts["contract_count"] == 7
    by_id = {row["sleeve_id"]: row for row in readiness["sleeves"]}
    assert by_id["C2_TREND_EQ_PRIMARY_V1"]["readiness"] == "READY_WITH_WARNINGS"
    assert "market.breadth.down_pct" in by_id["C2_TREND_EQ_PRIMARY_V1"]["warning_inputs"]
    assert "market.breadth.down_pct" not in by_id["C2_TREND_EQ_PRIMARY_V1"]["blocking_inputs"]
    assert by_id["C2_VOL_INCOME_DEFINED_RISK_V1"]["readiness"] == "READY_WITH_WARNINGS"
    assert by_id["C2_VOL_INCOME_DEFINED_RISK_V1"]["blocking_inputs"] == []
    assert "market.volatility.VIX" in by_id["C2_VOL_INCOME_DEFINED_RISK_V1"]["warning_inputs"]


def test_missing_contract_blocks_only_that_sleeve(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _market_data(root, include_vix=True, include_breadth=True)
    registry = build_data_registry_v1(truth_root=root, day_utc=DAY, symbols=sorted({symbol for _, symbols in SLEEVES for symbol in symbols}))
    write_data_registry_v1(truth_root=root, day_utc=DAY, payload=registry)
    contracts = build_sleeve_input_contracts_v1(repo_root=repo, day_utc=DAY)
    contracts["contracts"][0]["contract_status"] = "CONTRACT_MISSING"
    write_sleeve_input_contracts_v1(truth_root=root, day_utc=DAY, payload=contracts)
    readiness = build_sleeve_readiness_v1(truth_root=root, day_utc=DAY)
    by_id = {row["sleeve_id"]: row for row in readiness["sleeves"]}

    assert by_id["C2_CROSS_ASSET_TREND_V1"]["readiness"] == "BLOCKED"
    assert all(row["readiness"] == "READY" for sleeve, row in by_id.items() if sleeve != "C2_CROSS_ASSET_TREND_V1")


def test_stale_symbol_blocks_only_sleeves_requiring_that_symbol(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _market_data(root, include_vix=True, include_breadth=True)
    market_path = root / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json"
    market = json.loads(market_path.read_text(encoding="utf-8"))
    market["symbols"]["QQQ"]["freshness_status"] = "STALE"
    market["symbols"]["QQQ"]["market_session_date"] = "2026-05-15"
    _write_json(market_path, market)
    registry = build_data_registry_v1(truth_root=root, day_utc=DAY, symbols=sorted({symbol for _, symbols in SLEEVES for symbol in symbols}))
    write_data_registry_v1(truth_root=root, day_utc=DAY, payload=registry)
    contracts = build_sleeve_input_contracts_v1(repo_root=repo, day_utc=DAY)
    write_sleeve_input_contracts_v1(truth_root=root, day_utc=DAY, payload=contracts)
    readiness = build_sleeve_readiness_v1(truth_root=root, day_utc=DAY)
    by_id = {row["sleeve_id"]: row for row in readiness["sleeves"]}

    assert by_id["C2_MEAN_REVERSION_EQ_V1"]["readiness"] == "BLOCKED"
    assert by_id["C2_TREND_EQ_PRIMARY_V1"]["readiness"] == "READY"
    assert "market.price.QQQ" in by_id["C2_MEAN_REVERSION_EQ_V1"]["blocking_inputs"]


def _response(text: str):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            return text.encode("utf-8")

    return Response()


def test_cboe_before_finalization_missing_vix_row_is_not_finalized(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=CBOE\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    monkeypatch.setenv("AEGIS_MARKET_DATA_NOW_UTC", "2026-05-21T19:27:00Z")
    monkeypatch.setattr("ops.aegis.market_data.market_data_provider_v1.urllib.request.urlopen", lambda url, timeout: _response("DATE,OPEN,HIGH,LOW,CLOSE\n05/20/2026,18,18,17,17.44\n"))
    symbol_map = {"required_symbols": ["VIX"], "symbols": {"VIX": {"providers": {"CBOE": "VIX"}}}}

    result = fetch_market_data_v1(truth_root=tmp_path / "truth", day_utc="2026-05-21", symbols=["VIX"], symbol_map=symbol_map)

    assert result.request_status == "SOURCE_NOT_FINALIZED"
    assert result.failure_reason == "SOURCE_NOT_FINALIZED"
    assert result.provider_attempts[0]["status"] == "SOURCE_NOT_FINALIZED"


def test_cboe_after_finalization_missing_vix_row_is_source_unavailable(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=CBOE\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    monkeypatch.setenv("AEGIS_MARKET_DATA_NOW_UTC", "2026-05-21T23:30:00Z")
    monkeypatch.setattr("ops.aegis.market_data.market_data_provider_v1.urllib.request.urlopen", lambda url, timeout: _response("DATE,OPEN,HIGH,LOW,CLOSE\n05/20/2026,18,18,17,17.44\n"))
    symbol_map = {"required_symbols": ["VIX"], "symbols": {"VIX": {"providers": {"CBOE": "VIX"}}}}

    result = fetch_market_data_v1(truth_root=tmp_path / "truth", day_utc="2026-05-21", symbols=["VIX"], symbol_map=symbol_map)

    assert result.request_status == "SOURCE_UNAVAILABLE"
    assert result.failure_reason == "SOURCE_UNAVAILABLE"
    assert result.provider_attempts[0]["status"] == "SOURCE_UNAVAILABLE"


def test_stooq_per_symbol_timeout_does_not_discard_successful_symbols(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=STOOQ\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\nAEGIS_MARKET_DATA_STOOQ_RETRIES=0\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))

    def fake_urlopen(url: str, timeout: float):
        if "spy.us" in url:
            return _response("Date,Open,High,Low,Close,Volume\n2026-05-18,100,101,99,100,1000\n")
        raise TimeoutError("simulated slow symbol")

    monkeypatch.setattr("ops.aegis.market_data.market_data_provider_v1.urllib.request.urlopen", fake_urlopen)
    symbol_map = {"required_symbols": ["SPY", "QQQ"], "symbols": {"SPY": {"providers": {"STOOQ": "SPY.US"}}, "QQQ": {"providers": {"STOOQ": "QQQ.US"}}}}

    result = fetch_market_data_v1(truth_root=tmp_path / "truth", day_utc=DAY, symbols=["SPY", "QQQ"], symbol_map=symbol_map)

    assert result.fetched_symbols == ("SPY",)
    assert result.missing_symbols == ("QQQ",)
    assert {row["status"] for row in result.provider_attempts} >= {"SUCCESS", "TIMEOUT"}
    assert result.symbols["SPY"]["data_finality"] == "FINAL_EOD"


def test_partial_success_report_includes_accepted_and_rejected_symbols(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=STOOQ\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\nAEGIS_MARKET_DATA_STOOQ_RETRIES=0\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))

    def fake_urlopen(url: str, timeout: float):
        if "spy.us" in url:
            return _response("Date,Open,High,Low,Close,Volume\n2026-05-18,100,101,99,100,1000\n")
        raise TimeoutError("simulated slow symbol")

    monkeypatch.setattr("ops.aegis.market_data.market_data_provider_v1.urllib.request.urlopen", fake_urlopen)
    root = tmp_path / "truth"
    symbol_map = {"required_symbols": ["SPY", "QQQ"], "symbols": {"SPY": {"providers": {"STOOQ": "SPY.US"}}, "QQQ": {"providers": {"STOOQ": "QQQ.US"}}}}

    payload = build_market_data_report_v1(truth_root=root, day_utc=DAY, symbol_map_override=symbol_map)
    paths = write_market_data_report_v1(truth_root=root, day_utc=DAY, payload=payload)

    assert payload["operator_market_data_state"] == "PARTIAL_PROVIDER_SUCCESS"
    assert payload["fetched_symbols"] == ["SPY"]
    assert payload["missing_symbols"] == ["QQQ"]
    attempts = json.loads(Path(paths["provider_attempts_json"]).read_text(encoding="utf-8"))
    assert attempts["attempt_count"] >= 2
    assert any(row["accepted_reason"] for row in attempts["attempts"])
    assert any(row["rejected_reason"] for row in attempts["attempts"])


def test_prior_day_final_row_does_not_satisfy_current_day_final_requirement(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "truth"
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=LOCAL_CACHE\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    path = root / "market_data_snapshot_v1" / "SPY" / "2026.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"symbol": "SPY", "timestamp_utc": "2026-05-20T21:00:00Z", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000}) + "\n", encoding="utf-8")
    symbol_map = {"required_symbols": ["SPY"], "symbols": {"SPY": {"providers": {"LOCAL_CACHE": "SPY"}}}}

    payload = build_market_data_report_v1(truth_root=root, day_utc="2026-05-21", symbol_map_override=symbol_map)

    assert payload["symbols"]["SPY"]["data_finality"] == "FINAL_EOD"
    assert payload["symbols"]["SPY"]["freshness_status"] == "STALE"
    assert payload["usable_for_candidate_generation"] is False
    assert payload["operator_market_data_state"] == "CURRENT_DAY_DATA_STALE"


def test_provisional_intraday_data_is_visible_and_candidate_generation_ready_for_intraday_mode(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "truth"
    drop = tmp_path / "manual_drop"
    drop.mkdir(parents=True)
    drop.joinpath("latest_prices.csv").write_text("symbol,session_date,last,close,source,timestamp_utc\nSPY,2026-05-18,520.10,520.10,fixture,2026-05-18T15:59:00Z\n", encoding="utf-8")
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=MANUAL_CSV_DROP\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    monkeypatch.setattr("ops.aegis.market_data.market_data_provider_v1.MANUAL_DROP_ROOT", drop)
    symbol_map = {"required_symbols": ["SPY"], "symbols": {"SPY": {"providers": {"MANUAL_CSV_DROP": "SPY"}}}}

    payload = build_market_data_report_v1(truth_root=root, day_utc=DAY, symbol_map_override=symbol_map)

    assert payload["symbols"]["SPY"]["freshness_status"] == "CURRENT"
    assert payload["symbols"]["SPY"]["data_finality"] == "PROVISIONAL_INTRADAY"
    assert payload["provisional_intraday_symbols"] == ["SPY"]
    assert payload["usable_for_candidate_generation"] is True
    assert payload["usable_for_current_sleeve_candidate_generation"] is True
    assert payload["candidate_generation_label"] == "NON_CERTIFIED"
    assert payload["candidate_lane"] == "PROVISIONAL"
    assert payload["usable_for_execution_candidate_generation"] is False
    assert payload["final_eod_certification_status"] == "PENDING"
    assert payload["operator_market_data_state"] == "INTRADAY_OPERATIONAL_READY"


def test_market_data_timers_split_intraday_refresh_from_final_eod_certification() -> None:
    intraday_timer = (ROOT / "ops/systemd/user/aegis-market-data-refresh-v1.timer").read_text(encoding="utf-8")
    intraday_service = (ROOT / "ops/systemd/user/aegis-market-data-refresh-v1.service").read_text(encoding="utf-8")
    final_timer = (ROOT / "ops/systemd/user/aegis-market-data-final-eod-v1.timer").read_text(encoding="utf-8")
    final_service = (ROOT / "ops/systemd/user/aegis-market-data-final-eod-v1.service").read_text(encoding="utf-8")

    assert "AEGIS_MARKET_DATA_MODE=INTRADAY_OPERATIONAL" in intraday_service
    assert "--market-data-mode INTRADAY_OPERATIONAL" in intraday_service
    assert "OnCalendar=*-*-* 09:35:00 America/New_York" in intraday_timer
    assert "OnCalendar=*-*-* 11:55:00 America/New_York" in intraday_timer
    assert "OnCalendar=*-*-* 15:45:00 America/New_York" in intraday_timer
    assert "OnCalendar=*-*-* 16:10:00 America/New_York" in intraday_timer
    assert "OnCalendar=*-*-* 16:30:00 America/New_York" not in intraday_timer

    assert "AEGIS_MARKET_DATA_MODE=FINAL_EOD_CERTIFIED" in final_service
    assert "--market-data-mode FINAL_EOD_CERTIFIED" in final_service
    assert "OnCalendar=*-*-* 16:30:00 America/New_York" in final_timer
    assert "OnCalendar=*-*-* 17:00:00 America/New_York" in final_timer
    assert "OnCalendar=*-*-* 18:00:00 America/New_York" in final_timer
    assert "OnCalendar=*-*-* 08:15:00 America/New_York" in final_timer


def test_ad_hoc_sleeves_now_command_uses_intraday_operational_mode() -> None:
    package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
    command = package["scripts"]["aegis:run-sleeves-now"]

    assert "run_aegis_intraday_sleeves_now_v1.py" in command
    assert "--environment PAPER" in command
    wrapper = (ROOT / "ops/tools/run_aegis_intraday_sleeves_now_v1.py").read_text(encoding="utf-8")
    assert "INTRADAY_OPERATIONAL" in wrapper
    assert "AEGIS_LITE_MANUAL_ONLY" in wrapper
    assert "broker_submit_enabled" in wrapper
    assert "trade_advice_allowed" in wrapper
