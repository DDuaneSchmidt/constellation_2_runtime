from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis import data_registry_v1
from ops.aegis import market_data_inputs_v1 as mdi
from ops.aegis.market_data.market_data_provider_v1 import ProviderResult
from ops.aegis.market_data import market_data_provider_v1 as mdp


DAY = "2026-05-20"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _contracts(root: Path) -> None:
    _write(
        root / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json",
        {
            "schema_id": "aegis_sleeve_input_contracts",
            "contracts": [
                {
                    "sleeve_id": "TREND",
                    "contract_status": "OK",
                    "required_inputs": [{"data_item_id": "market.price.SPY", "required": True}],
                    "optional_inputs": [],
                },
                {
                    "sleeve_id": "VOL",
                    "contract_status": "OK",
                    "required_inputs": [{"data_item_id": "market.volatility.VIX", "required": True}],
                    "optional_inputs": [],
                },
            ],
        },
    )


def _provider_result(*, vix_day: str = DAY) -> ProviderResult:
    return ProviderResult(
        provider="LOCAL_CACHE",
        request_status="STALE" if vix_day != DAY else "SUCCESS",
        timestamp_utc=f"{DAY}T14:50:00Z",
        returned_data_date=DAY,
        symbols={
            "SPY": {
                "symbol": "SPY",
                "canonical_symbol": "SPY",
                "provider": "LOCAL_CACHE",
                "last_price": 100.25,
                "data_timestamp_utc": f"{DAY}T00:00:00Z",
                "market_session_date": DAY,
                "source": "LOCAL_CACHE",
                "source_hash": "a" * 64,
                "freshness_status": "CURRENT",
            },
            "VIX": {
                "symbol": "VIX",
                "canonical_symbol": "VIX",
                "provider": "LOCAL_CACHE",
                "last_price": 18.5,
                "data_timestamp_utc": f"{vix_day}T21:00:00Z",
                "market_session_date": vix_day,
                "source": "LOCAL_CACHE",
                "source_hash": "b" * 64,
                "freshness_status": "CURRENT" if vix_day == DAY else "STALE",
            },
        },
        breadth={},
        provider_results=(),
        requested_symbols=("SPY", "VIX"),
        fetched_symbols=("SPY", "VIX"),
        stale_symbols=(() if vix_day == DAY else ("VIX",)),
        normalized_records=(),
    )


def test_market_data_inputs_validate_prices_and_fail_closed_on_stale_vix(tmp_path: Path, monkeypatch) -> None:
    _contracts(tmp_path)
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "fetch_market_data_v1", lambda **_: _provider_result(vix_day="2026-05-19"))

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")

    statuses = {row["data_item_id"]: row["validation_status"] for row in payload["input_records"]}
    assert statuses["market.price.SPY"] == "VALID"
    assert statuses["market.volatility.VIX"] == "STALE"
    assert payload["status"] == "PARTIAL"
    assert payload["stale_input_ids"] == ["market.volatility.VIX"]
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False



def test_market_data_refresh_classifies_stale_vix_with_no_synthetic_substitute(tmp_path: Path, monkeypatch) -> None:
    from ops.tools import refresh_aegis_market_data_v1 as refresh

    monkeypatch.setattr(refresh, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(refresh, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(refresh, "fetch_market_data_v1", lambda **_: _provider_result(vix_day="2026-05-19"))

    payload = refresh.build_market_data_report_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["status"] == "STALE"
    assert payload["stale_fields"] == ["market.volatility.VIX"]
    vix = payload["missing_symbol_explanations"]["VIX"]
    assert vix["symbol_status"] == "STALE"
    assert vix["blocker_status"] == "STALE"
    assert vix["synthetic_data_allowed"] is False
    assert vix["broker_execution_allowed"] is False



def test_market_data_refresh_passes_explicit_final_eod_mode_to_provider_fetch(tmp_path: Path, monkeypatch) -> None:
    from ops.tools import refresh_aegis_market_data_v1 as refresh

    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "INTRADAY_OPERATIONAL")
    monkeypatch.setattr(refresh, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY"]})
    monkeypatch.setattr(refresh, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY"]})
    captured: dict[str, str] = {}

    def _fetch(**kwargs):
        captured["market_data_mode"] = kwargs["config_override"].market_data_mode
        return ProviderResult(
            provider="LOCAL_CACHE",
            request_status="SUCCESS",
            timestamp_utc=f"{DAY}T21:00:00Z",
            returned_data_date=DAY,
            symbols={},
            breadth={},
            requested_symbols=("SPY",),
            missing_symbols=("SPY",),
            normalized_records=(),
        )

    monkeypatch.setattr(refresh, "fetch_market_data_v1", _fetch)

    payload = refresh.build_market_data_report_v1(truth_root=tmp_path, day_utc=DAY, market_data_mode="FINAL_EOD_CERTIFIED")

    assert captured["market_data_mode"] == "FINAL_EOD_CERTIFIED"
    assert payload["market_data_mode"] == "FINAL_EOD_CERTIFIED"


def test_data_registry_consumes_canonical_market_data_inputs(tmp_path: Path) -> None:
    input_path = tmp_path / "reports/market_data_inputs_v1" / DAY / "market_data_inputs.v1.json"
    _write(
        input_path,
        {
            "schema_id": "market_data_inputs",
            "schema_version": "v1",
            "artifact_id": "market_data_inputs_v1",
            "day_utc": DAY,
            "generated_at_utc": f"{DAY}T15:00:00Z",
            "input_records": [
                {
                    "data_item_id": "market.price.SPY",
                    "symbol": "SPY",
                    "field_type": "price",
                    "value": 100.25,
                    "source_vendor": "LOCAL_CACHE",
                    "source_timestamp_utc": f"{DAY}T00:00:00Z",
                    "day_utc": DAY,
                    "raw_source_hash": "a" * 64,
                    "transformed_value_hash": "c" * 64,
                    "validation_status": "VALID",
                    "reason": "ok",
                }
            ],
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        },
    )

    registry = data_registry_v1.build_data_registry_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY"])

    spy = next(row for row in registry["data_items"] if row["data_item_id"] == "market.price.SPY")
    assert spy["status"] == "CURRENT"
    assert spy["value"] == 100.25
    assert spy["raw_source_hash"] == "a" * 64


def test_valid_vix_clears_volatility_sleeve_readiness(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import sleeve_readiness_v1

    _contracts(tmp_path)
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "fetch_market_data_v1", lambda **_: _provider_result(vix_day=DAY))

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")
    paths = mdi.write_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["market_data_inputs_json"]).exists()

    registry = data_registry_v1.build_data_registry_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY", "VIX"])
    data_registry_v1.write_data_registry_v1(truth_root=tmp_path, day_utc=DAY, payload=registry)
    readiness = sleeve_readiness_v1.build_sleeve_readiness_v1(truth_root=tmp_path, day_utc=DAY)

    vol = next(row for row in readiness["sleeves"] if row["sleeve_id"] == "VOL")
    assert vol["readiness"] == "READY"
    assert "market.volatility.VIX" not in vol["blocking_inputs"]


def test_optional_legacy_inputs_do_not_block_sleeve_readiness(tmp_path: Path) -> None:
    from ops.aegis import sleeve_readiness_v1

    _write(
        tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json",
        {
            "schema_id": "aegis_sleeve_input_contracts",
            "contracts": [
                {
                    "sleeve_id": "TREND",
                    "contract_status": "OK",
                    "required_inputs": [{"data_item_id": "market.price.SPY", "required": True}],
                    "optional_inputs": [{"data_item_id": "market.breadth.down_pct", "required": False}],
                }
            ],
        },
    )
    _write(
        tmp_path / "reports/market_data_inputs_v1" / DAY / "market_data_inputs.v1.json",
        {
            "schema_id": "market_data_inputs",
            "schema_version": "v1",
            "day_utc": DAY,
            "input_records": [
                {
                    "data_item_id": "market.price.SPY",
                    "symbol": "SPY",
                    "field_type": "price",
                    "value": 100.25,
                    "source_vendor": "LOCAL_CACHE",
                    "source_timestamp_utc": f"{DAY}T00:00:00Z",
                    "day_utc": DAY,
                    "raw_source_hash": "a" * 64,
                    "transformed_value_hash": "c" * 64,
                    "validation_status": "VALID",
                    "reason": "ok",
                }
            ],
        },
    )

    registry = data_registry_v1.build_data_registry_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY", "DIA"])
    data_registry_v1.write_data_registry_v1(truth_root=tmp_path, day_utc=DAY, payload=registry)
    readiness = sleeve_readiness_v1.build_sleeve_readiness_v1(truth_root=tmp_path, day_utc=DAY)

    trend = next(row for row in readiness["sleeves"] if row["sleeve_id"] == "TREND")
    assert trend["readiness"] == "READY_WITH_WARNINGS"
    assert trend["blocking_inputs"] == []
    assert registry["market_input_gap_classification"]["market.price.DIA"] == "LEGACY_OPTIONAL"
    assert registry["market_input_gap_classification"]["market.breadth.down_pct"] == "ADVISORY_ONLY"
    assert "market.price.DIA" not in registry["blocking_items"]
    assert "market.breadth.down_pct" in registry["warning_items"]


def test_required_missing_input_still_blocks(tmp_path: Path) -> None:
    from ops.aegis import sleeve_readiness_v1

    _write(
        tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json",
        {
            "schema_id": "aegis_sleeve_input_contracts",
            "contracts": [
                {
                    "sleeve_id": "TREND",
                    "contract_status": "OK",
                    "required_inputs": [{"data_item_id": "market.price.SPY", "required": True}],
                    "optional_inputs": [],
                }
            ],
        },
    )
    registry = data_registry_v1.build_data_registry_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY"])
    data_registry_v1.write_data_registry_v1(truth_root=tmp_path, day_utc=DAY, payload=registry)
    readiness = sleeve_readiness_v1.build_sleeve_readiness_v1(truth_root=tmp_path, day_utc=DAY)

    trend = next(row for row in readiness["sleeves"] if row["sleeve_id"] == "TREND")
    assert trend["readiness"] == "BLOCKED"
    assert trend["blocking_inputs"] == ["market.price.SPY"]


def test_defensive_tail_resolves_canonical_spy_without_local_snapshot(tmp_path: Path) -> None:
    from constellation_2.phaseI.defensive_tail.run import run_defensive_tail_intents_day_v1 as defensive_tail

    canonical = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    input_path = canonical / "reports/market_data_inputs_v1" / DAY / "market_data_inputs.v1.json"
    _write(
        input_path,
        {
            "schema_id": "market_data_inputs",
            "schema_version": "v1",
            "day_utc": DAY,
            "input_records": [
                {
                    "data_item_id": "market.price.SPY",
                    "symbol": "SPY",
                    "field_type": "price",
                    "value": 100.25,
                    "source_timestamp_utc": f"{DAY}T14:50:00Z",
                    "raw_source_hash": "a" * 64,
                    "validation_status": "VALID",
                }
            ],
        },
    )
    _write(sleeve_root / "accounting_v1/nav" / DAY / "nav_snapshot.v1.json", {"history": {"drawdown_pct": "0"}})
    _write(sleeve_root / "positions_snapshot_v2/snapshots" / DAY / "positions_snapshot.v2.json", {"positions": []})
    _write(sleeve_root / "monitoring_v1/engine_correlation_matrix" / DAY / "engine_correlation_matrix.v1.json", {"matrix": {"engine_ids": ["A"], "corr": [[1]]}})
    _write(sleeve_root / "monitoring_v1/regime_snapshot_v2" / DAY / "regime_snapshot.v2.json", {"regime": "NORMAL"})

    defensive_tail._bind_truth_root(str(sleeve_root))
    inputs = defensive_tail._resolve_inputs(DAY, "SPY")

    assert inputs.md_path == input_path.resolve()
    assert not (sleeve_root / "market_data_snapshot_v1/snapshots" / DAY / "SPY.market_data_snapshot.v1.json").exists()


def test_vix_stale_report_blocks_only_vol_sleeve(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import sleeve_readiness_v1

    _write(
        tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json",
        {
            "schema_id": "aegis_sleeve_input_contracts",
            "contracts": [
                {
                    "sleeve_id": "TREND",
                    "contract_status": "OK",
                    "required_inputs": [{"data_item_id": "market.price.SPY", "required": True}],
                    "optional_inputs": [{"data_item_id": "market.volatility.VIX", "required": False}],
                },
                {
                    "sleeve_id": "VOL",
                    "contract_status": "OK",
                    "required_inputs": [{"data_item_id": "market.volatility.VIX", "required": True}],
                    "optional_inputs": [],
                },
            ],
        },
    )
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "fetch_market_data_v1", lambda **_: _provider_result(vix_day="2026-05-18"))

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")
    mdi.write_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    registry = data_registry_v1.build_data_registry_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY", "VIX"])
    data_registry_v1.write_data_registry_v1(truth_root=tmp_path, day_utc=DAY, payload=registry)
    readiness = sleeve_readiness_v1.build_sleeve_readiness_v1(truth_root=tmp_path, day_utc=DAY)
    paths = sleeve_readiness_v1.write_sleeve_readiness_v1(truth_root=tmp_path, day_utc=DAY, payload=readiness)
    report = json.loads(Path(paths["vix_stale_report_json"]).read_text())

    trend = next(row for row in readiness["sleeves"] if row["sleeve_id"] == "TREND")
    vol = next(row for row in readiness["sleeves"] if row["sleeve_id"] == "VOL")
    assert trend["readiness"] == "READY_WITH_WARNINGS"
    assert vol["readiness"] == "BLOCKED"
    assert report["validation_status"] == "STALE"
    assert report["last_valid_date"] == "2026-05-18"
    assert report["required_blocked_sleeves"] == ["VOL"]
    assert report["optional_warning_sleeves"] == ["TREND"]
    assert report["raw_source_hash"] == "b" * 64


def test_missing_vix_report_is_precise(tmp_path: Path) -> None:
    from ops.aegis import sleeve_readiness_v1

    _contracts(tmp_path)
    registry = data_registry_v1.build_data_registry_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY", "VIX"])
    data_registry_v1.write_data_registry_v1(truth_root=tmp_path, day_utc=DAY, payload=registry)
    readiness = sleeve_readiness_v1.build_sleeve_readiness_v1(truth_root=tmp_path, day_utc=DAY)
    paths = sleeve_readiness_v1.write_sleeve_readiness_v1(truth_root=tmp_path, day_utc=DAY, payload=readiness)
    report = json.loads(Path(paths["vix_stale_report_json"]).read_text())

    assert report["validation_status"] == "MISSING"
    assert report["source_timestamp_utc"] == ""
    assert report["required_blocked_sleeves"] == ["VOL"]
    assert "build_aegis_market_data_inputs_v1.py" in report["next_expected_refresh_path"]


def test_intent_simulator_readiness_contract_is_optional_simulation() -> None:
    from ops.tools import run_sleeve_evaluation_kernel_v1 as kernel

    assert kernel._requires_sleeve_readiness_contract("C2_INTENT_SIMULATOR_V1") is False
    assert kernel._requires_sleeve_readiness_contract("C2_VOL_INCOME_DEFINED_RISK_V1") is True


def test_market_data_inputs_missing_vix_is_missing(tmp_path: Path, monkeypatch) -> None:
    _contracts(tmp_path)
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "VIX"]})
    result = _provider_result(vix_day=DAY)
    symbols = dict(result.symbols)
    symbols.pop("VIX")
    monkeypatch.setattr(
        mdi,
        "fetch_market_data_v1",
        lambda **_: ProviderResult(
            **{**result.__dict__, "symbols": symbols, "fetched_symbols": ("SPY",), "missing_symbols": ("VIX",), "provider_failed_symbols": ()}
        ),
    )

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")

    statuses = {row["data_item_id"]: row["validation_status"] for row in payload["input_records"]}
    assert statuses["market.volatility.VIX"] == "MISSING"
    assert payload["missing_input_ids"] == ["market.volatility.VIX"]


def test_market_data_inputs_unavailable_vix_is_external_source_unavailable(tmp_path: Path, monkeypatch) -> None:
    _contracts(tmp_path)
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(
        mdi,
        "fetch_market_data_v1",
        lambda **_: ProviderResult(
            provider="STOOQ",
            request_status="FAILED",
            timestamp_utc=f"{DAY}T14:50:00Z",
            returned_data_date="",
            symbols={},
            breadth={},
            failure_reason="MARKET_DATA_FETCH_FAILED",
            provider_results=(),
            requested_symbols=("SPY", "VIX"),
            fetched_symbols=(),
            missing_symbols=("SPY", "VIX"),
            provider_failed_symbols=("SPY", "VIX"),
            normalized_records=(),
        ),
    )

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")

    statuses = {row["data_item_id"]: row for row in payload["input_records"]}
    assert statuses["market.volatility.VIX"]["validation_status"] == "UNAVAILABLE_EXTERNAL_SOURCE"
    assert statuses["market.volatility.VIX"]["reason"] == "MARKET_DATA_FETCH_FAILED"
    assert "market.volatility.VIX" in payload["missing_input_ids"]


def test_market_data_inputs_future_vix_timestamp_rejects(tmp_path: Path, monkeypatch) -> None:
    _contracts(tmp_path)
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "VIX"]})
    result = _provider_result(vix_day=DAY)
    symbols = dict(result.symbols)
    symbols["VIX"] = {**symbols["VIX"], "data_timestamp_utc": "2026-05-21T21:00:00Z", "market_session_date": "2026-05-21"}
    monkeypatch.setattr(mdi, "fetch_market_data_v1", lambda **_: ProviderResult(**{**result.__dict__, "symbols": symbols}))

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")

    vix = next(row for row in payload["input_records"] if row["data_item_id"] == "market.volatility.VIX")
    assert vix["validation_status"] == "MALFORMED"
    assert vix["reason"] == "SOURCE_TIMESTAMP_FUTURE_DATED"


class _FakeUrlopen:
    def __init__(self, text: str):
        self._text = text

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self._text.encode("utf-8")


def _vix_only_contract(root: Path) -> None:
    _write(
        root / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json",
        {
            "schema_id": "aegis_sleeve_input_contracts",
            "contracts": [
                {
                    "sleeve_id": "VOL",
                    "contract_status": "OK",
                    "required_inputs": [{"data_item_id": "market.volatility.VIX", "required": True}],
                    "optional_inputs": [],
                }
            ],
        },
    )


def test_cboe_vix_source_timestamp_parsing_and_cache_write(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "FINAL_EOD_CERTIFIED")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "LOCAL_CACHE")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "DISABLED")
    monkeypatch.setattr(
        mdp.urllib.request,
        "urlopen",
        lambda *args, **kwargs: _FakeUrlopen("DATE,OPEN,HIGH,LOW,CLOSE\n05/20/2026,18.1,19.2,17.9,18.7\n"),
    )

    result = mdp.fetch_market_data_v1(truth_root=tmp_path, day_utc=DAY, symbols=["VIX"], symbol_map={})

    assert result.symbols["VIX"]["market_session_date"] == DAY
    assert result.symbols["VIX"]["data_timestamp_utc"] == f"{DAY}T21:00:00Z"
    assert result.symbols["VIX"]["provider"] == "CBOE"
    cache_path = tmp_path / "market_data_snapshot_v1/VIX/2026.jsonl"
    cache_row = json.loads(cache_path.read_text().splitlines()[-1])
    assert cache_row["symbol"] == "VIX"
    assert cache_row["timestamp_utc"] == f"{DAY}T21:00:00Z"
    assert cache_row["source_hash"]
    assert cache_row["transformed_hash"]


def test_unavailable_vix_reports_unavailable_external_source(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "FINAL_EOD_CERTIFIED")
    _vix_only_contract(tmp_path)
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "LOCAL_CACHE")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "DISABLED")

    def _raise(*args, **kwargs):
        raise OSError("network unavailable")

    monkeypatch.setattr(mdp.urllib.request, "urlopen", _raise)
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["VIX"]})

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")
    vix = next(row for row in payload["input_records"] if row["data_item_id"] == "market.volatility.VIX")

    assert vix["validation_status"] == "UNAVAILABLE_EXTERNAL_SOURCE"
    assert vix["reason"] == "FINAL_EOD_ARTIFACT_MISSING_OR_INCOMPLETE"
    assert payload["missing_input_ids"] == ["market.volatility.VIX"]


def test_cboe_vix_failure_does_not_leak_to_price_inputs(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "FINAL_EOD_CERTIFIED")
    _write(
        tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json",
        {
            "schema_id": "aegis_sleeve_input_contracts",
            "contracts": [
                {
                    "sleeve_id": "MIXED",
                    "contract_status": "OK",
                    "required_inputs": [
                        {"data_item_id": "market.price.SPY", "required": True},
                        {"data_item_id": "market.volatility.VIX", "required": True},
                    ],
                    "optional_inputs": [],
                }
            ],
        },
    )
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "LOCAL_CACHE")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "DISABLED")
    monkeypatch.setattr(
        mdp.urllib.request,
        "urlopen",
        lambda *args, **kwargs: _FakeUrlopen("DATE,OPEN,HIGH,LOW,CLOSE\n05/19/2026,18.1,19.2,17.9,18.7\n"),
    )
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "VIX"]})

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")
    by_id = {row["data_item_id"]: row for row in payload["input_records"]}

    assert by_id["market.volatility.VIX"]["validation_status"] == "UNAVAILABLE_EXTERNAL_SOURCE"
    assert by_id["market.volatility.VIX"]["reason"] == "FINAL_EOD_ARTIFACT_MISSING_OR_INCOMPLETE"
    assert by_id["market.price.SPY"]["validation_status"] in {"MISSING", "UNAVAILABLE_EXTERNAL_SOURCE"}
    assert by_id["market.price.SPY"]["reason"] != "CBOE_VIX_ROW_NOT_FOUND"


def test_wrong_day_cboe_vix_blocks(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "FINAL_EOD_CERTIFIED")
    _vix_only_contract(tmp_path)
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "LOCAL_CACHE")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "DISABLED")
    monkeypatch.setattr(
        mdp.urllib.request,
        "urlopen",
        lambda *args, **kwargs: _FakeUrlopen("DATE,OPEN,HIGH,LOW,CLOSE\n05/19/2026,18.1,19.2,17.9,18.7\n"),
    )
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["VIX"]})

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")
    vix = next(row for row in payload["input_records"] if row["data_item_id"] == "market.volatility.VIX")

    assert vix["validation_status"] == "UNAVAILABLE_EXTERNAL_SOURCE"
    assert vix["reason"] == "FINAL_EOD_ARTIFACT_MISSING_OR_INCOMPLETE"



def test_intraday_operational_mode_accepts_current_provisional_rows(tmp_path: Path, monkeypatch) -> None:
    _contracts(tmp_path)
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "INTRADAY_OPERATIONAL")
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "fetch_market_data_v1", lambda **_: ProviderResult(
        provider="STOOQ",
        request_status="SUCCESS",
        timestamp_utc=f"{DAY}T14:50:00Z",
        returned_data_date=DAY,
        symbols={
            "SPY": {"symbol": "SPY", "canonical_symbol": "SPY", "provider": "STOOQ", "last_price": 100.25, "data_timestamp_utc": f"{DAY}T14:50:00Z", "market_session_date": DAY, "source": "STOOQ", "source_hash": "a" * 64, "freshness_status": "CURRENT", "data_finality": "PROVISIONAL_INTRADAY", "market_data_mode": "PROVISIONAL_INTRADAY", "finalization_status": "NOT_FINAL_YET", "usable_for": {"sleeve_intraday_generation": True, "manual_capture": True, "final_eod_certification": False}},
            "VIX": {"symbol": "VIX", "canonical_symbol": "VIX", "provider": "CBOE", "last_price": 18.5, "data_timestamp_utc": f"{DAY}T14:50:00Z", "market_session_date": DAY, "source": "CBOE", "source_hash": "b" * 64, "freshness_status": "CURRENT", "data_finality": "PROVISIONAL_INTRADAY", "market_data_mode": "PROVISIONAL_INTRADAY", "finalization_status": "NOT_FINAL_YET", "usable_for": {"sleeve_intraday_generation": True, "manual_capture": True, "final_eod_certification": False}},
        },
        breadth={},
        requested_symbols=("SPY", "VIX"),
        fetched_symbols=("SPY", "VIX"),
        normalized_records=(),
    ))

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")

    assert payload["market_data_mode"] == "INTRADAY_OPERATIONAL"
    assert payload["status"] == "READY"
    assert payload["validation_status"] == "VALID"
    assert payload["final_eod_certification_status"] == "PENDING"
    assert all(row["validation_status"] == "VALID" for row in payload["input_records"])


def test_final_eod_certification_mode_rejects_provisional_rows(tmp_path: Path, monkeypatch) -> None:
    _contracts(tmp_path)
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "FINAL_EOD_CERTIFIED")
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY"]})
    monkeypatch.setattr(mdi, "fetch_market_data_v1", lambda **_: ProviderResult(
        provider="STOOQ",
        request_status="SUCCESS",
        timestamp_utc=f"{DAY}T14:50:00Z",
        returned_data_date=DAY,
        symbols={
            "SPY": {"symbol": "SPY", "canonical_symbol": "SPY", "provider": "STOOQ", "last_price": 100.25, "data_timestamp_utc": f"{DAY}T14:50:00Z", "market_session_date": DAY, "source": "STOOQ", "source_hash": "a" * 64, "freshness_status": "CURRENT", "data_finality": "PROVISIONAL_INTRADAY", "market_data_mode": "PROVISIONAL_INTRADAY", "finalization_status": "NOT_FINAL_YET"},
        },
        breadth={},
        requested_symbols=("SPY",),
        fetched_symbols=("SPY",),
        normalized_records=(),
    ))

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T15:00:00Z")

    assert payload["market_data_mode"] == "FINAL_EOD_CERTIFIED"
    assert payload["status"] == "BLOCKED"
    row = next(row for row in payload["input_records"] if row["data_item_id"] == "market.price.SPY")
    assert row["validation_status"] == "UNAVAILABLE_EXTERNAL_SOURCE"
    assert row["reason"] == "FINAL_EOD_ARTIFACT_MISSING_OR_INCOMPLETE"







def _write_eod_csv(path: Path, symbols: list[str], *, day: str = DAY, invalid: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["symbol,date,open,high,low,close,volume,provider"]
    for symbol in symbols:
        high = "90" if invalid and symbol == symbols[0] else "101"
        lines.append(f"{symbol},{day},99,{high},98,100,1000,manual_fixture")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _patch_eod_symbols(monkeypatch, symbols: list[str]) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    monkeypatch.setattr(dsb, "build_symbol_map_v1", lambda **_: {"required_symbols": symbols})



def _canonical_eod_artifact_path(root: Path, day: str = DAY) -> Path:
    return root / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json"


def _write_canonical_eod_artifact(root: Path, symbols: list[str], *, day: str = DAY, stale: list[str] | None = None) -> Path:
    stale_set = set(stale or [])
    rows = {}
    for symbol in symbols:
        row = _final_symbol_row(symbol, provider="TIINGO")
        if symbol in stale_set:
            row = {**row, "freshness_status": "STALE", "market_session_date": "2026-05-19", "data_timestamp_utc": "2026-05-19T21:00:00Z"}
        rows[symbol] = row
    final_symbols = [symbol for symbol in symbols if symbol not in stale_set]
    payload = {
        "schema_id": "final_eod_market_data_v1",
        "schema_version": "v1",
        "day_utc": day,
        "trading_day": day,
        "market_session_date": day,
        "status": "CURRENT" if not stale_set else "STALE_PROVIDER_ROWS",
        "validation_status": "VALID" if not stale_set else "REJECTED",
        "final_eod_certification_status": "VALID" if not stale_set else "REJECTED",
        "requested_symbols": symbols,
        "fetched_symbols": symbols,
        "final_eod_symbols": final_symbols,
        "missing_symbols": [],
        "stale_symbols": sorted(stale_set),
        "symbols": rows,
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    path = _canonical_eod_artifact_path(root, day)
    _write(path, payload)
    return path


def _patch_provider_builder(monkeypatch, dsb, symbols_returned: list[str], calls: list[tuple[str, ...]], provider: str = "TIINGO") -> None:
    monkeypatch.delenv("AEGIS_US_EQUITIES_EOD_SOURCE_FILE", raising=False)
    monkeypatch.delenv("AEGIS_FINAL_EOD_MARKET_DATA_SOURCE_FILE", raising=False)
    monkeypatch.setattr(dsb, "provider_config_from_env_v1", lambda: _provider_config("TIINGO", "ALPHA_VANTAGE"))
    monkeypatch.setattr(dsb, "_rerun_domain_certification_v1", lambda **_: {"ok": True, "certification_status": "CERTIFIED", "paths": {}})

    def _fetch(**kwargs):
        requested = tuple(kwargs["symbols"])
        calls.append(requested)
        rows = {symbol: _final_symbol_row(symbol, provider=provider) for symbol in symbols_returned if symbol in requested}
        return ProviderResult(
            provider=provider,
            request_status="SUCCESS" if rows else "FAILED",
            timestamp_utc=f"{DAY}T21:00:00Z",
            returned_data_date=DAY,
            symbols=rows,
            breadth={},
            requested_symbols=requested,
            fetched_symbols=tuple(sorted(rows)),
            missing_symbols=tuple(sorted(set(requested) - set(rows))),
            normalized_records=(),
            provider_coverage_plan={"status": "FULL_PROVIDER_PLAN", "unsupported_symbols": []},
        )

    monkeypatch.setattr(dsb, "fetch_market_data_v1", _fetch)


def test_valid_same_day_eod_artifact_skips_provider_calls(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY", "QQQ"])
    path = _write_canonical_eod_artifact(tmp_path / "truth", ["SPY", "QQQ"])
    artifact_before = path.read_text(encoding="utf-8")
    calls: list[tuple[str, ...]] = []
    _patch_provider_builder(monkeypatch, dsb, ["SPY", "QQQ"], calls)

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    artifact_after = Path(result["artifact_path"]).read_text(encoding="utf-8")
    artifact = json.loads(artifact_after)

    assert calls == []
    assert artifact_after == artifact_before
    assert result["result_status"] == "CERTIFIED_FROM_ARTIFACT"
    assert result["provider_fetch_skipped"] is True
    assert result["provider_fetch_status"] == "SKIPPED_VALID_ARTIFACT_EXISTS"


def test_missing_eod_artifact_triggers_provider_build(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY"])
    calls: list[tuple[str, ...]] = []
    _patch_provider_builder(monkeypatch, dsb, ["SPY"], calls)

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert calls == [("SPY",)]
    assert result["provider_fetch_status"] == "FETCHED_MISSING_OR_STALE_SYMBOLS"
    assert result["certification_status"] == "CERTIFIED"
    assert "/artifacts/" in result["artifact_path"]
    manifest = _canonical_eod_artifact_path(tmp_path / "truth")
    assert json.loads(manifest.read_text(encoding="utf-8"))["schema_id"] == "final_eod_market_data_current_manifest.v1"


def test_manifest_backed_eod_artifact_rerun_does_not_rewrite_content(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY"])
    calls: list[tuple[str, ...]] = []
    _patch_provider_builder(monkeypatch, dsb, ["SPY"], calls)

    first = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    artifact_path = Path(first["artifact_path"])
    artifact_before = artifact_path.read_text(encoding="utf-8")
    manifest_before = _canonical_eod_artifact_path(tmp_path / "truth").read_text(encoding="utf-8")
    second = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert calls == [("SPY",)]
    assert second["provider_fetch_status"] == "SKIPPED_VALID_ARTIFACT_EXISTS"
    assert second["artifact_path"] == str(artifact_path)
    assert artifact_path.read_text(encoding="utf-8") == artifact_before
    assert _canonical_eod_artifact_path(tmp_path / "truth").read_text(encoding="utf-8") == manifest_before


def test_partial_eod_artifact_fetches_only_missing_symbols(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY", "QQQ"])
    _write_canonical_eod_artifact(tmp_path / "truth", ["SPY"])
    calls: list[tuple[str, ...]] = []
    _patch_provider_builder(monkeypatch, dsb, ["QQQ"], calls)

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    artifact = json.loads(Path(result["artifact_path"]).read_text(encoding="utf-8"))

    assert calls == [("QQQ",)]
    assert sorted(artifact["final_eod_symbols"]) == ["QQQ", "SPY"]
    assert result["certification_status"] == "CERTIFIED"


def test_final_eod_certified_market_inputs_read_canonical_artifact(tmp_path: Path, monkeypatch) -> None:
    _contracts(tmp_path)
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "FINAL_EOD_CERTIFIED")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "TIINGO")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "ALPHA_VANTAGE")
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY"]})
    _write_canonical_eod_artifact(tmp_path, ["SPY", "VIX"])

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T22:00:00Z")

    assert payload["status"] == "READY"
    assert payload["provider_request_status"] == "SUCCESS"
    assert payload["source_vendor"] == "FINAL_EOD_ARTIFACT"
    assert payload["downstream_certification_invariant"]["status"] == "PASS"
    assert payload["missing_input_ids"] == []
    assert payload["input_records"][0]["source_vendor"] == "TIINGO"


def test_stale_eod_artifact_fetches_only_stale_symbols(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY", "QQQ"])
    _write_canonical_eod_artifact(tmp_path / "truth", ["SPY", "QQQ"], stale=["SPY"])
    calls: list[tuple[str, ...]] = []
    _patch_provider_builder(monkeypatch, dsb, ["SPY"], calls)

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert calls == [("SPY",)]
    assert result["certification_status"] == "CERTIFIED"


def test_force_provider_refresh_refetches_valid_artifact_symbols(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY", "QQQ"])
    _write_canonical_eod_artifact(tmp_path / "truth", ["SPY", "QQQ"])
    calls: list[tuple[str, ...]] = []
    _patch_provider_builder(monkeypatch, dsb, ["SPY", "QQQ"], calls)

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY, force_provider_refresh=True)

    assert calls == [("QQQ", "SPY")]
    assert result["provider_fetch_status"] == "FORCED_PROVIDER_REFRESH"


def test_vix_can_be_certified_from_governed_cboe_provider_path(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["VIX"])
    calls: list[tuple[str, ...]] = []
    _patch_provider_builder(monkeypatch, dsb, ["VIX"], calls, provider="CBOE")

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    artifact = json.loads(Path(result["artifact_path"]).read_text(encoding="utf-8"))

    assert calls == [("VIX",)]
    assert artifact["symbols"]["VIX"]["provider"] == "CBOE"
    assert result["certification_status"] == "CERTIFIED"


def test_partial_downstream_registry_after_certified_eod_has_precise_blocker(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "FINAL_EOD_CERTIFIED")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "TIINGO")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "ALPHA_VANTAGE")
    _write(
        tmp_path / "reports/aegis_sleeve_input_contracts_v1" / DAY / "sleeve_input_contracts.v1.json",
        {
            "schema_id": "aegis_sleeve_input_contracts",
            "contracts": [
                {
                    "sleeve_id": "TREND",
                    "contract_status": "OK",
                    "required_inputs": [
                        {"data_item_id": "market.price.SPY", "required": True},
                        {"data_item_id": "market.price.QQQ", "required": True},
                    ],
                    "optional_inputs": [],
                }
            ],
        },
    )
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "QQQ"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "QQQ"]})
    _write_canonical_eod_artifact(tmp_path, ["SPY"])

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T22:00:00Z")

    assert payload["status"] == "PARTIAL"
    assert payload["downstream_certification_invariant"]["status"] == "DOWNSTREAM_ONLY_BLOCKER"
    assert payload["downstream_certification_invariant"]["blocked_input_ids"] == ["market.price.QQQ"]
    assert "US_EQUITIES_EOD is certified" in payload["operator_downstream_blocker_message"]
    assert "market.price.QQQ" in payload["operator_downstream_blocker_message"]


def test_stale_valid_looking_final_eod_artifact_is_rejected_by_market_inputs(tmp_path: Path, monkeypatch) -> None:
    _contracts(tmp_path)
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "FINAL_EOD_CERTIFIED")
    monkeypatch.setattr(mdi, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "VIX"]})
    monkeypatch.setattr(mdi, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "VIX"]})
    stale_path = _write_canonical_eod_artifact(tmp_path, ["SPY", "VIX"])
    artifact = json.loads(stale_path.read_text(encoding="utf-8"))
    artifact["symbols"]["SPY"]["freshness_status"] = "STALE"
    artifact["symbols"]["SPY"]["market_session_date"] = "2026-05-19"
    artifact["status"] = "CURRENT"
    artifact["validation_status"] = "VALID"
    artifact["final_eod_certification_status"] = "VALID"
    _write(stale_path, artifact)

    payload = mdi.build_market_data_inputs_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=f"{DAY}T22:00:00Z")

    assert payload["status"] in {"BLOCKED", "PARTIAL"}
    assert payload["final_eod_artifact_lineage"]["artifact_valid_for_market_inputs"] is False
    assert payload["provider_request_status"] == "FAILED"


def test_provider_status_uses_configured_planner_chain_without_tiingo_hardcode(tmp_path: Path, monkeypatch) -> None:
    from ops.tools import manage_us_equities_eod_source_v1 as tool

    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "ALPHA_VANTAGE")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "DISABLED")
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "unit-test-alpha-token")
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    monkeypatch.setattr(tool, "build_symbol_map_v1", lambda **_: {
        "required_symbols": ["SPY"],
        "symbols": {"SPY": {"providers": {"ALPHA_VANTAGE": "SPY"}}},
    })

    status = tool.provider_status_v1(day_utc=DAY, truth_root=tmp_path / "truth")

    assert status["provider_priority"] == ["ALPHA_VANTAGE"]
    assert status["missing_credentials"] == []
    assert status["credential_status"]["TIINGO_API_KEY"] == "MISSING"
    assert status["result_status"] in {"READY", "SOURCE_SETUP_REQUIRED"}
    if status["result_status"] == "SOURCE_SETUP_REQUIRED":
        assert status["coverage_plan"]["status"] != "FULL_PROVIDER_PLAN" or status["capability_report"]["status"] != "VALID"

def test_complete_uploaded_eod_source_certifies(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY", "QQQ"])
    source = tmp_path / "uploaded_eod.csv"
    _write_eod_csv(source, ["SPY", "QQQ"])
    monkeypatch.setenv("AEGIS_US_EQUITIES_EOD_SOURCE_FILE", str(source))

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    artifact = json.loads(Path(result["artifact_path"]).read_text(encoding="utf-8"))

    assert result["ok"] is True
    assert result["certification_status"] == "CERTIFIED"
    assert artifact["final_eod_certification_status"] == "VALID"
    assert artifact["certification_eligible"] is True
    assert artifact["final_eod_symbols"] == ["QQQ", "SPY"]


def test_uploaded_eod_source_missing_symbols_fails_with_exact_list(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY", "QQQ", "IWM"])
    source = tmp_path / "uploaded_eod.csv"
    _write_eod_csv(source, ["SPY", "QQQ"])
    monkeypatch.setenv("AEGIS_US_EQUITIES_EOD_SOURCE_FILE", str(source))

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert result["ok"] is False
    assert result["status"] == "PROVIDER_INCOMPLETE"
    assert result["missing_symbols"] == ["IWM"]
    assert "MISSING_SYMBOL_COVERAGE" in result["failure_reason"]


def test_uploaded_eod_source_stale_dates_fail(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY"])
    source = tmp_path / "uploaded_eod.csv"
    _write_eod_csv(source, ["SPY"], day="2026-05-19")
    monkeypatch.setenv("AEGIS_US_EQUITIES_EOD_SOURCE_FILE", str(source))

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert result["ok"] is False
    assert "STALE_PROVIDER_ROWS" in result["failure_reason"]
    assert result["stale_date_symbols"] == ["SPY"]


def test_uploaded_eod_source_invalid_ohlcv_fails(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY"])
    source = tmp_path / "uploaded_eod.csv"
    _write_eod_csv(source, ["SPY"], invalid=True)
    monkeypatch.setenv("AEGIS_US_EQUITIES_EOD_SOURCE_FILE", str(source))

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert result["ok"] is False
    assert result["status"] == "INVALID_OHLCV"
    assert result["invalid_ohlcv_rows"][0]["symbol"] == "SPY"


def test_uploaded_eod_source_records_lineage(tmp_path: Path, monkeypatch) -> None:
    from ops.aegis import domain_source_builders_v1 as dsb

    _patch_eod_symbols(monkeypatch, ["SPY"])
    source = tmp_path / "uploaded_eod.json"
    source.write_text(json.dumps({"rows": [{"symbol": "SPY", "date": DAY, "open": 99, "high": 101, "low": 98, "close": 100, "volume": 1000, "provider": "operator_upload"}]}), encoding="utf-8")
    monkeypatch.setenv("AEGIS_US_EQUITIES_EOD_SOURCE_FILE", str(source))

    result = dsb.build_us_equities_eod_source_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    artifact = json.loads(Path(result["artifact_path"]).read_text(encoding="utf-8"))

    assert artifact["lineage"]["source_path"] == str(source)
    assert artifact["lineage"]["source_sha256"]
    assert artifact["lineage"]["certification_eligibility"] is True
    assert artifact["symbols"]["SPY"]["provider"] == "operator_upload"




def test_eod_source_template_includes_all_required_symbols(tmp_path: Path, monkeypatch) -> None:
    from ops.tools import manage_us_equities_eod_source_v1 as tool

    monkeypatch.setattr(tool, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY", "QQQ", "IWM"]})

    universe = tool.write_required_universe_v1(truth_root=tmp_path / "truth", day_utc=DAY)
    template = tool.template_csv_v1(day_utc=DAY, symbols=universe["symbols"])

    assert universe["symbols"] == ["SPY", "QQQ", "IWM"]
    assert Path(universe["path"]).read_text(encoding="utf-8").splitlines() == ["SPY", "QQQ", "IWM"]
    assert "symbol,date,open,high,low,close,volume" in template
    assert "SPY,2026-05-20" in template
    assert "QQQ,2026-05-20" in template
    assert "IWM,2026-05-20" in template


def test_uploaded_eod_source_duplicate_symbol_fails(tmp_path: Path, monkeypatch) -> None:
    from ops.tools import manage_us_equities_eod_source_v1 as tool

    monkeypatch.setattr(tool, "build_symbol_map_v1", lambda **_: {"required_symbols": ["SPY"]})
    source = tmp_path / "dupe_eod.csv"
    source.write_text(
        "symbol,date,open,high,low,close,volume\n"
        f"SPY,{DAY},99,101,98,100,1000\n"
        f"SPY,{DAY},100,102,99,101,2000\n",
        encoding="utf-8",
    )

    result = tool.validate_source_file_v1(source_file=source, day_utc=DAY, truth_root=tmp_path / "truth")

    assert result["validation_status"] == "INVALID"
    assert result["duplicate_symbols"] == ["SPY"]
    assert "DUPLICATE_SYMBOLS" in result["errors"]


def _provider_config(primary: str, fallback: str = "", mode: str = "FINAL_EOD_CERTIFIED") -> mdp.ProviderConfig:
    return mdp.ProviderConfig(
        primary=primary,
        fallback=fallback,
        allow_delayed=False,
        require_current_session=True,
        require_breadth=False,
        timeout_seconds=4,
        cache_ttl_seconds=900,
        per_symbol_timeout_seconds=4,
        total_timeout_seconds=60,
        stooq_retries=1,
        stooq_backoff_seconds=0,
        stooq_chunk_size=1,
        market_data_mode=mode,
        intraday_provider="",
    )


def _final_symbol_row(symbol: str, provider: str = "LOCAL_CACHE") -> dict:
    return {
        "symbol": symbol,
        "canonical_symbol": symbol,
        "provider": provider,
        "provider_symbol": symbol,
        "last_price": 100.0,
        "close": 100.0,
        "open": 99.0,
        "high": 101.0,
        "low": 98.0,
        "volume": 1000,
        "market_session_date": DAY,
        "data_timestamp_utc": f"{DAY}T21:00:00Z",
        "source_timestamp_utc": f"{DAY}T21:00:00Z",
        "freshness_status": "CURRENT",
        "data_finality": "FINAL_EOD",
        "market_data_mode": "FINAL_EOD_CERTIFIED",
        "source_hash": "a" * 64,
    }


def test_provider_coverage_plan_excludes_cboe_from_equities_eod_universe() -> None:
    config = _provider_config("CBOE")
    symbol_map = {
        "symbols": {
            "SPY": {"providers": {"CBOE": "SPY"}},
            "VIX": {"providers": {"CBOE": "VIX"}},
        }
    }

    plan = mdp.provider_coverage_plan_v1(config=config, symbols=["SPY", "VIX"], symbol_map=symbol_map)

    assert mdp.provider_capabilities_v1("CBOE")["classification"] == "VOLATILITY_VIX_ONLY"
    assert plan["status"] == "PROVIDER_COVERAGE_INCOMPLETE"
    assert plan["unsupported_symbols"] == ["SPY"]
    assert plan["assignment"]["VIX"]["fallback_candidates"] == ["CBOE"]


def test_provider_coverage_incomplete_fails_before_fetch(tmp_path: Path, monkeypatch) -> None:
    config = _provider_config("CBOE")
    symbol_map = {"symbols": {"SPY": {"providers": {"CBOE": "SPY"}}}}

    def _forbidden_fetch(**kwargs):
        raise AssertionError("fetch should not run without a full provider coverage plan")

    monkeypatch.setattr(mdp, "_fetch_provider", _forbidden_fetch)

    result = mdp.fetch_market_data_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY"], symbol_map=symbol_map, config_override=config)

    assert result.request_status == "FAILED"
    assert result.failure_reason == "PROVIDER_COVERAGE_INCOMPLETE"
    assert result.provider_coverage_plan["unsupported_symbols"] == ["SPY"]


def test_stale_local_cache_fetches_only_unresolved_symbols_from_fallback(tmp_path: Path, monkeypatch) -> None:
    config = _provider_config("LOCAL_CACHE", "STOOQ")
    symbol_map = {
        "symbols": {
            "SPY": {"providers": {"LOCAL_CACHE": "SPY", "STOOQ": "SPY.US"}},
            "QQQ": {"providers": {"LOCAL_CACHE": "QQQ", "STOOQ": "QQQ.US"}},
        }
    }
    calls: list[tuple[str, tuple[str, ...]]] = []

    def _fake_fetch_provider(**kwargs):
        provider = kwargs["provider"]
        symbols = tuple(kwargs["symbols"])
        calls.append((provider, symbols))
        if provider == "LOCAL_CACHE":
            spy = {**_final_symbol_row("SPY"), "freshness_status": "STALE", "market_session_date": "2026-05-19"}
            qqq = _final_symbol_row("QQQ")
            return ProviderResult(provider=provider, request_status="STALE", timestamp_utc=f"{DAY}T21:00:00Z", returned_data_date=DAY, symbols={"SPY": spy, "QQQ": qqq}, breadth={}, requested_symbols=symbols, fetched_symbols=("QQQ", "SPY"), stale_symbols=("SPY",), normalized_records=())
        assert provider == "STOOQ"
        assert symbols == ("SPY",)
        spy = _final_symbol_row("SPY", provider="STOOQ")
        return ProviderResult(provider=provider, request_status="SUCCESS", timestamp_utc=f"{DAY}T21:00:00Z", returned_data_date=DAY, symbols={"SPY": spy}, breadth={}, requested_symbols=symbols, fetched_symbols=("SPY",), normalized_records=())

    monkeypatch.setattr(mdp, "_fetch_provider", _fake_fetch_provider)

    result = mdp.fetch_market_data_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY", "QQQ"], symbol_map=symbol_map, config_override=config)

    assert result.request_status == "SUCCESS"
    assert calls == [("LOCAL_CACHE", ("QQQ", "SPY")), ("STOOQ", ("SPY",))]
    assert result.symbols["SPY"]["provider"] == "STOOQ"
    assert result.symbols["QQQ"]["provider"] == "LOCAL_CACHE"


def test_full_provider_coverage_certifies_final_eod_report(tmp_path: Path, monkeypatch) -> None:
    from ops.tools import refresh_aegis_market_data_v1 as refresh

    monkeypatch.setattr(refresh, "build_symbol_map_v1", lambda **_: {
        "required_symbols": ["SPY", "QQQ"],
        "symbols": {
            "SPY": {"providers": {"LOCAL_CACHE": "SPY"}},
            "QQQ": {"providers": {"LOCAL_CACHE": "QQQ"}},
        },
    })
    monkeypatch.setattr(refresh, "write_symbol_map_v1", lambda truth_root, day_utc, payload: {"json": str(tmp_path / "symbol_map.json")})
    _write(tmp_path / "symbol_map.json", {"required_symbols": ["SPY", "QQQ"]})
    config = _provider_config("LOCAL_CACHE")
    monkeypatch.setattr(refresh, "provider_config_from_env_v1", lambda: config)

    def _fetch(**kwargs):
        return ProviderResult(
            provider="LOCAL_CACHE",
            request_status="SUCCESS",
            timestamp_utc=f"{DAY}T21:00:00Z",
            returned_data_date=DAY,
            symbols={"SPY": _final_symbol_row("SPY"), "QQQ": _final_symbol_row("QQQ")},
            breadth={},
            requested_symbols=("QQQ", "SPY"),
            fetched_symbols=("QQQ", "SPY"),
            normalized_records=(),
            provider_coverage_plan={"status": "FULL_PROVIDER_PLAN", "unsupported_symbols": []},
        )

    monkeypatch.setattr(refresh, "fetch_market_data_v1", _fetch)

    payload = refresh.build_market_data_report_v1(truth_root=tmp_path, day_utc=DAY, market_data_mode="FINAL_EOD_CERTIFIED")

    assert payload["final_eod_ready"] is True
    assert payload["final_eod_certification_status"] == "VALID"
    assert payload["provider_coverage_action_item"]["status"] == "OK"


def _request_url_v1(request) -> str:
    return str(getattr(request, "full_url", request))


def _provider_symbol_from_url_v1(url: str) -> str:
    if "api.tiingo.com" in url:
        return url.split("/tiingo/daily/", 1)[1].split("/", 1)[0].upper()
    parsed = mdp.urllib.parse.urlparse(url)
    return str(mdp.urllib.parse.parse_qs(parsed.query).get("symbol", [""])[0]).upper()


def _tiingo_payload(symbol: str, day: str = DAY) -> str:
    return json.dumps([{"date": f"{day}T00:00:00.000Z", "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0, "volume": 1000}])


def _alpha_vantage_payload(day: str = DAY) -> str:
    return json.dumps({"Time Series (Daily)": {day: {"1. open": "100", "2. high": "102", "3. low": "99", "4. close": "101", "6. volume": "1000"}}})


def test_missing_tiingo_api_key_shows_setup_required_without_secret(tmp_path: Path, monkeypatch) -> None:
    from ops.tools import manage_us_equities_eod_source_v1 as tool

    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "TIINGO")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "ALPHA_VANTAGE")
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    result = mdp.fetch_market_data_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY"], symbol_map={}, config_override=_provider_config("TIINGO"))
    status = tool.provider_status_v1(day_utc=DAY, truth_root=tmp_path / "truth")

    assert result.request_status == "FAILED"
    assert result.failure_reason == "TIINGO_API_KEY_MISSING"
    assert result.provider_attempts[0]["status"] == "SOURCE_SETUP_REQUIRED"
    assert status["result_status"] == "SOURCE_SETUP_REQUIRED"
    assert "TIINGO_API_KEY" in status["missing_credentials"]
    tiingo = next(row for row in status["providers"] if row["provider"] == "TIINGO")
    assert tiingo["required_credentials"] == [{"env_var": "TIINGO_API_KEY", "status": "MISSING"}]


def test_full_tiingo_coverage_returns_certification_grade_final_rows(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TIINGO_API_KEY", "unit-test-tiingo-token")

    def _urlopen(request, timeout=0):
        symbol = _provider_symbol_from_url_v1(_request_url_v1(request))
        assert symbol in {"SPY", "QQQ"}
        return _FakeUrlopen(_tiingo_payload(symbol))

    monkeypatch.setattr(mdp.urllib.request, "urlopen", _urlopen)

    result = mdp.fetch_market_data_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY", "QQQ"], symbol_map={}, config_override=_provider_config("TIINGO"))
    dumped = json.dumps(result.__dict__, sort_keys=True, default=str)

    assert result.request_status == "SUCCESS"
    assert result.fetched_symbols == ("QQQ", "SPY")
    assert {row["provider"] for row in result.symbols.values()} == {"TIINGO"}
    assert "unit-test-tiingo-token" not in dumped


def test_partial_tiingo_coverage_fails_closed_for_final_eod_report(tmp_path: Path, monkeypatch) -> None:
    from ops.tools import refresh_aegis_market_data_v1 as refresh

    monkeypatch.setenv("TIINGO_API_KEY", "unit-test-tiingo-token")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "TIINGO")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "DISABLED")
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "FINAL_EOD_CERTIFIED")

    def _urlopen(request, timeout=0):
        symbol = _provider_symbol_from_url_v1(_request_url_v1(request))
        return _FakeUrlopen("[]" if symbol == "QQQ" else _tiingo_payload(symbol))

    monkeypatch.setattr(mdp.urllib.request, "urlopen", _urlopen)
    symbol_map = {"required_symbols": ["SPY", "QQQ"], "symbols": {"SPY": {"providers": {"TIINGO": "SPY"}}, "QQQ": {"providers": {"TIINGO": "QQQ"}}}}

    payload = refresh.build_market_data_report_v1(truth_root=tmp_path, day_utc=DAY, symbol_map_override=symbol_map, market_data_mode="FINAL_EOD_CERTIFIED")

    assert payload["final_eod_ready"] is False
    assert payload["final_eod_certification_status"] == "UNAVAILABLE"
    assert payload["final_eod_missing_symbols"] == ["QQQ"]


def test_alpha_vantage_fallback_used_only_when_full_validated_coverage_exists(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TIINGO_API_KEY", "unit-test-tiingo-token")
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "unit-test-alpha-vantage-token")

    def _urlopen(request, timeout=0):
        url = _request_url_v1(request)
        if "api.tiingo.com" in url:
            return _FakeUrlopen("[]")
        return _FakeUrlopen(_alpha_vantage_payload())

    monkeypatch.setattr(mdp.urllib.request, "urlopen", _urlopen)

    result = mdp.fetch_market_data_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY", "QQQ"], symbol_map={}, config_override=_provider_config("TIINGO", "ALPHA_VANTAGE"))

    assert result.request_status == "SUCCESS"
    assert result.fetched_symbols == ("QQQ", "SPY")
    assert {row["provider"] for row in result.symbols.values()} == {"ALPHA_VANTAGE"}


def test_tiingo_stale_rows_are_rejected_for_final_eod_certification(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TIINGO_API_KEY", "unit-test-tiingo-token")
    monkeypatch.setattr(mdp.urllib.request, "urlopen", lambda *args, **kwargs: _FakeUrlopen(_tiingo_payload("SPY", day="2026-05-19")))

    result = mdp.fetch_market_data_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY"], symbol_map={}, config_override=_provider_config("TIINGO"))

    assert result.request_status == "FAILED"
    assert result.fetched_symbols == ()
    assert result.missing_symbols == ("SPY",)
    assert result.symbols == {}


def test_provider_credentials_are_redacted_from_attempt_diagnostics(tmp_path: Path, monkeypatch) -> None:
    secret = "unit-test-alpha-redaction-token"
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", secret)

    def _raise(request, timeout=0):
        raise mdp.urllib.error.URLError(f"failed request {_request_url_v1(request)}")

    monkeypatch.setattr(mdp.urllib.request, "urlopen", _raise)

    result = mdp.fetch_market_data_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY"], symbol_map={}, config_override=_provider_config("ALPHA_VANTAGE"))
    dumped = json.dumps(result.__dict__, sort_keys=True, default=str)

    assert result.request_status == "FAILED"
    assert secret not in dumped
    assert "apikey=[REDACTED]" in dumped


def test_provider_capabilities_classify_stooq_as_daily_eod_and_yahoo_as_intraday() -> None:
    assert mdp.provider_capabilities_v1("STOOQ")["classification"] == "DAILY_EOD_PROVIDER"
    assert mdp.provider_capabilities_v1("STOOQ")["supports_intraday_snapshot"] is False
    assert mdp.provider_capabilities_v1("YAHOO_CHART")["supports_intraday_snapshot"] is True
    assert mdp.provider_capabilities_v1("YAHOO_CHART")["supports_final_eod"] is False


def test_intraday_operational_mode_can_use_primary_provider_without_separate_intraday_provider(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_MARKET_DATA_MODE", "INTRADAY_OPERATIONAL")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "STOOQ")
    monkeypatch.setenv("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "CBOE")
    monkeypatch.delenv("AEGIS_MARKET_DATA_INTRADAY_PROVIDER", raising=False)

    result = mdp.fetch_market_data_v1(truth_root=tmp_path, day_utc=DAY, symbols=["SPY"], symbol_map={"symbols": {"SPY": {"providers": {"STOOQ": "SPY.US", "CBOE": "SPY"}}}})
    report = mdp.provider_capability_report_v1(mdp.provider_config_from_env_v1())

    assert result.failure_reason != "INTRADAY_MARKET_DATA_PROVIDER_NOT_CONFIGURED"
    assert "STOOQ" in [row["provider"] for row in result.provider_results]
    assert report["status"] in {"VALID", "MISSING_INTRADAY_PROVIDER"}
    assert "AEGIS_MARKET_DATA_INTRADAY_PROVIDER" in report["required_configuration"]


def test_run_sleeves_now_script_uses_intraday_operational_mode() -> None:
    source = Path("ops/tools/run_aegis_intraday_sleeves_now_v1.py").read_text(encoding="utf-8")
    package = Path("package.json").read_text(encoding="utf-8")

    assert "INTRADAY_OPERATIONAL" in source
    assert "YAHOO_CHART" in source
    assert "broker_execution_allowed" in source
    assert "run_aegis_intraday_sleeves_now_v1.py" in package


def test_cboe_unavailable_fails_closed_for_vix_without_certification(tmp_path: Path, monkeypatch) -> None:
    def _raise(request, timeout=0):
        raise mdp.urllib.error.URLError("cboe unavailable")

    monkeypatch.setattr(mdp.urllib.request, "urlopen", _raise)
    result = mdp.fetch_market_data_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        symbols=["VIX"],
        symbol_map={"symbols": {"VIX": {"providers": {"CBOE": "VIX"}}}},
        config_override=_provider_config("CBOE"),
    )

    assert result.request_status == "FAILED"
    assert result.fetched_symbols == ()
    assert result.missing_symbols == ("VIX",)
    assert result.provider_results[0]["provider"] == "CBOE"
    assert result.provider_results[0]["request_status"] == "FAILED"


def test_final_eod_systemd_service_uses_secure_provider_environment_file() -> None:
    service = Path("ops/systemd/user/aegis-market-data-final-eod-v1.service").read_text(encoding="utf-8")

    assert "EnvironmentFile=/home/node/constellation_runtime_data/config/private/aegis_eod_provider.env" in service
    assert "run_aegis_final_eod_certification_v1.py" in service
    assert "TIINGO_API_KEY=" not in service
    assert "ALPHA_VANTAGE_API_KEY=" not in service


def test_final_eod_service_resolves_weekend_to_latest_equities_trading_day() -> None:
    from ops.aegis.market_calendar.session_calendar_v1 import (
        is_us_equities_trading_day_v1,
        latest_us_equities_trading_day_on_or_before_v1,
    )

    assert is_us_equities_trading_day_v1("2026-05-23") is False
    assert latest_us_equities_trading_day_on_or_before_v1("2026-05-23") == "2026-05-22"
    service = Path("ops/systemd/user/aegis-market-data-final-eod-v1.service").read_text(encoding="utf-8")
    assert "--use-latest-trading-day-on-or-before" in service
