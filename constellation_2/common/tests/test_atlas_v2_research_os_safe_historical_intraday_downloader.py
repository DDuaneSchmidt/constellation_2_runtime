from __future__ import annotations

from pathlib import Path

from constellation_2.common.atlas_v2_research_os import safe_historical_intraday_downloader as downloader
from constellation_2.common.atlas_v2_research_os.safe_historical_intraday_downloader import (
    build_intraday_download_plan,
    detect_available_data_providers,
    download_intraday_csv_if_explicitly_enabled,
    dry_run_intraday_download,
    normalize_downloaded_intraday_csv,
)


def _write_manifest(root: Path) -> None:
    path = root / "market_data_acquisition_plan"
    path.mkdir(parents=True, exist_ok=True)
    (path / "required_market_data_download_manifest.csv").write_text(
        "symbol,timeframe,start_date,end_date,required_for_candidates,target_csv_path,provider,status,notes\n"
        "DIA,30m,2021-01-01,2026-01-01,ptc1,data/cache/DIA_30m.csv,MANUAL_CSV_DROP,PLANNED_NOT_FETCHED,needed\n"
        "QQQ,5m,2021-01-01,2026-01-01,ptc2,data/cache/QQQ_5m.csv,MANUAL_CSV_DROP,PLANNED_NOT_FETCHED,needed\n"
        "TLT,15m,2021-01-01,2026-01-01,ptc3,data/cache/TLT_15m.csv,MANUAL_CSV_DROP,PLANNED_NOT_FETCHED,not priority 1\n",
        encoding="utf-8",
    )


def test_build_intraday_download_plan_filters_priority_1(tmp_path: Path) -> None:
    _write_manifest(tmp_path)

    plan = build_intraday_download_plan(tmp_path, provider="tiingo")

    assert [(row["symbol"], row["timeframe"]) for row in plan] == [("DIA", "30m"), ("QQQ", "5m")]
    assert all(row["provider"] == "tiingo" for row in plan)
    assert all(row["target_csv_path"].startswith("data/cache/") for row in plan)


def test_dry_run_intraday_download_writes_report_without_fetching(tmp_path: Path) -> None:
    _write_manifest(tmp_path)

    report = dry_run_intraday_download(tmp_path, provider="tiingo", created_at="2026-06-05T00:00:00Z")

    assert report["dry_run"] is True
    assert report["summary"]["planned_count"] == 2
    assert (tmp_path / "historical_intraday_download" / "latest.json").exists()
    assert report["authority_boundary"]["external_api_called"] is False


def test_normalize_downloaded_intraday_csv_outputs_expected_schema() -> None:
    raw = "date,open,high,low,close,volume\n2026-01-01T09:30:00,1,2,0.5,1.5,100\n"

    rows = normalize_downloaded_intraday_csv(raw, symbol="DIA", timeframe="30m", source_file="unit")

    assert rows == [
        {
            "timestamp": "2026-01-01T09:30:00",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100.0,
            "adjusted_close": 1.5,
            "source_file": "unit",
        }
    ]


def test_execute_path_requires_credentials_and_allowlist(tmp_path: Path, monkeypatch) -> None:
    _write_manifest(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    monkeypatch.setattr(downloader, "PRIVATE_PROVIDER_CONFIG", tmp_path / "missing_private.env")

    report = download_intraday_csv_if_explicitly_enabled(tmp_path, provider="tiingo", created_at="2026-06-05T00:00:00Z")

    assert report["mode"] == "EXECUTE"
    assert report["summary"]["blocked_count"] == 2
    assert not list((tmp_path / "data" / "cache").glob("*.csv")) if (tmp_path / "data" / "cache").exists() else True


def test_detect_available_data_providers_is_redacted() -> None:
    providers = detect_available_data_providers()

    assert {row["provider"] for row in providers} == {"tiingo", "alpha_vantage"}
    assert all("API_KEY" not in str(row.get("reason", "")) for row in providers)
