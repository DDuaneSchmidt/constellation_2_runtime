from __future__ import annotations

import pytest

import research_lab.providers.factory as factory
from research_lab.providers.base import DailyOHLCVProvider
from research_lab.workflows.first_api_workflow import build_first_api_evidence, first_api_dataset_status


class DummyTiingoProvider(DailyOHLCVProvider):
    provider_name = "tiingo"
    provider_version = "test"

    def fetch_daily_ohlcv(self, symbol, start, end):
        return []


def test_provider_factory_returns_tiingo(monkeypatch) -> None:
    monkeypatch.setenv("TIINGO_API_KEY", "test-key")
    monkeypatch.setattr(factory, "TiingoDailyProvider", DummyTiingoProvider)

    assert isinstance(factory.get_daily_ohlcv_provider("tiingo"), DummyTiingoProvider)


def test_tiingo_provider_diagnostics_reports_missing_api_key(monkeypatch) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)

    payload = factory.provider_diagnostics("tiingo")

    assert payload["provider"] == "tiingo"
    assert payload["available"] is False
    assert payload["api_key_present"] is False
    assert payload["error"] == "missing TIINGO_API_KEY"


def test_tiingo_provider_diagnostics_reports_api_key_present(monkeypatch) -> None:
    monkeypatch.setenv("TIINGO_API_KEY", "test-key")
    monkeypatch.setenv("TIINGO_RATE_LIMIT_SLEEP_SECONDS", "0")

    payload = factory.provider_diagnostics("tiingo")

    assert payload["available"] is True
    assert payload["api_key_present"] is True
    assert payload["rate_limit_policy"]["sleep_seconds"] == 0


def test_first_api_dataset_status_reports_missing_tiingo_api_key(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)

    status = first_api_dataset_status(
        provider="tiingo",
        universe="local_etf_minimum_viable_v1",
        store_root=tmp_path / "store",
    )

    assert status["ready_to_build"] is False
    assert status["blocking_reasons"] == ["missing TIINGO_API_KEY"]
    assert status["bar_policy_version"] == "bp_daily_ohlcv_tiingo_v1"


def test_first_api_dataset_status_prints_exact_tiingo_commands(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TIINGO_API_KEY", "test-key")

    status = first_api_dataset_status(
        provider="tiingo",
        universe="local_etf_minimum_viable_v1",
        store_root=tmp_path / "store",
    )

    assert "live-ohlcv-smoke-test --provider tiingo" in status["exact_smoke_test_command"]
    assert "build-ohlcv-dataset --provider tiingo" in status["exact_build_command"]
    assert "bp_daily_ohlcv_tiingo_v1" in status["exact_build_command"]


def test_build_first_api_evidence_fails_closed_if_tiingo_key_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="missing TIINGO_API_KEY"):
        build_first_api_evidence(
            provider="tiingo",
            universe="local_etf_minimum_viable_v1",
            start="2015-01-01",
            end="2025-12-31",
            threshold=-0.02,
            forward_windows=[1, 2, 5, 10],
            store_root=tmp_path / "store",
        )
