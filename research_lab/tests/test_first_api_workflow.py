from __future__ import annotations

import pytest

from research_lab.workflows.first_api_workflow import build_first_api_evidence, first_api_dataset_status


def test_first_api_dataset_status_reports_missing_api_key(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)

    status = first_api_dataset_status(
        provider="alpha_vantage",
        universe="local_etf_minimum_viable_v1",
        store_root=tmp_path / "store",
    )

    assert status["ready_to_build"] is False
    assert status["blocking_reasons"] == ["missing ALPHA_VANTAGE_API_KEY"]


def test_first_api_dataset_status_prints_exact_smoke_and_build_commands(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "test-key")

    status = first_api_dataset_status(
        provider="alpha_vantage",
        universe="local_etf_minimum_viable_v1",
        store_root=tmp_path / "store",
    )

    assert "live-ohlcv-smoke-test --provider alpha_vantage" in status["exact_smoke_test_command"]
    assert "build-ohlcv-dataset --provider alpha_vantage" in status["exact_build_command"]
    assert "create-standard-event-study" in status["exact_standard_event_study_commands"]["create_plan"]


def test_build_first_api_evidence_fails_closed_if_api_key_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="missing ALPHA_VANTAGE_API_KEY"):
        build_first_api_evidence(
            provider="alpha_vantage",
            universe="local_etf_minimum_viable_v1",
            start="2015-01-01",
            end="2025-12-31",
            threshold=-0.02,
            forward_windows=[1, 2, 5, 10],
            store_root=tmp_path / "store",
        )

