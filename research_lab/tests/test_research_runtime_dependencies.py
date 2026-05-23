from __future__ import annotations

from pathlib import Path

from research_lab.cli import main
from research_lab.runtime.dependencies import RESEARCH_DEPENDENCIES, research_dependency_health


def test_research_dependency_health_reports_required_packages() -> None:
    payload = research_dependency_health()

    assert set(RESEARCH_DEPENDENCIES) == {"pandas", "pyarrow", "duckdb", "yfinance", "yaml", "jsonschema"}
    assert {row["name"] for row in payload["dependencies"]} == set(RESEARCH_DEPENDENCIES)
    assert "install_commands" in payload
    assert payload["schema_version"] == "research_dependency_health.v1"


def test_dependency_health_cli_returns_structured_status() -> None:
    code = main(["dependency-health"])

    assert code in {0, 4}


def test_provider_diagnostics_cli_returns_structured_status() -> None:
    code = main(["provider-diagnostics", "--provider", "yfinance"])

    assert code in {0, 4}


def test_live_smoke_test_fails_closed_without_provider_stack(tmp_path: Path) -> None:
    code = main(
        [
            "--store-root",
            str(tmp_path / "store"),
            "live-ohlcv-smoke-test",
            "--provider",
            "not-a-provider",
            "--symbols",
            "SPY",
            "QQQ",
            "--start",
            "2024-01-02",
            "--end",
            "2024-02-01",
        ]
    )

    assert code == 4
