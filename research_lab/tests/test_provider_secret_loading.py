from __future__ import annotations

from pathlib import Path

from research_lab.config.provider_config import provider_config_status
from research_lab.config.secrets import mask_secret
from ops.aegis.research_lab.research_store_refs_v1 import AEGIS_RESEARCH_REFERENCE_TABLES


def test_missing_key_reports_ready_false(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    env_file = tmp_path / ".env.local"
    env_file.write_text("TIINGO_API_KEY=\n", encoding="utf-8")

    status = provider_config_status(env_file=env_file)
    tiingo = next(row for row in status["providers"] if row["provider"] == "tiingo")

    assert tiingo["ready"] is False
    assert tiingo["blocking_reason"] == "missing TIINGO_API_KEY"


def test_masked_key_never_exposes_full_secret(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.local"
    secret = "tiingo-secret-abcdef"
    env_file.write_text(f"TIINGO_API_KEY={secret}\n", encoding="utf-8")

    status = provider_config_status(env_file=env_file)
    tiingo = next(row for row in status["providers"] if row["provider"] == "tiingo")

    assert tiingo["masked_key"] == mask_secret(secret)
    assert secret not in str(status)


def test_provider_config_status_reports_bar_policy_availability(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.local"
    env_file.write_text("TIINGO_API_KEY=tiingo-secret-abcdef\n", encoding="utf-8")

    status = provider_config_status(env_file=env_file)
    tiingo = next(row for row in status["providers"] if row["provider"] == "tiingo")

    assert tiingo["bar_policy"] == "bp_daily_ohlcv_tiingo_v1"
    assert tiingo["bar_policy_available"] is True


def test_malformed_short_key_fails_readiness(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    env_file = tmp_path / ".env.local"
    env_file.write_text("TIINGO_API_KEY=short\n", encoding="utf-8")

    status = provider_config_status(env_file=env_file)
    tiingo = next(row for row in status["providers"] if row["provider"] == "tiingo")

    assert tiingo["ready"] is False
    assert "malformed TIINGO_API_KEY" in tiingo["blocking_reason"]


def test_aegis_still_stores_no_canonical_ohlcv_rows() -> None:
    fields = {field for table in AEGIS_RESEARCH_REFERENCE_TABLES for field in table.fields}

    assert not {"open", "high", "low", "close", "volume"}.intersection(fields)
