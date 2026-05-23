from __future__ import annotations

import os
from pathlib import Path

from research_lab.config.runtime_config import apply_runtime_config
from research_lab.config.secrets import load_provider_secrets


def test_loads_api_keys_from_explicit_env_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    env_file = tmp_path / ".env.local"
    env_file.write_text("TIINGO_API_KEY=tiingo-secret-123\n", encoding="utf-8")

    loaded = load_provider_secrets(env_file=env_file)

    assert loaded["TIINGO_API_KEY"].value == "tiingo-secret-123"
    assert loaded["TIINGO_API_KEY"].source == str(env_file)


def test_os_env_overrides_env_file(monkeypatch, tmp_path: Path) -> None:
    env_file = tmp_path / ".env.local"
    env_file.write_text("TIINGO_API_KEY=file-secret-123\n", encoding="utf-8")
    monkeypatch.setenv("TIINGO_API_KEY", "os-secret-456")

    loaded = load_provider_secrets(env_file=env_file)

    assert loaded["TIINGO_API_KEY"].value == "os-secret-456"
    assert loaded["TIINGO_API_KEY"].source == "os_environment"


def test_apply_runtime_config_sets_file_secret_without_overriding_os_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)
    monkeypatch.setenv("TIINGO_API_KEY", "os-secret-456")
    env_file = tmp_path / ".env.local"
    env_file.write_text("ALPHA_VANTAGE_API_KEY=alpha-secret-123\nTIINGO_API_KEY=file-secret-123\n", encoding="utf-8")

    payload = apply_runtime_config(env_file=env_file)

    assert os.environ["ALPHA_VANTAGE_API_KEY"] == "alpha-secret-123"
    assert os.environ["TIINGO_API_KEY"] == "os-secret-456"
    assert "ALPHA_VANTAGE_API_KEY" in payload["loaded_keys"]
