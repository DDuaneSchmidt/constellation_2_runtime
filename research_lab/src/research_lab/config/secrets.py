from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from research_lab.storage.paths import research_lab_root


SECRET_ENV_KEYS = {
    "ALPHA_VANTAGE_API_KEY",
    "TIINGO_API_KEY",
    "EODHD_API_KEY",
    "ALPHA_VANTAGE_RATE_LIMIT_SLEEP_SECONDS",
    "ALPHA_VANTAGE_MAX_RETRIES",
    "TIINGO_RATE_LIMIT_SLEEP_SECONDS",
    "TIINGO_MAX_RETRIES",
    "EODHD_RATE_LIMIT_SLEEP_SECONDS",
    "EODHD_MAX_RETRIES",
}


@dataclass(frozen=True)
class LoadedSecret:
    key: str
    value: str
    source: str


def default_env_file() -> Path:
    return research_lab_root() / ".env.local"


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip('"').strip("'")
        values[key] = value
    return values


def mask_secret(value: str) -> str | None:
    if not value:
        return None
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:2]}***{value[-2:]}"


def load_provider_secrets(*, env_file: Path | None = None, environ: Mapping[str, str] | None = None) -> dict[str, LoadedSecret]:
    env = dict(os.environ if environ is None else environ)
    source_path = env_file or default_env_file()
    file_values = parse_env_file(source_path)
    loaded: dict[str, LoadedSecret] = {}
    for key in SECRET_ENV_KEYS:
        env_value = env.get(key)
        if env_value is not None and env_value != "":
            loaded[key] = LoadedSecret(key=key, value=env_value, source="os_environment")
        elif key in file_values and file_values[key] != "":
            loaded[key] = LoadedSecret(key=key, value=file_values[key], source=str(source_path))
    return loaded


def apply_provider_secrets(*, env_file: Path | None = None) -> dict[str, LoadedSecret]:
    loaded = load_provider_secrets(env_file=env_file)
    for key, item in loaded.items():
        if key not in os.environ or os.environ.get(key, "") == "":
            os.environ[key] = item.value
    return loaded

