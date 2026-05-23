from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from research_lab.bars.bar_policy import require_bar_policy
from research_lab.config.secrets import load_provider_secrets, mask_secret
from research_lab.storage.paths import research_lab_root


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    enabled: bool
    api_key_env: str
    bar_policy: str


DEFAULT_PROVIDERS = {
    "alpha_vantage": ProviderConfig(
        name="alpha_vantage",
        enabled=True,
        api_key_env="ALPHA_VANTAGE_API_KEY",
        bar_policy="bp_daily_ohlcv_alpha_vantage_v1",
    ),
    "tiingo": ProviderConfig(
        name="tiingo",
        enabled=True,
        api_key_env="TIINGO_API_KEY",
        bar_policy="bp_daily_ohlcv_tiingo_v1",
    ),
    "eodhd": ProviderConfig(
        name="eodhd",
        enabled=False,
        api_key_env="EODHD_API_KEY",
        bar_policy="bp_daily_ohlcv_eodhd_v1",
    ),
}

DEFAULT_RATE_LIMITS = {
    "alpha_vantage": (15.0, 3),
    "tiingo": (2.0, 3),
    "eodhd": (2.0, 3),
}


def default_provider_yaml() -> Path:
    return research_lab_root() / "config" / "providers.example.yaml"


def load_provider_configs(path: Path | None = None) -> dict[str, ProviderConfig]:
    config_path = path or default_provider_yaml()
    if not config_path.exists():
        return dict(DEFAULT_PROVIDERS)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    providers = payload.get("providers") or {}
    configs: dict[str, ProviderConfig] = {}
    for name, row in providers.items():
        configs[str(name)] = ProviderConfig(
            name=str(name),
            enabled=bool(row.get("enabled", False)),
            api_key_env=str(row.get("api_key_env", "")),
            bar_policy=str(row.get("bar_policy", "")),
        )
    return configs or dict(DEFAULT_PROVIDERS)


def _rate_limit_values(provider_name: str, secrets: dict) -> tuple[float | None, int | None]:
    prefix = provider_name.upper()
    if provider_name == "alpha_vantage":
        prefix = "ALPHA_VANTAGE"
    sleep_key = f"{prefix}_RATE_LIMIT_SLEEP_SECONDS"
    retries_key = f"{prefix}_MAX_RETRIES"
    sleep_value = secrets.get(sleep_key)
    retries_value = secrets.get(retries_key)
    default_sleep, default_retries = DEFAULT_RATE_LIMITS.get(provider_name, (None, None))
    return (
        float(sleep_value.value) if sleep_value and sleep_value.value else default_sleep,
        int(retries_value.value) if retries_value and retries_value.value else default_retries,
    )


def provider_config_status(*, env_file: Path | None = None, config_path: Path | None = None) -> dict[str, Any]:
    secrets = load_provider_secrets(env_file=env_file)
    rows: list[dict[str, Any]] = []
    for provider_name, config in sorted(load_provider_configs(config_path).items()):
        secret = secrets.get(config.api_key_env)
        api_key_present = bool(secret and secret.value)
        malformed = api_key_present and len(secret.value.strip()) < 8
        try:
            require_bar_policy(config.bar_policy)
            bar_policy_available = True
        except Exception:
            bar_policy_available = False
        sleep_seconds, max_retries = _rate_limit_values(provider_name, secrets)
        blocking: list[str] = []
        if not config.enabled:
            blocking.append("provider_disabled")
        if not api_key_present:
            blocking.append(f"missing {config.api_key_env}")
        if malformed:
            blocking.append(f"malformed {config.api_key_env}")
        if not bar_policy_available:
            blocking.append(f"missing bar policy {config.bar_policy}")
        rows.append(
            {
                "provider": provider_name,
                "enabled": config.enabled,
                "api_key_present": api_key_present,
                "api_key_source": secret.source if secret else None,
                "masked_key": mask_secret(secret.value) if secret else None,
                "rate_limit_sleep_seconds": sleep_seconds,
                "max_retries": max_retries,
                "bar_policy": config.bar_policy,
                "bar_policy_available": bar_policy_available,
                "ready": not blocking,
                "blocking_reason": "; ".join(blocking) if blocking else None,
            }
        )
    return {"providers": rows, "schema_version": "provider_config_status.v1"}


def provider_ready_or_raise(*, provider: str, env_file: Path | None = None) -> dict[str, Any]:
    status = provider_config_status(env_file=env_file)
    for row in status["providers"]:
        if row["provider"] == provider:
            if not row["ready"]:
                raise RuntimeError(f"Provider {provider} is not ready: {row['blocking_reason']}")
            return row
    raise RuntimeError(f"Provider {provider} is not configured")
