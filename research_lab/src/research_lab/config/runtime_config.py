from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.config.secrets import apply_provider_secrets


def apply_runtime_config(*, env_file: Path | None = None) -> dict[str, Any]:
    loaded = apply_provider_secrets(env_file=env_file)
    return {
        "env_file": str(env_file) if env_file else None,
        "loaded_keys": sorted(loaded.keys()),
        "schema_version": "research_runtime_config.v1",
    }

