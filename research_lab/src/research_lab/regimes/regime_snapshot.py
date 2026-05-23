from __future__ import annotations

from typing import Any

from research_lab.storage.hashing import content_hash


REGIME_MODEL_VERSION = "spy_daily_regime_v1"


def regime_snapshot_content_hash(snapshot: dict[str, Any]) -> str:
    return content_hash(snapshot, exclude={"created_at", "content_hash", "regime_snapshot_id", "storage_uri"}, sort_lists=False)

