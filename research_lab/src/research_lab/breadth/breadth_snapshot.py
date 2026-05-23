from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso

SCHEMA_VERSION = "breadth_snapshot.v1"
REGIMES = {"strong_breadth", "normal_breadth", "weak_breadth", "breadth_collapse", "breadth_recovery", "unknown"}


def classify_breadth_regime(metrics: dict[str, Any], previous_metrics: dict[str, Any] | None = None) -> str:
    if not metrics or int(metrics.get("symbol_count") or 0) <= 0:
        return "unknown"
    pct_20 = float(metrics.get("pct_above_20dma") or 0.0)
    pct_50 = float(metrics.get("pct_above_50dma") or 0.0)
    pct_pos_5d = float(metrics.get("pct_positive_5d") or 0.0)
    if pct_50 <= 0.25 or pct_pos_5d <= 0.25:
        return "breadth_collapse"
    if previous_metrics:
        prev_20 = float(previous_metrics.get("pct_above_20dma") or 0.0)
        prev_pos_5d = float(previous_metrics.get("pct_positive_5d") or 0.0)
        if pct_20 >= 0.55 and pct_pos_5d >= 0.65 and (pct_20 - prev_20 >= 0.25 or pct_pos_5d - prev_pos_5d >= 0.25):
            return "breadth_recovery"
    if pct_20 >= 0.75 and pct_50 >= 0.65:
        return "strong_breadth"
    if pct_20 < 0.4 or pct_50 < 0.4:
        return "weak_breadth"
    return "normal_breadth"


def recompute_breadth_snapshot_hash(snapshot: dict[str, Any]) -> str:
    return content_hash(snapshot, exclude={"breadth_snapshot_id", "storage_uri", "created_at", "content_hash"}, sort_lists=False)


def build_breadth_snapshot(*, dataset_snapshot_id: str, universe_snapshot_id: str, metrics: list[dict[str, Any]], created_by: str = "Aegis", created_at: str | None = None) -> dict[str, Any]:
    regimes = sorted({str(row.get("breadth_regime") or "unknown") for row in metrics})
    payload = {
        "breadth_snapshot_id": "",
        "dataset_snapshot_id": dataset_snapshot_id,
        "universe_snapshot_id": universe_snapshot_id,
        "metric_count": len(metrics),
        "start_date": str(metrics[0]["date"]) if metrics else "",
        "end_date": str(metrics[-1]["date"]) if metrics else "",
        "breadth_regimes_present": regimes,
        "storage_uri": "",
        "created_at": created_at or utc_now_iso(),
        "created_by": created_by,
        "schema_version": SCHEMA_VERSION,
        "research_label": "RESEARCH_ONLY",
        "content_hash": "",
    }
    fingerprint = recompute_breadth_snapshot_hash(payload)
    payload["breadth_snapshot_id"] = f"brs_{short_hash(content_hash({'fingerprint': fingerprint, 'created_at': payload['created_at']}), 16)}"
    payload["storage_uri"] = f"research://breadth/{payload['breadth_snapshot_id']}"
    payload["content_hash"] = fingerprint
    validate_breadth_snapshot(payload)
    return payload


def validate_breadth_snapshot(snapshot: dict[str, Any]) -> None:
    validate_contract("breadth_snapshot", snapshot)
    actual = recompute_breadth_snapshot_hash(snapshot)
    if actual != snapshot.get("content_hash"):
        raise ValueError(f"BreadthSnapshot content_hash mismatch: expected {snapshot.get('content_hash')}, got {actual}")
