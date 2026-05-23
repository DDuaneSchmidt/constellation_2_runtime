from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.evidence.evidence_registry import load_evidence_package_manifest
from research_lab.storage.manifest_io import read_json, write_json
from research_lab.storage.paths import resolve_research_uri


def compare_evidence_packages(evidence_package_ids: list[str], *, store_root: Path | None = None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for package_id in evidence_package_ids:
        manifest = load_evidence_package_manifest(package_id, store_root=store_root)
        summary = read_json(resolve_research_uri(manifest["summary_uri"], store_root=store_root))
        for window in summary.get("forward_windows", []):
            key = str(window)
            rows.append(
                {
                    "evidence_package_id": package_id,
                    "hypothesis_id": manifest["hypothesis_id"],
                    "dataset_snapshot_id": manifest["dataset_snapshot_id"],
                    "event_count": manifest.get("event_count"),
                    "evidence_quality": manifest.get("evidence_quality"),
                    "forward_window": int(window),
                    "mean_return": summary.get("mean_forward_return_by_window", {}).get(key),
                    "median_return": summary.get("median_forward_return_by_window", {}).get(key),
                    "win_rate": summary.get("win_rate_by_window", {}).get(key),
                    "created_at": manifest["created_at"],
                }
            )
    return {"comparison_rows": rows, "package_count": len(evidence_package_ids), "schema_version": "evidence_comparison.v1"}


def write_evidence_comparison(comparison: dict[str, Any], output_path: Path) -> Path:
    return write_json(output_path, comparison, overwrite=True)

