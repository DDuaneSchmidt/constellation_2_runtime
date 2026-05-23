#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.observations.observation_cluster_batch import build_observation_cluster_batch, write_observation_cluster_batch  # noqa: E402
from research_lab.observations.observation_clustering import cluster_observations  # noqa: E402
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso  # noqa: E402


def _summary(result: dict[str, object], *, persisted: bool) -> dict[str, object]:
    batch = result["batch"]  # type: ignore[index]
    statuses = batch.get("cluster_status_counts") or {}
    families = batch.get("cluster_family_counts") or {}
    return {
        "observation_cluster_batch_id": batch["observation_cluster_batch_id"],
        "cluster_count": batch["cluster_count"],
        "cluster_family_counts": families,
        "cluster_status_counts": statuses,
        "highest_priority_cluster_ids": batch["highest_priority_cluster_ids"],
        "latest_integrity_report_id": batch["latest_integrity_report_id"],
        "latest_research_os_status_report_id": batch["latest_research_os_status_report_id"],
        "market_data_cluster_status": "blocked" if "market_price_volume" in families and "blocked" in statuses else "not_present",
        "registry_entries_present": bool(result.get("registry_row")) and bool(result.get("clusters")),
        "audit_events_present": bool(result.get("audit_event")) and all(bool(row.get("audit_event")) for row in result.get("clusters", [])),
        "lifecycle_mutation_count": 0,
        "hypothesis_created": False,
        "persisted": persisted,
        "json_path": result.get("json_path", ""),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_packet26_v1")
    parser.add_argument("--store-root", default=None)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--latest-only", action="store_true")
    mode.add_argument("--all-history", action="store_true")
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--max-clusters", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--actor", default="Aegis Packet 26")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    latest_only = not args.all_history
    if args.dry_run:
        generated_at = utc_now_iso()
        clusters, source_batches = cluster_observations(store_root=store_root, latest_only=latest_only, generated_at=generated_at, window_days=args.window_days, max_clusters=args.max_clusters)
        run_id = f"oclr_{short_hash(content_hash({'generated_at': generated_at, 'latest_only': latest_only}), 16)}"
        batch = build_observation_cluster_batch(clusters=clusters, source_batches=source_batches, generated_at=generated_at, clustering_run_id=run_id)
        payload = _summary({"batch": batch, "clusters": [], "registry_row": {}, "audit_event": {}, "json_path": ""}, persisted=False)
    else:
        result = write_observation_cluster_batch(
            store_root=store_root,
            latest_only=latest_only,
            window_days=args.window_days,
            max_clusters=args.max_clusters,
            actor=args.actor,
        )
        payload = _summary(result, persisted=True)
    print(json.dumps(payload, indent=2 if args.json else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

