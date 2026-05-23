#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.hypotheses.hypothesis_proposal_batch import build_hypothesis_proposal_batch, write_hypothesis_proposal_batch  # noqa: E402
from research_lab.hypotheses.hypothesis_proposal_engine import generate_hypothesis_proposals  # noqa: E402
from research_lab.storage.hashing import utc_now_iso  # noqa: E402


def _summary(result: dict[str, object], *, persisted: bool) -> dict[str, object]:
    batch = result["batch"]  # type: ignore[index]
    proposals = result.get("proposals") or []
    proposal_rows = [row.get("proposal", {}) for row in proposals]  # type: ignore[union-attr]
    return {
        "hypothesis_proposal_batch_id": batch["hypothesis_proposal_batch_id"],
        "proposal_count": batch["proposal_count"],
        "proposal_status_counts": batch["proposal_status_counts"],
        "proposal_family_counts": batch["proposal_family_counts"],
        "blocked_count": batch["blocked_count"],
        "proposed_for_review_count": batch["proposed_for_review_count"],
        "system_research_only_count": batch["system_research_only_count"],
        "latest_integrity_report_id": batch["latest_integrity_report_id"],
        "latest_research_os_status_report_id": batch["latest_research_os_status_report_id"],
        "source_observation_cluster_batch_id": batch["source_observation_cluster_batch_id"],
        "top_hypothesis_proposal_ids": batch["hypothesis_proposal_ids"][:5],
        "registry_entries_present": bool(result.get("registry_row")) and len(proposal_rows) == int(batch["proposal_count"]),
        "audit_events_present": bool(result.get("audit_event")) and all(bool(row.get("audit_event")) for row in proposals),  # type: ignore[union-attr]
        "active_hypothesis_created": False,
        "lifecycle_mutation_count": 0,
        "persisted": persisted,
        "json_path": result.get("json_path", ""),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_packet27_v1")
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--latest-cluster-batch", action="store_true")
    parser.add_argument("--cluster-batch-id", default=None)
    parser.add_argument("--min-priority-score", type=float, default=0.60)
    parser.add_argument("--max-proposals", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--actor", default="Aegis Packet 27")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    cluster_batch_id = None if args.latest_cluster_batch else args.cluster_batch_id
    if args.dry_run:
        generated_at = utc_now_iso()
        proposals, source_batch, integrity, research_status = generate_hypothesis_proposals(
            store_root=store_root,
            observation_cluster_batch_id=cluster_batch_id,
            min_priority_score=args.min_priority_score,
            max_proposals=args.max_proposals,
            generated_at=generated_at,
        )
        batch = build_hypothesis_proposal_batch(
            proposals=proposals,
            source_observation_cluster_batch_id=str(source_batch.get("observation_cluster_batch_id") or ""),
            latest_integrity_report_id=str((integrity or {}).get("integrity_report_id") or source_batch.get("latest_integrity_report_id") or ""),
            latest_research_os_status_report_id=str((research_status or {}).get("research_os_status_report_id") or source_batch.get("latest_research_os_status_report_id") or ""),
            generated_at=generated_at,
        )
        payload = _summary({"batch": batch, "proposals": [], "registry_row": {}, "audit_event": {}, "json_path": ""}, persisted=False)
    else:
        result = write_hypothesis_proposal_batch(
            store_root=store_root,
            observation_cluster_batch_id=cluster_batch_id,
            min_priority_score=args.min_priority_score,
            max_proposals=args.max_proposals,
            actor=args.actor,
        )
        payload = _summary(result, persisted=True)
    print(json.dumps(payload, indent=2 if args.json else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

