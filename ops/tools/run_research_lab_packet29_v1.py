#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.hypotheses.hypothesis_intake_batch import (  # noqa: E402
    build_hypothesis_intake_batch,
    build_intake_for_review_batch,
    write_hypothesis_intake_batch,
)
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso  # noqa: E402


def _summary(result: dict[str, object], *, persisted: bool) -> dict[str, object]:
    batch = result["batch"]  # type: ignore[index]
    decisions = result.get("decisions") or []
    research_hypotheses = result.get("research_hypotheses") or []
    return {
        "hypothesis_intake_batch_id": batch["hypothesis_intake_batch_id"],
        "intake_count": batch["intake_count"],
        "accepted_count": batch["accepted_count"],
        "blocked_count": batch["blocked_count"],
        "deferred_count": batch["deferred_count"],
        "created_research_hypothesis_ids": batch["created_research_hypothesis_ids"],
        "latest_integrity_report_id": batch["latest_integrity_report_id"],
        "latest_research_os_status_report_id": batch["latest_research_os_status_report_id"],
        "source_hypothesis_proposal_review_batch_id": batch["source_hypothesis_proposal_review_batch_id"],
        "top_hypothesis_intake_decision_ids": batch["hypothesis_intake_decision_ids"][:5],
        "registry_entries_present": bool(result.get("registry_row")) and len(decisions) == int(batch["intake_count"]),
        "audit_events_present": bool(result.get("audit_event")) and all(bool(row.get("audit_event")) for row in decisions),  # type: ignore[union-attr]
        "active_hypothesis_created": False,
        "inactive_research_hypothesis_created_count": len(research_hypotheses),
        "lifecycle_mutation_count": 0,
        "persisted": persisted,
        "json_path": result.get("json_path", ""),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_packet29_v1")
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--latest-review-batch", action="store_true")
    parser.add_argument("--review-batch-id", default=None)
    parser.add_argument("--review-id", default=None)
    parser.add_argument("--allow-red-status", action="store_true")
    parser.add_argument("--allow-unapproved-review", action="store_true")
    parser.add_argument("--override-reason", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--actor", default="Aegis Packet 29")
    args = parser.parse_args(argv)

    store_root = Path(args.store_root).resolve() if args.store_root else None
    source_batch_id = None if args.latest_review_batch else args.review_batch_id
    if args.dry_run:
        generated_at = utc_now_iso()
        decisions, hypotheses, source_batch = build_intake_for_review_batch(
            store_root=store_root,
            source_batch_id=source_batch_id,
            review_id=args.review_id,
            generated_at=generated_at,
            allow_red_status=args.allow_red_status,
            allow_unapproved_review=args.allow_unapproved_review,
            override_reason=args.override_reason,
        )
        batch = build_hypothesis_intake_batch(
            decisions=decisions,
            source_hypothesis_proposal_review_batch_id=str(source_batch.get("hypothesis_proposal_review_batch_id") or ""),
            created_research_hypothesis_ids=[row["research_hypothesis_id"] for row in hypotheses],
            latest_integrity_report_id=str((decisions[0] if decisions else source_batch).get("latest_integrity_report_id") or ""),
            latest_research_os_status_report_id=str((decisions[0] if decisions else source_batch).get("latest_research_os_status_report_id") or ""),
            generated_at=generated_at,
            intake_run_id=f"hirun_{short_hash(content_hash({'generated_at': generated_at, 'source_batch_id': source_batch.get('hypothesis_proposal_review_batch_id'), 'review_id': args.review_id or ''}), 16)}",
        )
        payload = _summary({"batch": batch, "decisions": [], "research_hypotheses": [], "registry_row": {}, "audit_event": {}, "json_path": ""}, persisted=False)
    else:
        result = write_hypothesis_intake_batch(
            store_root=store_root,
            source_batch_id=source_batch_id,
            review_id=args.review_id,
            allow_red_status=args.allow_red_status,
            allow_unapproved_review=args.allow_unapproved_review,
            override_reason=args.override_reason,
            actor=args.actor,
        )
        payload = _summary(result, persisted=True)
    print(json.dumps(payload, indent=2 if args.json else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
