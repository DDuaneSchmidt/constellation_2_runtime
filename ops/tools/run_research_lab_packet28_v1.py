#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.hypotheses.hypothesis_proposal_review_batch import (  # noqa: E402
    build_hypothesis_proposal_review_batch,
    build_reviews_for_batch,
    write_hypothesis_proposal_review_batch,
)
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso  # noqa: E402


def _summary(result: dict[str, object], *, persisted: bool) -> dict[str, object]:
    batch = result["batch"]  # type: ignore[index]
    reviews = result.get("reviews") or []
    return {
        "hypothesis_proposal_review_batch_id": batch["hypothesis_proposal_review_batch_id"],
        "review_count": batch["review_count"],
        "review_decision_counts": batch["review_decision_counts"],
        "review_status_counts": batch["review_status_counts"],
        "source_hypothesis_proposal_batch_id": batch["source_hypothesis_proposal_batch_id"],
        "latest_integrity_report_id": batch["latest_integrity_report_id"],
        "latest_research_os_status_report_id": batch["latest_research_os_status_report_id"],
        "top_hypothesis_proposal_review_ids": batch["hypothesis_proposal_review_ids"][:5],
        "registry_entries_present": bool(result.get("registry_row")) and len(reviews) == int(batch["review_count"]),
        "audit_events_present": bool(result.get("audit_event")) and all(bool(row.get("audit_event")) for row in reviews),  # type: ignore[union-attr]
        "active_hypothesis_created": False,
        "lifecycle_mutation_count": 0,
        "persisted": persisted,
        "json_path": result.get("json_path", ""),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_packet28_v1")
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--proposal-id", default=None)
    parser.add_argument("--decision", default=None)
    parser.add_argument("--reviewer-id", default="system_packet28")
    parser.add_argument("--rationale", default=None)
    parser.add_argument("--latest-batch", action="store_true")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--auto-review-safe", action="store_true")
    parser.add_argument("--allow-system-research-activation-review", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--actor", default="Aegis Packet 28")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    source_batch_id = None if args.latest_batch else args.batch_id
    auto_review_safe = args.auto_review_safe or not args.decision
    if args.dry_run:
        generated_at = utc_now_iso()
        reviews, source_batch = build_reviews_for_batch(
            store_root=store_root,
            source_batch_id=source_batch_id,
            generated_at=generated_at,
            reviewer_id=args.reviewer_id,
            auto_review_safe=auto_review_safe,
            decision=args.decision,
            rationale=args.rationale,
            proposal_id=args.proposal_id,
            allow_system_research_activation_review=args.allow_system_research_activation_review,
        )
        batch = build_hypothesis_proposal_review_batch(
            reviews=reviews,
            source_hypothesis_proposal_batch_id=str(source_batch.get("hypothesis_proposal_batch_id") or ""),
            latest_integrity_report_id=str(source_batch.get("latest_integrity_report_id") or ""),
            latest_research_os_status_report_id=str(source_batch.get("latest_research_os_status_report_id") or ""),
            generated_at=generated_at,
            review_run_id=f"hprun_{short_hash(content_hash({'generated_at': generated_at, 'source_batch_id': source_batch.get('hypothesis_proposal_batch_id')}), 16)}",
        )
        payload = _summary({"batch": batch, "reviews": [], "registry_row": {}, "audit_event": {}, "json_path": ""}, persisted=False)
    else:
        result = write_hypothesis_proposal_review_batch(
            store_root=store_root,
            source_batch_id=source_batch_id,
            reviewer_id=args.reviewer_id,
            auto_review_safe=auto_review_safe,
            decision=args.decision,
            rationale=args.rationale,
            proposal_id=args.proposal_id,
            allow_system_research_activation_review=args.allow_system_research_activation_review,
            actor=args.actor,
        )
        payload = _summary(result, persisted=True)
    print(json.dumps(payload, indent=2 if args.json else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

