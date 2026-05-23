#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.challengers.human_review_decision import (  # noqa: E402
    ALLOWED_DECISIONS,
    build_human_review_decision,
    human_review_decision_read_model,
    write_human_review_decision,
)
from research_lab.challengers.human_review_dossier import list_human_review_dossiers, load_human_review_dossier  # noqa: E402
from research_lab.storage.paths import ensure_store_layout  # noqa: E402


def _latest_dossier_id(store: Path) -> str:
    rows = list_human_review_dossiers(store_root=store)
    if not rows:
        raise RuntimeError("No human review dossiers found")
    return str(rows[-1]["human_review_dossier_id"])


def _dry_run(store: Path, dossier_id: str | None) -> dict[str, object]:
    selected = dossier_id or _latest_dossier_id(store)
    dossier = load_human_review_dossier(selected, store_root=store)
    candidates = [
        {
            "challenger_id": row.get("challenger_hypothesis_id"),
            "variant_name": row.get("variant_name"),
            "evidence_completeness": row.get("evidence_completeness"),
            "confidence_classification": row.get("confidence_classification"),
            "allowed_decisions": sorted(ALLOWED_DECISIONS),
        }
        for row in dossier.get("review_candidates") or []
    ]
    return {
        "ok": True,
        "mode": "dry_run",
        "human_review_dossier_id": selected,
        "allowed_decisions": sorted(ALLOWED_DECISIONS),
        "review_candidates": candidates,
        "read_model": human_review_decision_read_model(store_root=store),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_packet21_v1")
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--dossier-id", default=None)
    parser.add_argument("--challenger-id", default=None)
    parser.add_argument("--decision", choices=sorted(ALLOWED_DECISIONS), default=None)
    parser.add_argument("--decided-by", default=None)
    parser.add_argument("--decided-at", default="1970-01-01T00:00:00Z")
    parser.add_argument("--rationale", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    store = ensure_store_layout(Path(args.store_root).resolve() if args.store_root else None)
    if args.dry_run or not args.decision:
        print(json.dumps(_dry_run(store, args.dossier_id), indent=2, sort_keys=True))
        return 0
    if not args.dossier_id:
        raise RuntimeError("--dossier-id is required when recording a decision")
    if not args.decided_by:
        raise RuntimeError("--decided-by is required when recording a decision")
    if not args.rationale:
        raise RuntimeError("--rationale is required when recording a decision")
    preview = build_human_review_decision(
        human_review_dossier_id=args.dossier_id,
        challenger_id=args.challenger_id,
        decision=args.decision,
        decided_by=args.decided_by,
        decided_at=args.decided_at,
        rationale=args.rationale,
        store_root=store,
    )
    result = write_human_review_decision(
        human_review_dossier_id=args.dossier_id,
        challenger_id=args.challenger_id,
        decision=args.decision,
        decided_by=args.decided_by,
        decided_at=args.decided_at,
        rationale=args.rationale,
        store_root=store,
        actor=args.decided_by,
    )
    print(
        json.dumps(
            {
                "human_review_decision_id": result["decision"]["human_review_decision_id"],
                "linked_human_review_dossier_id": result["decision"]["human_review_dossier_id"],
                "decision_recorded": result["decision"]["decision"],
                "challenger_id": result["decision"]["challenger_id"],
                "registry_entry_present": bool(result["registry_row"]),
                "audit_event_present": bool(result["audit_event"]),
                "source_dossier_hash_verified": preview["source_dossier_hash"] == result["decision"]["source_dossier_hash"],
                "lifecycle_mutation_count": 0,
                "paper_trial_created": False,
                "broker_execution_allowed": False,
                "capital_allocation_allowed": False,
                "automatic_promotion_allowed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
