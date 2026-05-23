#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.paper_trials.paper_trial_proposal import build_paper_trial_proposal, write_paper_trial_proposal  # noqa: E402


def _summary(proposal: dict[str, object], *, persisted: bool) -> dict[str, object]:
    return {
        "paper_trial_proposal_id": proposal["paper_trial_proposal_id"],
        "proposal_status": proposal["proposal_status"],
        "eligibility_status": proposal["eligibility_status"],
        "linked_human_review_decision_id": proposal["human_review_decision_id"],
        "linked_human_review_dossier_id": proposal["human_review_dossier_id"],
        "linked_challenger_id": proposal["challenger_id"],
        "latest_integrity_report_id": proposal["latest_integrity_report_id"],
        "latest_research_os_status_report_id": proposal["latest_research_os_status_report_id"],
        "blocker_count": len(proposal.get("blockers") or []),  # type: ignore[arg-type]
        "blocker_codes": [row.get("blocker_code") for row in proposal.get("blockers") or []],  # type: ignore[union-attr]
        "paper_trial_created": False,
        "lifecycle_mutation_count": 0,
        "persisted": persisted,
        "registry_entry_present": False,
        "audit_event_present": False,
        "json_path": "",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_packet24_v1")
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--decision-id", default=None)
    parser.add_argument("--allow-red-status", action="store_true")
    parser.add_argument("--override-reason", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--actor", default="Aegis Packet 24")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    if args.dry_run:
        proposal = build_paper_trial_proposal(
            human_review_decision_id=args.decision_id,
            allow_red_status=args.allow_red_status,
            override_reason=args.override_reason,
            store_root=store_root,
        )
        payload = _summary(proposal, persisted=False)
    else:
        result = write_paper_trial_proposal(
            human_review_decision_id=args.decision_id,
            allow_red_status=args.allow_red_status,
            override_reason=args.override_reason,
            store_root=store_root,
            actor=args.actor,
        )
        proposal = result["proposal"]
        payload = _summary(proposal, persisted=True) | {
            "registry_entry_present": bool(result["registry_row"]),
            "audit_event_present": bool(result["audit_event"]),
            "json_path": result["json_path"],
        }
    print(json.dumps(payload, indent=2 if args.json else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

