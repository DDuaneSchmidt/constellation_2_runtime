#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.challengers.challenger_comparison import write_challenger_comparison_report
from research_lab.challengers.challenger_evidence import write_challenger_evidence_batch
from research_lab.challengers.human_review_dossier import write_human_review_dossier
from research_lab.storage.manifest_io import read_jsonl
from research_lab.storage.paths import ensure_store_layout


DEFAULT_TRACK_ID = "chtrk_slv_etf_drop_reversion_v1_3e4313496418"


def main() -> int:
    store = ensure_store_layout(None)
    track_id = DEFAULT_TRACK_ID
    evidence = write_challenger_evidence_batch(challenger_track_id=track_id, store_root=store, actor="Aegis Packet 20")
    batch = evidence["batch"]
    comparison = write_challenger_comparison_report(
        challenger_evidence_batch_id=batch["challenger_evidence_batch_id"],
        challenger_track_id=track_id,
        store_root=store,
        actor="Aegis Packet 20",
    )["report"]
    dossier = write_human_review_dossier(
        challenger_comparison_report_id=comparison["challenger_comparison_report_id"],
        store_root=store,
        actor="Aegis Packet 20",
    )["dossier"]
    audit_rows = read_jsonl(store / "audit_log" / "audit_events.jsonl")
    items = batch["challenger_evidence_items"]
    summary = {
        "packet": "Packet 20",
        "challenger_track_id": track_id,
        "challenger_evidence_batch_id": batch["challenger_evidence_batch_id"],
        "challenger_comparison_report_id": comparison["challenger_comparison_report_id"],
        "human_review_dossier_id": dossier["human_review_dossier_id"],
        "total_hypotheses": len(items),
        "materialized_chain_count": len(evidence["materialized_chains"]),
        "blocked_hypothesis_count": len(batch["blocked_hypotheses"]),
        "evidence_completeness_report": [
            {
                "challenger_hypothesis_id": item.get("challenger_hypothesis_id"),
                "status": item.get("status"),
                "evidence_completeness": item.get("evidence_completeness"),
                "artifact_lineage": item.get("artifact_lineage"),
                "failure_reason": item.get("failure_reason"),
            }
            for item in items
        ],
        "blocker_inventory": batch["blocked_hypotheses"],
        "deterministic_replay_verification": {
            "determinism_fingerprint": batch["determinism_fingerprint"],
            "content_hash": batch["content_hash"],
        },
        "artifact_lineage_summary": [item.get("artifact_lineage") for item in items if item.get("artifact_lineage")],
        "audit_event_count": len(audit_rows),
        "research_label_present": batch.get("research_label") == "RESEARCH_ONLY",
        "no_lifecycle_mutation": batch.get("governance_constraints", {}).get("evidence_generation_mutates_sleeve_state") is False,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
