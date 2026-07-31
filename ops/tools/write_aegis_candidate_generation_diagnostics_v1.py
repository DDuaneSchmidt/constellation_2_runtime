#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.entry_reference_price_certification_v1 import build_entry_reference_price_certification_v1, write_entry_reference_price_certification_v1  # noqa: E402
from ops.aegis.signal_evidence_graph_v1 import build_signal_evidence_graph_v1, write_signal_evidence_graph_v1  # noqa: E402
from ops.aegis.candidate_contracts_v1 import build_candidate_contracts_v1, write_candidate_contracts_v1  # noqa: E402
from ops.aegis.candidate_generation_diagnostics_v1 import (  # noqa: E402
    build_candidate_generation_diagnostics_v1,
    write_candidate_generation_diagnostics_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.hash_lineage_v1 import write_current_hash_lineage_v1  # noqa: E402
from ops.aegis.data_remediation_v1 import run_data_remediation_v1  # noqa: E402
from ops.aegis.run_context_v1 import child_run_context_v1, run_context_from_env_v1, step_allowed_v1  # noqa: E402
from ops.aegis.run_history_v1 import append_candidate_diagnostics_run_history_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_candidate_generation_diagnostics_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--skip-self-heal", action="store_true")
    args = parser.parse_args(argv)
    context = run_context_from_env_v1("candidate-diagnostics")
    if not args.skip_self_heal:
        allowed, _reason = step_allowed_v1(context, "self_heal")
        if allowed:
            run_data_remediation_v1(
                truth_root=Path(args.truth_root),
                repo_root=REPO_ROOT,
                day_utc=str(args.day_utc),
                run_context=child_run_context_v1(
                    context,
                    "self-heal-data",
                    allow_market_data_refresh=False,
                    allow_self_heal=True,
                    allow_projection_rebuild=True,
                ),
            )
    lineage = write_current_hash_lineage_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    if lineage.get("stale_downstream_artifacts"):
        print(json.dumps({"status": "BLOCKED", "blocker": "STALE_SLEEVE_INPUT_CONTRACT_MARKET_DATA_HASH", "message": "Coverage is ready, but downstream sleeve contracts were generated against an older market-data hash. Run repair-input-contracts.", "hash_lineage_path": (lineage.get("paths") or {}).get("json", ""), "required_regeneration_actions": lineage.get("required_regeneration_actions") or [], "broker_execution_allowed": False, "autonomous_execution_allowed": False, "trade_advice_allowed": False}, sort_keys=True))
        return 2
    cert_payload = build_entry_reference_price_certification_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    write_entry_reference_price_certification_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=cert_payload)
    graph_payload = build_signal_evidence_graph_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), repo_root=REPO_ROOT)
    write_signal_evidence_graph_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=graph_payload)
    contracts_payload = build_candidate_contracts_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), repo_root=REPO_ROOT)
    write_candidate_contracts_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=contracts_payload)
    payload = build_candidate_generation_diagnostics_v1(
        truth_root=Path(args.truth_root),
        repo_root=REPO_ROOT,
        day_utc=str(args.day_utc),
    )
    paths = write_candidate_generation_diagnostics_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        payload=payload,
    )
    run_history = append_candidate_diagnostics_run_history_v1(
        truth_root=Path(args.truth_root),
        day_utc=str(args.day_utc),
        command="npm run aegis:candidate-diagnostics",
        diagnostics=payload,
        diagnostics_path=str(paths.get("json") or ""),
        candidate_contracts_path=str(paths.get("candidate_contracts") or ""),
        paper_review_queue_path=str(paths.get("paper_review_queue") or ""),
    )
    print(
        json.dumps(
            {
                **paths,
                "run_history": str(Path(args.truth_root).expanduser().resolve() / "reports" / "aegis_run_history_v1" / str(args.day_utc) / "run_history.v1.json"),
                "latest_run_id": run_history.get("latest_run_id"),
                "candidate_generation_status": payload["candidate_generation_status"],
                "operator_interpretation": payload["operator_interpretation"],
                "total_sleeves_expected": payload["total_sleeves_expected"],
                "total_sleeves_run": payload["total_sleeves_run"],
                "total_raw_signals": payload["total_raw_signals"],
                "diagnostic_candidate_outputs": payload.get("diagnostic_candidate_outputs", 0),
                "valid_candidate_contracts": payload.get("valid_candidate_contracts", payload.get("total_candidates_generated", 0)),
                "rejected_candidate_contracts": payload.get("rejected_candidate_contracts", 0),
                "total_candidates_generated": payload["total_candidates_generated"],
                "total_candidates_rejected": payload["total_candidates_rejected"],
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
                "automatic_sleeve_mutation_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
