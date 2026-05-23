#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1
from ops.aegis.submit_boundary_precheck_v1 import build_and_write_submit_boundary_precheck_v1
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import build_and_write_paper_trade_construction_v1
from ops.aegis.trade_ticket_lineage_v1 import write_ticket_evidence_set_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_submit_boundary_precheck_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", required=True)
    parser.add_argument("--day_utc", "--day", dest="day_utc", required=True)
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    current_truth = resolve_current_operator_truth_v1(truth_root=root, day_utc=str(args.day_utc))
    construction, construction_path = build_and_write_paper_trade_construction_v1(
        truth_root=root,
        day_utc=str(args.day_utc),
        current_operator_truth=current_truth,
    )
    evidence = write_ticket_evidence_set_v1(truth_root=root, construction=construction)
    precheck, precheck_path, lineage = build_and_write_submit_boundary_precheck_v1(
        truth_root=root,
        construction=construction,
    )
    print(
        json.dumps(
            {
                "ok": precheck.get("validation_status") in {"VALIDATED", "NOT_APPLICABLE"},
                "validation_status": precheck.get("validation_status"),
                "ticket_id": precheck.get("ticket_id"),
                "lineage_status": lineage.get("lineage_status"),
                "blocker_codes": precheck.get("blocker_codes"),
                "paper_trade_construction_id": construction.get("construction_id"),
                "construction_contract_hash": construction.get("construction_contract_hash"),
                "paper_trade_construction_path": str(construction_path),
                "submit_boundary_precheck_path": str(precheck_path),
                "trade_ticket_lineage_path": lineage.get("artifact_path"),
                "evidence_paths": evidence.get("paths"),
                "broker_execution_allowed": False,
                "order_routing_allowed": False,
                "autonomous_execution_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0 if precheck.get("validation_status") in {"VALIDATED", "NOT_APPLICABLE"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
