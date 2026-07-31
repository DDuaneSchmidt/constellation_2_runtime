#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.human_reviewed_paper_mode_v1 import (
    active_candidate_state_rows_v1,
    paper_trade_receipts_path_v1,
)
from ops.aegis.operator_action_command_contracts_v1 import execute_aegis_command_v1
from ops.aegis.paper_operator_projection_v1 import build_and_write_paper_operator_projection_v1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute the same PAPER_TRADE_CANDIDATE backend command used by the Aegis UI.")
    parser.add_argument("--truth_root", "--truth-root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", dest="day", required=True)
    parser.add_argument("--candidate", required=True, help="Canonical active candidate_id from paper_operator_projection.current_day_candidates.")
    parser.add_argument("--json", action="store_true")
    return parser


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _find_candidate(projection: dict[str, Any], candidate_id: str) -> dict[str, Any]:
    for row in _as_list(projection.get("current_day_candidates")):
        if isinstance(row, dict) and str(row.get("candidate_id") or "") == candidate_id:
            return row
    return {}


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    candidate_id = str(args.candidate).strip()

    projection, projection_path = build_and_write_paper_operator_projection_v1(truth_root=root, day_utc=day)
    row = _find_candidate(projection, candidate_id)
    active_ids = {str(item.get("candidate_id") or "") for item in active_candidate_state_rows_v1(truth_root=root, day_utc=day) if isinstance(item, dict)}
    payload = {
        "command_id": "PAPER_TRADE_CANDIDATE",
        "target_type": "paper_review_candidate",
        "target_id": candidate_id,
        "operational_day": day,
        "payload": {
            "action": "PAPER_TRADE",
            "candidate_id": candidate_id,
            "candidate_contract_id": str(row.get("candidate_contract_id") or candidate_id),
            "paper_session_id": str(row.get("paper_session_id") or ""),
            "day_utc": day,
            "paper_entry_price": str(row.get("entry_price") or row.get("entry_reference_price") or ""),
            "entry_price": str(row.get("entry_price") or row.get("entry_reference_price") or ""),
            "paper_stop_price": str(row.get("stop_price") or ""),
            "stop_price": str(row.get("stop_price") or ""),
            "quantity": str(row.get("quantity") or ""),
            "notional": str(row.get("notional_value") or ""),
            "operator": "paper-trade-smoke",
            "reason": "PAPER_TRADE_SMOKE",
        },
    }
    result: dict[str, Any]
    if not row:
        result = {"ok": False, "error_code": "CANDIDATE_NOT_FOUND", "message": "candidate not found in paper_operator_projection.current_day_candidates"}
    else:
        result = execute_aegis_command_v1(payload, truth_root=root, repo_root=ROOT, day_utc=day, actor="paper-trade-smoke")

    refreshed_projection, refreshed_path = build_and_write_paper_operator_projection_v1(truth_root=root, day_utc=day)
    refreshed_row = _find_candidate(refreshed_projection, candidate_id)
    receipt_path = paper_trade_receipts_path_v1(truth_root=root, day_utc=day)
    receipt = result.get("receipt") if isinstance(result.get("receipt"), dict) else {}
    receipt_id = f"paper-review:{candidate_id}:{receipt.get('timestamp_utc') or receipt.get('timestamp') or ''}" if receipt else ""
    summary = {
        "candidate": candidate_id,
        "symbol": str(row.get("symbol") or refreshed_row.get("symbol") or ""),
        "candidate_found": bool(row),
        "active_review_present": candidate_id in active_ids,
        "construction_present": bool(row.get("construction_present")) if row else False,
        "entry_price": str(row.get("entry_price") or ""),
        "stop_price": str(row.get("stop_price") or ""),
        "quantity": str(row.get("quantity") or ""),
        "paper_session_id": str(row.get("paper_session_id") or ""),
        "endpoint_equivalent": "/api/aegis/commands/execute",
        "payload": payload,
        "command_ok": bool(result.get("ok") is True),
        "command_status": str(result.get("status") or result.get("result_status") or ""),
        "message": str(result.get("message") or result.get("user_message") or result.get("error_message") or ""),
        "receipt_created": bool(result.get("ok") is True and receipt),
        "receipt_id": receipt_id,
        "receipt_path": str(receipt_path),
        "projection_refreshed": True,
        "projection_path": str(refreshed_path),
        "final_row_state": str(refreshed_row.get("candidate_status") or refreshed_row.get("lifecycle_state") or "MISSING"),
        "final_row_actionable": bool(refreshed_row.get("actionable") is True),
        "response": result,
    }
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(f"candidate: {summary['candidate']}")
        print(f"symbol: {summary['symbol']}")
        print(f"candidate found: {summary['candidate_found']}")
        print(f"active review present: {summary['active_review_present']}")
        print(f"construction present: {summary['construction_present']}")
        print(f"endpoint equivalent: {summary['endpoint_equivalent']}")
        print(f"command ok: {summary['command_ok']}")
        print(f"command status: {summary['command_status']}")
        print(f"message: {summary['message']}")
        print(f"receipt created: {summary['receipt_created']}")
        print(f"receipt id: {summary['receipt_id'] or 'NONE'}")
        print(f"receipt path: {summary['receipt_path']}")
        print(f"projection refreshed: {summary['projection_refreshed']}")
        print(f"final row state: {summary['final_row_state']}")
        print(f"final row actionable: {summary['final_row_actionable']}")
    return 0 if summary["command_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
