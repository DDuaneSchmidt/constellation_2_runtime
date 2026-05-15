#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_lite_event_awareness_v1 import (  # noqa: E402
    build_event_tactical_packet_v1,
    validate_event_awareness_artifact_v1,
    write_event_awareness_artifact_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_event_tactical_review_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--event_id", required=True)
    parser.add_argument("--event_run_id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", required=True)
    parser.add_argument("--instrument_type", default="EQUITY")
    parser.add_argument("--entry_reference_price", required=True)
    parser.add_argument("--order_type_suggestion", default="MANUAL_LIMIT_OR_MARKET_BY_OPERATOR")
    parser.add_argument("--quantity_or_sizing_guidance", required=True)
    parser.add_argument("--stop_price", required=True)
    parser.add_argument("--stop_logic", required=True)
    parser.add_argument("--risk_per_trade", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--edge_family", required=True)
    parser.add_argument("--regime_state", default="UNKNOWN")
    parser.add_argument("--confidence", default="LOW")
    parser.add_argument("--execution_sensitivity", required=True)
    parser.add_argument("--valid_until", required=True)
    parser.add_argument("--max_entry_slippage", required=True)
    parser.add_argument("--invalidation_conditions", required=True)
    parser.add_argument("--inclusion_reason", required=True)
    parser.add_argument("--exclusion_reason", default="")
    parser.add_argument("--governance_notes", default="")
    args = parser.parse_args(argv)

    packet = build_event_tactical_packet_v1(
        event_id=args.event_id,
        event_run_id=args.event_run_id,
        day_utc=args.day_utc,
        symbol=args.symbol,
        side=args.side,
        instrument_type=args.instrument_type,
        entry_reference_price=args.entry_reference_price,
        order_type_suggestion=args.order_type_suggestion,
        quantity_or_sizing_guidance=args.quantity_or_sizing_guidance,
        stop_price=args.stop_price,
        stop_logic=args.stop_logic,
        risk_per_trade=args.risk_per_trade,
        event_type=args.event_type,
        edge_family=args.edge_family,
        regime_state=args.regime_state,
        confidence=args.confidence,
        execution_sensitivity=args.execution_sensitivity,
        valid_until=args.valid_until,
        max_entry_slippage=args.max_entry_slippage,
        invalidation_conditions=args.invalidation_conditions,
        inclusion_reason=args.inclusion_reason,
        exclusion_reason=args.exclusion_reason,
        governance_notes=args.governance_notes,
    )
    validate_event_awareness_artifact_v1(packet)
    path = write_event_awareness_artifact_v1(truth_root=Path(args.truth_root), payload=packet)
    print(json.dumps({"packet_path": str(path), "recommended_trade_id": packet["recommended_trade_id"], "non_canonical_event_packet": True, "broker_submit_required": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
