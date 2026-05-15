#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_lite_promoted_candidates_v1 import write_promoted_sleeve_library_v1  # noqa: E402
from constellation_2.common.aegis_research_lab_v1 import build_promoted_sleeve_library_v1, validate_research_lab_artifact_v1  # noqa: E402


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: str) -> dict[str, Any]:
    payload = json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON_OBJECT_REQUIRED:{path}")
    return payload


def _approved(promotion: dict[str, Any]) -> bool:
    schema = str(promotion.get("schema_id") or "")
    if schema == "research_to_lite_promotion":
        return (
            str(promotion.get("promotion_status") or "") == "APPROVED_FOR_LITE_IMPLEMENTATION"
            and bool(promotion.get("approved_by_human", False))
            and bool(promotion.get("eligible_for_lite_implementation", False))
        )
    if schema == "promotion_review":
        return bool(promotion.get("approved_by_human", False)) and bool(promotion.get("eligible_for_promoted_sleeve_library", False))
    return False


def _sleeve_from_promotion(promotion: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    hypothesis_id = str(promotion.get("hypothesis_id") or args.hypothesis_id)
    sleeve_id = args.sleeve_id or f"SLEEVE_{hypothesis_id}".upper().replace("-", "_")
    regimes = [item for item in args.expected_regimes.split(",") if item]
    instruments = [item for item in args.instrument_universe.split(",") if item]
    return {
        "sleeve_id": sleeve_id,
        "source_hypothesis_id": hypothesis_id,
        "research_hypothesis_id": hypothesis_id,
        "sleeve_name": args.sleeve_name or sleeve_id,
        "edge_family": args.edge_family,
        "behavioral_thesis": args.behavioral_thesis,
        "regime_fit": regimes,
        "instrument_universe": instruments,
        "entry_logic": args.entry_logic,
        "exit_logic": args.exit_logic,
        "stop_logic": args.stop_logic,
        "sizing_logic": args.sizing_logic,
        "invalidation_logic": args.invalidation_logic,
        "known_failure_modes": [item for item in args.known_failure_modes.split(",") if item],
        "overlap_tags": [item for item in args.overlap_tags.split(",") if item],
        "promotion_evidence_path": str(Path(args.promotion_json).expanduser().resolve()),
        "promotion_status": "promoted",
        "approved_by_human": True,
        "approved_for_lite_implementation": True,
        "approved_edge_families": [args.edge_family],
        "approved_trade_classes": [args.trade_class],
        "expected_regimes": regimes,
        "operational_constraints": [item for item in args.operational_constraints.split(",") if item],
        "archived": False,
        "human_approval_status": "approved",
        "implementation_status": "approved",
        "production_status": "manual_paper",
        "created_at": _now(),
        "updated_at": _now(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="promote_validated_hypothesis_to_sleeve_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--promotion_json", required=True)
    parser.add_argument("--sleeve_id", required=True)
    parser.add_argument("--edge_family", required=True)
    parser.add_argument("--trade_class", required=True)
    parser.add_argument("--sleeve_name", default="")
    parser.add_argument("--hypothesis_id", default="")
    parser.add_argument("--expected_regimes", default="UNKNOWN")
    parser.add_argument("--instrument_universe", default="SPY")
    parser.add_argument("--behavioral_thesis", default="Human-approved Research hypothesis prepared for manual-paper Lite evaluation.")
    parser.add_argument("--entry_logic", default="Entry must come from a complete Aegis Lite manual trade packet.")
    parser.add_argument("--exit_logic", default="Exit must be recorded through manual receipt and outcome ledger evidence.")
    parser.add_argument("--stop_logic", default="Protective stop is required before a packet can be actionable.")
    parser.add_argument("--sizing_logic", default="Use explicit manual packet quantity or sizing guidance only.")
    parser.add_argument("--invalidation_logic", default="Block if required entry, stop, risk, sizing, or lineage evidence is missing.")
    parser.add_argument("--known_failure_modes", default="insufficient_evidence,regime_mismatch")
    parser.add_argument("--overlap_tags", default="manual_paper")
    parser.add_argument("--operational_constraints", default="MANUAL_ONLY")
    parser.add_argument("--existing_promoted_sleeve_library", default="")
    parser.add_argument("--run_id", default="operator_promotion")
    args = parser.parse_args(argv)

    promotion = _read_json(args.promotion_json)
    if not _approved(promotion):
        raise SystemExit("FAIL: PROMOTION_NOT_APPROVED_FOR_LITE_IMPLEMENTATION")
    existing = []
    if args.existing_promoted_sleeve_library:
        existing_payload = _read_json(args.existing_promoted_sleeve_library)
        existing = [row for row in existing_payload.get("sleeves", []) if isinstance(row, dict)]
    sleeve = _sleeve_from_promotion(promotion, args)
    merged = [row for row in existing if str(row.get("sleeve_id") or "") != sleeve["sleeve_id"]]
    library = build_promoted_sleeve_library_v1(generated_at_utc=_now(), sleeves=[*merged, sleeve])
    validate_research_lab_artifact_v1(library)
    path = write_promoted_sleeve_library_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc, run_id=args.run_id, payload=library)
    print(json.dumps({"promoted_sleeve_library_path": str(path), "sleeve_id": sleeve["sleeve_id"], "broker_submit_required": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
