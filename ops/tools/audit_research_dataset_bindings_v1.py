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

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1  # noqa: E402


DATASETS = {
    "price_data": ["market_data_snapshot_v1", "prices", "ohlcv"],
    "volatility_data": ["volatility", "vix", "realized_volatility"],
    "breadth_data": ["breadth", "advance_decline", "market_breadth"],
    "macro_event_calendar": ["macro_event_calendar", "events/macro", "economic_calendar"],
    "regime_labels": ["regime", "regime_snapshot_v1", "regime_labels"],
    "sleeve_outcomes": ["sleeve_performance_report_v1", "outcome_ledger_v1", "trade_outcome_attribution_v1"],
}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _find(root: Path, names: list[str]) -> list[str]:
    found = []
    for name in names:
        for path in [root / name, root / "reports" / name, root / "research_lab" / name]:
            if path.exists():
                found.append(str(path))
    return sorted(set(found))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="audit_research_dataset_bindings_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    gaps = []
    for dataset, names in DATASETS.items():
        paths = _find(root, names)
        status = "BOUND" if paths else "MISSING"
        gaps.append(
            {
                "dataset_name": dataset,
                "required_source": ",".join(names),
                "current_status": status,
                "found_paths": paths,
                "blocker": "" if paths else f"{dataset.upper()}_NOT_BOUND",
                "next_action": "Use existing bound dataset." if paths else f"Bind or import {dataset} before relying on real Research tests.",
            }
        )
    payload = {
        "schema_id": "research_dataset_gap",
        "schema_version": "v1",
        "artifact_id": "research_dataset_gap_v1",
        "day_utc": args.day_utc,
        "generated_at_utc": _now(),
        "dataset_gaps": gaps,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    path = root / "reports" / "research_dataset_gap_v1" / args.day_utc / "research_dataset_gap.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    print(json.dumps({"path": str(path), "missing": [row["dataset_name"] for row in gaps if row["current_status"] == "MISSING"], "broker_submit_required": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
