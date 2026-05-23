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
from ops.aegis.event_append_transaction_v1 import (  # noqa: E402
    canonical_payload_hash_v1,
    contract_input_hashes_for_paths_v1,
    emit_artifact_evidence_transaction_v1,
    sha256_file_v1,
)


DATASETS = {
    "price_data": ["market_data_snapshot_v1", "prices", "ohlcv"],
    "volatility_data": ["event_market_snapshot_v1", "volatility", "vix", "realized_volatility"],
    "breadth_data": ["event_market_snapshot_v1", "breadth", "advance_decline", "market_breadth"],
    "macro_event_calendar": ["event_market_snapshot_v1", "macro_event_calendar", "events/macro", "economic_calendar"],
    "regime_labels": ["event_market_snapshot_v1", "regime", "regime_snapshot_v1", "regime_labels"],
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


def _hash_dataset_path(path: Path) -> str:
    if path.is_file():
        return sha256_file_v1(path)
    if path.is_dir():
        rows = []
        for child in sorted(item for item in path.rglob("*") if item.is_file()):
            rel = child.relative_to(path).as_posix()
            rows.append({"path": rel, "sha256": sha256_file_v1(child)})
        return canonical_payload_hash_v1({"directory": str(path), "files": rows})
    return ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="audit_research_dataset_bindings_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    gaps = []
    dataset_paths = []
    for dataset, names in DATASETS.items():
        paths = _find(root, names)
        status = "BOUND" if paths else "MISSING"
        dataset_paths.extend(paths)
        gaps.append(
            {
                "dataset_name": dataset,
                "required_source": ",".join(names),
                "current_status": status,
                "found_paths": paths,
                "canonical_dataset_paths": paths,
                "blocker": "" if paths else f"{dataset.upper()}_NOT_BOUND",
                "rejection_classification": "NONE" if paths else "MISSING",
                "binding_reason_trace": [
                    {
                        "step": "SOURCE_DISCOVERY",
                        "status": status,
                        "searched_names": names,
                        "found_path_count": len(paths),
                    }
                ],
                "next_action": "Use existing bound dataset." if paths else f"Bind or import {dataset} before relying on real Research tests.",
            }
        )
    dataset_hashes = {path: _hash_dataset_path(Path(path)) for path in sorted(set(dataset_paths)) if Path(path).exists()}
    input_dependency_hashes = contract_input_hashes_for_paths_v1(dataset_paths, extra={"dataset_discovery_hash": canonical_payload_hash_v1(gaps)})
    rejection_classification = "NONE" if all(row["current_status"] == "BOUND" for row in gaps) and dataset_hashes else "MISSING"
    payload = {
        "schema_id": "research_dataset_binding",
        "schema_version": "v1",
        "artifact_id": "research_dataset_binding_v1",
        "day_utc": args.day_utc,
        "generated_at_utc": _now(),
        "dataset_gaps": gaps,
        "canonical_dataset_paths": sorted(set(dataset_paths)),
        "dataset_content_hashes": dataset_hashes,
        "dataset_hashes": dataset_hashes,
        "input_dependency_hashes": input_dependency_hashes,
        "binding_input_hash": canonical_payload_hash_v1(gaps),
        "binding_reason_trace": [
            {
                "dataset_name": row["dataset_name"],
                "current_status": row["current_status"],
                "rejection_classification": row["rejection_classification"],
                "blocker": row["blocker"],
            }
            for row in gaps
        ],
        "validation_status": "VALID" if rejection_classification == "NONE" else "REJECTED",
        "rejection_classification": rejection_classification,
        "rejection_reason": "" if rejection_classification == "NONE" else "One or more required research datasets are not bound.",
        "freshness_timestamp_utc": _now(),
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    path = root / "reports" / "research_dataset_gap_v1" / args.day_utc / "research_dataset_gap.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    missing = [row["dataset_name"] for row in gaps if row["current_status"] == "MISSING"]
    event_results = emit_artifact_evidence_transaction_v1(
        truth_root=root,
        day_utc=args.day_utc,
        artifact_path=path,
        payload=payload,
        producer_id="ops/tools/audit_research_dataset_bindings_v1.py",
        producer_version="v1",
        run_id=f"audit_research_dataset_bindings_v1:{args.day_utc}",
        created_at_utc=str(payload["generated_at_utc"]),
        input_hashes=input_dependency_hashes,
        validation_status="VALID" if not missing and payload.get("dataset_hashes") else "INVALID",
    )
    print(json.dumps({"path": str(path), "missing": missing, "event_ids": [str(row.get("event", {}).get("event_id") or "") for row in event_results], "broker_submit_required": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
