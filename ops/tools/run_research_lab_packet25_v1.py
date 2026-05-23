#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.observations.observation_batch import build_observation_candidate_batch, write_observation_candidate_batch  # noqa: E402
from research_lab.observations.observation_scanner import run_observation_scanners  # noqa: E402
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso  # noqa: E402


def _summary(result: dict[str, object], *, persisted: bool) -> dict[str, object]:
    batch = result["batch"]  # type: ignore[index]
    return {
        "observation_candidate_batch_id": batch["observation_candidate_batch_id"],
        "observation_count": batch["observation_count"],
        "observation_type_counts": batch["observation_type_counts"],
        "severity_counts": batch["severity_counts"],
        "scanner_statuses": batch["scanner_statuses"],
        "latest_integrity_report_id": batch["latest_integrity_report_id"],
        "latest_research_os_status_report_id": batch["latest_research_os_status_report_id"],
        "top_observation_candidate_ids": batch["observation_candidate_ids"][:5],
        "market_data_scanner_status": next((row.get("status") for row in batch["scanner_statuses"] if row.get("scanner") == "market"), "not_run"),
        "registry_entries_present": bool(result.get("registry_row")) and bool(result.get("candidates")),
        "audit_events_present": bool(result.get("audit_event")) and all(bool(row.get("audit_event")) for row in result.get("candidates", [])),
        "lifecycle_mutation_count": 0,
        "persisted": persisted,
        "json_path": result.get("json_path", ""),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_packet25_v1")
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--scanner", choices=["research_store", "challenger", "candidate", "market", "all"], action="append", default=None)
    parser.add_argument("--max-observations", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--actor", default="Aegis Packet 25")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    scanners = args.scanner or ["all"]
    if args.dry_run:
        generated_at = utc_now_iso()
        observations, statuses = run_observation_scanners(store_root=store_root, scanners=scanners, generated_at=generated_at, max_observations=args.max_observations)
        scanner_run_id = f"obsrun_{short_hash(content_hash({'generated_at': generated_at, 'scanners': scanners}), 16)}"
        batch = build_observation_candidate_batch(observations=observations, scanner_statuses=statuses, generated_at=generated_at, scanner_run_id=scanner_run_id)
        payload = _summary({"batch": batch, "candidates": [], "registry_row": {}, "audit_event": {}, "json_path": ""}, persisted=False)
    else:
        result = write_observation_candidate_batch(
            store_root=store_root,
            scanners=scanners,
            max_observations=args.max_observations,
            actor=args.actor,
        )
        payload = _summary(result, persisted=True)
    print(json.dumps(payload, indent=2 if args.json else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
