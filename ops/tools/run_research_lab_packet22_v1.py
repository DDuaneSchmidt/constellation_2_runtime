#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.integrity.research_store_integrity import (  # noqa: E402
    build_research_store_integrity_report,
    error_count,
    warning_count,
    write_research_store_integrity_report,
)


def _summary(report: dict[str, object], *, persisted: bool) -> dict[str, object]:
    return {
        "integrity_report_id": report["integrity_report_id"],
        "overall_status": report["overall_status"],
        "artifact_count": report["artifact_counts"]["audited_json_artifacts"],  # type: ignore[index]
        "registry_count": len(report["registry_counts"]),  # type: ignore[arg-type]
        "audit_event_count": sum(int(value) for value in report["audit_event_counts"].values()),  # type: ignore[union-attr]
        "error_count": error_count(report),
        "warning_count": warning_count(report),
        "missing_reference_count": len(report["missing_references"]),  # type: ignore[arg-type]
        "orphan_count": len(report["orphaned_artifacts"]),  # type: ignore[arg-type]
        "hash_mismatch_count": len(report["hash_mismatches"]),  # type: ignore[arg-type]
        "schema_failure_count": len(report["schema_validation_failures"]),  # type: ignore[arg-type]
        "lineage_break_count": len(report["lineage_breaks"]),  # type: ignore[arg-type]
        "duplicate_registry_entry_count": len(report["duplicate_registry_entries"]),  # type: ignore[arg-type]
        "mutation_boundary_violation_count": len(report["mutation_boundary_violations"]),  # type: ignore[arg-type]
        "known_blocker_count": len(report["unresolved_blockers"]),  # type: ignore[arg-type]
        "persisted": persisted,
        "research_label_present": report.get("research_label") == "RESEARCH_ONLY",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_packet22_v1")
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--scope", choices=["full", "packet20-21"], default="full")
    parser.add_argument("--fail-on-warning", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--actor", default="Aegis Packet 22")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    if args.dry_run:
        report = build_research_store_integrity_report(store_root=store_root, scope=args.scope)
        payload = _summary(report, persisted=False)
    else:
        result = write_research_store_integrity_report(store_root=store_root, scope=args.scope, actor=args.actor)
        report = result["report"]
        payload = _summary(report, persisted=True) | {
            "registry_entry_present": bool(result["registry_row"]),
            "audit_event_present": bool(result["audit_event"]),
            "json_path": result["json_path"],
        }
    print(json.dumps(payload, indent=2 if args.json else None, sort_keys=True))
    if report["overall_status"] == "FAIL":
        return 2
    if args.fail_on_warning and report["overall_status"] == "PASS_WITH_WARNINGS":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
