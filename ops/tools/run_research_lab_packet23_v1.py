#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.status.research_os_status import (  # noqa: E402
    build_research_os_status_report,
    report_summary,
    write_research_os_status_report,
)


def _summary(report: dict[str, object], *, persisted: bool) -> dict[str, object]:
    return report_summary(report) | {
        "persisted": persisted,
        "registry_entry_present": False,
        "audit_event_present": False,
        "json_path": "",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_packet23_v1")
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--include-legacy-warnings", action="store_true")
    parser.add_argument("--fail-on-red", action="store_true")
    parser.add_argument("--actor", default="Aegis Packet 23")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    if args.dry_run:
        report = build_research_os_status_report(store_root=store_root)
        payload = _summary(report, persisted=False)
    else:
        result = write_research_os_status_report(store_root=store_root, actor=args.actor)
        report = result["report"]
        payload = report_summary(report) | {
            "persisted": True,
            "registry_entry_present": bool(result["registry_row"]),
            "audit_event_present": bool(result["audit_event"]),
            "json_path": result["json_path"],
        }
    print(json.dumps(payload, indent=2 if args.json else None, sort_keys=True))
    if args.fail_on_red and report["overall_status"] == "RED":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

