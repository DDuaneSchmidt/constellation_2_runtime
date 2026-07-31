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

from ops.aegis.evidence_lineage_integrity_v1 import (  # noqa: E402
    build_evidence_lineage_integrity_v1,
    write_evidence_lineage_integrity_v1,
    write_lineage_audit_doc_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_evidence_lineage_integrity_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--write-audit-doc", action="store_true")
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    payload = build_evidence_lineage_integrity_v1(truth_root=root, day_utc=day)
    path = write_evidence_lineage_integrity_v1(truth_root=root, day_utc=day, payload=payload)
    audit_doc = ""
    if args.write_audit_doc:
        audit_doc = str(write_lineage_audit_doc_v1(truth_root=root, day_utc=day, output_path=REPO_ROOT / "docs" / "aegis_evidence_lineage_audit.md"))
    panel = payload.get("evidence_coverage_panel") or {}
    print(json.dumps({"ok": True, "day_utc": day, "path": str(path), "audit_doc": audit_doc, "evidence_coverage_panel": panel}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
