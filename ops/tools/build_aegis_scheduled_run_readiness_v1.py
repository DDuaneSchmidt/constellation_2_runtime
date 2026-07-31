#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.scheduled_run_dependency_manifest_v1 import build_scheduled_run_dependency_manifest_v1, write_scheduled_run_dependency_manifest_v1  # noqa: E402
from ops.aegis.scheduled_run_readiness_certificate_v1 import build_scheduled_run_readiness_certificate_v1, write_scheduled_run_readiness_certificate_v1  # noqa: E402
from ops.aegis.scheduled_run_registry_v1 import build_scheduled_run_registry_v1, write_scheduled_run_registry_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_scheduled_run_readiness_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args()
    root = Path(args.truth_root)
    registry = build_scheduled_run_registry_v1(truth_root=root, day_utc=args.day_utc)
    registry_path = write_scheduled_run_registry_v1(truth_root=root, day_utc=args.day_utc, payload=registry)
    manifest = build_scheduled_run_dependency_manifest_v1(truth_root=root, day_utc=args.day_utc)
    manifest_path = write_scheduled_run_dependency_manifest_v1(truth_root=root, day_utc=args.day_utc, payload=manifest)
    certificate = build_scheduled_run_readiness_certificate_v1(truth_root=root, day_utc=args.day_utc)
    certificate_path = write_scheduled_run_readiness_certificate_v1(truth_root=root, day_utc=args.day_utc, payload=certificate)
    print(json.dumps({"ok": True, "day_utc": args.day_utc, "paths": {"registry": str(registry_path), "dependency_manifest": str(manifest_path), "readiness_certificate": str(certificate_path)}, "summary": certificate.get("summary", {})}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
