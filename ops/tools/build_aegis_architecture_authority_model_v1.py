from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.architecture_authority_model_v1 import build_and_write_architecture_authority_model_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Aegis Architecture Authority Model v1")
    parser.add_argument("--truth-root", default=os.environ.get("AEGIS_TRUTH_ROOT", "/home/node/constellation_runtime_data/truth"))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=os.environ.get("TARGET_DAY") or os.environ.get("DAY") or "2026-05-30")
    args = parser.parse_args()
    result = build_and_write_architecture_authority_model_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc)
    print(json.dumps({"ok": result["ok"], "path": result["path"], "validation": result["validation"]}, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
