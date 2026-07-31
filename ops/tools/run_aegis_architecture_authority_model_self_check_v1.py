from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.architecture_authority_model_v1 import architecture_authority_model_path_v1, validate_architecture_authority_model_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Self-check Aegis Architecture Authority Model v1")
    parser.add_argument("--truth-root", default=os.environ.get("AEGIS_TRUTH_ROOT", "/home/node/constellation_runtime_data/truth"))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=os.environ.get("TARGET_DAY") or os.environ.get("DAY") or "2026-05-30")
    args = parser.parse_args()
    path = architecture_authority_model_path_v1(Path(args.truth_root), args.day_utc)
    if not path.exists():
        result = {"ok": False, "failure_count": 1, "failures": [f"missing_authority_model:{path}"]}
    else:
        result = validate_architecture_authority_model_v1(json.loads(path.read_text(encoding="utf-8")))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
