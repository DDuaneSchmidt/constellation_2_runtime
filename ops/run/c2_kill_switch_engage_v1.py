#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

def _atomic_write(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    s = json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(s, encoding="utf-8")
    os.replace(tmp, path)

def main() -> int:
    ap = argparse.ArgumentParser(prog="c2_kill_switch_engage_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--failed_unit", required=True)
    ap.add_argument("--reason", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    unit = str(args.failed_unit).strip()
    reason = str(args.reason).strip()

    out = {
        "schema_id": "C2_GLOBAL_KILL_SWITCH_STATE_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": f"{day}T00:00:00Z",
        "status": "ACTIVE",
        "reason_codes": ["AUTOMATION_FAILURE"],
        "notes": [f"failed_unit={unit}", f"reason={reason}"],
        "producer": {"repo": "constellation_2_runtime", "module": "ops/run/c2_kill_switch_engage_v1.py"},
    }

    p = (TRUTH_ROOT / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json").resolve()
    _atomic_write(p, out)
    print(f"OK: KILL_SWITCH_ENGAGED path={p} unit={unit} reason={reason}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
