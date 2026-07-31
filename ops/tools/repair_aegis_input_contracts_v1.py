#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.hash_lineage_v1 import write_current_hash_lineage_v1


def _run(argv: list[str], *, env: dict[str, str]) -> dict[str, object]:
    proc = subprocess.run(argv, cwd=REPO_ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return {
        "command": " ".join(argv),
        "return_code": proc.returncode,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
        "status": "PASS" if proc.returncode == 0 else "FAIL",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="repair_aegis_input_contracts_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default="/home/node/constellation_runtime_data/truth")
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    env = os.environ.copy()
    env["TARGET_DAY"] = str(args.day_utc)
    env["AEGIS_TRUTH_ROOT"] = str(args.truth_root)
    root = str(args.truth_root)
    day = str(args.day_utc)
    steps = [
        ["npm", "run", "aegis:context-requirements"],
        ["npm", "run", "aegis:symbol-map"],
        ["npm", "run", "aegis:market-data-demand"],
        ["npm", "run", "aegis:refresh-market-data"],
        ["npm", "run", "aegis:data-registry"],
        ["npm", "run", "aegis:market-data-inputs"],
        ["npm", "run", "aegis:market-data-coverage"],
        ["npm", "run", "aegis:sleeve-input-contracts"],
        ["npm", "run", "aegis:hash-lineage"],
        ["npm", "run", "aegis:sleeve-readiness"],
        [sys.executable, "ops/tools/run_sleeve_evaluation_kernel_v1.py", "--day_utc", day, "--truth_root", root, "--environment", "PAPER", "--readiness-path", f"{root}/reports/aegis_sleeve_readiness_v1/{day}/sleeve_readiness.v1.json"],
        ["npm", "run", "aegis:hash-lineage"],
        ["npm", "run", "aegis:input-contract-reconciliation"],
        ["npm", "run", "aegis:entry-reference-price-certification"],
        ["npm", "run", "aegis:signal-evidence-graph"],
        ["npm", "run", "aegis:candidate-contracts"],
        ["npm", "run", "aegis:signal-death-report"],
        ["npm", "run", "aegis:candidate-diagnostics"],
        ["npm", "run", "aegis:paper:review-queue"],
        ["npm", "run", "aegis:roll-candidate-state"],
        ["npm", "run", "aegis:canonical-operator-state"],
        ["npm", "run", "aegis:audit"],
    ]
    results = []
    nonfatal_blocked_steps = {"ops/tools/run_sleeve_evaluation_kernel_v1.py"}
    nonfatal_market_data_steps = {"npm run aegis:refresh-market-data"}
    nonfatal_audit_steps = {"npm run aegis:audit"}
    for step in steps:
        result = _run(step, env=env)
        command = str(result.get("command") or "")
        nonfatal_blocked = result["return_code"] != 0 and any(marker in command for marker in nonfatal_blocked_steps)
        nonfatal_market_data = result["return_code"] != 0 and any(marker in command for marker in nonfatal_market_data_steps)
        nonfatal_audit = result["return_code"] != 0 and any(marker in command for marker in nonfatal_audit_steps)
        if nonfatal_blocked:
            result["status"] = "BLOCKED_CONTINUED"
            result["nonfatal_contract_blocker"] = True
        if nonfatal_market_data:
            result["status"] = "MARKET_DATA_REFRESH_BLOCKED_CONTINUED"
            result["nonfatal_market_data_blocker"] = True
        if nonfatal_audit:
            result["status"] = "AUDIT_BLOCKED_CONTINUED"
            result["nonfatal_audit_blocker"] = True
        results.append(result)
        print(json.dumps(result, sort_keys=True))
        if result["return_code"] != 0 and not nonfatal_blocked and not nonfatal_market_data and not nonfatal_audit:
            print(json.dumps({"status": "FAILED", "failed_step": result["command"], "results": results}, sort_keys=True))
            return int(result["return_code"] or 1)
    lineage = write_current_hash_lineage_v1(truth_root=Path(root), day_utc=day)
    print(json.dumps({"status": "HASH_LINEAGE_WRITTEN", "path": (lineage.get("paths") or {}).get("json", ""), "lineage_status": lineage.get("status"), "stale_downstream_artifacts": lineage.get("stale_downstream_artifacts") or []}, sort_keys=True))
    has_contract_blocker = any(row.get("nonfatal_contract_blocker") for row in results)
    has_market_data_blocker = any(row.get("nonfatal_market_data_blocker") for row in results)
    has_audit_blocker = any(row.get("nonfatal_audit_blocker") for row in results)
    suffixes = []
    if has_market_data_blocker:
        suffixes.append("MARKET_DATA")
    if has_contract_blocker:
        suffixes.append("CONTRACT")
    if has_audit_blocker:
        suffixes.append("AUDIT")
    final_status = "COMPLETED_WITH_" + "_AND_".join(suffixes) + "_BLOCKERS" if suffixes else "PASS"
    print(json.dumps({"status": final_status, "day_utc": day, "truth_root": root, "steps": len(results), "broker_execution_allowed": False, "autonomous_execution_allowed": False, "trade_advice_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
