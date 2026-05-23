#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_broker_lifecycle_proof_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--lifecycle_mode", choices=["NONE", "SIMULATED", "PAPER"], default="SIMULATED")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    mode = str(args.lifecycle_mode).upper()
    generated_at = _now()
    result = {"NONE": "NOT_CONFIGURED", "SIMULATED": "SIMULATED_CONFIRMED", "PAPER": "FAILED"}[mode]
    payload: dict[str, Any] = {
        "schema_id": "broker_lifecycle_proof",
        "schema_version": "v1",
        "artifact_id": "broker_lifecycle_proof_v1",
        "generated_at_utc": generated_at,
        "generated_at": generated_at,
        "day_utc": day,
        "lifecycle_mode": mode,
        "evaluated": True,
        "broker_connected": False,
        "order_created": mode == "SIMULATED",
        "order_submitted": False,
        "order_acknowledged": False,
        "order_filled": False,
        "order_cancelled": False,
        "account_type": None,
        "broker": None,
        "evidence_paths": [],
        "external_ids_redacted": [],
        "result": result,
        "generated_by_command": f"python3 ops/tools/write_broker_lifecycle_proof_v1.py --truth_root {root} --day_utc {day} --lifecycle_mode {mode}",
        "validation_command": "npm run aegis:audit",
        "evidence_hash": "",
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
        "paper_trade_ready_claim_allowed": False,
        "live_trade_ready_claim_allowed": False,
    }
    payload["evidence_hash"] = _stable_hash({**payload, "evidence_hash": ""})
    path = root / "reports" / "broker_lifecycle_proof_v1" / day / "index" / "broker_lifecycle_proof.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"path": str(path), "lifecycle_mode": mode, "result": result, "paper_trade_ready_claim_allowed": False, "live_trade_ready_claim_allowed": False}, sort_keys=True))
    return 0


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
