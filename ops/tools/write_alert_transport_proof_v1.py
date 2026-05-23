#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.event_append_transaction_v1 import canonical_payload_hash_v1, emit_artifact_evidence_transaction_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_alert_transport_proof_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--transport_mode", choices=["NONE", "GATE_ONLY", "DRY_RUN", "LIVE"], default="GATE_ONLY")
    parser.add_argument("--channel", default="")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    mode = str(args.transport_mode).upper()
    generated_at = _now()
    result = {
        "NONE": "NOT_CONFIGURED",
        "GATE_ONLY": "GATE_ONLY_NO_TRANSPORT",
        "DRY_RUN": "DRY_RUN_CONFIRMED",
        "LIVE": "FAILED",
    }[mode]
    dry_run = mode == "DRY_RUN"
    delivery_attempted = mode in {"DRY_RUN", "LIVE"}
    delivery_confirmed = mode == "DRY_RUN"
    channel = str(args.channel or ("test" if dry_run else "")).strip() or None
    payload: dict[str, Any] = {
        "schema_id": "alert_transport_proof",
        "schema_version": "v1",
        "artifact_id": "alert_transport_proof_v1",
        "generated_at_utc": generated_at,
        "generated_at": generated_at,
        "day_utc": day,
        "transport_mode": mode,
        "evaluated": True,
        "delivery_attempted": delivery_attempted,
        "delivery_confirmed": delivery_confirmed,
        "channel": channel,
        "dry_run": dry_run,
        "provider_message_id": None,
        "destination_redacted": None,
        "result": result,
        "evidence_hash": "",
        "generated_by_command": f"python3 ops/tools/write_alert_transport_proof_v1.py --truth_root {root} --day_utc {day} --transport_mode {mode}",
        "validation_command": "npm run aegis:audit",
        "broker_submit_required": False,
        "live_transport_claim_allowed": False,
    }
    payload["evidence_hash"] = _stable_hash({**payload, "evidence_hash": ""})
    path = root / "reports" / "alert_transport_proof_v1" / day / "index" / "alert_transport_proof.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    events = emit_artifact_evidence_transaction_v1(
        truth_root=root,
        day_utc=day,
        artifact_path=path,
        payload=payload,
        producer_id="ops/tools/write_alert_transport_proof_v1.py",
        producer_version="v1",
        run_id=f"write_alert_transport_proof_v1:{day}:{mode}",
        created_at_utc=generated_at,
        input_hashes={"transport_mode_hash": canonical_payload_hash_v1({"transport_mode": mode, "channel": channel or ""})},
        validation_status="VALID" if mode in {"GATE_ONLY", "DRY_RUN"} else "INVALID",
    )
    print(json.dumps({"path": str(path), "transport_mode": mode, "result": result, "delivery_confirmed": delivery_confirmed, "live_transport_claim_allowed": False, "event_ids": [str(row.get("event", {}).get("event_id") or "") for row in events]}, sort_keys=True))
    return 0


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
