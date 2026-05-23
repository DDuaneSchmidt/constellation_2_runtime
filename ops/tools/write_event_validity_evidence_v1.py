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

from ops.aegis.event_append_transaction_v1 import contract_input_hashes_for_paths_v1, emit_artifact_evidence_transaction_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_event_validity_evidence_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--event_packet_json", default="")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    generated_at = _now()
    event_packet_path = Path(args.event_packet_json).expanduser().resolve() if args.event_packet_json else None
    snapshot_path = _latest_existing(root / "reports" / "event_market_snapshot_v1" / day, "event_market_snapshot.v1.json")
    event_packet_present = bool(event_packet_path and event_packet_path.exists())
    validity_status = "VALID" if event_packet_present else "NO_EVENT_PACKET"
    reason = "Event packet exists and was evaluated." if event_packet_present else "No event packet exists for this day; evaluated absence is recorded."
    if event_packet_path and not event_packet_present:
        validity_status = "INVALID"
        reason = f"Event packet path was provided but not found: {event_packet_path}"
    evidence_paths = [str(path) for path in [snapshot_path, event_packet_path] if path]
    payload: dict[str, Any] = {
        "schema_id": "event_validity_gate",
        "schema_version": "v1",
        "artifact_id": "event_validity_gate_v1",
        "generated_at_utc": generated_at,
        "generated_at": generated_at,
        "day_utc": day,
        "source_event_snapshot_path": str(snapshot_path or ""),
        "evaluated": True,
        "event_packet_present": event_packet_present,
        "event_packet_path": str(event_packet_path) if event_packet_present else None,
        "validity_status": validity_status,
        "reason": reason,
        "evidence_paths": evidence_paths,
        "evidence_hash": "",
        "generated_by_command": _command(root, day, event_packet_path),
        "validation_command": "npm run aegis:audit",
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
        "trade_actionable": False if validity_status == "NO_EVENT_PACKET" else event_packet_present,
    }
    payload["evidence_hash"] = _stable_hash({**payload, "evidence_hash": ""})
    path = root / "reports" / "event_validity_gate_v1" / day / "index" / "event_validity_gate.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    events = emit_artifact_evidence_transaction_v1(
        truth_root=root,
        day_utc=day,
        artifact_path=path,
        payload=payload,
        producer_id="ops/tools/write_event_validity_evidence_v1.py",
        producer_version="v1",
        run_id=f"write_event_validity_evidence_v1:{day}",
        created_at_utc=generated_at,
        input_hashes=contract_input_hashes_for_paths_v1([item for item in [snapshot_path, event_packet_path] if item]),
        validation_status="VALID" if validity_status in {"VALID", "NO_EVENT_PACKET"} else "INVALID",
    )
    print(json.dumps({"path": str(path), "validity_status": validity_status, "event_ids": [str(row.get("event", {}).get("event_id") or "") for row in events], "broker_submit_required": False, "trade_actionable": payload["trade_actionable"]}, sort_keys=True))
    return 0


def _latest_existing(root: Path, filename: str) -> Path | None:
    if not root.exists():
        return None
    rows = sorted(path for path in root.rglob(filename) if path.is_file())
    return rows[-1] if rows else None


def _command(root: Path, day: str, event_packet_path: Path | None) -> str:
    command = f"python3 ops/tools/write_event_validity_evidence_v1.py --truth_root {root} --day_utc {day}"
    if event_packet_path:
        command += f" --event_packet_json {event_packet_path}"
    return command


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
