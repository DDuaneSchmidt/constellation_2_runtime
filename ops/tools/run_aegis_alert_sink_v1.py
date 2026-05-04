#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.evidence_ledger_v1 import read_events

ALERT_EVENT_PREFIXES = ("PORTAL_",)
ALERT_EVENT_TYPES = {"PROTECTED_MUTATION_BREACH_ATTEMPT", "CODEX_WRONG_REPO_GUARD_TRIGGERED"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-day", required=True)
    ap.add_argument("--truth-root", default="/home/node/constellation_runtime_data/truth")
    ap.add_argument("--environment", choices=["PAPER", "LIVE", "UNKNOWN"], default="UNKNOWN")
    args = ap.parse_args()
    alerts = build_alerts(target_day=args.target_day, truth_root=args.truth_root)
    path = alerts_path(args.truth_root, args.target_day)
    existing = existing_alert_ids(path)
    written = 0
    for alert in alerts:
        if alert["alert_id"] in existing:
            continue
        append_alert(path, alert)
        existing.add(alert["alert_id"])
        written += 1
    print(json.dumps({"alerts_written": written, "alerts_considered": len(alerts), "path": str(path)}, sort_keys=True))
    return 0


def build_alerts(*, target_day: str, truth_root: str | Path) -> list[dict]:
    events = read_events(truth_root=truth_root, target_day=target_day)
    alerts = []
    for event in events:
        if _alertable(event):
            alerts.append(_alert_for_event(event))
    state_path = Path(truth_root) / "reports" / "unified_truth_state_v1" / target_day / "unified_truth_state.v1.json"
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("final_status") == "UNKNOWN":
            alerts.append(_synthetic_alert("unified_truth_unknown", "ERROR", "Unified truth UNKNOWN", "Unified truth resolver returned UNKNOWN.", str(state_path)))
        for contradiction in state.get("contradictions", []):
            alerts.append(_synthetic_alert("resolver_contradiction", "ERROR", "Resolver contradiction", json.dumps(contradiction, sort_keys=True), str(state_path)))
    return _dedupe(alerts)


def _alertable(event: dict) -> bool:
    if event.get("severity") == "CRITICAL" and (event.get("event_type", "").startswith(ALERT_EVENT_PREFIXES) or event.get("event_type") in ALERT_EVENT_TYPES):
        return True
    if event.get("event_type") in {"PORTAL_PROJECTION_STALE", "PROJECTION_STALE"} and event.get("severity") in {"ERROR", "CRITICAL"}:
        return True
    return False


def _alert_for_event(event: dict) -> dict:
    title = event["event_type"].replace("_", " ").title()
    message = event.get("blocker") or event.get("next_action") or title
    return {
        "schema_version": "aegis_alert.v1",
        "alert_id": _hash("|".join([event["event_id"], title, message])),
        "source_event_id": event["event_id"],
        "severity": event["severity"] if event["severity"] in {"WARN", "ERROR", "CRITICAL"} else "WARN",
        "title": title,
        "message": message,
        "created_at_utc": _now(),
        "ack_required": True,
        "status": "OPEN",
    }


def _synthetic_alert(source: str, severity: str, title: str, message: str, evidence_path: str) -> dict:
    source_id = _hash("|".join([source, title, message, evidence_path]))
    return {
        "schema_version": "aegis_alert.v1",
        "alert_id": _hash("|".join([source_id, title, message])),
        "source_event_id": source_id,
        "severity": severity,
        "title": title,
        "message": message,
        "created_at_utc": _now(),
        "ack_required": True,
        "status": "OPEN",
    }


def alerts_path(truth_root: str | Path, day: str) -> Path:
    return Path(truth_root) / "alerts" / "aegis_alerts_v1" / day / "alerts.jsonl"


def existing_alert_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    ids: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                alert = json.loads(line)
            except Exception:
                continue
            if isinstance(alert, dict) and isinstance(alert.get("alert_id"), str):
                ids.add(alert["alert_id"])
    return ids


def append_alert(path: Path, alert: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(alert, sort_keys=True, separators=(",", ":")) + "\n"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)


def _dedupe(alerts: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for alert in alerts:
        if alert["alert_id"] in seen:
            continue
        seen.add(alert["alert_id"])
        out.append(alert)
    return out


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
