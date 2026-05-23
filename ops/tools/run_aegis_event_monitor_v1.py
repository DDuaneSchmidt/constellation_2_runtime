#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_event_monitoring_v1 import run_event_monitor_v1  # noqa: E402
from ops.aegis.event_regime_trigger_evaluator_v1 import (  # noqa: E402
    build_event_regime_trigger_evaluation_v1,
    write_event_regime_trigger_evaluation_v1,
)
from ops.tools.run_aegis_triggered_sleeves_v1 import build_triggered_sleeve_runs_v1, write_triggered_sleeve_runs_v1  # noqa: E402
from ops.aegis.event_append_transaction_v1 import contract_input_hashes_for_paths_v1, emit_artifact_evidence_transaction_v1  # noqa: E402


def _read_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def _today_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _default_snapshot_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "event_market_snapshot_v1"
        / day_utc
        / "event_market_snapshot.v1.json"
    )


def _load_snapshot_or_fail_closed(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {
            "data_snapshot_refs": [str(path)],
            "inputs": {},
            "snapshot_status": "MISSING_INPUT",
            "reason_codes": ["EVENT_MARKET_SNAPSHOT_MISSING"],
        }
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "data_snapshot_refs": [str(path)],
            "inputs": {},
            "snapshot_status": "INVALID_INPUT",
            "reason_codes": [f"EVENT_MARKET_SNAPSHOT_UNREADABLE:{type(exc).__name__}"],
        }
    return payload if isinstance(payload, dict) else {"inputs": {}, "reason_codes": ["EVENT_MARKET_SNAPSHOT_NOT_OBJECT"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_event_monitor_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--market_snapshot_json", default="")
    parser.add_argument("--event_rules_registry", default="")
    parser.add_argument("--monitor_run_id", default="")
    parser.add_argument("--timestamp_utc", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = args.day_utc or _today_utc()
    snapshot_path = Path(args.market_snapshot_json).expanduser().resolve() if args.market_snapshot_json else _default_snapshot_path(truth_root=truth_root, day_utc=day_utc)
    result = run_event_monitor_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        market_snapshot=_load_snapshot_or_fail_closed(snapshot_path),
        event_rules_registry_path=Path(args.event_rules_registry) if args.event_rules_registry else None,
        monitor_run_id=args.monitor_run_id,
        timestamp_utc=args.timestamp_utc,
    )
    trigger_eval_paths = {}
    triggered_run_paths = {}
    triggered_sleeves_enabled = _triggered_sleeves_enabled()
    trigger_eval = build_event_regime_trigger_evaluation_v1(
        truth_root=truth_root,
        repo_root=REPO_ROOT,
        day_utc=day_utc,
        triggered_sleeves_enabled=triggered_sleeves_enabled,
    )
    trigger_eval_paths = write_event_regime_trigger_evaluation_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        payload=trigger_eval,
    )
    event_results = []
    registry_payload = _read_json(result["event_rules_registry_snapshot_path"])
    event_results.extend(emit_artifact_evidence_transaction_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        artifact_path=Path(result["event_rules_registry_snapshot_path"]),
        payload=registry_payload,
        producer_id="ops/tools/run_aegis_event_monitor_v1.py:event_rules_registry",
        producer_version="v1",
        run_id=str(result["monitoring_status"].get("monitor_run_id") or f"event-monitor:{day_utc}"),
        created_at_utc=str(result["monitoring_status"].get("timestamp_utc") or args.timestamp_utc),
        input_hashes=contract_input_hashes_for_paths_v1([snapshot_path, Path(args.event_rules_registry).expanduser().resolve() if args.event_rules_registry else REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_EVENT_RULES_REGISTRY_V1.json"]),
        validation_status="VALID",
    ))
    event_results.extend(emit_artifact_evidence_transaction_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        artifact_path=Path(result["monitoring_status_path"]),
        payload=result["monitoring_status"],
        producer_id="ops/tools/run_aegis_event_monitor_v1.py:event_monitoring_status",
        producer_version="v1",
        run_id=str(result["monitoring_status"].get("monitor_run_id") or f"event-monitor:{day_utc}"),
        created_at_utc=str(result["monitoring_status"].get("timestamp_utc") or args.timestamp_utc),
        input_hashes=contract_input_hashes_for_paths_v1([snapshot_path, result["event_rules_registry_snapshot_path"]], extra={"event_status_detail_hash": str(result["monitoring_status"].get("market_snapshot_freshness_status") or "")}),
        validation_status="VALID" if str(result["monitoring_status"].get("market_snapshot_freshness_status") or "") else "INVALID",
    ))
    if triggered_sleeves_enabled:
        triggered_runs = build_triggered_sleeve_runs_v1(
            truth_root=truth_root,
            repo_root=REPO_ROOT,
            day_utc=day_utc,
            dry_run=False,
            reason="Event monitor integration",
        )
        triggered_run_paths = write_triggered_sleeve_runs_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            payload=triggered_runs,
        )
    print(
        json.dumps(
            {
                "monitoring_status_path": result["monitoring_status_path"],
                "event_ids": [str(row.get("event", {}).get("event_id") or "") for row in event_results],
                "event_awareness_ledger_path": result["event_awareness_ledger_path"],
                "event_rules_registry_snapshot_path": result["event_rules_registry_snapshot_path"],
                "triggered_events": result["monitoring_status"]["triggered_events"],
                "blocked_events": result["monitoring_status"]["blocked_events"],
                "tactical_packets_created": result["monitoring_status"]["tactical_packets_created"],
                "alert_gate_results": result["monitoring_status"]["alert_gate_results"],
                "email_delivery_results": result["monitoring_status"]["email_delivery_results"],
                "event_regime_trigger_evaluation_path": trigger_eval_paths.get("json", ""),
                "event_regime_triggered_sleeves_enabled": triggered_sleeves_enabled,
                "triggered_sleeve_runs_path": triggered_run_paths.get("json", ""),
                "market_snapshot_path": str(snapshot_path),
                "broker_submit_required": False,
                "canonical_eod_state_mutated": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _triggered_sleeves_enabled() -> bool:
    raw = os.environ.get("AEGIS_EVENT_REGIME_TRIGGERED_SLEEVES_ENABLED", "true").strip().lower()
    return raw in {"1", "true", "yes", "on", "enabled"}


if __name__ == "__main__":
    raise SystemExit(main())
