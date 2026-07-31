#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.paper_pnl_report_v1 import build_paper_pnl_report_v1, paper_pnl_report_path_v1, write_paper_pnl_report_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.sleeve_analytics_v1 import build_sleeve_analytics_v1, sleeve_analytics_path_v1  # noqa: E402
from ops.aegis.sleeve_attribution_recovery_v1 import SleeveAttributionRecoveryIndex  # noqa: E402

REPORT_FAMILY = "aegis_sleeve_attribution_reconciliation_v1"
REPORT_FILENAME = "sleeve_attribution_reconciliation.v1.json"


def report_path(*, truth_root: Path, day_utc: str) -> Path:
    return truth_root / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _rows(payload: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    value = payload.get(key)
    return [dict(row) for row in value if isinstance(row, Mapping)] if isinstance(value, list) else []


def build_report(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = truth_root.expanduser().resolve()
    existing = _read_json(paper_pnl_report_path_v1(truth_root=root, day_utc=day_utc))
    before_rows = _rows(existing, "open_positions")
    before_unknown = [row for row in before_rows if str(row.get("sleeve_id") or "UNKNOWN") == "UNKNOWN"]
    rebuilt = build_paper_pnl_report_v1(truth_root=root, day_utc=day_utc)
    after_rows = _rows(rebuilt, "open_positions")
    after_unknown = [row for row in after_rows if str(row.get("sleeve_id") or "UNKNOWN") == "UNKNOWN"]
    index = SleeveAttributionRecoveryIndex(truth_root=root, day_utc=day_utc)
    ledger = _read_json(root / "reports" / "aegis_paper_position_ledger_v1" / day_utc / "paper_position_ledger.v1.json")
    reconciliation = []
    recoverable_unknown = []
    unsafe_symbol_only = []
    for row in _rows(ledger, "open_positions") + _rows(ledger, "closed_positions"):
        current = str(row.get("sleeve_id") or "UNKNOWN")
        if current != "UNKNOWN":
            continue
        recovered = index.recover(row)
        entry = {
            "position_id": recovered.get("position_id"),
            "symbol": recovered.get("symbol"),
            "entry_receipt_id": recovered.get("entry_receipt_id"),
            "candidate_id": recovered.get("candidate_id"),
            "candidate_contract_id": recovered.get("candidate_contract_id"),
            "paper_session_id": recovered.get("paper_session_id"),
            "entry_timestamp": recovered.get("entry_timestamp"),
            "current_sleeve_id": current,
            "recovery_source_attempted": recovered.get("attempted_recovery_source"),
            "recovered_sleeve_id": recovered.get("recovered_sleeve_id"),
            "blocker_reason": recovered.get("blocker_reason"),
            "symbol_only_match_used": bool(recovered.get("symbol_only_match_used")),
            "attempts": recovered.get("attempts") or [],
        }
        reconciliation.append(entry)
        if entry["recovered_sleeve_id"] != "UNKNOWN" and entry["position_id"] in {r.get("position_id") for r in after_unknown}:
            recoverable_unknown.append(entry)
        if entry["symbol_only_match_used"]:
            unsafe_symbol_only.append(entry)
    analytics_artifact = _read_json(sleeve_analytics_path_v1(truth_root=root, day_utc=day_utc))
    pnl_sleeves = {str(row.get("sleeve_id") or "UNKNOWN"): int(row.get("open_position_count") or 0) for row in rebuilt.get("pnl_by_sleeve") or [] if isinstance(row, Mapping)}
    if analytics_artifact:
        analytics = build_sleeve_analytics_v1(truth_root=root, day_utc=day_utc)
        analytics_sleeves = {str(row.get("sleeve_id") or "UNKNOWN"): int(row.get("open_positions") or 0) for row in analytics.get("sleeves") or [] if isinstance(row, Mapping)}
        disagree = pnl_sleeves != analytics_sleeves
    else:
        analytics_sleeves = dict(pnl_sleeves)
        disagree = False
    failures = []
    if recoverable_unknown:
        failures.append("UNKNOWN_REMAINS_BUT_LINEAGE_CAN_RECOVER")
    if unsafe_symbol_only:
        failures.append("UNSAFE_SYMBOL_ONLY_RECOVERY_USED")
    if disagree:
        failures.append("PERFORMANCE_AND_SLEEVE_ANALYTICS_DISAGREE")
    return {
        "schema_id": "aegis_sleeve_attribution_reconciliation",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "ok": not failures,
        "before_unknown_open_position_count": len(before_unknown),
        "after_unknown_open_position_count": len(after_unknown),
        "reconciliation_count": len(reconciliation),
        "recoverable_unknown_count": len(recoverable_unknown),
        "unsafe_symbol_only_recovery_count": len(unsafe_symbol_only),
        "performance_sleeve_counts": pnl_sleeves,
        "sleeve_analytics_counts": analytics_sleeves,
        "failures": failures,
        "reconciliation": reconciliation,
        "unrecoverable": [row for row in reconciliation if row.get("recovered_sleeve_id") == "UNKNOWN"],
        "safety": {
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_live_trading_allowed": False,
        },
    }


def write_report(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    path = report_path(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_sleeve_attribution_self_check_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--write-pnl", action="store_true", help="write repaired paper PnL report before checking")
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    payload = build_report(truth_root=root, day_utc=day)
    if args.write_pnl:
        write_paper_pnl_report_v1(truth_root=root, day_utc=day)
    path = write_report(truth_root=root, day_utc=day, payload=payload)
    print("AEGIS SLEEVE ATTRIBUTION SELF CHECK v1")
    print(f"day_utc: {day}")
    print(f"before_unknown_open_positions: {payload['before_unknown_open_position_count']}")
    print(f"after_unknown_open_positions: {payload['after_unknown_open_position_count']}")
    print(f"unrecoverable: {len(payload['unrecoverable'])}")
    print(f"path: {path}")
    print(json.dumps({k: payload[k] for k in ['ok','before_unknown_open_position_count','after_unknown_open_position_count','recoverable_unknown_count','unsafe_symbol_only_recovery_count','failures','safety']}, sort_keys=True))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
