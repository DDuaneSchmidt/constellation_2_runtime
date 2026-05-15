#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import build_outcome_ledger_v1, validate_research_lab_artifact_v1, write_research_lab_artifact_v1  # noqa: E402


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON_OBJECT_REQUIRED:{path}")
    return payload


def _find_receipt(root: Path, receipt_or_trade_id: str) -> tuple[Path, dict[str, Any]]:
    for path in sorted(root.rglob("manual_execution_receipt.v1.json")):
        receipt = _read_json(path)
        if receipt_or_trade_id in {str(receipt.get("receipt_id") or ""), str(receipt.get("recommended_trade_id") or "")}:
            return path, receipt
    raise ValueError(f"RECEIPT_OR_TRADE_NOT_FOUND:{receipt_or_trade_id}")


def _find_trade(root: Path, trade_id: str) -> dict[str, Any]:
    for path in sorted(root.rglob("manual_trade_packet.v1.json")):
        packet = _read_json(path)
        for row in packet.get("trade_candidates", []):
            if isinstance(row, dict) and str(row.get("recommended_trade_id") or "") == trade_id:
                return row
    for path in sorted(root.rglob("event_tactical_packet.v1.json")):
        packet = _read_json(path)
        if str(packet.get("recommended_trade_id") or "") == trade_id:
            return packet
    return {}


def _existing_outcomes(root: Path, day: str) -> list[dict[str, Any]]:
    path = root / "research_lab" / "outcome_ledger_v1" / day / "index" / "outcome_ledger.v1.json"
    if not path.exists():
        return []
    return [row for row in _read_json(path).get("outcomes", []) if isinstance(row, dict)]


def _num(value: Any) -> float | None:
    try:
        return float(str(value).strip().replace("$", "").replace(",", ""))
    except (TypeError, ValueError):
        return None


def _fmt(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".rstrip("0").rstrip(".")


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="record_trade_outcome_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--trade_id", required=True)
    parser.add_argument("--exit_price", required=True)
    parser.add_argument("--exit_timestamp_utc", required=True)
    parser.add_argument("--outcome_status", required=True)
    parser.add_argument("--notes", default="")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    receipt_path, receipt = _find_receipt(root, args.trade_id)
    trade_id = str(receipt.get("recommended_trade_id") or args.trade_id)
    trade = _find_trade(root, trade_id)
    entry = _num(receipt.get("fill_price"))
    exit_price = _num(args.exit_price)
    side = str(receipt.get("actual_side") or trade.get("side") or "BUY").upper()
    ret = None
    if entry is not None and exit_price is not None and entry:
        ret = ((exit_price - entry) / entry) * 100
        if side in {"SELL", "SHORT"}:
            ret = -ret
    day = args.exit_timestamp_utc[:10]
    outcome = {
        "trade_id": trade_id,
        "alert_id": str(receipt.get("alert_id") or ""),
        "source_packet_type": str(receipt.get("source_packet_type") or "EOD_MANUAL_PACKET"),
        "event_id": str(receipt.get("event_id") or trade.get("event_id") or ""),
        "event_type": str(trade.get("event_type") or ""),
        "execution_sensitivity": str(trade.get("execution_sensitivity") or ""),
        "valid_until_respected": bool(receipt.get("fill_before_valid_until", False)),
        "entry_slippage_respected": bool(receipt.get("max_entry_slippage_respected", False)),
        "operator_action_taken": "ENTERED",
        "sleeve_id": str(trade.get("sleeve_id") or ""),
        "hypothesis_id": str(trade.get("source_hypothesis_id") or trade.get("hypothesis_id") or ""),
        "recommended_entry": str(trade.get("entry_reference_price") or ""),
        "actual_entry": str(receipt.get("fill_price") or ""),
        "recommended_stop": str(trade.get("stop_price") or ""),
        "actual_stop": str(receipt.get("stop_price") or ""),
        "exit_price": args.exit_price,
        "return_pct": _fmt(ret),
        "outcome_status": args.outcome_status,
        "operator_deviation": "; ".join(str(item) for item in receipt.get("deviations_from_recommendation", [])),
        "notes": args.notes,
        "sleeve_attribution": str(trade.get("sleeve_id") or ""),
        "edge_overlap_attribution": str(trade.get("edge_overlap_result") or ""),
    }
    existing = [row for row in _existing_outcomes(root, day) if str(row.get("trade_id") or "") != trade_id]
    ledger = build_outcome_ledger_v1(generated_at_utc=_now(), outcome_rows=[*existing, outcome])
    validate_research_lab_artifact_v1(ledger)
    path = write_research_lab_artifact_v1(truth_root=root, day_utc=day, payload=ledger)
    print(json.dumps({"outcome_ledger_path": str(path), "receipt_path": str(receipt_path), "trade_id": trade_id, "return_pct": outcome["return_pct"], "broker_submit_required": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
