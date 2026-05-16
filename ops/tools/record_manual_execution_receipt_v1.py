#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_manual_execution_receipt_v1,
    validate_research_lab_artifact_v1,
    write_research_lab_artifact_v1,
)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON_OBJECT_REQUIRED:{path}")
    return payload


def _find_source_packet(root: Path, source_packet_id: str) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    for path in sorted(root.rglob("manual_trade_packet.v1.json")):
        packet = _read_json(path)
        if str(packet.get("packet_id") or "") == source_packet_id:
            return path, packet, {}
        for row in packet.get("trade_candidates", []):
            if isinstance(row, dict) and str(row.get("recommended_trade_id") or "") == source_packet_id:
                return path, packet, row
    for path in sorted(root.rglob("event_tactical_packet.v1.json")):
        packet = _read_json(path)
        if str(packet.get("recommended_trade_id") or packet.get("event_id") or "") == source_packet_id:
            return path, packet, packet
    raise ValueError(f"SOURCE_PACKET_NOT_FOUND:{source_packet_id}")


def _day_from_source(packet: dict[str, Any], fill_timestamp: str) -> str:
    return str(packet.get("date") or packet.get("day_utc") or fill_timestamp[:10])


def _bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="record_manual_execution_receipt_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--source_packet_id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", required=True)
    parser.add_argument("--quantity", required=True, type=int)
    parser.add_argument("--fill_price", required=True)
    parser.add_argument("--fill_timestamp_utc", required=True)
    parser.add_argument("--stop_entered", required=True)
    parser.add_argument("--stop_price", required=True)
    parser.add_argument("--notes", default="")
    parser.add_argument("--order_type", default="MANUAL")
    parser.add_argument("--receipt_id", default="")
    parser.add_argument("--operator_override_reason", default="")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    source_path, packet, trade = _find_source_packet(root, args.source_packet_id)
    if trade and trade.get("actionable") is False:
        blockers = ",".join(str(item) for item in trade.get("do_not_trade_blockers", []))
        raise SystemExit(f"FAIL: SOURCE_TRADE_NOT_ACTIONABLE {blockers}")
    recommended_trade_id = str(trade.get("recommended_trade_id") or packet.get("recommended_trade_id") or args.source_packet_id)
    source_type = "EVENT_TACTICAL_PACKET" if packet.get("schema_id") == "event_tactical_packet" else "EOD_MANUAL_PACKET"
    recommended_entry = _num(trade.get("entry_reference_price") or packet.get("entry_reference_price"))
    fill = _num(args.fill_price)
    deviation = "" if recommended_entry is None or fill is None else f"{fill - recommended_entry:.4f}".rstrip("0").rstrip(".")
    suggested_quantity = int(trade.get("suggested_quantity") or packet.get("suggested_quantity") or 0)
    expected_risk = str(trade.get("max_loss_if_stopped") or trade.get("allowed_dollar_risk") or "")
    actual_risk = _actual_risk(fill_price=args.fill_price, stop_price=args.stop_price, quantity=args.quantity)
    sizing_quality = _sizing_quality(
        suggested_quantity=suggested_quantity,
        actual_quantity=args.quantity,
        expected_risk=expected_risk,
        actual_risk=actual_risk,
    )
    deviations = [] if not deviation else [f"entry_deviation={deviation}"]
    if suggested_quantity and suggested_quantity != args.quantity:
        deviations.append(f"quantity_override=suggested:{suggested_quantity}:actual:{args.quantity}")
    receipt = build_manual_execution_receipt_v1(
        receipt_id=args.receipt_id or f"receipt:{recommended_trade_id}:{args.fill_timestamp_utc}",
        recommended_trade_id=recommended_trade_id,
        actual_symbol=args.symbol,
        actual_side=args.side,
        actual_quantity=args.quantity,
        order_type=args.order_type,
        fill_price=args.fill_price,
        fill_timestamp=args.fill_timestamp_utc,
        stop_order_entered=_bool(args.stop_entered),
        stop_price=args.stop_price,
        operator_notes=args.notes,
        deviations_from_recommendation=deviations,
        source_packet_type=source_type,
        source_packet_id=args.source_packet_id,
        event_id=str(packet.get("event_id") or ""),
        event_run_id=str(packet.get("event_run_id") or ""),
        fill_timestamp_utc=args.fill_timestamp_utc,
        deviation_from_entry_reference_price=deviation,
        fill_before_valid_until=_fill_before_valid_until(trade or packet, args.fill_timestamp_utc),
        max_entry_slippage_respected=True,
        suggested_quantity=suggested_quantity,
        expected_risk=expected_risk,
        actual_risk=actual_risk,
        operator_override_reason=args.operator_override_reason,
        sizing_quality=sizing_quality,
    )
    validate_research_lab_artifact_v1(receipt)
    path = write_research_lab_artifact_v1(truth_root=root, day_utc=_day_from_source(packet, args.fill_timestamp_utc), payload=receipt)
    print(json.dumps({"receipt_path": str(path), "source_path": str(source_path), "receipt_id": receipt["receipt_id"], "broker_submit_required": False}, sort_keys=True))
    return 0


def _num(value: Any) -> float | None:
    try:
        return float(str(value).strip().replace("$", "").replace(",", ""))
    except (TypeError, ValueError):
        return None


def _fill_before_valid_until(packet: dict[str, Any], fill_timestamp: str) -> bool:
    valid_until = str(packet.get("valid_until") or "")
    if not valid_until:
        return False
    try:
        return datetime.fromisoformat(fill_timestamp.replace("Z", "+00:00")) <= datetime.fromisoformat(valid_until.replace("Z", "+00:00"))
    except ValueError:
        return False


def _decimal(value: Any) -> Decimal | None:
    try:
        text = str(value).strip().replace("$", "").replace(",", "")
        return Decimal(text) if text else None
    except (InvalidOperation, TypeError):
        return None


def _actual_risk(*, fill_price: str, stop_price: str, quantity: int) -> str:
    fill = _decimal(fill_price)
    stop = _decimal(stop_price)
    if fill is None or stop is None:
        return ""
    return _fmt(abs(fill - stop) * Decimal(int(quantity)))


def _sizing_quality(*, suggested_quantity: int, actual_quantity: int, expected_risk: str, actual_risk: str) -> str:
    if suggested_quantity <= 0:
        return "UNKNOWN"
    if suggested_quantity != actual_quantity:
        return "OVERRIDDEN"
    expected = _decimal(expected_risk)
    actual = _decimal(actual_risk)
    if expected is not None and actual is not None and actual > expected:
        return "RISK_ABOVE_EXPECTED"
    return "MATCHED"


def _fmt(value: Decimal) -> str:
    text = format(value.quantize(Decimal("0.01")).normalize(), "f")
    return "0" if text == "-0" else text


if __name__ == "__main__":
    raise SystemExit(main())
