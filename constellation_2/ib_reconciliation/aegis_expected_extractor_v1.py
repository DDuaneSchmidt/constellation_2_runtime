from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

from .paths_v1 import validate_day_utc_v1
from .schema_v1 import (
    AEGIS_EXPECTED_ACTIVITY_SCHEMA_VERSION,
    decimal_to_str_v1,
    validate_aegis_expected_activity_v1,
)


class AegisExpectedExtractorError(RuntimeError):
    pass


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AegisExpectedExtractorError(f"JSON_TOP_LEVEL_NOT_OBJECT:{path}")
    return payload


def _unique_sorted(values: Iterable[str]) -> list[str]:
    return sorted({str(v).strip() for v in values if str(v).strip()})


def _first_text(*values: Any, default: str = "") -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return default


def _candidate_execution_roots_v1() -> list[Path]:
    roots: list[Path] = []
    for raw in (
        os.environ.get("C2_TRUTH_ROOT"),
        "/home/node/constellation_runtime_data/truth",
        "/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER",
        "/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/LIVE",
    ):
        text = str(raw or "").strip()
        if not text:
            continue
        path = Path(text).expanduser().resolve()
        if path.exists() and path.is_dir():
            roots.append(path)
    # Preserve order, remove duplicates.
    out: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        if root in seen:
            continue
        seen.add(root)
        out.append(root)
    return out


def _load_submission_index_map(day_utc: str, execution_root: Path, source_artifacts: list[str]) -> dict[str, dict[str, Any]]:
    by_submission: dict[str, dict[str, Any]] = {}
    candidates = [
        (execution_root / "submission_index_v1" / day_utc / "submission_index.v1.json").resolve(),
        (execution_root / "execution_evidence_v1" / "submission_index" / day_utc / "submission_index.v1.json").resolve(),
    ]
    for candidate in candidates:
        if not candidate.exists() or not candidate.is_file():
            continue
        source_artifacts.append(str(candidate))
        try:
            payload = _read_json_object(candidate)
        except Exception:
            continue
        attempts = payload.get("attempts")
        if not isinstance(attempts, list):
            continue
        for row in attempts:
            if not isinstance(row, Mapping):
                continue
            submission_id = _first_text(row.get("submission_id"), row.get("attempt_id"), default="")
            if not submission_id:
                continue
            by_submission.setdefault(submission_id, dict(row))
    return by_submission


def _extract_order_fields_from_plan(plan_payload: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    symbol = _first_text(plan_payload.get("symbol"), default="")
    side = _first_text(plan_payload.get("action"), plan_payload.get("side"), default="")
    quantity: str = ""
    if "qty_shares" in plan_payload:
        try:
            quantity = decimal_to_str_v1(plan_payload.get("qty_shares"), field="expected_orders.quantity")
        except Exception:
            quantity = ""
    elif isinstance(plan_payload.get("risk_proof"), Mapping):
        risk = plan_payload.get("risk_proof")
        if "contracts" in (risk or {}):
            try:
                quantity = decimal_to_str_v1((risk or {}).get("contracts"), field="expected_orders.quantity")
            except Exception:
                quantity = ""
    order_terms = plan_payload.get("order_terms")
    order_type = ""
    if isinstance(order_terms, Mapping):
        order_type = _first_text(order_terms.get("order_type"), default="")
    order_type = _first_text(order_type, plan_payload.get("order_type"), default="UNKNOWN")
    source_intent_id = _first_text(plan_payload.get("source_intent_id"), plan_payload.get("intent_id"), default="UNKNOWN")
    return (symbol.upper(), side.upper(), quantity, order_type.upper(), source_intent_id)


def _find_first_json(paths: list[Path], source_artifacts: list[str]) -> dict[str, Any] | None:
    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        source_artifacts.append(str(path))
        try:
            return _read_json_object(path)
        except Exception:
            continue
    return None


def _extract_fill_from_sources(
    *,
    day_utc: str,
    submission_id: str,
    symbol: str,
    side: str,
    attempt_id: str,
    broker_order_id: str,
    broker_perm_id: str,
    execution_root: Path,
    source_artifacts: list[str],
    fallback_event_payload: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    fill_path = (execution_root / "fill_ledger_v1" / day_utc / f"{submission_id}.fill_ledger.v1.json").resolve()
    if fill_path.exists() and fill_path.is_file():
        source_artifacts.append(str(fill_path))
        try:
            ledger = _read_json_object(fill_path)
        except Exception as exc:
            raise AegisExpectedExtractorError(f"FILL_LEDGER_UNREADABLE:{fill_path}:{exc}") from exc
        filled_qty = ledger.get("filled_qty")
        try:
            quantity = decimal_to_str_v1(filled_qty, field="expected_fills.quantity")
        except Exception:
            quantity = "0"
        if quantity != "0":
            out: dict[str, Any] = {
                "attempt_id": attempt_id,
                "submission_id": submission_id,
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "broker_order_id": broker_order_id,
                "broker_perm_id": broker_perm_id,
            }
            avg_price = str(ledger.get("avg_fill_price_weighted") or "").strip()
            if avg_price and avg_price != "0":
                out["expected_price"] = decimal_to_str_v1(avg_price, field="expected_fills.expected_price")
            return out

    if isinstance(fallback_event_payload, Mapping):
        filled_qty = fallback_event_payload.get("filled_qty")
        try:
            quantity = decimal_to_str_v1(filled_qty, field="expected_fills.quantity")
        except Exception:
            quantity = "0"
        if quantity != "0":
            out = {
                "attempt_id": attempt_id,
                "submission_id": submission_id,
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "broker_order_id": broker_order_id,
                "broker_perm_id": broker_perm_id,
            }
            avg_price = str(fallback_event_payload.get("avg_price") or "").strip()
            if avg_price:
                out["expected_price"] = decimal_to_str_v1(avg_price, field="expected_fills.expected_price")
            return out
    return None


def extract_aegis_expected_activity_v1(day_utc: str) -> dict[str, Any]:
    day = validate_day_utc_v1(day_utc)
    roots = _candidate_execution_roots_v1()
    if not roots:
        raise AegisExpectedExtractorError("NO_EXECUTION_ROOTS_FOUND")

    source_artifacts: list[str] = []
    expected_orders: list[dict[str, Any]] = []
    expected_fills: list[dict[str, Any]] = []
    expected_positions: list[dict[str, Any]] = []

    for execution_root in roots:
        submission_index_by_submission = _load_submission_index_map(day, execution_root, source_artifacts)
        submissions_day_dir = (execution_root / "execution_evidence_v1" / "submissions" / day).resolve()
        if not submissions_day_dir.exists() or not submissions_day_dir.is_dir():
            continue
        for submission_dir in sorted([p for p in submissions_day_dir.iterdir() if p.is_dir()]):
            bsr_path = (submission_dir / "broker_submission_record.v2.json").resolve()
            if not bsr_path.exists() or not bsr_path.is_file():
                continue
            source_artifacts.append(str(bsr_path))
            bsr = _read_json_object(bsr_path)
            submission_id = _first_text(bsr.get("submission_id"), submission_dir.name, default=submission_dir.name)
            idx_row = submission_index_by_submission.get(submission_id, {})

            ack = _find_first_json([(submission_dir / "broker_acknowledgement_v1.json").resolve()], source_artifacts)
            event_record = _find_first_json([(submission_dir / "execution_event_record.v1.json").resolve()], source_artifacts)
            plan = _find_first_json(
                [
                    (submission_dir / "equity_order_plan.v2.json").resolve(),
                    (submission_dir / "equity_order_plan.v1.json").resolve(),
                    (submission_dir / "order_plan.v1.json").resolve(),
                ],
                source_artifacts,
            )
            if not isinstance(plan, Mapping):
                continue

            symbol, side, quantity, order_type, source_intent_id = _extract_order_fields_from_plan(plan)
            if not symbol or not side or not quantity:
                continue

            bsr_ids = bsr.get("broker_ids") if isinstance(bsr.get("broker_ids"), Mapping) else {}
            ack_ids = ack.get("broker_ids") if isinstance((ack or {}).get("broker_ids"), Mapping) else {}

            broker_order_id = _first_text(
                (bsr_ids or {}).get("order_id"),
                (ack_ids or {}).get("order_id"),
                idx_row.get("broker_order_id"),
                default="0",
            )
            broker_perm_id = _first_text(
                (bsr_ids or {}).get("perm_id"),
                (ack_ids or {}).get("perm_id"),
                idx_row.get("broker_perm_id"),
                default="0",
            )
            attempt_id = _first_text(
                bsr.get("attempt_id"),
                idx_row.get("attempt_id"),
                event_record.get("attempt_id") if isinstance(event_record, Mapping) else "",
                submission_id,
            )
            status = _first_text(bsr.get("status"), "UNKNOWN", default="UNKNOWN")

            expected_orders.append(
                {
                    "attempt_id": attempt_id,
                    "submission_id": submission_id,
                    "source_intent_id": source_intent_id,
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "expected_order_type": order_type,
                    "broker_order_id": broker_order_id,
                    "broker_perm_id": broker_perm_id,
                    "status": status,
                }
            )

            expected_fill = _extract_fill_from_sources(
                day_utc=day,
                submission_id=submission_id,
                symbol=symbol,
                side=side,
                attempt_id=attempt_id,
                broker_order_id=broker_order_id,
                broker_perm_id=broker_perm_id,
                execution_root=execution_root,
                source_artifacts=source_artifacts,
                fallback_event_payload=event_record,
            )
            if expected_fill is not None:
                expected_fills.append(expected_fill)

    expected_orders.sort(key=lambda row: (row["attempt_id"], row["submission_id"], row["symbol"], row["side"], row["quantity"]))
    expected_fills.sort(key=lambda row: (row["attempt_id"], row["submission_id"], row["symbol"], row["side"], row["quantity"]))

    payload = {
        "schema_version": AEGIS_EXPECTED_ACTIVITY_SCHEMA_VERSION,
        "day_utc": day,
        "source_artifacts": _unique_sorted(source_artifacts),
        "expected_orders": expected_orders,
        "expected_fills": expected_fills,
        "expected_positions": expected_positions,
    }
    validate_aegis_expected_activity_v1(payload)
    return payload
