#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

VALID_SIDES = {"BUY", "SELL", "SHORT", "COVER"}
VALID_ORDER_TYPES = {"MARKET", "LIMIT", "STOP", "OTHER", "UNKNOWN"}
REQUIRED_FIELDS = ("symbol", "side", "quantity", "price", "trade_date")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="capture_manual_trade_receipt_v1",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""examples:
  npm run aegis:capture-manual-trade -- --help
  npm run aegis:capture-manual-trade -- --json path/to/receipt.json
  npm run aegis:capture-manual-trade -- --symbol SPY --side BUY --quantity 10 --price 500 --trade-date YYYY-MM-DD --operator-attestation true

manual capture is journaling/audit only; Aegis never submits or transmits broker orders.
""",
    )
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--json", "--json-file", dest="json_file", default="")
    parser.add_argument("--symbol", default="")
    parser.add_argument("--side", default="")
    parser.add_argument("--quantity", default="")
    parser.add_argument("--price", default="")
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--entered-by", default="operator")
    parser.add_argument("--broker", default="")
    parser.add_argument("--account-alias", default="")
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--order-type", default="UNKNOWN")
    parser.add_argument("--execution-time", default="")
    parser.add_argument("--fees", default="0")
    parser.add_argument("--notes", default="")
    parser.add_argument("--strategy-or-sleeve", default="")
    parser.add_argument("--related-candidate-id", default="")
    parser.add_argument("--external-order-id-redacted", default="")
    parser.add_argument("--operator-attestation", default="false")
    parser.add_argument("--broker-submission-by-aegis", default="false")
    parser.add_argument("--autonomous-execution", default="false")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print the normalized receipt without writing artifacts.")
    parser.add_argument("--no-write-kernel", action="store_true", help="Do not refresh runtime truth snapshot/history after writing a valid receipt.")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    raw = _load_input(args)
    receipt, errors = validate_manual_trade_receipt_v1(raw)
    if errors:
        print(json.dumps({"ok": False, "validation_status": "INVALID", "validation_errors": errors}, sort_keys=True), file=sys.stderr)
        return 2
    gate = runtime_evaluation_manual_capture_gate_v1(truth_root=root, day_utc=str(receipt["trade_date"]))
    if not bool(gate.get("allowed", False)) and not args.dry_run:
        print(json.dumps({"ok": False, "validation_status": "BLOCKED_BY_RUNTIME_EVALUATION", "reason": gate.get("reason"), "runtime_evaluation_hash": gate.get("runtime_evaluation_hash", ""), "runtime_evaluation_path": gate.get("runtime_evaluation_path", ""), "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True), file=sys.stderr)
        return 3

    if args.dry_run:
        print(
            json.dumps(
                {
                    "ok": True,
                    "dry_run": True,
                    "validation_status": "VALID",
                    "receipt": receipt,
                    "broker_submit_required": False,
                    "autonomous_execution_allowed": False,
                    "command_examples": _command_examples(),
                },
                sort_keys=True,
            )
        )
        return 0

    day = str(receipt["trade_date"])
    receipt_id = str(receipt["receipt_id"])
    receipt_path = root / "manual_trade_receipts" / day / f"{receipt_id}.manual_trade_receipt.v1.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    aggregate_path = write_manual_execution_receipt_aggregate_v1(truth_root=root, day_utc=day)
    kernel_paths: dict[str, str] = {}
    if not args.no_write_kernel:
        from ops.aegis.runtime_truth_kernel_v1 import build_runtime_truth_kernel_v1, write_runtime_truth_kernel_reports_v1

        kernel = build_runtime_truth_kernel_v1(truth_root=root, day_utc=day)
        kernel_paths = write_runtime_truth_kernel_reports_v1(truth_root=root, payload=kernel)

    print(
        json.dumps(
            {
                "ok": True,
                "receipt_id": receipt_id,
                "receipt_path": str(receipt_path),
                "manual_execution_receipt_path": str(aggregate_path),
                "broker_submit_required": False,
                "autonomous_execution_allowed": False,
                "command_examples": _command_examples(),
                "kernel_paths": kernel_paths,
            },
            sort_keys=True,
        )
    )
    return 0


def validate_manual_trade_receipt_v1(raw: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    data = {key: value for key, value in raw.items() if value not in (None, "")}
    errors: list[str] = []
    for field in REQUIRED_FIELDS:
        if not str(data.get(field) or "").strip():
            errors.append(f"{field} is required")
    side = str(data.get("side") or "").strip().upper()
    if side and side not in VALID_SIDES:
        errors.append(f"side must be one of {', '.join(sorted(VALID_SIDES))}")
    order_type = str(data.get("order_type") or "UNKNOWN").strip().upper()
    if order_type not in VALID_ORDER_TYPES:
        errors.append(f"order_type must be one of {', '.join(sorted(VALID_ORDER_TYPES))}")
    quantity = _positive_decimal(data.get("quantity"), "quantity", errors)
    price = _positive_decimal(data.get("price"), "price", errors)
    fees = _non_negative_decimal(data.get("fees", "0"), "fees", errors)
    if not _bool(data.get("operator_attestation", False)):
        errors.append("operator_attestation=true is required")
    if _bool(data.get("broker_submission_by_aegis", False)):
        errors.append("broker_submission_by_aegis=true is forbidden for manual capture")
    if _bool(data.get("autonomous_execution", False)):
        errors.append("autonomous_execution=true is forbidden for manual capture")
    trade_date = str(data.get("trade_date") or "").strip()
    try:
        datetime.strptime(trade_date, "%Y-%m-%d")
    except ValueError:
        if trade_date:
            errors.append("trade_date must be YYYY-MM-DD")
    if errors:
        return {}, errors

    created_at = _now()
    receipt: dict[str, Any] = {
        "schema_id": "manual_trade_receipt",
        "schema_version": "v1",
        "artifact_id": "manual_trade_receipt_v1",
        "receipt_id": "",
        "created_at": created_at,
        "created_at_utc": created_at,
        "trade_date": trade_date,
        "day_utc": trade_date,
        "entered_by": str(data.get("entered_by") or "operator"),
        "source": "MANUAL_OPERATOR_ENTRY",
        "broker": str(data.get("broker") or "") or None,
        "account_alias": str(data.get("account_alias") or "") or None,
        "symbol": str(data["symbol"]).strip().upper(),
        "side": side,
        "quantity": str(quantity),
        "price": str(price),
        "currency": str(data.get("currency") or "USD").strip().upper(),
        "order_type": order_type,
        "execution_time": str(data.get("execution_time") or created_at),
        "fees": str(fees),
        "notes": str(data.get("notes") or ""),
        "strategy_or_sleeve": str(data.get("strategy_or_sleeve") or ""),
        "related_candidate_id": str(data.get("related_candidate_id") or "") or None,
        "external_order_id_redacted": str(data.get("external_order_id_redacted") or "") or None,
        "operator_attestation": True,
        "broker_submission_by_aegis": False,
        "autonomous_execution": False,
        "validation_status": "VALID",
        "validation_errors": [],
        "manual_capture_semantics": "JOURNALING_AUDIT_ONLY",
        "evidence_hash": "",
    }
    receipt["receipt_id"] = f"manual-{_stable_hash({key: receipt[key] for key in receipt if key not in {'receipt_id', 'evidence_hash'}})[:20]}"
    receipt["evidence_hash"] = _stable_hash({**receipt, "evidence_hash": ""})
    return receipt, []


def write_manual_execution_receipt_aggregate_v1(*, truth_root: Path, day_utc: str) -> Path:
    receipts = _valid_receipts(truth_root / "manual_trade_receipts" / day_utc)
    generated_at = _now()
    payload: dict[str, Any] = {
        "schema_id": "manual_execution_receipt",
        "schema_version": "v1",
        "artifact_id": "manual_execution_receipt_v1",
        "generated_at_utc": generated_at,
        "generated_at": generated_at,
        "day_utc": day_utc,
        "receipt_type": "MANUAL_FILL_RECORDED" if receipts else "NONE_DECLARED",
        "operator_declared_no_manual_execution": not receipts,
        "manual_fill_present": bool(receipts),
        "fill_details_present": bool(receipts),
        "source": "manual_entry" if receipts else "operator_declaration",
        "trade_ids": [str(row["receipt_id"]) for row in receipts],
        "manual_trade_receipt_count": len(receipts),
        "last_receipt_id": str(receipts[-1]["receipt_id"]) if receipts else None,
        "validation_status": "VALID",
        "validation_errors": [],
        "evidence_paths": [str(row["_path"]) for row in receipts],
        "evidence_hash": "",
        "result": "MANUAL_FILL_RECEIPT_VALID" if receipts else "NO_MANUAL_EXECUTION_DECLARED",
        "generated_by_command": "python3 ops/tools/capture_manual_trade_receipt_v1.py",
        "validation_command": "npm run aegis:audit",
        "manual_trade_execution_proven": bool(receipts),
        "broker_submission_by_aegis": False,
        "autonomous_execution": False,
        "broker_submit_required": False,
    }
    payload["evidence_hash"] = _stable_hash({**payload, "evidence_hash": ""})
    path = truth_root / "reports" / "manual_execution_receipt_v1" / day_utc / "index" / "manual_execution_receipt.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def runtime_evaluation_manual_capture_gate_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    try:
        from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1, runtime_evaluation_path_v1

        evaluation = read_canonical_runtime_evaluation_v1(truth_root=truth_root, day_utc=day_utc)
        evaluation_path = str(runtime_evaluation_path_v1(truth_root=truth_root, day_utc=day_utc))
    except Exception as exc:  # noqa: BLE001
        return {"allowed": False, "reason": f"RUNTIME_EVALUATION_UNAVAILABLE:{type(exc).__name__}", "runtime_evaluation_hash": "", "runtime_evaluation_path": ""}
    if not evaluation:
        return {"allowed": False, "reason": "RUNTIME_EVALUATION_MISSING", "runtime_evaluation_hash": "", "runtime_evaluation_path": evaluation_path}
    caps = evaluation.get("capabilities") if isinstance(evaluation.get("capabilities"), dict) else {}
    manual = caps.get("MANUAL_TRADE_CAPTURE_ALLOWED") if isinstance(caps.get("MANUAL_TRADE_CAPTURE_ALLOWED"), dict) else {}
    allowed = bool(manual.get("allowed", False))
    return {
        "allowed": allowed,
        "reason": "ALLOWED_BY_RUNTIME_EVALUATION" if allowed else str(manual.get("reason") or "MANUAL_TRADE_CAPTURE_BLOCKED_BY_RUNTIME_EVALUATION"),
        "runtime_evaluation_hash": str(evaluation.get("deterministic_output_hash") or ""),
        "runtime_evaluation_path": evaluation_path,
    }



def runtime_evaluation_allows_manual_capture_v1(*, truth_root: Path, day_utc: str) -> tuple[bool, str]:
    gate = runtime_evaluation_manual_capture_gate_v1(truth_root=truth_root, day_utc=day_utc)
    return bool(gate.get("allowed", False)), str(gate.get("reason") or "")



def _load_input(args: argparse.Namespace) -> dict[str, Any]:
    if args.json_file:
        payload = json.loads(Path(args.json_file).expanduser().read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise SystemExit("JSON input must be an object")
        return _normalize_keys(payload)
    return {
        "symbol": args.symbol,
        "side": args.side,
        "quantity": args.quantity,
        "price": args.price,
        "trade_date": args.trade_date,
        "entered_by": args.entered_by,
        "broker": args.broker,
        "account_alias": args.account_alias,
        "currency": args.currency,
        "order_type": args.order_type,
        "execution_time": args.execution_time,
        "fees": args.fees,
        "notes": args.notes,
        "strategy_or_sleeve": args.strategy_or_sleeve,
        "related_candidate_id": args.related_candidate_id,
        "external_order_id_redacted": args.external_order_id_redacted,
        "operator_attestation": args.operator_attestation,
        "broker_submission_by_aegis": args.broker_submission_by_aegis,
        "autonomous_execution": args.autonomous_execution,
    }


def _command_examples() -> list[str]:
    return [
        "npm run aegis:capture-manual-trade -- --help",
        "npm run aegis:capture-manual-trade -- --json path/to/receipt.json",
        "npm run aegis:capture-manual-trade -- --symbol SPY --side BUY --quantity 10 --price 500 --trade-date YYYY-MM-DD --operator-attestation true",
    ]


def _valid_receipts(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not root.exists():
        return rows
    for path in sorted(root.glob("*.manual_trade_receipt.v1.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict) and payload.get("validation_status") == "VALID":
            rows.append({**payload, "_path": path})
    return rows


def _normalize_keys(payload: dict[str, Any]) -> dict[str, Any]:
    return {str(key).replace("-", "_"): value for key, value in payload.items()}


def _positive_decimal(value: Any, field: str, errors: list[str]) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        errors.append(f"{field} must be numeric")
        return Decimal("0")
    if parsed <= 0:
        errors.append(f"{field} must be greater than zero")
    return parsed


def _non_negative_decimal(value: Any, field: str, errors: list[str]) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        errors.append(f"{field} must be numeric")
        return Decimal("0")
    if parsed < 0:
        errors.append(f"{field} must be non-negative")
    return parsed


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
