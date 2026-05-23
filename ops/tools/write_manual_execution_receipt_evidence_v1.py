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

from ops.aegis.manual_intent_v1 import validate_manual_intent_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_manual_execution_receipt_evidence_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--receipt_type", choices=["NONE_DECLARED"], default="NONE_DECLARED")
    parser.add_argument("--operator_intent_json", required=True)
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    generated_at = _now()
    runtime_eval = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day)
    runtime_hash = str((runtime_eval or {}).get("deterministic_output_hash") or "")
    try:
        operator_intent = json.loads(Path(args.operator_intent_json).expanduser().resolve().read_text(encoding="utf-8"))
    except Exception as exc:
        print(json.dumps({"error": "operator intent missing or malformed", "detail": type(exc).__name__}, sort_keys=True), file=sys.stderr)
        return 2
    intent_reasons = validate_manual_intent_v1(operator_intent, day_utc=day, runtime_evaluation_hash=runtime_hash, generated_at_utc=generated_at)
    if intent_reasons or str(operator_intent.get("operator_intent") or "") != "NONE_DECLARED":
        print(json.dumps({"error": "operator intent does not authorize NONE_DECLARED receipt", "reasons": intent_reasons, "operator_intent": operator_intent.get("operator_intent")}, sort_keys=True), file=sys.stderr)
        return 2
    source_report = _latest_existing(root / "reports" / "sleeve_performance_report_v1" / day, "sleeve_performance_report.v1.json")
    source_summary = _source_summary(source_report)
    if not source_report:
        print(
            json.dumps(
                {
                    "error": "missing sleeve performance report; refusing to create NONE_DECLARED receipt without day evidence",
                    "day_utc": day,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    if source_summary.get("total_executed_trades") not in (0, 0.0):
        print(
            json.dumps(
                {
                    "error": "manual executions are present; NONE_DECLARED receipt requires a fill receipt path instead",
                    "source_report": str(source_report) if source_report else None,
                    "source_summary": source_summary,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    payload: dict[str, Any] = {
        "schema_id": "manual_execution_receipt",
        "schema_version": "v1",
        "artifact_id": "manual_execution_receipt_v1",
        "generated_at_utc": generated_at,
        "generated_at": generated_at,
        "day_utc": day,
        "receipt_type": "NONE_DECLARED",
        "operator_declared_no_manual_execution": True,
        "manual_fill_present": False,
        "fill_details_present": False,
        "source": "operator_declaration",
        "operator_intent_path": str(Path(args.operator_intent_json).expanduser().resolve()),
        "operator_intent_hash": str(operator_intent.get("acknowledgment_hash") or ""),
        "runtime_evaluation_hash": runtime_hash,
        "trade_ids": [],
        "evidence_paths": [str(source_report)] if source_report else [],
        "source_summary": source_summary,
        "evidence_hash": "",
        "result": "NO_MANUAL_EXECUTION_DECLARED",
        "generated_by_command": f"python3 ops/tools/write_manual_execution_receipt_evidence_v1.py --truth_root {root} --day_utc {day} --receipt_type NONE_DECLARED",
        "validation_command": "npm run aegis:audit",
        "manual_trade_execution_proven": False,
        "broker_submission_by_aegis": False,
        "autonomous_execution": False,
        "broker_submit_required": False,
    }
    payload["evidence_hash"] = _stable_hash({**payload, "evidence_hash": ""})
    path = root / "reports" / "manual_execution_receipt_v1" / day / "index" / "manual_execution_receipt.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"path": str(path), "result": payload["result"], "manual_trade_execution_proven": False}, sort_keys=True))
    return 0


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _latest_existing(root: Path, filename: str) -> Path | None:
    if not root.exists():
        return None
    rows = sorted(path for path in root.rglob(filename) if path.is_file())
    return rows[-1] if rows else None


def _source_summary(path: Path | None) -> dict[str, Any]:
    if not path:
        return {"source_report_present": False, "total_executed_trades": 0}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"source_report_present": True, "source_report_read_error": str(exc), "total_executed_trades": 0}
    summary = payload.get("portfolio_summary") if isinstance(payload.get("portfolio_summary"), dict) else {}
    return {
        "source_report_present": True,
        "total_recommended_trades": summary.get("total_recommended_trades", 0),
        "total_executed_trades": summary.get("total_executed_trades", 0),
        "missing_receipt_count": summary.get("missing_receipt_count", 0),
        "missing_outcome_count": summary.get("missing_outcome_count", 0),
    }


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
