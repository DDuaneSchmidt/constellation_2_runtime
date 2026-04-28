from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_ID = "C2_TRADING_DAY_CLOSURE_AUTHORITY_V1"
SCHEMA_VERSION = 1
STATES = {
    "NOT_STARTED",
    "NO_TRADES_CLOSED",
    "DRY_RUN_CLOSED",
    "OPEN_EXECUTIONS",
    "RECONCILIATION_REQUIRED",
    "RECONCILED",
    "CLOSURE_BLOCKED",
    "CLOSURE_GAP",
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _sha256_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _reconciled(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    status = str(payload.get("status") or "").strip().upper()
    semantic = str(payload.get("semantic_status") or "").strip().upper()
    return status in {"PASS", "OK"} and semantic not in {"MATERIALIZED_FAILURE", "BLOCKED_BY_UPSTREAM_PREREQUISITE"}


def _submission_dirs(execution_root: Path, truth_root: Path, day_utc: str) -> list[Path]:
    out: list[Path] = []
    for root in (execution_root, truth_root):
        base = root / "execution_evidence_v1" / "submissions" / day_utc
        if base.exists() and base.is_dir():
            out.extend(path.resolve() for path in base.iterdir() if path.is_dir() and not path.name.startswith("_"))
    seen: set[str] = set()
    uniq: list[Path] = []
    for path in sorted(out):
        if path.name in seen:
            continue
        seen.add(path.name)
        uniq.append(path)
    return uniq


def trading_day_closure_authority_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "trading_day_closure_authority_v1"
        / day_utc
        / "trading_day_closure_authority.v1.json"
    ).resolve()


def evaluate_trading_day_closure_authority_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path | None = None,
    produced_utc: str | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root is not None else truth_root
    lifecycle_path = execution_root / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json"
    lineage_path = truth_root / "reports" / "trade_lineage_graph_v1" / day_utc / "trade_lineage_graph.v1.json"
    submission_index_path = execution_root / "submission_index_v1" / day_utc / "submission_index.v1.json"
    current_head_path = execution_root / "execution_evidence_v1" / "current_head" / day_utc / "current_head.v1.json"
    recon_path = truth_root / "reports" / "execution_reconciliation_v1" / day_utc / "execution_reconciliation.v1.json"
    if not recon_path.exists():
        recon_path = execution_root / "reports" / "execution_reconciliation_v1" / day_utc / "execution_reconciliation.v1.json"
    ledger_path = truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
    paper_authority_path = truth_root / "reports" / "paper_trading_day_authority_v1" / day_utc / "paper_trading_day_authority.v1.json"

    lifecycle = _read_json(lifecycle_path) or {}
    lineage = _read_json(lineage_path) or {}
    recon = _read_json(recon_path) or {}
    lifecycle_rows = {
        str(row.get("submission_id") or "").strip(): row
        for row in lifecycle.get("submissions", [])
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip()
    }
    lineage_rows = {
        str(row.get("submission_id") or "").strip(): row
        for row in lineage.get("lineages", [])
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip()
    }

    unresolved: list[dict[str, Any]] = []
    submissions: list[dict[str, Any]] = []
    closure_gaps: list[dict[str, Any]] = []
    fill_count = 0
    dry_run_count = 0
    transmitted_count = 0
    for subdir in _submission_dirs(execution_root, truth_root, day_utc):
        submission_id = subdir.name
        broker_path = subdir / "broker_submission_record.v2.json"
        attempt_path = subdir / "broker_submit_attempt_v1.json"
        fill_path = execution_root / "fill_ledger_v1" / day_utc / f"{submission_id}.fill_ledger.v1.json"
        if not fill_path.exists():
            fill_path = truth_root / "fill_ledger_v1" / day_utc / f"{submission_id}.fill_ledger.v1.json"
        broker = _read_json(broker_path) or {}
        attempt = _read_json(attempt_path) or {}
        lifecycle_row = lifecycle_rows.get(submission_id) or {}
        lineage_row = lineage_rows.get(submission_id) or {}
        lifecycle_state = str(lifecycle_row.get("current_lifecycle_state") or lineage_row.get("lifecycle_state") or "").strip().upper()
        identity_state = str(lineage_row.get("identity_state") or "").strip().upper()
        dry_run = bool(attempt.get("dry_run") is True or lineage_row.get("dry_run") is True)
        broker_transmit_enabled = bool(lineage_row.get("broker_transmit_enabled") is True)
        if dry_run:
            dry_run_count += 1
        if broker_transmit_enabled:
            transmitted_count += 1
        if fill_path.exists():
            fill_count += 1
        if not broker_path.exists():
            closure_gaps.append({"submission_id": submission_id, "blocker_code": "BROKER_SUBMISSION_RECORD_MISSING", "path": str(broker_path)})
        if lifecycle_state in {"SUBMITTED_PENDING_ACK", "ACKNOWLEDGED_OPEN", "PARTIALLY_FILLED"}:
            unresolved.append({"submission_id": submission_id, "lifecycle_state": lifecycle_state, "blocker_code": "OPEN_EXECUTION"})
        submissions.append(
            {
                "submission_id": submission_id,
                "dry_run": dry_run,
                "broker_transmit_enabled": broker_transmit_enabled,
                "lifecycle_state": lifecycle_state,
                "identity_state": identity_state,
                "fill_ledger_path": str(fill_path),
                "fill_ledger_present": fill_path.exists(),
                "broker_submission_record_path": str(broker_path),
                "broker_submission_record_present": broker_path.exists(),
            }
        )

    reconciliation_complete = _reconciled(recon)
    if closure_gaps:
        state = "CLOSURE_GAP"
    elif not submissions:
        state = "NO_TRADES_CLOSED"
    elif unresolved:
        state = "OPEN_EXECUTIONS"
    elif transmitted_count and fill_count and not reconciliation_complete:
        state = "RECONCILIATION_REQUIRED"
    elif reconciliation_complete:
        state = "RECONCILED"
    elif dry_run_count == len(submissions):
        state = "DRY_RUN_CLOSED"
    elif transmitted_count and not fill_count:
        state = "OPEN_EXECUTIONS"
    else:
        state = "CLOSURE_BLOCKED"

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "produced_utc": produced_utc or _utc_now_iso(),
        "authority_scope": "TRADING_DAY_CLOSURE",
        "status": "PASS" if state in {"NO_TRADES_CLOSED", "DRY_RUN_CLOSED", "RECONCILED"} else "FAIL",
        "closure_state": state,
        "reconciliation_complete": reconciliation_complete,
        "reconciliation_required": bool(transmitted_count and fill_count),
        "submission_count": len(submissions),
        "dry_run_submission_count": dry_run_count,
        "transmitted_submission_count": transmitted_count,
        "unresolved_submissions": unresolved,
        "closure_gaps": closure_gaps,
        "submissions": submissions,
        "first_blocker": str((closure_gaps or unresolved or [{}])[0].get("blocker_code") or ("EXECUTION_RECONCILIATION_MISSING" if state == "RECONCILIATION_REQUIRED" else "")),
        "input_evidence": [
            {"artifact_type": "execution_lifecycle_authority_v1", "path": str(lifecycle_path), "exists": lifecycle_path.exists()},
            {"artifact_type": "trade_lineage_graph_v1", "path": str(lineage_path), "exists": lineage_path.exists()},
            {"artifact_type": "submission_index_v1", "path": str(submission_index_path), "exists": submission_index_path.exists()},
            {"artifact_type": "execution_evidence_v1/current_head", "path": str(current_head_path), "exists": current_head_path.exists()},
            {"artifact_type": "execution_evidence_v1/submissions", "path": str((execution_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()), "exists": (execution_root / "execution_evidence_v1" / "submissions" / day_utc).exists()},
            {"artifact_type": "execution_reconciliation_v1", "path": str(recon_path), "exists": recon_path.exists()},
            {"artifact_type": "paper_session_ledger_v1", "path": str(ledger_path), "exists": ledger_path.exists()},
            {"artifact_type": "paper_trading_day_authority_v1", "path": str(paper_authority_path), "exists": paper_authority_path.exists()},
        ],
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_trading_day_closure_authority_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = trading_day_closure_authority_output_path(truth_root=truth_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(_canonical_bytes(payload) + b"\n")
    return output_path
