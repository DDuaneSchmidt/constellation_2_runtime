from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.paper_session_ledger_v1 import resolve_scheduled_paper_session_v1

REPORT_FAMILY = "aegis_signal_evidence_boundary_v1"
REPORT_FILENAME = "signal_evidence_boundary.v1.json"
SCHEMA_ID = "aegis_signal_evidence_boundary"
SCHEMA_VERSION = "v1"

SIGNAL_EVIDENCE_PRESENT = "SIGNAL_EVIDENCE_PRESENT"
SIGNAL_EVIDENCE_REJECTED_INTENT = "SIGNAL_EVIDENCE_REJECTED_INTENT"
SIGNAL_EVIDENCE_ARBITRATION_ONLY = "SIGNAL_EVIDENCE_ARBITRATION_ONLY"
SIGNAL_EVIDENCE_REVIEW_QUEUE_ONLY = "SIGNAL_EVIDENCE_REVIEW_QUEUE_ONLY"
SIGNAL_EVIDENCE_BLOCKED_MISSING_OUTPUT_INTENT = "SIGNAL_EVIDENCE_BLOCKED_MISSING_OUTPUT_INTENT"
NOT_SIGNAL_ELIGIBLE = "NOT_SIGNAL_ELIGIBLE"
SIGNAL_BOUNDARY_VIOLATION = "SIGNAL_BOUNDARY_VIOLATION"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def signal_evidence_boundary_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _rows(payload: Mapping[str, Any], *keys: str) -> list[Mapping[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, Mapping)]
    return []


def _text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _symbol(row: Mapping[str, Any]) -> str:
    return _text(row.get("symbol"), row.get("ticker"), row.get("symbol_or_pair")).upper()


def _candidate_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("candidate_id"), row.get("candidate_contract_id"), row.get("trade_candidate_id"), row.get("id"), row.get("intent_id"))


def _candidate_contract_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("candidate_contract_id"), row.get("candidate_id"), row.get("contract_id"))


def _raw_signal_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("raw_signal_id"), row.get("intent_id"), row.get("source_signal_id"), row.get("intent_hash"))


def _index_rows(rows: list[Mapping[str, Any]]) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    by_candidate: dict[str, Mapping[str, Any]] = {}
    by_contract: dict[str, Mapping[str, Any]] = {}
    by_symbol: dict[str, Mapping[str, Any]] = {}
    by_raw_signal: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        cid = _candidate_id(row)
        ccid = _candidate_contract_id(row)
        sym = _symbol(row)
        raw = _raw_signal_id(row)
        if cid:
            by_candidate[cid] = row
        if ccid:
            by_contract[ccid] = row
        if sym:
            by_symbol[sym] = row
        if raw:
            by_raw_signal[raw] = row
    return by_candidate, by_contract, by_symbol, by_raw_signal


def _lookup(row: Mapping[str, Any], indexes: tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]) -> Mapping[str, Any]:
    by_candidate, by_contract, by_symbol, by_raw_signal = indexes
    return (
        by_candidate.get(_candidate_id(row))
        or by_contract.get(_candidate_contract_id(row))
        or by_raw_signal.get(_raw_signal_id(row))
        or by_symbol.get(_symbol(row))
        or {}
    )


def _iter_sleeve_payloads(root: Path, day_utc: str) -> list[Mapping[str, Any]]:
    base = root / "reports" / "sleeve_evaluation_kernel_v1" / day_utc
    payloads: list[Mapping[str, Any]] = []
    if not base.exists():
        return payloads
    # Match the signal-evidence producer boundary: day-level sleeve directories,
    # not every historical run snapshot nested under the day.
    for child in sorted(base.iterdir()):
        if not child.is_dir():
            continue
        path = child / "sleeve_evaluation.v1.json"
        payload = _read_json(path)
        if payload:
            payloads.append(payload)
    return payloads


def _sleeve_output_intents(root: Path, day_utc: str) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for payload in _iter_sleeve_payloads(root, day_utc):
        batch = payload.get("exposure_intent_batch") if isinstance(payload.get("exposure_intent_batch"), Mapping) else {}
        for row in _rows(batch, "output_intents") or _rows(payload, "output_intents"):
            rows.append(row)
    return rows


def _sleeve_rejected_intents(root: Path, day_utc: str) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for payload in _iter_sleeve_payloads(root, day_utc):
        for row in _rows(payload, "rejected_intents", "stale_artifacts"):
            rows.append(row)
    return rows


def _classify(*, signal: Mapping[str, Any], output: Mapping[str, Any], rejected: Mapping[str, Any], arbitration: Mapping[str, Any], raw_candidate: Mapping[str, Any], queue: Mapping[str, Any]) -> tuple[str, str]:
    if signal:
        return SIGNAL_EVIDENCE_PRESENT, "Candidate is present in signal_evidence_graph_v1."
    if output:
        return SIGNAL_BOUNDARY_VIOLATION, "Candidate is present in sleeve output_intents but absent from signal_evidence_graph_v1."
    if rejected:
        return SIGNAL_EVIDENCE_REJECTED_INTENT, "Candidate is present in sleeve rejected_intents/stale_artifacts but absent from final output_intents."
    if arbitration or raw_candidate:
        return SIGNAL_EVIDENCE_ARBITRATION_ONLY, "Candidate is present in arbitration/raw candidate lineage but absent from final output_intents."
    if queue:
        return SIGNAL_EVIDENCE_REVIEW_QUEUE_ONLY, "Candidate is present in paper review queue but absent from signal evidence and output_intents."
    return SIGNAL_BOUNDARY_VIOLATION, "Current-session candidate has no signal evidence, output intent, rejected-intent, arbitration, or review-queue lineage."


def build_signal_evidence_boundary_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated = generated_at_utc or _now_iso()

    lifecycle_path = root / "reports" / "aegis_candidate_lifecycle_projection_v1" / day / "candidate_lifecycle_projection.v1.json"
    signal_path = root / "reports" / "aegis_signal_evidence_graph_v1" / day / "signal_evidence_graph.v1.json"
    arbitration_path = root / "reports" / "intent_arbitration_v1" / day / "intent_arbitration.v1.json"
    queue_path = root / "reports" / "aegis_paper_review_queue_v1" / day / "paper_review_queue.v1.json"

    lifecycle = _read_json(lifecycle_path)
    signal_graph = _read_json(signal_path)
    arbitration = _read_json(arbitration_path)
    queue = _read_json(queue_path)
    official_session = resolve_scheduled_paper_session_v1(truth_root=root, day_utc=day)

    current_candidates = _rows(lifecycle, "current_session_candidates")
    signal_rows = _rows(signal_graph, "signals")
    output_rows = _sleeve_output_intents(root, day)
    rejected_rows = _sleeve_rejected_intents(root, day)
    arbitration_rows = _rows(arbitration, "portfolio_ranking")
    raw_candidate_rows = _rows(arbitration, "raw_candidate_intents", "candidate_intents")
    queue_rows = _rows(queue, "rows")

    signal_indexes = _index_rows(signal_rows)
    output_indexes = _index_rows(output_rows)
    rejected_indexes = _index_rows(rejected_rows)
    arbitration_indexes = _index_rows(arbitration_rows)
    raw_candidate_indexes = _index_rows(raw_candidate_rows)
    queue_indexes = _index_rows(queue_rows)

    rows_out: list[dict[str, Any]] = []
    for candidate in current_candidates:
        signal = _lookup(candidate, signal_indexes)
        output = _lookup(candidate, output_indexes)
        rejected = _lookup(candidate, rejected_indexes)
        arbitration_row = _lookup(candidate, arbitration_indexes)
        raw_candidate = _lookup(candidate, raw_candidate_indexes)
        queue_row = _lookup(candidate, queue_indexes)
        boundary_status, boundary_reason = _classify(
            signal=signal,
            output=output,
            rejected=rejected,
            arbitration=arbitration_row,
            raw_candidate=raw_candidate,
            queue=queue_row,
        )
        rows_out.append(
            {
                "symbol": _symbol(candidate),
                "candidate_id": _candidate_id(candidate),
                "candidate_contract_id": _candidate_contract_id(candidate),
                "paper_session_id": _text(candidate.get("paper_session_id"), lifecycle.get("paper_session_id"), official_session.get("paper_session_id")),
                "in_current_session_candidates": True,
                "in_signal_evidence_graph": bool(signal),
                "in_output_intents": bool(output),
                "in_rejected_intents": bool(rejected),
                "in_arbitration_ranking": bool(arbitration_row),
                "in_raw_candidate_intents": bool(raw_candidate),
                "in_paper_review_queue": bool(queue_row),
                "raw_signal_id": _text(_raw_signal_id(signal), _raw_signal_id(output), _raw_signal_id(rejected), _raw_signal_id(arbitration_row), _raw_signal_id(queue_row), _raw_signal_id(candidate)),
                "boundary_status": boundary_status,
                "boundary_reason": boundary_reason,
            }
        )

    silent_omission_count = sum(1 for row in rows_out if row["boundary_status"] == SIGNAL_BOUNDARY_VIOLATION)
    review_or_arbitration_only_count = sum(1 for row in rows_out if row["boundary_status"] in {SIGNAL_EVIDENCE_ARBITRATION_ONLY, SIGNAL_EVIDENCE_REVIEW_QUEUE_ONLY})
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": generated,
        "status": SIGNAL_BOUNDARY_VIOLATION if silent_omission_count else "PASS",
        "paper_session_id": _text(lifecycle.get("paper_session_id"), queue.get("paper_session_id"), official_session.get("paper_session_id")),
        "current_session_candidate_count": len(rows_out),
        "signal_evidence_graph_signal_count": len(signal_rows),
        "output_intent_count": len({_raw_signal_id(row) or _symbol(row) for row in output_rows if _raw_signal_id(row) or _symbol(row)}),
        "rejected_intent_count": sum(1 for row in rows_out if row["in_rejected_intents"]),
        "arbitration_only_count": sum(1 for row in rows_out if row["boundary_status"] == SIGNAL_EVIDENCE_ARBITRATION_ONLY),
        "review_queue_only_count": sum(1 for row in rows_out if row["boundary_status"] == SIGNAL_EVIDENCE_REVIEW_QUEUE_ONLY),
        "review_arbitration_only_count": review_or_arbitration_only_count,
        "signal_evidence_present_count": sum(1 for row in rows_out if row["boundary_status"] == SIGNAL_EVIDENCE_PRESENT),
        "blocked_missing_output_intent_count": sum(1 for row in rows_out if not row["in_output_intents"] and row["boundary_status"] != SIGNAL_EVIDENCE_PRESENT and row["boundary_status"] != SIGNAL_BOUNDARY_VIOLATION),
        "silent_omission_count": silent_omission_count,
        "source_artifacts": {
            "candidate_lifecycle_projection_v1": str(lifecycle_path),
            "signal_evidence_graph_v1": str(signal_path),
            "intent_arbitration_v1": str(arbitration_path),
            "aegis_paper_review_queue_v1": str(queue_path),
            "sleeve_evaluation_kernel_v1": str(root / "reports" / "sleeve_evaluation_kernel_v1" / day),
        },
        "boundary_rows": rows_out,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def write_signal_evidence_boundary_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> Path:
    return write_json_v1(signal_evidence_boundary_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payload))


def build_and_write_signal_evidence_boundary_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> tuple[dict[str, Any], Path]:
    payload = build_signal_evidence_boundary_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    path = write_signal_evidence_boundary_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    return {**payload, "artifact_path": str(path)}, path


def render_signal_evidence_boundary_v1(payload: Mapping[str, Any]) -> str:
    session = str(payload.get("paper_session_id") or "")
    lines = [
        f"Signal evidence boundary for {session}:",
        f"- current-session candidates: {payload.get('current_session_candidate_count', 0)}",
        f"- signal evidence graph signals: {payload.get('signal_evidence_graph_signal_count', 0)}",
        f"- output intents: {payload.get('output_intent_count', 0)}",
        f"- rejected-intent lineage: {payload.get('rejected_intent_count', 0)}",
        f"- review/arbitration-only: {payload.get('review_arbitration_only_count', 0)}",
        f"- signal evidence present: {payload.get('signal_evidence_present_count', 0)}",
        f"- blocked missing output intent: {payload.get('blocked_missing_output_intent_count', 0)}",
        f"- silent omissions: {payload.get('silent_omission_count', 0)}",
    ]
    if int(payload.get("silent_omission_count") or 0) > 0:
        lines.append(f"SIGNAL BOUNDARY VIOLATION: {payload.get('silent_omission_count')} current-session candidates had no explicit signal lineage outcome.")
    return "\n".join(lines) + "\n"
