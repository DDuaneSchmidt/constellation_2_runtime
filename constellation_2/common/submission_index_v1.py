from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.execution_evidence_current_head_v1 import (
    evaluate_execution_evidence_current_head_v1,
    write_execution_evidence_current_head_v1,
)
from constellation_2.common.paper_submit_mode_status_v1 import classify_paper_submit_mode_status_v1


SCHEMA_VERSION = "submission_index.v1"


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"INVALID_JSON_OBJECT:{path}")
    return payload


def _stream_records_for_submission(stream_day_dir: Path, submission_id: str) -> list[Path]:
    out: list[Path] = []
    stream_day_dir = Path(stream_day_dir).resolve()
    replay_day_dir = (stream_day_dir.parent / "replays" / stream_day_dir.name).resolve()

    search_roots: list[Path] = []
    if stream_day_dir.exists() and stream_day_dir.is_dir():
        search_roots.append(stream_day_dir)
    if replay_day_dir.exists() and replay_day_dir.is_dir():
        replay_dirs = sorted([p for p in replay_day_dir.iterdir() if p.is_dir() and not p.name.startswith("_")], reverse=True)
        search_roots.extend(replay_dirs)

    for root in search_roots:
        for candidate in sorted(root.glob("*.execution_event_stream_record.v1.json")):
            try:
                payload = _read_json(candidate)
            except Exception:
                continue
            if str(payload.get("submission_id") or "").strip() == submission_id:
                out.append(candidate.resolve())
    return out


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _submission_attempts(submissions_day_dir: Path) -> list[Path]:
    if not submissions_day_dir.exists() or not submissions_day_dir.is_dir():
        return []
    return sorted([p for p in submissions_day_dir.iterdir() if p.is_dir() and not p.name.startswith("_")])


def _blocking_entry(code: str, path: str, detail: str) -> dict[str, str]:
    return {"code": code, "path": path, "detail": detail}


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _resolve_submission_broker_ids(
    *,
    submission_dir: Path,
    record_payload: dict[str, Any],
    execution_event_payload: dict[str, Any],
) -> tuple[int | None, int | None, str, str, bool]:
    broker_ids = record_payload.get("broker_ids") if isinstance(record_payload.get("broker_ids"), dict) else {}
    perm_field_present = "perm_id" in broker_ids

    record_order = _coerce_int(broker_ids.get("order_id"))
    record_perm = _coerce_int(broker_ids.get("perm_id"))

    supplemental_rows: list[tuple[str, int | None, int | None]] = []
    for source_name, filename in (
        ("BROKER_ACKNOWLEDGEMENT", "broker_acknowledgement_v1.json"),
        ("BROKER_ORDER_OUTCOME", "broker_order_outcome_v1.json"),
    ):
        artifact_path = (submission_dir / filename).resolve()
        if not artifact_path.exists() or not artifact_path.is_file():
            continue
        try:
            payload = _read_json(artifact_path)
        except Exception:
            continue
        artifact_ids = payload.get("broker_ids") if isinstance(payload.get("broker_ids"), dict) else {}
        supplemental_rows.append(
            (
                source_name,
                _coerce_int(artifact_ids.get("order_id")),
                _coerce_int(artifact_ids.get("perm_id")),
            )
        )

    supplemental_rows.append(
        (
            "EXECUTION_EVENT_RECORD",
            _coerce_int(execution_event_payload.get("broker_order_id")),
            _coerce_int(execution_event_payload.get("perm_id")),
        )
    )

    resolved_order = record_order
    order_source = "BROKER_SUBMISSION_RECORD" if isinstance(record_order, int) else "UNRESOLVED"
    if not isinstance(resolved_order, int):
        order_candidates = sorted({order for _, order, _ in supplemental_rows if isinstance(order, int) and order > 0})
        if len(order_candidates) == 1:
            resolved_order = order_candidates[0]
            for source_name, order_value, _ in supplemental_rows:
                if order_value == resolved_order:
                    order_source = source_name
                    break

    resolved_perm = record_perm
    perm_source = "BROKER_SUBMISSION_RECORD" if isinstance(record_perm, int) and record_perm > 0 else "UNRESOLVED"
    if not isinstance(resolved_perm, int) or resolved_perm == 0:
        ack_perm_candidates: set[int] = set()
        for source_name, order_value, perm_value in supplemental_rows:
            if source_name != "BROKER_ACKNOWLEDGEMENT":
                continue
            if not isinstance(perm_value, int) or perm_value <= 0:
                continue
            if isinstance(resolved_order, int) and resolved_order > 0 and isinstance(order_value, int) and order_value > 0 and order_value != resolved_order:
                continue
            ack_perm_candidates.add(perm_value)
        if len(ack_perm_candidates) == 1:
            resolved_perm = next(iter(ack_perm_candidates))
            perm_source = "BROKER_ACKNOWLEDGEMENT"

    return resolved_order, resolved_perm, order_source, perm_source, perm_field_present


def evaluate_submission_index_v1(
    *,
    day_utc: str,
    execution_root: Path,
    sleeve: str = "PRIMARY",
    environment: str = "PAPER",
) -> dict[str, Any]:
    execution_root = Path(execution_root).resolve()
    submissions_day_dir = (execution_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    stream_day_dir = (execution_root / "execution_stream_v1" / day_utc).resolve()
    stream_failure_path = (execution_root / "execution_stream_v1" / "failures" / day_utc / "failure.json").resolve()
    fill_day_dir = (execution_root / "fill_ledger_v1" / day_utc).resolve()

    current_head = evaluate_execution_evidence_current_head_v1(
        day_utc=day_utc,
        execution_root=execution_root,
        sleeve=sleeve,
        environment=environment,
    )
    write_execution_evidence_current_head_v1(
        execution_root=execution_root,
        day_utc=day_utc,
        payload=current_head,
    )

    attempts: list[dict[str, Any]] = []
    blocking_evidence: list[dict[str, str]] = []
    diagnostic_evidence: list[dict[str, str]] = []
    submit_mode = classify_paper_submit_mode_status_v1(execution_root=execution_root, day_utc=day_utc)
    dry_run_complete = str(submit_mode.get("submit_mode_status") or "").strip().upper() == "DRY_RUN_COMPLETE"

    for submission_dir in _submission_attempts(submissions_day_dir):
        submission_record_path = (submission_dir / "broker_submission_record.v2.json").resolve()
        submit_attempt_path = (submission_dir / "broker_submit_attempt_v1.json").resolve()
        execution_event_path = (submission_dir / "execution_event_record.v1.json").resolve()
        submission_blockers: list[dict[str, str]] = []

        record_payload: dict[str, Any] = {}
        if not submission_record_path.exists() or not submission_record_path.is_file():
            submission_blockers.append(
                _blocking_entry(
                    "SUBMISSION_RECORD_MISSING",
                    str(submission_record_path),
                    "broker_submission_record.v2.json is missing",
                )
            )
        else:
            try:
                record_payload = _read_json(submission_record_path)
            except Exception:
                submission_blockers.append(
                    _blocking_entry(
                        "SUBMISSION_RECORD_MISSING",
                        str(submission_record_path),
                        "broker_submission_record.v2.json is unreadable",
                    )
                )

        submit_attempt_payload: dict[str, Any] = {}
        if submit_attempt_path.exists() and submit_attempt_path.is_file():
            try:
                submit_attempt_payload = _read_json(submit_attempt_path)
            except Exception:
                submit_attempt_payload = {}
        submission_is_dry_run = bool(
            submit_attempt_payload.get("dry_run") is True
            or str((record_payload.get("error") or {}).get("code") if isinstance(record_payload.get("error"), dict) else "")
            .strip()
            .upper()
            == "DRY_RUN_NO_BROKER_ID"
        )

        execution_event_payload: dict[str, Any] = {}
        if execution_event_path.exists() and execution_event_path.is_file():
            try:
                execution_event_payload = _read_json(execution_event_path)
            except Exception:
                execution_event_payload = {}

        submission_id = str(record_payload.get("submission_id") or submission_dir.name).strip() or submission_dir.name
        attempt_id_from_evidence = _first_nonempty(
            record_payload.get("attempt_id"),
            submit_attempt_payload.get("attempt_id"),
            execution_event_payload.get("attempt_id"),
        )
        attempt_id = attempt_id_from_evidence or submission_id
        attempt_id_source = "SUBMISSION_EVIDENCE" if attempt_id_from_evidence else "SUBMISSION_ID_BRIDGE"
        attempt_id_bridge_evidence: list[str] = []

        (
            broker_order_id,
            broker_perm_id,
            broker_order_id_source,
            broker_perm_id_source,
            broker_perm_field_present,
        ) = _resolve_submission_broker_ids(
            submission_dir=submission_dir,
            record_payload=record_payload,
            execution_event_payload=execution_event_payload,
        )
        if not isinstance(broker_order_id, int):
            entry = _blocking_entry(
                "BROKER_ORDER_ID_MISSING",
                str(submission_record_path),
                "broker_ids.order_id is missing",
            )
            if submission_is_dry_run:
                diagnostic_evidence.append(entry)
            else:
                submission_blockers.append(entry)

        if not broker_perm_field_present:
            entry = _blocking_entry(
                "BROKER_PERM_ID_FIELD_MISSING",
                str(submission_record_path),
                "broker_ids.perm_id field is missing",
            )
            if submission_is_dry_run:
                diagnostic_evidence.append(entry)
            else:
                submission_blockers.append(entry)

        submission_status = str(record_payload.get("status") or "").strip().upper()
        if broker_perm_id == 0 and submission_status not in {"PENDINGSUBMIT", "PRESUBMITTED", "SUBMITTED", "PENDINGCANCEL"}:
            submission_blockers.append(
                _blocking_entry(
                    "POST_SUBMIT_LINEAGE_GAP",
                    str(submission_record_path),
                    f"broker perm_id=0 not allowed for terminal status={submission_status or 'UNKNOWN'}",
                )
            )

        stream_records = _stream_records_for_submission(stream_day_dir, submission_id)
        if not stream_records and not submission_is_dry_run:
            if stream_failure_path.exists() and stream_failure_path.is_file():
                submission_blockers.append(
                    _blocking_entry(
                        "EXECUTION_STREAM_FAILURE_PRESENT",
                        str(stream_failure_path),
                        "governed execution stream failure exists for day",
                    )
                )
            else:
                submission_blockers.append(
                    _blocking_entry(
                        "EXECUTION_STREAM_MISSING",
                        str(stream_day_dir),
                        "no execution stream record found for submission",
                    )
                )

        fill_path = (fill_day_dir / f"{submission_id}.fill_ledger.v1.json").resolve()
        fill_payload: dict[str, Any] = {}
        if not fill_path.exists() or not fill_path.is_file():
            if submission_is_dry_run:
                diagnostic_evidence.append(
                    _blocking_entry(
                        "FILL_LEDGER_MISSING",
                        str(fill_path),
                        "fill ledger artifact is missing for dry-run submit",
                    )
                )
            else:
                submission_blockers.append(
                    _blocking_entry(
                        "FILL_LEDGER_MISSING",
                        str(fill_path),
                        "fill ledger artifact is missing",
                    )
                )
        else:
            try:
                fill_payload = _read_json(fill_path)
            except Exception:
                fill_payload = {}
                submission_blockers.append(
                    _blocking_entry(
                        "FILL_LEDGER_MISSING",
                        str(fill_path),
                        "fill ledger artifact is unreadable",
                    )
                )

        stream_attempt_id = ""
        stream_order_id: int | None = None
        stream_perm_id: int | None = None
        stream_payload: dict[str, Any] = {}
        execution_stream_path = str(stream_records[0]) if stream_records else ""
        if stream_records:
            try:
                stream_payload = _read_json(stream_records[0])
            except Exception:
                stream_payload = {}
            stream_attempt_id = _first_nonempty(stream_payload.get("attempt_id"))
            stream_broker_ids = stream_payload.get("broker_ids") if isinstance(stream_payload.get("broker_ids"), dict) else {}
            if isinstance(stream_broker_ids.get("order_id"), int):
                stream_order_id = int(stream_broker_ids.get("order_id"))
            stream_perm_id = _coerce_int(stream_broker_ids.get("perm_id"))
            if (not isinstance(stream_perm_id, int) or stream_perm_id <= 0) and isinstance(stream_payload.get("event_attribution"), dict):
                stream_perm_id = _coerce_int(stream_payload["event_attribution"].get("raw_perm_id"))

        fill_attempt_id = _first_nonempty(fill_payload.get("attempt_id"))
        if attempt_id and stream_attempt_id and stream_attempt_id != attempt_id:
            submission_blockers.append(
                _blocking_entry(
                    "ATTEMPT_ID_MISMATCH",
                    execution_stream_path,
                    f"submission attempt_id={attempt_id} stream attempt_id={stream_attempt_id}",
                )
            )
        if attempt_id and fill_attempt_id and fill_attempt_id != attempt_id:
            submission_blockers.append(
                _blocking_entry(
                    "ATTEMPT_ID_MISMATCH",
                    str(fill_path),
                    f"submission attempt_id={attempt_id} fill attempt_id={fill_attempt_id}",
                )
            )

        fill_order_id = fill_payload.get("broker_order_id")
        if isinstance(broker_order_id, int) and isinstance(stream_order_id, int) and stream_order_id != broker_order_id:
            submission_blockers.append(
                _blocking_entry(
                    "BROKER_ORDER_ID_MISMATCH",
                    execution_stream_path,
                    f"submission broker_order_id={broker_order_id} stream broker_order_id={stream_order_id}",
                )
            )
        if isinstance(broker_order_id, int) and isinstance(fill_order_id, int) and fill_order_id != broker_order_id:
            submission_blockers.append(
                _blocking_entry(
                    "BROKER_ORDER_ID_MISMATCH",
                    str(fill_path),
                    f"submission broker_order_id={broker_order_id} fill broker_order_id={fill_order_id}",
                )
            )

        record_broker_ids = record_payload.get("broker_ids") if isinstance(record_payload.get("broker_ids"), dict) else {}
        record_perm_id = _coerce_int(record_broker_ids.get("perm_id"))
        linkage_method = "ORDER_ID_ONLY_PENDING"
        linkage_confidence = "DEGRADED"
        if isinstance(record_perm_id, int) and record_perm_id > 0:
            linkage_method = "PERM_ID_EXACT_FROM_SUBMISSION"
            linkage_confidence = "EXACT"
            if isinstance(stream_perm_id, int) and stream_perm_id > 0 and stream_perm_id != record_perm_id:
                submission_blockers.append(
                    _blocking_entry(
                        "POST_SUBMIT_LINEAGE_GAP",
                        execution_stream_path or str(submission_record_path),
                        f"execution perm_id={stream_perm_id} does not match submission perm_id={record_perm_id}",
                    )
                )
                linkage_method = "FAIL_CLOSED"
                linkage_confidence = "NONE"
        elif isinstance(broker_perm_id, int) and broker_perm_id > 0 and broker_perm_id_source == "BROKER_ACKNOWLEDGEMENT":
            linkage_method = "PERM_ID_BACKFILL_FROM_ACK"
            linkage_confidence = "DERIVED_EXACT"
            if isinstance(stream_perm_id, int) and stream_perm_id > 0 and stream_perm_id != broker_perm_id:
                submission_blockers.append(
                    _blocking_entry(
                        "POST_SUBMIT_LINEAGE_GAP",
                        execution_stream_path or str(submission_record_path),
                        f"execution perm_id={stream_perm_id} does not match acknowledgement perm_id={broker_perm_id}",
                    )
                )
                linkage_method = "FAIL_CLOSED"
                linkage_confidence = "NONE"
        elif isinstance(stream_perm_id, int) and stream_perm_id > 0:
            submission_blockers.append(
                _blocking_entry(
                    "POST_SUBMIT_LINEAGE_GAP",
                    execution_stream_path or str(submission_record_path),
                    f"execution perm_id={stream_perm_id} has no exact submission/acknowledgement match",
                )
            )
            linkage_method = "FAIL_CLOSED"
            linkage_confidence = "NONE"

        if not attempt_id_from_evidence:
            record_submission_id = str(record_payload.get("submission_id") or "").strip()
            record_matches_submission = (
                bool(record_submission_id)
                and record_submission_id == submission_id
                and submission_id == submission_dir.name
            )
            if record_matches_submission:
                attempt_id_bridge_evidence.append(str(submission_record_path))
            if str(submit_attempt_payload.get("submission_id") or "").strip() == submission_id:
                attempt_id_bridge_evidence.append(str(submit_attempt_path))
            if str(fill_payload.get("submission_id") or "").strip() == submission_id:
                attempt_id_bridge_evidence.append(str(fill_path))
            if stream_records:
                attempt_id_bridge_evidence.append(str(stream_records[0]))

            cross_surface_match_count = len(attempt_id_bridge_evidence) - (1 if record_matches_submission else 0)
            if record_matches_submission and cross_surface_match_count >= 2:
                attempt_id_source = "SUBMISSION_ID_BRIDGE_PROVEN"
            elif not submission_is_dry_run:
                attempt_id_source = "SUBMISSION_ID_BRIDGE_UNPROVEN"
                submission_blockers.append(
                    _blocking_entry(
                        "ATTEMPT_ID_MISSING",
                        str(submission_record_path),
                        "attempt_id is missing from submission evidence and submission_id bridge is unproven",
                    )
                )

        attempts.append(
            {
                "attempt_id": attempt_id,
                "attempt_id_source": attempt_id_source,
                "attempt_id_bridge_evidence": attempt_id_bridge_evidence,
                "submission_record_path": str(submission_record_path),
                "execution_stream_path": execution_stream_path or str(stream_failure_path if stream_failure_path.exists() else stream_day_dir),
                "fill_ledger_path": str(fill_path),
                "broker_order_id": broker_order_id if isinstance(broker_order_id, int) else None,
                "broker_order_id_source": broker_order_id_source,
                "broker_perm_id": broker_perm_id,
                "broker_perm_id_source": broker_perm_id_source,
                "linkage_method": linkage_method,
                "linkage_confidence": linkage_confidence,
                "submit_mode_status": "DRY_RUN_COMPLETE" if submission_is_dry_run else submit_mode.get("submit_mode_status", ""),
                "broker_transmit_enabled": False if submission_is_dry_run else submit_mode.get("broker_transmit_enabled"),
                "broker_order_transmitted": False if submission_is_dry_run else submit_mode.get("broker_order_transmitted"),
                "lineage_status": "PASS" if not submission_blockers else "GAP",
                "blocking_evidence": submission_blockers,
            }
        )
        blocking_evidence.extend(submission_blockers)

    if str(current_head.get("status") or "").strip().upper() != "PASS" and not dry_run_complete:
        rejected = current_head.get("rejected_candidates") if isinstance(current_head.get("rejected_candidates"), list) else []
        reasons = {str(item.get("reason") or "").strip().upper() for item in rejected if isinstance(item, dict)}
        if "STALE_DAY" in reasons or "DAY_MISMATCH" in reasons:
            blocking_evidence.append(
                _blocking_entry(
                    "EXECUTION_POINTER_DAY_MISMATCH",
                    str((execution_root / "execution_evidence_v1" / "latest_pointer.v1.json").resolve()),
                    "execution current-head selection rejected pointer due day mismatch",
                )
            )
        else:
            blocking_evidence.append(
                _blocking_entry(
                    "STALE_EXECUTION_POINTER",
                    str((execution_root / "execution_evidence_v1" / "latest_pointer.v1.json").resolve()),
                    "execution current-head selection failed",
                )
            )

    status = "PASS" if not blocking_evidence and attempts else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "day": day_utc,
        "sleeve": sleeve,
        "environment": environment,
        "status": status,
        "submit_mode_status": submit_mode.get("submit_mode_status", "NO_SUBMIT_ATTEMPT"),
        "dry_run_policy": submit_mode.get("dry_run_policy", "UNKNOWN"),
        "broker_transmit_enabled": submit_mode.get("broker_transmit_enabled"),
        "broker_order_transmitted": submit_mode.get("broker_order_transmitted"),
        "missing_broker_ids_blocker": submit_mode.get("missing_broker_ids_blocker"),
        "missing_broker_ids_diagnostic": submit_mode.get("missing_broker_ids_diagnostic"),
        "attempts": attempts,
        "blocking_evidence": blocking_evidence,
        "diagnostic_evidence": diagnostic_evidence,
        "generated_at_utc": _utc_now_iso(),
    }


def submission_index_output_path(*, execution_root: Path, day_utc: str) -> Path:
    return (Path(execution_root).resolve() / "submission_index_v1" / day_utc / "submission_index.v1.json").resolve()


def write_submission_index_v1(*, execution_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = submission_index_output_path(execution_root=execution_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return output_path
