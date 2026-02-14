"""
evidence_writer_v1.py

Constellation 2.0 Phase D
Single-writer evidence output writer (BROKER BOUNDARY SAFE).

Design authority:
- constellation_2/governance/C2_EXECUTION_CONTRACT.md (single-writer rule)
- constellation_2/governance/C2_DETERMINISM_STANDARD.md (canonical JSON)
- constellation_2/governance/C2_INVARIANTS_AND_REASON_CODES.md (C2_SINGLE_WRITER_VIOLATION)
- constellation_2/governance/C2_AUDIT_EVIDENCE_CHAIN.md (required outputs)

Rules:
- Refuse overwrite: if any target output file exists => HARD FAIL
- Refuse non-empty out_dir unless explicitly empty (caller may pre-create it)
- Write JSON deterministically (canonical) and atomically (temp + rename)
- No post-write mutation. Once written, caller must not rewrite.

Phase D outputs:
- SUCCESS (broker ids present):
  - broker_submission_record.v2.json
  - execution_event_record.v1.json
- BROKER REJECTED / PARTIAL BROKER FAILURE (ids absent):
  - broker_submission_record.v2.json only
- BLOCK (fail-closed before broker call):
  - veto_record.v1.json only

Optional identity inputs (written when provided; immutable, canonical):
- order_plan.v1.json
- binding_record.v1.json
- mapping_ledger_record.v1.json
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

from .canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1


class EvidenceWriteError(Exception):
    pass


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise EvidenceWriteError(f"TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")
    try:
        with tmp.open("wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(str(tmp), str(path))
    except Exception as e:  # noqa: BLE001
        # Best-effort cleanup
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:  # noqa: BLE001
            pass
        raise EvidenceWriteError(f"ATOMIC_WRITE_FAILED: {str(path)}: {e}") from e


def _ensure_out_dir_ready(out_dir: Path) -> None:
    if out_dir.exists():
        if not out_dir.is_dir():
            raise EvidenceWriteError(f"OUT_DIR_NOT_DIRECTORY: {str(out_dir)}")
        entries = list(out_dir.iterdir())
        if entries:
            raise EvidenceWriteError(f"OUT_DIR_NOT_EMPTY: {str(out_dir)}")
        return
    try:
        out_dir.mkdir(parents=True, exist_ok=False)
    except Exception as e:  # noqa: BLE001
        raise EvidenceWriteError(f"OUT_DIR_CREATE_FAILED: {str(out_dir)}: {e}") from e


def _refuse_if_exists(path: Path) -> None:
    if path.exists():
        raise EvidenceWriteError(f"REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")


def _write_optional_inputs_v1(
    out_dir: Path,
    *,
    order_plan: Optional[Dict[str, Any]],
    binding_record: Optional[Dict[str, Any]],
    mapping_ledger_record: Optional[Dict[str, Any]],
) -> None:
    pairs = [
        ("order_plan.v1.json", order_plan),
        ("binding_record.v1.json", binding_record),
        ("mapping_ledger_record.v1.json", mapping_ledger_record),
    ]
    for fname, obj in pairs:
        if obj is None:
            continue
        p = out_dir / fname
        _refuse_if_exists(p)
        try:
            _atomic_write_bytes(p, canonical_json_bytes_v1(obj) + b"\n")
        except CanonicalizationError as e:
            raise EvidenceWriteError(f"CANONICALIZATION_FAILED_DURING_WRITE: {fname}: {e}") from e


def write_phased_submission_only_v1(
    out_dir: Path,
    *,
    broker_submission_record: Dict[str, Any],
    order_plan: Optional[Dict[str, Any]] = None,
    binding_record: Optional[Dict[str, Any]] = None,
    mapping_ledger_record: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Write BrokerSubmissionRecord only (used when broker rejects and no ids/event are available).
    Optional identity inputs may also be written.
    """
    _ensure_out_dir_ready(out_dir)

    p_sub = out_dir / "broker_submission_record.v2.json"
    _refuse_if_exists(p_sub)

    # Primary required output first (prevents "optional-only" partial dirs).
    try:
        _atomic_write_bytes(p_sub, canonical_json_bytes_v1(broker_submission_record) + b"\n")
    except CanonicalizationError as e:
        raise EvidenceWriteError(f"CANONICALIZATION_FAILED_DURING_WRITE: {e}") from e

    _write_optional_inputs_v1(
        out_dir,
        order_plan=order_plan,
        binding_record=binding_record,
        mapping_ledger_record=mapping_ledger_record,
    )


def write_phased_success_outputs_v1(
    out_dir: Path,
    *,
    broker_submission_record: Dict[str, Any],
    execution_event_record: Dict[str, Any],
    order_plan: Optional[Dict[str, Any]] = None,
    binding_record: Optional[Dict[str, Any]] = None,
    mapping_ledger_record: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Write BrokerSubmissionRecord + ExecutionEventRecord (ids present).
    Optional identity inputs may also be written.
    """
    _ensure_out_dir_ready(out_dir)

    p_sub = out_dir / "broker_submission_record.v2.json"
    p_evt = out_dir / "execution_event_record.v1.json"

    for p in (p_sub, p_evt):
        _refuse_if_exists(p)

    try:
        _atomic_write_bytes(p_sub, canonical_json_bytes_v1(broker_submission_record) + b"\n")
        _atomic_write_bytes(p_evt, canonical_json_bytes_v1(execution_event_record) + b"\n")
    except CanonicalizationError as e:
        raise EvidenceWriteError(f"CANONICALIZATION_FAILED_DURING_WRITE: {e}") from e

    _write_optional_inputs_v1(
        out_dir,
        order_plan=order_plan,
        binding_record=binding_record,
        mapping_ledger_record=mapping_ledger_record,
    )


def write_phased_veto_only_v1(
    out_dir: Path,
    *,
    veto_record: Dict[str, Any],
    order_plan: Optional[Dict[str, Any]] = None,
    binding_record: Optional[Dict[str, Any]] = None,
    mapping_ledger_record: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Write VetoRecord only (blocked before broker call).
    Optional identity inputs may also be written for forensic traceability.
    """
    _ensure_out_dir_ready(out_dir)

    p_veto = out_dir / "veto_record.v1.json"
    _refuse_if_exists(p_veto)

    try:
        _atomic_write_bytes(p_veto, canonical_json_bytes_v1(veto_record) + b"\n")
    except CanonicalizationError as e:
        raise EvidenceWriteError(f"CANONICALIZATION_FAILED_DURING_WRITE: {e}") from e

    _write_optional_inputs_v1(
        out_dir,
        order_plan=order_plan,
        binding_record=binding_record,
        mapping_ledger_record=mapping_ledger_record,
    )


def expected_outputs_for_dir_v1(out_dir: Path) -> Dict[str, Optional[Path]]:
    return {
        "broker_submission_record": out_dir / "broker_submission_record.v2.json",
        "execution_event_record": out_dir / "execution_event_record.v1.json",
        "veto_record": out_dir / "veto_record.v1.json",
        "order_plan": out_dir / "order_plan.v1.json",
        "binding_record": out_dir / "binding_record.v1.json",
        "mapping_ledger_record": out_dir / "mapping_ledger_record.v1.json",
    }
