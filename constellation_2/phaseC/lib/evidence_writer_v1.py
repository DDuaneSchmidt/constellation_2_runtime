"""
evidence_writer_v1.py

Constellation 2.0 Phase C
Single-writer evidence output writer (OFFLINE ONLY).

Design authority:
- constellation_2/governance/C2_EXECUTION_CONTRACT.md (single-writer rule)
- constellation_2/governance/C2_DETERMINISM_STANDARD.md (canonical JSON)
- constellation_2/governance/C2_INVARIANTS_AND_REASON_CODES.md (C2_SINGLE_WRITER_VIOLATION)

Rules:
- Refuse overwrite: if any target output file exists => HARD FAIL
- Refuse non-empty out_dir unless explicitly empty (caller may pre-create it)
- Write JSON deterministically (canonical) and atomically (temp + rename)
- No post-write mutation. Once written, caller must not rewrite.

This module does not call network or broker APIs.
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
        # must be empty
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


def write_phasec_success_outputs_v1(
    out_dir: Path,
    *,
    order_plan: Dict[str, Any],
    mapping_ledger_record: Dict[str, Any],
    binding_record: Dict[str, Any],
    submit_preflight_decision: Dict[str, Any],
) -> None:
    _ensure_out_dir_ready(out_dir)

    p_plan = out_dir / "order_plan.v1.json"
    p_map = out_dir / "mapping_ledger_record.v1.json"
    p_bind = out_dir / "binding_record.v1.json"
    p_dec = out_dir / "submit_preflight_decision.v1.json"

    for p in (p_plan, p_map, p_bind, p_dec):
        _refuse_if_exists(p)

    try:
        _atomic_write_bytes(p_plan, canonical_json_bytes_v1(order_plan) + b"\n")
        _atomic_write_bytes(p_map, canonical_json_bytes_v1(mapping_ledger_record) + b"\n")
        _atomic_write_bytes(p_bind, canonical_json_bytes_v1(binding_record) + b"\n")
        _atomic_write_bytes(p_dec, canonical_json_bytes_v1(submit_preflight_decision) + b"\n")
    except CanonicalizationError as e:
        raise EvidenceWriteError(f"CANONICALIZATION_FAILED_DURING_WRITE: {e}") from e


def write_phasec_veto_only_v1(
    out_dir: Path,
    *,
    veto_record: Dict[str, Any],
) -> None:
    _ensure_out_dir_ready(out_dir)

    p_veto = out_dir / "veto_record.v1.json"
    _refuse_if_exists(p_veto)

    try:
        _atomic_write_bytes(p_veto, canonical_json_bytes_v1(veto_record) + b"\n")
    except CanonicalizationError as e:
        raise EvidenceWriteError(f"CANONICALIZATION_FAILED_DURING_WRITE: {e}") from e


def expected_outputs_for_dir_v1(out_dir: Path) -> Dict[str, Optional[Path]]:
    """
    Convenience for callers/tests.
    """
    return {
        "order_plan": out_dir / "order_plan.v1.json",
        "mapping_ledger_record": out_dir / "mapping_ledger_record.v1.json",
        "binding_record": out_dir / "binding_record.v1.json",
        "submit_preflight_decision": out_dir / "submit_preflight_decision.v1.json",
        "veto_record": out_dir / "veto_record.v1.json",
    }
