#!/usr/bin/env python3
"""
evidence_writer_v2.py

Constellation 2.0 Phase C
Single-writer evidence output writer (NO BROKER CALLS).

Adds Equity v2 output set:
- equity_order_plan.v2.json
- mapping_ledger_record.v2.json
- binding_record.v2.json
- submit_preflight_decision.v1.json

Rules (same as v1):
- Refuse overwrite
- Refuse non-empty out_dir
- Canonical JSON + newline
- Atomic temp + rename
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1


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


def _write_json_obj(out_dir: Path, filename: str, obj: Dict[str, Any]) -> None:
    p = out_dir / filename
    _refuse_if_exists(p)
    try:
        _atomic_write_bytes(p, canonical_json_bytes_v1(obj) + b"\n")
    except CanonicalizationError as e:
        raise EvidenceWriteError(f"CANONICALIZATION_FAILED_DURING_WRITE: {filename}: {e}") from e


def write_phasec_success_outputs_equity_v2(
    out_dir: Path,
    *,
    equity_order_plan_v2: Dict[str, Any],
    mapping_ledger_record_v2: Dict[str, Any],
    binding_record_v2: Dict[str, Any],
    submit_preflight_decision: Dict[str, Any],
) -> None:
    _ensure_out_dir_ready(out_dir)

    _write_json_obj(out_dir, "equity_order_plan.v2.json", equity_order_plan_v2)
    _write_json_obj(out_dir, "mapping_ledger_record.v2.json", mapping_ledger_record_v2)
    _write_json_obj(out_dir, "binding_record.v2.json", binding_record_v2)
    _write_json_obj(out_dir, "submit_preflight_decision.v1.json", submit_preflight_decision)
