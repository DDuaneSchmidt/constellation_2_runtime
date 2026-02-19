#!/usr/bin/env python3
"""
run_phaseC_preflight_day_v2.py

Constellation 2.0 — Phase C
Day runner v2 to generate Phase C preflight truth for all intents in intents_v1/snapshots/<day_utc>/.

Differences vs v1 (institutional exit support):
- Supports ExposureIntent v1 for BOTH exposure types in schema enum:
    LONG_EQUITY, SHORT_VOL_DEFINED
- Allows deterministic EXIT intents expressed as:
    target_notional_pct == 0
  (for any supported exposure_type)
- For EXIT intents (tgt==0): constraints may be null (exit is risk-reducing).
- For ENTRY intents (tgt>0): constraints required and max_risk_pct must be > 0.

Outputs are identical shape/location as v1:
  runtime/truth/phaseC_preflight_v1/<day_utc>/
    <intent_hash>.submit_preflight_decision.v1.json  OR
    <intent_hash>.veto_record.v1.json

Fail-closed:
- Unknown intent schemas vetoed.
- Refuses overwrite.
- Deterministic: intent_hash = sha256(file bytes).

NOTE: This v2 runner writes into phaseC_preflight_v1 to preserve downstream compatibility.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

INTENTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()
OUT_ROOT = (TRUTH_ROOT / "phaseC_preflight_v1").resolve()

SCHEMA_EXPOSURE_INTENT = "constellation_2/schemas/exposure_intent.v1.schema.json"
SCHEMA_VETO = "constellation_2/schemas/veto_record.v1.schema.json"
SCHEMA_DECISION = "constellation_2/schemas/submit_preflight_decision.v1.schema.json"

RC_SUBMIT_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"

SUPPORTED_EXPOSURE_TYPES = {"LONG_EQUITY", "SHORT_VOL_DEFINED"}


class PreflightDayError(Exception):
    pass


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _atomic_write_bytes_refuse_overwrite(path: Path, data: bytes) -> None:
    if path.exists():
        raise PreflightDayError(f"REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        raise PreflightDayError(f"TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")
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
        raise PreflightDayError(f"ATOMIC_WRITE_FAILED: {str(path)}: {e}") from e


def _parse_day_utc(d: str) -> str:
    s = (d or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise PreflightDayError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def _refuse_if_dir_missing_or_empty(d: Path) -> List[Path]:
    if not d.exists() or not d.is_dir():
        raise FileNotFoundError(f"INTENTS_DAY_DIR_MISSING: {str(d)}")
    files = sorted([p for p in d.iterdir() if p.is_file() and p.name.endswith(".json")])
    if not files:
        raise ValueError(f"INTENTS_DAY_DIR_EMPTY: {str(d)}")
    return files


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise PreflightDayError(f"INTENT_TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _decimal_str_in_0_1(s: str, field: str) -> Decimal:
    try:
        d = Decimal(str(s).strip())
    except (InvalidOperation, ValueError) as e:
        raise PreflightDayError(f"DECIMAL_PARSE_FAILED: field={field} value={s!r}") from e
    if d < Decimal("0") or d > Decimal("1"):
        raise PreflightDayError(f"DECIMAL_OUT_OF_RANGE_0_1: field={field} value={s!r}")
    return d


def _mk_veto(*, observed_at_utc: str, reason_detail: str, intent_hash: Optional[str], pointers: List[str]) -> Dict[str, Any]:
    veto: Dict[str, Any] = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "observed_at_utc": observed_at_utc,
        "boundary": "SUBMIT",
        "reason_code": RC_SUBMIT_FAIL_CLOSED,
        "reason_detail": reason_detail,
        "inputs": {"intent_hash": intent_hash, "plan_hash": None, "chain_snapshot_hash": None, "freshness_cert_hash": None},
        "pointers": list(pointers) if pointers else ["<none>"],
        "canonical_json_hash": None,
        "upstream_hash": None,
    }
    veto["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(veto)
    validate_against_repo_schema_v1(veto, REPO_ROOT, SCHEMA_VETO)
    return veto


def _mk_allow_decision(*, created_at_utc: str, binding_hash: str) -> Dict[str, Any]:
    dec: Dict[str, Any] = {
        "schema_id": "submit_preflight_decision",
        "schema_version": "v1",
        "created_at_utc": created_at_utc,
        "binding_hash": binding_hash,
        "decision": "ALLOW",
        "block_detail": None,
        "upstream_hash": binding_hash,
        "canonical_json_hash": None,
    }
    dec["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(dec)
    validate_against_repo_schema_v1(dec, REPO_ROOT, SCHEMA_DECISION)
    return dec


def _is_exit_intent(tgt: Decimal) -> bool:
    return tgt == Decimal("0")


def _evaluate_exposure_intent_v1(intent_obj: Dict[str, Any], *, eval_time_utc: str, intent_hash: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    validate_against_repo_schema_v1(intent_obj, REPO_ROOT, SCHEMA_EXPOSURE_INTENT)

    engine = intent_obj.get("engine")
    if not isinstance(engine, dict):
        raise PreflightDayError("EXPOSURE_INTENT_ENGINE_NOT_OBJECT")

    suite = str(engine.get("suite") or "").strip()
    if suite != "C2_HYBRID_V1":
        raise PreflightDayError(f"EXPOSURE_INTENT_SUITE_INVALID: {suite!r}")

    exposure_type = str(intent_obj.get("exposure_type") or "").strip()
    if exposure_type not in SUPPORTED_EXPOSURE_TYPES:
        raise PreflightDayError(f"EXPOSURE_TYPE_UNSUPPORTED_V2: {exposure_type!r}")

    tgt = _decimal_str_in_0_1(str(intent_obj.get("target_notional_pct") or ""), "target_notional_pct")

    if _is_exit_intent(tgt):
        # EXIT intent allowed for any supported exposure type; constraints may be null.
        binding_hash = canonical_hash_for_c2_artifact_v1({"intent_hash": intent_hash, "binding_mode": "EXPOSURE_INTENT_V1_EXIT"})
        decision = _mk_allow_decision(created_at_utc=eval_time_utc, binding_hash=binding_hash)
        return decision, None

    # ENTRY intent: constraints required and max_risk_pct > 0
    constraints = intent_obj.get("constraints")
    if not isinstance(constraints, dict):
        raise PreflightDayError("CONSTRAINTS_REQUIRED_FOR_ENTRY_EXPOSURE_INTENT_V1")
    mr = _decimal_str_in_0_1(str(constraints.get("max_risk_pct") or ""), "constraints.max_risk_pct")
    if mr == Decimal("0"):
        raise PreflightDayError("MAX_RISK_PCT_MUST_BE_GT_ZERO")

    binding_hash = canonical_hash_for_c2_artifact_v1({"intent_hash": intent_hash, "binding_mode": "EXPOSURE_INTENT_V1_ENTRY"})
    decision = _mk_allow_decision(created_at_utc=eval_time_utc, binding_hash=binding_hash)
    return decision, None


def _mark_exists_for_intent(out_day_dir: Path, intent_hash: str) -> bool:
    p_dec = out_day_dir / f"{intent_hash}.submit_preflight_decision.v1.json"
    p_veto = out_day_dir / f"{intent_hash}.veto_record.v1.json"
    return bool(p_dec.exists() or p_veto.exists())


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_phaseC_preflight_day_v2")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--eval_time_utc", required=True, help="Evaluation time UTC ISO-8601 with Z suffix (deterministic)")
    args = ap.parse_args(argv)

    day_utc = _parse_day_utc(args.day_utc)
    eval_time_utc = str(args.eval_time_utc).strip()
    if not eval_time_utc.endswith("Z"):
        print("FAIL: eval_time_utc must be Z-suffix ISO-8601", file=sys.stderr)
        return 2

    intents_day_dir = (INTENTS_ROOT / day_utc).resolve()
    out_day_dir = (OUT_ROOT / day_utc).resolve()

    try:
        intent_files = _refuse_if_dir_missing_or_empty(intents_day_dir)
    except Exception as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 2

    if not out_day_dir.exists():
        out_day_dir.mkdir(parents=True, exist_ok=False)
    if not out_day_dir.is_dir():
        print(f"FAIL: OUT_DAY_DIR_NOT_DIR: {str(out_day_dir)}", file=sys.stderr)
        return 2

    wrote = 0
    exists = 0

    for p_intent in intent_files:
        try:
            intent_bytes = p_intent.read_bytes()
        except Exception as e:  # noqa: BLE001
            print(f"FAIL: INTENT_READ_FAILED: intent_file={str(p_intent)} err={e}", file=sys.stderr)
            return 2

        intent_hash = _sha256_bytes(intent_bytes)

        if _mark_exists_for_intent(out_day_dir, intent_hash):
            exists += 1
            continue

        try:
            intent_obj = _read_json_obj(p_intent)

            schema_id = str(intent_obj.get("schema_id") or "").strip()
            schema_version = str(intent_obj.get("schema_version") or "").strip()

            if schema_id == "exposure_intent" and schema_version == "v1":
                decision, veto = _evaluate_exposure_intent_v1(intent_obj, eval_time_utc=eval_time_utc, intent_hash=intent_hash)
            else:
                raise PreflightDayError(f"UNSUPPORTED_INTENT_SCHEMA_FOR_THIS_RUNNER: schema_id={schema_id!r} schema_version={schema_version!r}")

            if veto is not None:
                out_path = out_day_dir / f"{intent_hash}.veto_record.v1.json"
                payload = canonical_json_bytes_v1(veto) + b"\n"
                _atomic_write_bytes_refuse_overwrite(out_path, payload)
                wrote += 1
                continue

            assert decision is not None
            out_path = out_day_dir / f"{intent_hash}.submit_preflight_decision.v1.json"
            payload = canonical_json_bytes_v1(decision) + b"\n"
            _atomic_write_bytes_refuse_overwrite(out_path, payload)
            wrote += 1

        except Exception as e:  # noqa: BLE001
            try:
                if _mark_exists_for_intent(out_day_dir, intent_hash):
                    exists += 1
                    continue
                veto = _mk_veto(
                    observed_at_utc=eval_time_utc,
                    reason_detail=str(e),
                    intent_hash=intent_hash,
                    pointers=[str(p_intent.resolve())],
                )
                out_path = out_day_dir / f"{intent_hash}.veto_record.v1.json"
                payload = canonical_json_bytes_v1(veto) + b"\n"
                _atomic_write_bytes_refuse_overwrite(out_path, payload)
                wrote += 1
            except Exception as e2:  # noqa: BLE001
                print(f"FAIL: PREFLIGHT_INTENT_PROCESSING_FAILED: intent_file={str(p_intent)} err={e} veto_write_err={e2}", file=sys.stderr)
                return 2

    print(f"OK: PHASEC_PREFLIGHT_V2_WRITTEN day={day_utc} intents={len(intent_files)} wrote={wrote} exists={exists}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
