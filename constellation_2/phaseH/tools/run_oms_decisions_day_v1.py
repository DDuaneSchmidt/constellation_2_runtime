from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

INTENTS_SNAPSHOTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()

# Canonical preflight decision truth intake (Bundle H-0 requires these be persisted)
# One file per intent_hash per day:
#   <intent_hash>.submit_preflight_decision.v1.json  OR
#   <intent_hash>.veto_record.v1.json
PREFLIGHT_ROOT = (TRUTH_ROOT / "phaseC_preflight_v1").resolve()

OMS_OUT_ROOT = (TRUTH_ROOT / "oms_decisions_v1" / "decisions").resolve()

SCHEMA_OMS_DECISION = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/oms_decision.v1.schema.json"


# Bundle H enum (blocked reasons only). Strict.
BLOCK_REASON_ENUM = [
    "FRESHNESS_EXPIRED",
    "ALLOCATION_LIMIT",
    "RISK_LIMIT",
    "MODE_MISMATCH",
    "MAPPING_INVALID",
    "DUPLICATE_INTENT",
]

# Source reason codes -> normalized blocked reason (fail-closed if unknown)
# Proven source veto reason:
#   C2_FRESHNESS_CERT_INVALID_OR_EXPIRED
SOURCE_REASON_MAP = {
    "C2_FRESHNESS_CERT_INVALID_OR_EXPIRED": "FRESHNESS_EXPIRED",
}


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    if not path.is_file():
        raise ValueError(f"NOT_A_FILE: {str(path)}")
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _sha256_bytes(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _intent_hash_from_file(path: Path) -> str:
    # Constellation 2.0 convention: intent_hash = sha256(bytes of canonical JSON file)
    return _sha256_file(path)


def _list_intent_files(day_utc: str) -> List[Path]:
    d = (INTENTS_SNAPSHOTS_ROOT / day_utc).resolve()
    if not d.exists() or not d.is_dir():
        raise FileNotFoundError(f"INTENTS_DAY_DIR_MISSING: {str(d)}")
    files = sorted([p for p in d.iterdir() if p.is_file() and p.name.endswith(".json")])
    if not files:
        raise ValueError(f"INTENTS_DAY_DIR_EMPTY: {str(d)}")
    return files


def _load_preflight_for_intent(day_utc: str, intent_hash: str) -> Tuple[str, Path, Dict[str, Any]]:
    """
    Return (source_type, path, obj) where source_type is:
      - submit_preflight_decision_v1
      - veto_record_v1
    """
    d = (PREFLIGHT_ROOT / day_utc).resolve()
    if not d.exists() or not d.is_dir():
        raise FileNotFoundError(f"PREFLIGHT_DAY_DIR_MISSING: {str(d)}")

    p_allow = d / f"{intent_hash}.submit_preflight_decision.v1.json"
    p_veto = d / f"{intent_hash}.veto_record.v1.json"

    found: List[Tuple[str, Path]] = []
    if p_allow.exists():
        found.append(("submit_preflight_decision_v1", p_allow))
    if p_veto.exists():
        found.append(("veto_record_v1", p_veto))

    if len(found) == 0:
        raise FileNotFoundError(f"MISSING_PREFLIGHT_DECISION_FOR_INTENT_HASH: {intent_hash}")
    if len(found) > 1:
        raise ValueError(f"DUPLICATE_PREFLIGHT_DECISION_FOR_INTENT_HASH: {intent_hash}")

    source_type, path = found[0]
    obj = _read_json_obj(path)
    return source_type, path, obj


def _normalize_decision(source_type: str, src_obj: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str], str, str]:
    """
    Returns:
      (disposition, normalized_reason_code, normalized_reason_detail, source_reason_code, source_decision)

    Rules:
      - RELEASED => normalized_reason_code must be None
      - BLOCKED  => normalized_reason_code must be in BLOCK_REASON_ENUM
    """
    if source_type == "submit_preflight_decision_v1":
        # Proven shape:
        # {"decision":"ALLOW","block_detail":null,...}
        dec = str(src_obj.get("decision") or "").strip()
        if dec != "ALLOW":
            raise ValueError(f"UNEXPECTED_SUBMIT_PREFLIGHT_DECISION: {dec}")
        # Release has no block reason.
        return ("RELEASED", None, None, "C2_SUBMIT_PREFLIGHT_ALLOW", "ALLOW")

    if source_type == "veto_record_v1":
        # Proven fields: reason_code (string), reason_detail (string), boundary, inputs.intent_hash, etc.
        src_reason = str(src_obj.get("reason_code") or "").strip()
        if not src_reason:
            raise ValueError("VETO_REASON_CODE_MISSING")

        rd = src_obj.get("reason_detail")
        if rd is None:
            rd_s: Optional[str] = None
        elif isinstance(rd, str) and rd.strip():
            rd_s = rd.strip()
        else:
            raise ValueError("VETO_REASON_DETAIL_INVALID")

        if src_reason not in SOURCE_REASON_MAP:
            raise ValueError(f"UNKNOWN_SOURCE_REASON_CODE: {src_reason}")

        norm = SOURCE_REASON_MAP[src_reason]
        if norm not in BLOCK_REASON_ENUM:
            raise ValueError(f"INVALID_NORMALIZED_REASON: {norm}")

        return ("BLOCKED", norm, rd_s, src_reason, "VETO")

    raise ValueError(f"UNKNOWN_SOURCE_TYPE: {source_type}")


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_oms_decisions_day_v1",
        description="Bundle H-0: per-intent OMS decisions (immutable, deterministic, strict reconciliation).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()
    module = "constellation_2/phaseH/tools/run_oms_decisions_day_v1.py"

    try:
        intent_files = _list_intent_files(day_utc)
    except Exception as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 2

    produced_utc = f"{day_utc}T00:00:00Z"
    out_day_dir = (OMS_OUT_ROOT / day_utc).resolve()

    mismatch = 0
    wrote = 0

    for p_intent in intent_files:
        try:
            intent_obj = _read_json_obj(p_intent)
            intent_path_abs = str(p_intent.resolve())
            intent_sha = _sha256_file(p_intent)
            intent_hash = _intent_hash_from_file(p_intent)

            engine = intent_obj.get("engine")
            if not isinstance(engine, dict):
                raise ValueError("INTENT_ENGINE_MISSING")
            engine_id = str(engine.get("engine_id") or "").strip()
            mode = str(engine.get("mode") or "").strip()
            suite = str(engine.get("suite") or "").strip()
            if not engine_id or not mode or not suite:
                raise ValueError("INTENT_ENGINE_FIELDS_MISSING")

            intent_id = str(intent_obj.get("intent_id") or "").strip()
            if not intent_id:
                raise ValueError("INTENT_ID_MISSING")

            source_type, p_src, src_obj = _load_preflight_for_intent(day_utc, intent_hash)
            src_path_abs = str(p_src.resolve())
            src_sha = _sha256_file(p_src)

            disposition, norm_reason, norm_detail, src_reason_code, src_decision = _normalize_decision(source_type, src_obj)

            out_obj: Dict[str, Any] = {
                "schema_id": "C2_OMS_DECISION_V1",
                "schema_version": 1,
                "produced_utc": produced_utc,
                "day_utc": day_utc,
                "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
                "status": "OK",
                "reason_codes": [],
                "input_manifest": [
                    {"type": "intent", "path": intent_path_abs, "sha256": intent_sha, "producer": "intents_v1", "day_utc": day_utc},
                    {"type": source_type, "path": src_path_abs, "sha256": src_sha, "producer": "phaseC_preflight_v1", "day_utc": day_utc},
                ],
                "engine": {"engine_id": engine_id, "mode": mode, "suite": suite},
                "intent": {"intent_hash": intent_hash, "intent_id": intent_id, "intent_path": intent_path_abs},
                "decision": {"disposition": disposition, "reason_code": norm_reason, "reason_detail": norm_detail},
                "source": {
                    "source_type": source_type,
                    "source_path": src_path_abs,
                    "source_sha256": src_sha,
                    "source_reason_code": src_reason_code,
                    "source_decision": src_decision,
                },
            }

            validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_OMS_DECISION)

            b = canonical_json_bytes_v1(out_obj) + b"\n"
            out_path = out_day_dir / f"{intent_hash}.oms_decision.v1.json"
            _ = write_file_immutable_v1(path=out_path, data=b, create_dirs=True)
            wrote += 1

        except ImmutableWriteError as e:
            print(f"FAIL: IMMUTABLE_WRITE: {e}", file=sys.stderr)
            return 4
        except Exception as e:
            mismatch += 1
            print(f"FAIL: INTENT_PROCESSING_FAILED: intent_file={str(p_intent)} err={e}", file=sys.stderr)

    if mismatch > 0:
        print(f"STATUS=FAIL_RECONCILIATION day={day_utc} mismatch={mismatch}", file=sys.stderr)
        return 5

    print(f"OK: OMS_DECISIONS_WRITTEN day={day_utc} intents={len(intent_files)} wrote={wrote}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
