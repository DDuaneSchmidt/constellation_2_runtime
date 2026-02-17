from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.execution_evidence.lib.paths_v1 import day_paths_v1 as exec_day_paths_v1
from constellation_2.phaseF.positions.lib.paths_v5 import REPO_ROOT, day_paths_v5
from constellation_2.phaseF.positions.lib.write_failure_v1 import build_failure_obj_v1, write_failure_immutable_v1

SCHEMA_OUT = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json"
EXEC_EVENT_V1_SCHEMA = "constellation_2/schemas/execution_event_record.v1.schema.json"
EXEC_EVENT_V2_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_event_record.v2.schema.json"
BROKER_SUB_REC_V3_SCHEMA = "constellation_2/schemas/broker_submission_record.v2.schema.json"


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _parse_price_to_cents(price_str: str) -> int:
    s = str(price_str).strip()
    if not s:
        raise ValueError("AVG_PRICE_EMPTY")
    if s.count(".") > 1:
        raise ValueError("AVG_PRICE_INVALID_DECIMAL")
    if "." in s:
        whole, frac = s.split(".", 1)
    else:
        whole, frac = s, ""
    if not whole.isdigit():
        raise ValueError("AVG_PRICE_INVALID_WHOLE")
    if frac and not frac.isdigit():
        raise ValueError("AVG_PRICE_INVALID_FRAC")
    if len(frac) > 2:
        raise ValueError("AVG_PRICE_TOO_MANY_DECIMALS")
    frac2 = (frac + "00")[:2]
    return int(whole) * 100 + int(frac2)


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_positions_snapshot_day_v5")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--producer_git_sha", required=True)
    ap.add_argument("--producer_repo", default="constellation_2_runtime")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()

    dp_exec = exec_day_paths_v1(day_utc)
    dp_pos = day_paths_v5(day_utc)

    if not dp_exec.submissions_day_dir.exists():
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/positions/run/run_positions_snapshot_day_v5.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["EXECUTION_EVIDENCE_DAY_DIR_MISSING"],
            input_manifest=[{"type": "execution_evidence_day_dir", "path": str(dp_exec.submissions_day_dir), "sha256": "0" * 64}],
            code="FAIL_CORRUPT_INPUTS",
            message=f"Missing execution evidence day directory: {str(dp_exec.submissions_day_dir)}",
            details={"missing_path": str(dp_exec.submissions_day_dir)},
            attempted_outputs=[{"path": str(dp_pos.snapshot_path), "sha256": None}],
        )
        _ = write_failure_immutable_v1(failure_path=dp_pos.failure_path, failure_obj=failure)
        print("FAIL: EXECUTION_EVIDENCE_DAY_DIR_MISSING (failure artifact written)")
        return 2

    items: List[Dict[str, Any]] = []
    missing_attr = False

    sub_dirs = sorted([p for p in dp_exec.submissions_day_dir.iterdir() if p.is_dir()], key=lambda p: p.name)
    for sd in sub_dirs:
        submission_id = sd.name.strip()

        # Require broker submission record v3 (Phase D v2 should write it). If absent, fail attribution.
        bsr_path = sd / "broker_submission_record.v3.json"
        if not bsr_path.exists():
            missing_attr = True
            continue

        bsr = _read_json_obj(bsr_path)
        # We cannot prove a v3 schema exists in constellation_2/schemas. Validate at least as v2 shape if present.
        validate_against_repo_schema_v1(bsr, REPO_ROOT, BROKER_SUB_REC_V3_SCHEMA)

        engine_id = str(bsr.get("engine_id") or "").strip()
        source_intent_id = str(bsr.get("source_intent_id") or "").strip()
        intent_sha256 = str(bsr.get("intent_sha256") or "").strip()

        if not engine_id or not source_intent_id or len(intent_sha256) != 64:
            missing_attr = True
            continue

        # Prefer linked execution event v2 if present; else v1 (but v1 might be synth-filled)
        evt = None
        p2 = sd / "execution_event_record.v2.json"
        p1 = sd / "execution_event_record.v1.json"
        if p2.exists():
            evt = _read_json_obj(p2)
            validate_against_repo_schema_v1(evt, REPO_ROOT, EXEC_EVENT_V2_SCHEMA)
        elif p1.exists():
            evt = _read_json_obj(p1)
            validate_against_repo_schema_v1(evt, REPO_ROOT, EXEC_EVENT_V1_SCHEMA)
        else:
            continue

        qty = int(evt.get("filled_qty") or 0)
        avg_price = str(evt.get("avg_price") or "0")
        avg_cents = _parse_price_to_cents(avg_price)

        # Instrument identity: reuse v4 order-plan parsing by reading the same plans (kept simple here)
        instr: Dict[str, Any] = {"kind": "UNKNOWN"}
        if (sd / "equity_order_plan.v1.json").exists():
            ep = _read_json_obj(sd / "equity_order_plan.v1.json")
            instr = {"kind": "EQUITY", "symbol": str(ep.get("symbol") or "").strip(), "currency": str(ep.get("currency") or "").strip()}
        elif (sd / "order_plan.v1.json").exists():
            op = _read_json_obj(sd / "order_plan.v1.json")
            instr = {"kind": "OPTIONS_PLAN", "underlying": op.get("underlying"), "legs": op.get("legs")}

        pos_id = str(evt.get("binding_hash") or submission_id).strip()
        if not pos_id:
            missing_attr = True
            continue

        items.append(
            {
                "position_id": pos_id,
                "engine_id": engine_id,
                "source_intent_id": source_intent_id,
                "intent_sha256": intent_sha256,
                "instrument": instr,
                "qty": qty,
                "avg_cost_cents": avg_cents,
                "opened_day_utc": day_utc,
                "status": "OPEN",
            }
        )

    if missing_attr:
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/positions/run/run_positions_snapshot_day_v5.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["MISSING_ENGINE_ATTRIBUTION"],
            input_manifest=[{"type": "execution_evidence_day_dir", "path": str(dp_exec.submissions_day_dir), "sha256": "0" * 64}],
            code="FAIL_CORRUPT_INPUTS",
            message="Missing engine attribution required for positions v5 (needs broker_submission_record.v3 with engine_id/source_intent_id/intent_sha256).",
            details={"day_utc": day_utc},
            attempted_outputs=[{"path": str(dp_pos.snapshot_path), "sha256": None}],
        )
        _ = write_failure_immutable_v1(failure_path=dp_pos.failure_path, failure_obj=failure)
        print("FAIL: MISSING_ENGINE_ATTRIBUTION (failure artifact written)")
        return 2

    produced_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    out: Dict[str, Any] = {
        "schema_id": "C2_POSITIONS_SNAPSHOT_V5",
        "schema_version": 5,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v5.py"},
        "status": "OK",
        "reason_codes": ["ENGINE_ATTRIBUTION_FROM_BROKER_SUBMISSION_RECORD_V3"],
        "items": items,
        "canonical_json_hash": None,
    }
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA_OUT)

    try:
        payload = canonical_json_bytes_v1(out) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        _ = write_file_immutable_v1(path=dp_pos.snapshot_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    print("OK: POSITIONS_SNAPSHOT_V5_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
