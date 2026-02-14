from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.execution_evidence.lib.paths_v1 import day_paths_v1 as exec_day_paths_v1
from constellation_2.phaseF.positions.lib.paths_v2 import REPO_ROOT, day_paths_v2
from constellation_2.phaseF.positions.lib.write_failure_v1 import build_failure_obj_v1, write_failure_immutable_v1

SCHEMA_POSITIONS_SNAPSHOT_V2 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v2.schema.json"
SCHEMA_POSITIONS_LATEST_PTR_V2 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_latest_pointer.v2.schema.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_price_to_cents(price_str: str) -> int:
    # Same deterministic cents parsing approach as cash ledger.
    if not isinstance(price_str, str):
        raise ValueError("AVG_PRICE_NOT_STRING")
    s = price_str.strip()
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
        # truncate NOT allowed; fail closed
        raise ValueError("AVG_PRICE_TOO_MANY_DECIMALS")
    frac2 = (frac + "00")[:2]
    return int(whole) * 100 + int(frac2)


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def build_latest_ptr_obj_v2(
    *,
    produced_utc: str,
    day_utc: str,
    producer_repo: str,
    producer_git_sha: str,
    producer_module: str,
    status: str,
    reason_codes: List[str],
    snapshot_path: str,
    snapshot_sha256: str,
) -> Dict[str, Any]:
    return {
        "schema_id": "C2_POSITIONS_LATEST_POINTER_V2",
        "schema_version": 2,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_git_sha, "module": producer_module},
        "status": status,
        "reason_codes": reason_codes,
        "pointers": {"snapshot_path": snapshot_path, "snapshot_sha256": snapshot_sha256},
    }



def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_positions_snapshot_day_v2",
        description="C2 Positions Snapshot Truth Spine v2 (bootstrap: UNKNOWN instruments).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()

    dp_exec = exec_day_paths_v1(day_utc)
    dp_pos = day_paths_v2(day_utc)

    # Fail-closed: if snapshot already exists, lock producer git sha to the original.
    if dp_pos.snapshot_path.exists() and dp_pos.snapshot_path.is_file():
        try:
            ex = _read_json_obj(dp_pos.snapshot_path)
            ex_prod = ex.get("producer") if isinstance(ex, dict) else None
            ex_sha = ex_prod.get("git_sha") if isinstance(ex_prod, dict) else None
            if isinstance(ex_sha, str) and ex_sha.strip():
                if ex_sha.strip() != producer_sha:
                    print(
                        f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha.strip()} provided={producer_sha}",
                        file=sys.stderr,
                    )
                    return 4
        except Exception:
            print("FAIL: EXISTING_SNAPSHOT_UNREADABLE_FOR_SHA_LOCK", file=sys.stderr)
            return 4

    if not dp_exec.submissions_day_dir.exists():
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["EXECUTION_EVIDENCE_DAY_DIR_MISSING"],
            input_manifest=[{"type": "execution_evidence", "path": str(dp_exec.submissions_day_dir), "sha256": "0"*64, "day_utc": day_utc, "producer": "execution_evidence_v1"}],
            code="FAIL_CORRUPT_INPUTS",
            message=f"Missing execution evidence day directory: {str(dp_exec.submissions_day_dir)}",
            details={"missing_path": str(dp_exec.submissions_day_dir)},
            attempted_outputs=[{"path": str(dp_pos.snapshot_path), "sha256": None}, {"path": str(dp_pos.latest_path), "sha256": None}],
        )
        _ = write_failure_immutable_v1(failure_path=dp_pos.failure_path, failure_obj=failure)
        print("FAIL: EXECUTION_EVIDENCE_DAY_DIR_MISSING (failure artifact written)")
        return 2

    # Build one position per submission with an execution event.
    items: List[Dict[str, Any]] = []
    reason_codes: List[str] = ["BOOTSTRAP_UNKNOWN_INSTRUMENT_V2"]
    status = "DEGRADED_MISSING_INSTRUMENT_IDENTITY"

    sub_dirs = sorted([p for p in dp_exec.submissions_day_dir.iterdir() if p.is_dir()], key=lambda p: p.name)
    for sd in sub_dirs:
        submission_id = sd.name.strip()
        p_exec = sd / "execution_event_record.v1.json"
        if not p_exec.exists():
            continue
        evt = _read_json_obj(p_exec)
        validate_against_repo_schema_v1(evt, REPO_ROOT, "constellation_2/schemas/execution_event_record.v1.schema.json")

        qty = int(evt["filled_qty"])
        avg_cents = _parse_price_to_cents(str(evt["avg_price"]))

        # Deterministic identity: use binding_hash if present; else submission_id.
        pos_id = str(evt.get("binding_hash") or submission_id)

        items.append(
            {
                "position_id": pos_id,
                "engine_id": "unknown",
                "instrument": {"kind": "UNKNOWN", "underlying": None, "expiry": None, "strike": None, "right": None},
                "qty": qty,
                "avg_cost_cents": avg_cents,
                "market_exposure_type": "UNDEFINED_RISK",
                "max_loss_cents": None,
                "opened_day_utc": day_utc,
                "status": "OPEN",
            }
        )

    snapshot_obj: Dict[str, Any] = {
        "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
        "schema_version": 2,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_sha,
            "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
        },
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "input_manifest": [
            {"type": "execution_evidence", "path": str(dp_exec.submissions_day_dir), "sha256": "0"*64, "day_utc": day_utc, "producer": "execution_evidence_v1"}
        ],
        "positions": {
            "currency": "USD",
            "asof_utc": f"{day_utc}T00:00:00Z",
            "items": items,
            "notes": ["bootstrap: instrument identity unavailable in execution evidence v1; emitting UNKNOWN instruments"],
        },
    }

    validate_against_repo_schema_v1(snapshot_obj, REPO_ROOT, SCHEMA_POSITIONS_SNAPSHOT_V2)
    try:
        snap_bytes = canonical_json_bytes_v1(snapshot_obj) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: SNAPSHOT_CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        wr_snap = write_file_immutable_v1(path=dp_pos.snapshot_path, data=snap_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    latest_obj = build_latest_ptr_obj_v2(
        produced_utc=f"{day_utc}T00:00:00Z",
        day_utc=day_utc,
        producer_repo=producer_repo,
        producer_git_sha=producer_sha,
        producer_module="constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
        status=status,
        reason_codes=sorted(set(reason_codes)),
        snapshot_path=str(dp_pos.snapshot_path),
        snapshot_sha256=wr_snap.sha256,
    )

    validate_against_repo_schema_v1(latest_obj, REPO_ROOT, SCHEMA_POSITIONS_LATEST_PTR_V2)
    latest_bytes = canonical_json_bytes_v1(latest_obj) + b"\n"
    try:
        _ = write_file_immutable_v1(path=dp_pos.latest_path, data=latest_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    print("OK: POSITIONS_SNAPSHOT_V2_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
