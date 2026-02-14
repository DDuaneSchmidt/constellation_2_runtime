from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.execution_evidence.lib.paths_v1 import day_paths_v1 as exec_day_paths_v1
from constellation_2.phaseF.positions.lib.paths_effective_v1 import day_paths_effective_v1 as pos_eff_day_paths_v1

from constellation_2.phaseF.position_lifecycle.lib.paths_v1 import REPO_ROOT, day_paths_v1 as lifecycle_day_paths_v1


SCHEMA_LIFECYCLE_SNAPSHOT_V1 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/position_lifecycle_snapshot.v1.schema.json"
SCHEMA_LIFECYCLE_LATEST_V1 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/position_lifecycle_latest_pointer.v1.schema.json"
SCHEMA_LIFECYCLE_FAILURE_V1 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/position_lifecycle_failure.v1.schema.json"

SCHEMA_POS_EFF_PTR_V1 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_effective_pointer.v1.schema.json"
SCHEMA_POS_V3 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v3.schema.json"
SCHEMA_POS_V2 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v2.schema.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _lock_git_sha_if_exists(existing_path: Path, provided_sha: str) -> Optional[str]:
    if existing_path.exists() and existing_path.is_file():
        ex = _read_json_obj(existing_path)
        prod = ex.get("producer")
        ex_sha = prod.get("git_sha") if isinstance(prod, dict) else None
        if isinstance(ex_sha, str) and ex_sha.strip():
            if ex_sha.strip() != provided_sha:
                return ex_sha.strip()
    return None


def _write_failure(
    *,
    out: Path,
    day_utc: str,
    producer_repo: str,
    producer_sha: str,
    module: str,
    reason_codes: List[str],
    input_manifest: List[Dict[str, Any]],
    code: str,
    message: str,
    details: Dict[str, Any],
    attempted_outputs: List[Dict[str, Any]],
) -> int:
    failure_obj: Dict[str, Any] = {
        "schema_id": "C2_POSITION_LIFECYCLE_FAILURE_V1",
        "schema_version": 1,
        "produced_utc": _utc_now_iso(),
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": "FAIL_CORRUPT_INPUTS",
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "failure": {"code": code, "message": message, "details": details, "attempted_outputs": attempted_outputs},
    }
    validate_against_repo_schema_v1(failure_obj, REPO_ROOT, SCHEMA_LIFECYCLE_FAILURE_V1)
    b = canonical_json_bytes_v1(failure_obj) + b"\n"
    _ = write_file_immutable_v1(path=out, data=b, create_dirs=True)
    return 2


def _validate_positions_snapshot(obj: Dict[str, Any]) -> Tuple[str, int]:
    sid = str(obj.get("schema_id") or "").strip()
    sver = int(obj.get("schema_version") or 0)
    if sid == "C2_POSITIONS_SNAPSHOT_V3" and sver == 3:
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_POS_V3)
        return (sid, sver)
    if sid == "C2_POSITIONS_SNAPSHOT_V2" and sver == 2:
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_POS_V2)
        return (sid, sver)
    raise ValueError(f"UNSUPPORTED_POSITIONS_SCHEMA: {sid} v{sver}")


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_position_lifecycle_day_v1",
        description="C2 Position Lifecycle v1 (bootstrap: OPEN-only; links to execution events; immutable).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()
    module = "constellation_2/phaseF/position_lifecycle/run/run_position_lifecycle_day_v1.py"

    out = lifecycle_day_paths_v1(day_utc)

    # Producer sha lock is DAY-SCOPED (snapshot only). Global latest.json is not written in strict immutability mode.
    ex_sha = _lock_git_sha_if_exists(out.snapshot_path, producer_sha)
    if ex_sha is not None:
        print(f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha} provided={producer_sha}", file=sys.stderr)
        return 4

    dp_exec = exec_day_paths_v1(day_utc)
    dp_pos_eff = pos_eff_day_paths_v1(day_utc)

    attempted_outputs = [{"path": str(out.snapshot_path), "sha256": None}]

    # Required: positions effective pointer for the day
    if not dp_pos_eff.pointer_path.exists():
        return _write_failure(
            out=out.failure_path,
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_sha=producer_sha,
            module=module,
            reason_codes=["POSITIONS_EFFECTIVE_POINTER_MISSING"],
            input_manifest=[
                {"type": "positions_effective_pointer", "path": str(dp_pos_eff.pointer_path), "sha256": "0" * 64, "day_utc": day_utc, "producer": "positions_effective_v1"}
            ],
            code="FAIL_CORRUPT_INPUTS",
            message="Missing positions effective pointer for day",
            details={"missing_path": str(dp_pos_eff.pointer_path)},
            attempted_outputs=attempted_outputs,
        )

    # Required: execution evidence submissions day dir
    if not dp_exec.submissions_day_dir.exists():
        return _write_failure(
            out=out.failure_path,
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_sha=producer_sha,
            module=module,
            reason_codes=["EXECUTION_EVIDENCE_DAY_DIR_MISSING"],
            input_manifest=[
                {"type": "execution_evidence_day_dir", "path": str(dp_exec.submissions_day_dir), "sha256": "0" * 64, "day_utc": day_utc, "producer": "execution_evidence_v1"}
            ],
            code="FAIL_CORRUPT_INPUTS",
            message="Missing execution evidence submissions directory for day",
            details={"missing_path": str(dp_exec.submissions_day_dir)},
            attempted_outputs=attempted_outputs,
        )

    # Load effective pointer and selected positions snapshot
    try:
        eff = _read_json_obj(dp_pos_eff.pointer_path)
        validate_against_repo_schema_v1(eff, REPO_ROOT, SCHEMA_POS_EFF_PTR_V1)
        snap_path_s = str(((eff.get("pointers") or {}).get("snapshot_path") or "")).strip()
        if not snap_path_s:
            raise ValueError("EFFECTIVE_POINTER_MISSING_SNAPSHOT_PATH")
        snap_path = Path(snap_path_s).resolve()
        pos = _read_json_obj(snap_path)
        _ = _validate_positions_snapshot(pos)
    except Exception as e:
        return _write_failure(
            out=out.failure_path,
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_sha=producer_sha,
            module=module,
            reason_codes=["POSITIONS_INPUT_INVALID"],
            input_manifest=[
                {"type": "positions_effective_pointer", "path": str(dp_pos_eff.pointer_path), "sha256": "0" * 64, "day_utc": day_utc, "producer": "positions_effective_v1"}
            ],
            code="FAIL_CORRUPT_INPUTS",
            message=str(e),
            details={"error": str(e)},
            attempted_outputs=attempted_outputs,
        )

    # Build map: binding_hash -> list of event pointers
    events_by_binding: Dict[str, List[Dict[str, Any]]] = {}
    exec_sub_dirs = sorted([p for p in dp_exec.submissions_day_dir.iterdir() if p.is_dir()], key=lambda p: p.name)
    for sd in exec_sub_dirs:
        sub_id = sd.name.strip()
        p_evt = sd / "execution_event_record.v1.json"
        if not p_evt.exists():
            continue
        try:
            evt_obj = _read_json_obj(p_evt)
            validate_against_repo_schema_v1(evt_obj, REPO_ROOT, "constellation_2/schemas/execution_event_record.v1.schema.json")
            bh = str(evt_obj.get("binding_hash") or "").strip()
            if not bh:
                continue
            ptr = {"submission_id": sub_id, "path": str(p_evt.resolve()), "sha256": _sha256_file(p_evt)}
            events_by_binding.setdefault(bh, []).append(ptr)
        except Exception:
            # Fail closed: corrupt event is not ignorable.
            return _write_failure(
                out=out.failure_path,
                day_utc=day_utc,
                producer_repo=producer_repo,
                producer_sha=producer_sha,
                module=module,
                reason_codes=["EXECUTION_EVENT_INVALID"],
                input_manifest=[
                    {"type": "execution_evidence_day_dir", "path": str(dp_exec.submissions_day_dir), "sha256": "0" * 64, "day_utc": day_utc, "producer": "execution_evidence_v1"}
                ],
                code="FAIL_CORRUPT_INPUTS",
                message=f"Invalid execution event: {str(p_evt)}",
                details={"event_path": str(p_evt)},
                attempted_outputs=attempted_outputs,
            )

    pos_items = ((pos.get("positions") or {}).get("items") or [])
    if not isinstance(pos_items, list):
        return _write_failure(
            out=out.failure_path,
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_sha=producer_sha,
            module=module,
            reason_codes=["POSITIONS_ITEMS_INVALID"],
            input_manifest=[{"type": "positions_snapshot", "path": str(snap_path), "sha256": "0" * 64, "day_utc": day_utc, "producer": "positions"}],
            code="FAIL_CORRUPT_INPUTS",
            message="positions.items is not a list",
            details={"snapshot_path": str(snap_path)},
            attempted_outputs=attempted_outputs,
        )

    lifecycle_items: List[Dict[str, Any]] = []
    status = "OK"
    reason_codes: List[str] = ["BOOTSTRAP_OPEN_ONLY_V1"]
    notes: List[str] = ["bootstrap: OPEN-only lifecycle; closes not yet provable"]

    missing_events_count = 0

    for it in pos_items:
        if not isinstance(it, dict):
            continue
        position_id = str(it.get("position_id") or "").strip()
        if not position_id:
            continue

        instr = it.get("instrument")
        qty = int(it.get("qty") or 0)
        avg_cost_cents = int(it.get("avg_cost_cents") or 0)
        opened_day_utc = str(it.get("opened_day_utc") or day_utc).strip() or day_utc

        evs = events_by_binding.get(position_id, [])
        if not evs:
            missing_events_count += 1

        lifecycle_items.append(
            {
                "position_id": position_id,
                "state": "OPEN",
                "opened_day_utc": opened_day_utc,
                "closed_day_utc": None,
                "instrument": instr,
                "qty": qty,
                "avg_cost_cents": avg_cost_cents,
                "events": evs,
                "notes": [] if evs else ["no execution_event_record matched by binding_hash for this position_id"],
            }
        )

    if missing_events_count > 0:
        status = "DEGRADED_MISSING_EXECUTION_EVENTS"
        reason_codes.append("MISSING_EXECUTION_EVENTS_FOR_SOME_POSITIONS")

    input_manifest = [
        {"type": "positions_effective_pointer", "path": str(dp_pos_eff.pointer_path), "sha256": _sha256_file(dp_pos_eff.pointer_path), "day_utc": day_utc, "producer": "positions_effective_v1"},
        {"type": "positions_snapshot", "path": str(snap_path), "sha256": _sha256_file(snap_path), "day_utc": day_utc, "producer": "positions"},
        {"type": "execution_evidence_day_dir", "path": str(dp_exec.submissions_day_dir), "sha256": "0" * 64, "day_utc": day_utc, "producer": "execution_evidence_v1"},
    ]

    snap_obj: Dict[str, Any] = {
        "schema_id": "C2_POSITION_LIFECYCLE_SNAPSHOT_V1",
        "schema_version": 1,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "input_manifest": input_manifest,
        "lifecycle": {"asof_utc": f"{day_utc}T00:00:00Z", "items": lifecycle_items, "notes": notes},
    }

    validate_against_repo_schema_v1(snap_obj, REPO_ROOT, SCHEMA_LIFECYCLE_SNAPSHOT_V1)
    try:
        snap_bytes = canonical_json_bytes_v1(snap_obj) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: SNAPSHOT_CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        _ = write_file_immutable_v1(path=out.snapshot_path, data=snap_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    # NOTE: No global latest.json write.
    # Global latest pointers are incompatible with strict no-overwrite invariants.
    # Downstream spines consume day-scoped snapshots.

    print("OK: POSITION_LIFECYCLE_SNAPSHOT_V1_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
