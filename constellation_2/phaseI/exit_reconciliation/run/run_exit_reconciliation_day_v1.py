#!/usr/bin/env python3
"""
run_exit_reconciliation_day_v1.py

C2 Phase I — Exit Reconciliation Spine V1

Produces deterministic exit obligations from:
- positions snapshot (latest pointer or explicit path)
- engine intents day dir (optional; may contain mixed schemas)

Fail-closed design:
- If positions snapshot missing/unreadable -> FAIL
- If intents dir missing -> DEGRADED (still produce obligations from positions)

No network access.
Stdlib only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class ExitReconError(Exception):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_dir_contents(root: Path) -> str:
    """
    Deterministic directory hash: sha256 of (relative_path + NUL + file_sha256) lines.
    Only includes regular files. Sorted by relative path.
    """
    if not root.exists() or not root.is_dir():
        return ""
    rows: List[Tuple[str, str]] = []
    for p in root.rglob("*"):
        if p.is_file():
            rel = str(p.relative_to(root))
            rows.append((rel, sha256_file(p)))
    rows.sort(key=lambda x: x[0])
    h = hashlib.sha256()
    for rel, fh in rows:
        h.update(rel.encode("utf-8"))
        h.update(b"\x00")
        h.update(fh.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    tmp.write_text(raw, encoding="utf-8")
    os.replace(tmp, path)


def repo_root_from_here() -> Path:
    here = Path(__file__).resolve()
    # .../constellation_2/phaseI/exit_reconciliation/run/run_exit_reconciliation_day_v1.py
    # parents: run(0), exit_reconciliation(1), phaseI(2), constellation_2(3), repo_root(4)
    root = here.parents[4]
    if not (root / ".git").exists():
        raise ExitReconError(f"Derived repo root not a git repo: {root}")
    return root


def load_json(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise ExitReconError(f"Failed reading/parsing JSON: {path}: {e}") from e
    if not isinstance(obj, dict):
        raise ExitReconError(f"JSON root not object: {path}")
    return obj


def read_positions_snapshot_from_latest(repo_root: Path) -> Tuple[Path, Dict[str, Any], str]:
    latest = repo_root / "constellation_2" / "runtime" / "truth" / "positions_v1" / "latest.json"
    if not latest.exists():
        raise ExitReconError(f"positions latest pointer missing: {latest}")
    latest_obj = load_json(latest)
    pointers = latest_obj.get("pointers") or {}
    snap_path = pointers.get("snapshot_path")
    snap_sha = pointers.get("snapshot_sha256")
    if not isinstance(snap_path, str) or snap_path.strip() == "":
        raise ExitReconError(f"positions latest pointer missing snapshot_path: {latest}")
    p = Path(snap_path)
    if not p.exists():
        raise ExitReconError(f"positions snapshot path does not exist: {p}")
    actual_sha = sha256_file(p)
    if isinstance(snap_sha, str) and snap_sha and actual_sha != snap_sha:
        raise ExitReconError(f"positions snapshot sha256 mismatch: expected={snap_sha} actual={actual_sha} path={p}")
    return p, load_json(p), actual_sha


def discover_exposure_intents_in_dir(intents_day_dir: Path) -> Dict[str, Dict[str, Any]]:
    """
    Returns engine_id -> intent object for ExposureIntent v1 only.
    If multiple intents for same engine_id exist, fail-closed.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not intents_day_dir.exists() or not intents_day_dir.is_dir():
        return out

    for p in intents_day_dir.rglob("*.json"):
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        if obj.get("schema_id") != "exposure_intent":
            continue
        if obj.get("schema_version") != "v1":
            continue
        eng = obj.get("engine") or {}
        engine_id = eng.get("engine_id")
        if not isinstance(engine_id, str) or engine_id.strip() == "":
            continue
        if engine_id in out:
            raise ExitReconError(f"Duplicate ExposureIntent v1 for engine_id={engine_id} under {intents_day_dir}")
        out[engine_id] = obj
    return out


def recommended_exposure_type_from_position_item(item: Dict[str, Any]) -> Tuple[str, List[str]]:
    """
    Best-effort mapping from positions snapshot instrument fields.
    Returns (recommended_exposure_type, reasons_additions)
    """
    reasons: List[str] = []
    inst = item.get("instrument") or {}
    kind = inst.get("kind")
    if kind == "EQUITY":
        return "LONG_EQUITY", reasons
    if kind == "OPTION":
        # Defined-risk option structures map to SHORT_VOL_DEFINED for exit obligation purposes.
        # If this is not correct for some engines, Bundle A2 will enforce engine policy registries.
        return "SHORT_VOL_DEFINED", reasons
    reasons.append("BOOTSTRAP_UNKNOWN_INSTRUMENT_KIND")
    return "LONG_EQUITY", reasons


def build_exit_reconciliation(
    repo_root: Path,
    day_utc: str,
    positions_path: Path,
    positions_obj: Dict[str, Any],
    positions_sha256: str,
    intents_day_dir: Optional[Path],
) -> Dict[str, Any]:
    reasons: List[str] = []
    status = "OK"

    intents_sha256 = None
    engine_intents: Dict[str, Dict[str, Any]] = {}
    if intents_day_dir is None:
        status = "DEGRADED_MISSING_ENGINE_INTENTS"
        reasons.append("MISSING_ENGINE_INTENTS_DAY_DIR")
    else:
        if not intents_day_dir.exists():
            status = "DEGRADED_MISSING_ENGINE_INTENTS"
            reasons.append("MISSING_ENGINE_INTENTS_DAY_DIR")
        else:
            intents_sha256 = sha256_dir_contents(intents_day_dir)
            try:
                engine_intents = discover_exposure_intents_in_dir(intents_day_dir)
            except ExitReconError:
                # Duplicate intents is a hard fail: determinism and idempotency violation.
                raise

    pos_block = positions_obj.get("positions") or {}
    currency = pos_block.get("currency")
    if not isinstance(currency, str) or len(currency) != 3:
        status = "FAIL_CORRUPT_INPUTS"
        reasons.append("POSITIONS_CURRENCY_INVALID")

    items = pos_block.get("items")
    if not isinstance(items, list):
        status = "FAIL_CORRUPT_INPUTS"
        reasons.append("POSITIONS_ITEMS_INVALID")
        items = []

    obligations: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "OPEN":
            continue
        engine_id = item.get("engine_id")
        position_id = item.get("position_id")
        inst = item.get("instrument") or {}
        if not isinstance(engine_id, str) or engine_id.strip() == "":
            status = "FAIL_CORRUPT_INPUTS"
            reasons.append("OPEN_POSITION_MISSING_ENGINE_ID")
            continue
        if not isinstance(position_id, str) or position_id.strip() == "":
            status = "FAIL_CORRUPT_INPUTS"
            reasons.append("OPEN_POSITION_MISSING_POSITION_ID")
            continue

        # If engine already emitted an explicit exposure intent today, no obligation.
        if engine_id in engine_intents:
            continue

        rec_type, rec_reasons = recommended_exposure_type_from_position_item(item)
        if rec_reasons:
            if status == "OK":
                status = "DEGRADED_UNKNOWN_INSTRUMENT_FIELDS"
            reasons.extend(rec_reasons)

        underlying = inst.get("underlying")
        if underlying is None or (isinstance(underlying, str) and underlying.strip() == ""):
            if status == "OK":
                status = "DEGRADED_UNKNOWN_INSTRUMENT_FIELDS"
            reasons.append("BOOTSTRAP_UNKNOWN_INSTRUMENT_UNDERLYING")

        obligation = {
            "engine_id": engine_id,
            "position_id": position_id,
            "instrument": {
                "kind": inst.get("kind"),
                "underlying": inst.get("underlying"),
                "expiry": inst.get("expiry"),
                "strike": inst.get("strike"),
                "right": inst.get("right"),
            },
            "currency": currency if isinstance(currency, str) else "USD",
            "recommended_exposure_type": rec_type,
            "recommended_target_notional_pct": "0",
            "reason_code": "ENGINE_SILENCE_REQUIRES_EXPLICIT_EXIT",
            "upstream": {
                "positions_snapshot_sha256": positions_sha256,
                "engine_intents_day_dir_sha256": intents_sha256,
            },
        }
        obligations.append(obligation)

    # Stable ordering for determinism
    obligations.sort(key=lambda o: (o["engine_id"], o["position_id"]))

    # de-dupe reasons while keeping stable order
    seen = set()
    reasons_stable: List[str] = []
    for r in reasons:
        if r not in seen:
            seen.add(r)
            reasons_stable.append(r)

    produced_utc = utc_now_iso()
    git_sha = os.environ.get("GIT_SHA", "").strip()
    if git_sha == "":
        # Best-effort: read from .git/HEAD is non-trivial without invoking git.
        # Fail-closed requirement for audit lineage is enforced later in Bundle A2 gating;
        # for now we allow missing git_sha but surface it as reason.
        reasons_stable.append("PRODUCER_GIT_SHA_MISSING_ENV")

    out = {
        "schema_id": "C2_EXIT_RECONCILIATION_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {
            "repo": "constellation_2_runtime",
            "git_sha": git_sha if git_sha else "UNKNOWN",
            "module": "constellation_2/phaseI/exit_reconciliation/run/run_exit_reconciliation_day_v1.py",
        },
        "status": status,
        "reason_codes": reasons_stable,
        "input_manifest": [
            {
                "type": "positions_snapshot",
                "path": str(positions_path),
                "sha256": positions_sha256,
                "day_utc": day_utc,
                "producer": "positions_snapshot.v2",
            },
            {
                "type": "engine_intents_day_dir",
                "path": str(intents_day_dir) if intents_day_dir else "",
                "sha256": intents_sha256 if intents_sha256 else "0" * 64,
                "day_utc": day_utc,
                "producer": "intents_v1",
            },
        ],
        "obligations": obligations,
    }
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day_utc", required=True, help="Day in UTC YYYY-MM-DD")
    ap.add_argument("--intents_day_dir", required=False, default="", help="Path to intents day directory (optional)")
    ap.add_argument("--positions_snapshot_path", required=False, default="", help="Explicit positions snapshot path (optional)")
    ap.add_argument("--out_path", required=False, default="", help="Output path override (optional)")
    args = ap.parse_args()

    repo_root = repo_root_from_here()

    day_utc = str(args.day_utc).strip()
    if not day_utc or len(day_utc) != 10:
        raise ExitReconError(f"Invalid --day_utc: {day_utc!r}")

    if str(args.positions_snapshot_path).strip():
        positions_path = Path(str(args.positions_snapshot_path).strip())
        if not positions_path.exists():
            raise ExitReconError(f"--positions_snapshot_path does not exist: {positions_path}")
        positions_obj = load_json(positions_path)
        positions_sha = sha256_file(positions_path)
    else:
        positions_path, positions_obj, positions_sha = read_positions_snapshot_from_latest(repo_root)

    intents_day_dir: Optional[Path]
    if str(args.intents_day_dir).strip():
        intents_day_dir = Path(str(args.intents_day_dir).strip())
    else:
        # Default to standard intents snapshots location
        intents_day_dir = repo_root / "constellation_2" / "runtime" / "truth" / "intents_v1" / "snapshots" / day_utc

    out_obj = build_exit_reconciliation(
        repo_root=repo_root,
        day_utc=day_utc,
        positions_path=positions_path,
        positions_obj=positions_obj,
        positions_sha256=positions_sha,
        intents_day_dir=intents_day_dir,
    )

    if str(args.out_path).strip():
        out_path = Path(str(args.out_path).strip())
    else:
        out_path = repo_root / "constellation_2" / "runtime" / "truth" / "exit_reconciliation_v1" / day_utc / "exit_reconciliation.v1.json"

    atomic_write_json(out_path, out_obj)
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
