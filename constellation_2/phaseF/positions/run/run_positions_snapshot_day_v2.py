from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

_REPO_ROOT_BOOTSTRAP = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT_BOOTSTRAP) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_BOOTSTRAP))

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root
from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.phaseF.execution_evidence.lib.paths_v1 import day_paths_v1 as exec_day_paths_v1
from constellation_2.phaseF.positions.lib.paths_v2 import REPO_ROOT, day_paths_v2
from constellation_2.phaseF.positions.lib.paths_v4 import day_paths_v4
from constellation_2.phaseF.positions.lib.write_failure_v1 import build_failure_obj_v1, write_failure_immutable_v1

SCHEMA_POSITIONS_SNAPSHOT_V2 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v2.schema.json"
SCHEMA_POSITIONS_SNAPSHOT_V5 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json"
SCHEMA_POSITIONS_LATEST_PTR_V2 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_latest_pointer.v2.schema.json"
SCHEMA_POSITIONS_SNAPSHOT_V4 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v4.schema.json"
EQUITY_ORDER_PLAN_V1_SCHEMA = "constellation_2/schemas/equity_order_plan.v1.schema.json"
EQUITY_ORDER_PLAN_V2_SCHEMA = "constellation_2/schemas/equity_order_plan.v2.schema.json"
ZERO_SHA = "0" * 64


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_price_to_cents(price_str: str) -> int:
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
        raise ValueError("AVG_PRICE_TOO_MANY_DECIMALS")
    frac2 = (frac + "00")[:2]
    return int(whole) * 100 + int(frac2)


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_bytes_replace(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(data)
    tmp.replace(path)


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


def _items_by_position_id(snapshot_obj: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    positions = snapshot_obj.get("positions") if isinstance(snapshot_obj.get("positions"), dict) else {}
    items = positions.get("items") if isinstance(positions, dict) else []
    if not isinstance(items, list):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        position_id = str(item.get("position_id") or "").strip()
        if position_id:
            out[position_id] = item
    return out


def _is_unknown_instrument_v2(instr: Dict[str, Any]) -> bool:
    return (
        str(instr.get("kind") or "").strip().upper() == "UNKNOWN"
        and instr.get("underlying") in (None, "")
        and instr.get("expiry") in (None, "")
        and instr.get("strike") in (None, "")
        and instr.get("right") in (None, "")
    )


def _is_zero_qty_open_item(item: Dict[str, Any]) -> bool:
    try:
        qty = int(item.get("qty") or 0)
    except Exception:
        return False
    return str(item.get("status") or "").strip().upper() == "OPEN" and qty <= 0


def _is_safe_item_identity_upgrade(existing_item: Dict[str, Any], candidate_item: Dict[str, Any]) -> bool:
    for field in (
        "position_id",
        "qty",
        "avg_cost_cents",
        "market_exposure_type",
        "max_loss_cents",
        "opened_day_utc",
        "status",
    ):
        if existing_item.get(field) != candidate_item.get(field):
            return False

    existing_engine = str(existing_item.get("engine_id") or "").strip()
    candidate_engine = str(candidate_item.get("engine_id") or "").strip()
    if existing_engine != candidate_engine and existing_engine not in {"", "unknown"}:
        return False

    existing_instr = existing_item.get("instrument") if isinstance(existing_item.get("instrument"), dict) else {}
    candidate_instr = candidate_item.get("instrument") if isinstance(candidate_item.get("instrument"), dict) else {}
    if existing_instr != candidate_instr and not _is_unknown_instrument_v2(existing_instr):
        return False
    return True


def _is_safe_backfill_upgrade(existing_obj: Dict[str, Any], candidate_obj: Dict[str, Any]) -> bool:
    if str(existing_obj.get("schema_id") or "").strip() != "C2_POSITIONS_SNAPSHOT_V2":
        return False
    if str(candidate_obj.get("schema_id") or "").strip() != "C2_POSITIONS_SNAPSHOT_V2":
        return False
    if str(existing_obj.get("day_utc") or "").strip() != str(candidate_obj.get("day_utc") or "").strip():
        return False

    existing_items = _items_by_position_id(existing_obj)
    candidate_items = _items_by_position_id(candidate_obj)
    removed_position_ids = set(existing_items) - set(candidate_items)
    for position_id in removed_position_ids:
        if not _is_zero_qty_open_item(existing_items[position_id]):
            return False

    for position_id, existing_item in existing_items.items():
        candidate_item = candidate_items.get(position_id)
        if position_id in removed_position_ids:
            continue
        if not isinstance(candidate_item, dict):
            return False
        if not _is_safe_item_identity_upgrade(existing_item, candidate_item):
            return False
    return True


def _safe_idle_snapshot_obj(
    *,
    day_utc: str,
    producer_sha: str,
    producer_repo: str,
    exec_day_dir: Path,
    exec_manifest_sha: str,
) -> Dict[str, Any]:
    return {
        "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
        "schema_version": 2,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_sha,
            "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
        },
        "status": "OK",
        "reason_codes": ["NO_SUBMISSIONS_EMPTY_POSITIONS_V2"],
        "input_manifest": [
            {
                "type": "execution_evidence",
                "path": str(exec_day_dir),
                "sha256": exec_manifest_sha,
                "day_utc": day_utc,
                "producer": "execution_evidence_v1",
            }
        ],
        "positions": {
            "currency": "USD",
            "asof_utc": f"{day_utc}T00:00:00Z",
            "items": [],
            "notes": ["SAFE_IDLE: no submissions present for day; emitting empty positions snapshot (v2)"],
        },
    }


def _v2_instrument_from_v4(instr: Dict[str, Any]) -> Dict[str, Any]:
    kind = str(instr.get("kind") or "").strip().upper()
    if kind == "EQUITY":
        return {
            "kind": "EQUITY",
            "underlying": str(instr.get("symbol") or "").strip() or None,
            "expiry": None,
            "strike": None,
            "right": None,
        }
    if kind in {"OPTION_SINGLE", "OPTION_MULTI"}:
        summary = instr.get("summary") if isinstance(instr.get("summary"), dict) else {}
        return {
            "kind": "OPTION",
            "underlying": str(instr.get("underlying") or "").strip() or None,
            "expiry": str(summary.get("expiry_utc") or "").strip() or None,
            "strike": str(summary.get("strike") or "").strip() or None,
            "right": str(summary.get("right") or "").strip() or None,
        }
    return {
        "kind": "UNKNOWN",
        "underlying": None,
        "expiry": None,
        "strike": None,
        "right": None,
    }


def _resolve_previous_day_v4_path(day_utc: str) -> Path | None:
    prev_day = (date.fromisoformat(day_utc) - timedelta(days=1)).isoformat()
    local_prev_v4_path = day_paths_v4(prev_day).snapshot_path
    if local_prev_v4_path.exists() and local_prev_v4_path.is_file():
        return local_prev_v4_path

    current_truth_root = resolve_truth_root(repo_root=REPO_ROOT)
    canonical_truth_root = resolve_canonical_truth_root()
    if current_truth_root == canonical_truth_root:
        return None

    canonical_prev_v4_path = (
        canonical_truth_root / "positions_v1" / "snapshots" / prev_day / "positions_snapshot.v4.json"
    ).resolve()
    if canonical_prev_v4_path.exists() and canonical_prev_v4_path.is_file():
        return canonical_prev_v4_path
    return None


def _carry_forward_open_positions_items_v2(day_utc: str) -> tuple[Path | None, List[Dict[str, Any]]]:
    prev_day = (date.fromisoformat(day_utc) - timedelta(days=1)).isoformat()
    prev_v4_path = _resolve_previous_day_v4_path(day_utc)
    if prev_v4_path is None:
        return None, []
    if not prev_v4_path.exists() or not prev_v4_path.is_file():
        return None, []

    prev_obj = _read_json_obj(prev_v4_path)
    validate_against_repo_schema_v1(prev_obj, REPO_ROOT, SCHEMA_POSITIONS_SNAPSHOT_V4)
    positions = prev_obj.get("positions") if isinstance(prev_obj.get("positions"), dict) else {}
    items = positions.get("items") if isinstance(positions, dict) else []
    if not isinstance(items, list):
        return prev_v4_path, []

    carried: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "").strip().upper() != "OPEN":
            continue
        qty = int(item.get("qty") or 0)
        if qty <= 0:
            continue
        instr = item.get("instrument") if isinstance(item.get("instrument"), dict) else {}
        carried.append(
            {
                "position_id": str(item.get("position_id") or "").strip(),
                "engine_id": str(item.get("engine_id") or "unknown").strip() or "unknown",
                "instrument": _v2_instrument_from_v4(instr),
                "qty": qty,
                "avg_cost_cents": int(item.get("avg_cost_cents") or 0),
                "market_exposure_type": str(item.get("market_exposure_type") or "UNDEFINED_RISK").strip() or "UNDEFINED_RISK",
                "max_loss_cents": item.get("max_loss_cents") if isinstance(item.get("max_loss_cents"), int) else None,
                "opened_day_utc": str(item.get("opened_day_utc") or prev_day).strip() or prev_day,
                "status": "OPEN",
            }
        )
    return prev_v4_path, carried


def _carry_forward_snapshot_obj(
    *,
    day_utc: str,
    producer_sha: str,
    producer_repo: str,
    exec_day_dir: Path,
    exec_manifest_sha: str,
    previous_snapshot_path: Path,
    carried_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
        "schema_version": 2,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": {
            "repo": producer_repo,
            "git_sha": producer_sha,
            "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
        },
        "status": "OK",
        "reason_codes": ["CARRY_FORWARD_OPEN_POSITIONS_V2"],
        "input_manifest": [
            {
                "type": "execution_evidence",
                "path": str(exec_day_dir),
                "sha256": exec_manifest_sha,
                "day_utc": day_utc,
                "producer": "execution_evidence_v1",
            },
            {
                "type": "other",
                "path": str(previous_snapshot_path),
                "sha256": _sha256_bytes(previous_snapshot_path.read_bytes()),
                "day_utc": previous_snapshot_path.parent.name,
                "producer": "positions_v1_snapshot_v4",
            },
        ],
        "positions": {
            "currency": "USD",
            "asof_utc": f"{day_utc}T00:00:00Z",
            "items": carried_items,
            "notes": ["CARRY_FORWARD_OPEN_POSITIONS_V2: sourced from previous-day authoritative positions snapshot"],
        },
    }


def _unknown_instrument_v2() -> Dict[str, Any]:
    return {
        "kind": "UNKNOWN",
        "underlying": None,
        "expiry": None,
        "strike": None,
        "right": None,
    }


def _equity_identity_from_submission_dir(sd: Path) -> tuple[str, Dict[str, Any]]:
    p_ep_v2 = sd / "equity_order_plan.v2.json"
    p_ep_v1 = sd / "equity_order_plan.v1.json"
    if p_ep_v2.exists():
        ep = _read_json_obj(p_ep_v2)
        validate_against_repo_schema_v1(ep, REPO_ROOT, EQUITY_ORDER_PLAN_V2_SCHEMA)
        engine_id = str(ep.get("engine_id") or "").strip() or "unknown"
        underlying = str(ep.get("symbol") or "").strip() or None
        return engine_id, {
            "kind": "EQUITY" if underlying else "UNKNOWN",
            "underlying": underlying,
            "expiry": None,
            "strike": None,
            "right": None,
        }
    if p_ep_v1.exists():
        ep = _read_json_obj(p_ep_v1)
        schema_version = str(ep.get("schema_version") or "").strip()
        if schema_version == "v1":
            validate_against_repo_schema_v1(ep, REPO_ROOT, EQUITY_ORDER_PLAN_V1_SCHEMA)
        underlying = str(ep.get("symbol") or "").strip() or None
        engine_id = str(ep.get("engine_id") or "").strip() or "unknown"
        return engine_id, {
            "kind": "EQUITY" if underlying else "UNKNOWN",
            "underlying": underlying,
            "expiry": None,
            "strike": None,
            "right": None,
        }
    return "unknown", _unknown_instrument_v2()


def _resolve_bridge_truth_root(raw: str) -> Path:
    if str(raw or "").strip():
        path = Path(str(raw).strip()).expanduser().resolve()
        if not path.exists() or not path.is_dir():
            raise SystemExit(f"FAIL: TRUTH_ROOT_INVALID:{path}")
        return path
    return resolve_truth_root(repo_root=REPO_ROOT)


def _v2_instrument_from_v5(instr: Dict[str, Any]) -> Dict[str, Any]:
    kind = str(instr.get("kind") or "").strip().upper()
    if kind == "EQUITY":
        return {
            "kind": "EQUITY",
            "underlying": str(instr.get("symbol") or "").strip() or None,
            "expiry": None,
            "strike": None,
            "right": None,
        }
    if kind.startswith("OPTION"):
        summary = instr.get("summary") if isinstance(instr.get("summary"), dict) else {}
        return {
            "kind": "OPTION",
            "underlying": str(instr.get("underlying") or "").strip() or None,
            "expiry": str(summary.get("expiry_utc") or "").strip() or None,
            "strike": str(summary.get("strike") or "").strip() or None,
            "right": str(summary.get("right") or "").strip() or None,
        }
    return {
        "kind": "UNKNOWN",
        "underlying": str(instr.get("symbol") or instr.get("underlying") or "").strip() or None,
        "expiry": None,
        "strike": None,
        "right": None,
    }


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_positions_snapshot_day_v2",
        description="C2 Positions Snapshot v2 compatibility bridge over canonical positions_snapshot.v5.",
    )
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--producer_git_sha", required=True)
    ap.add_argument("--producer_repo", default="constellation_2_runtime")
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()
    truth_root = _resolve_bridge_truth_root(str(args.truth_root))
    v5_path = (truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json").resolve()
    dp_pos = day_paths_v2(day_utc)
    out_path = (truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json").resolve()
    latest_ptr = (truth_root / "positions_v1" / "latest_pointer.v2.json").resolve()
    failure_path = (truth_root / "positions_v1" / "failures" / day_utc / "positions_snapshot.v2.failure.json").resolve()

    try:
        v5_obj = _read_json_obj(v5_path)
        validate_against_repo_schema_v1(v5_obj, REPO_ROOT, SCHEMA_POSITIONS_SNAPSHOT_V5)
        items_v5 = v5_obj.get("items")
        if not isinstance(items_v5, list):
            raise ValueError("V5_ITEMS_NOT_LIST")

        items: List[Dict[str, Any]] = []
        for item in items_v5:
            if not isinstance(item, dict):
                continue
            instrument = item.get("instrument") if isinstance(item.get("instrument"), dict) else {}
            items.append(
                {
                    "position_id": str(item.get("position_id") or "").strip(),
                    "engine_id": str(item.get("engine_id") or "unknown").strip() or "unknown",
                    "instrument": _v2_instrument_from_v5(instrument),
                    "qty": int(item.get("qty") or 0),
                    "avg_cost_cents": int(item.get("avg_cost_cents") or 0),
                    "market_exposure_type": "UNDEFINED_RISK",
                    "max_loss_cents": None,
                    "opened_day_utc": str(item.get("opened_day_utc") or day_utc).strip() or day_utc,
                    "status": str(item.get("status") or "OPEN").strip().upper(),
                }
            )

        snap_obj = {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
            "schema_version": 2,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "day_utc": day_utc,
            "producer": {
                "repo": producer_repo,
                "git_sha": producer_sha,
                "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
            },
            "status": "OK",
            "reason_codes": ["COMPAT_BRIDGE_FROM_POSITIONS_V5"],
            "input_manifest": [
                {
                    "type": "other",
                    "path": str(v5_path),
                    "sha256": _sha256_bytes(v5_path.read_bytes()),
                    "day_utc": day_utc,
                    "producer": "positions_snapshot_v5",
                }
            ],
            "positions": {
                "currency": str(((v5_obj.get("accounts") or [{}])[0] or {}).get("currency") or "USD").strip() or "USD",
                "asof_utc": f"{day_utc}T00:00:00Z",
                "items": items,
                "notes": ["COMPATIBILITY BRIDGE: derived only from canonical positions_snapshot.v5"],
            },
        }

        validate_against_repo_schema_v1(snap_obj, REPO_ROOT, SCHEMA_POSITIONS_SNAPSHOT_V2)
        snap_bytes = canonical_json_bytes_v1(snap_obj) + b"\n"
        wr_snap = write_file_immutable_v1(path=out_path, data=snap_bytes, create_dirs=True)

        latest_obj = build_latest_ptr_obj_v2(
            produced_utc=f"{day_utc}T00:00:00Z",
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
            status="OK",
            reason_codes=["COMPAT_BRIDGE_FROM_POSITIONS_V5"],
            snapshot_path=str(out_path),
            snapshot_sha256=wr_snap.sha256,
        )
        validate_against_repo_schema_v1(latest_obj, REPO_ROOT, SCHEMA_POSITIONS_LATEST_PTR_V2)
        latest_bytes = canonical_json_bytes_v1(latest_obj) + b"\n"
        if not latest_ptr.exists():
            _ = write_file_immutable_v1(path=latest_ptr, data=latest_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4
    except CanonicalizationError as e:
        print(f"FAIL: SNAPSHOT_CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4
    except Exception as exc:  # noqa: BLE001
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["V2_COMPAT_BRIDGE_FAILED"],
            input_manifest=[{"type": "other", "path": str(v5_path), "sha256": ZERO_SHA, "day_utc": day_utc, "producer": "positions_snapshot_v5"}],
            code="FAIL_CORRUPT_INPUTS",
            message=repr(exc),
            details={"day_utc": day_utc, "truth_root": str(truth_root)},
            attempted_outputs=[{"path": str(out_path), "sha256": None}],
        )
        _ = write_failure_immutable_v1(failure_path=failure_path, failure_obj=failure)
        print(f"FAIL: V2_COMPAT_BRIDGE_FAILED: {exc}", file=sys.stderr)
        return 2

    print("OK: POSITIONS_SNAPSHOT_V2_COMPAT_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
