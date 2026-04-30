#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

from constellation_2.common.paper_session_fact_plane_v1 import resolve_paper_intent_truth_root_v1
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root

REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()
TRUTH_ROOT = resolve_canonical_truth_root().resolve()
REGISTRY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "ENGINE_MODEL_REGISTRY_V1.json"
SIMULATOR_ENGINE_ID = "C2_INTENT_SIMULATOR_V1"

DAY0_RC_ALLOWED = "DAY0_BOOTSTRAP_ATTRIB_DEGRADED_OK"

def _bootstrap_window_true(day_utc: str) -> bool:
    """
    Day-0 Bootstrap Window iff:
      TRUTH/execution_evidence_v1/submissions/<DAY>/ is missing OR contains zero submission dirs.
    """
    root = (TRUTH_ROOT / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    if (not root.exists()) or (not root.is_dir()):
        return True
    try:
        for p in root.iterdir():
            if p.is_dir():
                return False
    except Exception:
        return False
    return True



def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _immut_write(path: Path, content: bytes) -> None:
    if path.exists():
        if hashlib.sha256(path.read_bytes()).hexdigest() != hashlib.sha256(content).hexdigest():
            raise RuntimeError(f"ImmutableWriteError: ATTEMPTED_REWRITE path={path}")
        return
    _atomic_write(path, content)


def _load_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_truth_root(*, environment: str, truth_root_arg: str) -> Path:
    raw = str(truth_root_arg or "").strip()
    if raw:
        root = Path(raw).expanduser().resolve()
    else:
        root = resolve_canonical_truth_root().resolve()
        if str(environment or "").strip().upper() == "PAPER":
            root = resolve_paper_intent_truth_root_v1(truth_root=root, repo_root=REPO_ROOT).resolve()
    if not root.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {root}")
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not directory: {root}")
    return root


def _active_trading_engine_ids() -> List[str]:
    registry = _load_json(REGISTRY_PATH)
    models = registry.get("engines") if isinstance(registry.get("engines"), list) else []
    out: List[str] = []
    for row in models:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("engine_id") or row.get("sleeve_id") or "").strip()
        status = str(row.get("activation_status") or row.get("activation") or row.get("status") or "").strip().upper()
        if not sleeve_id or sleeve_id == SIMULATOR_ENGINE_ID or status != "ACTIVE":
            continue
        out.append(sleeve_id)
    return sorted(set(out))


def _position_items(pos: Dict[str, Any]) -> List[Dict[str, Any]]:
    top_items = pos.get("items")
    if isinstance(top_items, list):
        return [item for item in top_items if isinstance(item, dict)]
    positions = pos.get("positions") if isinstance(pos.get("positions"), dict) else {}
    nested = positions.get("items")
    if isinstance(nested, list):
        return [item for item in nested if isinstance(item, dict)]
    return []

def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Immutable truth rule (audit-grade):
    - If report already exists at day-keyed path, DO NOT rewrite.
    - Treat existing report as authoritative for that day.
    - Return rc based on existing status:
        ACTIVE -> 0
        otherwise -> 2
    """
    if not out_path.exists():
        return None

    existing = _load_json(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()
    status = str(existing.get("status") or "").strip().upper()

    if schema_id != "C2_ACCOUNTING_ENGINE_ATTRIBUTION_V2":
        raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}")
    if status == "":
        raise SystemExit(f"FAIL: EXISTING_REPORT_STATUS_MISSING: path={out_path}")

    sha = _sha256_file(out_path)
    print(f"OK: accounting_attribution_v2_exists day_utc={expected_day_utc} status={status} path={out_path} sha256={sha} action=EXISTS")
    if status == "ACTIVE":
        return 0
    # Day-0 bootstrap: allow DEGRADED_MISSING_INPUTS to pass strict orchestrator stages.
    if status == "DEGRADED_MISSING_INPUTS" and _bootstrap_window_true(expected_day_utc):
        return 0
    return 2

def main() -> int:
    ap = argparse.ArgumentParser(prog="run_accounting_attribution_v2_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--producer_git_sha", required=True)
    ap.add_argument("--producer_repo", required=True)
    ap.add_argument("--environment", default="")
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    global TRUTH_ROOT
    TRUTH_ROOT = _resolve_truth_root(environment=str(args.environment), truth_root_arg=str(args.truth_root))

    out_dir = TRUTH_ROOT / "accounting_v2" / "attribution" / day
    out_path = out_dir / "engine_attribution.v2.json"

    existing_rc = _return_if_existing_report(out_path=out_path, expected_day_utc=day)
    if existing_rc is not None:
        return int(existing_rc)

    pos_v5_path = TRUTH_ROOT / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json"
    pos_v2_path = TRUTH_ROOT / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json"
    pos_path = pos_v5_path if pos_v5_path.exists() else pos_v2_path
    marks_path = TRUTH_ROOT / "market_data_snapshot_v1" / "broker_marks_v1" / day / "broker_marks.v1.json"
    link_path = TRUTH_ROOT / "engine_linkage_v1" / "snapshots" / day / "engine_linkage.v1.json"

    missing = []
    if not pos_path.exists():
        missing.append(str(pos_path.relative_to(TRUTH_ROOT)))

    status = "ACTIVE"
    reason_codes: List[str] = []
    notes: List[str] = []

    if missing:
        status = "DEGRADED_MISSING_INPUTS"
        reason_codes.append("MISSING_INPUTS")
        notes.extend([f"MISSING: {m}" for m in missing])

    by_engine: List[Dict[str, Any]] = []
    currency = "USD"

    if status == "ACTIVE":
        pos = _load_json(pos_path)
        items = _position_items(pos)
        if items:
            for p in [marks_path, link_path]:
                if not p.exists():
                    missing.append(str(p.relative_to(TRUTH_ROOT)))
            if missing:
                status = "DEGRADED_MISSING_INPUTS"
                reason_codes.append("MISSING_INPUTS")
                notes.extend([f"MISSING: {m}" for m in missing])
        if not items:
            reason_codes.append("SAFE_IDLE_EMPTY_POSITIONS")
            notes.append("SAFE_IDLE: positions snapshot empty; zero PnL attributed to all active trading sleeves.")
            by_engine = [
                {
                    "engine_id": engine_id,
                    "realized_pnl_to_date": "0",
                    "unrealized_pnl": "0",
                    "pnl_to_date": "0",
                    "basis": "SAFE_IDLE_EMPTY_POSITIONS",
                }
                for engine_id in _active_trading_engine_ids()
            ]
        elif status == "ACTIVE":
            status = "DEGRADED_NOT_IMPLEMENTED"
            reason_codes.append("JOIN_KEYS_NOT_PROVEN")
            notes.append("Positions present but join keys for linkage+marks not yet proven in this environment.")

    out = {
        "schema_id": "C2_ACCOUNTING_ENGINE_ATTRIBUTION_V2",
        "schema_version": 2,
        "produced_utc": f"{day}T00:00:00Z",
        "day_utc": day,
        "producer": {"repo": args.producer_repo, "git_sha": args.producer_git_sha, "module": "ops/tools/run_accounting_attribution_v2_day_v1.py"},
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "input_manifest": [
            {"type": "positions_truth", "path": str(pos_path), "sha256": _sha256_file(pos_path) if pos_path.exists() else "0" * 64, "day_utc": day, "producer": "positions_v1"},
            {"type": "broker_marks", "path": str(marks_path), "sha256": _sha256_file(marks_path) if marks_path.exists() else "0" * 64, "day_utc": day, "producer": "broker_marks_v1"},
            {"type": "engine_linkage", "path": str(link_path), "sha256": _sha256_file(link_path) if link_path.exists() else "0" * 64, "day_utc": day, "producer": "engine_linkage_v1"},
            {"type": "engine_model_registry", "path": str(REGISTRY_PATH), "sha256": _sha256_file(REGISTRY_PATH), "day_utc": day, "producer": "governance_registry"},
        ],
        "attribution": {
            "currency": currency,
            "by_engine": by_engine,
            "notes": notes,
        },
    }

    _immut_write(out_path, _json_bytes(out))

    print(f"OK: wrote {out_path}")
    if status == "ACTIVE":
        return 0
    if status == "DEGRADED_MISSING_INPUTS" and _bootstrap_window_true(day):
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
