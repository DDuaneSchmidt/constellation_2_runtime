#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import sys
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.truth_root_v1 import resolve_truth_root

TRUTH_ROOT = resolve_truth_root(repo_root=REPO_ROOT)

def _require_truth_root(raw: str | None) -> Path:
    if raw is None or not str(raw).strip():
        return TRUTH_ROOT
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or (not p.exists()) or (not p.is_dir()):
        raise SystemExit(f"FAIL: invalid --truth_root: {p}")
    return p

def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_runtime_hash(truth_root: Path, day_utc: str) -> str:
    runtime_path = truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc / "runtime_evaluation.v1.json"
    try:
        obj = json.loads(runtime_path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    return str(obj.get("deterministic_output_hash") or obj.get("runtime_evaluation_hash") or "")


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


def _write_nav_report(path: Path, content: bytes) -> None:
    if not path.exists():
        _immut_write(path, content)
        return

    existing = _load_json(path)
    candidate = json.loads(content.decode("utf-8"))
    existing_status = str(existing.get("status") or "").strip().upper()
    candidate_status = str(candidate.get("status") or "").strip().upper()
    if existing_status == "BOOTSTRAP" and candidate_status == "ACTIVE":
        _atomic_write(path, content)
        return
    if existing_status == "ACTIVE" and candidate_status == "ACTIVE" and _active_history_backfill_needed(existing):
        _atomic_write(path, content)
        return
    _immut_write(path, content)


def _load_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _existing_bootstrap_can_be_upgraded(*, existing: Dict[str, Any], truth_root: Path, day_utc: str) -> bool:
    if str(existing.get("status") or "").strip().upper() != "BOOTSTRAP":
        return False

    cash_path = truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json"
    pos_path = truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json"
    if (not cash_path.exists()) or (not pos_path.exists()):
        return False

    try:
        pos = _load_json(pos_path)
        marks_path = truth_root / "market_data_snapshot_v1" / "broker_marks_v1" / day_utc / "broker_marks.v1.json"
        p = pos.get("positions")
        if isinstance(p, dict) and isinstance(p.get("items"), list):
            return (len(p.get("items") or [])) == 0 or marks_path.exists()
        items = pos.get("items")
        if isinstance(items, list):
            return len(items) == 0 or marks_path.exists()
    except Exception:
        return False
    return False


def _active_history_backfill_needed(existing: Dict[str, Any]) -> bool:
    if str(existing.get("status") or "").strip().upper() != "ACTIVE":
        return False
    history = existing.get("history")
    if not isinstance(history, dict):
        return True
    drawdown_pct = history.get("drawdown_pct")
    peak_nav = history.get("peak_nav")
    drawdown_abs = history.get("drawdown_abs")
    if not isinstance(drawdown_pct, str) or not drawdown_pct.strip():
        return True
    if not isinstance(peak_nav, int):
        return True
    if not isinstance(drawdown_abs, int):
        return True
    return False


def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Immutable truth rule (audit-grade):
    - If the report already exists at the day-keyed immutable path, DO NOT rewrite.
    - Treat existing report as authoritative for that day.
    - Return 0 if existing looks valid, else fail-closed.

    Rationale:
    - The service runs with git HEAD; producer_git_sha can change between runs.
    - That makes candidate bytes differ and triggers immutable rewrite failure.
    """
    if not out_path.exists():
        return None

    existing = _load_json(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()

    if schema_id != "C2_ACCOUNTING_NAV_V2":
        raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}")

    if _existing_bootstrap_can_be_upgraded(existing=existing, truth_root=TRUTH_ROOT, day_utc=expected_day_utc):
        print(f"OK: accounting_nav_v2_existing_bootstrap_upgrade_allowed day_utc={expected_day_utc} path={out_path}")
        return None
    if _active_history_backfill_needed(existing):
        print(f"OK: accounting_nav_v2_existing_history_backfill_allowed day_utc={expected_day_utc} path={out_path}")
        return None

    sha = _sha256_file(out_path)
    print(f"OK: accounting_nav_v2_exists day_utc={expected_day_utc} path={out_path} sha256={sha} action=EXISTS")
    return 0

def _d(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _ds(d: Decimal) -> str:
    q = d.quantize(Decimal("0.00000001"))
    s = format(q, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s


DAY0_RC_ALLOWED = "DAY0_BOOTSTRAP_MISSING_CASH_OR_POSITIONS_ALLOWED"

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

def _write_bootstrap_stub(*, out_path: Path, day: str, producer_repo: str, producer_git_sha: str, missing: list[str], marks_path: Path) -> None:
    # Deterministic stub: all values zero, produced_utc is day-keyed.
    out_dir = out_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    input_manifest = []
    for rel in missing:
        input_manifest.append({"type": "missing_required_input", "path": rel, "sha256": "", "day_utc": day, "producer": "UNKNOWN"})

    if marks_path.exists():
        try:
            input_manifest.append({"type": "broker_marks", "path": str(marks_path), "sha256": _sha256_file(marks_path), "day_utc": day, "producer": "broker_marks_v1"})
        except Exception:
            input_manifest.append({"type": "broker_marks", "path": str(marks_path), "sha256": "", "day_utc": day, "producer": "broker_marks_v1"})

    out = {
        "schema_id": "C2_ACCOUNTING_NAV_V2",
        "schema_version": 2,
        "produced_utc": f"{day}T00:00:00Z",
        "day_utc": day,
        "producer": {"repo": str(producer_repo), "git_sha": str(producer_git_sha), "module": "ops/tools/run_accounting_nav_v2_day_v1.py"},
        "status": "BOOTSTRAP",
        "reason_codes": [DAY0_RC_ALLOWED],
        "input_manifest": input_manifest,
        "nav": {
            "currency": "USD",
            "nav_total": 0,
            "cash_total": 0,
            "gross_positions_value": 0,
            "realized_pnl_to_date": 0,
            "unrealized_pnl": 0,
            "components": [],
            "notes": ["DAY0_BOOTSTRAP_STUB_NAV_V2"],
        },
        "history": {
            "peak_nav": 0,
            "drawdown_abs": 0,
            "drawdown_pct": "0.000000",
        },
        "runtime_evaluation_hash": _read_runtime_hash(TRUTH_ROOT, day),
        "source_type": "STATIC_RISK_BUDGET_BOOTSTRAP",
        "cash_hash": "",
        "positions_hash": "",
        "day0_bootstrap_classification": DAY0_RC_ALLOWED,
    }

    _write_nav_report(out_path, _json_bytes(out))
    print(f"OK: wrote {out_path} (DAY0_BOOTSTRAP)")



def main() -> int:
    ap = argparse.ArgumentParser(prog="run_accounting_nav_v2_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--producer_repo", default="constellation_2_runtime")
    ap.add_argument("--producer_git_sha", required=True)
    ap.add_argument("--truth_root", required=False)
    args = ap.parse_args()

    global TRUTH_ROOT
    TRUTH_ROOT = _require_truth_root(args.truth_root)

    day = str(args.day_utc).strip()

    out_dir = TRUTH_ROOT / "accounting_v2" / "nav" / day
    out_path = out_dir / "nav.v2.json"

    existing_rc = _return_if_existing_report(out_path=out_path, expected_day_utc=day)
    if existing_rc is not None:

        return int(existing_rc)

    cash_path = TRUTH_ROOT / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json"
    pos_path = TRUTH_ROOT / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json"
    marks_path = TRUTH_ROOT / "market_data_snapshot_v1" / "broker_marks_v1" / day / "broker_marks.v1.json"

    # Required inputs for all days:
    missing_required: list[str] = []
    for p in [cash_path, pos_path]:
        if not p.exists():
            missing_required.append(str(p.relative_to(TRUTH_ROOT)))

    if missing_required:
        if _bootstrap_window_true(day):
            _write_bootstrap_stub(
                out_path=out_path,
                day=day,
                producer_repo=str(args.producer_repo),
                producer_git_sha=str(args.producer_git_sha),
                missing=list(missing_required),
                marks_path=marks_path,
            )
            return 0
        raise SystemExit("FATAL: missing required inputs: " + ", ".join(missing_required))

    cash = _load_json(cash_path)
    pos = _load_json(pos_path)

    def _extract_position_items(pos_obj: Dict[str, Any]) -> list[Any] | None:
        """
        Return the list of positions items if we can locate it.
        Fail-closed if unknown shape by returning None.
        """
        # Common: {"positions": {"items": [...]}}
        p = pos_obj.get("positions")
        if isinstance(p, dict):
            it = p.get("items")
            if isinstance(it, list):
                return it
        # Alternate: {"items": [...]}
        it2 = pos_obj.get("items")
        if isinstance(it2, list):
            return it2
        return None

    def _item_requires_mark(item: Any) -> bool:
        if not isinstance(item, dict):
            return True
        qty = _d(item.get("qty"))
        if qty != 0:
            return True
        status = str(item.get("status") or "").strip().upper()
        lifecycle_state = str(item.get("lifecycle_state") or "").strip().upper()
        if status == "CLOSED" or lifecycle_state == "CLOSED":
            return False
        # Fail closed for zero-qty rows with ambiguous status.
        return True

    items = _extract_position_items(pos)
    if items is None:
        # Fail-closed: schema unknown → require broker marks (treat as positions potentially present)
        has_positions = True
    else:
        has_positions = any(_item_requires_mark(item) for item in items)

    # Broker marks are required ONLY if positions exist.
    if has_positions and (not marks_path.exists()):
        # If we have positions but marks are missing, allow DAY0 bootstrap only in bootstrap window.
        missing = [str(marks_path.relative_to(TRUTH_ROOT))]
        if _bootstrap_window_true(day):
            _write_bootstrap_stub(
                out_path=out_path,
                day=day,
                producer_repo=str(args.producer_repo),
                producer_git_sha=str(args.producer_git_sha),
                missing=list(missing),
                marks_path=marks_path,
            )
            return 0
        raise SystemExit("FATAL: missing required inputs: " + ", ".join(missing))

    # If no positions, marks are optional; synthesize an empty marks payload.
    if marks_path.exists():
        marks = _load_json(marks_path)
    else:
        marks = {"currency": str(cash.get("snapshot", {}).get("currency") or "USD"), "marks": []}

    cash_total_cents = int(cash["snapshot"]["cash_total_cents"])
    cash_total = int(cash_total_cents // 100)

    components: List[Dict[str, Any]] = [
        {
            "kind": "CASH",
            "symbol": "USD",
            "qty": str(cash_total),
            "mv": cash_total,
            "mark": {"bid": None, "ask": None, "last": None, "source": "CASH_LEDGER", "asof_utc": f"{day}T00:00:00Z"},
        }
    ]

    gross_mv = 0
    unreal = 0

    for m in marks.get("marks", []):
        sym = str(m.get("symbol") or "").strip()
        sec = str(m.get("sec_type") or "").strip()
        qty = _d(m.get("qty"))
        mv = int(_d(m.get("market_value")))
        ip = _d(m.get("implied_price"))
        avg = _d(m.get("avg_cost"))
        gross_mv += mv
        unreal += int(qty * (ip - avg))

        components.append(
            {
                "kind": "BROKER_MARK",
                "symbol": sym,
                "sec_type": sec,
                "qty": _ds(qty),
                "mv": mv,
                "mark": {"bid": None, "ask": None, "last": _ds(ip), "source": "BROKER_MARKS_V1", "asof_utc": f"{day}T00:00:00Z"},
            }
        )

    nav_total = int(cash_total) + int(gross_mv)
    day_date = date.fromisoformat(day)
    prev_day = (day_date - timedelta(days=1)).isoformat()
    prev_nav_path = TRUTH_ROOT / "accounting_v2" / "nav" / prev_day / "nav.v2.json"
    peak_nav = nav_total
    if prev_nav_path.exists() and prev_nav_path.is_file():
        try:
            prev_nav_obj = _load_json(prev_nav_path)
            prev_history = prev_nav_obj.get("history") if isinstance(prev_nav_obj.get("history"), dict) else {}
            prev_peak = prev_history.get("peak_nav")
            if not isinstance(prev_peak, int):
                prev_nav_total = ((prev_nav_obj.get("nav") or {}).get("nav_total")) if isinstance(prev_nav_obj.get("nav"), dict) else None
                prev_peak = prev_nav_total if isinstance(prev_nav_total, int) else nav_total
            peak_nav = max(int(prev_peak), nav_total)
        except Exception:
            peak_nav = nav_total
    drawdown_abs = int(nav_total - peak_nav)
    if peak_nav > 0:
        drawdown_pct = _ds((Decimal(drawdown_abs) / Decimal(peak_nav)).quantize(Decimal("0.000001")))
    else:
        drawdown_pct = "0.000000"

    input_manifest = [
        {"type": "cash_ledger", "path": str(cash_path), "sha256": _sha256_file(cash_path), "day_utc": day, "producer": "cash_ledger_v1"},
        {"type": "positions_truth", "path": str(pos_path), "sha256": _sha256_file(pos_path), "day_utc": day, "producer": "positions_v1"},
    ]
    if marks_path.exists():
        input_manifest.append(
            {"type": "broker_marks", "path": str(marks_path), "sha256": _sha256_file(marks_path), "day_utc": day, "producer": "broker_marks_v1"}
        )

    out = {
        "schema_id": "C2_ACCOUNTING_NAV_V2",
        "schema_version": 2,
        "produced_utc": f"{day}T00:00:00Z",
        "day_utc": day,
        "producer": {"repo": str(args.producer_repo), "git_sha": str(args.producer_git_sha), "module": "ops/tools/run_accounting_nav_v2_day_v1.py"},
        "status": "ACTIVE",
        "reason_codes": ["BROKER_MARKS_SOURCE_V1"],
        "input_manifest": input_manifest,
        "nav": {
            "currency": str(marks.get("currency", "USD")),
            "nav_total": nav_total,
            "cash_total": cash_total,
            "gross_positions_value": int(gross_mv),
            "realized_pnl_to_date": 0,
            "unrealized_pnl": int(unreal),
            "components": components,
            "notes": ["marks derived from broker-of-record (IB Flex)"],
        },
        "history": {
            "peak_nav": int(peak_nav),
            "drawdown_abs": int(drawdown_abs),
            "drawdown_pct": str(drawdown_pct),
        },
        "runtime_evaluation_hash": _read_runtime_hash(TRUTH_ROOT, day),
        "source_type": str(cash.get("source_type") or pos.get("source_type") or "ACCOUNT_EVIDENCE"),
        "cash_hash": _sha256_file(cash_path),
        "positions_hash": _sha256_file(pos_path),
        "day0_bootstrap_classification": "NOT_DAY0_BOOTSTRAP",
    }

    _write_nav_report(out_path, _json_bytes(out))

    print(f"OK: wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
