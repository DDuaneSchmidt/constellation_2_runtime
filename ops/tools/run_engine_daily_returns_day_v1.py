#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_authority_bridge_v1 import resolve_canonical_truth_root_bridge_v1
from constellation_2.common.paper_session_fact_plane_v1 import resolve_paper_intent_truth_root_v1
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance_release_current_first_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

DEFAULT_TRUTH_ROOT = resolve_canonical_truth_root_bridge_v1(
    caller="ops/tools/run_engine_daily_returns_day_v1.py"
).resolve()
_TRUTH_ROOT = DEFAULT_TRUTH_ROOT


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


def _parse_day(day_utc: str) -> date:
    return date.fromisoformat(str(day_utc).strip())


def _resolve_truth_root(raw: str, *, environment: str = "") -> Path:
    text = str(raw or "").strip()
    truth_root = Path(text).expanduser().resolve() if text else DEFAULT_TRUTH_ROOT
    if not text and str(environment or "").strip().upper() == "PAPER":
        truth_root = resolve_paper_intent_truth_root_v1(truth_root=DEFAULT_TRUTH_ROOT, repo_root=REPO_ROOT).resolve()
    if not truth_root.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {truth_root}")
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not directory: {truth_root}")
    return truth_root


def _git_sha() -> str:
    try:
        provenance = resolve_release_provenance_release_current_first_v1(
            caller="ops/tools/run_engine_daily_returns_day_v1.py"
        )
        if isinstance(provenance, dict):
            git_sha = str(provenance.get("git_sha") or "").strip()
            if git_sha:
                return git_sha
    except Exception:
        pass
    try:
        return subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT)).decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _bootstrap_window_true(day_utc: str) -> bool:
    """
    Day-0 Bootstrap Window iff:
      TRUTH/execution_evidence_v1/submissions/<DAY>/ is missing OR contains zero submission dirs.
    """
    root = (_TRUTH_ROOT / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    if (not root.exists()) or (not root.is_dir()):
        return True
    try:
        for p in root.iterdir():
            if p.is_dir():
                return False
    except Exception:
        return False
    return True


def _as_decimal(value: Any) -> Decimal | None:
    text = str(value if value is not None else "").strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _engine_pnl_to_date_map(attribution_doc: Dict[str, Any]) -> Dict[str, Decimal]:
    attribution = attribution_doc.get("attribution") if isinstance(attribution_doc.get("attribution"), dict) else {}
    by_engine = attribution.get("by_engine") if isinstance(attribution.get("by_engine"), list) else []
    out: Dict[str, Decimal] = {}
    for row in by_engine:
        if not isinstance(row, dict):
            continue
        engine_id = str(row.get("engine_id") or "").strip()
        if not engine_id:
            continue
        pnl_to_date = _as_decimal(row.get("pnl_to_date"))
        if pnl_to_date is None:
            realized = _as_decimal(row.get("realized_pnl_to_date"))
            unrealized = _as_decimal(row.get("unrealized_pnl"))
            if realized is not None and unrealized is not None:
                pnl_to_date = realized + unrealized
        if pnl_to_date is None:
            continue
        out[engine_id] = pnl_to_date
    return out


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_engine_daily_returns_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--prev_day_utc", default="", help="Optional previous day; if empty, returns NOT_AVAILABLE")
    ap.add_argument("--environment", default="")
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day = str(args.day_utc).strip()
    prev = str(args.prev_day_utc).strip() or (_parse_day(day) - timedelta(days=1)).isoformat()
    global _TRUTH_ROOT
    _TRUTH_ROOT = _resolve_truth_root(str(args.truth_root), environment=str(args.environment))

    today_path = _TRUTH_ROOT / "accounting_v2" / "attribution" / day / "engine_attribution.v2.json"
    prev_path = _TRUTH_ROOT / "accounting_v2" / "attribution" / prev / "engine_attribution.v2.json" if prev else None
    nav_prev_path = _TRUTH_ROOT / "accounting_v2" / "nav" / prev / "nav.v2.json" if prev else None

    status = "ACTIVE"
    reason_codes: List[str] = []
    returns: List[Dict[str, str]] = []

    if not today_path.exists():
        status = "NOT_AVAILABLE"
        reason_codes.append("MISSING_ATTRIBUTION_TODAY")

    if prev_path is None or not prev_path.exists():
        status = "NOT_AVAILABLE"
        reason_codes.append("MISSING_ATTRIBUTION_PREV")

    if nav_prev_path is None or not nav_prev_path.exists():
        status = "NOT_AVAILABLE"
        reason_codes.append("MISSING_NAV_PREV")

    if status == "ACTIVE":
        o_today = _load_json(today_path)
        o_prev = _load_json(prev_path)  # type: ignore[arg-type]
        nav_prev_doc = _load_json(nav_prev_path)  # type: ignore[arg-type]
        today_status = str(o_today.get("status") or "").strip().upper()
        prev_status = str(o_prev.get("status") or "").strip().upper()
        if today_status not in {"ACTIVE"} or prev_status not in {"ACTIVE"}:
            status = "NOT_AVAILABLE"
            reason_codes.append("ATTRIBUTION_STATUS_NOT_ACTIVE")
        else:
            nav_obj = nav_prev_doc.get("nav") if isinstance(nav_prev_doc.get("nav"), dict) else {}
            nav_prev_total = _as_decimal(nav_obj.get("nav_total"))
            if nav_prev_total is None:
                status = "NOT_AVAILABLE"
                reason_codes.append("NAV_PREV_TOTAL_INVALID")
            elif nav_prev_total <= Decimal("0"):
                status = "NOT_AVAILABLE"
                reason_codes.append("NAV_PREV_NONPOSITIVE")
            else:
                by_today = _engine_pnl_to_date_map(o_today)
                by_prev = _engine_pnl_to_date_map(o_prev)
                engine_ids = sorted(set(by_today.keys()) | set(by_prev.keys()))
                if not engine_ids:
                    status = "NOT_AVAILABLE"
                    reason_codes.append("NO_ENGINE_DATA_SAFE_IDLE")
                else:
                    quant = Decimal("0.00000001")
                    for engine_id in engine_ids:
                        pnl_prev = by_prev.get(engine_id, Decimal("0"))
                        pnl_today = by_today.get(engine_id, Decimal("0"))
                        daily_return = (pnl_today - pnl_prev) / nav_prev_total
                        daily_return_q = daily_return.quantize(quant, rounding=ROUND_HALF_UP)
                        returns.append({"engine_id": engine_id, "daily_return": format(daily_return_q, "f")})

    out = {
        "schema_id": "C2_MONITORING_ENGINE_DAILY_RETURNS_V1",
        "schema_version": "1.0.0",
        "produced_utc": f"{day}T00:00:00Z",
        "day_utc": day,
        "producer": "ops/tools/run_engine_daily_returns_day_v1.py",
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "inputs": {
            "attribution_today_path": str(today_path.relative_to(_TRUTH_ROOT)),
            "attribution_today_sha256": _sha256_file(today_path) if today_path.exists() else "0" * 64,
            "attribution_prev_path": str(prev_path.relative_to(_TRUTH_ROOT)) if prev_path else "",
            "attribution_prev_sha256": _sha256_file(prev_path) if (prev_path and prev_path.exists()) else ("0" * 64 if prev else ""),
        },
        "returns": returns,
    }

    validate_against_repo_schema_v1(out, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/MONITORING/engine_daily_returns.v1.schema.json")

    out_dir = _TRUTH_ROOT / "monitoring_v1" / "engine_daily_returns_v1" / day
    out_path = out_dir / "engine_daily_returns.v1.json"
    if out_path.exists():
        print(f"SKIP: already exists {out_path}")
        # In bootstrap, do not fail the pipeline if this is NOT_AVAILABLE.
        return 0

    _immut_write(out_path, _json_bytes(out))
    print(f"OK: wrote {out_path}")

    # Day-0 bootstrap: NOT_AVAILABLE should not block orchestrator.
    if _bootstrap_window_true(day):
        return 0

    return 0 if status == "ACTIVE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
