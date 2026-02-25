#!/usr/bin/env python3
"""
ops/tools/run_eod_slo_sentinel_v1.py

Truth-only EOD observability:
- Reads EOD run certificate (truth)
- Emits EOD SLO sentinel (truth)

Stdlib only. Fail-closed write: output always written; rc indicates OK/FAIL.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _is_day(s: str) -> bool:
    if not isinstance(s, str):
        return False
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        return False
    y, m, d = s[0:4], s[5:7], s[8:10]
    return y.isdigit() and m.isdigit() and d.isdigit()


def _git_sha(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
        s = out.decode("utf-8", errors="replace").strip()
        return s if s else "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def _read_json(path: Path) -> Tuple[Optional[Dict[str, Any]], str]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            return None, "INVALID_JSON_OBJECT"
        return obj, "OK"
    except FileNotFoundError:
        return None, "MISSING"
    except Exception:
        return None, "UNREADABLE"


def _mkdirp(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    data = json.dumps(obj, indent=2, sort_keys=True) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(str(tmp), str(path))


@dataclass
class EvalResult:
    status: str  # OK|FAIL
    state: str   # OK|CERT_MISSING|CERT_FAIL|LATE
    notes: List[str]
    cert_present: bool
    cert_status: str


def evaluate(
    *,
    truth_root: Path,
    day_utc: str,
    deadline_utc: datetime,
) -> Tuple[EvalResult, Path]:
    cert_path = (truth_root / "reports" / "eod_run_certificate_v1" / day_utc / "eod_run_certificate.v1.json").resolve()
    cert_obj, cert_err = _read_json(cert_path)

    if cert_obj is None:
        now = _utc_now()
        late = now >= deadline_utc
        if late:
            return (
                EvalResult(
                    status="FAIL",
                    state="LATE",
                    notes=[f"CERT_MISSING:{cert_err}", f"deadline_utc={_iso(deadline_utc)}", f"now_utc={_iso(now)}"],
                    cert_present=False,
                    cert_status="MISSING",
                ),
                cert_path,
            )
        return (
            EvalResult(
                status="FAIL",
                state="CERT_MISSING",
                notes=[f"CERT_MISSING:{cert_err}", f"deadline_utc={_iso(deadline_utc)}"],
                cert_present=False,
                cert_status="MISSING",
            ),
            cert_path,
        )

    st = str(cert_obj.get("status") or "").strip().upper() or "UNKNOWN"
    if st == "PASS":
        return (
            EvalResult(
                status="OK",
                state="OK",
                notes=[],
                cert_present=True,
                cert_status="PASS",
            ),
            cert_path,
        )

    # Anything not PASS is treated as fail for observability
    return (
        EvalResult(
            status="FAIL",
            state="CERT_FAIL",
            notes=[f"cert.status={st}"],
            cert_present=True,
            cert_status=st,
        ),
        cert_path,
    )


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_eod_slo_sentinel_v1")
    ap.add_argument("--day_utc", required=True, help="Target day in YYYY-MM-DD (UTC)")
    ap.add_argument(
        "--deadline_utc",
        required=False,
        default="01:30:00Z",
        help='Deadline time in UTC for "today" in HH:MM:SSZ (default 01:30:00Z)',
    )
    ns = ap.parse_args(argv)

    day = str(ns.day_utc).strip()
    if not _is_day(day):
        raise SystemExit(f"FAIL: invalid --day_utc: {day!r}")

    # Derive repo root from this file location: .../ops/tools/run_eod_slo_sentinel_v1.py
    repo_root = Path(__file__).resolve().parents[2]
    truth_root = (repo_root / "constellation_2/runtime/truth").resolve()

    now = _utc_now()
    # Deadline is "today at HH:MM:SSZ"
    hhmmss = str(ns.deadline_utc).strip()
    if not (len(hhmmss) == 9 and hhmmss.endswith("Z") and hhmmss[2] == ":" and hhmmss[5] == ":"):
        raise SystemExit(f"FAIL: invalid --deadline_utc: {hhmmss!r}")
    h = int(hhmmss[0:2])
    m = int(hhmmss[3:5])
    s = int(hhmmss[6:8])
    deadline = datetime(now.year, now.month, now.day, h, m, s, tzinfo=timezone.utc)

    res, cert_path = evaluate(truth_root=truth_root, day_utc=day, deadline_utc=deadline)

    out_dir = (truth_root / "monitoring_v1" / "eod_slo_sentinel_v1" / day).resolve()
    _mkdirp(out_dir)
    out_path = (out_dir / "eod_slo_sentinel.v1.json").resolve()

    doc: Dict[str, Any] = {
        "schema_id": "eod_slo_sentinel",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": _iso(now),
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_eod_slo_sentinel_v1.py", "git_sha": _git_sha(repo_root)},
        "status": res.status,
        "state": res.state,
        "details": {
            "expected_day_utc": day,
            "deadline_utc": _iso(deadline),
            "certificate_path": str(cert_path),
            "certificate_present": bool(res.cert_present),
            "certificate_status": str(res.cert_status),
            "notes": res.notes or [],
        },
    }

    _atomic_write_json(out_path, doc)
    print(f"OK: EOD_SLO_SENTINEL_V1_WRITTEN day_utc={day} status={res.status} state={res.state} path={out_path}")
    return 0 if res.status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
