#!/usr/bin/env python3
"""
gen_drawdown_window_pack_v1.py

Computes 30/60/90 drawdown window metrics from NAV History Ledger v1.

Fail-closed:
- Requires >= N observations for each window size
- No floats; canonical JSON; schema validated; immutable write
- Writes day-scoped pack:
  constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/drawdown_window_pack/<DAY>/drawdown_window_pack.v1.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple

DAY_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import (  # type: ignore
    canonical_hash_excluding_fields_v1,
    canonical_json_bytes_v1,
)

TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
BUNDLE_ROOT = TRUTH_ROOT / "monitoring_v1" / "economic_nav_drawdown_v1"

LEDGER_ROOT = BUNDLE_ROOT / "nav_history_ledger"
PACK_ROOT = BUNDLE_ROOT / "drawdown_window_pack"
PACK_NAME = "drawdown_window_pack.v1.json"

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/drawdown_window_pack.v1.schema.json"
CONTRACT_RELPATH = "governance/05_CONTRACTS/C2/drawdown_window_pack_v1.contract.md"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: JSON_NOT_OBJECT: {str(p)}")
    return obj


def _validate_schema_or_fail(obj: Dict[str, Any], schema_path: Path) -> None:
    try:
        import jsonschema  # type: ignore
    except Exception as e:
        raise SystemExit(f"FAIL: jsonschema not available: {e}")

    if not schema_path.exists():
        raise SystemExit(f"FAIL: missing governed schema: {str(schema_path)}")
    schema_obj = _read_json_obj(schema_path)
    try:
        jsonschema.validate(instance=obj, schema=schema_obj)
    except Exception as e:
        raise SystemExit(f"FAIL: schema validation failed: {e}")


@dataclass(frozen=True)
class _WriteResult:
    path: str
    sha256: str
    action: str


def _write_immutable_canon(path: Path, obj: Dict[str, Any]) -> _WriteResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
    sha = _sha256_bytes(payload)

    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return _WriteResult(path=str(path), sha256=sha, action="EXISTS_IDENTICAL")
        raise SystemExit(f"FAIL: refusing overwrite (different bytes): {str(path)}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    os.replace(tmp, path)
    return _WriteResult(path=str(path), sha256=sha, action="WRITTEN")


def _git_sha_failclosed() -> str:
    import subprocess
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception as e:
        raise SystemExit(f"FAIL: cannot read git sha: {e!r}")


def _max_drawdown_pct(points: List[Dict[str, Any]]) -> str:
    # most negative (min) drawdown_pct
    vals: List[Decimal] = []
    for p in points:
        dd = p.get("drawdown_pct")
        if not isinstance(dd, str) or dd.strip() == "":
            raise SystemExit("FAIL: missing drawdown_pct in ledger points")
        vals.append(Decimal(dd))
    m = min(vals)
    # preserve 6dp string
    return f"{m:.6f}"


def main() -> int:
    ap = argparse.ArgumentParser(prog="gen_drawdown_window_pack_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not DAY_RE.match(day):
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    ledger_path = (LEDGER_ROOT / day / "nav_history_ledger.v1.json").resolve()
    if not ledger_path.exists():
        raise SystemExit(f"FAIL: missing required ledger: {str(ledger_path)}")

    ledger = _read_json_obj(ledger_path)
    days = ledger.get("days")
    if not isinstance(days, list) or len(days) < 1:
        raise SystemExit(f"FAIL: ledger days missing/empty: {str(ledger_path)}")

    schema_path = (REPO_ROOT / SCHEMA_RELPATH).resolve()
    contract_path = (REPO_ROOT / CONTRACT_RELPATH).resolve()

    reason_codes: List[str] = []
    windows_out: List[Dict[str, Any]] = []

    def _sentinel_window(n: int) -> Dict[str, Any]:
        # Deterministic FAIL payload (schema-valid) for insufficient history:
        # - no partial windows in v1
        # - still emits required window objects
        return {
            "window_days": n,
            "window_start_day_utc": day,
            "window_end_day_utc": day,
            "max_drawdown_pct": "0.000000",
        }

    # windows in fixed order; ALWAYS emit 3 entries (schema requires exactly 3)
    for n in (30, 60, 90):
        if len(days) < n:
            reason_codes.append(f"INSUFFICIENT_HISTORY_WINDOW_{n}")
            windows_out.append(_sentinel_window(n))
            continue

        w = days[-n:]
        start_day = w[0].get("day_utc")
        end_day = w[-1].get("day_utc")
        if not isinstance(start_day, str) or not isinstance(end_day, str):
            raise SystemExit("FAIL: ledger day_utc missing/invalid")
        max_dd = _max_drawdown_pct(w)
        windows_out.append(
            {
                "window_days": n,
                "window_start_day_utc": start_day,
                "window_end_day_utc": end_day,
                "max_drawdown_pct": max_dd,
            }
        )

    status = "OK"
    exit_code = 0
    if reason_codes:
        status = "FAIL_INSUFFICIENT_HISTORY"
        exit_code = 2

    produced_utc = _now_utc_iso()
    git_sha = _git_sha_failclosed()

    input_manifest: List[Dict[str, str]] = [
        {"type": "nav_history_ledger", "path": str(ledger_path), "sha256": _sha256_file(ledger_path)},
        {"type": "drawdown_window_pack_contract", "path": str(contract_path), "sha256": _sha256_file(contract_path)},
        {"type": "output_schema", "path": str(schema_path), "sha256": _sha256_file(schema_path)},
    ]

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_DRAWDOWN_WINDOW_PACK_V1",
        "schema_version": 1,
        "day_utc": day,
        "status": status,
        "windows": windows_out,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "produced_utc": produced_utc,
        "producer": {"git_sha": git_sha, "module": "ops/tools/gen_drawdown_window_pack_v1.py", "repo": "constellation_2_runtime"},
        "canonical_json_hash": None,
    }

    out_obj["canonical_json_hash"] = canonical_hash_excluding_fields_v1(out_obj, fields=("canonical_json_hash",))
    _validate_schema_or_fail(out_obj, schema_path)

    out_path = (PACK_ROOT / day / PACK_NAME).resolve()
    wr = _write_immutable_canon(out_path, out_obj)

    print(f"DRAWDOWN_WINDOW_PACK_V1 day_utc={day} status={status} windows={len(windows_out)} path={wr.path} sha256={wr.sha256} action={wr.action}")
    if exit_code != 0:
        print(f"FAIL: {status} reason_codes={reason_codes}", file=sys.stderr)
    return exit_code

if __name__ == "__main__":
    raise SystemExit(main())
