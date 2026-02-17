#!/usr/bin/env python3
"""
gen_nav_snapshot_v1.py

Writes NAV Snapshot Truth v1 for a given day.

Deterministic / audit-grade / fail-closed:
- Canonical JSON via constellation_2.phaseD.lib.canon_json_v1
- No floats anywhere
- Immutable write (exists identical OK; exists different FAIL)
- Schema validated (governed)
- Full input_manifest with sha256
- Self-hash canonical_json_hash over canonical JSON with field set to null

Output:
  constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_snapshot/<DAY>/nav_snapshot.v1.json

Upstream required input:
  constellation_2/runtime/truth/accounting_v1/nav/<DAY>/nav.json
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
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Determinism
getcontext().prec = 50

DAY_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import (  # type: ignore
    canonical_hash_excluding_fields_v1,
    canonical_json_bytes_v1,
)

TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

# Bundle output roots
BUNDLE_ROOT = TRUTH_ROOT / "monitoring_v1" / "economic_nav_drawdown_v1"
OUT_DIR = BUNDLE_ROOT / "nav_snapshot"
OUT_NAME = "nav_snapshot.v1.json"

# Upstream input
ACCOUNTING_NAV_ROOT = TRUTH_ROOT / "accounting_v1" / "nav"

# Governed schema
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/nav_snapshot.v1.schema.json"

# Contract reference (for input_manifest)
CONTRACT_RELPATH = "governance/05_CONTRACTS/C2/nav_snapshot_truth_v1.contract.md"
DRAWDOWN_CONTRACT_RELPATH = "governance/05_CONTRACTS/C2/drawdown_convention_v1.contract.md"


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


def _quant6(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def _load_prior_peak_from_latest_pointer() -> Optional[Decimal]:
    """
    Reads the latest ledger pointer if present and returns the last day's peak_nav_to_date.
    Fail-closed on parse errors; returns None if pointer missing.
    """
    p_latest = (BUNDLE_ROOT / "nav_history_ledger" / "latest.json").resolve()
    if not p_latest.exists():
        return None

    try:
        latest = _read_json_obj(p_latest)
        pointers = latest.get("pointers") if isinstance(latest.get("pointers"), dict) else {}
        ledger_path = pointers.get("ledger_path")
        if not isinstance(ledger_path, str) or ledger_path.strip() == "":
            raise SystemExit(f"FAIL: latest pointer missing ledger_path: {str(p_latest)}")
        p_ledger = Path(ledger_path).resolve()
        if not p_ledger.exists():
            raise SystemExit(f"FAIL: latest pointer ledger missing: {ledger_path}")
        ledger = _read_json_obj(p_ledger)
        days = ledger.get("days")
        if not isinstance(days, list) or len(days) < 1:
            raise SystemExit(f"FAIL: ledger days missing/empty: {ledger_path}")
        last = days[-1]
        if not isinstance(last, dict):
            raise SystemExit(f"FAIL: ledger days[-1] not object: {ledger_path}")
        peak_s = last.get("peak_nav_to_date")
        if not isinstance(peak_s, str) or peak_s.strip() == "":
            raise SystemExit(f"FAIL: ledger peak_nav_to_date missing: {ledger_path}")
        return Decimal(peak_s)
    except SystemExit:
        raise
    except Exception as e:
        raise SystemExit(f"FAIL: cannot parse prior peak from latest pointer: {e!r}")


def _compute_drawdown_pct(end_nav: Decimal, peak_nav: Decimal) -> Decimal:
    if peak_nav <= Decimal("0"):
        raise SystemExit(f"FAIL: peak_nav_to_date <= 0 (fail-closed): peak={str(peak_nav)}")
    return _quant6((end_nav - peak_nav) / peak_nav)


def main() -> int:
    ap = argparse.ArgumentParser(prog="gen_nav_snapshot_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not DAY_RE.match(day):
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    # Required upstream input
    p_nav = (ACCOUNTING_NAV_ROOT / day / "nav.json").resolve()
    if not p_nav.exists():
        raise SystemExit(f"FAIL: missing required input: {str(p_nav)}")

    nav_obj = _read_json_obj(p_nav)
    nav_block = nav_obj.get("nav") if isinstance(nav_obj.get("nav"), dict) else {}
    nav_total = nav_block.get("nav_total")

    if not isinstance(nav_total, int):
        raise SystemExit(f"FAIL: required field nav.nav_total missing or not int: path={str(p_nav)} value={nav_total!r}")
    if nav_total < 0:
        raise SystemExit(f"FAIL: nav.nav_total < 0 prohibited: {nav_total}")

    end_nav_dec = Decimal(str(nav_total))

    prior_peak = _load_prior_peak_from_latest_pointer()
    if prior_peak is None:
        peak_dec = end_nav_dec
        dd_dec = Decimal("0.000000")
        genesis = True
    else:
        peak_dec = max(end_nav_dec, prior_peak)
        dd_dec = _compute_drawdown_pct(end_nav_dec, peak_dec)
        genesis = False

    # Construct output object (no floats; decimals as strings)
    produced_utc = _now_utc_iso()
    git_sha = _git_sha_failclosed()

    schema_path = (REPO_ROOT / SCHEMA_RELPATH).resolve()
    contract_path = (REPO_ROOT / CONTRACT_RELPATH).resolve()
    drawdown_contract_path = (REPO_ROOT / DRAWDOWN_CONTRACT_RELPATH).resolve()

    input_manifest: List[Dict[str, str]] = [
        {"type": "accounting_nav", "path": str(p_nav), "sha256": _sha256_file(p_nav)},
        {"type": "nav_snapshot_contract", "path": str(contract_path), "sha256": _sha256_file(contract_path)},
        {"type": "drawdown_contract", "path": str(drawdown_contract_path), "sha256": _sha256_file(drawdown_contract_path)},
        {"type": "output_schema", "path": str(schema_path), "sha256": _sha256_file(schema_path)},
    ]

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_NAV_SNAPSHOT_TRUTH_V1",
        "schema_version": 1,
        "day_utc": day,
        "end_nav": str(end_nav_dec),
        "peak_nav_to_date": str(peak_dec),
        "drawdown_pct": f"{dd_dec:.6f}",
        "input_manifest": input_manifest,
        "produced_utc": produced_utc,
        "producer": {"git_sha": git_sha, "module": "ops/tools/gen_nav_snapshot_v1.py", "repo": "constellation_2_runtime"},
        "canonical_json_hash": None,
    }

    out_obj["canonical_json_hash"] = canonical_hash_excluding_fields_v1(out_obj, fields=("canonical_json_hash",))

    _validate_schema_or_fail(out_obj, schema_path)

    out_path = (OUT_DIR / day / OUT_NAME).resolve()
    wr = _write_immutable_canon(out_path, out_obj)

    print(
        f"NAV_SNAPSHOT_V1 day_utc={day} end_nav={out_obj['end_nav']} peak_nav_to_date={out_obj['peak_nav_to_date']} "
        f"drawdown_pct={out_obj['drawdown_pct']} genesis={str(genesis).lower()} path={wr.path} sha256={wr.sha256} action={wr.action}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
