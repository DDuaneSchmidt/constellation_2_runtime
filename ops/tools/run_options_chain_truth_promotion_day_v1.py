#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root

REPO_ROOT = _REPO_ROOT_FROM_FILE
DEFAULT_TRUTH_ROOT = resolve_canonical_truth_root().resolve()


class PromotionError(Exception):
    pass


def _require_day(day_utc: str) -> str:
    s = str(day_utc or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise PromotionError(f"BAD_DAY_UTC: {s!r}")
    return s


def _require_eval(eval_time_utc: str) -> str:
    s = str(eval_time_utc or "").strip()
    if not s.endswith("Z") or "T" not in s:
        raise PromotionError(f"BAD_EVAL_TIME_UTC: {s!r}")
    return s


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise PromotionError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _same_day_or_raise(day_utc: str, ts: str, field_name: str) -> None:
    if not str(ts or "").startswith(day_utc):
        raise PromotionError(f"NOT_SAME_DAY: {field_name}={ts!r} expected_day={day_utc}")


def _find_latest_capture_dir(truth_root: Path, day_utc: str, symbol: str) -> Path:
    root = (truth_root / "options_chain_raw_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        raise PromotionError(f"RAW_ROOT_MISSING: {root}")

    matches = []
    for d in sorted(root.iterdir()):
        p = (d / "raw_chain.json").resolve()
        if not p.exists() or not p.is_file():
            continue
        try:
            obj = _read_json(p)
        except Exception:
            continue
        sym = str(((obj.get("underlying") or {}) if isinstance(obj.get("underlying"), dict) else {}).get("symbol") or "").strip().upper()
        if sym == symbol.upper():
            matches.append(d.resolve())

    if not matches:
        raise PromotionError(f"RAW_CAPTURE_MISSING_FOR_SYMBOL: day={day_utc} symbol={symbol}")
    return matches[-1]


def _promote(*, day_utc: str, eval_time_utc: str, truth_root: Path, symbol: str, max_age_seconds: int, clock_skew_tolerance_seconds: int) -> Path:
    capture_dir = _find_latest_capture_dir(truth_root, day_utc, symbol)
    raw_input = (capture_dir / "raw_chain.json").resolve()
    if not raw_input.exists() or not raw_input.is_file():
        raise PromotionError(f"RAW_INPUT_MISSING: {raw_input}")

    capture_run_id = capture_dir.name
    out_dir = (truth_root / "options_chain_snapshot_v1" / day_utc / capture_run_id).resolve()
    out_dir.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        "constellation_2.phaseB.tools.c2_build_chain_truth_v1",
        "--raw_input",
        str(raw_input),
        "--out_dir",
        str(out_dir),
        "--max_age_seconds",
        str(max_age_seconds),
        "--clock_skew_tolerance_seconds",
        str(clock_skew_tolerance_seconds),
    ]
    res = subprocess.run(cmd, cwd=str(REPO_ROOT), text=True, capture_output=True, check=False)
    if res.returncode != 0:
        raise PromotionError(
            "PHASEB_PROMOTION_FAILED:"
            f" rc={res.returncode};"
            f" stdout={res.stdout.strip()!r};"
            f" stderr={res.stderr.strip()!r}"
        )

    snap = (out_dir / "options_chain_snapshot.v1.json").resolve()
    cert = (out_dir / "freshness_certificate.v1.json").resolve()
    if not snap.exists() or not snap.is_file():
        raise PromotionError(f"SNAPSHOT_MISSING: {snap}")
    if not cert.exists() or not cert.is_file():
        raise PromotionError(f"FRESHNESS_CERT_MISSING: {cert}")

    snap_obj = _read_json(snap)
    cert_obj = _read_json(cert)

    _same_day_or_raise(day_utc, str(snap_obj.get("as_of_utc") or ""), "snapshot.as_of_utc")
    _same_day_or_raise(day_utc, str(cert_obj.get("snapshot_as_of_utc") or ""), "freshness.snapshot_as_of_utc")
    _same_day_or_raise(day_utc, eval_time_utc, "eval_time_utc")

    return out_dir


def main() -> int:
    ap = argparse.ArgumentParser(description="Governed same-day Phase B truth promotion writer.")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--eval_time_utc", required=True)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    ap.add_argument("--max_age_seconds", type=int, default=300)
    ap.add_argument("--clock_skew_tolerance_seconds", type=int, default=5)
    args = ap.parse_args()

    try:
        day_utc = _require_day(args.day_utc)
        eval_time_utc = _require_eval(args.eval_time_utc)
        symbol = str(args.symbol or "").strip().upper()
        if not symbol:
            raise PromotionError("SYMBOL_REQUIRED")
        truth_root = Path(str(args.truth_root)).expanduser().resolve()
        if not truth_root.exists() or not truth_root.is_dir():
            raise PromotionError(f"TRUTH_ROOT_MISSING: {truth_root}")

        out_dir = _promote(
            day_utc=day_utc,
            eval_time_utc=eval_time_utc,
            truth_root=truth_root,
            symbol=symbol,
            max_age_seconds=int(args.max_age_seconds),
            clock_skew_tolerance_seconds=int(args.clock_skew_tolerance_seconds),
        )
        print(f"OK: wrote {out_dir}")
        return 0
    except PromotionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
