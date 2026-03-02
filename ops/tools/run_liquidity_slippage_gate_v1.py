#!/usr/bin/env python3
"""
run_liquidity_slippage_gate_v1.py

Liquidity + Slippage Envelope Gate (v1).

Writes:
  constellation_2/runtime/truth/reports/liquidity_slippage_gate_v1/<DAY>/liquidity_slippage_gate.v1.json

Determinism:
- Uses only day-keyed truth inputs and governed policy.
- No wall-clock reads.
- Canonical JSON hashing.

Fail-closed:
- Missing policy, policy schema invalid, missing market data (unless allow-listed) => FAIL.

Scope:
- v1 evaluates exposure_intent snapshots (equity-style notionals) as a pre-trade capacity control.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_LIQUIDITY_SLIPPAGE_POLICY_V1.json").resolve()
POLICY_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/liquidity_slippage_policy.v1.schema.json"

OUT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/liquidity_slippage_gate.v1.schema.json"
OUT_ROOT = (TRUTH_ROOT / "reports" / "liquidity_slippage_gate_v1").resolve()

DATASET_MANIFEST = (TRUTH_ROOT / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()
DATASET_ROOT = (TRUTH_ROOT / "market_data_snapshot_v1").resolve()

INTENTS_DIR_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()

NAV_V2_ROOT = (TRUTH_ROOT / "accounting_v2" / "nav").resolve()
NAV_V1_ROOT = (TRUTH_ROOT / "accounting_v1" / "nav").resolve()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {p}")
    return o


def _parse_day(day_utc: str) -> str:
    d = str(day_utc).strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {d!r}")
    return d


def _decimal_str_6dp(x: Decimal) -> str:
    q = x.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
    s = format(q, "f")
    if "." not in s:
        s = s + ".000000"
    else:
        a, b = s.split(".", 1)
        s = a + "." + (b + "000000")[:6]
    return s


def _decimal_str_2dp(x: Decimal) -> str:
    q = x.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    s = format(q, "f")
    if "." not in s:
        s = s + ".00"
    else:
        a, b = s.split(".", 1)
        s = a + "." + (b + "00")[:2]
    return s


def _parse_decimal_strict(s: Any, field: str) -> Decimal:
    if s is None:
        raise ValueError(f"DECIMAL_MISSING:{field}")
    t = str(s).strip()
    if not t:
        raise ValueError(f"DECIMAL_EMPTY:{field}")
    return Decimal(t)


def _read_nav_total_cents(day: str) -> Tuple[int, Path, str]:
    p2 = (NAV_V2_ROOT / day / "nav.v2.json").resolve()
    if p2.exists():
        o = _read_json_obj(p2)
        nav_cents = int(((o.get("nav_total_cents") if "nav_total_cents" in o else None) or 0))
        return nav_cents, p2, _sha256_file(p2)

    p1 = (NAV_V1_ROOT / day / "nav.json").resolve()
    if p1.exists():
        o = _read_json_obj(p1)
        nav = int((o.get("nav_total") or 0))
        return nav * 100, p1, _sha256_file(p1)

    raise SystemExit(f"FAIL: LIQPOL_MISSING_NAV day={day}")


def _policy_effective(policy: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    eff = dict(policy.get("defaults") or {})
    for row in (policy.get("symbol_overrides") or []):
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").strip().upper()
        if sym != symbol:
            continue
        ov = row.get("overrides") or {}
        if isinstance(ov, dict):
            for k, v in ov.items():
                eff[k] = v
    return eff


@dataclass(frozen=True)
class Bar:
    ts: str
    close: Decimal
    volume: int


def _load_bars_for_symbol_year(symbol: str, year: int) -> Tuple[Path, str, List[Bar]]:
    p = (DATASET_ROOT / symbol.upper() / f"{int(year)}.jsonl").resolve()
    if not p.exists():
        return p, _sha256_bytes(b""), []
    sha = _sha256_file(p)
    bars: List[Bar] = []
    try:
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            o = json.loads(line)
            if not isinstance(o, dict):
                continue
            sym = str(o.get("symbol") or "").strip().upper()
            if sym != symbol.upper():
                continue
            ts = str(o.get("timestamp_utc") or "").strip()
            close = _parse_decimal_strict(o.get("close"), "close")
            vol = int(o.get("volume") or 0)
            bars.append(Bar(ts=ts, close=close, volume=vol))
    except Exception:
        raise SystemExit(f"FAIL: LIQPOL_MARKET_DATA_PARSE_ERROR symbol={symbol} year={year} path={p}")
    bars_sorted = sorted(bars, key=lambda b: b.ts)
    return p, sha, bars_sorted


def _bars_up_to_day(bars: List[Bar], day: str) -> List[Bar]:
    cutoff = f"{day}T23:59:59Z"
    return [b for b in bars if b.ts <= cutoff]


def _latest_close(bars_upto: List[Bar]) -> Optional[Decimal]:
    if not bars_upto:
        return None
    return bars_upto[-1].close


def _adv_shares_and_adv_dollar(bars_upto: List[Bar], lookback: int) -> Tuple[int, Decimal, int]:
    if not bars_upto:
        return (0, Decimal("0"), 0)
    window = bars_upto[-lookback:] if len(bars_upto) >= lookback else bars_upto[:]
    n = len(window)
    if n <= 0:
        return (0, Decimal("0"), 0)
    sum_vol = sum(int(b.volume) for b in window)
    sum_dol = sum((b.close * Decimal(int(b.volume))) for b in window)
    adv_sh = int(Decimal(sum_vol) / Decimal(n))
    adv_dol = (sum_dol / Decimal(n)) if n > 0 else Decimal("0")
    return adv_sh, adv_dol, n


def _read_intents_for_day(day: str) -> List[Path]:
    d = (INTENTS_DIR_ROOT / day).resolve()
    if not d.exists() or not d.is_dir():
        return []
    files = sorted([p for p in d.glob("*.json") if p.is_file()])
    return files


def _extract_intent_symbol_and_pct(intent_obj: Dict[str, Any]) -> Tuple[str, str]:
    # symbol: under "underlying" or "underlying.symbol" or "symbol"
    sym = ""
    if "underlying" in intent_obj:
        u = intent_obj.get("underlying")
        if isinstance(u, str):
            sym = u
        elif isinstance(u, dict):
            sym = str(u.get("symbol") or "")
    if not sym:
        sym = str(intent_obj.get("symbol") or "")
    sym = sym.strip().upper()

    tnp = str(intent_obj.get("target_notional_pct") or "").strip()
    if not tnp:
        # Some intents may use alternate naming; fail-closed in v1.
        raise ValueError("TARGET_NOTIONAL_PCT_MISSING")
    return sym, tnp


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_liquidity_slippage_gate_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day(str(args.day_utc))

    input_manifest: List[Dict[str, str]] = []

    if not POLICY_PATH.exists():
        raise SystemExit(f"FAIL: LIQPOL_POLICY_MISSING: {POLICY_PATH}")

    pol = _read_json_obj(POLICY_PATH)
    validate_against_repo_schema_v1(pol, REPO_ROOT, POLICY_SCHEMA_RELPATH)

    pol_sha = _sha256_file(POLICY_PATH)
    pol_schema_path = (REPO_ROOT / POLICY_SCHEMA_RELPATH).resolve()
    pol_schema_sha = _sha256_file(pol_schema_path)

    input_manifest.append({"type": "policy_manifest", "path": str(POLICY_PATH), "sha256": pol_sha})
    input_manifest.append({"type": "policy_schema", "path": str(pol_schema_path), "sha256": pol_schema_sha})

    if not DATASET_MANIFEST.exists():
        raise SystemExit(f"FAIL: LIQPOL_MARKET_DATA_FILE_MISSING: {DATASET_MANIFEST}")

    ds_manifest_sha = _sha256_file(DATASET_MANIFEST)
    input_manifest.append({"type": "market_data_dataset_manifest", "path": str(DATASET_MANIFEST), "sha256": ds_manifest_sha})

    nav_cents, nav_path, nav_sha = _read_nav_total_cents(day)
    input_manifest.append({"type": "accounting_nav", "path": str(nav_path), "sha256": nav_sha})

    intents = _read_intents_for_day(day)
    if not intents:
        allow = bool(((pol.get("defaults") or {}).get("allow_zero_intents_pass")) is True)
        status = "PASS" if allow else "FAIL"
        reason_codes = ["LIQPOL_MISSING_INTENTS_DIR"] if not allow else ["LIQPOL_PASS"]
        out_obj: Dict[str, Any] = {
            "schema_id": "liquidity_slippage_gate",
            "schema_version": "v1",
            "day_utc": day,
            "produced_utc": f"{day}T00:00:00Z",
            "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_liquidity_slippage_gate_v1.py", "git_sha": _git_sha()},
            "status": status,
            "reason_codes": reason_codes,
            "input_manifest": input_manifest,
            "policy": {
                "path": str(POLICY_PATH),
                "sha256": pol_sha,
                "schema_path": str(pol_schema_path),
                "schema_sha256": pol_schema_sha,
            },
            "results": {"per_intent": [], "totals": {"intents_total": 0, "intents_failed": 0, "intents_passed": 0, "intents_skipped": 0}},
            "gate_sha256": "0" * 64,
        }
        tmp = dict(out_obj)
        tmp["gate_sha256"] = None
        out_obj["gate_sha256"] = _sha256_bytes(canonical_json_bytes_v1(tmp))
        validate_against_repo_schema_v1(out_obj, REPO_ROOT, OUT_SCHEMA_RELPATH)

        out_dir = (OUT_ROOT / day).resolve()
        out_path = (out_dir / "liquidity_slippage_gate.v1.json").resolve()
        payload = canonical_json_bytes_v1(out_obj) + b"\n"
        out_dir.mkdir(parents=True, exist_ok=True)
        try:
            write_file_immutable_v1(path=out_path, data=payload, create_dirs=False)
        except ImmutableWriteError as e:
            raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e
        print(f"OK: liquidity_slippage_gate_v1 status={status} sha256={_sha256_file(out_path)} path={out_path}")
        return 0 if status in ("PASS", "OK") else 1

    per_intent: List[Dict[str, Any]] = []
    failed = 0
    passed = 0
    skipped = 0

    allow_missing = set([str(x).strip().upper() for x in ((pol.get("defaults") or {}).get("allow_missing_symbols") or [])])

    for ip in intents:
        sha = _sha256_file(ip)
        input_manifest.append({"type": "intent", "path": str(ip), "sha256": sha})

        # Preserve attribution across failures
        engine_id = "UNKNOWN"
        symbol = "UNKNOWN"
        tnp_str = ""

        try:
            intent_obj = _read_json_obj(ip)

            engine_id = ""
            if "engine" in intent_obj and isinstance(intent_obj.get("engine"), dict):
                engine_id = str((intent_obj["engine"].get("engine_id") or "")).strip()
            if not engine_id:
                engine_id = str(intent_obj.get("engine_id") or "").strip()
            if not engine_id:
                engine_id = "UNKNOWN"

            symbol, tnp_str = _extract_intent_symbol_and_pct(intent_obj)
            if not symbol:
                symbol = "UNKNOWN"
                raise ValueError("SYMBOL_MISSING")

            eff = _policy_effective(pol, symbol)

            lookback = int(eff.get("lookback_days") or 20)
            min_hist = int(eff.get("min_history_days") or 10)
            min_adv = int(eff.get("min_adv_shares") or 0)

            cap_part = _parse_decimal_strict(eff.get("max_participation_pct_adv"), "max_participation_pct_adv")
            cap_slip = _parse_decimal_strict(eff.get("max_est_slippage_bps"), "max_est_slippage_bps")
            base_bps = _parse_decimal_strict(eff.get("base_slippage_bps"), "base_slippage_bps")
            slope_bps = _parse_decimal_strict(eff.get("slippage_bps_per_1pct_adv"), "slippage_bps_per_1pct_adv")
            cap_notional = Decimal(str(eff.get("max_notional_per_symbol_usd") or "0"))
            cap_orders = int(eff.get("max_orders_per_symbol_per_day") or 1)

            tnp = _parse_decimal_strict(tnp_str, "target_notional_pct")

            est_notional = (Decimal(nav_cents) / Decimal(100)) * tnp
            est_notional_2dp = Decimal(_decimal_str_2dp(est_notional))

            year = int(day[0:4])
            data_path, data_sha, bars = _load_bars_for_symbol_year(symbol, year)
            if not bars:
                if symbol in allow_missing:
                    decision = "SKIP"
                    rc = ["LIQPOL_MARKET_DATA_FILE_MISSING"]
                    skipped += 1
                    per_intent.append(
                        {
                            "intent_hash": sha,
                            "engine_id": engine_id or "UNKNOWN",
                            "symbol": symbol,
                            "decision": decision,
                            "reason_codes": rc,
                            "metrics": {
                                "nav_total_cents": nav_cents,
                                "target_notional_pct": _decimal_str_6dp(tnp),
                                "est_notional_usd": _decimal_str_2dp(est_notional_2dp),
                                "close": "0.00",
                                "est_shares": 0,
                                "adv_shares": 0,
                                "adv_dollar": "0.00",
                                "participation_pct_adv": "0.000000",
                                "est_slippage_bps": "0.00",
                                "caps": {
                                    "max_participation_pct_adv": _decimal_str_6dp(cap_part),
                                    "max_est_slippage_bps": _decimal_str_2dp(cap_slip),
                                    "max_notional_per_symbol_usd": str(cap_notional.quantize(Decimal("1"), rounding=ROUND_DOWN)),
                                },
                            },
                        }
                    )
                    continue
                raise SystemExit(f"FAIL: LIQPOL_MARKET_DATA_FILE_MISSING symbol={symbol} path={data_path}")

            input_manifest.append({"type": f"market_data:{symbol}:{year}", "path": str(data_path), "sha256": data_sha})

            bars_upto = _bars_up_to_day(bars, day)
            close = _latest_close(bars_upto)
            if close is None:
                raise ValueError("CLOSE_MISSING")

            adv_sh, adv_dol, hist_days = _adv_shares_and_adv_dollar(bars_upto, lookback)
            if hist_days < min_hist:
                raise ValueError("INSUFFICIENT_HISTORY")
            if adv_sh < min_adv:
                raise ValueError("ADV_BELOW_MIN")

            if est_notional_2dp <= Decimal("0"):
                raise ValueError("NOTIONAL_ZERO")

            est_shares = int((est_notional_2dp / close).to_integral_value(rounding=ROUND_DOWN))
            part = (Decimal(est_shares) / Decimal(adv_sh)) if adv_sh > 0 else Decimal("0")
            part_6 = Decimal(_decimal_str_6dp(part))

            # Slippage model (deterministic):
            # est_slip_bps = base_bps + slope_bps_per_1pct_adv * (participation_pct_adv * 100)
            est_slip = base_bps + (slope_bps * (part * Decimal("100")))
            est_slip_2 = Decimal(_decimal_str_2dp(est_slip))

            rc: List[str] = []
            decision = "PASS"

            # Max orders per symbol per day (v1: count intents for symbol)
            sym_count = 0
            for pth in intents:
                try:
                    o2 = _read_json_obj(pth)
                    sym2, _ = _extract_intent_symbol_and_pct(o2)
                    if sym2.strip().upper() == symbol:
                        sym_count += 1
                except Exception:
                    continue
            if sym_count > cap_orders:
                decision = "FAIL"
                rc.append("LIQPOL_ORDERS_PER_SYMBOL_EXCEEDS_CAP")

            if est_notional_2dp > cap_notional:
                decision = "FAIL"
                rc.append("LIQPOL_NOTIONAL_EXCEEDS_CAP")

            if part_6 > cap_part:
                decision = "FAIL"
                rc.append("LIQPOL_PARTICIPATION_EXCEEDS_CAP")

            if est_slip_2 > cap_slip:
                decision = "FAIL"
                rc.append("LIQPOL_SLIPPAGE_EXCEEDS_CAP")

            if decision == "PASS":
                rc.append("LIQPOL_PASS")
                passed += 1
            else:
                failed += 1

            per_intent.append(
                {
                    "intent_hash": sha,
                    "engine_id": engine_id or "UNKNOWN",
                    "symbol": symbol,
                    "decision": decision,
                    "reason_codes": rc,
                    "metrics": {
                        "nav_total_cents": nav_cents,
                        "target_notional_pct": _decimal_str_6dp(tnp),
                        "est_notional_usd": _decimal_str_2dp(est_notional_2dp),
                        "close": _decimal_str_2dp(close),
                        "est_shares": est_shares,
                        "adv_shares": int(adv_sh),
                        "adv_dollar": _decimal_str_2dp(adv_dol),
                        "participation_pct_adv": _decimal_str_6dp(part_6),
                        "est_slippage_bps": _decimal_str_2dp(est_slip_2),
                        "caps": {
                            "max_participation_pct_adv": _decimal_str_6dp(cap_part),
                            "max_est_slippage_bps": _decimal_str_2dp(cap_slip),
                            "max_notional_per_symbol_usd": str(cap_notional.quantize(Decimal("1"), rounding=ROUND_DOWN)),
                        },
                    },
                }
            )
        except SystemExit:
            raise
        except Exception as e:
            failed += 1
            per_intent.append(
                {
                    "intent_hash": sha,
                    "engine_id": engine_id or "UNKNOWN",
                    "symbol": symbol or "UNKNOWN",
                    "decision": "FAIL",
                    "reason_codes": [
                        "LIQPOL_INTENT_PARSE_ERROR",
                        f"LIQPOL_EXC:{type(e).__name__}",
                        "LIQPOL_FAIL_CLOSED_REQUIRED",
                    ],
                    "metrics": {
                        "nav_total_cents": nav_cents,
                        "target_notional_pct": "0.000000",
                        "est_notional_usd": "0.00",
                        "close": "0.00",
                        "est_shares": 0,
                        "adv_shares": 0,
                        "adv_dollar": "0.00",
                        "participation_pct_adv": "0.000000",
                        "est_slippage_bps": "0.00",
                        "caps": {
                            "max_participation_pct_adv": "0.000000",
                            "max_est_slippage_bps": "0.00",
                            "max_notional_per_symbol_usd": "0",
                        },
                    },
                }
            )

    totals = {
        "intents_total": len(per_intent),
        "intents_failed": failed,
        "intents_passed": passed,
        "intents_skipped": skipped,
    }

    status = "PASS" if failed == 0 else "FAIL"
    reason_codes = ["LIQPOL_PASS"] if status == "PASS" else ["LIQPOL_FAIL_CLOSED_REQUIRED"]

    out_obj: Dict[str, Any] = {
        "schema_id": "liquidity_slippage_gate",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": f"{day}T00:00:00Z",
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_liquidity_slippage_gate_v1.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "policy": {
            "path": str(POLICY_PATH),
            "sha256": pol_sha,
            "schema_path": str(pol_schema_path),
            "schema_sha256": pol_schema_sha,
        },
        "results": {"per_intent": per_intent, "totals": totals},
        "gate_sha256": "0" * 64,
    }

    tmp = dict(out_obj)
    tmp["gate_sha256"] = None
    out_obj["gate_sha256"] = _sha256_bytes(canonical_json_bytes_v1(tmp))

    validate_against_repo_schema_v1(out_obj, REPO_ROOT, OUT_SCHEMA_RELPATH)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "liquidity_slippage_gate.v1.json").resolve()
    payload = canonical_json_bytes_v1(out_obj) + b"\n"

    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        write_file_immutable_v1(path=out_path, data=payload, create_dirs=False)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    print(f"OK: liquidity_slippage_gate_v1 status={status} sha256={_sha256_file(out_path)} path={out_path}")
    return 0 if status in ("PASS", "OK") else 1


if __name__ == "__main__":
    raise SystemExit(main())
