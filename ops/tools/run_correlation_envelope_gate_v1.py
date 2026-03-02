#!/usr/bin/env python3
"""
Correlation Envelope Gate (CEG) v1 — HARD pre-trade deterministic gate.

Outputs allocation-consumable caps as per-sleeve multipliers (basis points).
Allocation must apply multiplier caps as hard ceilings and embed binding hash.

Escalation additions (2026-03-02):
- Convex Shock Envelope (CSE) enforced deterministically:
  - Writes convex_risk_assessment.v1.json (artifact-backed)
  - Tightens CEG caps: final_caps = min(linear_caps, convex_caps)

Depth-aware liquidity stress upgrade (DALSM, 2026-03-02):
- Writes depth_liquidity_stress.v1.json (artifact-backed)
- Computes binding depth_scale_bp (basis points)
- Tightens CSE caps: cse_scale_bp_final = min(scale_after_liquidity, depth_scale_bp)
- Convex assessment hash-links depth artifact (path+sha)

Determinism + immutability:
- No wall clock dependency (produced_utc is day marker)
- Canonical JSON bytes
- Day-scoped inputs only
- Fail-closed if any required input missing/corrupt
- NO overwrite: if output exists, compare candidate bytes; PASS only if identical else FAIL-CLOSED.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from decimal import Decimal, InvalidOperation
from math import sqrt
from pathlib import Path
from typing import Any, Dict, List, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_CORRELATION_ENVELOPE_POLICY_V1.json").resolve()
CAPAUTH_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
CSE_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_CONVEX_SHOCK_ENVELOPE_POLICY_V1.json").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/correlation_envelope_gate.v1.schema.json"
CSE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/convex_risk_assessment.v1.schema.json"

IN_CORR = TRUTH / "monitoring_v1/engine_correlation_matrix"
IN_INTENTS = TRUTH / "intents_v1/snapshots"
OUT_ROOT = TRUTH / "reports/correlation_envelope_gate_v1"
CSE_OUT_ROOT = TRUTH / "reports/convex_risk_assessment_v1"

LIQ_DATASET_MANIFEST = (TRUTH / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()

DEPTH_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_DEPTH_LIQUIDITY_STRESS_POLICY_V1.json").resolve()
DEPTH_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/depth_liquidity_stress.v1.schema.json"
DEPTH_OUT_ROOT = TRUTH / "reports/depth_liquidity_stress_v1"
NAV_ROOT = (TRUTH / "accounting_compat_v1" / "nav").resolve()
DATASET_ROOT = (TRUTH / "market_data_snapshot_v1").resolve()


def _parse_day(d: str) -> str:
    s = str(d).strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {s!r}")
    return s


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    h.update(p.read_bytes())
    return h.hexdigest()


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json_obj(p: Path) -> Dict[str, Any]:
    if not p.exists() or not p.is_file():
        raise SystemExit(f"FAIL: missing_or_not_file: {str(p)}")
    try:
        o = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: json_parse_failed: {str(p)}: {e!r}") from e
    if not isinstance(o, dict):
        raise SystemExit(f"FAIL: top_level_not_object: {str(p)}")
    return o


def _dec(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        raise SystemExit(f"FAIL: invalid_decimal: {x!r}")


def _canonical_json_bytes_v1(obj: Any) -> bytes:
    try:
        from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # type: ignore

        return canonical_json_bytes_v1(obj)
    except Exception:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _validate_against_repo_schema_v1(repo_root: Path, schema_relpath: str, obj: Any) -> None:
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore

    validate_against_repo_schema_v1(obj, repo_root, schema_relpath)


def _write_immutable_or_compare(path: Path, candidate_bytes: bytes, mismatch_code: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cand = candidate_bytes if candidate_bytes.endswith(b"\n") else (candidate_bytes + b"\n")
    cand_sha = _sha256_bytes(cand)

    if not path.exists():
        path.write_bytes(cand)
        return

    existing = path.read_bytes()
    exist_sha = _sha256_bytes(existing)

    if exist_sha == cand_sha:
        return

    raise SystemExit(f"FAIL: {mismatch_code}: existing_sha={exist_sha} candidate_sha={cand_sha} path={str(path)}")


def _load_engine_corr(day: str) -> Tuple[List[str], List[List[Decimal]], Path]:
    p = (IN_CORR / day / "engine_correlation_matrix.v1.json").resolve()
    o = _read_json_obj(p)
    m = o.get("matrix")
    if not isinstance(m, dict):
        raise SystemExit("FAIL: ENGINE_CORR_MATRIX_MISSING_MATRIX")
    eids = m.get("engine_ids")
    corr = m.get("corr")
    if not isinstance(eids, list) or not isinstance(corr, list):
        raise SystemExit("FAIL: ENGINE_CORR_MATRIX_BAD_FORMAT")
    engine_ids = [str(x).strip() for x in eids]
    if any(not x for x in engine_ids):
        raise SystemExit("FAIL: ENGINE_CORR_MATRIX_EMPTY_ENGINE_ID")
    n = len(engine_ids)
    if n == 0:
        raise SystemExit("FAIL: ENGINE_CORR_MATRIX_EMPTY")
    if len(corr) != n:
        raise SystemExit("FAIL: ENGINE_CORR_MATRIX_DIM_MISMATCH")
    out: List[List[Decimal]] = []
    for r in corr:
        if not isinstance(r, list) or len(r) != n:
            raise SystemExit("FAIL: ENGINE_CORR_MATRIX_ROW_DIM_MISMATCH")
        out.append([_dec(v) for v in r])
    return (engine_ids, out, p)


def _load_capauth_engine_to_sleeve() -> Dict[str, str]:
    pol = _read_json_obj(CAPAUTH_POLICY_PATH)
    sleeves = pol.get("sleeves")
    if not isinstance(sleeves, list) or not sleeves:
        raise SystemExit("FAIL: CAPAUTH_POLICY_SLEEVES_INVALID_OR_EMPTY")
    m: Dict[str, str] = {}
    for s in sleeves:
        if not isinstance(s, dict):
            continue
        sleeve_id = str(s.get("sleeve_id") or "").strip()
        eids = s.get("engine_ids")
        if not sleeve_id or not isinstance(eids, list):
            continue
        for eid in eids:
            k = str(eid).strip()
            if k:
                m[k] = sleeve_id
    if not m:
        raise SystemExit("FAIL: CAPAUTH_POLICY_ENGINE_TO_SLEEVE_EMPTY")
    return m


def _scan_intents(day: str) -> Tuple[List[str], Dict[str, int]]:
    d = (IN_INTENTS / day).resolve()
    if not d.exists() or not d.is_dir():
        raise SystemExit(f"FAIL: INTENTS_DIR_MISSING: {str(d)}")

    engine_ids: List[str] = []
    sym_engine_ids: Dict[str, set] = {}

    files = sorted([p for p in d.iterdir() if p.is_file() and p.name.endswith(".json")], key=lambda p: p.name)
    for p in files:
        o = _read_json_obj(p)
        eng = o.get("engine") if isinstance(o.get("engine"), dict) else {}
        engine_id = str(eng.get("engine_id") or "").strip()
        if engine_id:
            engine_ids.append(engine_id)

        underlying = o.get("underlying") if isinstance(o.get("underlying"), dict) else {}
        sym = str(underlying.get("symbol") or "").strip().upper()

        if sym and engine_id:
            sym_engine_ids.setdefault(sym, set()).add(engine_id)

    engine_ids = sorted(set([e for e in engine_ids if e]))
    sym_counts = {sym: len(eids) for sym, eids in sym_engine_ids.items()}
    return (engine_ids, sym_counts)


def _load_nav_snapshot(day: str, max_staleness_days: int) -> Tuple[Decimal, Path]:
    # Prefer same-day nav snapshot. If missing, deterministically fall back to latest prior day within max_staleness_days.
    def _nav_path_for(d: str) -> Path:
        return (NAV_ROOT / d / "nav_snapshot.v1.json").resolve()

    p = _nav_path_for(day)
    chosen_day = day

    if not (p.exists() and p.is_file()):
        # deterministic scan of available days
        if not NAV_ROOT.exists() or not NAV_ROOT.is_dir():
            raise SystemExit(f"FAIL: DEPTH_NAV_SNAPSHOT_MISSING: {str(p)}")

        candidates = sorted([x.name for x in NAV_ROOT.iterdir() if x.is_dir()])
        # keep only <= day (lexical safe for YYYY-MM-DD)
        candidates = [d for d in candidates if d <= day]

        # walk backwards to find a candidate within staleness window
        found = None
        for d in reversed(candidates):
            # staleness in days using simple YYYY-MM-DD arithmetic via python stdlib
            try:
                from datetime import date
                y1, m1, d1 = [int(x) for x in day.split("-")]
                y2, m2, d2 = [int(x) for x in d.split("-")]
                delta = (date(y1, m1, d1) - date(y2, m2, d2)).days
            except Exception:
                continue

            if delta < 0:
                continue
            if delta > int(max_staleness_days):
                break

            pp = _nav_path_for(d)
            if pp.exists() and pp.is_file():
                found = (d, pp)
                break

        if not found:
            raise SystemExit(f"FAIL: DEPTH_NAV_SNAPSHOT_TOO_STALE day={day} max_staleness_days={max_staleness_days}")

        chosen_day, p = found

    o = _read_json_obj(p)
    nav = o.get("nav") if isinstance(o.get("nav"), dict) else {}
    total = nav.get("nav_total", None)
    if total is None:
        raise SystemExit("FAIL: DEPTH_NAV_TOTAL_MISSING")
    nav_total = _dec(total)
    if nav_total <= Decimal("0"):
        raise SystemExit(f"FAIL: DEPTH_NAV_TOTAL_NONPOSITIVE: {str(nav_total)}")

    # NOTE: caller may record p path+sha; chosen_day is implicit in path
    return (nav_total, p)

def _spread_proxy_bps_from_adv_dollar(policy: Dict[str, Any], adv_dollar: Decimal) -> Decimal:
    sp = policy.get("spread_proxy")
    if not isinstance(sp, dict):
        raise SystemExit("FAIL: DEPTH_POLICY_BAD_SPREAD_PROXY")
    method = str(sp.get("method") or "").strip()
    if method != "ADV_DOLLAR_BUCKET_TABLE_V1":
        raise SystemExit("FAIL: DEPTH_POLICY_UNSUPPORTED_SPREAD_PROXY_METHOD")

    buckets = sp.get("buckets")
    if not isinstance(buckets, list) or not buckets:
        raise SystemExit("FAIL: DEPTH_POLICY_SPREAD_BUCKETS_EMPTY")

    for b in buckets:
        if not isinstance(b, dict):
            continue
        lo = _dec(b.get("min_adv_dollar"))
        hi = _dec(b.get("max_adv_dollar"))
        sbps = _dec(b.get("spread_bps"))
        if adv_dollar >= lo and adv_dollar < hi:
            return sbps

    raise SystemExit("FAIL: DEPTH_POLICY_SPREAD_BUCKET_NO_MATCH")


def _load_bars_for_symbol_year(symbol: str, year: int) -> List[Dict[str, Any]]:
    p = (DATASET_ROOT / symbol.upper() / f"{int(year)}.jsonl").resolve()
    if not p.exists():
        return []
    out: List[Dict[str, Any]] = []
    try:
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            o = json.loads(line)
            if isinstance(o, dict):
                out.append(o)
    except Exception as e:
        raise SystemExit(f"FAIL: DEPTH_MARKET_DATA_PARSE_ERROR symbol={symbol} year={year} path={p} err={e!r}")
    return out


def _bars_upto_day(symbol: str, day: str) -> List[Tuple[str, Decimal, int]]:
    year = int(day[:4])
    raw = _load_bars_for_symbol_year(symbol, year)
    cutoff = f"{day}T23:59:59Z"
    rows: List[Tuple[str, Decimal, int]] = []
    for o in raw:
        ts = str(o.get("timestamp_utc") or "").strip()
        if not ts or ts > cutoff:
            continue
        close = _dec(o.get("close"))
        vol = int(o.get("volume") or 0)
        rows.append((ts, close, vol))
    rows.sort(key=lambda r: r[0])
    return rows


def _latest_close_and_adv(symbol: str, day: str, lookback_days: int) -> Tuple[Decimal, Decimal, Decimal]:
    bars = _bars_upto_day(symbol, day)
    if not bars:
        raise SystemExit(f"FAIL: DEPTH_MISSING_SYMBOL_BARS symbol={symbol} day={day}")
    last_close = bars[-1][1]

    window = bars[-lookback_days:] if len(bars) >= lookback_days else bars[:]
    n = len(window)
    if n <= 0:
        raise SystemExit(f"FAIL: DEPTH_ADV_WINDOW_EMPTY symbol={symbol} day={day}")

    sum_vol = sum(int(v) for _, _, v in window)
    sum_dol = sum((c * Decimal(int(v))) for _, c, v in window)

    adv_shares = (Decimal(sum_vol) / Decimal(n)) if n > 0 else Decimal("0")
    adv_dollar = (sum_dol / Decimal(n)) if n > 0 else Decimal("0")
    return (last_close, adv_shares, adv_dollar)


def _scan_intents_notional_by_symbol(day: str, nav_total: Decimal) -> Dict[str, Decimal]:
    d = (IN_INTENTS / day).resolve()
    if not d.exists() or not d.is_dir():
        raise SystemExit(f"FAIL: DEPTH_INTENTS_ROOT_MISSING: {str(d)}")

    out: Dict[str, Decimal] = {}
    files = sorted([p for p in d.iterdir() if p.is_file() and p.name.endswith(".json")], key=lambda p: p.name)
    for p in files:
        o = _read_json_obj(p)

        underlying = o.get("underlying")
        sym = ""
        if isinstance(underlying, dict):
            sym = str(underlying.get("symbol") or "").strip().upper()
        elif isinstance(underlying, str):
            sym = str(underlying).strip().upper()
        if not sym:
            raise SystemExit("FAIL: DEPTH_INTENT_MISSING_SYMBOL")

        if "target_notional_pct" not in o:
            raise SystemExit("FAIL: DEPTH_INTENT_MISSING_TARGET_NOTIONAL_PCT")
        pct = _dec(o.get("target_notional_pct"))
        if pct < Decimal("0"):
            raise SystemExit("FAIL: DEPTH_INTENT_NEGATIVE_TARGET_PCT")

        notional = nav_total * pct
        out[sym] = out.get(sym, Decimal("0")) + notional

    return out


def _max_offdiag_subset(all_eids: List[str], corr: List[List[Decimal]], subset: List[str]) -> Decimal:
    idx = {eid: i for i, eid in enumerate(all_eids)}
    inds = [idx[e] for e in subset if e in idx]
    if len(inds) <= 1:
        return Decimal("0")
    m = Decimal("-999")
    for i in inds:
        for j in inds:
            if i == j:
                continue
            v = corr[i][j]
            if v > m:
                m = v
    if m == Decimal("-999"):
        return Decimal("0")
    return m


def _linear_scale_bp(max_corr: Decimal, start: Decimal, fail: Decimal, min_bp: int) -> int:
    if max_corr <= start:
        return 10000
    if max_corr >= fail:
        return 0
    num = (fail - max_corr)
    den = (fail - start)
    if den <= Decimal("0"):
        return 0
    frac = num / den
    bp = int((frac * Decimal("10000")).to_integral_value(rounding="ROUND_FLOOR"))
    if bp < min_bp:
        return min_bp
    if bp > 10000:
        return 10000
    return bp


def _clamp(x: Decimal, lo: Decimal, hi: Decimal) -> Decimal:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _portfolio_risk_from_corr(active_ids: List[str], all_ids: List[str], corr: List[List[Decimal]], corr_mult: Decimal, vol_mult: Decimal) -> Decimal:
    idx = {eid: i for i, eid in enumerate(all_ids)}
    inds = [idx[e] for e in active_ids if e in idx]
    if not inds:
        return Decimal("0")
    n = len(inds)

    total = Decimal("0")
    for a in range(n):
        for b in range(n):
            i = inds[a]
            j = inds[b]
            r = corr[i][j]
            if i != j:
                r = _clamp(r * corr_mult, Decimal("-1.0"), Decimal("0.999"))
            else:
                r = Decimal("1.0")
            total += r

    var = (vol_mult * vol_mult) * total
    if var <= Decimal("0"):
        return Decimal("0")
    return Decimal(str(sqrt(float(var))))


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_correlation_envelope_gate_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"

    policy = _read_json_obj(POLICY_PATH)
    policy_sha = _sha256_file(POLICY_PATH)
    engine_to_sleeve = _load_capauth_engine_to_sleeve()

    cse_policy = _read_json_obj(CSE_POLICY_PATH)
    cse_policy_sha = _sha256_file(CSE_POLICY_PATH)

    violations: List[Dict[str, str]] = []
    status = "PASS"
    fail_closed = False

    multiplier_by_sleeve: Dict[str, int] = {}
    blocked_sleeves: List[str] = []

    corr_path: Path = (IN_CORR / day / "engine_correlation_matrix.v1.json").resolve()
    intents_root: Path = (IN_INTENTS / day).resolve()

    try:
        if not corr_path.exists():
            raise RuntimeError("MISSING_ENGINE_CORRELATION_MATRIX")
        if not intents_root.exists():
            raise RuntimeError("MISSING_INTENTS_DIR")
        if not LIQ_DATASET_MANIFEST.exists():
            raise RuntimeError("MISSING_LIQUIDITY_DATASET_MANIFEST")
        if not DEPTH_POLICY_PATH.exists():
            raise RuntimeError("MISSING_DEPTH_POLICY")

        all_eids, corr, _ = _load_engine_corr(day)
        active_eids, sym_counts = _scan_intents(day)

        sleeves = sorted(set(engine_to_sleeve.values()))
        for s in sleeves:
            multiplier_by_sleeve[s] = 10000

        # -------- linear CEG --------
        hard_n = int(policy["thresholds"]["hard_block_same_symbol_engine_count"])
        for sym, cnt in sorted(sym_counts.items(), key=lambda kv: kv[0]):
            if cnt >= hard_n:
                status = "BLOCK_ALL"
                fail_closed = True
                violations.append({"code": "HARD_BLOCK_SAME_SYMBOL_4PLUS", "detail": f"symbol={sym} engine_count={cnt}"})

        if status != "BLOCK_ALL":
            start = _dec(policy["thresholds"]["engine_pairwise_corr_scale_start"])
            fail = _dec(policy["thresholds"]["engine_pairwise_corr_max_fail"])
            min_bp = int(policy["scaling"]["min_scale_bp"])

            max_corr = _max_offdiag_subset(all_eids, corr, active_eids)
            scale_bp = _linear_scale_bp(max_corr, start, fail, min_bp)

            if scale_bp == 0:
                status = "BLOCK_ALL"
                fail_closed = True
                violations.append({"code": "ENGINE_CORRELATION_FAIL", "detail": f"max_pairwise={str(max_corr)}"})
            elif scale_bp < 10000:
                status = "SCALE"
                violations.append({"code": "ENGINE_CORRELATION_SCALE", "detail": f"max_pairwise={str(max_corr)} scale_bp={scale_bp}"})

            for s in sleeves:
                multiplier_by_sleeve[s] = min(multiplier_by_sleeve[s], int(scale_bp))

            sym_scale_start = int(policy["thresholds"]["same_symbol_engine_count_scale_start"])
            same_symbol_mult = int(policy["caps"]["max_same_symbol_capital_at_risk_multiplier_bp"])
            for sym, cnt in sorted(sym_counts.items(), key=lambda kv: kv[0]):
                if cnt >= sym_scale_start:
                    for s in sleeves:
                        multiplier_by_sleeve[s] = min(multiplier_by_sleeve[s], same_symbol_mult)
                    violations.append({"code": "SAME_SYMBOL_STACKING_DETECTED", "detail": f"symbol={sym} engine_count={cnt}"})
                    if status == "PASS":
                        status = "SCALE"

        if status == "BLOCK_ALL":
            for s in sleeves:
                multiplier_by_sleeve[s] = 0
            blocked_sleeves = sleeves[:]

        # -------- convex (CSE) --------
        corr_mult = _dec(cse_policy["shock"]["correlation_spike_multiplier"])
        vol_mult = _dec(cse_policy["shock"]["volatility_expansion_multiplier"])
        liq_mult = _dec(cse_policy["shock"]["liquidity_contraction_multiplier"])
        max_risk = _dec(cse_policy["thresholds"]["max_shocked_portfolio_risk"])
        cse_min_bp = int(cse_policy["thresholds"]["min_scale_bp"])

        baseline_risk = _portfolio_risk_from_corr(active_eids, all_eids, corr, Decimal("1.0"), Decimal("1.0"))
        shocked_risk = _portfolio_risk_from_corr(active_eids, all_eids, corr, corr_mult, vol_mult)

        # -------- depth-aware liquidity stress (DALSM) --------
        depth_policy = _read_json_obj(DEPTH_POLICY_PATH)
        depth_policy_sha = _sha256_file(DEPTH_POLICY_PATH)

        max_stale = int(depth_policy.get("nav_fallback", {}).get("max_staleness_days", 0))
        nav_total, nav_path = _load_nav_snapshot(day, max_stale)
        notional_by_symbol = _scan_intents_notional_by_symbol(day, nav_total)

        rs = depth_policy.get("regime_selection")
        if not isinstance(rs, dict):
            raise RuntimeError("DEPTH_POLICY_MISSING_REGIME_SELECTION")
        rsm = str(rs.get("method") or "").strip()
        if rsm != "FIXED_REGIME":
            raise RuntimeError("DEPTH_POLICY_UNSUPPORTED_REGIME_SELECTION")
        regime_used = str(rs.get("fixed_regime") or "").strip()
        if regime_used not in ("NORMAL", "VOL_EXPANSION", "LIQ_CONTRACTION"):
            raise RuntimeError("DEPTH_POLICY_BAD_FIXED_REGIME")

        regime = depth_policy.get("regimes", {}).get(regime_used)
        if not isinstance(regime, dict):
            raise RuntimeError("DEPTH_POLICY_MISSING_REGIME_PARAMS")

        lookback = int(depth_policy.get("lookbacks", {}).get("adv_lookback_days", 20))

        depth_fraction_of_adv = _dec(regime.get("depth_fraction_of_adv"))
        depth_removal_pct = _dec(regime.get("depth_removal_pct"))
        spread_widen_mult = _dec(regime.get("spread_widen_multiplier"))
        impact_k_bps = _dec(regime.get("impact_k_bps"))
        impact_alpha = int(str(regime.get("impact_alpha")))

        if depth_removal_pct < Decimal("0") or depth_removal_pct >= Decimal("1"):
            raise RuntimeError("DEPTH_POLICY_BAD_DEPTH_REMOVAL_PCT")
        if depth_fraction_of_adv <= Decimal("0"):
            raise RuntimeError("DEPTH_POLICY_BAD_DEPTH_FRACTION")
        if spread_widen_mult <= Decimal("0"):
            raise RuntimeError("DEPTH_POLICY_BAD_SPREAD_WIDEN_MULT")
        if impact_k_bps < Decimal("0") or impact_alpha < 1:
            raise RuntimeError("DEPTH_POLICY_BAD_IMPACT_PARAMS")

        by_symbol: Dict[str, Any] = {}
        total_notional = Decimal("0")
        total_cost_dol = Decimal("0")
        max_sym = ""
        max_sym_cost_bps = Decimal("0")

        for sym in sorted(notional_by_symbol.keys()):
            intent_notional = notional_by_symbol[sym]
            total_notional += intent_notional

            price_close, adv_shares, adv_dollar = _latest_close_and_adv(sym, day, lookback)
            spread_bps_norm = _spread_proxy_bps_from_adv_dollar(depth_policy, adv_dollar)

            depth_dol_norm = adv_dollar * depth_fraction_of_adv
            depth_dol_stressed = depth_dol_norm * (Decimal("1") - depth_removal_pct)
            if depth_dol_stressed <= Decimal("0"):
                raise RuntimeError("DEPTH_NONPOSITIVE_STRESSED_DEPTH")

            spread_bps_stressed = spread_bps_norm * spread_widen_mult

            if intent_notional <= Decimal("0"):
                impact_bps = Decimal("0")
            else:
                q = intent_notional / depth_dol_stressed
                impact_bps = impact_k_bps * (q ** impact_alpha)

            total_cost_bps = spread_bps_stressed + impact_bps
            cost_dol = (intent_notional * total_cost_bps) / Decimal("10000")
            total_cost_dol += cost_dol

            if total_cost_bps > max_sym_cost_bps:
                max_sym_cost_bps = total_cost_bps
                max_sym = sym

            by_symbol[sym] = {
                "intent_notional_dollar": str(intent_notional),
                "price_close": str(price_close),
                "adv_shares": str(adv_shares),
                "adv_dollar": str(adv_dollar),
                "depth_dollar_normal": str(depth_dol_norm),
                "depth_dollar_stressed": str(depth_dol_stressed),
                "spread_bps_stressed": str(spread_bps_stressed),
                "impact_bps": str(impact_bps),
                "total_cost_bps": str(total_cost_bps),
                "total_cost_dollar": str(cost_dol),
            }

        if total_notional <= Decimal("0"):
            portfolio_cost_bps = Decimal("0")
        else:
            portfolio_cost_bps = (total_cost_dol / total_notional) * Decimal("10000")

        thr = depth_policy.get("thresholds")
        if not isinstance(thr, dict):
            raise RuntimeError("DEPTH_POLICY_MISSING_THRESHOLDS")
        max_port_bps = _dec(thr.get("max_depth_portfolio_cost_bps"))
        max_sym_bps = _dec(thr.get("max_depth_symbol_cost_bps"))
        min_scale_bp = int(thr.get("min_scale_bp"))

        scale_port = Decimal("10000")
        if portfolio_cost_bps > max_port_bps and portfolio_cost_bps > Decimal("0"):
            scale_port = (max_port_bps / portfolio_cost_bps) * Decimal("10000")

        scale_sym = Decimal("10000")
        if max_sym_cost_bps > max_sym_bps and max_sym_cost_bps > Decimal("0"):
            scale_sym = (max_sym_bps / max_sym_cost_bps) * Decimal("10000")

        depth_scale_bp = int(min(scale_port, scale_sym).to_integral_value(rounding="ROUND_FLOOR"))
        if depth_scale_bp > 10000:
            depth_scale_bp = 10000
        if depth_scale_bp < 0:
            depth_scale_bp = 0

        depth_reason_codes: List[str] = []
        depth_violations: List[str] = []
        if (portfolio_cost_bps > max_port_bps) or (max_sym_cost_bps > max_sym_bps):
            depth_reason_codes.append("DEPTH_COST_EXCEEDS_THRESHOLD")
        if depth_scale_bp < min_scale_bp:
            depth_scale_bp = 0
            depth_reason_codes.append("DEPTH_SCALE_BELOW_MIN_BLOCK")
            depth_violations.append("DEPTH_SCALE_BELOW_MIN_BLOCK")

        depth_out: Dict[str, Any] = {
            "schema_id": "C2_DEPTH_LIQUIDITY_STRESS_V1",
            "schema_version": 1,
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_correlation_envelope_gate_v1.py"},
            "status": "BLOCK_ALL" if depth_scale_bp == 0 else ("SCALE" if depth_scale_bp < 10000 else "PASS"),
            "fail_closed": False,
            "policy": {"path": str(DEPTH_POLICY_PATH.relative_to(REPO_ROOT)), "sha256": depth_policy_sha, "policy_id": "C2_DEPTH_LIQUIDITY_STRESS_POLICY_V1"},
            "inputs": {
                "intents_root": str(intents_root.relative_to(REPO_ROOT)),
                "liquidity_dataset_manifest_path": str(LIQ_DATASET_MANIFEST.relative_to(REPO_ROOT)),
                "liquidity_dataset_manifest_sha256": _sha256_file(LIQ_DATASET_MANIFEST),
                "nav_snapshot_path": str(nav_path.relative_to(REPO_ROOT)),
                "nav_snapshot_sha256": _sha256_file(nav_path),
            },
            "regime_used": regime_used,
            "aggregation": {
                "by_symbol": by_symbol,
                "portfolio": {
                    "total_intent_notional_dollar": str(total_notional),
                    "total_cost_dollar": str(total_cost_dol),
                    "portfolio_cost_bps": str(portfolio_cost_bps),
                    "max_symbol": max_sym if max_sym else ("NONE"),
                    "max_symbol_cost_bps": str(max_sym_cost_bps),
                },
            },
            "enforcement": {
                "depth_scale_bp": int(depth_scale_bp),
                "max_depth_portfolio_cost_bps": str(max_port_bps),
                "max_depth_symbol_cost_bps": str(max_sym_bps),
                "portfolio_cost_bps_used": str(portfolio_cost_bps),
                "max_symbol_cost_bps_used": str(max_sym_cost_bps),
                "reason_codes": depth_reason_codes,
            },
            "violations": depth_violations,
        }

        _validate_against_repo_schema_v1(REPO_ROOT, DEPTH_SCHEMA_RELPATH, depth_out)

        depth_out_path = (DEPTH_OUT_ROOT / day / "depth_liquidity_stress.v1.json").resolve()
        _write_immutable_or_compare(depth_out_path, _canonical_json_bytes_v1(depth_out), "DEPTH_OUTPUT_MISMATCH")

        depth_out_rel = str(depth_out_path.relative_to(REPO_ROOT))
        depth_out_sha = _sha256_file(depth_out_path)

        if shocked_risk <= max_risk or shocked_risk == Decimal("0"):
            cse_scale_bp_before = 10000
            cse_status = "PASS"
        else:
            ratio = (max_risk / shocked_risk)
            scale = ratio * ratio
            bp = int((scale * Decimal("10000")).to_integral_value(rounding="ROUND_FLOOR"))
            if bp < cse_min_bp:
                bp = cse_min_bp
            if bp <= 0:
                bp = 0
            cse_scale_bp_before = bp
            cse_status = "SCALE" if bp > 0 else "BLOCK_ALL"

        cse_scale_bp_after = int((Decimal(str(cse_scale_bp_before)) / liq_mult).to_integral_value(rounding="ROUND_FLOOR"))
        if cse_scale_bp_after < 0:
            cse_scale_bp_after = 0
        if cse_scale_bp_after > 10000:
            cse_scale_bp_after = 10000

        cse_scale_bp_final = min(int(cse_scale_bp_after), int(depth_scale_bp))

        cse_caps_by_sleeve: Dict[str, int] = {}
        cse_blocked: List[str] = []
        for s in sleeves:
            cse_caps_by_sleeve[s] = int(cse_scale_bp_final)
            if cse_caps_by_sleeve[s] == 0:
                cse_blocked.append(s)

        for s in sleeves:
            multiplier_by_sleeve[s] = min(multiplier_by_sleeve[s], cse_caps_by_sleeve[s])
            if multiplier_by_sleeve[s] == 0 and s not in blocked_sleeves:
                blocked_sleeves.append(s)

        cse_out: Dict[str, Any] = {
            "schema_id": "C2_CONVEX_RISK_ASSESSMENT_V1",
            "schema_version": 1,
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_correlation_envelope_gate_v1.py"},
            "status": cse_status,
            "fail_closed": False,
            "policy": {"path": str(CSE_POLICY_PATH.relative_to(REPO_ROOT)), "sha256": cse_policy_sha, "policy_id": "C2_CONVEX_SHOCK_ENVELOPE_POLICY_V1"},
            "inputs": {
                "engine_correlation_matrix_path": str(corr_path.relative_to(REPO_ROOT)),
                "engine_correlation_matrix_sha256": _sha256_file(corr_path),
                "intents_root": str(intents_root.relative_to(REPO_ROOT)),
                "liquidity_dataset_manifest_path": str(LIQ_DATASET_MANIFEST.relative_to(REPO_ROOT)),
                "liquidity_dataset_manifest_sha256": _sha256_file(LIQ_DATASET_MANIFEST),
                "depth_stress_path": depth_out_rel,
                "depth_stress_sha256": depth_out_sha,
            },
            "shock": {
                "correlation_spike_multiplier": str(cse_policy["shock"]["correlation_spike_multiplier"]),
                "volatility_expansion_multiplier": str(cse_policy["shock"]["volatility_expansion_multiplier"]),
                "liquidity_contraction_multiplier": str(cse_policy["shock"]["liquidity_contraction_multiplier"]),
            },
            "results": {
                "active_engine_ids": active_eids,
                "active_sleeve_ids": sorted(set([engine_to_sleeve[e] for e in active_eids if e in engine_to_sleeve])),
                "baseline_portfolio_risk": str(baseline_risk),
                "shocked_portfolio_risk": str(shocked_risk),
                "max_shocked_portfolio_risk": str(max_risk),
                "scale_bp_before_liquidity": int(cse_scale_bp_before),
                "scale_bp_after_liquidity": int(cse_scale_bp_after),
                "depth_regime_used": regime_used,
                "depth_scale_bp": int(depth_scale_bp),
                "scale_bp_final": int(cse_scale_bp_final),
            },
            "caps": {
                "multiplier_bp_by_sleeve": {k: int(cse_caps_by_sleeve[k]) for k in sorted(cse_caps_by_sleeve.keys())},
                "blocked_sleeves": [s for s in sorted(set(cse_blocked))],
            },
            "violations": [],
        }

        _validate_against_repo_schema_v1(REPO_ROOT, CSE_SCHEMA_RELPATH, cse_out)

        cse_out_path = (CSE_OUT_ROOT / day / "convex_risk_assessment.v1.json").resolve()
        _write_immutable_or_compare(cse_out_path, _canonical_json_bytes_v1(cse_out), "CSE_OUTPUT_MISMATCH")

    except Exception as e:
        status = "MISSING_INPUTS"
        fail_closed = True
        violations.append({"code": "FAIL_CLOSED_EXCEPTION", "detail": repr(e)})
        for s in sorted(set(engine_to_sleeve.values())):
            multiplier_by_sleeve[s] = 0
        blocked_sleeves = sorted(set(engine_to_sleeve.values()))

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_CORRELATION_ENVELOPE_GATE_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": str(REPO_ROOT), "git_sha": _git_sha(), "module": "ops/tools/run_correlation_envelope_gate_v1.py"},
        "status": status,
        "fail_closed": bool(fail_closed),
        "policy": {"path": str(POLICY_PATH.relative_to(REPO_ROOT)), "sha256": policy_sha, "policy_id": "C2_CORRELATION_ENVELOPE_POLICY_V1"},
        "inputs": {
            "engine_correlation_matrix_path": str(corr_path.relative_to(REPO_ROOT)),
            "engine_correlation_matrix_sha256": _sha256_file(corr_path) if corr_path.exists() else "0" * 64,
            "intents_root": str(intents_root.relative_to(REPO_ROOT)),
        },
        "caps": {
            "multiplier_bp_by_sleeve": {k: int(multiplier_by_sleeve[k]) for k in sorted(multiplier_by_sleeve.keys())},
            "blocked_sleeves": [s for s in sorted(set(blocked_sleeves))],
        },
        "violations": violations,
    }

    _validate_against_repo_schema_v1(REPO_ROOT, SCHEMA_RELPATH, out_obj)

    out_path = (OUT_ROOT / day / "correlation_envelope_gate.v1.json").resolve()
    _write_immutable_or_compare(out_path, _canonical_json_bytes_v1(out_obj), "CEG_OUTPUT_MISMATCH")

    print(_sha256_file(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
