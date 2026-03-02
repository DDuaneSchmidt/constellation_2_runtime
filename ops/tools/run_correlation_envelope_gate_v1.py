#!/usr/bin/env python3
"""
Correlation Envelope Gate (CEG) v1 — HARD pre-trade deterministic gate.

Outputs allocation-consumable caps as per-sleeve multipliers (basis points).
Allocation must apply multiplier caps as hard ceilings and embed binding hash.

Escalation additions (2026-03-02):
- Convex Shock Envelope (CSE) enforced deterministically:
  - Writes convex_risk_assessment.v1.json (artifact-backed)
  - Tightens CEG caps: final_caps = min(linear_caps, convex_caps)

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
        # idempotent rerun: identical bytes
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

        cse_caps_by_sleeve: Dict[str, int] = {}
        cse_blocked: List[str] = []
        for s in sleeves:
            cse_caps_by_sleeve[s] = int(cse_scale_bp_after)
            if cse_caps_by_sleeve[s] == 0:
                cse_blocked.append(s)

        # tighten final caps
        for s in sleeves:
            multiplier_by_sleeve[s] = min(multiplier_by_sleeve[s], cse_caps_by_sleeve[s])
            if multiplier_by_sleeve[s] == 0 and s not in blocked_sleeves:
                blocked_sleeves.append(s)

        # write convex assessment (idempotent compare)
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
                "liquidity_dataset_manifest_sha256": _sha256_file(LIQ_DATASET_MANIFEST)
            },
            "shock": {
                "correlation_spike_multiplier": str(cse_policy["shock"]["correlation_spike_multiplier"]),
                "volatility_expansion_multiplier": str(cse_policy["shock"]["volatility_expansion_multiplier"]),
                "liquidity_contraction_multiplier": str(cse_policy["shock"]["liquidity_contraction_multiplier"])
            },
            "results": {
                "active_engine_ids": active_eids,
                "active_sleeve_ids": sorted(set([engine_to_sleeve[e] for e in active_eids if e in engine_to_sleeve])),
                "baseline_portfolio_risk": str(baseline_risk),
                "shocked_portfolio_risk": str(shocked_risk),
                "max_shocked_portfolio_risk": str(max_risk),
                "scale_bp_before_liquidity": int(cse_scale_bp_before),
                "scale_bp_after_liquidity": int(cse_scale_bp_after)
            },
            "caps": {
                "multiplier_bp_by_sleeve": {k: int(cse_caps_by_sleeve[k]) for k in sorted(cse_caps_by_sleeve.keys())},
                "blocked_sleeves": [s for s in sorted(set(cse_blocked))]
            },
            "violations": []
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
            "intents_root": str(intents_root.relative_to(REPO_ROOT))
        },
        "caps": {
            "multiplier_bp_by_sleeve": {k: int(multiplier_by_sleeve[k]) for k in sorted(multiplier_by_sleeve.keys())},
            "blocked_sleeves": [s for s in sorted(set(blocked_sleeves))]
        },
        "violations": violations
    }

    _validate_against_repo_schema_v1(REPO_ROOT, SCHEMA_RELPATH, out_obj)

    out_path = (OUT_ROOT / day / "correlation_envelope_gate.v1.json").resolve()
    _write_immutable_or_compare(out_path, _canonical_json_bytes_v1(out_obj), "CEG_OUTPUT_MISMATCH")

    print(_sha256_file(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
