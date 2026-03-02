#!/usr/bin/env python3
"""
Correlation Envelope Gate (CEG) v1 — HARD pre-trade deterministic gate.

Outputs allocation-consumable caps as per-sleeve multipliers (basis points).
Allocation must apply multiplier caps as hard ceilings and embed binding hash.

Determinism:
- No wall clock dependency (produced_utc is day marker)
- Canonical JSON bytes
- Day-scoped inputs only
- Fail-closed if any required input missing/corrupt
- Refuse overwrite of outputs
"""

from __future__ import annotations

import argparse
import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_CORRELATION_ENVELOPE_POLICY_V1.json").resolve()
CAPAUTH_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/correlation_envelope_gate.v1.schema.json"

IN_CORR = TRUTH / "monitoring_v1/engine_correlation_matrix"
IN_INTENTS = TRUTH / "intents_v1/snapshots"
OUT_ROOT = TRUTH / "reports/correlation_envelope_gate_v1"


def _parse_day(d: str) -> str:
    s = str(d).strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {s!r}")
    return s


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


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


def _atomic_write_refuse_overwrite(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise SystemExit(f"FAIL: REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    path.write_bytes(data)


def _canonical_json_bytes_v1(obj: Any) -> bytes:
    # Deterministic JSON encoding: sorted keys, no whitespace variance.
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _validate_against_repo_schema_v1(repo_root: Path, schema_relpath: str, obj: Any) -> None:
    # Repo-local schema validation: import your validator if present; otherwise fail closed.
    # We fail closed if validator module is missing, because institutional mode requires proofs.
    try:
        from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore
    except Exception as e:
        raise SystemExit(f"FAIL: missing_schema_validator: {e!r}") from e
    validate_against_repo_schema_v1(obj, repo_root, schema_relpath)


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

        # Common intent symbol location: underlying.symbol (uppercase)
        sym = ""
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


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_correlation_envelope_gate_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"

    policy = _read_json_obj(POLICY_PATH)
    policy_sha = _sha256_file(POLICY_PATH)
    engine_to_sleeve = _load_capauth_engine_to_sleeve()

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

        all_eids, corr, _ = _load_engine_corr(day)
        active_eids, sym_counts = _scan_intents(day)

        # Default sleeve multipliers = 1.0
        sleeves = sorted(set(engine_to_sleeve.values()))
        for s in sleeves:
            multiplier_by_sleeve[s] = 10000

        # HARD BLOCK: same symbol appears in >=4 engines
        hard_n = int(policy["thresholds"]["hard_block_same_symbol_engine_count"])
        for sym, cnt in sorted(sym_counts.items(), key=lambda kv: kv[0]):
            if cnt >= hard_n:
                status = "BLOCK_ALL"
                fail_closed = True
                violations.append({"code": "HARD_BLOCK_SAME_SYMBOL_4PLUS", "detail": f"symbol={sym} engine_count={cnt}"})

        if status != "BLOCK_ALL":
            # Engine pairwise correlation scaling
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

            # SAME-SYMBOL SOFT SCALE (v1 conservative global tightening once stacking detected)
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
            for s in sorted(set(engine_to_sleeve.values())):
                multiplier_by_sleeve[s] = 0
            blocked_sleeves = sorted(set(engine_to_sleeve.values()))

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
        "producer": {
            "repo": str(REPO_ROOT),
            "git_sha": (REPO_ROOT / ".git/HEAD").read_text(encoding="utf-8").strip() if (REPO_ROOT / ".git/HEAD").exists() else "UNKNOWN_GIT_HEAD",
            "module": "ops/tools/run_correlation_envelope_gate_v1.py"
        },
        "status": status,
        "fail_closed": bool(fail_closed),
        "policy": {
            "path": str(POLICY_PATH.relative_to(REPO_ROOT)),
            "sha256": policy_sha,
            "policy_id": "C2_CORRELATION_ENVELOPE_POLICY_V1"
        },
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

    # Validate output against repo schema (fail closed if validator missing)
    _validate_against_repo_schema_v1(REPO_ROOT, SCHEMA_RELPATH, out_obj)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "correlation_envelope_gate.v1.json").resolve()

    out_bytes = _canonical_json_bytes_v1(out_obj)
    _atomic_write_refuse_overwrite(out_path, out_bytes)

    print(_sha256_file(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
