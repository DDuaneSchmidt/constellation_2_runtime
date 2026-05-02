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
import os
import subprocess
import sys
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import resolve_authoritative_repo_root_v1
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance
from constellation_2.common.runtime_contract_v1 import require_truth_root_under_contract
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.common.runtime_authority_bridge_v1 import resolve_truth_root_bridge_v1

TRUTH_ROOT = resolve_truth_root_bridge_v1(
    repo_root=REPO_ROOT,
    caller="ops/tools/run_liquidity_slippage_gate_v1.py",
)

POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_LIQUIDITY_SLIPPAGE_POLICY_V1.json").resolve()
CAPAUTH_POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
POLICY_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/liquidity_slippage_policy.v1.schema.json"

OUT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/liquidity_slippage_gate.v1.schema.json"
OUT_ROOT = (TRUTH_ROOT / "reports" / "liquidity_slippage_gate_v1").resolve()

DATASET_MANIFEST = (TRUTH_ROOT / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()
DATASET_ROOT = (TRUTH_ROOT / "market_data_snapshot_v1").resolve()

INTENTS_DIR_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()

NAV_V2_ROOT = (TRUTH_ROOT / "accounting_v2" / "nav").resolve()
NAV_V1_ROOT = (TRUTH_ROOT / "accounting_v1" / "nav").resolve()


def _require_supported_truth_root(truth_root: Path) -> Path:
    resolved = Path(truth_root).expanduser().resolve()
    if not resolved.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {resolved}")
    if not resolved.exists() or not resolved.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not dir: {resolved}")
    try:
        return require_truth_root_under_contract(resolved)
    except BaseException:
        authoritative_repo_root = resolve_authoritative_repo_root_v1(REPO_ROOT)
        authoritative_runtime_root = (authoritative_repo_root / "constellation_2" / "runtime").resolve()
        try:
            resolved.relative_to(authoritative_runtime_root)
        except Exception as exc:
            raise SystemExit(
                "FAIL: truth_root must remain under active runtime contract or authoritative runtime root: "
                f"{resolved}"
            ) from exc
        return resolved


def _resolve_gate_truth_root(arg_truth_root: str) -> Path:
    raw = str(arg_truth_root or "").strip()
    if raw:
        return _require_supported_truth_root(Path(raw))
    env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env_root:
        return _require_supported_truth_root(Path(env_root))
    return _require_supported_truth_root(
        resolve_truth_root_bridge_v1(
            repo_root=REPO_ROOT,
            caller="ops/tools/run_liquidity_slippage_gate_v1.py",
        )
    )


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_gate_report(out_path: Path, out_obj: Dict[str, Any]) -> str:
    validate_against_repo_schema_v1(out_obj, REPO_ROOT, OUT_SCHEMA_RELPATH)
    payload = canonical_json_bytes_v1(out_obj) + b"\n"
    candidate_sha = _sha256_bytes(payload)
    candidate_manifest_hash = _sha256_bytes(
        canonical_json_bytes_v1(out_obj.get("input_manifest") or [])
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        existing = _read_json_obj(out_path)
        validate_against_repo_schema_v1(existing, REPO_ROOT, OUT_SCHEMA_RELPATH)

        existing_schema_id = str(existing.get("schema_id") or "").strip()
        existing_day = str(existing.get("day_utc") or "").strip()
        if existing_schema_id != "liquidity_slippage_gate":
            raise SystemExit(
                f"FAIL: EXISTING_SCHEMA_ID_MISMATCH path={out_path} schema_id={existing_schema_id!r}"
            )
        if existing_day != str(out_obj.get("day_utc") or "").strip():
            raise SystemExit(
                f"FAIL: EXISTING_DAY_MISMATCH path={out_path} existing_day={existing_day!r} "
                f"candidate_day={str(out_obj.get('day_utc') or '').strip()!r}"
            )

        existing_bytes = canonical_json_bytes_v1(existing) + b"\n"
        existing_sha = _sha256_bytes(existing_bytes)
        existing_manifest_hash = _sha256_bytes(
            canonical_json_bytes_v1(existing.get("input_manifest") or [])
        )

        if existing_manifest_hash == candidate_manifest_hash:
            if existing_sha != candidate_sha:
                raise SystemExit(
                    "FAIL: SAME_INPUT_MANIFEST_BUT_DIFFERENT_OUTPUT_BYTES "
                    f"path={out_path} manifest_hash={candidate_manifest_hash} "
                    f"existing_sha={existing_sha} candidate_sha={candidate_sha}"
                )
            return f"EXISTS_IDENTICAL sha256={existing_sha}"

        tmp = out_path.with_suffix(out_path.suffix + ".tmp")
        tmp.write_bytes(payload)
        os.replace(tmp, out_path)
        return f"REPLACED_STALE prior_sha256={existing_sha} sha256={candidate_sha}"

    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_bytes(payload)
    os.replace(tmp, out_path)
    return f"WROTE sha256={candidate_sha}"


def _git_sha() -> str:
    try:
        return str(resolve_release_provenance().get("git_sha") or "").strip() or "UNKNOWN"
    except Exception:
        pass
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


def _nav_total_cents_from_nav_v2(o: Dict[str, Any]) -> int:
    # Deterministic extraction for accounting_nav_v2.
    # Prefer explicit cents if present; else use nav.nav_total (dollars); else history.end_nav (dollars).
    v = o.get("nav_total_cents")
    if v is not None:
        try:
            return int(v)
        except Exception:
            raise ValueError("NAV_TOTAL_CENTS_NOT_INT")

    nav = o.get("nav")
    if isinstance(nav, dict):
        nt = nav.get("nav_total")
        if nt is not None:
            try:
                # nav_total is dollars
                return int(Decimal(str(nt)) * Decimal("100"))
            except Exception:
                raise ValueError("NAV_NAV_TOTAL_NOT_NUMERIC")

    hist = o.get("history")
    if isinstance(hist, dict):
        end_nav = hist.get("end_nav")
        if end_nav is not None:
            try:
                return int(Decimal(str(end_nav)) * Decimal("100"))
            except Exception:
                raise ValueError("HISTORY_END_NAV_NOT_NUMERIC")

    raise ValueError("NAV_TOTAL_MISSING")


def _read_nav_total_cents(day: str) -> Tuple[int, Path, str]:
    p2 = (NAV_V2_ROOT / day / "nav.v2.json").resolve()
    if p2.exists():
        o = _read_json_obj(p2)
        nav_cents = _nav_total_cents_from_nav_v2(o)
        if nav_cents <= 0:
            # Allow explicit bootstrap stub NAV v2 (day0) to pass with nav_cents=0.
            nav = o.get("nav") if isinstance(o, dict) else None
            notes = nav.get("notes") if isinstance(nav, dict) else None
            if isinstance(notes, list) and ("DAY0_BOOTSTRAP_STUB_NAV_V2" in notes):
                return 0, p2, _sha256_file(p2)

            # fail-closed: zero NAV makes capacity model meaningless
            raise SystemExit(f"FAIL: LIQPOL_NAV_TOTAL_CENTS_NONPOSITIVE day={day} nav_total_cents={nav_cents}")
        return nav_cents, p2, _sha256_file(p2)

    p1 = (NAV_V1_ROOT / day / "nav.json").resolve()
    if p1.exists():
        o = _read_json_obj(p1)
        nav = int((o.get("nav_total") or 0))
        nav_cents = nav * 100
        if nav_cents <= 0:
            raise SystemExit(f"FAIL: LIQPOL_NAV_TOTAL_CENTS_NONPOSITIVE day={day} nav_total_cents={nav_cents}")
        return nav_cents, p1, _sha256_file(p1)

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


def _load_capauth_engine_execution_sets() -> Tuple[set[str], set[str], Path, str]:
    if not CAPAUTH_POLICY_PATH.exists():
        raise SystemExit(f"FAIL: LIQPOL_CAPITAL_AUTHORITY_POLICY_MISSING: {CAPAUTH_POLICY_PATH}")
    policy = _read_json_obj(CAPAUTH_POLICY_PATH)
    sleeves = policy.get("sleeves")
    if not isinstance(sleeves, list) or not sleeves:
        raise SystemExit("FAIL: LIQPOL_CAPITAL_AUTHORITY_POLICY_SLEEVES_INVALID_OR_EMPTY")

    all_engine_ids: set[str] = set()
    executable_engine_ids: set[str] = set()
    for sleeve in sleeves:
        if not isinstance(sleeve, dict):
            continue
        limits = sleeve.get("limits") if isinstance(sleeve.get("limits"), dict) else {}
        max_risk = limits.get("max_capital_at_risk_cents")
        engine_ids = sleeve.get("engine_ids")
        if not isinstance(engine_ids, list):
            continue
        is_executable = isinstance(max_risk, int) and max_risk > 0
        for engine_id_raw in engine_ids:
            engine_id = str(engine_id_raw or "").strip()
            if not engine_id:
                continue
            all_engine_ids.add(engine_id)
            if is_executable:
                executable_engine_ids.add(engine_id)

    if not all_engine_ids:
        raise SystemExit("FAIL: LIQPOL_CAPITAL_AUTHORITY_POLICY_ENGINE_IDS_EMPTY")
    if not executable_engine_ids:
        raise SystemExit("FAIL: LIQPOL_CAPITAL_AUTHORITY_POLICY_EXECUTABLE_ENGINE_IDS_EMPTY")
    return executable_engine_ids, all_engine_ids, CAPAUTH_POLICY_PATH, _sha256_file(CAPAUTH_POLICY_PATH)


def _recovery_command_for_reason(reason_codes: List[str], *, symbol: str) -> str:
    reasons = set(reason_codes)
    if "LIQPOL_MARKET_DATA_FILE_MISSING" in reasons:
        return f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_market_data_snapshot_required_day_v1.py --symbol {symbol}"
    if "LIQPOL_INSUFFICIENT_HISTORY" in reasons or "LIQPOL_ADV_BELOW_MIN" in reasons:
        return f"refresh governed market_data_snapshot_v1 history for {symbol} before rerunning liquidity_slippage_gate_v1"
    if "LIQPOL_NOTIONAL_EXCEEDS_CAP" in reasons:
        return "reduce governed intent notional or update capital policy only through approved governance"
    if "LIQPOL_PARTICIPATION_EXCEEDS_CAP" in reasons or "LIQPOL_SLIPPAGE_EXCEEDS_CAP" in reasons:
        return f"provide fresher governed liquidity evidence for {symbol} or leave intent rejected"
    if "LIQPOL_ENGINE_NOT_EXECUTABLE_BY_CAPITAL_POLICY" in reasons:
        return "no recovery required for liquidity gate; engine is zero-cap non-executable by capital authority policy"
    if "LIQPOL_ENGINE_NOT_IN_CAPITAL_POLICY" in reasons:
        return "register engine in C2_CAPITAL_AUTHORITY_POLICY_V1 before liquidity gate evaluation"
    if "LIQPOL_PASS" in reasons:
        return "none"
    return "rerun governed liquidity_slippage_gate_v1 after fixing listed input evidence"


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
    # Same-day corrected snapshots may coexist with stale prior snapshots for the
    # same intent_id. Collapse to one deterministic file per intent_id in stable
    # path order so refreshed artifacts do not double-count against symbol caps.
    by_intent_id: Dict[str, Path] = {}
    passthrough: List[Path] = []
    for p in sorted([p for p in d.glob("*.json") if p.is_file()]):
        try:
            obj = _read_json_obj(p)
        except Exception:
            passthrough.append(p)
            continue
        intent_id = str(obj.get("intent_id") or "").strip()
        if not intent_id:
            passthrough.append(p)
            continue
        by_intent_id[intent_id] = p
    return sorted(passthrough + list(by_intent_id.values()))


def _extract_intent_symbol_and_pct(intent_obj: Dict[str, Any]) -> Tuple[str, str]:
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
    if tnp == "":
        raise ValueError("TARGET_NOTIONAL_PCT_MISSING")
    return sym, tnp


def main() -> int:
    global TRUTH_ROOT, OUT_ROOT, DATASET_MANIFEST, DATASET_ROOT, INTENTS_DIR_ROOT, NAV_V2_ROOT, NAV_V1_ROOT

    ap = argparse.ArgumentParser(prog="run_liquidity_slippage_gate_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--truth_root", default="", help="Absolute source-authoritative truth root override.")
    args = ap.parse_args()

    TRUTH_ROOT = _resolve_gate_truth_root(str(args.truth_root or ""))
    OUT_ROOT = (TRUTH_ROOT / "reports" / "liquidity_slippage_gate_v1").resolve()
    DATASET_MANIFEST = (TRUTH_ROOT / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()
    DATASET_ROOT = (TRUTH_ROOT / "market_data_snapshot_v1").resolve()
    INTENTS_DIR_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()
    NAV_V2_ROOT = (TRUTH_ROOT / "accounting_v2" / "nav").resolve()
    NAV_V1_ROOT = (TRUTH_ROOT / "accounting_v1" / "nav").resolve()

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

    executable_engine_ids, all_policy_engine_ids, capauth_policy_path, capauth_policy_sha = (
        _load_capauth_engine_execution_sets()
    )
    input_manifest.append(
        {"type": "capital_authority_policy", "path": str(capauth_policy_path), "sha256": capauth_policy_sha}
    )

    if not DATASET_MANIFEST.exists():
        raise SystemExit(f"FAIL: LIQPOL_MARKET_DATA_FILE_MISSING: {DATASET_MANIFEST}")

    ds_manifest_sha = _sha256_file(DATASET_MANIFEST)
    input_manifest.append({"type": "market_data_dataset_manifest", "path": str(DATASET_MANIFEST), "sha256": ds_manifest_sha})

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
            "producer": {"repo": "constellation", "module": "ops/tools/run_liquidity_slippage_gate_v1.py", "git_sha": _git_sha()},
            "status": status,
            "reason_codes": reason_codes,
            "input_manifest": input_manifest,
            "policy": {"path": str(POLICY_PATH), "sha256": pol_sha, "schema_path": str(pol_schema_path), "schema_sha256": pol_schema_sha},
            "results": {"per_intent": [], "totals": {"intents_total": 0, "intents_failed": 0, "intents_passed": 0, "intents_skipped": 0}},
            "gate_sha256": "0" * 64,
        }

        tmp = dict(out_obj)
        tmp["gate_sha256"] = None
        out_obj["gate_sha256"] = _sha256_bytes(canonical_json_bytes_v1(tmp))
        out_dir = (OUT_ROOT / day).resolve()
        out_path = (out_dir / "liquidity_slippage_gate.v1.json").resolve()
        try:
            action = _write_gate_report(out_path, out_obj)
        except ImmutableWriteError as e:
            raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

        print(
            f"OK: liquidity_slippage_gate_v1 status={status} sha256={_sha256_file(out_path)} "
            f"path={out_path} action={action}"
        )
        return 0 if status in ("PASS", "OK") else 1

    nav_cents, nav_path, nav_sha = _read_nav_total_cents(day)
    input_manifest.append({"type": "accounting_nav", "path": str(nav_path), "sha256": nav_sha})

    per_intent: List[Dict[str, Any]] = []
    failed = 0
    passed = 0
    skipped = 0

    allow_missing = set([str(x).strip().upper() for x in ((pol.get("defaults") or {}).get("allow_missing_symbols") or [])])

    for ip in intents:
        sha = _sha256_file(ip)
        input_manifest.append({"type": "intent", "path": str(ip), "sha256": sha})

        engine_id = "UNKNOWN"
        symbol = "UNKNOWN"
        tnp = Decimal("0")

        try:
            intent_obj = _read_json_obj(ip)

            if "engine" in intent_obj and isinstance(intent_obj.get("engine"), dict):
                engine_id = str((intent_obj["engine"].get("engine_id") or "")).strip() or "UNKNOWN"
            else:
                engine_id = str(intent_obj.get("engine_id") or "").strip() or "UNKNOWN"

            symbol, tnp_str = _extract_intent_symbol_and_pct(intent_obj)
            if not symbol:
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

            if engine_id and engine_id != "UNKNOWN" and engine_id in all_policy_engine_ids and engine_id not in executable_engine_ids:
                skipped += 1
                rc = ["LIQPOL_ENGINE_NOT_EXECUTABLE_BY_CAPITAL_POLICY"]
                per_intent.append(
                    {
                        "intent_hash": sha,
                        "engine_id": engine_id,
                        "symbol": symbol,
                        "decision": "SKIP",
                        "reason_codes": rc,
                        "recovery_command": _recovery_command_for_reason(rc, symbol=symbol),
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

            if engine_id and engine_id != "UNKNOWN" and engine_id not in all_policy_engine_ids:
                failed += 1
                rc = ["LIQPOL_ENGINE_NOT_IN_CAPITAL_POLICY", "LIQPOL_FAIL_CLOSED_REQUIRED"]
                per_intent.append(
                    {
                        "intent_hash": sha,
                        "engine_id": engine_id,
                        "symbol": symbol,
                        "decision": "FAIL",
                        "reason_codes": rc,
                        "recovery_command": _recovery_command_for_reason(rc, symbol=symbol),
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

            # IMPORTANT: zero-notional intents are SKIP (not parse failure)
            if est_notional_2dp <= Decimal("0"):
                rc = ["LIQPOL_NOTIONAL_ZERO"]
                skipped += 1
                per_intent.append(
                    {
                        "intent_hash": sha,
                        "engine_id": engine_id,
                        "symbol": symbol,
                        "decision": "SKIP",
                        "reason_codes": rc,
                        "recovery_command": _recovery_command_for_reason(rc, symbol=symbol),
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

            year = int(day[0:4])
            data_path, data_sha, bars = _load_bars_for_symbol_year(symbol, year)
            if not bars:
                if symbol in allow_missing:
                    rc = ["LIQPOL_MARKET_DATA_FILE_MISSING"]
                    skipped += 1
                    per_intent.append(
                        {
                            "intent_hash": sha,
                            "engine_id": engine_id,
                            "symbol": symbol,
                            "decision": "SKIP",
                            "reason_codes": rc,
                            "recovery_command": _recovery_command_for_reason(rc, symbol=symbol),
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

            est_shares = int((est_notional_2dp / close).to_integral_value(rounding=ROUND_DOWN))
            part = (Decimal(est_shares) / Decimal(adv_sh)) if adv_sh > 0 else Decimal("0")
            part_6 = Decimal(_decimal_str_6dp(part))

            est_slip = base_bps + (slope_bps * (part * Decimal("100")))
            est_slip_2 = Decimal(_decimal_str_2dp(est_slip))

            rc: List[str] = []
            decision = "PASS"

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
                    "engine_id": engine_id,
                    "symbol": symbol,
                    "decision": decision,
                    "reason_codes": rc,
                    "recovery_command": _recovery_command_for_reason(rc, symbol=symbol),
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
            rc = ["LIQPOL_INTENT_PARSE_ERROR", f"LIQPOL_EXC:{type(e).__name__}", "LIQPOL_FAIL_CLOSED_REQUIRED"]
            per_intent.append(
                {
                    "intent_hash": sha,
                    "engine_id": engine_id,
                    "symbol": symbol,
                    "decision": "FAIL",
                    "reason_codes": rc,
                    "recovery_command": _recovery_command_for_reason(rc, symbol=symbol),
                    "metrics": {
                        "nav_total_cents": nav_cents,
                        "target_notional_pct": _decimal_str_6dp(tnp) if isinstance(tnp, Decimal) else "0.000000",
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

    totals = {"intents_total": len(per_intent), "intents_failed": failed, "intents_passed": passed, "intents_skipped": skipped}

    status = "PASS" if failed == 0 else "FAIL"
    reason_codes = ["LIQPOL_PASS"] if status == "PASS" else ["LIQPOL_FAIL_CLOSED_REQUIRED"]
    recovery_commands = sorted(
        {
            str(row.get("recovery_command") or "").strip()
            for row in per_intent
            if str(row.get("decision") or "").strip() == "FAIL"
            and str(row.get("recovery_command") or "").strip()
            and str(row.get("recovery_command") or "").strip() != "none"
        }
    )

    out_obj: Dict[str, Any] = {
        "schema_id": "liquidity_slippage_gate",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": f"{day}T00:00:00Z",
        "producer": {"repo": "constellation", "module": "ops/tools/run_liquidity_slippage_gate_v1.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "recovery_commands": recovery_commands,
        "input_manifest": input_manifest,
        "policy": {"path": str(POLICY_PATH), "sha256": pol_sha, "schema_path": str(pol_schema_path), "schema_sha256": pol_schema_sha},
        "results": {"per_intent": per_intent, "totals": totals},
        "gate_sha256": "0" * 64,
    }

    tmp = dict(out_obj)
    tmp["gate_sha256"] = None
    out_obj["gate_sha256"] = _sha256_bytes(canonical_json_bytes_v1(tmp))

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "liquidity_slippage_gate.v1.json").resolve()
    try:
        action = _write_gate_report(out_path, out_obj)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    print(
        f"OK: liquidity_slippage_gate_v1 status={status} sha256={_sha256_file(out_path)} "
        f"path={out_path} action={action}"
    )
    return 0 if status in ("PASS", "OK") else 1


if __name__ == "__main__":
    raise SystemExit(main())
