from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import write_file_immutable_v1
from constellation_2.phaseF.accounting.lib.mutable_write_v1 import write_file_atomic_mutable_v1


# Drawdown convention authority (canonical, negative underwater)
# Contract: C2_DRAWDOWN_CONVENTION_V1
C2_DRAWDOWN_CONTRACT_ID = "C2_DRAWDOWN_CONVENTION_V1"
DRAWDOWN_QUANT = Decimal("0.000001")  # 6dp

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

ALLOC_ROOT = (TRUTH_ROOT / "allocation_v1").resolve()
INTENTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()

SCHEMA_SUMMARY = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_summary.v1.schema.json"
SCHEMA_DECISION = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_decision.v1.schema.json"
SCHEMA_LATEST = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_latest_pointer.v1.schema.json"
SCHEMA_FAILURE = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_failure.v1.schema.json"

# Deterministic sleeve caps (capital buckets) — v1 implementation constant map.
# NOTE: This is the minimum viable capital allocation enforcement until a governed operator_inputs risk_budget spine exists.
ENGINE_CAP_PCT = {
    "C2_TREND_EQ_PRIMARY_V1": Decimal("0.40"),
    "C2_VOL_INCOME_DEFINED_RISK_V1": Decimal("0.40"),
    "C2_MEAN_REVERSION_EQ_V1": Decimal("0.20"),
}

SUPPORTED_INTENT_SCHEMAS = {
    ("exposure_intent", "v1"),
}

RC_ACCOUNTING_NOT_OK = "G_BLOCK_ACCOUNTING_NOT_OK"
RC_INTENTS_DAY_DIR_MISSING = "G_INTENTS_DAY_DIR_MISSING"
RC_INTENTS_DAY_DIR_EMPTY = "G_INTENTS_DAY_DIR_EMPTY"
RC_UNSUPPORTED_INTENT_SCHEMA = "G_UNSUPPORTED_INTENT_SCHEMA"
RC_ENGINE_NOT_IN_CAPS = "G_ENGINE_NOT_ALLOCATED"
RC_INTENT_EXCEEDS_CAP = "G_INTENT_EXCEEDS_ENGINE_CAP"


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _sha256_file(path: Path) -> str:
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sha256_bytes(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()


def _lock_git_sha_if_exists(existing_path: Path, provided_sha: str) -> Optional[str]:
    if existing_path.exists() and existing_path.is_file():
        ex = _read_json_obj(existing_path)
        prod = ex.get("producer")
        ex_sha = prod.get("git_sha") if isinstance(prod, dict) else None
        if isinstance(ex_sha, str) and ex_sha.strip():
            if ex_sha.strip() != provided_sha:
                return ex_sha.strip()
    return None


def _parse_dd_pct_str_or_fail(nav_obj: Dict[str, Any]) -> Tuple[int, int, int, str]:
    """
    Fail-closed: require accounting nav history has populated drawdown fields.
    Returns: (nav_total_int, peak_nav_int, drawdown_abs_int, drawdown_pct_str)
    """
    nav = nav_obj.get("nav")
    if not isinstance(nav, dict):
        raise ValueError("ACCOUNTING_NAV_OBJECT_MISSING")
    nav_total = nav.get("nav_total")
    if not isinstance(nav_total, int):
        raise ValueError("ACCOUNTING_NAV_TOTAL_NOT_INT")

    hist = nav_obj.get("history")
    if not isinstance(hist, dict):
        raise ValueError("ACCOUNTING_HISTORY_MISSING")
    peak_nav = hist.get("peak_nav")
    dd_abs = hist.get("drawdown_abs")
    dd_pct = hist.get("drawdown_pct")
    if not isinstance(peak_nav, int):
        raise ValueError("ACCOUNTING_PEAK_NAV_NOT_INT")
    if not isinstance(dd_abs, int):
        raise ValueError("ACCOUNTING_DRAWDOWN_ABS_NOT_INT")
    if not isinstance(dd_pct, str) or not dd_pct.strip():
        raise ValueError("ACCOUNTING_DRAWDOWN_PCT_MISSING_OR_NOT_STRING")

    d = Decimal(dd_pct).quantize(DRAWDOWN_QUANT, rounding=ROUND_HALF_UP)
    dd_pct_s = f"{d:.6f}"
    return int(nav_total), int(peak_nav), int(dd_abs), dd_pct_s


def drawdown_multiplier_v1(drawdown_pct_s: str) -> str:
    """
    Canonical multiplier rule per C2_DRAWDOWN_CONVENTION_V1 and G_THROTTLE_RULES_V1.
    Returns multiplier as string with 2 dp.
    """
    dd = Decimal(drawdown_pct_s).quantize(DRAWDOWN_QUANT, rounding=ROUND_HALF_UP)

    if dd <= Decimal("-0.150000"):
        return "0.25"
    if dd <= Decimal("-0.100000"):
        return "0.50"
    if dd <= Decimal("-0.050000"):
        return "0.75"
    return "1.00"


def _dec01(s: str, name: str) -> Decimal:
    if not isinstance(s, str) or not s.strip():
        raise ValueError(f"DECIMAL_STRING_REQUIRED: {name}")
    try:
        d = Decimal(s.strip())
    except InvalidOperation as e:
        raise ValueError(f"DECIMAL_PARSE_FAILED: {name}={s!r}") from e
    if d < Decimal("0") or d > Decimal("1"):
        raise ValueError(f"DECIMAL_OUT_OF_RANGE_0_1: {name}={s!r}")
    return d


def _list_intent_files(day_utc: str) -> List[Path]:
    d = (INTENTS_ROOT / day_utc).resolve()
    if not d.exists() or not d.is_dir():
        raise FileNotFoundError(f"INTENTS_DAY_DIR_MISSING: {str(d)}")
    files = sorted([p for p in d.iterdir() if p.is_file() and p.name.endswith(".json")])
    if not files:
        raise ValueError(f"INTENTS_DAY_DIR_EMPTY: {str(d)}")
    return files


def _write_failure(
    *,
    day_utc: str,
    producer_repo: str,
    producer_sha: str,
    module: str,
    reason_codes: List[str],
    input_manifest: List[Dict[str, Any]],
    code: str,
    message: str,
    details: Dict[str, Any],
    attempted_outputs: List[Dict[str, Any]],
) -> None:
    produced_utc = f"{day_utc}T00:00:00Z"
    fail_obj: Dict[str, Any] = {
        "schema_id": "C2_ALLOCATION_FAILURE_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": "FAIL_CORRUPT_INPUTS",
        "reason_codes": list(reason_codes),
        "input_manifest": list(input_manifest),
        "failure": {
            "code": code,
            "message": message,
            "details": dict(details),
            "attempted_outputs": list(attempted_outputs),
        },
    }
    validate_against_repo_schema_v1(fail_obj, REPO_ROOT, SCHEMA_FAILURE)
    b = canonical_json_bytes_v1(fail_obj) + b"\n"
    out_path = (ALLOC_ROOT / "failures" / day_utc / "failure.json").resolve()


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_allocation_day_v1",
        description="C2 Bundle G Allocation v1 (drawdown enforcement + per-intent allocation decisions for exposure_intent.v1).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()
    module = "constellation_2/phaseG/allocation/run/run_allocation_day_v1.py"

    summary_dir = (ALLOC_ROOT / "summary" / day_utc).resolve()
    summary_path = summary_dir / "summary.json"

    # fail-closed if rerun with different SHA
    ex_sha = _lock_git_sha_if_exists(summary_path, producer_sha)
    if ex_sha is not None:
        print(f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha} provided={producer_sha}", file=sys.stderr)
        return 4

    produced_utc = f"{day_utc}T00:00:00Z"

    # ---- Required accounting nav ----
    nav_path = (TRUTH_ROOT / "accounting_v1" / "nav" / day_utc / "nav.json").resolve()
    input_manifest: List[Dict[str, Any]] = []
    attempted_outputs: List[Dict[str, Any]] = []

    try:
        nav_obj = _read_json_obj(nav_path)
        nav_status = str(nav_obj.get("status") or "").strip() or "UNKNOWN"
        nav_sha = _sha256_file(nav_path)
        nav_total, peak_nav, dd_abs, dd_pct_s = _parse_dd_pct_str_or_fail(nav_obj)
    except Exception as e:
        _write_failure(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_sha=producer_sha,
            module=module,
            reason_codes=["G_FAIL_ACCOUNTING_NAV_INVALID"],
            input_manifest=[],
            code="ACCOUNTING_DAY_NAV_MISSING_OR_INVALID",
            message="Accounting nav missing or invalid",
            details={"error": str(e), "nav_path": str(nav_path)},
            attempted_outputs=[],
        )
        print(f"FAIL: ACCOUNTING_DAY_NAV_MISSING_OR_INVALID: {e}", file=sys.stderr)
        return 2

    input_manifest.append({"type": "other", "path": str(nav_path), "sha256": nav_sha, "day_utc": day_utc, "producer": "bundle_f_accounting_v1"})

    # Optional positions effective pointer (schema supports it in input_manifest enum; we keep as other if present)
    pos_eff_ptr_path = (TRUTH_ROOT / "positions_v1" / "effective_v1" / "days" / day_utc / "positions_effective_pointer.v1.json").resolve()
    if pos_eff_ptr_path.exists() and pos_eff_ptr_path.is_file():
        try:
            _ = _read_json_obj(pos_eff_ptr_path)
            pos_eff_ptr_sha = _sha256_file(pos_eff_ptr_path)
            input_manifest.append({"type": "other", "path": str(pos_eff_ptr_path), "sha256": pos_eff_ptr_sha, "day_utc": day_utc, "producer": "positions_effective_v1"})
        except Exception as e:
            _write_failure(
                day_utc=day_utc,
                producer_repo=producer_repo,
                producer_sha=producer_sha,
                module=module,
                reason_codes=["G_FAIL_POSITIONS_EFFECTIVE_POINTER_INVALID"],
                input_manifest=list(input_manifest),
                code="POSITIONS_EFFECTIVE_POINTER_INVALID",
                message="Positions effective pointer invalid",
                details={"error": str(e), "path": str(pos_eff_ptr_path)},
                attempted_outputs=[],
            )
            print(f"FAIL: POSITIONS_EFFECTIVE_POINTER_INVALID: {e}", file=sys.stderr)
            return 2

    # ---- Drawdown block ----
    mult_s = drawdown_multiplier_v1(dd_pct_s)
    thresholds = [
        {"drawdown_pct": "0.000000", "multiplier": "1.00"},
        {"drawdown_pct": "-0.050000", "multiplier": "0.75"},
        {"drawdown_pct": "-0.100000", "multiplier": "0.50"},
        {"drawdown_pct": "-0.150000", "multiplier": "0.25"},
    ]
    dd_block = {
        "contract_id": C2_DRAWDOWN_CONTRACT_ID,
        "nav_source_path": str(nav_path),
        "nav_source_sha256": nav_sha,
        "nav_asof_day_utc": day_utc,
        "rolling_peak_nav": int(peak_nav),
        "nav_total": int(nav_total),
        "drawdown_abs": int(dd_abs),
        "drawdown_pct": dd_pct_s,
        "multiplier": mult_s,
        "thresholds": thresholds,
    }

    reason_codes: List[str] = []
    notes: List[str] = []

    # Accounting not OK blocks all new entries deterministically
    accounting_ok = (nav_status == "OK")
    if not accounting_ok:
        reason_codes.append(RC_ACCOUNTING_NOT_OK)
        notes.append("accounting not OK: block all new entries")

    # ---- Intent decisions ----
    decisions_summary: List[Dict[str, Any]] = []
    allow_ct = 0
    block_ct = 0

    try:
        intent_files = _list_intent_files(day_utc)
        # Add intents_list to input manifest: stable deterministic list file = directory path hash is not allowed,
        # so we include each intent file as input_manifest entries (type=intent is allowed in allocation_decision schema;
        # summary input_manifest uses enum 'other'/'intents_list'. We'll record intents_day_dir as intents_list with sha256=0..0 is not allowed.
        # Instead: include each intent file as type=other for summary, and as type=intent for per-decision.
        for p in intent_files:
            input_manifest.append({"type": "other", "path": str(p.resolve()), "sha256": _sha256_file(p), "day_utc": day_utc, "producer": "intents_v1"})
    except FileNotFoundError as e:
        reason_codes.append(RC_INTENTS_DAY_DIR_MISSING)
        notes.append("intents day dir missing: no decisions produced")
        intent_files = []
    except ValueError as e:
        reason_codes.append(RC_INTENTS_DAY_DIR_EMPTY)
        notes.append("intents day dir empty: no decisions produced")
        intent_files = []
    except Exception as e:
        _write_failure(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_sha=producer_sha,
            module=module,
            reason_codes=["G_FAIL_INTENTS_LIST_INVALID"],
            input_manifest=list(input_manifest),
            code="INTENTS_LIST_INVALID",
            message="Failed listing intents for day",
            details={"error": str(e)},
            attempted_outputs=[],
        )
        print(f"FAIL: INTENTS_LIST_INVALID: {e}", file=sys.stderr)
        return 2

    # Decisions dir
    decisions_dir = (ALLOC_ROOT / "decisions" / day_utc).resolve()

    for p_intent in intent_files:
        intent_bytes = p_intent.read_bytes()
        intent_hash = _sha256_bytes(intent_bytes)

        out_dec_path = (decisions_dir / f"{intent_hash}.allocation_decision.v1.json").resolve()
        attempted_outputs.append({"path": str(out_dec_path), "sha256": None})

        try:
            intent_obj = _read_json_obj(p_intent)
            schema_id = str(intent_obj.get("schema_id") or "").strip()
            schema_version = str(intent_obj.get("schema_version") or "").strip()

            engine = intent_obj.get("engine")
            if not isinstance(engine, dict):
                raise ValueError("INTENT_ENGINE_MISSING")
            engine_id = str(engine.get("engine_id") or "").strip()
            intent_id = str(intent_obj.get("intent_id") or "").strip()
            if not intent_id:
                raise ValueError("INTENT_ID_MISSING")

            # default BLOCK
            status = "BLOCK"
            binding_constraints: List[str] = []
            contracts_allowed = 0

            # effective_risk_budget is a JSON number; use integer basis-points (bp) to avoid floats.
            effective_risk_budget_bp = 0

            if (schema_id, schema_version) not in SUPPORTED_INTENT_SCHEMAS:
                reason = f"{RC_UNSUPPORTED_INTENT_SCHEMA}: {schema_id}.{schema_version}"
                binding_constraints.append(reason)
                block_ct += 1
            elif not accounting_ok:
                binding_constraints.append(RC_ACCOUNTING_NOT_OK)
                block_ct += 1
            else:
                # ExposureIntent v1 cap enforcement
                cap = ENGINE_CAP_PCT.get(engine_id)
                if cap is None:
                    binding_constraints.append(RC_ENGINE_NOT_IN_CAPS)
                    block_ct += 1
                else:
                    target_pct = _dec01(str(intent_obj.get("target_notional_pct") or ""), "target_notional_pct")
                    dd_mult = Decimal(mult_s)  # "1.00" etc
                    effective_cap = (cap * dd_mult).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

                    # Convert to bp int for schema number field (integer is allowed)
                    effective_risk_budget_bp = int((effective_cap * Decimal("10000")).to_integral_value(rounding=ROUND_HALF_UP))

                    binding_constraints.append(f"ENGINE_CAP_PCT={str(cap)}")
                    binding_constraints.append(f"DRAWDOWN_MULTIPLIER={mult_s}")
                    binding_constraints.append(f"EFFECTIVE_CAP_PCT={str(effective_cap)}")

                    if target_pct <= effective_cap:
                        status = "ALLOW"
                        contracts_allowed = 1
                        allow_ct += 1
                    else:
                        binding_constraints.append(RC_INTENT_EXCEEDS_CAP)
                        block_ct += 1

            # Build allocation_decision.v1 object (per schema)
            dec_obj: Dict[str, Any] = {
                "schema_id": "C2_ALLOCATION_DECISION_V1",
                "schema_version": 1,
                "produced_utc": produced_utc,
                "day_utc": day_utc,
                "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
                "status": status,
                "reason_codes": [],
                "input_manifest": [
                    {"type": "intent", "path": str(p_intent.resolve()), "sha256": _sha256_file(p_intent), "day_utc": day_utc, "producer": "intents_v1"},
                    {"type": "other", "path": str(nav_path), "sha256": nav_sha, "day_utc": day_utc, "producer": "bundle_f_accounting_v1"},
                ],
                "decision": {
                    "intent_id": intent_id,
                    "engine_id": engine_id,
                    "contracts_allowed": int(contracts_allowed),
                    "effective_risk_budget": int(effective_risk_budget_bp),
                    "binding_constraints": list(binding_constraints),
                },
            }

            validate_against_repo_schema_v1(dec_obj, REPO_ROOT, SCHEMA_DECISION)

            payload = canonical_json_bytes_v1(dec_obj) + b"\n"
            _ = write_file_atomic_mutable_v1(path=latest_path, data=l_bytes, create_dirs=True)
            dec_sha = _sha256_bytes(payload)

            decisions_summary.append({"intent_id": intent_id, "status": status, "path": str(out_dec_path), "sha256": dec_sha})

        except Exception as e:  # noqa: BLE001
            # If decision cannot be produced, fail closed at day level
            _write_failure(
                day_utc=day_utc,
                producer_repo=producer_repo,
                producer_sha=producer_sha,
                module=module,
                reason_codes=["G_FAIL_DECISION_BUILD"],
                input_manifest=list(input_manifest),
                code="ALLOCATION_DECISION_BUILD_FAILED",
                message="Failed building allocation decision",
                details={"error": str(e), "intent_path": str(p_intent), "intent_hash": intent_hash},
                attempted_outputs=list(attempted_outputs),
            )
            print(f"FAIL: ALLOCATION_DECISION_BUILD_FAILED: intent_file={str(p_intent)} err={e}", file=sys.stderr)
            return 2

    # ---- Summary ----
    summary_obj: Dict[str, Any] = {
        "schema_id": "C2_ALLOCATION_SUMMARY_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": "OK",
        "reason_codes": list(reason_codes),
        "input_manifest": list(input_manifest) if input_manifest else [{"type": "other", "path": str(nav_path), "sha256": nav_sha, "day_utc": day_utc, "producer": "bundle_f_accounting_v1"}],
        "summary": {
            "decisions": list(decisions_summary),
            "counts": {"allow": int(allow_ct), "block": int(block_ct)},
            "notes": list(notes),
            "drawdown_enforcement": dd_block,
        },
    }

    validate_against_repo_schema_v1(summary_obj, REPO_ROOT, SCHEMA_SUMMARY)
    s_bytes = canonical_json_bytes_v1(summary_obj) + b"\n"
    _ = write_file_immutable_v1(path=summary_path, data=s_bytes, create_dirs=True)
    s_sha = _sha256_bytes(s_bytes)

    # ---- Latest pointer (points to summary) ----
    latest_obj: Dict[str, Any] = {
        "schema_id": "C2_ALLOCATION_LATEST_POINTER_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": "OK",
        "reason_codes": list(reason_codes),
        "pointers": {"summary_path": str(summary_path.resolve()), "summary_sha256": s_sha},
    }
    validate_against_repo_schema_v1(latest_obj, REPO_ROOT, SCHEMA_LATEST)
    l_bytes = canonical_json_bytes_v1(latest_obj) + b"\n"
    latest_path = (ALLOC_ROOT / "latest.json").resolve()
    _ = write_file_atomic_mutable_v1(path=latest_path, data=l_bytes, create_dirs=True)

    print("OK: ALLOCATION_SUMMARY_AND_DECISIONS_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
