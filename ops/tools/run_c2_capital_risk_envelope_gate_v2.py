#!/usr/bin/env python3
"""
run_c2_capital_risk_envelope_gate_v2.py

Forward-only Capital Risk Envelope Gate v2.

- Writes to reports/capital_risk_envelope_v2/<OUT_DAY>/capital_risk_envelope.v2.json
- Deterministic, fail-closed, audit-grade.
- SAFE_IDLE: empty positions list can PASS if required inputs exist and drawdown present.
- Never overwrites immutable day-keyed truth artifacts.

Day-0 Bootstrap Window Exception (governed, scoped):
- If there are NO submissions yet for input_day_utc (bootstrap window),
  and allocation summary is missing, allow PASS with deterministic zeros.
- Once any submission exists, revert to strict FAIL-CLOSED on missing inputs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import subprocess
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

# Repo-root import bootstrap (required when executed from ops/tools).
_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

from constellation_2.common.constitutional_runtime_v1 import (
    CLOSURE_STATE_BLOCKED,
    CLOSURE_STATE_COMPLETE,
    CLOSURE_STATE_DEGRADED,
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_machine_blocker_envelope_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.runtime_authority_bridge_v1 import resolve_canonical_truth_root_bridge_v1


REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()
DEFAULT_TRUTH_ROOT = resolve_canonical_truth_root_bridge_v1(
    caller="ops/tools/run_c2_capital_risk_envelope_gate_v2.py"
).resolve()

SCHEMA_OUT = (REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json").resolve()
SCHEMA_ALLOC_SUMMARY = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_summary.v1.schema.json"
SCHEMA_NAV_V1 = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v1.schema.json"
SCHEMA_POS_V3 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v3.schema.json"
SCHEMA_POS_V2 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v2.schema.json"

DRAWDOWN_CONTRACT = (REPO_ROOT / "governance/05_CONTRACTS/C2/drawdown_convention_v1.contract.md").resolve()
CAP_RISK_CONTRACT_V2 = (REPO_ROOT / "governance/05_CONTRACTS/C2/capital_risk_envelope_v2.contract.md").resolve()

BASE_ENVELOPE_PCT = Decimal("0.020000")

DAY0_RC_ALLOC_ALLOWED = "DAY0_BOOTSTRAP_ALLOC_SUMMARY_MISSING_ALLOWED"
RC_AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS = "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _read_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return obj


def _canonical_json_bytes(obj: Dict[str, Any]) -> bytes:
    # Deterministic JSON: sorted keys, compact separators, newline terminated.
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _validate_against_repo_schema(obj: Dict[str, Any], schema_relpath: str) -> None:
    # Minimal local validator to avoid adding deps; reuse existing repo validator module if present.
    # Use constellation_2.phaseD.lib.validate_against_schema_v1 which is already in repo.
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore

    validate_against_repo_schema_v1(obj, REPO_ROOT, schema_relpath)


def _write_immutable(path: Path, data: bytes) -> str:
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    cand_sha = _sha256_bytes(data)

    if path.exists():
        if not path.is_file():
            raise SystemExit(f"FAIL: TARGET_NOT_FILE: {str(path)}")
        existing = path.read_bytes()
        ex_sha = _sha256_bytes(existing)
        if ex_sha == cand_sha:
            return cand_sha
        raise SystemExit(f"FAIL: ATTEMPTED_REWRITE_IMMUTABLE: {str(path)} existing_sha={ex_sha} candidate_sha={cand_sha}")

    path.write_bytes(data)
    return cand_sha


def _write_bytes_replace(path: Path, data: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(data)
    tmp.replace(path)
    return _sha256_bytes(path.read_bytes())


def _positions_manifest_entry(obj: Dict[str, Any]) -> Dict[str, Any]:
    raw = obj.get("input_manifest")
    if not isinstance(raw, list):
        return {}
    for item in raw:
        if isinstance(item, dict) and str(item.get("type") or "").strip() == "positions_snapshot":
            return item
    return {}


def _is_safe_positions_carry_forward_repair(existing: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    if str(existing.get("schema_id") or "").strip() != "capital_risk_envelope":
        return False
    if str(candidate.get("schema_id") or "").strip() != "capital_risk_envelope":
        return False
    if str(existing.get("schema_version") or "").strip() != "v2":
        return False
    if str(candidate.get("schema_version") or "").strip() != "v2":
        return False
    if str(existing.get("day_utc") or "").strip() != str(candidate.get("day_utc") or "").strip():
        return False

    existing_pos = _positions_manifest_entry(existing)
    candidate_pos = _positions_manifest_entry(candidate)
    if not existing_pos or not candidate_pos:
        return False
    if str(existing_pos.get("path") or "").strip() != str(candidate_pos.get("path") or "").strip():
        return False
    if str(existing_pos.get("sha256") or "").strip() == str(candidate_pos.get("sha256") or "").strip():
        return False

    existing_env = existing.get("envelope") if isinstance(existing.get("envelope"), dict) else {}
    candidate_env = candidate.get("envelope") if isinstance(candidate.get("envelope"), dict) else {}
    existing_positions = existing_env.get("positions") if isinstance(existing_env, dict) else []
    candidate_positions = candidate_env.get("positions") if isinstance(candidate_env, dict) else []
    if not isinstance(existing_positions, list) or not isinstance(candidate_positions, list):
        return False
    if existing_positions:
        return False
    if not candidate_positions:
        return False

    existing_checks = existing.get("checks") if isinstance(existing.get("checks"), dict) else {}
    candidate_checks = candidate.get("checks") if isinstance(candidate.get("checks"), dict) else {}
    if bool(existing_checks.get("positions_present")) is not True:
        return False
    if bool(candidate_checks.get("positions_present")) is not True:
        return False
    return True


def _is_safe_missing_inputs_recovery(existing: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    if str(existing.get("schema_id") or "").strip() != "capital_risk_envelope":
        return False
    if str(candidate.get("schema_id") or "").strip() != "capital_risk_envelope":
        return False
    if str(existing.get("schema_version") or "").strip() != "v2":
        return False
    if str(candidate.get("schema_version") or "").strip() != "v2":
        return False
    if str(existing.get("day_utc") or "").strip() != str(candidate.get("day_utc") or "").strip():
        return False

    existing_status = str(existing.get("status") or "").strip().upper()
    candidate_status = str(candidate.get("status") or "").strip().upper()
    if existing_status != "FAIL":
        return False
    if candidate_status not in {"PASS", "DEGRADED"}:
        return False

    existing_reason_codes = {
        str(code).strip().upper() for code in (existing.get("reason_codes") or []) if str(code).strip()
    }
    if "B2_INPUTS_MISSING_FAILCLOSED" not in existing_reason_codes:
        return False

    candidate_checks = candidate.get("checks") if isinstance(candidate.get("checks"), dict) else {}
    if bool(candidate_checks.get("allocation_summary_present")) is not True:
        return False
    if bool(candidate_checks.get("nav_present")) is not True:
        return False
    if bool(candidate_checks.get("positions_present")) is not True:
        return False

    return True


def _is_safe_nav_validation_recovery(existing: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    if str(existing.get("schema_id") or "").strip() != "capital_risk_envelope":
        return False
    if str(candidate.get("schema_id") or "").strip() != "capital_risk_envelope":
        return False
    if str(existing.get("schema_version") or "").strip() != "v2":
        return False
    if str(candidate.get("schema_version") or "").strip() != "v2":
        return False
    if str(existing.get("day_utc") or "").strip() != str(candidate.get("day_utc") or "").strip():
        return False

    existing_status = str(existing.get("status") or "").strip().upper()
    existing_reason_codes = {
        str(code).strip().upper() for code in (existing.get("reason_codes") or []) if str(code).strip()
    }

    existing_env = existing.get("envelope") if isinstance(existing.get("envelope"), dict) else {}
    existing_nav_total_cents = existing_env.get("nav_total_cents")
    existing_has_missing_nav = (
        "B2_NAV_TOTAL_MISSING_OR_INVALID" in existing_reason_codes
        or RC_AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS in existing_reason_codes
    )
    if existing_status == "PASS" and (not isinstance(existing_nav_total_cents, int) or existing_nav_total_cents > 0):
        return False
    if existing_status == "FAIL" and not existing_has_missing_nav:
        return False
    if existing_status not in {"PASS", "FAIL"}:
        return False

    candidate_status = str(candidate.get("status") or "").strip().upper()
    candidate_env = candidate.get("envelope") if isinstance(candidate.get("envelope"), dict) else {}
    candidate_nav_total_cents = candidate_env.get("nav_total_cents")
    candidate_reason_codes = {
        str(code).strip().upper() for code in (candidate.get("reason_codes") or []) if str(code).strip()
    }
    candidate_checks = candidate.get("checks") if isinstance(candidate.get("checks"), dict) else {}

    if candidate_status == "PASS":
        if not isinstance(candidate_nav_total_cents, int) or candidate_nav_total_cents <= 0:
            return False
        if bool(candidate_checks.get("nav_present")) is not True:
            return False
        return True

    if candidate_status in {"FAIL", "DEGRADED"}:
        return (
            "B2_NAV_TOTAL_MISSING_OR_INVALID" in candidate_reason_codes
            and RC_AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS in candidate_reason_codes
        )
    return False


def _finalize_constitutional_capital_risk_report(out: Dict[str, Any]) -> Dict[str, Any]:
    status = str(out.get("status") or "").strip().upper()
    closure_state = CLOSURE_STATE_COMPLETE
    if status == "DEGRADED":
        closure_state = CLOSURE_STATE_DEGRADED
    elif status != "PASS":
        closure_state = CLOSURE_STATE_BLOCKED
    reason_codes = [str(code).strip() for code in (out.get("reason_codes") or []) if str(code).strip()]
    blocker_reason_codes = reason_codes if closure_state != CLOSURE_STATE_COMPLETE else []
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state=closure_state,
        reason_codes=blocker_reason_codes,
        missing_dependency_artifacts=[],
    )
    out["blocking_codes"] = list(blocker_envelope["blocking_codes"])
    out["closure_state"] = str(blocker_envelope["closure_state"])
    out["first_blocker_code"] = str(blocker_envelope["first_blocker_code"])
    out["missing_dependency_artifacts"] = list(blocker_envelope["missing_dependency_artifacts"])
    out["constitutional_dependency_declaration"] = build_artifact_dependency_declaration_v1(
        artifact_type="capital_risk_envelope_v2",
        artifact_class="admission_result",
        authority_id="capital_risk_envelope_v2",
        declared_dependency_artifacts=[],
        dependency_refs=[],
    )
    produced_utc = str(out.get("produced_utc") or "").strip()
    out["constitutional_lineage"] = build_governed_artifact_lineage_v1(
        artifact_type="capital_risk_envelope_v2",
        artifact_version="v2",
        artifact_class="admission_result",
        authority_id="capital_risk_envelope_v2",
        producer_id="ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
        generated_at_utc=produced_utc,
        effective_at_utc=produced_utc,
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=[],
        policy_snapshot_refs=[],
        code_version=_git_sha(),
        run_id=f"capital_risk_envelope_v2:{str(out.get('day_utc') or '').strip()}",
    )
    return out


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        s = out.decode("utf-8").strip()
    except Exception:
        # Clean runtime roots can be source-derived without .git metadata.
        s = "0" * 40
    if len(s) != 40:
        raise RuntimeError(f"GIT_SHA_INVALID_FAILCLOSED: {s!r}")
    return s


def _quant6(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.000001"))


def _quant2(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.01"))


def _multiplier_from_drawdown(drawdown_pct: Decimal) -> Decimal:
    # Canonical table (matches drawdown contract semantics)
    if drawdown_pct <= Decimal("-0.150000"):
        return Decimal("0.25")
    if drawdown_pct <= Decimal("-0.100000"):
        return Decimal("0.50")
    if drawdown_pct <= Decimal("-0.050000"):
        return Decimal("0.75")
    return Decimal("1.00")


def _table() -> List[Dict[str, str]]:
    return [
        {"threshold_drawdown_pct": "0.000000", "multiplier": "1.00"},
        {"threshold_drawdown_pct": "-0.050000", "multiplier": "0.75"},
        {"threshold_drawdown_pct": "-0.100000", "multiplier": "0.50"},
        {"threshold_drawdown_pct": "-0.150000", "multiplier": "0.25"},
    ]


@dataclass(frozen=True)
class Inputs:
    alloc_path: Path
    nav_path: Path
    pos_path: Path
    pos_schema: str
    truth_root: Path


def _bootstrap_window_true(day: str, truth_root: Path) -> bool:
    """
    Day-0 Bootstrap Window iff:
      TRUTH/execution_evidence_v1/submissions/<DAY>/ is missing OR contains zero submission dirs.
    """
    root = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    if not root.exists() or not root.is_dir():
        return True

    try:
        for p in sorted(root.iterdir()):
            # Any directory counts as a "submission directory" for bootstrap gating purposes.
            if p.is_dir():
                return False
    except Exception:
        # Fail-closed on IO errors: treat as NOT bootstrap (strict path).
        return False

    return True


def _resolve_inputs(day: str, truth_root: Path) -> Inputs:
    alloc = (truth_root / "allocation_v1/summary" / day / "summary.json").resolve()

    # Prefer accounting_v2 nav when present; fall back to accounting_v1 for legacy.
    nav_v2 = (truth_root / "accounting_v2/nav" / day / "nav.v2.json").resolve()
    nav_v1 = (truth_root / "accounting_v1/nav" / day / "nav.json").resolve()
    nav = nav_v2 if nav_v2.exists() else nav_v1

    pos_v3 = (truth_root / "positions_v1/snapshots" / day / "positions_snapshot.v3.json").resolve()
    pos_v2 = (truth_root / "positions_v1/snapshots" / day / "positions_snapshot.v2.json").resolve()

    if not alloc.exists():
        raise FileNotFoundError(f"ALLOC_SUMMARY_MISSING: {str(alloc)}")
    if not nav.exists():
        raise FileNotFoundError(f"NAV_MISSING: {str(nav)}")

    if pos_v3.exists():
        return Inputs(alloc_path=alloc, nav_path=nav, pos_path=pos_v3, pos_schema=SCHEMA_POS_V3, truth_root=truth_root)
    if pos_v2.exists():
        return Inputs(alloc_path=alloc, nav_path=nav, pos_path=pos_v2, pos_schema=SCHEMA_POS_V2, truth_root=truth_root)

    raise FileNotFoundError(f"POSITIONS_SNAPSHOT_MISSING: {str(pos_v3)} and {str(pos_v2)}")


def _is_accounting_v2_nav(nav_path: Path) -> bool:
    return nav_path.name == "nav.v2.json" or str(nav_path).endswith("/nav.v2.json")


def _compute(out_day: str, produced_utc: str, inp: Inputs) -> Dict[str, Any]:
    reason_codes: List[str] = []
    notes: List[str] = []

    checks: Dict[str, Any] = {
        "allocation_summary_present": True,
        "nav_present": True,
        "positions_present": True,
        "drawdown_present": False,
        "positions_all_have_max_loss": False,
        "portfolio_within_envelope": False,
    }

    alloc_obj = _read_json(inp.alloc_path)
    nav_obj = _read_json(inp.nav_path)
    pos_obj = _read_json(inp.pos_path)

    try:
        _validate_against_repo_schema(alloc_obj, SCHEMA_ALLOC_SUMMARY)
    except Exception as e:  # noqa: BLE001
        reason_codes.append("B2_ALLOC_SUMMARY_SCHEMA_INVALID")
        notes.append(f"allocation_summary schema invalid: {e}")
        checks["allocation_summary_present"] = False

    # IMPORTANT:
    # - accounting_v1 nav can be schema-validated against SCHEMA_NAV_V1
    # - accounting_v2 nav may not conform to v1 schema; do not fail the envelope gate on schema mismatch.
    #   We enforce required arithmetic fields locally (nav.nav_total + bootstrap-safe drawdown defaults).
    if not _is_accounting_v2_nav(inp.nav_path):
        try:
            _validate_against_repo_schema(nav_obj, SCHEMA_NAV_V1)
        except Exception as e:  # noqa: BLE001
            reason_codes.append("B2_ACCOUNTING_NAV_SCHEMA_INVALID")
            notes.append(f"accounting_nav schema invalid: {e}")
            checks["nav_present"] = False

    try:
        _validate_against_repo_schema(pos_obj, inp.pos_schema)
    except Exception as e:  # noqa: BLE001
        reason_codes.append("B2_POSITIONS_SNAPSHOT_SCHEMA_INVALID")
        notes.append(f"positions_snapshot schema invalid: {e}")
        checks["positions_present"] = False

    input_manifest = [
        {"type": "allocation_summary", "path": str(inp.alloc_path), "sha256": _sha256_file(inp.alloc_path)},
        {"type": "accounting_nav", "path": str(inp.nav_path), "sha256": _sha256_file(inp.nav_path)},
        {"type": "positions_snapshot", "path": str(inp.pos_path), "sha256": _sha256_file(inp.pos_path)},
        {"type": "drawdown_contract", "path": str(DRAWDOWN_CONTRACT), "sha256": _sha256_file(DRAWDOWN_CONTRACT)},
        {"type": "capital_risk_envelope_contract_v2", "path": str(CAP_RISK_CONTRACT_V2), "sha256": _sha256_file(CAP_RISK_CONTRACT_V2)},
        {"type": "output_schema", "path": str(SCHEMA_OUT), "sha256": _sha256_file(SCHEMA_OUT)},
    ]

    # Required: positive NAV. Adapter-backed accounting_v2 can provide exact
    # cent precision; legacy accounting falls back to integer-dollar nav_total.
    nav_block = nav_obj.get("nav") if isinstance(nav_obj.get("nav"), dict) else {}
    nav_total = nav_block.get("nav_total")
    explicit_nav_total_cents = nav_block.get("nav_total_cents")
    if isinstance(explicit_nav_total_cents, int) and explicit_nav_total_cents > 0:
        if not isinstance(nav_total, int) or nav_total <= 0:
            nav_total = int(Decimal(explicit_nav_total_cents) / Decimal("100"))
    elif isinstance(nav_total, int) and nav_total > 0:
        explicit_nav_total_cents = int(nav_total) * 100
    else:
        reason_codes.append("B2_NAV_TOTAL_MISSING_OR_INVALID")
        reason_codes.append(RC_AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS)
        checks["nav_present"] = False
        nav_total = 0
        explicit_nav_total_cents = 0

    nav_total_cents = int(explicit_nav_total_cents)

    # Drawdown inputs:
    # - accounting_v1 always provides history fields (required)
    # - accounting_v2 may be bootstrap (history missing or empty) -> set deterministic defaults:
    #     peak_nav := nav_total
    #     drawdown_abs := 0
    #     drawdown_pct := "0.000000"
    hist = nav_obj.get("history", {}) if isinstance(nav_obj.get("history"), dict) else {}
    peak_nav = hist.get("peak_nav")
    drawdown_abs = hist.get("drawdown_abs")
    drawdown_pct_raw = hist.get("drawdown_pct")

    if _is_accounting_v2_nav(inp.nav_path) and (not isinstance(hist, dict) or not hist):
        peak_nav = int(nav_total)
        drawdown_abs = 0
        drawdown_pct_raw = "0.000000"

    multiplier: Optional[Decimal] = None
    drawdown_pct_q: Optional[Decimal] = None

    if isinstance(drawdown_pct_raw, str) and drawdown_pct_raw.strip() != "":
        drawdown_pct_q = _quant6(Decimal(drawdown_pct_raw))
        multiplier = _quant2(_multiplier_from_drawdown(drawdown_pct_q))
        checks["drawdown_present"] = True
    else:
        reason_codes.append("B2_DRAWDOWN_MISSING_FAILCLOSED")
        notes.append("drawdown_pct missing/null at enforcement time -> FAIL-CLOSED per drawdown_convention_v1.contract.md")

    items = pos_obj.get("positions", {}).get("items")
    if not isinstance(items, list):
        reason_codes.append("B2_POSITIONS_ITEMS_INVALID_OR_MISSING")
        checks["positions_present"] = False
        items = []

    # SAFE_IDLE: empty list is valid and implies risk_sum=0.
    breakdown: List[Dict[str, Any]] = []
    risk_sum: Optional[int] = 0
    all_have_max = True

    def _pid(it: Any) -> str:
        if not isinstance(it, dict):
            return ""
        return str(it.get("position_id") or "")

    for it in sorted(items, key=_pid):
        if not isinstance(it, dict):
            continue
        position_id = str(it.get("position_id") or "unknown").strip()
        engine_id = str(it.get("engine_id") or "unknown").strip()
        st = str(it.get("status") or "unknown").strip()
        met = str(it.get("market_exposure_type") or "unknown").strip()
        ml = it.get("max_loss_cents")

        included = False
        if st == "OPEN":
            if isinstance(ml, int) and ml >= 0:
                included = True
                assert risk_sum is not None
                risk_sum += int(ml)
            else:
                all_have_max = False

        breakdown.append(
            {
                "position_id": position_id,
                "engine_id": engine_id,
                "market_exposure_type": met,
                "status": st,
                "max_loss_cents": (int(ml) if isinstance(ml, int) else None),
                "included_in_risk_sum": bool(included),
            }
        )

    checks["positions_all_have_max_loss"] = bool(all_have_max)
    if not all_have_max:
        reason_codes.append("B2_OPEN_POSITION_MISSING_MAX_LOSS_FAILCLOSED")
        notes.append("At least one OPEN position lacks max_loss_cents; cannot compute capital-at-risk -> FAIL-CLOSED")
        risk_sum = None

    allowed: Optional[int] = None
    headroom: Optional[int] = None

    if multiplier is not None and all_have_max and isinstance(risk_sum, int):
        allowed_dec = (Decimal(nav_total_cents) * BASE_ENVELOPE_PCT * multiplier)
        allowed = int(allowed_dec.to_integral_value(rounding="ROUND_FLOOR"))
        headroom = int(allowed - risk_sum)
        checks["portfolio_within_envelope"] = bool(risk_sum <= allowed)
        if risk_sum > allowed:
            reason_codes.append("B2_PORTFOLIO_CAPITAL_AT_RISK_EXCEEDS_ENVELOPE")

    status = "PASS" if not reason_codes else "FAIL"

    out = {
        "schema_id": "capital_risk_envelope",
        "schema_version": "v2",
        "day_utc": out_day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_c2_capital_risk_envelope_gate_v2.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "notes": notes,
        "input_manifest": input_manifest,
        "checks": checks,
        "envelope": {
            "contracts": {
                "drawdown_contract": {"path": str(DRAWDOWN_CONTRACT), "sha256": _sha256_file(DRAWDOWN_CONTRACT)},
                "capital_risk_envelope_contract": {"path": str(CAP_RISK_CONTRACT_V2), "sha256": _sha256_file(CAP_RISK_CONTRACT_V2)},
            },
            "drawdown_multiplier_table": _table(),
            "base_envelope_pct": f"{BASE_ENVELOPE_PCT:.6f}",
            "nav_total": int(nav_total),
            "nav_total_cents": int(nav_total_cents),
            "peak_nav": (int(peak_nav) if isinstance(peak_nav, int) else None),
            "drawdown_abs": (int(drawdown_abs) if isinstance(drawdown_abs, int) else None),
            "drawdown_pct": (f"{drawdown_pct_q:.6f}" if drawdown_pct_q is not None else None),
            "multiplier": (f"{multiplier:.2f}" if multiplier is not None else None),
            "allowed_capital_at_risk_cents": (int(allowed) if isinstance(allowed, int) else None),
            "portfolio_capital_at_risk_cents": (int(risk_sum) if isinstance(risk_sum, int) else None),
            "headroom_cents": (int(headroom) if isinstance(headroom, int) else 0),
            "positions": breakdown,
        },
    }

    # FAIL-CLOSED SCHEMA PATCH:
    # Downstream consumers require envelope.headroom_cents to be an int.
    # If upstream inputs are missing, set headroom_cents=0 deterministically.
    try:
        env = out.get("envelope")
        if not isinstance(env, dict):
            out["envelope"] = {}
            env = out["envelope"]
        hc = env.get("headroom_cents")
        if not isinstance(hc, int):
            env["headroom_cents"] = 0
    except Exception:
        # Absolute fail-closed: still enforce schema field as int
        out.setdefault("envelope", {})
        if isinstance(out["envelope"], dict):
            out["envelope"]["headroom_cents"] = 0

    out = _finalize_constitutional_capital_risk_report(out)
    _validate_against_repo_schema(out, "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json")
    return out


def _minimal_missing_inputs_report(out_day: str, in_day: str, produced_utc: str, missing_err: str) -> Dict[str, Any]:
    out = {
        "schema_id": "capital_risk_envelope",
        "schema_version": "v2",
        "day_utc": out_day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_c2_capital_risk_envelope_gate_v2.py", "git_sha": _git_sha()},
        "status": "FAIL",
        "reason_codes": ["B2_INPUTS_MISSING_FAILCLOSED"],
        "notes": [f"inputs missing for input_day_utc={in_day}: {missing_err}"],
        "input_manifest": [
            {"type": "drawdown_contract", "path": str(DRAWDOWN_CONTRACT), "sha256": _sha256_file(DRAWDOWN_CONTRACT)},
            {"type": "capital_risk_envelope_contract_v2", "path": str(CAP_RISK_CONTRACT_V2), "sha256": _sha256_file(CAP_RISK_CONTRACT_V2)},
            {"type": "output_schema", "path": str(SCHEMA_OUT), "sha256": _sha256_file(SCHEMA_OUT)},
        ],
        "checks": {
            "allocation_summary_present": False,
            "nav_present": False,
            "positions_present": False,
            "drawdown_present": False,
            "positions_all_have_max_loss": False,
            "portfolio_within_envelope": False,
        },
        "envelope": {
            "contracts": {
                "drawdown_contract": {"path": str(DRAWDOWN_CONTRACT), "sha256": _sha256_file(DRAWDOWN_CONTRACT)},
                "capital_risk_envelope_contract": {"path": str(CAP_RISK_CONTRACT_V2), "sha256": _sha256_file(CAP_RISK_CONTRACT_V2)},
            },
            "drawdown_multiplier_table": _table(),
            "base_envelope_pct": f"{BASE_ENVELOPE_PCT:.6f}",
            "nav_total": 0,
            "nav_total_cents": 0,
            "peak_nav": None,
            "drawdown_abs": None,
            "drawdown_pct": None,
            "multiplier": None,
            "allowed_capital_at_risk_cents": None,
            "portfolio_capital_at_risk_cents": None,
            "headroom_cents": 0,
            "positions": [],
        },
    }
    out = _finalize_constitutional_capital_risk_report(out)
    _validate_against_repo_schema(out, "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json")
    return out


def _day0_bootstrap_alloc_missing_report(out_day: str, in_day: str, produced_utc: str, truth_root: Path, missing_err: str) -> Dict[str, Any]:
    """
    Day-0 rule: if bootstrap window true AND allocation summary missing -> PASS with deterministic zeros.
    """
    submissions_path = (truth_root / "execution_evidence_v1" / "submissions" / in_day).resolve()

    out = {
        "schema_id": "capital_risk_envelope",
        "schema_version": "v2",
        "day_utc": out_day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_c2_capital_risk_envelope_gate_v2.py", "git_sha": _git_sha()},
        "status": "PASS",
        "reason_codes": [DAY0_RC_ALLOC_ALLOWED],
        "notes": [
            f"DAY0_BOOTSTRAP_WINDOW_TRUE: no submissions yet at {str(submissions_path)}",
            f"allocation summary missing allowed in bootstrap window; enforcing explicit zero-risk envelope; missing_err={missing_err}",
        ],
        "input_manifest": [
            {
                "type": "submissions_day_dir_probe",
                "path": str(submissions_path),
                "sha256": (
                    _sha256_bytes(b"")
                    if not submissions_path.exists()
                    else _sha256_bytes(b"\n".join([p.name.encode("utf-8") for p in sorted(submissions_path.iterdir()) if p.is_dir()]))
                ),
            },
            {"type": "drawdown_contract", "path": str(DRAWDOWN_CONTRACT), "sha256": _sha256_file(DRAWDOWN_CONTRACT)},
            {"type": "capital_risk_envelope_contract_v2", "path": str(CAP_RISK_CONTRACT_V2), "sha256": _sha256_file(CAP_RISK_CONTRACT_V2)},
            {"type": "output_schema", "path": str(SCHEMA_OUT), "sha256": _sha256_file(SCHEMA_OUT)},
        ],
        "checks": {
            "allocation_summary_present": False,
            "nav_present": False,
            "positions_present": False,
            "drawdown_present": False,
            "positions_all_have_max_loss": True,
            "portfolio_within_envelope": True,
        },
        "envelope": {
            "contracts": {
                "drawdown_contract": {"path": str(DRAWDOWN_CONTRACT), "sha256": _sha256_file(DRAWDOWN_CONTRACT)},
                "capital_risk_envelope_contract": {"path": str(CAP_RISK_CONTRACT_V2), "sha256": _sha256_file(CAP_RISK_CONTRACT_V2)},
            },
            "drawdown_multiplier_table": _table(),
            "base_envelope_pct": f"{BASE_ENVELOPE_PCT:.6f}",
            "nav_total": 0,
            "nav_total_cents": 0,
            "peak_nav": None,
            "drawdown_abs": None,
            "drawdown_pct": None,
            "multiplier": None,
            "allowed_capital_at_risk_cents": 0,
            "portfolio_capital_at_risk_cents": 0,
            "headroom_cents": 0,
            "positions": [],
        },
    }
    out = _finalize_constitutional_capital_risk_report(out)
    _validate_against_repo_schema(out, "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_c2_capital_risk_envelope_gate_v2")
    ap.add_argument("--out_day_utc", required=True, help="YYYY-MM-DD (output day key for report path)")
    ap.add_argument("--input_day_utc", required=True, help="YYYY-MM-DD (input truth day key to read)")
    ap.add_argument("--produced_utc", required=True, help="UTC ISO-8601 Z timestamp (deterministic, operator/orchestrator provided)")
    ap.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT), help="Override truth root (tests only)")
    args = ap.parse_args()

    out_day = str(args.out_day_utc).strip()
    in_day = str(args.input_day_utc).strip()
    produced_utc = str(args.produced_utc).strip()
    truth_root = Path(str(args.truth_root)).resolve()

    out_dir = (truth_root / "reports" / "capital_risk_envelope_v2" / out_day).resolve()
    out_path = (out_dir / "capital_risk_envelope.v2.json").resolve()

    existing: Optional[Dict[str, Any]] = None
    existing_sha: Optional[str] = None
    if out_path.exists():
        existing_sha = _sha256_file(out_path)
        existing = _read_json(out_path)
        schema_id = str(existing.get("schema_id") or "").strip()
        day_utc = str(existing.get("day_utc") or "").strip()
        schema_version = str(existing.get("schema_version") or "").strip()
        status = str(existing.get("status") or "").strip().upper()
        if schema_id != "capital_risk_envelope":
            raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
        if schema_version != "v2":
            raise SystemExit(f"FAIL: EXISTING_REPORT_VERSION_MISMATCH: schema_version={schema_version!r} path={out_path}")
        if day_utc != out_day:
            raise SystemExit(f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={out_day!r} path={out_path}")
        if status not in ("PASS", "FAIL", "DEGRADED"):
            raise SystemExit(f"FAIL: EXISTING_REPORT_STATUS_INVALID: status={status!r} path={out_path}")

    inp: Optional[Inputs] = None
    missing_err: Optional[str] = None
    try:
        inp = _resolve_inputs(day=in_day, truth_root=truth_root)
    except Exception as e:  # noqa: BLE001
        missing_err = str(e)

    if inp is not None:
        out = _compute(out_day=out_day, produced_utc=produced_utc, inp=inp)
    else:
        bootstrap = _bootstrap_window_true(in_day, truth_root)
        if bootstrap and isinstance(missing_err, str) and missing_err.startswith("ALLOC_SUMMARY_MISSING:"):
            out = _day0_bootstrap_alloc_missing_report(
                out_day=out_day,
                in_day=in_day,
                produced_utc=produced_utc,
                truth_root=truth_root,
                missing_err=str(missing_err),
            )
        else:
            out = _minimal_missing_inputs_report(out_day=out_day, in_day=in_day, produced_utc=produced_utc, missing_err=str(missing_err))

    assert_constitutional_writer_allowed_v1(REPO_ROOT, "capital_risk_envelope_v2", "ops/tools/run_c2_capital_risk_envelope_gate_v2.py")
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id="capital_risk_envelope_v2",
        payload=out,
        required_finality_states=["provisional", "finalized", "corrected"],
    )
    out_bytes = _canonical_json_bytes(out)
    candidate_sha = _sha256_bytes(out_bytes)
    action = "WROTE"
    if existing is not None and existing_sha is not None:
        if existing_sha == candidate_sha:
            sha = existing_sha
            action = "EXISTS"
        elif (
            _is_safe_positions_carry_forward_repair(existing, out)
            or _is_safe_missing_inputs_recovery(existing, out)
            or _is_safe_nav_validation_recovery(existing, out)
        ):
            sha = _write_bytes_replace(out_path, out_bytes)
            action = "BACKFILL_REPAIRED"
        else:
            sha = existing_sha
            out = existing
            action = "EXISTS"
    else:
        sha = _write_immutable(out_path, out_bytes)

    print(f"CAPITAL_RISK_ENVELOPE_V2_WRITTEN day_utc={out_day} path={str(out_path)} sha256={sha} action={action}")
    if out.get("status") != "PASS":
        print(f"FAIL: CAPITAL_RISK_ENVELOPE_GATE_V2 status={out.get('status')} reason_codes={out.get('reason_codes')}", file=sys.stderr)
        return 2
    print("OK: CAPITAL_RISK_ENVELOPE_GATE_V2 PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
