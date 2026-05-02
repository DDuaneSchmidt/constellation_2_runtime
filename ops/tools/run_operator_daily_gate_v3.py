#!/usr/bin/env python3
"""
run_operator_daily_gate_v3.py

Operator Daily Gate v3:
- Same checks as v2
- Adds Exit Reconciliation enforcement (Bundle A2)
- Writes to a new immutable-safe output root:
    truth/reports/operator_daily_gate_v3/<DAY>/operator_daily_gate.v3.json

Run:
  python3 ops/tools/run_operator_daily_gate_v3.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import os
import hashlib
import json
import subprocess
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root, resolve_release_provenance
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_governed_dependency_ref_v1,
    build_machine_blocker_envelope_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1

REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()

def _truth_root_from_args_or_env(truth_root_arg: str | None) -> Path:
    if truth_root_arg is not None and str(truth_root_arg).strip():
        p = Path(str(truth_root_arg).strip()).expanduser().resolve()
        if not p.is_absolute():
            raise SystemExit(f"FAIL: --truth_root must be absolute: {p}")
        if not p.exists() or (not p.is_dir()):
            raise SystemExit(f"FAIL: --truth_root must exist and be a directory: {p}")
        return p

    env = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env:
        p = Path(env).expanduser().resolve()
        if not p.is_absolute():
            raise SystemExit(f"FAIL: C2_TRUTH_ROOT must be absolute: {p}")
        if not p.exists() or (not p.is_dir()):
            raise SystemExit(f"FAIL: C2_TRUTH_ROOT must exist and be a directory: {p}")
        return p

    return (REPO_ROOT / "constellation_2/runtime/truth").resolve()

TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()  # placeholder; overwritten in main()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_daily_gate.v3.schema.json"
POLICY_RELPATH = "governance/02_REGISTRIES/C2_BUNDLE_C_DRAWDOWN_POLICY_V1.json"
OUT_ROOT = (TRUTH / "reports" / "operator_daily_gate_v3").resolve()
ECONOMIC_BUILD_FAMILY = "economic_state_build_v1"

RECON_ROOT_V3 = (TRUTH / "reports" / "reconciliation_report_v3").resolve()
POS_SNAP_ROOT = (TRUTH / "positions_v1/snapshots").resolve()
CAP_ENV_ROOT_V2 = (TRUTH / "reports" / "capital_risk_envelope_v2").resolve()

CASH_SNAP_ROOT = (TRUTH / "cash_ledger_v1/snapshots").resolve()
CASH_FAIL_ROOT = (TRUTH / "cash_ledger_v1/failures").resolve()

EXIT_RECON_ROOT = (TRUTH / "exit_reconciliation_v1").resolve()
INTENTS_ROOT = (TRUTH / "intents_v1/snapshots").resolve()

RC_EXIT_RECON_MISSING = "MISSING_EXIT_RECONCILIATION_V1"
RC_EXIT_RECON_PARSE_FAIL = "EXIT_RECONCILIATION_PARSE_ERROR_FAILCLOSED"
RC_EXIT_INTENTS_UNSATISFIED = "EXIT_INTENTS_UNSATISFIED_FAILCLOSED"


def _git_sha() -> str:
    try:
        s = str(resolve_release_provenance().get("git_sha") or "").strip()
        if s:
            return s
    except Exception:
        pass
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        # Clean runtime roots can be source-derived without .git metadata.
        return "0" * 40


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _day_prefix(day_utc: str) -> str:
    return f"{day_utc}T"


def _prior_day_utc(day_utc: str) -> str:
    return (date.fromisoformat(day_utc) - timedelta(days=1)).isoformat()


def _decimal_or_none(raw: Any) -> Decimal | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _canonical_economic_truth_root() -> Path:
    return resolve_canonical_truth_root().resolve()


def _load_bundle_c_drawdown_policy() -> Dict[str, Any]:
    policy_path = (REPO_ROOT / POLICY_RELPATH).resolve()
    policy = _read_json(policy_path)
    profiles = policy.get("profiles")
    if not isinstance(profiles, dict):
        raise SystemExit(f"FAIL: BUNDLE_C_DRAWDOWN_POLICY_INVALID:path={policy_path}")
    return policy


def _select_bundle_c_drawdown_profile(*, mode: str, operation_type: str) -> Dict[str, Any]:
    policy = _load_bundle_c_drawdown_policy()
    mode_text = str(mode or "").strip().upper()
    op_text = str(operation_type or "").strip()
    profiles = policy["profiles"]
    for profile_id in ("PAPER_BOOTSTRAP", "PRODUCTION"):
        profile = profiles.get(profile_id)
        if not isinstance(profile, dict):
            continue
        modes = {str(item).strip().upper() for item in profile.get("applies_to_modes") or []}
        ops = {str(item).strip() for item in profile.get("operation_types") or []}
        if mode_text in modes and (not op_text or "*" in ops or op_text in ops):
            return {**profile, "policy_path": str((REPO_ROOT / POLICY_RELPATH).resolve())}
    production = profiles.get("PRODUCTION")
    if not isinstance(production, dict):
        raise SystemExit("FAIL: BUNDLE_C_DRAWDOWN_POLICY_PRODUCTION_PROFILE_MISSING")
    return {**production, "policy_path": str((REPO_ROOT / POLICY_RELPATH).resolve())}


def _load_previous_day_economic_build_state(*, truth_root: Path, day_utc: str, mode: str) -> Dict[str, Any]:
    prev_day_utc = _prior_day_utc(day_utc)
    # economic_state_build_v1 is governed as canonical truth, while this gate is
    # emitted under the execution truth root. Do not silently look for canonical
    # economic state inside the sleeve/execution root.
    economic_truth_root = _canonical_economic_truth_root()
    build_root = (economic_truth_root / "reports" / ECONOMIC_BUILD_FAMILY / prev_day_utc).resolve()
    unknown = {
        "status": "UNKNOWN",
        "source_day_utc": prev_day_utc,
        "expected_artifact_root": str(build_root),
        "artifact_path": "",
        "artifact_sha256": "",
        "drawdown_pct": None,
        "drawdown_guard_status": "UNKNOWN",
        "policy_baseline_comparison_vs_portfolio_return": None,
        "external_benchmark_underperformer_count": 0,
        "drawdown_policy": _select_bundle_c_drawdown_profile(mode=mode, operation_type=""),
        "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BUILD_MISSING"],
    }
    if not build_root.exists() or not build_root.is_dir():
        return dict(unknown)

    candidates = sorted(build_root.glob("*/economic_state_build.v1.json"))
    if not candidates:
        return dict(unknown)
    if len(candidates) != 1:
        raise SystemExit(
            "FAIL: BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BUILD_AMBIGUOUS:"
            f"day_utc={prev_day_utc}:count={len(candidates)}"
        )

    build_path = candidates[0].resolve()
    build_sha256 = _sha256_file(build_path)
    build_obj = _read_json(build_path)
    if str(build_obj.get("schema_id") or "").strip() != "economic_state_build":
        return {
            **unknown,
            "artifact_path": str(build_path),
            "artifact_sha256": build_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BUILD_SCHEMA_INVALID"],
        }
    if str(build_obj.get("day_utc") or "").strip() != prev_day_utc:
        return {
            **unknown,
            "artifact_path": str(build_path),
            "artifact_sha256": build_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BUILD_DAY_MISMATCH"],
        }
    if str(build_obj.get("closure_status") or "").strip().upper() != "COMPLETE":
        return {
            **unknown,
            "artifact_path": str(build_path),
            "artifact_sha256": build_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BUILD_NOT_COMPLETE"],
        }

    evaluation = build_obj.get("economic_evaluation")
    if not isinstance(evaluation, dict):
        return {
            **unknown,
            "artifact_path": str(build_path),
            "artifact_sha256": build_sha256,
            "reason_codes": ["BUNDLE_C_PREVIOUS_DAY_ECONOMIC_EVALUATION_MISSING"],
        }

    risk_state = evaluation.get("risk_state") if isinstance(evaluation.get("risk_state"), dict) else {}
    operation_type = str(build_obj.get("operation_type") or "").strip()
    drawdown_policy = _select_bundle_c_drawdown_profile(mode=mode, operation_type=operation_type)
    block_limit = _decimal_or_none(drawdown_policy.get("drawdown_block_limit"))
    if block_limit is None:
        raise SystemExit(
            "FAIL: BUNDLE_C_DRAWDOWN_POLICY_BLOCK_LIMIT_INVALID:"
            f"profile={drawdown_policy.get('profile_id')}:path={drawdown_policy.get('policy_path')}"
        )
    benchmark_state = evaluation.get("benchmark_state") if isinstance(evaluation.get("benchmark_state"), dict) else {}
    policy_baseline = benchmark_state.get("policy_baseline") if isinstance(benchmark_state.get("policy_baseline"), dict) else {}
    drawdown_pct = risk_state.get("drawdown_pct")
    drawdown_decimal = _decimal_or_none(drawdown_pct)
    drawdown_guard_status = "UNKNOWN"
    if drawdown_decimal is not None:
        drawdown_guard_status = "BLOCKED" if drawdown_decimal <= block_limit else "PASS"

    external_underperformer_count = 0
    for row in benchmark_state.get("external_benchmarks") or []:
        if not isinstance(row, dict):
            continue
        comparison = _decimal_or_none(row.get("comparison_vs_portfolio_return"))
        if comparison is not None and comparison < 0:
            external_underperformer_count += 1

    return {
        "status": "OK",
        "source_day_utc": prev_day_utc,
        "expected_artifact_root": str(build_root),
        "artifact_path": str(build_path),
        "artifact_sha256": build_sha256,
        "drawdown_pct": drawdown_pct,
        "drawdown_guard_status": drawdown_guard_status,
        "drawdown_policy": drawdown_policy,
        "policy_baseline_comparison_vs_portfolio_return": policy_baseline.get("comparison_vs_portfolio_return"),
        "external_benchmark_underperformer_count": external_underperformer_count,
        "reason_codes": [],
    }


def _cash_snapshot_day_integrity(day_utc: str, cash_obj: Dict[str, Any]) -> Tuple[bool, List[str]]:
    rc: List[str] = []
    pu = str(cash_obj.get("produced_utc") or "").strip()
    snap = cash_obj.get("snapshot") if isinstance(cash_obj.get("snapshot"), dict) else {}
    ou = str(snap.get("observed_at_utc") or "").strip()
    if not pu.startswith(_day_prefix(day_utc)):
        rc.append("CASH_LEDGER_PRODUCED_UTC_DAY_MISMATCH")
    if not ou.startswith(_day_prefix(day_utc)):
        rc.append("CASH_LEDGER_OBSERVED_AT_UTC_DAY_MISMATCH")
    return (len(rc) == 0, rc)


def _scan_exit_intents(day: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    d = (INTENTS_ROOT / day).resolve()
    if not d.exists() or not d.is_dir():
        return out
    for p in d.iterdir():
        if not p.is_file() or not p.name.endswith(".json"):
            continue
        try:
            o = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(o, dict):
            continue
        if o.get("schema_id") != "exposure_intent":
            continue
        if o.get("schema_version") != "v1":
            continue
        if str(o.get("target_notional_pct") or "").strip() != "0":
            continue
        eng = o.get("engine")
        if not isinstance(eng, dict):
            continue
        engine_id = str(eng.get("engine_id") or "").strip()
        if not engine_id:
            continue
        out[engine_id] = out.get(engine_id, 0) + 1
    return out


def main() -> int:
    contract = assert_constitutional_writer_allowed_v1(
        REPO_ROOT,
        "operator_daily_gate_v3",
        "ops/tools/run_operator_daily_gate_v3.py",
    )
    ap = argparse.ArgumentParser(prog="run_operator_daily_gate_v3")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--truth_root", default=None, help="Absolute truth root directory (optional). If omitted, uses env C2_TRUTH_ROOT, else global truth.")
    ap.add_argument("--produced_utc", required=True, help="Must equal <DAY>T00:00:00Z")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    expected = f"{day}T00:00:00Z"
    if str(args.produced_utc).strip() != expected:
        raise SystemExit(f"FAIL: produced_utc_must_equal_day_marker expected={expected!r} got={str(args.produced_utc).strip()!r}")
    produced_utc = expected

    global TRUTH, OUT_ROOT, RECON_ROOT_V3, POS_SNAP_ROOT, ALLOC_SUM_ROOT, CAP_ENV_ROOT_V2, CASH_SNAP_ROOT, CASH_FAIL_ROOT, EXIT_RECON_ROOT, INTENTS_ROOT
    TRUTH = _truth_root_from_args_or_env(args.truth_root)
    OUT_ROOT = (TRUTH / "reports" / "operator_daily_gate_v3").resolve()

    RECON_ROOT_V3 = (TRUTH / "reports" / "reconciliation_report_v3").resolve()
    POS_SNAP_ROOT = (TRUTH / "positions_v1/snapshots").resolve()
    ALLOC_SUM_ROOT = (TRUTH / "allocation_v1/summary").resolve()
    CAP_ENV_ROOT_V2 = (TRUTH / "reports" / "capital_risk_envelope_v2").resolve()
    CASH_SNAP_ROOT = (TRUTH / "cash_ledger_v1/snapshots").resolve()
    CASH_FAIL_ROOT = (TRUTH / "cash_ledger_v1/failures").resolve()
    EXIT_RECON_ROOT = (TRUTH / "exit_reconciliation_v1").resolve()
    INTENTS_ROOT = (TRUTH / "intents_v1/snapshots").resolve()

    input_manifest: List[Dict[str, str]] = []
    reason_codes: List[str] = []
    notes: List[str] = []

    # Reconciliation v3 required and must be OK
    recon_path = (RECON_ROOT_V3 / day / "reconciliation_report.v3.json").resolve()
    recon_status = "MISSING"
    if recon_path.exists():
        input_manifest.append({"type": "reconciliation_report_v3", "path": str(recon_path), "sha256": _sha256_file(recon_path)})
        recon = _read_json(recon_path)
        recon_status = str(recon.get("status") or "MISSING").strip().upper() or "MISSING"
        if recon_status != "OK":
            reason_codes.append("RECONCILIATION_V3_NOT_OK")
    else:
        reason_codes.append("MISSING_RECONCILIATION_REPORT_V3")
        input_manifest.append({"type": "reconciliation_report_v3_missing", "path": str(recon_path), "sha256": _sha256_bytes(b"")})

    # Canonical Bundle A positions snapshot required: v5 only.
    pos_path = (POS_SNAP_ROOT / day / "positions_snapshot.v5.json").resolve()
    pos_present = pos_path.exists() and pos_path.is_file()
    if pos_present:
        input_manifest.append({"type": "positions_snapshot_v5", "path": str(pos_path), "sha256": _sha256_file(pos_path)})
    else:
        reason_codes.append("MISSING_POSITIONS_SNAPSHOT_V5")
        input_manifest.append({"type": "positions_snapshot_v5_missing", "path": str(pos_path), "sha256": _sha256_bytes(b"")})

    # Capital envelope v2 required and must PASS
    cap_path = (CAP_ENV_ROOT_V2 / day / "capital_risk_envelope.v2.json").resolve()
    cap_status = "MISSING"
    if cap_path.exists():
        input_manifest.append({"type": "capital_risk_envelope_v2", "path": str(cap_path), "sha256": _sha256_file(cap_path)})
        ce = _read_json(cap_path)
        cap_status = str(ce.get("status") or "MISSING").strip().upper() or "MISSING"
        if cap_status != "PASS":
            reason_codes.append("CAPITAL_RISK_ENVELOPE_V2_NOT_PASS")
    else:
        reason_codes.append("MISSING_CAPITAL_RISK_ENVELOPE_V2")
        input_manifest.append({"type": "capital_risk_envelope_v2_missing", "path": str(cap_path), "sha256": _sha256_bytes(b"")})

    # Cash ledger failure artifact (fail-closed)
    cash_fail_path = (CASH_FAIL_ROOT / day / "failure.json").resolve()
    cash_fail_present = cash_fail_path.exists()
    if cash_fail_present:
        reason_codes.append("CASH_LEDGER_FAILURE_PRESENT_FAILCLOSED")
        input_manifest.append({"type": "cash_ledger_failure_v1", "path": str(cash_fail_path), "sha256": _sha256_file(cash_fail_path)})
    else:
        input_manifest.append({"type": "cash_ledger_failure_missing", "path": str(cash_fail_path), "sha256": _sha256_bytes(b"")})

    # Cash ledger snapshot required + integrity
    cash_path = (CASH_SNAP_ROOT / day / "cash_ledger_snapshot.v1.json").resolve()
    cash_present = cash_path.exists()
    cash_integrity_ok = False
    if cash_present:
        input_manifest.append({"type": "cash_ledger_snapshot_v1", "path": str(cash_path), "sha256": _sha256_file(cash_path)})
        try:
            cash_obj = _read_json(cash_path)
            ok, rc = _cash_snapshot_day_integrity(day, cash_obj)
            cash_integrity_ok = bool(ok)
            if not ok:
                reason_codes += rc
                reason_codes.append("CASH_LEDGER_SNAPSHOT_DAY_INTEGRITY_FAILCLOSED")
        except Exception:
            reason_codes.append("CASH_LEDGER_SNAPSHOT_PARSE_ERROR_FAILCLOSED")
    else:
        reason_codes.append("MISSING_CASH_LEDGER_SNAPSHOT")
        input_manifest.append({"type": "cash_ledger_snapshot_missing", "path": str(cash_path), "sha256": _sha256_bytes(b"")})

    # ---- Exit reconciliation enforcement ----
    exit_recon_path = (EXIT_RECON_ROOT / day / "exit_reconciliation.v1.json").resolve()
    exit_recon_present = False
    exit_intents_satisfied = True
    obligations_engine_ids: List[str] = []

    if exit_recon_path.exists() and exit_recon_path.is_file():
        exit_recon_present = True
        input_manifest.append({"type": "exit_reconciliation_v1", "path": str(exit_recon_path), "sha256": _sha256_file(exit_recon_path)})
        try:
            er = _read_json(exit_recon_path)
            obligations = er.get("obligations")
            if not isinstance(obligations, list):
                raise ValueError("EXIT_RECON_OBLIGATIONS_NOT_LIST")
            for ob in obligations:
                if isinstance(ob, dict):
                    eid = str(ob.get("engine_id") or "").strip()
                    if eid:
                        obligations_engine_ids.append(eid)
        except Exception:
            reason_codes.append(RC_EXIT_RECON_PARSE_FAIL)
            exit_intents_satisfied = False
    else:
        reason_codes.append(RC_EXIT_RECON_MISSING)
        input_manifest.append({"type": "exit_reconciliation_v1_missing", "path": str(exit_recon_path), "sha256": _sha256_bytes(b"")})
        exit_intents_satisfied = False

    obligations_engine_ids = sorted(set(obligations_engine_ids))
    if obligations_engine_ids:
        exit_map = _scan_exit_intents(day)
        missing_eids = [eid for eid in obligations_engine_ids if exit_map.get(eid, 0) <= 0]
        if missing_eids:
            reason_codes.append(RC_EXIT_INTENTS_UNSATISFIED)
            notes.append(f"missing_exit_intents_for_engines={','.join(missing_eids)}")
            exit_intents_satisfied = False

    economic_state = _load_previous_day_economic_build_state(
        truth_root=TRUTH,
        day_utc=day,
        mode=str(args.mode).strip().upper(),
    )
    if str(economic_state.get("artifact_path") or "").strip():
        input_manifest.append(
            {
                "type": "economic_state_build_v1",
                "path": str(economic_state["artifact_path"]),
                "sha256": str(economic_state.get("artifact_sha256") or ""),
            }
        )
    if str(economic_state.get("status") or "").strip().upper() == "OK":
        if str(economic_state.get("drawdown_guard_status") or "").strip().upper() == "BLOCKED":
            reason_codes.append("BUNDLE_C_DRAWDOWN_LIMIT_EXCEEDED")
            notes.append(
                "bundle_c_drawdown_block:"
                f"source_day_utc={economic_state['source_day_utc']}:"
                f"drawdown_pct={economic_state.get('drawdown_pct')}:"
                f"limit_pct={str((economic_state.get('drawdown_policy') or {}).get('drawdown_block_limit') or '')}:"
                f"profile={str((economic_state.get('drawdown_policy') or {}).get('profile_id') or '')}"
            )
        policy_comparison = _decimal_or_none(
            economic_state.get("policy_baseline_comparison_vs_portfolio_return")
        )
        if policy_comparison is not None:
            if policy_comparison < 0:
                notes.append("bundle_c_policy_baseline=UNDERPERFORMING")
            elif policy_comparison > 0:
                notes.append("bundle_c_policy_baseline=OUTPERFORMING")
            else:
                notes.append("bundle_c_policy_baseline=INLINE")
        external_underperformer_count = int(economic_state.get("external_benchmark_underperformer_count") or 0)
        if external_underperformer_count > 0:
            notes.append(
                f"bundle_c_external_benchmark_underperformer_count={external_underperformer_count}"
            )
    else:
        reason_codes.append("MISSING_PREVIOUS_DAY_ECONOMIC_STATE_FAILCLOSED")
        notes.append("bundle_c_previous_day_economic_state=UNKNOWN_FAILCLOSED")

    status = "PASS"
    if reason_codes:
        status = "FAIL"
    reason_codes = sorted(set(reason_codes))
    constitutional_dependency_refs: List[Dict[str, Any]] = []
    missing_dependency_artifacts: List[str] = []
    governed_dependency_reason_codes: List[str] = []
    if recon_path.exists() and recon_path.is_file():
        recon_sha = _sha256_file(recon_path)
        constitutional_dependency_refs.append(
            build_governed_dependency_ref_v1(
                repo_root=REPO_ROOT,
                artifact_id="reconciliation_report_v3",
                path=recon_path,
                sha256=recon_sha,
                finality_state=FINALITY_PROVISIONAL,
            )
        )
        try:
            validate_governed_artifact_payload_v1(
                repo_root=REPO_ROOT,
                artifact_id="reconciliation_report_v3",
                payload=_read_json(recon_path),
            )
        except Exception:
            governed_dependency_reason_codes.append("INVALID_GOVERNED_DEPENDENCY:reconciliation_report_v3")
    else:
        missing_dependency_artifacts.append("reconciliation_report_v3")
    if pos_present:
        constitutional_dependency_refs.append(
            build_governed_dependency_ref_v1(
                repo_root=REPO_ROOT,
                artifact_id="positions_snapshot_v5",
                path=pos_path,
                sha256=_sha256_file(pos_path),
                finality_state=FINALITY_PROVISIONAL,
            )
        )
    else:
        missing_dependency_artifacts.append("positions_snapshot_v5")
    if cap_path.exists() and cap_path.is_file():
        cap_sha = _sha256_file(cap_path)
        constitutional_dependency_refs.append(
            build_governed_dependency_ref_v1(
                repo_root=REPO_ROOT,
                artifact_id="capital_risk_envelope_v2",
                path=cap_path,
                sha256=cap_sha,
                finality_state=FINALITY_PROVISIONAL,
            )
        )
        try:
            validate_governed_artifact_payload_v1(
                repo_root=REPO_ROOT,
                artifact_id="capital_risk_envelope_v2",
                payload=_read_json(cap_path),
            )
        except Exception:
            governed_dependency_reason_codes.append("INVALID_GOVERNED_DEPENDENCY:capital_risk_envelope_v2")
    else:
        missing_dependency_artifacts.append("capital_risk_envelope_v2")
    if cash_present:
        constitutional_dependency_refs.append(
            build_governed_dependency_ref_v1(
                repo_root=REPO_ROOT,
                artifact_id="cash_ledger_snapshot_v1",
                path=cash_path,
                sha256=_sha256_file(cash_path),
                finality_state=FINALITY_PROVISIONAL,
            )
        )
    else:
        missing_dependency_artifacts.append("cash_ledger_snapshot_v1")
    if exit_recon_present:
        exit_recon_sha = _sha256_file(exit_recon_path)
        constitutional_dependency_refs.append(
            build_governed_dependency_ref_v1(
                repo_root=REPO_ROOT,
                artifact_id="exit_reconciliation_v1",
                path=exit_recon_path,
                sha256=exit_recon_sha,
                finality_state=FINALITY_PROVISIONAL,
            )
        )
        try:
            validate_governed_artifact_payload_v1(
                repo_root=REPO_ROOT,
                artifact_id="exit_reconciliation_v1",
                payload=_read_json(exit_recon_path),
            )
        except Exception:
            governed_dependency_reason_codes.append("INVALID_GOVERNED_DEPENDENCY:exit_reconciliation_v1")
    else:
        missing_dependency_artifacts.append("exit_reconciliation_v1")
    if str(economic_state.get("artifact_path") or "").strip() and str(economic_state.get("artifact_sha256") or "").strip():
        constitutional_dependency_refs.append(
            build_governed_dependency_ref_v1(
                repo_root=REPO_ROOT,
                artifact_id="economic_state_build_v1",
                path=str(economic_state.get("artifact_path") or "").strip(),
                sha256=str(economic_state.get("artifact_sha256") or "").strip(),
                finality_state=FINALITY_PROVISIONAL,
            )
        )
    else:
        missing_dependency_artifacts.append("economic_state_build_v1")
    if governed_dependency_reason_codes:
        status = "FAIL"
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state=(
            "COMPLETE"
            if status == "PASS" and not governed_dependency_reason_codes and not missing_dependency_artifacts
            else "BLOCKED"
        ),
        reason_codes=[*reason_codes, *governed_dependency_reason_codes],
        missing_dependency_artifacts=missing_dependency_artifacts,
    )
    constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type="operator_daily_gate_v3",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="operator_daily_gate_v3",
        declared_dependency_artifacts=[
            str(item).strip()
            for item in (contract.get("required_upstream_dependencies") or [])
            if str(item).strip()
        ],
        dependency_refs=constitutional_dependency_refs,
    )
    constitutional_lineage = build_governed_artifact_lineage_v1(
        artifact_type="operator_daily_gate_v3",
        artifact_version="v3",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id="operator_daily_gate_v3",
        producer_id="ops/tools/run_operator_daily_gate_v3.py",
        generated_at_utc=produced_utc,
        effective_at_utc=produced_utc,
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=constitutional_dependency_refs,
        policy_snapshot_refs=[],
        code_version=_git_sha(),
        run_id=f"operator_daily_gate:{day}:{str(args.mode).strip().upper()}",
    )

    payload_reason_codes = sorted(set([*reason_codes, *governed_dependency_reason_codes]))
    gate: Dict[str, Any] = {
        "schema_id": "operator_daily_gate",
        "schema_version": "v3",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_operator_daily_gate_v3.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": payload_reason_codes,
        "blocking_codes": list(blocker_envelope["blocking_codes"]),
        "closure_state": str(blocker_envelope["closure_state"]),
        "first_blocker_code": str(blocker_envelope["first_blocker_code"]),
        "missing_dependency_artifacts": list(blocker_envelope["missing_dependency_artifacts"]),
        "constitutional_dependency_declaration": constitutional_dependency_declaration,
        "constitutional_lineage": constitutional_lineage,
        "notes": notes,
        "input_manifest": input_manifest,
        "checks": {
            "reconciliation_v3_status": (recon_status if recon_status in ("OK", "FAIL", "MISSING") else "MISSING"),
            "cash_ledger_integrity_ok": bool(cash_present and cash_integrity_ok and (not cash_fail_present)),
            "positions_snapshot_present": bool(pos_present),
            "capital_risk_envelope_v2_status": (cap_status if cap_status in ("PASS", "FAIL", "MISSING") else "MISSING"),
            "exit_reconciliation_present": bool(exit_recon_present),
            "exit_intents_satisfied_when_obligations_exist": bool(exit_intents_satisfied),
            "previous_day_economic_state_status": str(economic_state.get("status") or "UNKNOWN"),
            "previous_day_drawdown_guard_status": str(economic_state.get("drawdown_guard_status") or "UNKNOWN"),
        },
        "economic_state": {
            "status": str(economic_state.get("status") or "UNKNOWN"),
            "source_day_utc": str(economic_state.get("source_day_utc") or ""),
            "artifact_path": str(economic_state.get("artifact_path") or ""),
            "artifact_sha256": str(economic_state.get("artifact_sha256") or ""),
            "drawdown_pct": economic_state.get("drawdown_pct"),
            "drawdown_guard_status": str(economic_state.get("drawdown_guard_status") or "UNKNOWN"),
            "drawdown_policy": dict(economic_state.get("drawdown_policy") or {}),
            "policy_baseline_comparison_vs_portfolio_return": economic_state.get(
                "policy_baseline_comparison_vs_portfolio_return"
            ),
        "external_benchmark_underperformer_count": int(
                economic_state.get("external_benchmark_underperformer_count") or 0
            ),
            "reason_codes": list(economic_state.get("reason_codes") or []),
            "expected_artifact_root": str(economic_state.get("expected_artifact_root") or ""),
            "producer_command": (
                "PYTHONPATH=\"$PWD\" python3 ops/tools/run_economic_state_authority_v1.py "
                f"--operation_type fresh_paper_entry_v1 --day_utc {economic_state.get('source_day_utc') or _prior_day_utc(day)} "
                "--sleeve_id PRIMARY --environment PAPER --ib_account DUO847203 --materialize YES --emit_package YES"
            ),
            "recovery_command": (
                "PYTHONPATH=\"$PWD\" python3 ops/tools/run_gate_authority_plane_v1.py "
                f"--day_utc {day} --truth_root {TRUTH} --produced_utc {produced_utc} --mode {str(args.mode).strip().upper()}"
            ),
        },
    }

    validate_against_repo_schema_v1(gate, REPO_ROOT, SCHEMA_RELPATH)
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id="operator_daily_gate_v3",
        payload=gate,
        required_finality_states=["provisional", "finalized", "corrected"],
    )

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "operator_daily_gate.v3.json").resolve()
    payload = (json.dumps(gate, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    wr = write_day_artifact_refreshable_v1(
        path=out_path,
        data=payload,
        expected_day_utc=day,
        expected_schema_id="operator_daily_gate",
        expected_schema_version="v3",
        preserve_statuses=("PASS", "OK"),
    )
    if wr.action == "REFRESHED":
        print(
            f"WARN: OPERATOR_DAILY_GATE_V3_REFRESHED_STALE day_utc={day} path={wr.path} "
            f"prior_sha256={wr.prior_sha256} quarantined_path={wr.quarantined_path}"
        )
    print(f"OK: OPERATOR_DAILY_GATE_V3_WRITTEN day_utc={day} status={status} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
