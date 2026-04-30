#!/usr/bin/env python3
"""
Constellation 2.0 — Phase L — Live Ops Dashboard

- Serves static UI + JSON API
- Reads canonical runtime truth artifacts
- Never talks to IB, never submits orders
- Configuration API writes are draft-governed and activation-audited
- Fail-closed: missing artifacts are surfaced with explicit error codes + file pointers
- Minimal deps: Python stdlib only
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

_BOOTSTRAP_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_BOOTSTRAP_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_REPO_ROOT))

from constellation_2.common.operator_control_plane_v1 import (
    build_operator_home_bundle,
    build_operator_query_bundle,
)
from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.phaseL.ui_api import (
    STATUS_SEMANTICS,
    build_action_inventory,
    build_advisory_view,
    build_alerts_view,
    build_capital_accounts_view,
    build_capital_allocation_view,
    build_capital_cashflow_view,
    build_capital_flows_view,
    build_capital_history_view,
    build_capital_overview_view,
    build_capital_query_surface_v1,
    build_capital_validation_view,
    build_command_overview_view,
    build_configuration_catalog_v1,
    build_configuration_current_v1,
    build_financial_state_view,
    build_kernel_status_rail_view,
    build_kernel_status_rail_summary_view,
    build_integrity_view,
    build_operations_view,
    build_opportunity_state_view,
    build_operator_work_queue_view,
    build_orders_view,
    build_operator_workflow_summary,
    build_outcome_state_view,
    build_policy_evolution_view,
    build_positions_view,
    build_reconciliation_view,
    build_readiness_kernel_v1,
    build_refinement_state_view,
    build_sleeve_evaluation_view,
    build_system_summary_view,
    build_tax_state_view,
    build_value_state_view,
    build_workspace_view,
    create_configuration_draft_v1,
    create_reliability_fix_attempt_v1,
    create_reliability_issue_v1,
    create_reliability_issue_verification_v1,
    create_reliability_issue_work_order_v1,
    create_reliability_observation_v1,
    create_reliability_verification_v1,
    create_reliability_work_order_fix_attempt_v1,
    create_reliability_work_order_from_issue_v1,
    create_reliability_work_order_v1,
    dispatch_kernel_command,
    draft_reliability_issue_v1,
    get_reliability_fix_attempt_v1,
    get_latest_reliability_readiness_v1,
    get_operator_state,
    get_configuration_draft_v1,
    get_reliability_issue_v1,
    get_reliability_readiness_v1,
    get_reliability_verification_v1,
    get_reliability_work_order_v1,
    list_action_audit_entries,
    list_reliability_fix_attempts_v1,
    list_reliability_issue_verifications_v1,
    list_reliability_issue_work_orders_v1,
    list_reliability_next_actions_v1,
    link_reliability_issue_observation_v1,
    list_reliability_issues_v1,
    list_reliability_observations_v1,
    list_reliability_verifications_v1,
    list_reliability_work_order_fix_attempts_v1,
    list_reliability_work_orders_v1,
    record_reliability_fix_attempt_v1,
    reject_configuration_draft_v1,
    resolve_effective_capital_cashflow_inputs_v1,
    review_configuration_draft_v1,
    run_action,
    assess_reliability_readiness_v1,
    update_reliability_fix_attempt_v1,
    update_reliability_issue_v1,
    update_reliability_verification_v1,
    update_reliability_work_order_v1,
    validate_configuration_draft_v1,
    activate_configuration_draft_v1,
    verify_reliability_issue_v1,
)
from constellation_2.phaseL.ui_api.configuration_workflow_v1 import ConfigurationWorkflowApiError
from constellation_2.phaseL.ui_api.common import ADVISORY_RUNTIME_ROOT, GLOBAL_TRUTH_ROOT, SLEEVE_TRUTH_ROOT
# --------------------------
# Error codes (audit-safe)
# --------------------------

E_TRUTH_ROOT_MISSING = "TRUTH_ROOT_MISSING"
E_SUBMISSIONS_ROOT_MISSING = "SUBMISSIONS_ROOT_MISSING"
E_NO_DAYS_FOUND = "NO_DAYS_FOUND"
E_DAY_INVALID = "DAY_INVALID"
E_NO_SUBMISSIONS_FOUND = "NO_SUBMISSIONS_FOUND"
E_NO_ORDER_PLAN_PRESENT = "NO_ORDER_PLAN_PRESENT"
E_NAV_MISSING = "NAV_MISSING"
E_ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE = "ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE"

# Activity endpoints
E_ACTIVITY_DAY_NOT_RESOLVED = "ACTIVITY_DAY_NOT_RESOLVED"
E_ACTIVITY_ARTIFACT_MISSING = "ACTIVITY_ARTIFACT_MISSING"
E_ACTIVITY_ARTIFACT_UNREADABLE = "ACTIVITY_ARTIFACT_UNREADABLE"

# Submission index (day-level) — preferred for speed when present (legacy path used by this UI)
SUBMISSION_INDEX_SCHEMA_ID = "C2_SUBMISSION_INDEX_V1"
SUBMISSION_INDEX_SCHEMA_VERSION = 1
SUBMISSION_INDEX_FILENAME = "submission_index.v1.json"

# Pillars decision record (preferred submission evidence surface)
PILLARS_DECISION_SCHEMA_ID = "submission_decision_record"
PILLARS_DECISION_SCHEMA_VERSION = "v1"
PILLARS_DECISION_SUFFIX = ".submission_decision_record.v1.json"

# --------------------------
# Repo / truth roots (deterministic)
# --------------------------

THIS_FILE = Path(__file__).resolve()
# .../constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py
# parents: [server, ui, phaseL, constellation_2, <repo_root>, ...]
REPO_ROOT = THIS_FILE.parents[4]
TRUTH_ROOT = SLEEVE_TRUTH_ROOT
RUNTIME_ROOT = Path(os.environ.get("C2_RUNTIME_STATE_ROOT", "/home/node/constellation_runtime_data/runtime")).resolve()
PERFORMANCE_SHOWCASE_FAMILY = "aegis_performance_showcase_v1"
PERFORMANCE_SHOWCASE_HTML = "aegis_performance_showcase.v1.html"


def _known_truth_roots() -> List[Path]:
    roots: List[Path] = []
    for r in [TRUTH_ROOT, SLEEVE_TRUTH_ROOT]:
        if isinstance(r, Path) and r.exists() and r.is_dir():
            roots.append(r.resolve())
    uniq: List[Path] = []
    seen = set()
    for r in roots:
        s = str(r)
        if s in seen:
            continue
        seen.add(s)
        uniq.append(r)
    return uniq


def _has_orchestrator_day(truth_root: Path, day: str) -> bool:
    p = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day).resolve()
    return p.exists() and p.is_dir()


def _truth_root_for_day(day: Optional[str]) -> Path:
    if not isinstance(day, str) or not _is_day_str(day):
        return TRUTH_ROOT

    roots = _known_truth_roots()
    if not roots:
        return TRUTH_ROOT

    for r in roots:
        if _has_orchestrator_day(r, day):
            return r
    return roots[0]


# Canonical surfaces (as proven on disk)
SUBMISSIONS_ROOT = (TRUTH_ROOT / "execution_evidence_v1" / "submissions").resolve()
INTENTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()
GATE_VERDICT_ROOT = (TRUTH_ROOT / "reports" / "gate_stack_verdict_v1").resolve()

ACCOUNTING_NAV_ROOT = (TRUTH_ROOT / "accounting_v2" / "nav").resolve()
ACCOUNTING_ATTR_ROOT = (TRUTH_ROOT / "accounting_v2" / "attribution").resolve()
ENGINE_LINKAGE_ROOT = (TRUTH_ROOT / "engine_linkage_v1").resolve()

# Pillars roots (preferred submission evidence)
PILLARS_V1_ROOT = (TRUTH_ROOT / "pillars_v1").resolve()
PILLARS_V1R1_ROOT = (TRUTH_ROOT / "pillars_v1r1").resolve()

# Activity monitoring roots (authoritative)
INTENTS_SUMMARY_ROOT = (TRUTH_ROOT / "monitoring_v1" / "intents_summary_v1").resolve()
SUBMISSIONS_SUMMARY_ROOT = (TRUTH_ROOT / "monitoring_v1" / "submissions_summary_v1").resolve()
ACTIVITY_ROLLUP_ROOT = (TRUTH_ROOT / "monitoring_v1" / "activity_ledger_rollup_v1").resolve()

# Instance config (service provides path via env; fail-closed if missing)
def _instance_config_path() -> Path:
    # C2_INSTANCE_CONFIG is set by systemd unit; if absent, use a non-existent sentinel to surface MISSING deterministically.
    import os
    raw = os.environ.get("C2_INSTANCE_CONFIG") or ""
    p = Path(raw) if raw else (REPO_ROOT / "__MISSING_INSTANCE_CONFIG__").resolve()
    return p.resolve()

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _service_version() -> Optional[str]:
    import os

    env_version = (os.environ.get("C2_SERVICE_VERSION") or "").strip()
    if env_version:
        return env_version

    package_path = (REPO_ROOT / "package.json").resolve()
    payload, _ = _safe_read_json(package_path)
    if isinstance(payload, dict):
        raw_version = payload.get("version")
        if isinstance(raw_version, str) and raw_version.strip():
            return raw_version.strip()
    return None


def _safe_read_json(path: Path) -> Tuple[Optional[Any], Optional[str]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except FileNotFoundError:
        return None, "FILE_NOT_FOUND"
    except json.JSONDecodeError:
        return None, "JSON_DECODE_ERROR"
    except Exception:
        return None, "READ_ERROR"


def _mtime(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except Exception:
        return None


def _is_day_str(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except Exception:
        return False


def _list_day_dirs(root: Path) -> List[str]:
    if not root.exists() or not root.is_dir():
        return []
    days: List[str] = []
    for p in root.iterdir():
        if p.is_dir() and _is_day_str(p.name):
            days.append(p.name)
    days.sort()
    return days


def _union_days() -> List[str]:
    """
    Authoritative day discovery for UI.

    Includes:
    - gate_stack_verdict_v1 days (authoritative day spine)
    - intents snapshot days
    - accounting_v2 nav/attribution days
    - submissions days (if submissions root exists)
    - pillars days
    - activity monitoring days (if present)
    """
    days = set()

    roots = _known_truth_roots()
    if not roots:
        roots = [TRUTH_ROOT]
    for troot in roots:
        submissions_root = (troot / "execution_evidence_v1" / "submissions").resolve()
        intents_root = (troot / "intents_v1" / "snapshots").resolve()
        gate_root = (troot / "reports" / "gate_stack_verdict_v1").resolve()
        nav_root = (troot / "accounting_v2" / "nav").resolve()
        attr_root = (troot / "accounting_v2" / "attribution").resolve()
        pillars_v1 = (troot / "pillars_v1").resolve()
        pillars_v1r1 = (troot / "pillars_v1r1").resolve()
        intents_summary_root = (troot / "monitoring_v1" / "intents_summary_v1").resolve()
        submissions_summary_root = (troot / "monitoring_v1" / "submissions_summary_v1").resolve()
        activity_rollup_root = (troot / "monitoring_v1" / "activity_ledger_rollup_v1").resolve()

        for root in [gate_root, intents_root, nav_root, attr_root]:
            for d in _list_day_dirs(root):
                days.add(d)

        if submissions_root.exists() and submissions_root.is_dir():
            for d in _list_day_dirs(submissions_root):
                days.add(d)

        for root in [pillars_v1r1, pillars_v1]:
            for d in _list_day_dirs(root):
                days.add(d)

        for root in [intents_summary_root, submissions_summary_root, activity_rollup_root]:
            for d in _list_day_dirs(root):
                days.add(d)

    # UI safety: exclude future days (e.g. 2199-01-19 bootstrap placeholders).
    # Selectable days must not exceed today's UTC date.
    today_utc = date.today().isoformat()
    days2 = [d for d in days if isinstance(d, str) and d <= today_utc]

    return sorted(days2)



def _select_latest_day(days: List[str]) -> Optional[str]:
    return days[-1] if days else None


def _pillars_decisions_dir(day: str) -> Optional[Path]:
    d1 = (PILLARS_V1R1_ROOT / day / "decisions").resolve()
    if d1.exists() and d1.is_dir():
        return d1
    d0 = (PILLARS_V1_ROOT / day / "decisions").resolve()
    if d0.exists() and d0.is_dir():
        return d0
    return None


def _try_load_submission_index(day: str) -> Tuple[Optional[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    idx_path = (SUBMISSIONS_ROOT / day / SUBMISSION_INDEX_FILENAME).resolve()
    if not idx_path.exists():
        missing.append(str(idx_path))
        return None, missing, source_paths, source_mtimes, warnings

    obj, err = _safe_read_json(idx_path)
    source_paths.append(str(idx_path))
    mt = _mtime(idx_path)
    if mt is not None:
        source_mtimes[str(idx_path)] = mt

    if obj is None or not isinstance(obj, dict):
        warnings.append(f"SUBMISSION_INDEX_UNREADABLE:{err}")
        return None, missing, source_paths, source_mtimes, warnings

    if obj.get("schema_id") != SUBMISSION_INDEX_SCHEMA_ID or obj.get("schema_version") != SUBMISSION_INDEX_SCHEMA_VERSION:
        warnings.append("SUBMISSION_INDEX_SCHEMA_MISMATCH")
        return None, missing, source_paths, source_mtimes, warnings

    if obj.get("day_utc") != day:
        warnings.append("SUBMISSION_INDEX_DAY_MISMATCH")
        return None, missing, source_paths, source_mtimes, warnings

    if not isinstance(obj.get("items"), list):
        warnings.append("SUBMISSION_INDEX_ITEMS_MISSING_OR_INVALID")
        return None, missing, source_paths, source_mtimes, warnings

    return obj, missing, source_paths, source_mtimes, warnings


def _try_load_pillars_decisions(day: str) -> Tuple[List[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    ddir = _pillars_decisions_dir(day)
    if ddir is None:
        return [], missing, source_paths, source_mtimes, warnings

    files = sorted([p for p in ddir.iterdir() if p.is_file() and p.name.endswith(PILLARS_DECISION_SUFFIX)], key=lambda p: p.name)
    if not files:
        missing.append(str(ddir))
        warnings.append("PILLARS_DECISIONS_EMPTY")
        return [], missing, source_paths, source_mtimes, warnings

    out: List[Dict[str, Any]] = []
    for fp in files:
        obj, err = _safe_read_json(fp)
        source_paths.append(str(fp))
        mt = _mtime(fp)
        if mt is not None:
            source_mtimes[str(fp)] = mt

        if obj is None or not isinstance(obj, dict):
            warnings.append(f"PILLARS_DECISION_UNREADABLE:{fp}:{err}")
            continue

        if str(obj.get("schema_id") or "") != PILLARS_DECISION_SCHEMA_ID or str(obj.get("schema_version") or "") != PILLARS_DECISION_SCHEMA_VERSION:
            warnings.append(f"PILLARS_DECISION_SCHEMA_MISMATCH:{fp}")
            continue

        decision_id = str(obj.get("decision_id") or "").strip()
        if decision_id == "":
            warnings.append(f"PILLARS_DECISION_MISSING_DECISION_ID:{fp}")
            continue

        input_manifest = obj.get("input_manifest")
        if not isinstance(input_manifest, list):
            warnings.append(f"PILLARS_DECISION_INPUT_MANIFEST_INVALID:{fp}")
            input_manifest = []

        broker_path: Optional[str] = None
        exec_path: Optional[str] = None
        plan_path: Optional[str] = None

        for it in input_manifest:
            if not isinstance(it, dict):
                continue
            t = str(it.get("type") or "")
            p = str(it.get("path") or "")
            if t == "broker_submission_record_v2" and p:
                broker_path = p
            elif t == "execution_event_record_v1" and p:
                exec_path = p
            elif t == "order_plan_v1" and p:
                plan_path = p

        rec: Dict[str, Any] = {
            "submission_dir": None,
            "submission_id": decision_id,
            "decision": obj.get("decision"),
            "decision_status": obj.get("status"),
            "decision_reason_codes": obj.get("reason_codes"),
            "broker_submission_record": None,
            "execution_event_record": None,
            "order_plan": None,
            "missing_paths": [],
        }

        if isinstance(broker_path, str) and broker_path:
            try:
                rec["submission_dir"] = str(Path(broker_path).resolve().parent)
            except Exception:
                rec["submission_dir"] = None

        if isinstance(broker_path, str) and broker_path:
            bobj, berr = _safe_read_json(Path(broker_path))
            if bobj is None:
                rec["missing_paths"].append(broker_path)
                warnings.append(f"PILLARS_BROKER_RECORD_UNREADABLE:{berr}")
            else:
                rec["broker_submission_record"] = bobj
                source_paths.append(broker_path)
                mtb = _mtime(Path(broker_path))
                if mtb is not None:
                    source_mtimes[broker_path] = mtb

        if isinstance(exec_path, str) and exec_path:
            eobj, _eerr = _safe_read_json(Path(exec_path))
            if eobj is None:
                rec["missing_paths"].append(exec_path)
            else:
                rec["execution_event_record"] = eobj
                source_paths.append(exec_path)
                mte = _mtime(Path(exec_path))
                if mte is not None:
                    source_mtimes[exec_path] = mte

        if isinstance(plan_path, str) and plan_path:
            pobj, _perr = _safe_read_json(Path(plan_path))
            if pobj is None:
                rec["missing_paths"].append(plan_path)
            else:
                rec["order_plan"] = pobj
                source_paths.append(plan_path)
                mtp = _mtime(Path(plan_path))
                if mtp is not None:
                    source_mtimes[plan_path] = mtp

        out.append(rec)

    return out, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def _scan_submissions_for_day(day: str) -> Tuple[List[Dict[str, Any]], List[str], List[str], Dict[str, float]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}

    day_root = SUBMISSIONS_ROOT / day
    if not day_root.exists():
        missing.append(str(day_root))
        return [], missing, source_paths, source_mtimes

    idx, miss_i, sp_i, sm_i, _w_i = _try_load_submission_index(day)
    if idx is not None:
        missing.extend(miss_i)
        source_paths.extend(sp_i)
        source_mtimes.update(sm_i)

        out: List[Dict[str, Any]] = []
        for it in idx.get("items", []):
            if not isinstance(it, dict):
                continue

            paths = it.get("paths") if isinstance(it.get("paths"), dict) else {}
            subdir = paths.get("submission_dir") if isinstance(paths, dict) else None

            rec: Dict[str, Any] = {
                "submission_dir": subdir,
                "submission_id": it.get("submission_id"),
                "broker_submission_record": {
                    "schema_id": "broker_submission_record",
                    "schema_version": "v2",
                    "submission_id": it.get("submission_id"),
                    "binding_hash": it.get("binding_hash"),
                    "broker": it.get("broker"),
                    "broker_ids": it.get("broker_ids"),
                    "status": it.get("broker_status"),
                    "submitted_at_utc": it.get("submitted_at_utc"),
                },
                "execution_event_record": None,
                "order_plan": None,
                "missing_paths": [],
            }

            ex = it.get("execution") if isinstance(it.get("execution"), dict) else None
            if isinstance(ex, dict) and ex.get("status") is not None:
                rec["execution_event_record"] = {
                    "schema_id": "execution_event_record",
                    "schema_version": "v1",
                    "status": ex.get("status"),
                    "filled_qty": ex.get("filled_qty"),
                    "avg_price": ex.get("avg_price"),
                    "event_time_utc": ex.get("event_time_utc"),
                    "perm_id": ex.get("perm_id"),
                    "broker_order_id": ex.get("broker_order_id"),
                }

            op_path = paths.get("order_plan") if isinstance(paths, dict) else None
            if isinstance(op_path, str) and op_path:
                op_obj, _op_err = _safe_read_json(Path(op_path))
                if op_obj is None:
                    rec["missing_paths"].append(op_path)
                else:
                    rec["order_plan"] = op_obj
                    source_paths.append(op_path)
                    mt2 = _mtime(Path(op_path))
                    if mt2 is not None:
                        source_mtimes[op_path] = mt2

            out.append(rec)

        return out, sorted(set(missing)), sorted(set(source_paths)), source_mtimes

    pill_records, miss_p, sp_p, sm_p, _w_p = _try_load_pillars_decisions(day)
    if pill_records:
        missing.extend(miss_p)
        source_paths.extend(sp_p)
        source_mtimes.update(sm_p)
        return pill_records, sorted(set(missing)), sorted(set(source_paths)), source_mtimes

    missing.extend(miss_i)
    source_paths.extend(sp_i)
    source_mtimes.update(sm_i)
    return [], sorted(set(missing)), sorted(set(source_paths)), source_mtimes


def _load_engine_join_map_for_day(day: str) -> Tuple[Dict[str, str], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []
    subid_to_engine: Dict[str, str] = {}

    day_snap_dir = ENGINE_LINKAGE_ROOT / "snapshots" / day
    candidates: List[Path] = []
    if day_snap_dir.exists() and day_snap_dir.is_dir():
        candidates = sorted([p for p in day_snap_dir.iterdir() if p.is_file() and p.suffix == ".json"])

    to_try: List[Path] = []
    if candidates:
        to_try.extend(candidates)

    for p in to_try:
        obj, _ = _safe_read_json(p)
        if obj is None:
            continue
        source_paths.append(str(p))
        mt = _mtime(p)
        if mt is not None:
            source_mtimes[str(p)] = mt

        if isinstance(obj, dict):
            for key in ["subid_to_engine", "submission_id_to_engine", "engine_by_submission_id", "engine_by_subid"]:
                v = obj.get(key)
                if isinstance(v, dict) and v:
                    ok = True
                    tmp: Dict[str, str] = {}
                    for k2, v2 in v.items():
                        if not isinstance(k2, str) or not isinstance(v2, str):
                            ok = False
                            break
                        tmp[k2] = v2
                    if ok and tmp:
                        subid_to_engine.update(tmp)
                        return subid_to_engine, missing, source_paths, source_mtimes, warnings

    attr_path = ACCOUNTING_ATTR_ROOT / day / "engine_attribution.v2.json"
    obj, _ = _safe_read_json(attr_path)
    if obj is None:
        missing.append(str(attr_path))
    else:
        source_paths.append(str(attr_path))
        mt = _mtime(attr_path)
        if mt is not None:
            source_mtimes[str(attr_path)] = mt

    warnings.append(E_ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE)
    return {}, missing, source_paths, source_mtimes, warnings


def _count_intents_for_day(day: str) -> Tuple[int, List[str]]:
    d = (INTENTS_ROOT / day).resolve()
    if not d.exists() or not d.is_dir():
        return 0, [str(d)]
    files = sorted([p for p in d.iterdir() if p.is_file()])
    return len(files), []


def _nav_summary_for_day(day: str) -> Tuple[Optional[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    nav_path = ACCOUNTING_NAV_ROOT / day / "nav.v2.json"
    obj, _err = _safe_read_json(nav_path)
    if obj is None:
        missing.append(str(nav_path))
        warnings.append(E_NAV_MISSING)
        return None, missing, source_paths, source_mtimes, warnings

    source_paths.append(str(nav_path))
    mt = _mtime(nav_path)
    if mt is not None:
        source_mtimes[str(nav_path)] = mt

    nav_end = None
    if isinstance(obj, dict):
        nav = obj.get("nav")
        if isinstance(nav, dict) and "nav_total" in nav:
            nav_end = nav.get("nav_total")
        elif "nav_total" in obj:
            nav_end = obj.get("nav_total")

    return {"source": "accounting_v2/nav", "day_utc": day, "nav_end": nav_end}, missing, source_paths, source_mtimes, warnings


def _series_nav_points(last_n_days: int) -> Tuple[List[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    days = _union_days()
    if not days:
        warnings.append(E_NO_DAYS_FOUND)
        return [], missing, source_paths, source_mtimes, warnings

    sel = days[-last_n_days:] if last_n_days > 0 else days
    pts: List[Dict[str, Any]] = []
    for d in sel:
        nav, m, sp, sm, w = _nav_summary_for_day(d)
        missing.extend(m)
        source_paths.extend(sp)
        source_mtimes.update(sm)
        warnings.extend(w)
        if nav is None:
            continue
        pts.append({"day_utc": d, "nav_end": nav.get("nav_end"), "source": nav.get("source")})

    return pts, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def _read_activity_artifact(path: Path, expected_schema_id: str, expected_day_field: str, expected_day_value: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """
    Returns (doc_or_none, errors[])
    """
    if not path.exists():
        return None, [E_ACTIVITY_ARTIFACT_MISSING]
    obj, err = _safe_read_json(path)
    if obj is None or not isinstance(obj, dict):
        return None, [f"{E_ACTIVITY_ARTIFACT_UNREADABLE}:{err}"]
    if str(obj.get("schema_id") or "") != expected_schema_id:
        return None, [f"ACTIVITY_SCHEMA_MISMATCH:{expected_schema_id}"]
    if str(obj.get(expected_day_field) or "") != expected_day_value:
        return None, [f"ACTIVITY_DAY_MISMATCH:{expected_day_value}"]
    return obj, []


def _activity_latest_day() -> Optional[str]:
    return _select_latest_day(_union_days())


def _activity_today(day: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "errors": [],
        "warnings": [],
        "missing_paths": [],
        "source_paths": [],
        "intents_summary": None,
        "submissions_summary": None,
        "rollup_asof": None,
    }

    ip = (INTENTS_SUMMARY_ROOT / day / "intents_summary.v1.json").resolve()
    sp = (SUBMISSIONS_SUMMARY_ROOT / day / "submissions_summary.v1.json").resolve()
    rp = (ACTIVITY_ROLLUP_ROOT / day / "activity_ledger_rollup.v1.json").resolve()

    doc, errs = _read_activity_artifact(ip, "intents_summary", "day_utc", day)
    if doc is None:
        resp["warnings"].extend(errs)
        resp["missing_paths"].append(str(ip))
    else:
        resp["intents_summary"] = doc
        resp["source_paths"].append(str(ip))

    doc, errs = _read_activity_artifact(sp, "submissions_summary", "day_utc", day)
    if doc is None:
        resp["warnings"].extend(errs)
        resp["missing_paths"].append(str(sp))
    else:
        resp["submissions_summary"] = doc
        resp["source_paths"].append(str(sp))

    doc, errs = _read_activity_artifact(rp, "activity_ledger_rollup", "asof_day_utc", day)
    if doc is None:
        resp["warnings"].extend(errs)
        resp["missing_paths"].append(str(rp))
    else:
        resp["rollup_asof"] = doc
        resp["source_paths"].append(str(rp))

    resp["errors"] = sorted(set(resp["errors"]))
    resp["warnings"] = sorted(set(resp["warnings"]))
    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["source_paths"] = sorted(set(resp["source_paths"]))
    return resp


def _day_summary(day: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "errors": [],
        "warnings": [],
        "source_paths": [],
        "source_mtimes": {},
        "missing_paths": [],
        "data_freshness_max_mtime": None,
        "counts": {
            "intents": 0,
            "planned_actions": None,
            "submissions": 0,
            "fills": 0,
            "partials": 0,
            "rejects": 0,
            "errors": 0,
            "unknown_status": 0,
        },
        "by_engine": [],
        "nav": None,
    }

    if not _is_day_str(day):
        resp["ok"] = False
        resp["errors"].append(E_DAY_INVALID)
        return resp

    if not TRUTH_ROOT.exists():
        resp["ok"] = False
        resp["errors"].append(E_TRUTH_ROOT_MISSING)
        resp["missing_paths"].append(str(TRUTH_ROOT))
        return resp

    icnt, imiss = _count_intents_for_day(day)
    resp["counts"]["intents"] = icnt
    resp["missing_paths"].extend(imiss)

    if not SUBMISSIONS_ROOT.exists():
        resp["warnings"].append(E_SUBMISSIONS_ROOT_MISSING)
        resp["missing_paths"].append(str(SUBMISSIONS_ROOT))

    submissions, miss, sps, smt = _scan_submissions_for_day(day)
    resp["missing_paths"].extend(miss)
    resp["source_paths"].extend(sps)
    resp["source_mtimes"].update(smt)

    if not submissions:
        resp["warnings"].append(E_NO_SUBMISSIONS_FOUND)
    resp["counts"]["submissions"] = len(submissions)

    planned_actions = 0
    any_plan = False
    for rec in submissions:
        op = rec.get("order_plan")
        if op is None:
            continue
        any_plan = True
        if isinstance(op, dict):
            acts = op.get("actions")
            if isinstance(acts, list):
                planned_actions += len(acts)
            else:
                planned_actions += 1
        else:
            planned_actions += 1

    resp["counts"]["planned_actions"] = planned_actions if any_plan else 0
    if not any_plan:
        resp["warnings"].append(E_NO_ORDER_PLAN_PRESENT)

    subid_to_engine, miss2, sps2, smt2, warns2 = _load_engine_join_map_for_day(day)
    resp["missing_paths"].extend(miss2)
    resp["source_paths"].extend(sps2)
    resp["source_mtimes"].update(smt2)
    resp["warnings"].extend(warns2)

    by_engine: Dict[str, Dict[str, Any]] = {}

    def eng_for(submission_id: str) -> str:
        e = subid_to_engine.get(submission_id)
        return e if isinstance(e, str) and e else "unknown"

    for rec in submissions:
        subid = str(rec.get("submission_id") or rec.get("submission_dir") or "unknown")
        engine = eng_for(subid)

        if engine not in by_engine:
            by_engine[engine] = {
                "engine": engine,
                "submissions": 0,
                "fills": 0,
                "partials": 0,
                "rejects": 0,
                "errors": 0,
                "unknown_status": 0,
            }
        by_engine[engine]["submissions"] += 1

        bsr = rec.get("broker_submission_record") or {}
        status = bsr.get("status") if isinstance(bsr, dict) else None

        eer = rec.get("execution_event_record")
        if isinstance(eer, dict):
            ev_status = eer.get("status")
            if isinstance(ev_status, str):
                s = ev_status.upper()
                if "FILL" in s:
                    resp["counts"]["fills"] += 1
                    by_engine[engine]["fills"] += 1
                elif "PART" in s:
                    resp["counts"]["partials"] += 1
                    by_engine[engine]["partials"] += 1

        if isinstance(status, str):
            s2 = status.upper()
            if "REJECT" in s2:
                resp["counts"]["rejects"] += 1
                by_engine[engine]["rejects"] += 1
            elif "ERROR" in s2 or "FAIL" in s2:
                resp["counts"]["errors"] += 1
                by_engine[engine]["errors"] += 1
        else:
            resp["counts"]["unknown_status"] += 1
            by_engine[engine]["unknown_status"] += 1

    resp["by_engine"] = [by_engine[k] for k in sorted(by_engine.keys())]

    nav, miss3, sps3, smt3, warns3 = _nav_summary_for_day(day)
    resp["nav"] = nav
    resp["missing_paths"].extend(miss3)
    resp["source_paths"].extend(sps3)
    resp["source_mtimes"].update(smt3)
    resp["warnings"].extend(warns3)

    mt_values = [v for v in resp["source_mtimes"].values() if isinstance(v, (int, float))]
    resp["data_freshness_max_mtime"] = max(mt_values) if mt_values else None

    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["source_paths"] = sorted(set(resp["source_paths"]))
    resp["warnings"] = sorted(set(resp["warnings"]))
    resp["errors"] = sorted(set(resp["errors"]))
    return resp


def _day_plan(day: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "errors": [],
        "warnings": [],
        "source_paths": [],
        "source_mtimes": {},
        "missing_paths": [],
        "plans": [],
    }

    if not _is_day_str(day):
        resp["ok"] = False
        resp["errors"].append(E_DAY_INVALID)
        return resp

    submissions, miss, sps, smt = _scan_submissions_for_day(day)
    resp["missing_paths"].extend(miss)
    resp["source_paths"].extend(sps)
    resp["source_mtimes"].update(smt)

    plans: List[Dict[str, Any]] = []
    for rec in submissions:
        op = rec.get("order_plan")
        if op is None:
            continue
        plans.append({"submission_id": rec.get("submission_id"), "order_plan": op})

    if not plans:
        resp["warnings"].append(E_NO_ORDER_PLAN_PRESENT)
    resp["plans"] = plans

    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["source_paths"] = sorted(set(resp["source_paths"]))
    return resp


def _day_submissions(day: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "errors": [],
        "warnings": [],
        "source_paths": [],
        "source_mtimes": {},
        "missing_paths": [],
        "submissions": [],
        "engine_join": {"status": "unknown", "warning": E_ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE, "source_paths": []},
    }

    if not _is_day_str(day):
        resp["ok"] = False
        resp["errors"].append(E_DAY_INVALID)
        return resp

    submissions, miss, sps, smt = _scan_submissions_for_day(day)
    resp["missing_paths"].extend(miss)
    resp["source_paths"].extend(sps)
    resp["source_mtimes"].update(smt)

    if not submissions:
        resp["warnings"].append(E_NO_SUBMISSIONS_FOUND)

    subid_to_engine, miss2, sps2, smt2, warns2 = _load_engine_join_map_for_day(day)
    resp["missing_paths"].extend(miss2)
    resp["source_paths"].extend(sps2)
    resp["source_mtimes"].update(smt2)

    if subid_to_engine:
        resp["engine_join"] = {"status": "available", "warning": None, "source_paths": sps2}
    else:
        resp["warnings"].extend(warns2)

    out = []
    for rec in submissions:
        subid = str(rec.get("submission_id") or rec.get("submission_dir") or "unknown")
        engine = subid_to_engine.get(subid, "unknown")
        x = dict(rec)
        x["engine"] = engine
        out.append(x)

    resp["submissions"] = out

    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["source_paths"] = sorted(set(resp["source_paths"]))
    resp["warnings"] = sorted(set(resp["warnings"]))
    resp["errors"] = sorted(set(resp["errors"]))
    return resp


def build_operational_truth_v1(truth_root: Path, day: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "truth_root": str(truth_root),
        "errors": [],
        "warnings": [],
        "missing_paths": [],
        "source_paths": [],
        "source_mtimes": {},
        "summary": {
            "positions_total": 0,
            "open_positions": 0,
            "orders_total": 0,
            "working_orders": 0,
            "alerts_total": 0,
            "readiness_status": "UNKNOWN",
        },
        "positions_panel": {
            "asof_utc": None,
            "snapshot_path": None,
            "pointer_path": None,
            "rows": [],
        },
        "orders_panel": {
            "rows": [],
        },
        "system_state_panel": {
            "rows": [],
        },
        "alerts_panel": {
            "rows": [],
        },
    }

    if not _is_day_str(day):
        resp["ok"] = False
        resp["errors"].append(E_DAY_INVALID)
        return resp

    def note_source(path: Path) -> None:
        p = str(path.resolve())
        resp["source_paths"].append(p)
        mt = _mtime(path)
        if mt is not None:
            resp["source_mtimes"][p] = mt

    def read_json(path: Path) -> Optional[Any]:
        obj, err = _safe_read_json(path)
        if obj is None:
            if err == "FILE_NOT_FOUND":
                resp["missing_paths"].append(str(path.resolve()))
            else:
                resp["warnings"].append(f"UNREADABLE:{path.name}:{err}")
            return None
        note_source(path)
        return obj

    def fmt_cents_to_dollars(value: Any) -> Optional[str]:
        try:
            if value is None:
                return None
            cents = int(value)
            return f"{cents / 100:.2f}"
        except Exception:
            return None

    def parse_isoish(value: Any) -> str:
        return str(value).strip() if isinstance(value, str) and str(value).strip() else ""

    def latest_ts(*values: Any) -> Optional[str]:
        items = [parse_isoish(v) for v in values if parse_isoish(v)]
        return max(items) if items else None

    def add_alert(severity: str, code: str, summary: str, artifact_path: Optional[str] = None) -> None:
        resp["alerts_panel"]["rows"].append(
            {
                "severity": severity,
                "code": code,
                "summary": summary,
                "artifact_path": artifact_path,
            }
        )

    def add_system_row(
        key: str,
        label: str,
        status: Any,
        detail: str,
        artifact_path: Optional[str],
        produced_utc: Optional[str],
    ) -> None:
        resp["system_state_panel"]["rows"].append(
            {
                "key": key,
                "label": label,
                "status": str(status or "UNKNOWN"),
                "detail": detail,
                "artifact_path": artifact_path,
                "produced_utc": produced_utc,
            }
        )

    def extract_order_terms(order_plan: Any) -> Dict[str, Any]:
        if not isinstance(order_plan, dict):
            return {}
        order_terms = order_plan.get("order_terms")
        if isinstance(order_terms, dict):
            return order_terms
        return {}

    def find_order_plan(submission_dir: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        for name in ["equity_order_plan.v1.json", "equity_order_plan.v2.json"]:
            candidate = (submission_dir / name).resolve()
            obj = read_json(candidate)
            if isinstance(obj, dict):
                return obj, str(candidate)
        return None, None

    positions_pointer_path = (truth_root / "positions_v1" / "effective_v1" / "days" / day / "positions_effective_pointer.v1.json").resolve()
    positions_snapshot_path: Optional[Path] = None
    positions_pointer = read_json(positions_pointer_path)
    if isinstance(positions_pointer, dict):
        raw_snapshot_path = (((positions_pointer.get("pointers") or {}) if isinstance(positions_pointer.get("pointers"), dict) else {}).get("snapshot_path"))
        if isinstance(raw_snapshot_path, str) and raw_snapshot_path:
            candidate = Path(raw_snapshot_path).resolve()
            snapshot_obj = read_json(candidate)
            if isinstance(snapshot_obj, dict):
                positions_snapshot_path = candidate
                resp["positions_panel"]["pointer_path"] = str(positions_pointer_path)
                resp["positions_panel"]["snapshot_path"] = str(candidate)
                resp["positions_panel"]["asof_utc"] = (((snapshot_obj.get("positions") or {}) if isinstance(snapshot_obj.get("positions"), dict) else {}).get("asof_utc"))
                items = (((snapshot_obj.get("positions") or {}) if isinstance(snapshot_obj.get("positions"), dict) else {}).get("items"))
                if isinstance(items, list):
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        instrument = item.get("instrument") if isinstance(item.get("instrument"), dict) else {}
                        avg_price = fmt_cents_to_dollars(item.get("avg_cost_cents"))
                        row = {
                            "position_id": item.get("position_id"),
                            "engine_id": item.get("engine_id"),
                            "symbol": instrument.get("symbol"),
                            "quantity": item.get("qty"),
                            "avg_price": avg_price,
                            "status": item.get("status"),
                            "currency": instrument.get("currency"),
                            "unrealized_pnl": None,
                        }
                        resp["positions_panel"]["rows"].append(row)
    if positions_snapshot_path is None:
        for name in ["positions_snapshot.v4.json", "positions_snapshot.v2.json"]:
            candidate = (truth_root / "positions_v1" / "snapshots" / day / name).resolve()
            snapshot_obj = read_json(candidate)
            if not isinstance(snapshot_obj, dict):
                continue
            positions_snapshot_path = candidate
            resp["positions_panel"]["snapshot_path"] = str(candidate)
            positions = snapshot_obj.get("positions")
            if isinstance(positions, dict):
                resp["positions_panel"]["asof_utc"] = positions.get("asof_utc")
                items = positions.get("items")
                if isinstance(items, list):
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        instrument = item.get("instrument") if isinstance(item.get("instrument"), dict) else {}
                        avg_price = fmt_cents_to_dollars(item.get("avg_cost_cents"))
                        resp["positions_panel"]["rows"].append(
                            {
                                "position_id": item.get("position_id"),
                                "engine_id": item.get("engine_id"),
                                "symbol": instrument.get("symbol"),
                                "quantity": item.get("qty"),
                                "avg_price": avg_price,
                                "status": item.get("status"),
                                "currency": instrument.get("currency"),
                                "unrealized_pnl": None,
                            }
                        )
            break

    stream_day_dir = (truth_root / "execution_stream_v1" / day).resolve()
    latest_stream_by_submission: Dict[str, Dict[str, Any]] = {}
    if stream_day_dir.exists() and stream_day_dir.is_dir():
        for record_path in sorted(stream_day_dir.iterdir(), key=lambda p: p.name):
            if not record_path.is_file() or record_path.suffix != ".json":
                continue
            rec = read_json(record_path)
            if not isinstance(rec, dict):
                continue
            submission_id = str(rec.get("submission_id") or "").strip()
            if not submission_id:
                continue
            ts = latest_ts(rec.get("observed_at_utc"), rec.get("event_time_utc"), rec.get("produced_utc")) or ""
            cur = latest_stream_by_submission.get(submission_id)
            cur_ts = latest_ts((cur or {}).get("_latest_ts")) or ""
            if cur is None or ts >= cur_ts:
                rec["_latest_ts"] = ts
                rec["_artifact_path"] = str(record_path.resolve())
                latest_stream_by_submission[submission_id] = rec
            reason_codes = rec.get("reason_codes") if isinstance(rec.get("reason_codes"), list) else []
            if any("ORPHAN" in str(code).upper() for code in reason_codes):
                broker_ids = rec.get("broker_ids") if isinstance(rec.get("broker_ids"), dict) else {}
                add_alert(
                    "WARNING",
                    "ORPHAN_EVENT_LINEAGE",
                    f"submission_id={submission_id} order_id={broker_ids.get('order_id')} perm_id={broker_ids.get('perm_id')}",
                    str(record_path.resolve()),
                )

    failure_path = (truth_root / "execution_stream_v1" / "failures" / day / "failure.json").resolve()
    failure_doc = read_json(failure_path)
    if isinstance(failure_doc, dict):
        add_alert(
            "ERROR",
            str(failure_doc.get("reason_code") or failure_doc.get("error_code") or "EXECUTION_STREAM_FAILURE"),
            str(failure_doc.get("summary") or failure_doc.get("details") or "execution_stream_v1 failure"),
            str(failure_path),
        )

    submissions_root = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    fill_ledger_root = (truth_root / "fill_ledger_v1" / day).resolve()
    lifecycle_authority_path = (
        truth_root
        / "reports"
        / "execution_lifecycle_authority_v1"
        / day
        / "execution_lifecycle_authority.v1.json"
    ).resolve()
    lifecycle_authority = read_json(lifecycle_authority_path)
    lifecycle_by_submission = {
        str(row.get("submission_id") or "").strip(): row
        for row in ((lifecycle_authority or {}).get("submissions") or [])
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip()
    }
    lineage_candidates = [
        (
            truth_root
            / "reports"
            / "trade_lineage_graph_v1"
            / day
            / "trade_lineage_graph.v1.json"
        ).resolve(),
        (
            GLOBAL_TRUTH_ROOT
            / "reports"
            / "trade_lineage_graph_v1"
            / day
            / "trade_lineage_graph.v1.json"
        ).resolve(),
    ]
    lineage_authority_path = next((path for path in lineage_candidates if path.exists()), lineage_candidates[0])
    lineage_authority = read_json(lineage_authority_path)
    lineage_by_submission = {
        str(row.get("submission_id") or "").strip(): row
        for row in ((lineage_authority or {}).get("lineages") or [])
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip()
    }
    if submissions_root.exists() and submissions_root.is_dir():
        for submission_dir in sorted([p for p in submissions_root.iterdir() if p.is_dir()], key=lambda p: p.name):
            if submission_dir.name.startswith("__"):
                continue
            submission_id = submission_dir.name
            lifecycle_row = lifecycle_by_submission.get(submission_id) or {}
            lineage_row = lineage_by_submission.get(submission_id) or {}
            broker_record_path = (submission_dir / "broker_submission_record.v2.json").resolve()
            execution_event_path = (submission_dir / "execution_event_record.v1.json").resolve()
            broker_record = read_json(broker_record_path)
            execution_event = read_json(execution_event_path)
            order_plan, order_plan_path = find_order_plan(submission_dir)
            fill_ledger_path = (fill_ledger_root / f"{submission_id}.fill_ledger.v1.json").resolve()
            fill_ledger = read_json(fill_ledger_path)
            latest_stream = latest_stream_by_submission.get(submission_id)

            broker_ids = {}
            if lifecycle_row:
                broker_ids = {
                    "order_id": lifecycle_row.get("broker_order_id"),
                    "perm_id": lifecycle_row.get("broker_perm_id"),
                }
            elif isinstance(broker_record, dict) and isinstance(broker_record.get("broker_ids"), dict):
                broker_ids = broker_record.get("broker_ids") or {}
            elif isinstance(latest_stream, dict) and isinstance(latest_stream.get("broker_ids"), dict):
                broker_ids = latest_stream.get("broker_ids") or {}

            order_terms = extract_order_terms(order_plan)
            order_status = (
                lifecycle_row.get("current_lifecycle_state")
                or (((latest_stream.get("order_state") or {}) if isinstance((latest_stream or {}).get("order_state"), dict) else {}).get("status"))
                or (execution_event.get("raw_broker_status") if isinstance(execution_event, dict) else None)
                or (broker_record.get("status") if isinstance(broker_record, dict) else None)
                or (fill_ledger.get("lifecycle_status") if isinstance(fill_ledger, dict) else None)
                or "UNKNOWN"
            )
            last_update_utc = latest_ts(
                ((latest_stream.get("observed_at_utc") if isinstance(latest_stream, dict) else None)),
                ((latest_stream.get("event_time_utc") if isinstance(latest_stream, dict) else None)),
                (execution_event.get("event_time_utc") if isinstance(execution_event, dict) else None),
                (broker_record.get("submitted_at_utc") if isinstance(broker_record, dict) else None),
                (fill_ledger.get("produced_utc") if isinstance(fill_ledger, dict) else None),
            )

            row = {
                "submission_id": submission_id,
                "symbol": order_plan.get("symbol") if isinstance(order_plan, dict) else None,
                "side": order_plan.get("action") if isinstance(order_plan, dict) else None,
                "quantity": order_plan.get("qty_shares") if isinstance(order_plan, dict) else None,
                "status": str(order_status),
                "broker_order_id": broker_ids.get("order_id"),
                "perm_id": broker_ids.get("perm_id"),
                "last_update_utc": last_update_utc,
                "order_type": order_terms.get("order_type"),
                "time_in_force": order_terms.get("time_in_force"),
                "limit_price": order_terms.get("limit_price"),
                "filled_qty": (fill_ledger.get("filled_qty") if isinstance(fill_ledger, dict) else None),
                "remaining_qty": (fill_ledger.get("remaining_qty") if isinstance(fill_ledger, dict) else None),
                "lifecycle_status": (fill_ledger.get("lifecycle_status") if isinstance(fill_ledger, dict) else None),
                "identity_state": lineage_row.get("identity_state") if lineage_row else "UNKNOWN",
                "trade_lineage_id": lineage_row.get("trade_lineage_id") if lineage_row else None,
                "artifact_paths": {
                    "submission_dir": str(submission_dir.resolve()),
                    "broker_submission_record": str(broker_record_path),
                    "execution_event_record": str(execution_event_path),
                    "order_plan": order_plan_path,
                    "fill_ledger": str(fill_ledger_path),
                    "latest_stream": latest_stream.get("_artifact_path") if isinstance(latest_stream, dict) else None,
                    "execution_lifecycle_authority": str(lifecycle_authority_path),
                    "trade_lineage_graph": str(lineage_authority_path),
                },
            }
            resp["orders_panel"]["rows"].append(row)

            latest_hash = str(latest_stream.get("canonical_json_hash") or "") if isinstance(latest_stream, dict) else ""
            if latest_hash and isinstance(execution_event, dict):
                if str(execution_event.get("upstream_hash") or "") != latest_hash:
                    add_alert(
                        "WARNING",
                        "STALE_EXECUTION_EVENT",
                        f"submission_id={submission_id} execution_event_record.v1.json does not reflect latest execution_stream observation",
                        str(execution_event_path),
                    )
            if latest_hash and isinstance(fill_ledger, dict):
                hashes = fill_ledger.get("event_hashes") if isinstance(fill_ledger.get("event_hashes"), list) else []
                if latest_hash not in [str(x) for x in hashes]:
                    add_alert(
                        "WARNING",
                        "STALE_FILL_LEDGER",
                        f"submission_id={submission_id} fill_ledger_v1 missing latest execution_stream hash",
                        str(fill_ledger_path),
                    )

    build_ref = None
    admission_ref = None
    boundary_ref = None
    ledger_ref = None
    control_plane_ref = None
    day_authority_ref = None
    session_status_ref = None
    try:
        build_ref = read_control_plane_surface_v1(domain="session", surface="target_day_build", truth_root=truth_root, day_utc=day)
    except Exception:
        pass
    try:
        admission_ref = read_control_plane_surface_v1(domain="session", surface="target_day_admission", truth_root=truth_root, day_utc=day)
    except Exception:
        pass
    try:
        boundary_ref = read_control_plane_surface_v1(domain="execution", surface="submit_boundary_status", truth_root=truth_root, day_utc=day)
    except Exception:
        pass
    try:
        ledger_ref = read_control_plane_surface_v1(domain="execution", surface="paper_session_ledger", truth_root=truth_root, day_utc=day)
    except Exception:
        pass
    try:
        control_plane_ref = read_control_plane_surface_v1(domain="execution", surface="paper_day_control_plane", truth_root=truth_root, day_utc=day)
    except Exception:
        pass
    try:
        day_authority_ref = read_control_plane_surface_v1(
            domain="execution",
            surface="paper_trading_day_authority",
            truth_root=truth_root,
            day_utc=day,
        )
    except Exception:
        pass
    try:
        session_status_ref = read_control_plane_surface_v1(domain="session", surface="session_authority_status_current", truth_root=truth_root)
    except Exception:
        pass
    build_path = build_ref.path if build_ref is not None else (truth_root / "target_day_build_v1" / f"{day}.json").resolve()
    admission_path = admission_ref.path if admission_ref is not None else (truth_root / "target_day_admission_v1" / f"{day}.json").resolve()
    boundary_path = boundary_ref.path if boundary_ref is not None else (truth_root / "reports" / "submit_boundary_status_v1" / day / "submit_boundary_status.v1.json").resolve()
    ledger_path = ledger_ref.path if ledger_ref is not None else (truth_root / "reports" / "paper_session_ledger_v1" / day / "paper_session_ledger.v1.json").resolve()
    control_plane_path = control_plane_ref.path if control_plane_ref is not None else (truth_root / "reports" / "paper_day_control_plane_v1" / day / "paper_day_control_plane.v1.json").resolve()
    day_authority_path = (
        day_authority_ref.path
        if day_authority_ref is not None
        else (truth_root / "reports" / "paper_trading_day_authority_v1" / day / "paper_trading_day_authority.v1.json").resolve()
    )
    session_status_path = session_status_ref.path if session_status_ref is not None else (truth_root / "session_authority_status_v1" / "current.json").resolve()
    execution_recon_path = (truth_root / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json").resolve()
    runtime_service_authority_path = (truth_root / "reports" / "runtime_service_authority_v1" / day / "runtime_service_authority.v1.json").resolve()
    market_data_authority_path = (truth_root / "reports" / "market_data_authority_v1" / day / "market_data_authority.v1.json").resolve()
    strategy_decision_authority_path = (truth_root / "reports" / "strategy_decision_authority_v1" / day / "strategy_decision_authority.v1.json").resolve()
    portfolio_account_authority_path = (truth_root / "reports" / "portfolio_account_authority_v1" / day / "portfolio_account_authority.v1.json").resolve()
    risk_sizing_authority_path = (truth_root / "reports" / "risk_sizing_authority_v1" / day / "risk_sizing_authority.v1.json").resolve()
    execution_mode_authority_path = (truth_root / "reports" / "execution_mode_authority_v1" / day / "execution_mode_authority.v1.json").resolve()
    trading_day_closure_authority_path = (truth_root / "reports" / "trading_day_closure_authority_v1" / day / "trading_day_closure_authority.v1.json").resolve()
    aegis_operating_contract_path = (truth_root / "reports" / "aegis_operating_contract_v1" / day / "aegis_operating_contract.v1.json").resolve()
    aegis_authority_graph_path = (truth_root / "reports" / "aegis_authority_graph_v1" / day / "aegis_authority_graph.v1.json").resolve()
    aegis_day_evidence_ledger_path = (truth_root / "reports" / "aegis_day_evidence_ledger_v1" / day / "aegis_day_evidence_ledger.v1.json").resolve()
    aegis_daily_operator_summary_path = (truth_root / "reports" / "aegis_daily_operator_summary_v1" / day / "aegis_daily_operator_summary.v1.json").resolve()
    try:
        replay_gate_ref = read_control_plane_surface_v1(domain="execution", surface="replay_certification_gate", truth_root=truth_root, day_utc=day)
    except Exception:
        replay_gate_ref = None
    try:
        replay_bundle_ref = read_control_plane_surface_v1(domain="execution", surface="replay_certification_bundle", truth_root=truth_root, day_utc=day)
    except Exception:
        replay_bundle_ref = None
    replay_gate_path = replay_gate_ref.path if replay_gate_ref is not None else (truth_root / "reports" / "replay_certification_gate_v1" / day / "replay_certification_gate.v1.json").resolve()
    replay_bundle_path = replay_bundle_ref.path if replay_bundle_ref is not None else (truth_root / "reports" / "replay_certification_bundle_v1" / day / "replay_certification_bundle.v1.json").resolve()

    build_doc = dict(build_ref.payload) if build_ref is not None else read_json(build_path)
    admission_doc = dict(admission_ref.payload) if admission_ref is not None else read_json(admission_path)
    boundary_doc = dict(boundary_ref.payload) if boundary_ref is not None else read_json(boundary_path)
    ledger_doc = dict(ledger_ref.payload) if ledger_ref is not None else read_json(ledger_path)
    control_plane_doc = dict(control_plane_ref.payload) if control_plane_ref is not None else read_json(control_plane_path)
    day_authority_doc = dict(day_authority_ref.payload) if day_authority_ref is not None else read_json(day_authority_path)
    session_status_doc = dict(session_status_ref.payload) if session_status_ref is not None else read_json(session_status_path)
    execution_recon_doc = read_json(execution_recon_path)
    runtime_service_authority_doc = read_json(runtime_service_authority_path)
    market_data_authority_doc = read_json(market_data_authority_path)
    strategy_decision_authority_doc = read_json(strategy_decision_authority_path)
    portfolio_account_authority_doc = read_json(portfolio_account_authority_path)
    risk_sizing_authority_doc = read_json(risk_sizing_authority_path)
    execution_mode_authority_doc = read_json(execution_mode_authority_path)
    trading_day_closure_authority_doc = read_json(trading_day_closure_authority_path)
    aegis_operating_contract_doc = read_json(aegis_operating_contract_path)
    aegis_authority_graph_doc = read_json(aegis_authority_graph_path)
    aegis_day_evidence_ledger_doc = read_json(aegis_day_evidence_ledger_path)
    aegis_daily_operator_summary_doc = read_json(aegis_daily_operator_summary_path)
    replay_gate_doc = dict(replay_gate_ref.payload) if replay_gate_ref is not None else None
    replay_bundle_doc = dict(replay_bundle_ref.payload) if replay_bundle_ref is not None else None

    if isinstance(build_doc, dict):
        add_system_row(
            "build",
            "Build",
            build_doc.get("build_status"),
            f"completeness={build_doc.get('completeness_result', 'n/a')} closure={build_doc.get('closure_status', 'n/a')}",
            str(build_path),
            build_doc.get("generated_utc"),
        )
    if isinstance(admission_doc, dict):
        add_system_row(
            "admission",
            "Admission",
            admission_doc.get("admission_status"),
            f"closure={admission_doc.get('closure_status', 'n/a')}",
            str(admission_path),
            admission_doc.get("generated_utc"),
        )
    if isinstance(day_authority_doc, dict):
        add_system_row(
            "day_authority",
            "Day Authority",
            day_authority_doc.get("state"),
            (
                f"submit={day_authority_doc.get('can_submit_paper_orders')} "
                f"trade_today={day_authority_doc.get('can_paper_trade_today')} "
                f"blocker={day_authority_doc.get('canonical_blocker') or '<none>'}"
            ),
            str(day_authority_path),
            day_authority_doc.get("produced_at_utc"),
        )
    if isinstance(aegis_daily_operator_summary_doc, dict):
        add_system_row(
            "aegis_daily_operator_summary",
            "Aegis Daily Summary",
            aegis_daily_operator_summary_doc.get("no_silent_day_outcome"),
            (
                f"mode={aegis_daily_operator_summary_doc.get('mode')} "
                f"run_style={aegis_daily_operator_summary_doc.get('run_style')} "
                f"dry_run={aegis_daily_operator_summary_doc.get('dry_run')} "
                f"transmitted={aegis_daily_operator_summary_doc.get('broker_orders_transmitted')} "
                f"first_blocker={(aegis_daily_operator_summary_doc.get('first_blocker') or {}).get('code', '<none>') if isinstance(aegis_daily_operator_summary_doc.get('first_blocker'), dict) else '<none>'}"
            ),
            str(aegis_daily_operator_summary_path),
            aegis_daily_operator_summary_doc.get("produced_utc"),
        )
    if isinstance(aegis_operating_contract_doc, dict):
        add_system_row(
            "aegis_operating_contract",
            "Operating Contract",
            aegis_operating_contract_doc.get("mode"),
            f"run_style={aegis_operating_contract_doc.get('run_style')} target={aegis_operating_contract_doc.get('target_sleeve')}",
            str(aegis_operating_contract_path),
            aegis_operating_contract_doc.get("produced_utc"),
        )
    if isinstance(aegis_authority_graph_doc, dict):
        graph_nodes = aegis_authority_graph_doc.get("authority_nodes") if isinstance(aegis_authority_graph_doc.get("authority_nodes"), list) else []
        graph_blockers = aegis_authority_graph_doc.get("blocking_nodes") if isinstance(aegis_authority_graph_doc.get("blocking_nodes"), list) else []
        add_system_row(
            "aegis_authority_graph",
            "Authority Graph",
            "BLOCKED" if graph_blockers else "CLEAR",
            f"nodes={len(graph_nodes)} blockers={len(graph_blockers)}",
            str(aegis_authority_graph_path),
            aegis_authority_graph_doc.get("produced_utc"),
        )
    if isinstance(aegis_day_evidence_ledger_doc, dict):
        commands = aegis_day_evidence_ledger_doc.get("commands") if isinstance(aegis_day_evidence_ledger_doc.get("commands"), list) else []
        add_system_row(
            "aegis_day_evidence_ledger",
            "Evidence Ledger",
            aegis_day_evidence_ledger_doc.get("final_daily_outcome"),
            f"commands={len(commands)} blockers={len(aegis_day_evidence_ledger_doc.get('blockers') or [])}",
            str(aegis_day_evidence_ledger_path),
            aegis_day_evidence_ledger_doc.get("finished_utc"),
        )
    if isinstance(runtime_service_authority_doc, dict):
        add_system_row(
            "runtime_service_authority",
            "Runtime Services",
            runtime_service_authority_doc.get("service_state"),
            f"mode={runtime_service_authority_doc.get('expected_run_mode')} submit_creator={runtime_service_authority_doc.get('submit_creator_available')}",
            str(runtime_service_authority_path),
            runtime_service_authority_doc.get("produced_utc"),
        )
    if isinstance(market_data_authority_doc, dict):
        market_impact = str(market_data_authority_doc.get("operator_impact") or "UNKNOWN")
        add_system_row(
            "market_data_authority",
            "Market Data",
            market_data_authority_doc.get("market_data_state"),
            f"impact={market_impact} symbols={','.join(str(x) for x in market_data_authority_doc.get('required_symbols', []))} blocker={market_data_authority_doc.get('first_blocker') or '<none>'}",
            str(market_data_authority_path),
            market_data_authority_doc.get("produced_utc"),
        )
    if isinstance(strategy_decision_authority_doc, dict):
        add_system_row(
            "strategy_decision_authority",
            "Strategy Decision",
            strategy_decision_authority_doc.get("strategy_decision_state"),
            (
                f"intent_count={strategy_decision_authority_doc.get('intent_count')} "
                f"zero_reason={strategy_decision_authority_doc.get('zero_intent_reason') or '<none>'}"
            ),
            str(strategy_decision_authority_path),
            strategy_decision_authority_doc.get("produced_utc"),
        )
    if isinstance(portfolio_account_authority_doc, dict):
        account_values = portfolio_account_authority_doc.get("account_values") if isinstance(portfolio_account_authority_doc.get("account_values"), dict) else {}
        add_system_row(
            "portfolio_account_authority",
            "Portfolio Account",
            portfolio_account_authority_doc.get("account_state"),
            (
                f"source={portfolio_account_authority_doc.get('source_type')} "
                f"cash_cents={account_values.get('cash_total_cents')} "
                f"nlv_cents={account_values.get('net_liquidation_cents')}"
            ),
            str(portfolio_account_authority_path),
            portfolio_account_authority_doc.get("produced_utc"),
        )
    if isinstance(risk_sizing_authority_doc, dict):
        final_size = risk_sizing_authority_doc.get("final_size_summary") if isinstance(risk_sizing_authority_doc.get("final_size_summary"), dict) else {}
        final_qty = final_size.get("final_quantity") if isinstance(final_size, dict) else None
        final_risk = final_size.get("final_risk_cents") if isinstance(final_size, dict) else None
        add_system_row(
            "risk_sizing_authority",
            "Risk Sizing",
            risk_sizing_authority_doc.get("risk_sizing_state"),
            (
                f"final_qty={final_qty} final_risk_cents={final_risk} "
                f"reason={risk_sizing_authority_doc.get('first_sizing_reason') or '<none>'} "
                f"blocker={risk_sizing_authority_doc.get('first_blocker') or '<none>'}"
            ),
            str(risk_sizing_authority_path),
            risk_sizing_authority_doc.get("produced_utc"),
        )
    if isinstance(execution_mode_authority_doc, dict):
        add_system_row(
            "execution_mode_authority",
            "Execution Mode",
            execution_mode_authority_doc.get("mode_state"),
            f"broker_transmit_enabled={execution_mode_authority_doc.get('broker_transmit_enabled')} ids_expected={execution_mode_authority_doc.get('broker_ids_expected')}",
            str(execution_mode_authority_path),
            execution_mode_authority_doc.get("produced_utc"),
        )
    if isinstance(trading_day_closure_authority_doc, dict):
        add_system_row(
            "trading_day_closure_authority",
            "Trading Day Closure",
            trading_day_closure_authority_doc.get("closure_state"),
            f"closure_safe={str(trading_day_closure_authority_doc.get('closure_state') == 'DRY_RUN_CLOSED').lower()} submissions={trading_day_closure_authority_doc.get('submission_count')} blocker={trading_day_closure_authority_doc.get('first_blocker') or '<none>'}",
            str(trading_day_closure_authority_path),
            trading_day_closure_authority_doc.get("produced_utc"),
        )
    if isinstance(boundary_doc, dict):
        add_system_row(
            "boundary",
            "Boundary",
            boundary_doc.get("boundary_status"),
            f"submission_authorized={boundary_doc.get('submission_authorized')}",
            str(boundary_path),
            boundary_doc.get("produced_at_utc"),
        )
    if isinstance(ledger_doc, dict):
        control_state = ledger_doc.get("control_state") if isinstance(ledger_doc.get("control_state"), dict) else {}
        add_system_row(
            "ledger",
            "Ledger",
            control_state.get("authority_status"),
            f"evidence={ledger_doc.get('evidence_status', 'n/a')} system_ready={control_state.get('system_ready')}",
            str(ledger_path),
            ledger_doc.get("evaluated_at_utc"),
        )
    if isinstance(control_plane_doc, dict):
        add_system_row(
            "control_plane",
            "Control Plane",
            control_plane_doc.get("final_start_decision"),
            str(control_plane_doc.get("human_readable_summary") or ""),
            str(control_plane_path),
            control_plane_doc.get("evaluated_at_utc"),
        )
    consistency_status = "UNKNOWN"
    consistency_detail = ""
    if isinstance(session_status_doc, dict):
        paper_projection = (
            session_status_doc.get("paper_authority_projection")
            if isinstance(session_status_doc.get("paper_authority_projection"), dict)
            else {}
        )
        add_system_row(
            "session_status",
            "Session Status",
            paper_projection.get("open_state")
            or session_status_doc.get("submission_authorization_status"),
            (
                f"authority={paper_projection.get('authority_status', 'UNKNOWN')} "
                f"degraded={paper_projection.get('degraded_mode', False)} "
                f"submission_authorized={paper_projection.get('submission_authorized', False)} "
                f"traceability={session_status_doc.get('traceability_status', 'n/a')}"
            ),
            str(session_status_path),
            session_status_doc.get("generated_utc"),
        )
        monitoring_checks = session_status_doc.get("monitoring_checks") if isinstance(session_status_doc.get("monitoring_checks"), list) else []
        for check in monitoring_checks:
            if not isinstance(check, dict):
                continue
            if str(check.get("check_name") or "") == "canonical_readiness_authority":
                consistency_status = check.get("status") or "UNKNOWN"
                consistency_detail = str(check.get("summary") or "")
                break
        if consistency_status == "UNKNOWN":
            traceability = str(session_status_doc.get("traceability_status") or "")
            if traceability.upper() == "VALID":
                consistency_status = "PASS"
                consistency_detail = "traceability_status=VALID"
            elif traceability:
                consistency_status = traceability
                consistency_detail = f"traceability_status={traceability}"
        top_blockers = session_status_doc.get("top_blocker_reason_codes") if isinstance(session_status_doc.get("top_blocker_reason_codes"), list) else []
        for code in top_blockers:
            add_alert(
                "WARNING",
                f"SESSION_AUTHORITY:{code}",
                f"session_authority_status_v1 reported {code}",
                str(session_status_path),
            )
    add_system_row(
        "consistency_gate",
        "Consistency Gate",
        consistency_status,
        consistency_detail or "No canonical consistency detail available",
        str(session_status_path),
        session_status_doc.get("generated_utc") if isinstance(session_status_doc, dict) else None,
    )

    if isinstance(execution_recon_doc, dict):
        recon_status = str(execution_recon_doc.get("status") or "UNKNOWN")
        reason_codes = execution_recon_doc.get("reason_codes") if isinstance(execution_recon_doc.get("reason_codes"), list) else []
        if recon_status.upper() != "PASS" or reason_codes:
            add_alert(
                "WARNING" if recon_status.upper() == "PASS" else "ERROR",
                "EXECUTION_RECONCILIATION",
                f"status={recon_status} reason_codes={','.join([str(x) for x in reason_codes]) or 'none'}",
                str(execution_recon_path),
            )
    if isinstance(replay_gate_doc, dict) and str(replay_gate_doc.get("status") or "").upper() != "PASS":
        add_alert(
            "ERROR",
            "REPLAY_CERTIFICATION_GATE",
            f"status={replay_gate_doc.get('status')} reason_codes={','.join([str(x) for x in (replay_gate_doc.get('reason_codes') or [])]) or 'none'}",
            str(replay_gate_path),
        )
    elif replay_gate_doc is None and replay_bundle_doc is not None:
        add_alert(
            "WARNING",
            "REPLAY_GATE_MISSING",
            "canonical replay certification gate is unavailable",
            str(replay_gate_path),
        )
    if isinstance(replay_bundle_doc, dict) and str(replay_bundle_doc.get("status") or "").upper() != "PASS":
        add_alert(
            "ERROR",
            "REPLAY_CERTIFICATION_BUNDLE",
            f"status={replay_bundle_doc.get('status')}",
            str(replay_bundle_path),
        )

    resp["positions_panel"]["rows"].sort(key=lambda row: (str(row.get("symbol") or ""), str(row.get("position_id") or "")))
    resp["orders_panel"]["rows"].sort(key=lambda row: (str(row.get("last_update_utc") or ""), str(row.get("submission_id") or "")), reverse=True)
    resp["alerts_panel"]["rows"].sort(key=lambda row: (str(row.get("severity") or ""), str(row.get("code") or ""), str(row.get("artifact_path") or "")))
    resp["system_state_panel"]["rows"].sort(key=lambda row: [
        "build",
        "admission",
        "day_authority",
        "runtime_service_authority",
        "market_data_authority",
        "strategy_decision_authority",
        "portfolio_account_authority",
        "risk_sizing_authority",
        "execution_mode_authority",
        "boundary",
        "ledger",
        "control_plane",
        "trading_day_closure_authority",
        "session_status",
        "consistency_gate",
    ].index(str(row.get("key")) if str(row.get("key")) in {
        "build",
        "admission",
        "day_authority",
        "runtime_service_authority",
        "market_data_authority",
        "strategy_decision_authority",
        "portfolio_account_authority",
        "risk_sizing_authority",
        "execution_mode_authority",
        "boundary",
        "ledger",
        "control_plane",
        "trading_day_closure_authority",
        "session_status",
        "consistency_gate",
    } else "consistency_gate"))

    positions_rows = resp["positions_panel"]["rows"]
    order_rows = resp["orders_panel"]["rows"]
    resp["summary"]["positions_total"] = len(positions_rows)
    resp["summary"]["open_positions"] = sum(1 for row in positions_rows if str(row.get("status") or "").upper() == "OPEN")
    resp["summary"]["orders_total"] = len(order_rows)
    resp["summary"]["working_orders"] = sum(1 for row in order_rows if str(row.get("status") or "").upper() not in {"FILLED", "CANCELLED", "INACTIVE"})
    resp["summary"]["alerts_total"] = len(resp["alerts_panel"]["rows"])
    readiness_rows = {str(row.get("key")): row for row in resp["system_state_panel"]["rows"]}
    day_authority_state = str((readiness_rows.get("day_authority") or {}).get("status") or "UNKNOWN")
    control_status = str((readiness_rows.get("control_plane") or {}).get("status") or "UNKNOWN")
    session_state = str((readiness_rows.get("session_status") or {}).get("status") or "UNKNOWN")
    if day_authority_state == "OPEN_READY":
        resp["summary"]["readiness_status"] = "OPEN_READY"
    elif control_status == "READY_NOW" and session_state == "AUTHORIZED":
        resp["summary"]["readiness_status"] = "READY_NOW"
    elif control_status and control_status != "UNKNOWN":
        resp["summary"]["readiness_status"] = control_status

    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["source_paths"] = sorted(set(resp["source_paths"]))
    resp["warnings"] = sorted(set(resp["warnings"]))
    resp["errors"] = sorted(set(resp["errors"]))
    return resp


def _days_list() -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "errors": [],
        "warnings": [],
        "source_paths": [],
        "source_mtimes": {},
        "missing_paths": [],
        "days": [],
        "default_day_utc": None,
    }

    if not TRUTH_ROOT.exists():
        resp["ok"] = False
        resp["errors"].append(E_TRUTH_ROOT_MISSING)
        resp["missing_paths"].append(str(TRUTH_ROOT))
        return resp

    days = _union_days()
    resp["days"] = days
    resp["default_day_utc"] = _select_latest_day(days)
    if not days:
        resp["warnings"].append(E_NO_DAYS_FOUND)

    for p in [GATE_VERDICT_ROOT, INTENTS_ROOT, ACCOUNTING_NAV_ROOT, ACCOUNTING_ATTR_ROOT, SUBMISSIONS_ROOT, PILLARS_V1R1_ROOT, PILLARS_V1_ROOT, INTENTS_SUMMARY_ROOT, SUBMISSIONS_SUMMARY_ROOT, ACTIVITY_ROLLUP_ROOT]:
        resp["source_paths"].append(str(p))
        mt = _mtime(p)
        if mt is not None:
            resp["source_mtimes"][str(p)] = mt

    resp["source_paths"] = sorted(set(resp["source_paths"]))
    return resp


def _series_nav_endpoint(qs: Dict[str, List[str]]) -> Dict[str, Any]:
    last_n = 60
    if "days" in qs:
        try:
            last_n = int(qs["days"][0])
        except Exception:
            last_n = 60
    pts, missing, sps, smt, warns = _series_nav_points(last_n)
    return {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "errors": [],
        "warnings": warns,
        "source_paths": sps,
        "source_mtimes": smt,
        "missing_paths": missing,
        "points": pts,
    }


class OpsHandler(SimpleHTTPRequestHandler):
    STATIC_DIR = (Path(__file__).resolve().parents[1] / "static").resolve()
    SHELL_ROUTES = {
        "/",
        "/capital",
        "/capital/accounts",
        "/capital/allocation",
        "/capital/history",
        "/capital/flows",
        "/capital/cashflow",
        "/capital/validation",
        "/portfolio",
        "/performance",
        "/sleeves",
        "/advisory",
        "/tax",
        "/operations",
        "/aegis-runtime",
        "/configuration",
        "/reliability",
        "/reliability/readiness",
        "/reliability/issues",
        "/reliability/issues/detail",
        "/reliability/observations",
        "/reliability/work-orders",
        "/reliability/work-orders/detail",
        "/reliability/verifications",
        "/reliability/ai",
        "/reliability/ai-draft",
        "/audit",
        "/reports",
        "/control",
        "/state",
        "/submission",
        "/lifecycle",
    }

    @staticmethod
    def _local_cors_origin(origin: Optional[str]) -> Optional[str]:
        if not isinstance(origin, str) or not origin.strip():
            return None
        try:
            parsed = urlparse(origin)
        except Exception:
            return None
        scheme = (parsed.scheme or "").lower()
        hostname = (parsed.hostname or "").lower()
        if scheme not in {"http", "https"}:
            return None
        if hostname not in {"127.0.0.1", "localhost"}:
            return None
        if not parsed.netloc:
            return None
        return f"{scheme}://{parsed.netloc}"

    def end_headers(self) -> None:
        # Prevent stale browser assets; dashboard is operational truth UI.
        self.send_header("Cache-Control", "no-store")
        cors_origin = self._local_cors_origin(self.headers.get("Origin"))
        if cors_origin:
            self.send_header("Access-Control-Allow-Origin", cors_origin)
            self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Vary", "Origin")
        super().end_headers()

    def _send_json(self, code: int, obj: Any) -> None:
        b = json.dumps(obj, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _send_performance_cockpit_html(self, requested_day: Optional[str]) -> bool:
        day = requested_day if isinstance(requested_day, str) and _is_day_str(requested_day) else date.today().isoformat()
        path = (
            GLOBAL_TRUTH_ROOT
            / "reports"
            / PERFORMANCE_SHOWCASE_FAMILY
            / day
            / PERFORMANCE_SHOWCASE_HTML
        ).resolve()
        allowed_root = GLOBAL_TRUTH_ROOT.resolve()
        if not (str(path).startswith(str(allowed_root) + "/") or str(path) == str(allowed_root)):
            self._send_json(HTTPStatus.FORBIDDEN, {"ok": False, "errors": ["PATH_OUTSIDE_TRUTH_ROOT"], "path": str(path)})
            return True
        if not path.exists() or not path.is_file():
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["PERFORMANCE_COCKPIT_NOT_FOUND"], "path": str(path)})
            return True
        b = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)
        return True

    @staticmethod
    def _single_query_values(qs: Dict[str, List[str]]) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for key, values in (qs or {}).items():
            if not values:
                continue
            result[key] = str(values[0])
        return result

    def _send_reliability_error(self, exc: Exception) -> None:
        code = str(exc) or "RELIABILITY_REQUEST_INVALID"
        status = HTTPStatus.BAD_REQUEST
        if code.endswith("_NOT_FOUND"):
            status = HTTPStatus.NOT_FOUND
        self._send_json(
            status,
            {
                "ok": False,
                "message": code,
                "errors": [code],
            },
        )

    def _route_reliability_get(self, path: str, qs: Dict[str, List[str]]) -> bool:
        try:
            if path == "/api/reliability/observations":
                payload = list_reliability_observations_v1(self._single_query_values(qs))
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path == "/api/reliability/issues":
                payload = list_reliability_issues_v1(self._single_query_values(qs))
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path == "/api/reliability/work-orders":
                payload = list_reliability_work_orders_v1(self._single_query_values(qs))
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path == "/api/reliability/fix-attempts":
                payload = list_reliability_fix_attempts_v1(self._single_query_values(qs))
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path == "/api/reliability/verifications":
                payload = list_reliability_verifications_v1(self._single_query_values(qs))
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path == "/api/reliability/next-actions":
                payload = list_reliability_next_actions_v1()
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path.startswith("/api/reliability/issues/"):
                parts = path.strip("/").split("/")
                if len(parts) == 5 and parts[4] == "work-orders":
                    issue_id = unquote(parts[3])
                    payload = list_reliability_issue_work_orders_v1(issue_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
                if len(parts) == 5 and parts[4] == "verifications":
                    issue_id = unquote(parts[3])
                    payload = list_reliability_issue_verifications_v1(issue_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
                if len(parts) == 4:
                    issue_id = unquote(parts[3])
                    payload = get_reliability_issue_v1(issue_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/work-orders/"):
                parts = path.strip("/").split("/")
                if len(parts) == 5 and parts[4] == "fix-attempts":
                    work_order_id = unquote(parts[3])
                    payload = list_reliability_work_order_fix_attempts_v1(work_order_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
                if len(parts) == 4:
                    work_order_id = unquote(parts[3])
                    payload = get_reliability_work_order_v1(work_order_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/fix-attempts/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4:
                    fix_attempt_id = unquote(parts[3])
                    payload = get_reliability_fix_attempt_v1(fix_attempt_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/verifications/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4:
                    verification_id = unquote(parts[3])
                    payload = get_reliability_verification_v1(verification_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path == "/api/reliability/readiness/latest":
                payload = get_latest_reliability_readiness_v1()
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path.startswith("/api/reliability/readiness/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[2] == "readiness":
                    assessment_id = unquote(parts[3])
                    payload = get_reliability_readiness_v1(assessment_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
        except ValueError as exc:
            self._send_reliability_error(exc)
            return True
        return False

    def _route_reliability_post(self, path: str) -> bool:
        try:
            if path == "/api/reliability/observations":
                body = self._read_json_body()
                payload = create_reliability_observation_v1(body)
                self._send_json(HTTPStatus.CREATED, {"ok": True, "observation": payload})
                return True
            if path == "/api/reliability/issues":
                body = self._read_json_body()
                payload = create_reliability_issue_v1(body)
                self._send_json(HTTPStatus.CREATED, payload)
                return True
            if path == "/api/reliability/work-orders":
                body = self._read_json_body()
                payload = create_reliability_work_order_v1(body)
                self._send_json(HTTPStatus.CREATED, payload)
                return True
            if path == "/api/reliability/fix-attempts":
                body = self._read_json_body()
                payload = create_reliability_fix_attempt_v1(body)
                self._send_json(HTTPStatus.CREATED, payload)
                return True
            if path == "/api/reliability/verifications":
                body = self._read_json_body()
                payload = create_reliability_verification_v1(body)
                self._send_json(HTTPStatus.CREATED, payload)
                return True
            if path.startswith("/api/reliability/issues/"):
                parts = path.strip("/").split("/")
                if len(parts) == 5 and parts[4] == "link-observation":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = link_reliability_issue_observation_v1(issue_id, body)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
                if len(parts) == 5 and parts[4] == "work-orders":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = create_reliability_issue_work_order_v1(issue_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
                if len(parts) == 5 and parts[4] == "verifications":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = create_reliability_issue_verification_v1(issue_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
                if len(parts) == 5 and parts[4] == "create-work-order":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = create_reliability_work_order_from_issue_v1(issue_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
                if len(parts) == 5 and parts[4] == "verify":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = verify_reliability_issue_v1(issue_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
            if path.startswith("/api/reliability/work-orders/"):
                parts = path.strip("/").split("/")
                if len(parts) == 5 and parts[4] == "fix-attempts":
                    work_order_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = create_reliability_work_order_fix_attempt_v1(work_order_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
                if len(parts) == 5 and parts[4] == "record-fix-attempt":
                    work_order_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = record_reliability_fix_attempt_v1(work_order_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
            if path == "/api/reliability/ai/draft-issue":
                body = self._read_json_body()
                payload = draft_reliability_issue_v1(body)
                status = HTTPStatus.CREATED if payload.get("created") else HTTPStatus.OK
                self._send_json(status, payload)
                return True
            if path == "/api/reliability/readiness/assess":
                body = self._read_json_body()
                payload = assess_reliability_readiness_v1(body)
                self._send_json(HTTPStatus.CREATED, payload)
                return True
        except ConfigurationWorkflowApiError as exc:
            self._send_json(
                exc.status_code,
                {
                    "ok": False,
                    "message": str(exc),
                    "reason_codes": exc.reason_codes,
                    "details": exc.details,
                },
            )
            return True
        except ValueError as exc:
            self._send_reliability_error(exc)
            return True
        return False

    def _route_reliability_patch(self, path: str) -> bool:
        try:
            if path.startswith("/api/reliability/issues/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[2] == "issues":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = update_reliability_issue_v1(issue_id, body)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/work-orders/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[2] == "work-orders":
                    work_order_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = update_reliability_work_order_v1(work_order_id, body)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/fix-attempts/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[2] == "fix-attempts":
                    fix_attempt_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = update_reliability_fix_attempt_v1(fix_attempt_id, body)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/verifications/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[2] == "verifications":
                    verification_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = update_reliability_verification_v1(verification_id, body)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
        except ConfigurationWorkflowApiError as exc:
            self._send_json(
                exc.status_code,
                {
                    "ok": False,
                    "message": str(exc),
                    "reason_codes": exc.reason_codes,
                    "details": exc.details,
                },
            )
            return True
        except ValueError as exc:
            self._send_reliability_error(exc)
            return True
        return False

    def _health_payload(self) -> Dict[str, Any]:
        host = ""
        port = 0
        try:
            host = str(self.server.server_address[0])
            port = int(self.server.server_address[1])
        except Exception:
            host = "127.0.0.1"
            port = 0

        checks: Dict[str, Any] = {
            "static_dir_exists": OpsHandler.STATIC_DIR.exists(),
            "truth_root_exists": TRUTH_ROOT.exists(),
            "truth_root_is_dir": TRUTH_ROOT.is_dir(),
            "runtime_root_exists": RUNTIME_ROOT.exists(),
            "runtime_root_is_dir": RUNTIME_ROOT.is_dir(),
        }
        status = "READY" if all(bool(v) for v in checks.values()) else "DEGRADED"
        payload: Dict[str, Any] = {
            "service": "ops_dashboard",
            "status": status,
            "timestamp_utc": _utc_now_iso(),
            "host": host,
            "port": port,
            "checks": checks,
            "runtime_root": str(RUNTIME_ROOT),
        }
        version = _service_version()
        if isinstance(version, str) and version:
            payload["version"] = version
        return payload

    def translate_path(self, path: str) -> str:
        u = urlparse(path)
        normalized = u.path.rstrip("/") or "/"
        if normalized.startswith("/reliability/work-orders/") and normalized != "/reliability/work-orders":
            rel = "index.html"
        elif normalized in self.SHELL_ROUTES:
            rel = "index.html"
        else:
            rel = u.path.lstrip("/")
        if rel == "":
            rel = "index.html"
        full = (self.STATIC_DIR / rel).resolve()
        if not str(full).startswith(str(self.STATIC_DIR)):
            return str(self.STATIC_DIR / "index.html")
        return str(full)

    def _route_api(self) -> bool:
        u = urlparse(self.path)
        path = u.path
        if not path.startswith("/api/"):
            return False

        qs = parse_qs(u.query)
        raw_day = (qs.get("day") or [None])[0]
        requested_day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else None

        if path.startswith("/api/reliability/"):
            if self._route_reliability_get(path, qs):
                return True

        if path == "/api/shared/status-semantics":
            self._send_json(HTTPStatus.OK, {"ok": True, "status_semantics": STATUS_SEMANTICS})
            return True

        if path == "/api/shell/status-rail":
            summary_only = (qs.get("summary") or [""])[0] in {"1", "true", "TRUE", "yes", "YES"}
            self._send_json(
                HTTPStatus.OK,
                build_kernel_status_rail_summary_view() if summary_only else build_kernel_status_rail_view(),
            )
            return True

        if path == "/api/work-queue":
            self._send_json(HTTPStatus.OK, build_operator_work_queue_view())
            return True

        if path.startswith("/api/workspace/"):
            workspace_id = path.rsplit("/", 1)[-1]
            payload = build_workspace_view(workspace_id)
            status_code = HTTPStatus.OK if payload.get("ok") else HTTPStatus.NOT_FOUND
            self._send_json(status_code, payload)
            return True

        if path in {"/api/system/summary", "/api/product-summary"}:
            self._send_json(HTTPStatus.OK, build_system_summary_view(requested_day))
            return True

        if path == "/api/refinement":
            self._send_json(HTTPStatus.OK, build_refinement_state_view(requested_day))
            return True

        if path == "/api/readiness-kernel":
            self._send_json(HTTPStatus.OK, build_readiness_kernel_v1(requested_day))
            return True

        if path == "/api/system/actions":
            self._send_json(HTTPStatus.OK, {"ok": True, **build_action_inventory()})
            return True

        if path == "/api/system/action-audit":
            self._send_json(HTTPStatus.OK, {"ok": True, "audit_entries": list_action_audit_entries()})
            return True

        if path == "/api/operations":
            self._send_json(HTTPStatus.OK, build_operations_view(requested_day))
            return True

        if path == "/api/aegis/operator-state":
            self._send_json(HTTPStatus.OK, get_operator_state(GLOBAL_TRUTH_ROOT))
            return True

        if path == "/api/command/overview":
            self._send_json(HTTPStatus.OK, build_command_overview_view(requested_day))
            return True

        if path == "/api/operator-workflow":
            self._send_json(
                HTTPStatus.OK,
                build_operator_workflow_summary(
                    build_operations_view(requested_day),
                    build_alerts_view(requested_day),
                    build_reconciliation_view(requested_day),
                    build_positions_view(requested_day),
                    build_orders_view(requested_day),
                    build_action_inventory(),
                ),
            )
            return True

        if path == "/api/advisory":
            summary_only = (qs.get("summary") or [""])[0] in {"1", "true", "TRUE", "yes", "YES"}
            self._send_json(HTTPStatus.OK, build_advisory_view(requested_day, include_evidence=not summary_only))
            return True

        if path == "/api/policy-evolution":
            self._send_json(HTTPStatus.OK, build_policy_evolution_view(requested_day))
            return True

        if path == "/api/opportunities":
            self._send_json(HTTPStatus.OK, build_opportunity_state_view(requested_day))
            return True

        if path in {"/api/outcomes", "/api/value"}:
            self._send_json(HTTPStatus.OK, build_value_state_view(requested_day))
            return True

        if path == "/api/financial-state":
            self._send_json(HTTPStatus.OK, build_financial_state_view(requested_day))
            return True

        if path == "/api/capital":
            self._send_json(HTTPStatus.OK, build_capital_query_surface_v1())
            return True

        if path == "/api/capital/overview":
            self._send_json(HTTPStatus.OK, build_capital_overview_view())
            return True

        if path == "/api/capital/accounts":
            self._send_json(HTTPStatus.OK, build_capital_accounts_view())
            return True

        if path == "/api/capital/allocation":
            self._send_json(HTTPStatus.OK, build_capital_allocation_view())
            return True

        if path == "/api/capital/history":
            self._send_json(HTTPStatus.OK, build_capital_history_view())
            return True

        if path == "/api/capital/flows":
            self._send_json(HTTPStatus.OK, build_capital_flows_view())
            return True

        if path == "/api/capital/cashflow":
            effective = resolve_effective_capital_cashflow_inputs_v1()
            effective_values = dict(effective.get("values") or {})
            raw_scenario = (qs.get("scenario") or [effective_values.get("scenario") or "florida"])[0]
            scenario = str(raw_scenario or "florida").strip().lower()
            if not scenario:
                scenario = "florida"
            raw_include = (qs.get("include_inheritance") or [effective_values.get("include_inheritance")])[0]
            if isinstance(raw_include, bool):
                include_inheritance = raw_include
            else:
                include_inheritance = str(raw_include).strip().lower() in {"1", "true", "yes", "on"}
            raw_horizon = (qs.get("horizon_months") or [effective_values.get("horizon_months") or "24"])[0]
            horizon_months = int(effective_values.get("horizon_months") or 24)
            try:
                horizon_months = int(raw_horizon)
            except Exception:
                horizon_months = int(effective_values.get("horizon_months") or 24)
            raw_start_month = (qs.get("start_month") or [effective_values.get("start_month")])[0]
            start_month = None if raw_start_month is None else str(raw_start_month)
            self._send_json(
                HTTPStatus.OK,
                build_capital_cashflow_view(
                    scenario=scenario,
                    include_inheritance=include_inheritance,
                    horizon_months=horizon_months,
                    start_month=start_month,
                ),
            )
            return True

        if path == "/api/capital/validation":
            self._send_json(HTTPStatus.OK, build_capital_validation_view())
            return True

        if path == "/api/configuration/catalog":
            self._send_json(HTTPStatus.OK, build_configuration_catalog_v1())
            return True

        if path == "/api/configuration/current":
            self._send_json(HTTPStatus.OK, build_configuration_current_v1())
            return True

        if path.startswith("/api/configuration/drafts/"):
            parts = path.strip("/").split("/")
            if len(parts) == 4 and parts[0] == "api" and parts[1] == "configuration" and parts[2] == "drafts":
                draft_id = parts[3]
                try:
                    payload = get_configuration_draft_v1(draft_id)
                except ConfigurationWorkflowApiError as exc:
                    self._send_json(
                        exc.status_code,
                        {
                            "ok": False,
                            "message": str(exc),
                            "reason_codes": exc.reason_codes,
                            "details": exc.details,
                        },
                    )
                    return True
                self._send_json(HTTPStatus.OK, payload)
                return True

        if path == "/api/sleeves":
            self._send_json(HTTPStatus.OK, build_sleeve_evaluation_view(requested_day))
            return True

        if path == "/api/tax":
            self._send_json(HTTPStatus.OK, build_tax_state_view(requested_day))
            return True

        if path == "/api/orders":
            self._send_json(HTTPStatus.OK, build_orders_view(requested_day))
            return True

        if path == "/api/positions":
            self._send_json(HTTPStatus.OK, build_positions_view(requested_day))
            return True

        if path == "/api/reconciliation":
            self._send_json(HTTPStatus.OK, build_reconciliation_view(requested_day))
            return True

        if path == "/api/integrity":
            self._send_json(HTTPStatus.OK, build_integrity_view(requested_day))
            return True

        if path == "/api/alerts":
            self._send_json(HTTPStatus.OK, build_alerts_view(requested_day))
            return True

        if path == "/api/days":
            self._send_json(HTTPStatus.OK, _days_list())
            return True

        if path == "/api/latest_day":
            days = _union_days()
            self._send_json(HTTPStatus.OK, {"ok": True, "errors": [], "day_utc": _select_latest_day(days)})
            return True

        if path == "/api/activity/latest":
            day = _activity_latest_day()
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": True, "errors": [E_ACTIVITY_DAY_NOT_RESOLVED], "day_utc": None})
            else:
                self._send_json(HTTPStatus.OK, {"ok": True, "errors": [], "day_utc": day})
            return True

        if path == "/api/activity/today":
            day = None
            raw = (qs.get("day") or [None])[0]
            if isinstance(raw, str) and raw and _is_day_str(raw):
                day = raw
            if day is None:
                day = _activity_latest_day()
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": [E_ACTIVITY_DAY_NOT_RESOLVED], "path": path})
                return True
            self._send_json(HTTPStatus.OK, _activity_today(day))
            return True

        if path == "/api/activity/rollup":
            raw = (qs.get("asof") or [None])[0]
            asof = raw if isinstance(raw, str) and raw and _is_day_str(raw) else _activity_latest_day()
            if not asof:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": [E_ACTIVITY_DAY_NOT_RESOLVED], "path": path})
                return True
            self._send_json(HTTPStatus.OK, _activity_today(asof))
            return True

        if path == "/api/artifact":
            try:
                raw = (qs.get("path") or [None])[0]
                if not isinstance(raw, str) or not raw:
                    self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["MISSING_QUERY_PATH"], "path": None, "content": ""})
                    return True

                day_raw = (qs.get("day") or [None])[0]
                day = day_raw if isinstance(day_raw, str) and day_raw and _is_day_str(day_raw) else None
                selected_root = _truth_root_for_day(day)
                allowed_roots = _known_truth_roots() or [selected_root]
                for extra_root in [GLOBAL_TRUTH_ROOT, ADVISORY_RUNTIME_ROOT]:
                    if isinstance(extra_root, Path) and extra_root.exists() and extra_root.is_dir():
                        allowed_roots.append(extra_root.resolve())

                p = Path(raw)
                if not p.is_absolute():
                    p = (selected_root / raw).resolve()
                else:
                    p = p.resolve()

                allowed = False
                for root in allowed_roots:
                    root_s = str(root.resolve())
                    if str(p).startswith(root_s + "/") or str(p) == root_s:
                        allowed = True
                        break
                if not allowed:
                    self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["PATH_OUTSIDE_TRUTH_ROOT"], "path": str(p), "content": ""})
                    return True

                if not p.exists() or not p.is_file():
                    self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["ARTIFACT_NOT_FOUND"], "path": str(p), "content": ""})
                    return True

                data = p.read_text(encoding="utf-8", errors="replace")
                truncated = False
                if len(data) > 20000:
                    data = data[:20000] + "\n\n...TRUNCATED...\n"
                    truncated = True

                self._send_json(HTTPStatus.OK, {"ok": True, "errors": [], "path": str(p), "content": data, "truncated": truncated})
                return True
            except Exception:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["ARTIFACT_READ_FAILED"], "path": None, "content": ""})
                return True

        if path == "/api/status":
            from constellation_2.phaseL.ui.server.c3_ui_status_collector_v1 import build_c3_ui_status

            self._send_json(HTTPStatus.OK, build_c3_ui_status(TRUTH_ROOT))
            return True

        if path == "/api/operational_truth":
            raw_day = (qs.get("day") or [None])[0]
            day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else _select_latest_day(_union_days())
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["DAY_NOT_RESOLVED"]})
                return True
            selected_root = _truth_root_for_day(day)
            payload = build_operational_truth_v1(selected_root, day)
            payload["truth_root"] = str(selected_root)
            self._send_json(HTTPStatus.OK, payload)
            return True

        if path == "/api/attempts":
            from constellation_2.phaseL.ui.server.c2_ops_cockpit_status_v2_collector_v1 import discover_attempts, select_preferred_attempt

            raw_day = (qs.get("day") or [None])[0]
            day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else _select_latest_day(_union_days())
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["DAY_NOT_RESOLVED"], "attempts": []})
                return True
            selected_root = _truth_root_for_day(day)
            attempts, missing, source_paths, source_mtimes, warnings = discover_attempts(selected_root, day)
            preferred_attempt = select_preferred_attempt(selected_root, day, attempts)
            self._send_json(HTTPStatus.OK, {
                "ok": True,
                "generated_utc": _utc_now_iso(),
                "day_utc": day,
                "attempts": attempts,
                "recommended_attempt_id": preferred_attempt,
                "truth_root": str(selected_root),
                "warnings": warnings,
                "missing_paths": missing,
                "source_paths": source_paths,
                "source_mtimes": source_mtimes,
            })
            return True

        if path == "/api/status_v2":
            from constellation_2.phaseL.ui.server.c2_ops_cockpit_status_v2_collector_v1 import build_status_v2
            from constellation_2.phaseL.ui.server.c3_ui_status_collector_v1 import build_c3_ui_status

            # Consolidated payload for Ops Cockpit UI V2.
            raw_day = (qs.get("day") or [None])[0]
            day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else _select_latest_day(_union_days())
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["DAY_NOT_RESOLVED"]})
                return True
            selected_root = _truth_root_for_day(day)
            raw_attempt = (qs.get("attempt_id") or [None])[0]
            attempt_id = raw_attempt if isinstance(raw_attempt, str) and raw_attempt else None

            # C3 status used only as an informational truth-derived surface for some gate tiles.
            c3 = build_c3_ui_status(selected_root)
            inst = _instance_config_path()

            payload = build_status_v2(selected_root, inst, day, attempt_id, c3)
            if isinstance(payload.get("meta"), dict):
                payload["meta"]["truth_root"] = str(selected_root)
            payload["ok"] = True
            payload["errors"] = []
            self._send_json(HTTPStatus.OK, payload)
            return True

        if path == "/api/operator/home":
            raw_day = (qs.get("day") or [None])[0]
            day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else _select_latest_day(_union_days())
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["DAY_NOT_RESOLVED"]})
                return True
            selected_root = _truth_root_for_day(day)
            bundle = build_operator_home_bundle(repo_root=REPO_ROOT, truth_root=selected_root, day_utc=day)
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "generated_utc": _utc_now_iso(),
                    "day_utc": day,
                    "truth_root": str(selected_root),
                    "home_view": bundle["home_view"],
                    "trust_panel": bundle["trust_panel"],
                    "retrieval_manifest": bundle["retrieval_manifest"],
                },
            )
            return True

        if path == "/api/operator/query":
            raw_day = (qs.get("day") or [None])[0]
            day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else _select_latest_day(_union_days())
            query_text = (qs.get("q") or [""])[0]
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["DAY_NOT_RESOLVED"]})
                return True
            if not isinstance(query_text, str) or not query_text.strip():
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["QUERY_TEXT_REQUIRED"]})
                return True
            selected_root = _truth_root_for_day(day)
            bundle = build_operator_query_bundle(
                repo_root=REPO_ROOT,
                truth_root=selected_root,
                day_utc=day,
                query_text=query_text,
            )
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "generated_utc": _utc_now_iso(),
                    "day_utc": day,
                    "truth_root": str(selected_root),
                    "query_response": bundle["query_response"],
                    "trust_panel": bundle["trust_panel"],
                    "retrieval_manifest": bundle["retrieval_manifest"],
                },
            )
            return True

        if path.startswith("/api/day/"):
            parts = path.strip("/").split("/")
            if len(parts) != 4:
                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": path})
                return True
            _, _, day, leaf = parts
            if leaf == "summary":
                self._send_json(HTTPStatus.OK, _day_summary(day))
                return True
            if leaf == "plan":
                self._send_json(HTTPStatus.OK, _day_plan(day))
                return True
            if leaf == "submissions":
                self._send_json(HTTPStatus.OK, _day_submissions(day))
                return True
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": path})
            return True

        if path == "/api/series/nav":
            self._send_json(HTTPStatus.OK, _series_nav_endpoint(qs))
            return True

        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": path})
        return True

    def _read_json_body(self) -> Dict[str, Any]:
        try:
            content_length = int(self.headers.get("Content-Length") or "0")
        except Exception:
            content_length = 0
        raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception as exc:
            raise ConfigurationWorkflowApiError(
                "Invalid JSON body.",
                status_code=400,
                reason_codes=["INVALID_JSON_BODY"],
            ) from exc
        if not isinstance(body, dict):
            raise ConfigurationWorkflowApiError(
                "Body must be an object.",
                status_code=400,
                reason_codes=["BODY_MUST_BE_OBJECT"],
            )
        return body

    def _route_configuration_post(self) -> bool:
        u = urlparse(self.path)
        path = u.path
        if path == "/api/configuration/drafts":
            try:
                body = self._read_json_body()
                payload = create_configuration_draft_v1(body)
            except ConfigurationWorkflowApiError as exc:
                self._send_json(
                    exc.status_code,
                    {
                        "ok": False,
                        "message": str(exc),
                        "reason_codes": exc.reason_codes,
                        "details": exc.details,
                    },
                )
                return True
            self._send_json(HTTPStatus.CREATED, payload)
            return True
        if not path.startswith("/api/configuration/drafts/"):
            return False
        parts = path.strip("/").split("/")
        if len(parts) != 5 or parts[0] != "api" or parts[1] != "configuration" or parts[2] != "drafts":
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": path})
            return True
        draft_id = parts[3]
        action = parts[4]
        try:
            body = self._read_json_body()
            if action == "validate":
                payload = validate_configuration_draft_v1(draft_id)
            elif action == "review":
                payload = review_configuration_draft_v1(draft_id)
            elif action == "activate":
                payload = activate_configuration_draft_v1(draft_id)
            elif action == "reject":
                payload = reject_configuration_draft_v1(draft_id, body)
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": path})
                return True
        except ConfigurationWorkflowApiError as exc:
            self._send_json(
                exc.status_code,
                {
                    "ok": False,
                    "message": str(exc),
                    "reason_codes": exc.reason_codes,
                    "details": exc.details,
                },
            )
            return True
        self._send_json(HTTPStatus.OK, payload)
        return True

    def _route_action_post(self) -> bool:
        u = urlparse(self.path)
        path = u.path
        if path.startswith("/api/configuration/"):
            return self._route_configuration_post()
        if path.startswith("/api/reliability/"):
            return self._route_reliability_post(path)
        if path.startswith("/api/commands/"):
            try:
                content_length = int(self.headers.get("Content-Length") or "0")
            except Exception:
                content_length = 0
            raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                body = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "result": "INVALID_JSON_BODY"})
                return True
            if not isinstance(body, dict):
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "result": "BODY_MUST_BE_OBJECT"})
                return True
            result = dispatch_kernel_command(path, body)
            self._send_json(HTTPStatus.OK, result)
            return True
        if not path.startswith("/api/actions/"):
            return False

        try:
            content_length = int(self.headers.get("Content-Length") or "0")
        except Exception:
            content_length = 0
        raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "result": "INVALID_JSON_BODY"})
            return True
        if not isinstance(body, dict):
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "result": "BODY_MUST_BE_OBJECT"})
            return True

        action_map = {
            "/api/actions/refresh-lifecycle": "refresh-lifecycle",
            "/api/actions/run-reconciliation": "run-reconciliation",
            "/api/actions/run-replay-check": "run-replay-check",
            "/api/actions/cancel-working-order": "cancel-working-order",
        }
        action_name = action_map.get(path)
        if action_name is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "result": "ENDPOINT_NOT_FOUND", "path": path})
            return True

        result = run_action(action_name, body)
        status_code = HTTPStatus.OK if result.get("ok") else HTTPStatus.CONFLICT
        self._send_json(status_code, result)
        return True

    def do_GET(self) -> None:
        started = time.perf_counter()
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/health":
            self._send_json(HTTPStatus.OK, self._health_payload())
            sys.stderr.write(f"TIMING: api endpoint=/health duration_ms={(time.perf_counter() - started) * 1000:.1f}\n")
            return
        if path == "/performance/cockpit.html":
            qs = parse_qs(parsed.query)
            raw_day = (qs.get("day") or [None])[0]
            requested_day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else None
            self._send_performance_cockpit_html(requested_day)
            sys.stderr.write(f"TIMING: performance cockpit duration_ms={(time.perf_counter() - started) * 1000:.1f}\n")
            return
        if self._route_api():
            sys.stderr.write(f"TIMING: api endpoint={path} duration_ms={(time.perf_counter() - started) * 1000:.1f}\n")
            return
        return super().do_GET()

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self) -> None:
        if self._route_action_post():
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": self.path})

    def do_PATCH(self) -> None:
        path = urlparse(self.path).path
        if path.startswith("/api/reliability/") and self._route_reliability_patch(path):
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": self.path})

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s - - [%s] %s\n" % (self.client_address[0], _utc_now_iso(), fmt % args))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3000)
    ns = ap.parse_args(argv)

    if not TRUTH_ROOT.exists():
        sys.stderr.write(f"ERROR: {E_TRUTH_ROOT_MISSING}: {TRUTH_ROOT}\n")

    httpd = ThreadingHTTPServer((ns.host, ns.port), OpsHandler)
    sys.stderr.write(f"OK: OPS_DASHBOARD_LISTENING http://{ns.host}:{ns.port}\n")
    sys.stderr.write(f"OK: STATIC_DIR {OpsHandler.STATIC_DIR}\n")
    sys.stderr.write(f"OK: TRUTH_ROOT {TRUTH_ROOT}\n")
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
