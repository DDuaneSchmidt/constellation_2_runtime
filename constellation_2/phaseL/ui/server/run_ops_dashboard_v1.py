#!/usr/bin/env python3
"""
Constellation 2.0 — Phase L — Live Ops Dashboard (Read-Only)

- Serves static UI + read-only JSON API
- Reads ONLY canonical truth artifacts under constellation_2/runtime/truth
- Never talks to IB, never submits orders, never mutates truth
- Fail-closed: missing artifacts are surfaced with explicit error codes + file pointers
- Minimal deps: Python stdlib only
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from constellation_2.phaseL.ui.server.c3_ui_status_collector_v1 import build_c3_ui_status

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
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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

    for root in [GATE_VERDICT_ROOT, INTENTS_ROOT, ACCOUNTING_NAV_ROOT, ACCOUNTING_ATTR_ROOT]:
        for d in _list_day_dirs(root):
            days.add(d)

    if SUBMISSIONS_ROOT.exists() and SUBMISSIONS_ROOT.is_dir():
        for d in _list_day_dirs(SUBMISSIONS_ROOT):
            days.add(d)

    for root in [PILLARS_V1R1_ROOT, PILLARS_V1_ROOT]:
        for d in _list_day_dirs(root):
            days.add(d)

    for root in [INTENTS_SUMMARY_ROOT, SUBMISSIONS_SUMMARY_ROOT, ACTIVITY_ROLLUP_ROOT]:
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
    latest_path = ENGINE_LINKAGE_ROOT / "latest.json"
    if latest_path.exists():
        to_try.append(latest_path)

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

    def _send_json(self, code: int, obj: Any) -> None:
        b = json.dumps(obj, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def translate_path(self, path: str) -> str:
        u = urlparse(path)
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

                p = Path(raw)
                if not p.is_absolute():
                    p = (TRUTH_ROOT / raw).resolve()
                else:
                    p = p.resolve()

                truth_root_s = str(TRUTH_ROOT.resolve())
                if not str(p).startswith(truth_root_s + "/") and str(p) != truth_root_s:
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
            self._send_json(HTTPStatus.OK, build_c3_ui_status(TRUTH_ROOT))
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

    def do_GET(self) -> None:
        if self._route_api():
            return
        return super().do_GET()

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s - - [%s] %s\n" % (self.client_address[0], _utc_now_iso(), fmt % args))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8787)
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
