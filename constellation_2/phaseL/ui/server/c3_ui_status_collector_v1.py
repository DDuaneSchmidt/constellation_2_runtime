# constellation_2/phaseL/ui/server/c3_ui_status_collector_v1.py
# C3 UI Status Collector (truth-only, fail-closed)
#
# Allowed sources only:
# - constellation_2/runtime/truth/latest.json
# - constellation_2/runtime/truth/reports/gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json
# - constellation_2/runtime/truth/reports/broker_reconciliation_*
# - constellation_2/runtime/truth/market_data_snapshot_v1
# - constellation_2/runtime/truth/monitoring_*
#
# No compatibility layer, no hardcoded components list, no legacy status sources.

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except FileNotFoundError:
        return None, "MISSING"
    except Exception:
        return None, "UNREADABLE_OR_INVALID_JSON"


def _list_day_dirs(root: Path) -> List[str]:
    if not root.exists() or not root.is_dir():
        return []
    out: List[str] = []
    for p in root.iterdir():
        if p.is_dir():
            out.append(p.name)
    out.sort()
    return out


def _max_day(days: List[str]) -> Optional[str]:
    if not days:
        return None
    return sorted(days)[-1]


def _safe_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def build_c3_ui_status(truth_root: Path) -> Dict[str, Any]:
    generated = _utc_now_iso()

    errors: List[str] = []
    warnings: List[str] = []
    missing_paths: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}

    def note_source(p: Path) -> None:
        source_paths.append(str(p))
        try:
            source_mtimes[str(p)] = p.stat().st_mtime
        except Exception:
            warnings.append("SOURCE_MTIME_UNREADABLE")

    # ---- latest.json ----
    latest_path = truth_root / "latest.json"
    latest, latest_err = _read_json(latest_path)
    if latest is None:
        errors.append("LATEST_POINTER_MISSING" if latest_err == "MISSING" else "LATEST_POINTER_UNREADABLE")
        missing_paths.append(str(latest_path))
        day = None
    else:
        note_source(latest_path)
        day = latest.get("day_utc")
        if not isinstance(day, str) or not day:
            errors.append("LATEST_POINTER_DAY_INVALID")

    verdict_source = "gate_stack_verdict_v1"
    verdict_day = day if isinstance(day, str) else None

    # ---- verdict ----
    verdict_state = "UNKNOWN"
    verdict_obj: Dict[str, Any] = {
        "state": "UNKNOWN",
        "source": verdict_source,
        "day": verdict_day or "n/a",
        "artifact_path": None,
        # Evidence fields (truth-only)
        "blocking_class": "n/a",
        "reason_codes_top": [],
        "required_failures_top": [],  # [{gate_id,status,reason_codes_top,artifact_path}]
        "gates_top": [],  # [{gate_id,required,blocking,gate_class,status,artifact_path,reason_codes_top}]
    }

    if verdict_day:
        gate_path = truth_root / "reports" / "gate_stack_verdict_v1" / verdict_day / "gate_stack_verdict.v1.json"
        verdict_obj["artifact_path"] = str(gate_path)
        gate_doc, gate_err = _read_json(gate_path)

        if gate_doc is None:
            errors.append("VERDICT_MISSING" if gate_err == "MISSING" else "VERDICT_UNREADABLE")
            missing_paths.append(str(gate_path))
            verdict_state = "DEGRADED"
            verdict_obj["state"] = "DEGRADED"
        else:
            note_source(gate_path)

            st = gate_doc.get("status")
            if st == "PASS":
                verdict_state = "PASS"
                verdict_obj["state"] = "PASS"
            elif st == "FAIL":
                verdict_state = "FAIL"
                verdict_obj["state"] = "FAIL"
            else:
                verdict_state = "DEGRADED"
                verdict_obj["state"] = "DEGRADED"

            blocking_class = gate_doc.get("blocking_class")
            top_reasons = gate_doc.get("reason_codes")
            gates = _safe_list(gate_doc.get("gates"))

            verdict_obj["blocking_class"] = blocking_class if isinstance(blocking_class, str) else "n/a"
            verdict_obj["reason_codes_top"] = top_reasons[:3] if isinstance(top_reasons, list) else []

            # Build gates table (stable ordering: required first, then blocking, then non-PASS, then gate_id)
            def gate_rank(g: Dict[str, Any]) -> Tuple[int, int, int, str]:
                required = 1 if g.get("required") is True else 0
                blocking = 1 if g.get("blocking") is True else 0
                status = g.get("status")
                nonpass = 1 if status != "PASS" else 0
                gate_id = g.get("gate_id") if isinstance(g.get("gate_id"), str) else "n/a"
                return (-required, -blocking, -nonpass, gate_id)

            gates_norm: List[Dict[str, Any]] = []
            required_failures: List[Dict[str, Any]] = []

            for raw in gates:
                if not isinstance(raw, dict):
                    continue
                rc = raw.get("reason_codes")
                rc_top = rc[:3] if isinstance(rc, list) else []
                artifact_path = raw.get("artifact_path")
                artifact_path_s = artifact_path if isinstance(artifact_path, str) else None

                row = {
                    "gate_id": raw.get("gate_id") if isinstance(raw.get("gate_id"), str) else "n/a",
                    "required": bool(raw.get("required") is True),
                    "blocking": bool(raw.get("blocking") is True),
                    "gate_class": raw.get("gate_class") if isinstance(raw.get("gate_class"), str) else "n/a",
                    "status": raw.get("status") if isinstance(raw.get("status"), str) else "n/a",
                    "artifact_path": artifact_path_s,
                    "reason_codes_top": rc_top,
                }
                gates_norm.append(row)

                if row["required"] and row["status"] != "PASS":
                    required_failures.append(
                        {
                            "gate_id": row["gate_id"],
                            "status": row["status"],
                            "artifact_path": row["artifact_path"],
                            "reason_codes_top": row["reason_codes_top"],
                        }
                    )

            gates_norm.sort(key=gate_rank)
            verdict_obj["gates_top"] = gates_norm[:12]
            verdict_obj["required_failures_top"] = required_failures[:3]
    else:
        verdict_state = "DEGRADED"
        verdict_obj["state"] = "DEGRADED"
        warnings.append("DAY_NOT_RESOLVED")

    # ---- broker reconciliation ----
    broker_obj: Dict[str, Any] = {
        "state": "UNKNOWN",
        "day": verdict_day or "n/a",
        "account": "n/a",
        "artifact_path": None,
        # Evidence fields (truth-only)
        "cash_diff": "n/a",
        "notes_count": 0,
        "position_mismatches_count": 0,
        "mismatches_top": [],  # [{symbol,sec_type,broker_qty,internal_qty,qty_diff}]
    }

    if verdict_day:
        reports_root = truth_root / "reports"
        candidates: List[Tuple[int, Path]] = []
        for v in (3, 2, 1):
            candidates.append(
                (v, reports_root / f"broker_reconciliation_v{v}" / verdict_day / f"broker_reconciliation.v{v}.json")
            )

        chosen_doc: Optional[Dict[str, Any]] = None
        chosen_path: Optional[Path] = None

        for _, p in candidates:
            doc, err = _read_json(p)
            if doc is not None:
                chosen_doc = doc
                chosen_path = p
                note_source(p)
                break
            if err == "MISSING":
                continue
            if err == "UNREADABLE_OR_INVALID_JSON":
                warnings.append("BROKER_RECONCILIATION_UNREADABLE")
                missing_paths.append(str(p))

        if chosen_doc is None:
            broker_obj["state"] = "MISSING"
            warnings.append("BROKER_RECONCILIATION_MISSING")
        else:
            broker_obj["artifact_path"] = str(chosen_path) if chosen_path is not None else None

            st = chosen_doc.get("status")
            if st in ("OK", "PASS", "MATCH"):
                broker_obj["state"] = "OK"
            elif st in ("FAIL", "MISMATCH", "ERROR"):
                broker_obj["state"] = "FAIL"
            else:
                broker_obj["state"] = st if isinstance(st, str) and st else "UNKNOWN"

            acct = chosen_doc.get("account") or chosen_doc.get("account_id") or "n/a"
            broker_obj["account"] = acct if isinstance(acct, str) else "n/a"

            cash_diff = chosen_doc.get("cash_diff")
            notes = chosen_doc.get("notes") or []
            mism = chosen_doc.get("position_mismatches") or []

            broker_obj["cash_diff"] = cash_diff if isinstance(cash_diff, str) else "n/a"
            broker_obj["notes_count"] = len(notes) if isinstance(notes, list) else 0
            broker_obj["position_mismatches_count"] = len(mism) if isinstance(mism, list) else 0

            mism_top: List[Dict[str, Any]] = []
            if isinstance(mism, list):
                for m in mism:
                    if not isinstance(m, dict):
                        continue
                    mism_top.append(
                        {
                            "symbol": m.get("symbol") if isinstance(m.get("symbol"), str) else "n/a",
                            "sec_type": m.get("sec_type") if isinstance(m.get("sec_type"), str) else "n/a",
                            "broker_qty": m.get("broker_qty") if isinstance(m.get("broker_qty"), str) else "n/a",
                            "internal_qty": m.get("internal_qty") if isinstance(m.get("internal_qty"), str) else "n/a",
                            "qty_diff": m.get("qty_diff") if isinstance(m.get("qty_diff"), str) else "n/a",
                        }
                    )
            broker_obj["mismatches_top"] = mism_top[:8]
    else:
        broker_obj["state"] = "UNKNOWN"

    # ---- market data snapshot presence ----
    md_root = truth_root / "market_data_snapshot_v1" / "broker_marks_v1"
    md_days = _list_day_dirs(md_root)
    md_latest = _max_day(md_days)
    if md_latest is None:
        market_obj = {"state": "MISSING", "latest_snapshot_day": "n/a"}
        warnings.append("MARKET_DATA_SNAPSHOT_MISSING")
        missing_paths.append(str(md_root))
    else:
        market_obj = {"state": "PRESENT", "latest_snapshot_day": md_latest}
        note_source(md_root)

    # ---- components ----
    components: List[Dict[str, Any]] = []
    mon_roots = [truth_root / "monitoring_v1", truth_root / "monitoring_v2"]
    seen: set[str] = set()

    def exclude_component(name: str) -> bool:
        if name.startswith("c2_"):
            return True
        if name in ("nav_series",):
            return True
        return False

    for mon_root in mon_roots:
        if not mon_root.exists() or not mon_root.is_dir():
            continue
        for child in sorted(mon_root.iterdir(), key=lambda p: p.name):
            if not child.is_dir():
                continue
            name = child.name
            if name in seen:
                continue
            seen.add(name)

            if exclude_component(name):
                continue

            state = "UNKNOWN"
            reason = "UNKNOWN"
            if verdict_day:
                day_dir = child / verdict_day
                if day_dir.exists() and day_dir.is_dir():
                    state = "PRESENT"
                    reason = "OK"
                else:
                    state = "MISSING"
                    reason = "NO_DAY_DIR"
            else:
                state = "UNKNOWN"
                reason = "DAY_NOT_RESOLVED"

            components.append({"name": name, "state": state, "reason_code": reason})

    # ---- overall state (fail-closed) ----
    if verdict_state == "FAIL":
        overall = "FAIL"
    elif errors:
        overall = "DEGRADED"
    elif warnings:
        overall = "DEGRADED"
    elif verdict_state == "PASS":
        overall = "PASS"
    elif verdict_state == "DEGRADED":
        overall = "DEGRADED"
    else:
        overall = "UNKNOWN"

    return {
        "ok": True,
        "generated_utc": generated,
        "errors": errors,
        "warnings": warnings,
        "missing_paths": missing_paths,
        "source_paths": source_paths,
        "source_mtimes": source_mtimes,
        "schema_version": "C3",
        "generated_at_utc": generated,
        "verdict": verdict_obj,
        "broker_reconciliation": broker_obj,
        "market_data": market_obj,
        "components": components,
        "overall_state": overall,
    }
