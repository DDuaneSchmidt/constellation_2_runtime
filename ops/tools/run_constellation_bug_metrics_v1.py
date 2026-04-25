#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.release_baseline_common_v1 import resolve_release_baseline_roots_v1  # noqa: E402


ROOTS = resolve_release_baseline_roots_v1(REPO_ROOT)
REPO_ROOT = ROOTS.repo_root
TRUTH_ROOT = ROOTS.canonical_truth_root
SYSTEM_SNAPSHOT_ROOT = ROOTS.system_snapshot_root
POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_PLATFORM_READINESS_POLICY_V1.json").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/READINESS/bug_metrics.v1.schema.json"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise SystemExit(f"FAIL_CLOSED: missing required file: {path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"FAIL_CLOSED: cannot parse json {path}: {exc}")
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL_CLOSED: top-level json must be object: {path}")
    return obj


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _status_norm(v: Any) -> str:
    s = str(v or "").strip().upper()
    if s in {"PASS", "OK", "SUCCESS", "ACTIVE"}:
        return "PASS"
    if s in {"DEGRADED", "PARTIALLY_PROVEN"}:
        return "DEGRADED"
    if s in {"FAIL", "ABORTED", "ERROR", "BLOCKING"}:
        return "FAIL"
    return "UNKNOWN"


def _parse_day(day: str) -> datetime:
    return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _iter_days_back(day: str, lookback_days: int) -> list[str]:
    d0 = _parse_day(day)
    out = []
    for i in range(lookback_days):
        out.append((d0 - timedelta(days=i)).strftime("%Y-%m-%d"))
    return sorted(out)


def _latest_json_in_dir(day_dir: Path) -> Path | None:
    if not day_dir.exists() or not day_dir.is_dir():
        return None
    candidates = sorted([p for p in day_dir.rglob("*.json") if p.is_file()], key=lambda p: str(p))
    if not candidates:
        return None
    return candidates[-1]


def _load_policy() -> dict[str, Any]:
    obj = _read_json(POLICY_PATH)
    if str(obj.get("schema_id") or "") != "C2_PLATFORM_READINESS_POLICY":
        raise SystemExit("FAIL_CLOSED: platform readiness policy schema_id mismatch")
    if int(obj.get("schema_version") or 0) != 1:
        raise SystemExit("FAIL_CLOSED: platform readiness policy schema_version mismatch")
    return obj


def _append_event(events: list[dict[str, Any]], day: str, root_cause_class: str, surface: str, reason_code: str, severity: str, evidence_path: str) -> None:
    events.append(
        {
            "day_utc": day,
            "root_cause_class": root_cause_class,
            "affected_surface": surface,
            "reason_code": reason_code,
            "severity": severity,
            "evidence_path": evidence_path,
        }
    )


def _collect_monitoring_events(day: str, events: list[dict[str, Any]], evidence_paths: set[str]) -> None:
    lifecycle_path = (
        TRUTH_ROOT
        / "monitoring_v1"
        / "lifecycle_monitor"
        / day
        / "lifecycle_monitor_report.v1.json"
    ).resolve()
    if lifecycle_path.exists() and lifecycle_path.is_file():
        obj = _read_json(lifecycle_path)
        st = _status_norm(obj.get("status"))
        if st in {"FAIL", "DEGRADED"}:
            reasons = obj.get("reason_codes") if isinstance(obj.get("reason_codes"), list) else []
            if not reasons:
                reasons = [f"LIFECYCLE_MONITOR_{st}"]
            for rc in reasons:
                _append_event(
                    events,
                    day,
                    "MONITORING_SURFACE_FAIL",
                    "lifecycle_monitor",
                    str(rc),
                    "ERROR" if st == "FAIL" else "WARN",
                    str(lifecycle_path),
                )
        evidence_paths.add(str(lifecycle_path))

    paper_path = (
        TRUTH_ROOT
        / "monitoring_v1"
        / "paper_readiness"
        / day
        / "paper_readiness_report.v1.json"
    ).resolve()
    if paper_path.exists() and paper_path.is_file():
        obj = _read_json(paper_path)
        st = _status_norm(obj.get("status"))
        if st in {"FAIL", "DEGRADED"}:
            reasons = obj.get("reason_codes") if isinstance(obj.get("reason_codes"), list) else []
            if not reasons:
                reasons = [f"PAPER_READINESS_{st}"]
            for rc in reasons:
                _append_event(
                    events,
                    day,
                    "MONITORING_SURFACE_FAIL",
                    "paper_readiness",
                    str(rc),
                    "ERROR" if st == "FAIL" else "WARN",
                    str(paper_path),
                )
        evidence_paths.add(str(paper_path))


def _collect_orchestrator_events(day: str, events: list[dict[str, Any]], evidence_paths: set[str]) -> None:
    run_day_dir = (TRUTH_ROOT / "reports" / "orchestrator_run_verdict_v2" / day).resolve()
    p = _latest_json_in_dir(run_day_dir)
    if p is None:
        return
    obj = _read_json(p)
    st = _status_norm(obj.get("status") or obj.get("state"))
    if st in {"FAIL", "DEGRADED"}:
        reasons = obj.get("reason_codes") if isinstance(obj.get("reason_codes"), list) else []
        if not reasons:
            reasons = [f"ORCHESTRATOR_{st}"]
        for rc in reasons:
            _append_event(
                events,
                day,
                "EXECUTION_VERDICT_NON_PASS",
                "orchestrator_run_verdict_v2",
                str(rc),
                "ERROR" if st == "FAIL" else "WARN",
                str(p),
            )
    evidence_paths.add(str(p))


def _collect_root_cause_events(day: str, events: list[dict[str, Any]], evidence_paths: set[str]) -> None:
    root_path = (SYSTEM_SNAPSHOT_ROOT / "constellation_root_cause_report.v1.json").resolve()
    obj = _read_json(root_path)
    if str(obj.get("latest_operating_day") or "") != day:
        evidence_paths.add(str(root_path))
        return
    rows = obj.get("root_causes") if isinstance(obj.get("root_causes"), list) else []
    for rc in rows:
        if not isinstance(rc, dict):
            continue
        code = str(rc.get("root_cause_code") or "").strip()
        if not code:
            continue
        sev = str(rc.get("severity") or "WARN").upper()
        _append_event(
            events,
            day,
            code,
            "constellation_root_cause_report",
            code,
            sev if sev in {"INFO", "WARN", "ERROR", "BLOCKING"} else "WARN",
            str(root_path),
        )
    evidence_paths.add(str(root_path))


def _bug_velocity_for_window(day_counts: dict[str, int], ordered_days: list[str], window_days: int) -> tuple[int | None, int]:
    sub = ordered_days[-window_days:] if len(ordered_days) >= window_days else ordered_days
    if len(sub) < window_days:
        return (None, len(sub))
    total = sum(day_counts.get(d, 0) for d in sub)
    return (int(round((total * 100) / window_days)), len(sub))


def _fmt_x100_as_per_day(v: int | None) -> str:
    if not isinstance(v, int):
        return "UNKNOWN"
    return f"{v // 100}.{v % 100:02d} bugs/day"


def _fmt_bps_as_percent(v: int | None) -> str:
    if not isinstance(v, int):
        return "UNKNOWN"
    return f"{v // 100}.{v % 100:02d}%"


def _run(day: str) -> dict[str, Any]:
    policy = _load_policy()
    bug_cfg = policy.get("bug_metrics") if isinstance(policy.get("bug_metrics"), dict) else {}
    hist_cfg = bug_cfg.get("history_windows") if isinstance(bug_cfg.get("history_windows"), dict) else {}
    w7 = int(hist_cfg.get("velocity_7d_days") or 7)
    w14 = int(hist_cfg.get("velocity_14d_days") or 14)

    all_days = _iter_days_back(day, max(w14, w7))
    events: list[dict[str, Any]] = []
    evidence_paths: set[str] = set()

    for d in all_days:
        _collect_monitoring_events(d, events, evidence_paths)
        _collect_orchestrator_events(d, events, evidence_paths)

    _collect_root_cause_events(day, events, evidence_paths)

    day_counts = {d: 0 for d in all_days}
    for ev in events:
        d = str(ev.get("day_utc") or "")
        if d in day_counts:
            day_counts[d] += 1

    v7, observed_7 = _bug_velocity_for_window(day_counts, all_days, w7)
    v14, observed_14 = _bug_velocity_for_window(day_counts, all_days, w14)

    recurrence_counts: dict[str, int] = {}
    recurrence_window_days = all_days[-w14:] if observed_14 >= w14 else all_days[-w7:]
    for ev in events:
        d = str(ev.get("day_utc") or "")
        if d not in recurrence_window_days:
            continue
        key = "|".join(
            [
                str(ev.get("root_cause_class") or "UNKNOWN"),
                str(ev.get("affected_surface") or "UNKNOWN"),
                str(ev.get("reason_code") or "UNKNOWN"),
            ]
        )
        recurrence_counts[key] = recurrence_counts.get(key, 0) + 1

    recurring = sorted(
        [{"recurrence_key": k, "count": c} for k, c in recurrence_counts.items() if c > 1],
        key=lambda x: (-int(x["count"]), str(x["recurrence_key"])),
    )

    total_recurrence_events = sum(recurrence_counts.values())
    recurring_instances = sum(int(x["count"]) for x in recurring)
    recurrence_rate = None
    if total_recurrence_events > 0:
        recurrence_rate = int(round((recurring_instances * 10000) / total_recurrence_events))

    unknown_fields: list[str] = []

    if v7 is None:
        unknown_fields.append("bug_velocity_7d_avg")
    if v14 is None:
        unknown_fields.append("bug_velocity_14d_avg")

    trend: str | None = None
    if v14 is not None and len(all_days) >= (2 * w7):
        last7 = all_days[-w7:]
        prev7 = all_days[-(2 * w7):-w7]
        avg_last7 = int(round((sum(day_counts[d] for d in last7) * 100) / w7))
        avg_prev7 = int(round((sum(day_counts[d] for d in prev7) * 100) / w7))
        if avg_last7 < avg_prev7:
            trend = "IMPROVING"
        elif avg_last7 > avg_prev7:
            trend = "WORSENING"
        else:
            trend = "STABLE"
    else:
        trend = "UNKNOWN"
        unknown_fields.append("bug_velocity_trend")

    mttr_hours = None
    median_ttr_hours = None
    half_life_days = None
    unknown_fields.extend(["mttr_hours", "median_ttr_hours", "bug_half_life_estimate_days"])

    if observed_7 >= w7:
        stable_days = sum(1 for d in all_days[-w7:] if day_counts.get(d, 0) == 0)
        stability_rate = int(round((stable_days * 10000) / w7))
    else:
        stability_rate = None
        unknown_fields.append("diagnostic_stability_rate")

    out = {
        "schema_id": "C2_BUG_METRICS_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "window_days_evaluated": {
            "window_7d": int(observed_7),
            "window_14d": int(observed_14),
        },
        "new_bug_events_today": int(day_counts.get(day, 0)),
        "bug_velocity_7d_avg": v7,
        "bug_velocity_14d_avg": v14,
        "recurring_bug_events": recurring,
        "recurrence_rate": recurrence_rate,
        "mttr_hours": mttr_hours,
        "median_ttr_hours": median_ttr_hours,
        "bug_velocity_trend": trend,
        "bug_half_life_estimate_days": half_life_days,
        "diagnostic_stability_rate": stability_rate,
        "metric_views": {
            "new_bug_events_today": {
                "raw_value": int(day_counts.get(day, 0)),
                "unit": "count",
                "display_value": f"{int(day_counts.get(day, 0))} events",
            },
            "bug_velocity_7d_avg": {
                "raw_value": v7,
                "unit": "events_per_day_x100",
                "display_value": _fmt_x100_as_per_day(v7),
            },
            "bug_velocity_14d_avg": {
                "raw_value": v14,
                "unit": "events_per_day_x100",
                "display_value": _fmt_x100_as_per_day(v14),
            },
            "recurrence_rate": {
                "raw_value": recurrence_rate,
                "unit": "basis_points",
                "display_value": _fmt_bps_as_percent(recurrence_rate),
            },
            "diagnostic_stability_rate": {
                "raw_value": stability_rate,
                "unit": "basis_points",
                "display_value": _fmt_bps_as_percent(stability_rate),
            },
            "mttr_hours": {
                "raw_value": mttr_hours,
                "unit": "hours",
                "display_value": "UNKNOWN",
            },
            "median_ttr_hours": {
                "raw_value": median_ttr_hours,
                "unit": "hours",
                "display_value": "UNKNOWN",
            },
            "bug_half_life_estimate_days": {
                "raw_value": half_life_days,
                "unit": "days",
                "display_value": "UNKNOWN",
            },
        },
        "calculation_summary": {
            "new_bug_events_today_basis": "count of classified bug events for day_utc in evidence window",
            "bug_velocity_basis": "sum(event_count over window_days) * 100 / window_days",
            "recurrence_rate_basis": "sum(recurring_key_counts where count>1) * 10000 / total_recurrence_events",
            "diagnostic_stability_basis": "count(days with zero events in last 7d) * 10000 / 7",
            "window_days_used": {
                "velocity_7d_days": int(observed_7),
                "velocity_14d_days": int(observed_14),
                "recurrence_days": int(len(recurrence_window_days)),
            },
        },
        "event_counts_by_day": [{"day_utc": d, "event_count": int(day_counts.get(d, 0))} for d in all_days],
        "evidence_paths": sorted(evidence_paths),
        "unknown_fields": sorted(set(unknown_fields)),
    }
    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA_RELPATH)
    return out


def _write_json_atomic(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(payload)
    os.replace(tmp, path)


def _write_pointer(path: Path, target_path: Path) -> None:
    pointer = {
        "schema_id": "C2_BUG_METRICS_POINTER_V1",
        "schema_version": 1,
        "target_path": str(target_path),
        "target_sha256": _sha256_file(target_path),
    }
    _write_json_atomic(path, pointer)


def main() -> None:
    ap = argparse.ArgumentParser(prog="run_constellation_bug_metrics_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL_CLOSED: invalid day_utc: {day}")

    out_obj = _run(day)

    out_path = (
        TRUTH_ROOT
        / "readiness_v1"
        / "constellation_bug_metrics_v1"
        / day
        / "constellation_bug_metrics.v1.json"
    ).resolve()
    _write_json_atomic(out_path, out_obj)

    pointer_path = (
        TRUTH_ROOT
        / "readiness_v1"
        / "constellation_bug_metrics_v1"
        / "latest_pointer.v1.json"
    ).resolve()
    _write_pointer(pointer_path, out_path)

    print(f"OK: CONSTELLATION_BUG_METRICS_V1_WRITTEN day_utc={day} path={out_path}")
    print(f"OK: CONSTELLATION_BUG_METRICS_V1_POINTER path={pointer_path}")


if __name__ == "__main__":
    main()
