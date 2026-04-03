#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
SYSTEM_SNAPSHOT_ROOT = (TRUTH_ROOT / "system_snapshot").resolve()

RUNTIME_STATE_PATH = (SYSTEM_SNAPSHOT_ROOT / "constellation_runtime_state.v1.json").resolve()
ROOT_CAUSE_PATH = (SYSTEM_SNAPSHOT_ROOT / "constellation_root_cause_report.v1.json").resolve()
REPAIR_PLAN_PATH = (SYSTEM_SNAPSHOT_ROOT / "constellation_repair_plan.v1.json").resolve()
POLICY_PATH = (REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_PLATFORM_READINESS_POLICY_V1.json").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/READINESS/platform_readiness.v1.schema.json"


@dataclass
class CheckResult:
    check_id: str
    status: str
    weight: int
    score_awarded: int
    details: dict[str, Any]


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


def _grade_for_score(score: int, bands: list[dict[str, Any]]) -> str:
    sorted_bands = sorted(
        [b for b in bands if isinstance(b, dict) and isinstance(b.get("min_score"), int) and isinstance(b.get("grade"), str)],
        key=lambda b: int(b["min_score"]),
        reverse=True,
    )
    for b in sorted_bands:
        if score >= int(b["min_score"]):
            return str(b["grade"])
    return "UNKNOWN"


def _load_policy() -> dict[str, Any]:
    p = _read_json(POLICY_PATH)
    if str(p.get("schema_id") or "") != "C2_PLATFORM_READINESS_POLICY":
        raise SystemExit("FAIL_CLOSED: platform readiness policy schema_id mismatch")
    if int(p.get("schema_version") or 0) != 1:
        raise SystemExit("FAIL_CLOSED: platform readiness policy schema_version mismatch")
    return p


def _blocker_detail(code: str, evidence_paths: list[str]) -> dict[str, str | None]:
    mapping = {
        "EXECUTION_STATUS_FAIL": ("EXECUTION_BLOCKER", "BLOCKING", "scope_health.sleeve_execution_health", "Execution health must be PASS"),
        "MONITORING_STATUS_FAIL": ("MONITORING_BLOCKER", "ERROR", "scope_health.system_monitoring_health", "Monitoring health must be at least DEGRADED"),
        "GOVERNANCE_INTEGRITY_FAIL": ("READINESS_BLOCKER", "ERROR", "root_cause_report.root_causes", "No governance-integrity ERROR/BLOCKING root causes"),
        "DIAGNOSTICS_DETERMINISM_FAIL": ("READINESS_BLOCKER", "ERROR", "system_snapshot linkage", "Root-cause and repair plan must match current runtime snapshot"),
        "INSUFFICIENT_STABILITY_HISTORY": ("READINESS_BLOCKER", "ERROR", "bug_metrics.window_days_evaluated", "Minimum stability history days must be satisfied"),
        "BUG_VELOCITY_TOO_HIGH": ("READINESS_BLOCKER", "ERROR", "bug_metrics.bug_velocity_7d_avg", "7d bug velocity must be <= policy threshold"),
        "BUG_RECURRENCE_TOO_HIGH": ("READINESS_BLOCKER", "ERROR", "bug_metrics.recurrence_rate", "Bug recurrence rate must be <= policy threshold"),
        "MTTR_UNAVAILABLE_OR_TOO_HIGH": ("READINESS_BLOCKER", "ERROR", "bug_metrics.mttr_hours", "MTTR must be available and <= policy threshold when required"),
        "PLATFORM_SCORE_BELOW_THRESHOLD": ("READINESS_BLOCKER", "WARN", "platform_readiness_score", "Platform score must meet threshold"),
    }
    cls, sev, surface, cond = mapping.get(code, ("READINESS_BLOCKER", "WARN", "UNKNOWN", "Resolve blocker"))
    return {
        "blocker_code": code,
        "blocker_class": cls,
        "severity": sev,
        "affected_surface": surface,
        "exact_evidence_path": evidence_paths[0] if evidence_paths else None,
        "clearance_condition": cond,
    }


def _run(day: str) -> dict[str, Any]:
    policy = _load_policy()
    runtime_state = _read_json(RUNTIME_STATE_PATH)
    root_cause = _read_json(ROOT_CAUSE_PATH)
    repair_plan = _read_json(REPAIR_PLAN_PATH)

    bug_path = (
        TRUTH_ROOT
        / "readiness_v1"
        / "constellation_bug_metrics_v1"
        / day
        / "constellation_bug_metrics.v1.json"
    ).resolve()
    bug_metrics = _read_json(bug_path)
    bug_metric_views = bug_metrics.get("metric_views") if isinstance(bug_metrics.get("metric_views"), dict) else {}

    scoring_cfg = policy.get("scoring") if isinstance(policy.get("scoring"), dict) else {}
    bands = scoring_cfg.get("grade_bands") if isinstance(scoring_cfg.get("grade_bands"), list) else []
    threshold = int(scoring_cfg.get("score_threshold_ready") or 85)
    weights = policy.get("weights") if isinstance(policy.get("weights"), dict) else {}
    hard = policy.get("hard_blockers") if isinstance(policy.get("hard_blockers"), dict) else {}

    w_exec = int(weights.get("execution_integrity") or 20)
    w_mon = int(weights.get("monitoring_integrity") or 20)
    w_gov = int(weights.get("governance_integrity") or 20)
    w_ops = int(weights.get("operator_control_visibility") or 15)
    w_stab = int(weights.get("operational_stability") or 15)
    w_bug = int(weights.get("bug_stability") or 10)

    scope = runtime_state.get("scope_health") if isinstance(runtime_state.get("scope_health"), dict) else {}
    exec_status = _status_norm((scope.get("sleeve_execution_health") or {}).get("status"))
    mon_status = _status_norm((scope.get("system_monitoring_health") or {}).get("status"))

    checks: list[CheckResult] = []
    root_blockers: list[str] = []
    derived_blockers: list[str] = []
    evidence_paths = sorted(
        {
            str(RUNTIME_STATE_PATH.relative_to(REPO_ROOT)),
            str(ROOT_CAUSE_PATH.relative_to(REPO_ROOT)),
            str(REPAIR_PLAN_PATH.relative_to(REPO_ROOT)),
            str(bug_path.relative_to(REPO_ROOT)),
        }
    )

    # 1) Execution integrity
    if exec_status == "PASS":
        checks.append(CheckResult("execution_integrity", "PASS", w_exec, w_exec, {"status": exec_status}))
    elif exec_status == "DEGRADED":
        checks.append(CheckResult("execution_integrity", "FAIL", w_exec, max(0, w_exec // 2), {"status": exec_status}))
        root_blockers.append("EXECUTION_STATUS_FAIL")
    else:
        checks.append(CheckResult("execution_integrity", "FAIL", w_exec, 0, {"status": exec_status}))
        root_blockers.append("EXECUTION_STATUS_FAIL")

    # 2) Monitoring integrity
    mon_min = _status_norm(hard.get("minimum_monitoring_integrity_status"))
    mon_ok = mon_status == "PASS" or (mon_status == "DEGRADED" and mon_min == "DEGRADED")
    mon_score = w_mon if mon_status == "PASS" else max(0, w_mon // 2) if mon_status == "DEGRADED" else 0
    checks.append(CheckResult("monitoring_integrity", "PASS" if mon_ok else "FAIL", w_mon, mon_score, {"status": mon_status, "required_min": mon_min}))
    if not mon_ok or (mon_status == "FAIL" and bool(hard.get("monitoring_status_fail_blocks", True))):
        root_blockers.append("MONITORING_STATUS_FAIL")

    # 3) Governance integrity
    rc_rows = root_cause.get("root_causes") if isinstance(root_cause.get("root_causes"), list) else []
    gov_problem = []
    for rc in rc_rows:
        if not isinstance(rc, dict):
            continue
        code = str(rc.get("root_cause_code") or "")
        sev = str(rc.get("severity") or "INFO").upper()
        if code in {"NON_AUTHORITATIVE_SURFACE_COMPETITION", "INVALID_ARTIFACT_PREFERRED", "QUARANTINE_ACTIVITY_PRESENT"} and sev in {"ERROR", "BLOCKING", "WARN"}:
            gov_problem.append({"code": code, "severity": sev})
    if not gov_problem:
        checks.append(CheckResult("governance_integrity", "PASS", w_gov, w_gov, {"issues": []}))
    else:
        score = max(0, w_gov // 2)
        sev_set = {x["severity"] for x in gov_problem}
        if "ERROR" in sev_set or "BLOCKING" in sev_set:
            score = 0
            root_blockers.append("GOVERNANCE_INTEGRITY_FAIL")
        checks.append(CheckResult("governance_integrity", "FAIL", w_gov, score, {"issues": gov_problem}))

    # 4) Operator control + visibility
    surfaced = repair_plan.get("surfaced_blockers") if isinstance(repair_plan.get("surfaced_blockers"), list) else []
    counts = repair_plan.get("blocker_counts_by_class") if isinstance(repair_plan.get("blocker_counts_by_class"), dict) else {}
    op_ok = isinstance(surfaced, list) and all(k in counts for k in ["EXECUTION_BLOCKER", "MONITORING_BLOCKER", "READINESS_BLOCKER"])
    checks.append(
        CheckResult(
            "operator_control_visibility",
            "PASS" if op_ok else "FAIL",
            w_ops,
            w_ops if op_ok else 0,
            {"surfaced_blocker_count": len(surfaced), "blocker_counts_by_class": counts},
        )
    )
    if not op_ok:
        derived_blockers.append("OPERATOR_VISIBILITY_INCOMPLETE")

    # 5) Operational stability
    min_hist = int(hard.get("minimum_stability_history_days") or 7)
    eval_windows = bug_metrics.get("window_days_evaluated") if isinstance(bug_metrics.get("window_days_evaluated"), dict) else {}
    hist_days = int(eval_windows.get("window_7d") or 0)
    stability_rate = bug_metrics.get("diagnostic_stability_rate")
    if hist_days < min_hist:
        checks.append(CheckResult("operational_stability", "FAIL", w_stab, 0, {"window_7d": hist_days, "required": min_hist}))
        root_blockers.append("INSUFFICIENT_STABILITY_HISTORY")
    elif isinstance(stability_rate, int) and stability_rate >= 6000:
        checks.append(CheckResult("operational_stability", "PASS", w_stab, w_stab, {"diagnostic_stability_rate": stability_rate}))
    elif isinstance(stability_rate, int):
        checks.append(CheckResult("operational_stability", "FAIL", w_stab, max(0, w_stab // 2), {"diagnostic_stability_rate": stability_rate}))
        derived_blockers.append("DIAGNOSTIC_STABILITY_RATE_LOW")
    else:
        checks.append(CheckResult("operational_stability", "UNKNOWN", w_stab, 0, {"diagnostic_stability_rate": stability_rate}))
        root_blockers.append("INSUFFICIENT_STABILITY_HISTORY")

    # 6) Bug stability
    velocity = bug_metrics.get("bug_velocity_7d_avg")
    recurrence = bug_metrics.get("recurrence_rate")
    mttr = bug_metrics.get("mttr_hours")
    max_velocity = int(hard.get("max_bug_velocity_7d_avg_for_candidate") or 100)
    max_recurrence = int(hard.get("max_recurrence_rate_for_candidate") or 5000)
    require_mttr = bool(hard.get("require_mttr_metric"))
    max_mttr = int(hard.get("max_mttr_hours_for_candidate") or 24)

    bug_ok = True
    bug_reasons = []
    if not isinstance(velocity, int):
        bug_ok = False
        bug_reasons.append("BUG_VELOCITY_UNKNOWN")
    elif velocity > max_velocity:
        bug_ok = False
        bug_reasons.append("BUG_VELOCITY_TOO_HIGH")
        root_blockers.append("BUG_VELOCITY_TOO_HIGH")

    if not isinstance(recurrence, int):
        bug_ok = False
        bug_reasons.append("RECURRENCE_RATE_UNKNOWN")
    elif recurrence > max_recurrence:
        bug_ok = False
        bug_reasons.append("BUG_RECURRENCE_TOO_HIGH")
        root_blockers.append("BUG_RECURRENCE_TOO_HIGH")

    if require_mttr:
        if not isinstance(mttr, int) or mttr > max_mttr:
            bug_ok = False
            bug_reasons.append("MTTR_UNAVAILABLE_OR_TOO_HIGH")
            root_blockers.append("MTTR_UNAVAILABLE_OR_TOO_HIGH")

    checks.append(
        CheckResult(
            "bug_stability",
            "PASS" if bug_ok else "FAIL",
            w_bug,
            w_bug if bug_ok else 0,
            {
                "bug_velocity_7d_avg": velocity,
                "recurrence_rate": recurrence,
                "mttr_hours": mttr,
                "unknown_fields": bug_metrics.get("unknown_fields"),
                "policy_thresholds": {
                    "max_bug_velocity_7d_avg_for_candidate": max_velocity,
                    "max_recurrence_rate_for_candidate": max_recurrence,
                    "require_mttr_metric": require_mttr,
                    "max_mttr_hours_for_candidate": max_mttr,
                },
                "reason_codes": bug_reasons,
            },
        )
    )

    # Diagnostics determinism linkage
    if bool(hard.get("require_diagnostics_determinism", True)):
        rc_runtime_utc = str(root_cause.get("runtime_state_generated_utc") or "")
        rs_utc = str(runtime_state.get("generated_utc") or "")
        rp_input = repair_plan.get("inputs") if isinstance(repair_plan.get("inputs"), dict) else {}
        linkage_ok = bool(rc_runtime_utc and rs_utc and rc_runtime_utc == rs_utc and isinstance(rp_input, dict))
        if not linkage_ok:
            root_blockers.append("DIAGNOSTICS_DETERMINISM_FAIL")

    score = sum(c.score_awarded for c in checks)
    grade = _grade_for_score(score, bands)

    root_blockers = sorted(set(root_blockers))
    derived_blockers = sorted(set(derived_blockers))
    if score < threshold:
        derived_blockers.append("PLATFORM_SCORE_BELOW_THRESHOLD")
    derived_blockers = sorted(set(derived_blockers))

    platform_candidate = (len(root_blockers) == 0 and score >= threshold)
    if root_blockers:
        state = "BLOCKED"
    elif platform_candidate:
        state = "READY"
    else:
        state = "NOT_READY"

    top_blockers = root_blockers + [b for b in derived_blockers if b not in root_blockers]

    minimum_conditions_summary = [
        f"execution_integrity=PASS",
        f"monitoring_integrity>={mon_min}",
        "governance_integrity free of ERROR/BLOCKING governance faults",
        f"stability_history_days>={min_hist}",
        f"bug_velocity_7d_avg<={max_velocity}",
        f"recurrence_rate<={max_recurrence}",
        f"platform_score>={threshold}",
    ]

    false_conditions: list[str] = []
    if exec_status != "PASS":
        false_conditions.append(f"execution_integrity status={exec_status}")
    if not mon_ok:
        false_conditions.append(f"monitoring_integrity status={mon_status} required_min={mon_min}")
    if "GOVERNANCE_INTEGRITY_FAIL" in root_blockers:
        false_conditions.append("governance_integrity has unresolved ERROR/BLOCKING issues")
    if "INSUFFICIENT_STABILITY_HISTORY" in root_blockers:
        false_conditions.append(f"stability_history={hist_days}/{min_hist}")
    if "BUG_VELOCITY_TOO_HIGH" in root_blockers:
        false_conditions.append(f"bug_velocity_7d_avg={velocity} threshold={max_velocity}")
    if "BUG_RECURRENCE_TOO_HIGH" in root_blockers:
        false_conditions.append(f"recurrence_rate={recurrence} threshold={max_recurrence}")
    if score < threshold:
        false_conditions.append(f"platform_score={score}/{threshold}")

    summary_parts = []
    if "MONITORING_STATUS_FAIL" in root_blockers:
        summary_parts.append("monitoring integrity failure")
    if "INSUFFICIENT_STABILITY_HISTORY" in root_blockers:
        summary_parts.append(f"insufficient stability history ({hist_days}/{min_hist})")
    if "BUG_VELOCITY_TOO_HIGH" in root_blockers:
        summary_parts.append("bug velocity above threshold")
    if "BUG_RECURRENCE_TOO_HIGH" in root_blockers:
        summary_parts.append("bug recurrence above threshold")
    if not summary_parts and root_blockers:
        summary_parts.append(root_blockers[0])
    if not summary_parts:
        summary_parts.append("all hard conditions satisfied")

    readiness_summary = (
        f"{state} — blocked by " + ", ".join(summary_parts)
        if not platform_candidate
        else "READY — platform hard-gate and score requirements satisfied"
    )

    decision_basis = (
        "Promotion withheld because " + ", ".join(false_conditions)
        if false_conditions
        else "Promotion eligible because all governed platform readiness conditions are satisfied"
    )

    blocker_details = [_blocker_detail(b, evidence_paths) for b in top_blockers]
    smallest_clearance_set = []
    for d in blocker_details:
        cond = str(d.get("clearance_condition") or "").strip()
        if cond and cond not in smallest_clearance_set:
            smallest_clearance_set.append(cond)

    blocker_dependency_order = [
        x
        for x in [
            "execution_integrity",
            "monitoring_integrity",
            "governance_integrity",
            "diagnostics_determinism",
            "operational_stability",
            "bug_stability",
            "platform_score_threshold",
        ]
        if (
            (x == "execution_integrity" and "EXECUTION_STATUS_FAIL" in root_blockers)
            or (x == "monitoring_integrity" and "MONITORING_STATUS_FAIL" in root_blockers)
            or (x == "governance_integrity" and "GOVERNANCE_INTEGRITY_FAIL" in root_blockers)
            or (x == "diagnostics_determinism" and "DIAGNOSTICS_DETERMINISM_FAIL" in root_blockers)
            or (x == "operational_stability" and "INSUFFICIENT_STABILITY_HISTORY" in root_blockers)
            or (x == "bug_stability" and ("BUG_VELOCITY_TOO_HIGH" in root_blockers or "BUG_RECURRENCE_TOO_HIGH" in root_blockers or "MTTR_UNAVAILABLE_OR_TOO_HIGH" in root_blockers))
            or (x == "platform_score_threshold" and score < threshold)
        )
    ]

    velocity_display = ((bug_metric_views.get("bug_velocity_7d_avg") or {}).get("display_value") if isinstance(bug_metric_views.get("bug_velocity_7d_avg"), dict) else None) or "UNKNOWN"
    recurrence_display = ((bug_metric_views.get("recurrence_rate") or {}).get("display_value") if isinstance(bug_metric_views.get("recurrence_rate"), dict) else None) or "UNKNOWN"
    stability_display = ((bug_metric_views.get("diagnostic_stability_rate") or {}).get("display_value") if isinstance(bug_metric_views.get("diagnostic_stability_rate"), dict) else None) or "UNKNOWN"
    bug_stability_summary = (
        f"new_bug_events_today={bug_metrics.get('new_bug_events_today')} "
        f"velocity_7d={velocity_display} "
        f"recurrence={recurrence_display} "
        f"diagnostic_stability={stability_display} "
        f"trend={bug_metrics.get('bug_velocity_trend') or 'UNKNOWN'}"
    )

    score_contribution = [
        {"check_id": c.check_id, "weight": c.weight, "score_awarded": c.score_awarded, "status": c.status}
        for c in checks
    ]

    current_vs_required = {
        "execution": {"current": exec_status, "required": "PASS", "ok": exec_status == "PASS"},
        "monitoring": {"current": mon_status, "required_min": mon_min, "ok": mon_ok},
        "history_days": {"current": hist_days, "required": min_hist, "ok": hist_days >= min_hist},
        "bug_velocity_7d_avg": {
            "current": velocity,
            "required_max": max_velocity,
            "ok": isinstance(velocity, int) and velocity <= max_velocity,
        },
        "recurrence_rate": {
            "current": recurrence,
            "required_max": max_recurrence,
            "ok": isinstance(recurrence, int) and recurrence <= max_recurrence,
        },
        "score": {"current": score, "required": threshold, "ok": score >= threshold},
    }

    promotion_checklist = {
        "must_be_true": minimum_conditions_summary,
        "currently_false": false_conditions,
        "gating_conditions": smallest_clearance_set,
        "informational_conditions": [
            f"platform_readiness_grade={grade}",
            f"platform_readiness_score={score}/{threshold}",
            f"derived_blockers={len(derived_blockers)}",
            f"bug_stability_summary={bug_stability_summary}",
        ],
    }

    out = {
        "schema_id": "C2_PLATFORM_READINESS_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "platform_readiness_state": state,
        "platform_readiness_score": int(score),
        "platform_readiness_grade": grade,
        "score_threshold_ready": int(threshold),
        "platform_promotion_candidate": bool(platform_candidate),
        "root_blockers": root_blockers,
        "derived_blockers": derived_blockers,
        "aggregate_blocker_summary": {
            "root_blocker_count": len(root_blockers),
            "derived_blocker_count": len(derived_blockers),
            "total_blocker_count": len(root_blockers) + len(derived_blockers),
            "promotion_candidate": bool(platform_candidate),
        },
        "readiness_summary": readiness_summary,
        "promotion_decision_basis": decision_basis,
        "top_blockers_ordered": top_blockers,
        "minimum_conditions_summary": minimum_conditions_summary,
        "current_vs_required": current_vs_required,
        "promotion_checklist": promotion_checklist,
        "smallest_clearance_set": smallest_clearance_set,
        "blocker_dependency_order": blocker_dependency_order,
        "score_contribution": score_contribution,
        "metric_views": {
            "platform_readiness_score": {
                "raw_value": int(score),
                "unit": "score_points_0_to_100",
                "display_value": f"{int(score)} points",
            },
            "score_threshold_ready": {
                "raw_value": int(threshold),
                "unit": "score_points_0_to_100",
                "display_value": f"{int(threshold)} points",
            },
            "bug_velocity_7d_avg": {
                "raw_value": velocity if isinstance(velocity, int) else None,
                "unit": "events_per_day_x100",
                "display_value": velocity_display,
            },
            "recurrence_rate": {
                "raw_value": recurrence if isinstance(recurrence, int) else None,
                "unit": "basis_points",
                "display_value": recurrence_display,
            },
            "diagnostic_stability_rate": {
                "raw_value": stability_rate if isinstance(stability_rate, int) else None,
                "unit": "basis_points",
                "display_value": stability_display,
            },
        },
        "policy_values": {
            "policy_path": str(POLICY_PATH.relative_to(REPO_ROOT)),
            "policy_sha256": _sha256_file(POLICY_PATH),
            "score_threshold_ready": threshold,
            "weights": {
                "execution_integrity": w_exec,
                "monitoring_integrity": w_mon,
                "governance_integrity": w_gov,
                "operator_control_visibility": w_ops,
                "operational_stability": w_stab,
                "bug_stability": w_bug,
            },
            "hard_blockers": hard,
        },
        "evidence_paths": evidence_paths,
        "bug_stability_summary": bug_stability_summary,
        "calibration_support": {
            "policy_values": {
                "score_threshold_ready": threshold,
                "hard_blockers": hard,
            },
            "platform_outcome": {
                "state": state,
                "score": int(score),
                "grade": grade,
                "promotion_candidate": bool(platform_candidate),
            },
            "failed_checks": [
                {
                    "check_id": c.check_id,
                    "status": c.status,
                    "weight": c.weight,
                    "score_awarded": c.score_awarded,
                    "details": c.details,
                }
                for c in checks
                if c.status != "PASS"
            ],
            "score_contribution": score_contribution,
            "blocker_details": blocker_details,
        },
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
        "schema_id": "C2_PLATFORM_READINESS_POINTER_V1",
        "schema_version": 1,
        "target_path": str(target_path),
        "target_sha256": _sha256_file(target_path),
    }
    _write_json_atomic(path, pointer)


def main() -> None:
    ap = argparse.ArgumentParser(prog="run_constellation_platform_readiness_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL_CLOSED: invalid day_utc: {day}")

    out_obj = _run(day)

    out_path = (
        TRUTH_ROOT
        / "readiness_v1"
        / "constellation_platform_readiness_v1"
        / day
        / "constellation_platform_readiness.v1.json"
    ).resolve()
    _write_json_atomic(out_path, out_obj)

    pointer_path = (
        TRUTH_ROOT
        / "readiness_v1"
        / "constellation_platform_readiness_v1"
        / "latest_pointer.v1.json"
    ).resolve()
    _write_pointer(pointer_path, out_path)

    print(f"OK: CONSTELLATION_PLATFORM_READINESS_V1_WRITTEN day_utc={day} path={out_path}")
    print(f"OK: CONSTELLATION_PLATFORM_READINESS_V1_POINTER path={pointer_path}")


if __name__ == "__main__":
    main()
