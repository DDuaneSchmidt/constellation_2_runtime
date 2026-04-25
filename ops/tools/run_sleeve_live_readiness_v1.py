#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
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
RUNTIME_STATE_PATH = (ROOTS.system_snapshot_root / "constellation_runtime_state.v1.json").resolve()

SLEEVE_REGISTRY = (REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_LIVE_READINESS_POLICY_V1.json").resolve()
FRESHNESS_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_DIAGNOSTICS_FRESHNESS_POLICY_V1.json").resolve()
LIFECYCLE_CLASSIFICATION_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_LIFECYCLE_DEPENDENCY_CLASSIFICATION_V1.json").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/READINESS/sleeve_live_readiness.v1.schema.json"


@dataclass
class CheckResult:
    check_id: str
    required: bool
    status: str
    weight: int
    score_awarded: int
    details: dict[str, Any]


DATE_FMT = "%Y-%m-%d"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DENY_NAME_TOKENS = (".INVALID_", ".QUARANTINED_")
DENY_DIR_PREFIXES = ("__quarantine", "__quarantined", "__archived")


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


def _resolve_sleeve_truth_root(sleeve_id: str, mode: str, truth_root_arg: str) -> Path:
    if truth_root_arg.strip():
        p = Path(truth_root_arg).resolve()
        if not p.exists() or not p.is_dir():
            raise SystemExit(f"FAIL_CLOSED: provided truth_root missing/invalid: {p}")
        return p

    env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env_root:
        p = Path(env_root).resolve()
        if not p.exists() or not p.is_dir():
            raise SystemExit(f"FAIL_CLOSED: C2_TRUTH_ROOT missing/invalid: {p}")
        return p

    reg = _read_json(SLEEVE_REGISTRY)
    sleeves = reg.get("sleeves")
    if not isinstance(sleeves, list):
        raise SystemExit("FAIL_CLOSED: invalid sleeve registry structure")

    for s in sleeves:
        if not isinstance(s, dict):
            continue
        if str(s.get("sleeve_id") or "") != sleeve_id:
            continue
        if str(s.get("mode") or "").upper() != mode:
            continue
        if not bool(s.get("enabled")):
            raise SystemExit(f"FAIL_CLOSED: sleeve is disabled: {sleeve_id}/{mode}")
        rel = str(s.get("truth_partition") or "").strip()
        if not rel:
            raise SystemExit(f"FAIL_CLOSED: sleeve truth partition missing: {sleeve_id}/{mode}")
        p = (ROOTS.truth_sleeves_root / rel).resolve()
        if not p.exists() or not p.is_dir():
            raise SystemExit(f"FAIL_CLOSED: sleeve truth partition not found: {p}")
        return p

    raise SystemExit(f"FAIL_CLOSED: no sleeve registry binding for {sleeve_id}/{mode}")


def _latest_verdict_for_day(sleeve_truth_root: Path, day: str) -> dict[str, Any] | None:
    root = (sleeve_truth_root / "reports" / "orchestrator_run_verdict_v2" / day).resolve()
    if not root.exists() or not root.is_dir():
        return None
    candidates = sorted(root.glob("*/orchestrator_run_verdict.v2.json"), key=lambda p: str(p))
    if not candidates:
        return None
    p = candidates[-1]
    obj = _read_json(p)
    return {"path": str(p), "doc": obj}


def _iter_days_back(day: str, lookback_days: int) -> list[str]:
    d0 = datetime.strptime(day, DATE_FMT)
    out = []
    for i in range(lookback_days):
        out.append((d0 - timedelta(days=i)).strftime(DATE_FMT))
    return sorted(out)


def _status_norm(v: Any) -> str:
    s = str(v or "").strip().upper()
    if s in {"PASS", "OK", "SUCCESS", "ACTIVE"}:
        return "PASS"
    if s in {"DEGRADED", "PARTIALLY_PROVEN"}:
        return "DEGRADED"
    if s in {"FAIL", "BLOCKING", "ABORTED", "ERROR"}:
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
    if str(p.get("schema_id") or "") != "C2_SLEEVE_LIVE_READINESS_POLICY":
        raise SystemExit("FAIL_CLOSED: live readiness policy schema_id mismatch")
    if int(p.get("schema_version") or 0) != 1:
        raise SystemExit("FAIL_CLOSED: live readiness policy schema_version mismatch")
    return p


def _load_lifecycle_dependency_governance() -> dict[str, Any]:
    obj = _read_json(LIFECYCLE_CLASSIFICATION_PATH)
    if str(obj.get("schema_id") or "") != "C2_LIFECYCLE_DEPENDENCY_CLASSIFICATION":
        raise SystemExit("FAIL_CLOSED: lifecycle dependency classification schema_id mismatch")
    if int(obj.get("schema_version") or 0) != 1:
        raise SystemExit("FAIL_CLOSED: lifecycle dependency classification schema_version mismatch")
    deps = obj.get("dependencies")
    if not isinstance(deps, list) or not deps:
        raise SystemExit("FAIL_CLOSED: lifecycle dependency classification dependencies missing/empty")
    return obj


def _runtime_lifecycle_monitor_surface() -> dict[str, Any]:
    if not RUNTIME_STATE_PATH.exists() or not RUNTIME_STATE_PATH.is_file():
        return {}
    obj = _read_json(RUNTIME_STATE_PATH)
    scope = obj.get("scope_health") if isinstance(obj.get("scope_health"), dict) else {}
    mon = scope.get("system_monitoring_health") if isinstance(scope.get("system_monitoring_health"), dict) else {}
    fresh = mon.get("freshness") if isinstance(mon.get("freshness"), dict) else {}
    results = fresh.get("surface_results") if isinstance(fresh.get("surface_results"), list) else []
    for r in results:
        if not isinstance(r, dict):
            continue
        if str(r.get("surface_id") or "") == "lifecycle_monitor":
            return r
    return {}


def _safe_status_from_path(path: Path) -> tuple[str, dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return ("UNKNOWN", {"path": str(path), "exists": False})
    obj = _read_json(path)
    return (_status_norm(obj.get("status")), {"path": str(path), "exists": True, "status_raw": obj.get("status")})


def _parse_day(day: str) -> datetime:
    return datetime.strptime(day, DATE_FMT).replace(tzinfo=timezone.utc)


def _eval_day_freshness(*, policy: str, ref_day: str, actual_day: str | None) -> tuple[bool, str | None]:
    if actual_day is None:
        return (False, "MISSING_DAY")
    if policy == "SAME_DAY_REQUIRED":
        return (actual_day == ref_day, None if actual_day == ref_day else f"EXPECT_SAME_DAY:{ref_day}:GOT:{actual_day}")
    if policy == "PREVIOUS_DAY_ACCEPTABLE":
        ref_dt = _parse_day(ref_day)
        min_dt = ref_dt - timedelta(days=1)
        act_dt = _parse_day(actual_day)
        ok = min_dt <= act_dt <= ref_dt
        return (ok, None if ok else f"EXPECT_PREV_OR_SAME:{ref_day}:GOT:{actual_day}")
    if policy == "PER_RUN_REQUIRED":
        return (actual_day == ref_day, None if actual_day == ref_day else f"EXPECT_PER_RUN_DAY:{ref_day}:GOT:{actual_day}")
    return (False, f"UNKNOWN_FRESHNESS_POLICY:{policy}")


def _latest_json_in_stream(rel_path: str) -> tuple[str | None, Path | None]:
    root = (REPO_ROOT / rel_path).resolve()
    if not root.exists() or not root.is_dir():
        return (None, None)
    candidates: list[tuple[str, Path]] = []
    for p in root.rglob("*.json"):
        if not p.is_file():
            continue
        if any(tok in p.name for tok in DENY_NAME_TOKENS):
            continue
        if any(part.startswith(DENY_DIR_PREFIXES) for part in p.parts):
            continue
        day = None
        for part in p.parts:
            if DATE_RE.match(part):
                day = part
        if day is None:
            continue
        candidates.append((day, p))
    if not candidates:
        return (None, None)
    candidates.sort(key=lambda x: (x[0], str(x[1])))
    return candidates[-1]


def _run(day: str, sleeve_id: str, mode: str, sleeve_truth_root: Path) -> dict[str, Any]:
    policy = _load_policy()
    lifecycle_dep_gov = _load_lifecycle_dependency_governance()
    lifecycle_surface_runtime = _runtime_lifecycle_monitor_surface()
    history_cfg = policy.get("history") if isinstance(policy.get("history"), dict) else {}
    scoring_cfg = policy.get("scoring") if isinstance(policy.get("scoring"), dict) else {}
    weights = policy.get("weights") if isinstance(policy.get("weights"), dict) else {}

    lookback_days = int(history_cfg.get("lookback_days") or 15)
    min_pass_days = int(history_cfg.get("min_pass_days") or 5)
    count_statuses = [str(x).upper() for x in (history_cfg.get("count_statuses") or ["PASS"]) if isinstance(x, str)]

    score_threshold = int(scoring_cfg.get("score_threshold_ready") or 85)
    grade_bands = scoring_cfg.get("grade_bands") if isinstance(scoring_cfg.get("grade_bands"), list) else []

    history_weight = int(weights.get("history_window") or 30)
    hard_gates_weight = int(weights.get("hard_gates") or 30)
    freshness_weight = int(weights.get("freshness_compliance") or 20)
    safety_weight = int(weights.get("safety_control_clear") or 20)

    checks: list[CheckResult] = []
    reasons: list[str] = []
    promotion_blockers: list[str] = []
    evidence: list[dict[str, Any]] = []

    # 1) History window criterion
    pass_days = 0
    observed: list[dict[str, Any]] = []
    for d in _iter_days_back(day, lookback_days):
        v = _latest_verdict_for_day(sleeve_truth_root, d)
        if v is None:
            continue
        st = _status_norm(v["doc"].get("status"))
        observed.append({"day": d, "status": st, "path": v["path"]})
        evidence.append({"path": v["path"], "sha256": _sha256_file(Path(v["path"])), "note": f"verdict_{d}"})
        if st in count_statuses:
            pass_days += 1

    history_ok = pass_days >= min_pass_days
    if not history_ok:
        promotion_blockers.append(f"INSUFFICIENT_PASS_HISTORY:{pass_days}<{min_pass_days}")
    checks.append(
        CheckResult(
            check_id="history_window",
            required=True,
            status="PASS" if history_ok else "FAIL",
            weight=history_weight,
            score_awarded=history_weight if history_ok else 0,
            details={
                "lookback_days": lookback_days,
                "min_pass_days": min_pass_days,
                "count_statuses": count_statuses,
                "pass_days": pass_days,
                "observed_days": observed,
            },
        )
    )

    # 2) Hard gates for target day
    hard_req = policy.get("hard_gate_requirements") if isinstance(policy.get("hard_gate_requirements"), dict) else {}
    gate_stack_req = _status_norm(hard_req.get("gate_stack_status") or "PASS")
    exec_ready_req = _status_norm(hard_req.get("execution_readiness_status") or "PASS")

    gate_stack_path = (sleeve_truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json").resolve()
    exec_ready_path = (sleeve_truth_root / "reports" / "execution_readiness_gate_v1" / day / "execution_readiness_gate.v1.json").resolve()
    gate_stack_st, gate_stack_details = _safe_status_from_path(gate_stack_path)
    exec_ready_st, exec_ready_details = _safe_status_from_path(exec_ready_path)

    hard_ok = gate_stack_st == gate_stack_req and exec_ready_st == exec_ready_req
    if not hard_ok:
        promotion_blockers.append(f"HARD_GATES_NOT_PASS:gate_stack={gate_stack_st},execution_readiness={exec_ready_st}")
    checks.append(
        CheckResult(
            check_id="hard_gates",
            required=True,
            status="PASS" if hard_ok else "FAIL",
            weight=hard_gates_weight,
            score_awarded=hard_gates_weight if hard_ok else 0,
            details={
                "required": {
                    "gate_stack_status": gate_stack_req,
                    "execution_readiness_status": exec_ready_req,
                },
                "actual": {
                    "gate_stack_status": gate_stack_st,
                    "execution_readiness_status": exec_ready_st,
                },
                "artifacts": {
                    "gate_stack": gate_stack_details,
                    "execution_readiness": exec_ready_details,
                },
            },
        )
    )
    if gate_stack_path.exists():
        evidence.append({"path": str(gate_stack_path), "sha256": _sha256_file(gate_stack_path), "note": "gate_stack"})
    if exec_ready_path.exists():
        evidence.append({"path": str(exec_ready_path), "sha256": _sha256_file(exec_ready_path), "note": "execution_readiness"})

    # 3) Freshness compliance from governed monitoring artifacts only.
    freshness_cfg = policy.get("freshness_requirements") if isinstance(policy.get("freshness_requirements"), dict) else {}
    freshness_policy_rel = str(freshness_cfg.get("diagnostics_policy_path") or str(FRESHNESS_POLICY_PATH.relative_to(REPO_ROOT))).strip()
    freshness_policy_path = (REPO_ROOT / freshness_policy_rel).resolve()
    freshness_policy = _read_json(freshness_policy_path)
    if str(freshness_policy.get("schema_id") or "") != "C2_DIAGNOSTICS_FRESHNESS_POLICY":
        raise SystemExit("FAIL_CLOSED: diagnostics freshness policy schema_id mismatch")

    required_surfaces = {"paper_readiness", "lifecycle_monitor", "capital_authority_allocation"}
    mon_reasons: list[str] = []
    mon_details: list[dict[str, Any]] = []
    for surface in freshness_policy.get("surfaces", []):
        if not isinstance(surface, dict):
            continue
        sid = str(surface.get("surface_id") or "").strip()
        if sid not in required_surfaces:
            continue
        s_type = str(surface.get("surface_type") or "").strip().upper()
        rel_path = str(surface.get("path") or "").strip()
        status_field = str(surface.get("status_field") or "status").strip()
        freshness_rule = str(surface.get("freshness_policy") or "").strip().upper()
        violation_status = str(surface.get("violation_status") or "DEGRADED").strip().upper()
        if not rel_path:
            raise SystemExit(f"FAIL_CLOSED: freshness surface path missing for {sid}")
        if s_type != "DAY_STREAM":
            raise SystemExit(f"FAIL_CLOSED: sleeve readiness requires DAY_STREAM surfaces for {sid}")

        actual_day, latest_file = _latest_json_in_stream(rel_path)
        exists = latest_file is not None
        status_raw = None
        status_norm = "UNKNOWN"
        if latest_file is not None:
            obj = _read_json(latest_file)
            status_raw = obj.get(status_field)
            status_norm = _status_norm(status_raw)
            evidence.append({"path": str(latest_file), "sha256": _sha256_file(latest_file), "note": f"{sid}_surface"})

        freshness_ok_surface, freshness_reason = _eval_day_freshness(policy=freshness_rule, ref_day=day, actual_day=actual_day)
        surface_ok = exists and status_norm != "FAIL" and freshness_ok_surface
        if not surface_ok:
            if not exists:
                mon_reasons.append(f"MISSING_SURFACE:{sid}")
            elif status_norm == "FAIL":
                mon_reasons.append(f"SURFACE_FAIL:{sid}:{status_raw}")
            elif not freshness_ok_surface:
                mon_reasons.append(f"FRESHNESS_VIOLATION:{sid}:{freshness_reason}")

        mon_details.append({
            "surface_id": sid,
            "path": rel_path,
            "exists": exists,
            "actual_day": actual_day,
            "expected_day": day,
            "status_raw": status_raw,
            "status_norm": status_norm,
            "freshness_policy": freshness_rule,
            "freshness_ok": freshness_ok_surface,
            "freshness_reason": freshness_reason,
            "violation_status": violation_status,
            "surface_ok": surface_ok,
        })

    if len(mon_details) != len(required_surfaces):
        missing = sorted(required_surfaces - {d["surface_id"] for d in mon_details})
        raise SystemExit(f"FAIL_CLOSED: freshness policy missing required surfaces: {missing}")

    freshness_ok = len(mon_reasons) == 0
    monitoring_health_required = _status_norm(freshness_cfg.get("monitoring_health_required") or "PASS")
    monitoring_health_actual = "PASS" if freshness_ok else "FAIL"
    monitoring_requirement_ok = monitoring_health_actual == monitoring_health_required
    if not monitoring_requirement_ok:
        mon_reasons.append(f"MONITORING_HEALTH_REQUIREMENT_NOT_MET:{monitoring_health_actual}!={monitoring_health_required}")
        freshness_ok = False
    if not freshness_ok:
        promotion_blockers.extend(sorted(set(mon_reasons)))
    checks.append(
        CheckResult(
            check_id="freshness_compliance",
            required=True,
            status="PASS" if freshness_ok else "FAIL",
            weight=freshness_weight,
            score_awarded=freshness_weight if freshness_ok else 0,
            details={
                "diagnostics_freshness_policy_path": str(freshness_policy_path.relative_to(REPO_ROOT)),
                "monitoring_health_required": monitoring_health_required,
                "monitoring_health_actual": monitoring_health_actual,
                "surfaces": mon_details,
            },
        )
    )

    # 4) Safety/control clear from sleeve orchestrator safety breach signals.
    safety_cfg = policy.get("safety_control_requirements") if isinstance(policy.get("safety_control_requirements"), dict) else {}
    if "require_no_safety_breaches" not in safety_cfg:
        raise SystemExit("FAIL_CLOSED: policy missing safety_control_requirements.require_no_safety_breaches")
    require_no_safety_breaches = bool(safety_cfg.get("require_no_safety_breaches"))

    latest_verdict = _latest_verdict_for_day(sleeve_truth_root, day)
    safety_breaches: list[str] = []
    verdict_status = "UNKNOWN"
    verdict_path = None
    if latest_verdict is None:
        promotion_blockers.append("MISSING_DAY_VERDICT_FOR_SAFETY")
    else:
        verdict_doc = latest_verdict["doc"]
        verdict_path = Path(str(latest_verdict["path"]))
        verdict_status = _status_norm(verdict_doc.get("status"))
        raw_breaches = verdict_doc.get("safety_breaches")
        if isinstance(raw_breaches, list):
            safety_breaches = [str(x) for x in raw_breaches if str(x).strip()]
        if verdict_status == "FAIL" and not safety_breaches:
            safety_breaches.append("VERDICT_FAIL")
        if verdict_path.exists():
            evidence.append({"path": str(verdict_path), "sha256": _sha256_file(verdict_path), "note": "day_verdict"})

    safety_ok = (len(safety_breaches) == 0 and latest_verdict is not None) if require_no_safety_breaches else latest_verdict is not None
    if not safety_ok:
        promotion_blockers.append("UNRESOLVED_SAFETY_OR_CONTROL_FAILURES")

    checks.append(
        CheckResult(
            check_id="safety_control_clear",
            required=True,
            status="PASS" if safety_ok else "FAIL",
            weight=safety_weight,
            score_awarded=safety_weight if safety_ok else 0,
            details={
                "verdict_path": str(verdict_path) if verdict_path else None,
                "verdict_status": verdict_status,
                "safety_breaches": safety_breaches,
                "policy_require_no_safety_breaches": require_no_safety_breaches,
            },
        )
    )

    score = sum(c.score_awarded for c in checks)
    readiness_grade = _grade_for_score(score, grade_bands)

    required_checks_ok = all(c.status == "PASS" for c in checks if c.required)
    if not required_checks_ok and not promotion_blockers:
        promotion_blockers.append("REQUIRED_CHECK_FAILURE")

    promotion_candidate = required_checks_ok and score >= score_threshold
    if promotion_candidate:
        readiness_state = "READY"
    else:
        readiness_state = "NOT_READY"
    reasons = sorted(set(promotion_blockers))
    check_by_id = {c.check_id: c for c in checks}
    pass_conditions_remaining: list[str] = []
    if check_by_id.get("history_window") and check_by_id["history_window"].status != "PASS":
        pass_conditions_remaining.append(f"history_window: pass_days >= {min_pass_days} within {lookback_days}d")
    if check_by_id.get("hard_gates") and check_by_id["hard_gates"].status != "PASS":
        pass_conditions_remaining.append("hard_gates: gate_stack=PASS and execution_readiness=PASS")
    if check_by_id.get("freshness_compliance") and check_by_id["freshness_compliance"].status != "PASS":
        pass_conditions_remaining.append("freshness_compliance: paper_readiness/lifecycle_monitor/capital_authority_allocation pass freshness+status")
    if check_by_id.get("safety_control_clear") and check_by_id["safety_control_clear"].status != "PASS":
        pass_conditions_remaining.append("safety_control_clear: no verdict safety_breaches and no day verdict safety failure")
    if score < score_threshold:
        pass_conditions_remaining.append(f"readiness_score >= {score_threshold}")
    minimum_conditions_summary = list(pass_conditions_remaining)

    def _is_derived_blocker(code: str) -> bool:
        return code.startswith("MONITORING_HEALTH_REQUIREMENT_NOT_MET") or code.startswith("REQUIRED_CHECK_FAILURE")

    root_blockers = [c for c in reasons if not _is_derived_blocker(c)]
    derived_blockers = [c for c in reasons if _is_derived_blocker(c)]

    top_blockers_ordered = reasons[:5]
    recommended_next_actions: list[str] = []
    for b in top_blockers_ordered:
        if b.startswith("INSUFFICIENT_PASS_HISTORY"):
            recommended_next_actions.append("Accumulate additional PASS days in orchestrator_run_verdict_v2 within policy lookback window.")
        elif b.startswith("SURFACE_FAIL:lifecycle_monitor"):
            recommended_next_actions.append("Resolve lifecycle monitor FAIL checks (position_lifecycle_v2, exit_obligations_v1, exposure_reconciliation_v2).")
        elif b.startswith("FRESHNESS_VIOLATION:capital_authority_allocation"):
            recommended_next_actions.append("Regenerate capital_authority_allocation_v1 for current day to satisfy SAME_DAY_REQUIRED freshness.")
        elif b.startswith("FRESHNESS_VIOLATION:paper_readiness"):
            recommended_next_actions.append("Regenerate paper_readiness monitor for current day and confirm status != FAIL.")
        elif b.startswith("FRESHNESS_VIOLATION:lifecycle_monitor"):
            recommended_next_actions.append("Regenerate lifecycle_monitor for current day and confirm status != FAIL.")
        elif b.startswith("HARD_GATES_NOT_PASS"):
            recommended_next_actions.append("Restore hard gate artifacts to PASS (gate_stack_verdict_v1 and execution_readiness_gate_v1).")
        elif b.startswith("UNRESOLVED_SAFETY_OR_CONTROL_FAILURES") or b.startswith("MISSING_DAY_VERDICT_FOR_SAFETY"):
            recommended_next_actions.append("Clear safety/control failures in latest orchestrator day verdict before promotion evaluation.")
        else:
            recommended_next_actions.append(f"Investigate blocker and remediate source artifact: {b}")
    seen_actions: set[str] = set()
    recommended_next_actions = [a for a in recommended_next_actions if not (a in seen_actions or seen_actions.add(a))]

    surface_detail_by_id = {d["surface_id"]: d for d in mon_details}

    def _blocker_detail(code: str) -> dict[str, Any]:
        blocker_class = "UNKNOWN"
        severity = "ERROR"
        affected_surface = "unknown"
        exact_evidence_path = None
        clearance_condition = "Clear blocking condition and regenerate governed artifact."

        if code.startswith("INSUFFICIENT_PASS_HISTORY"):
            blocker_class = "HISTORY_WINDOW"
            severity = "ERROR"
            affected_surface = "orchestrator_run_verdict_v2"
            exact_evidence_path = observed[-1]["path"] if observed else None
            clearance_condition = f"pass_days >= {min_pass_days} within {lookback_days}d window"
        elif code.startswith("HARD_GATES_NOT_PASS"):
            blocker_class = "HARD_GATE"
            severity = "BLOCKING"
            affected_surface = "gate_stack_verdict_v1/execution_readiness_gate_v1"
            exact_evidence_path = str(gate_stack_path if gate_stack_st != gate_stack_req else exec_ready_path)
            clearance_condition = "gate_stack_status=PASS and execution_readiness_status=PASS"
        elif code.startswith("FRESHNESS_VIOLATION:"):
            blocker_class = "FRESHNESS"
            severity = "ERROR"
            sid = code.split(":", 2)[1] if ":" in code else "unknown"
            affected_surface = sid
            d = surface_detail_by_id.get(sid, {})
            if isinstance(d, dict):
                for ev in evidence:
                    if ev.get("note") == f"{sid}_surface":
                        exact_evidence_path = ev.get("path")
                        break
            clearance_condition = f"{sid} must satisfy governed freshness policy and surface status must not be FAIL"
        elif code.startswith("MONITORING_HEALTH_REQUIREMENT_NOT_MET"):
            blocker_class = "FRESHNESS"
            severity = "ERROR"
            affected_surface = "monitoring_health"
            exact_evidence_path = str(freshness_policy_path)
            clearance_condition = f"monitoring_health_actual must equal monitoring_health_required ({monitoring_health_required})"
        elif code.startswith("SURFACE_FAIL:"):
            blocker_class = "SURFACE_FAIL"
            severity = "ERROR"
            sid = code.split(":", 2)[1] if ":" in code else "unknown"
            affected_surface = sid
            for ev in evidence:
                if ev.get("note") == f"{sid}_surface":
                    exact_evidence_path = ev.get("path")
                    break
            clearance_condition = f"{sid} status must be PASS/OK in produced artifact"
        elif code.startswith("UNRESOLVED_SAFETY_OR_CONTROL_FAILURES"):
            blocker_class = "SAFETY_CONTROL"
            severity = "BLOCKING"
            affected_surface = "orchestrator_run_verdict_v2"
            exact_evidence_path = str(verdict_path) if verdict_path else None
            clearance_condition = "latest day verdict has no safety_breaches and no safety failure"
        elif code.startswith("MISSING_DAY_VERDICT_FOR_SAFETY"):
            blocker_class = "SAFETY_CONTROL"
            severity = "BLOCKING"
            affected_surface = "orchestrator_run_verdict_v2"
            exact_evidence_path = None
            clearance_condition = "authoritative day verdict must exist and pass safety checks"

        return {
            "blocker_code": code,
            "blocker_class": blocker_class,
            "severity": severity,
            "affected_surface": affected_surface,
            "exact_evidence_path": exact_evidence_path,
            "clearance_condition": clearance_condition,
        }

    promotion_blockers_detail = [_blocker_detail(code) for code in reasons]
    lifecycle_detail = surface_detail_by_id.get("lifecycle_monitor", {})
    capital_detail = surface_detail_by_id.get("capital_authority_allocation", {})
    dep_rows = lifecycle_dep_gov.get("dependencies") if isinstance(lifecycle_dep_gov.get("dependencies"), list) else []
    lifecycle_classification_counts = {"REQUIRED_FOR_EXECUTION": 0, "REQUIRED_FOR_READINESS": 0, "OPTIONAL_MONITORING": 0}
    for d in dep_rows:
        if not isinstance(d, dict):
            continue
        cls = str(d.get("classification") or "").strip().upper()
        if cls in lifecycle_classification_counts:
            lifecycle_classification_counts[cls] += 1
    lifecycle_cause_runtime = str(lifecycle_surface_runtime.get("lifecycle_cause_class") or "").strip()
    current_vs_required = {
        "pass_history": {
            "current": pass_days,
            "required": min_pass_days,
            "window_days": lookback_days,
            "ok": history_ok,
        },
        "hard_gates": {
            "gate_stack": {"current": gate_stack_st, "required": gate_stack_req},
            "execution_readiness": {"current": exec_ready_st, "required": exec_ready_req},
            "ok": hard_ok,
        },
        "freshness": {
            "policy_path": str(freshness_policy_path.relative_to(REPO_ROOT)),
            "ok": freshness_ok,
            "capital_authority_allocation": {
                "current_day": capital_detail.get("actual_day"),
                "required_day": day,
                "status": capital_detail.get("status_norm"),
                "ok": bool(capital_detail.get("surface_ok")),
            },
        },
        "lifecycle_monitor": {
            "current_status": lifecycle_detail.get("status_norm"),
            "required_status": "PASS",
            "current_day": lifecycle_detail.get("actual_day"),
            "required_day": day,
            "ok": bool(lifecycle_detail.get("surface_ok")),
            "runtime_cause_class": lifecycle_cause_runtime or None,
            "dependency_classification_counts": lifecycle_classification_counts,
            "dependency_governance_path": str(LIFECYCLE_CLASSIFICATION_PATH.relative_to(REPO_ROOT)),
        },
        "score": {"current": int(score), "required": int(score_threshold), "ok": bool(score >= score_threshold)},
    }
    smallest_clearance_set = []
    for b in root_blockers:
        d = _blocker_detail(b)
        cond = str(d.get("clearance_condition") or "").strip()
        if cond and cond not in smallest_clearance_set:
            smallest_clearance_set.append(cond)

    blocker_dependency_order = []
    if any(x.startswith("HARD_GATES_NOT_PASS") for x in root_blockers):
        blocker_dependency_order.append("hard_gates")
    if any(x.startswith("UNRESOLVED_SAFETY_OR_CONTROL_FAILURES") or x.startswith("MISSING_DAY_VERDICT_FOR_SAFETY") for x in root_blockers):
        blocker_dependency_order.append("safety_control_clear")
    if any(x.startswith("FRESHNESS_VIOLATION:") or x.startswith("SURFACE_FAIL:") or x.startswith("MISSING_SURFACE:") or x.startswith("MONITORING_HEALTH_REQUIREMENT_NOT_MET") for x in reasons):
        blocker_dependency_order.append("freshness_compliance")
    if any(x.startswith("INSUFFICIENT_PASS_HISTORY") for x in root_blockers):
        blocker_dependency_order.append("history_window")
    if score < score_threshold:
        blocker_dependency_order.append("readiness_score_threshold")

    estimated_promotion_gate_sequence = [
        "hard_gates",
        "safety_control_clear",
        "freshness_compliance",
        "history_window",
        "readiness_score_threshold",
    ]
    aggregate_blocker_summary = {
        "root_blocker_count": len(root_blockers),
        "derived_blocker_count": len(derived_blockers),
        "total_blocker_count": len(reasons),
        "promotion_candidate": bool(promotion_candidate),
    }

    summary_fragments: list[str] = []
    if any(b.startswith("FRESHNESS_VIOLATION:capital_authority_allocation") for b in root_blockers):
        summary_fragments.append("freshness violation (capital_authority_allocation)")
    if any(b.startswith("INSUFFICIENT_PASS_HISTORY") for b in root_blockers):
        summary_fragments.append(f"insufficient pass history ({pass_days}/{min_pass_days})")
    if any(b.startswith("SURFACE_FAIL:lifecycle_monitor") for b in root_blockers):
        if lifecycle_cause_runtime == "NOT_SCHEDULED":
            summary_fragments.append("lifecycle prerequisites not scheduled")
        else:
            summary_fragments.append("lifecycle monitor FAIL")
    if not summary_fragments and root_blockers:
        summary_fragments = [root_blockers[0]]
    if not summary_fragments:
        summary_fragments = ["no root blockers"]
    readiness_summary = f"{readiness_state} — blocked by " + ", ".join(summary_fragments) if not promotion_candidate else "READY — all required promotion conditions satisfied"

    decision_fragments: list[str] = []
    if any(b.startswith("SURFACE_FAIL:lifecycle_monitor") for b in root_blockers):
        if lifecycle_cause_runtime == "NOT_SCHEDULED":
            decision_fragments.append("REQUIRED_FOR_READINESS lifecycle dependencies are not scheduled")
        else:
            decision_fragments.append("lifecycle monitor requirements are not met")
    if any(b.startswith("INSUFFICIENT_PASS_HISTORY") for b in root_blockers):
        decision_fragments.append(f"pass history is below minimum ({pass_days}/{min_pass_days})")
    if any(b.startswith("FRESHNESS_VIOLATION:") for b in root_blockers):
        decision_fragments.append("freshness requirements are not met")
    if not decision_fragments and root_blockers:
        decision_fragments.append("root blockers remain unresolved")
    if not decision_fragments:
        decision_fragments.append("all promotion requirements are met")
    promotion_decision_basis = (
        "Promotion withheld because " + ", ".join(decision_fragments)
        if not promotion_candidate
        else "Promotion eligible because all required promotion conditions are satisfied"
    )

    calibration_support = {
        "policy_values": {
            "score_threshold_ready": int(score_threshold),
            "history": {
                "lookback_days": lookback_days,
                "min_pass_days": min_pass_days,
                "count_statuses": count_statuses,
            },
            "hard_gate_requirements": {
                "gate_stack_status": gate_stack_req,
                "execution_readiness_status": exec_ready_req,
            },
            "freshness_requirements": {
                "monitoring_health_required": monitoring_health_required,
                "diagnostics_policy_path": str(freshness_policy_path.relative_to(REPO_ROOT)),
            },
            "weights": {
                "history_window": history_weight,
                "hard_gates": hard_gates_weight,
                "freshness_compliance": freshness_weight,
                "safety_control_clear": safety_weight,
            },
        },
        "current_sleeve_outcome": {
            "readiness_state": readiness_state,
            "readiness_score": int(score),
            "readiness_grade": readiness_grade,
            "promotion_candidate": bool(promotion_candidate),
        },
        "failed_checks": [
            {
                "check_id": c.check_id,
                "status": c.status,
                "weight": c.weight,
                "score_awarded": c.score_awarded,
                "score_gap_to_full": int(c.weight - c.score_awarded),
            }
            for c in checks
            if c.status != "PASS"
        ],
        "score_contribution": [
            {
                "check_id": c.check_id,
                "weight": c.weight,
                "score_awarded": c.score_awarded,
            }
            for c in checks
        ],
    }
    promotion_checklist = {
        "must_be_true": [
            f"history_window: pass_days >= {min_pass_days} within {lookback_days}d",
            "hard_gates: gate_stack=PASS and execution_readiness=PASS",
            "freshness_compliance: required monitoring surfaces satisfy freshness and non-FAIL status",
            "safety_control_clear: no safety breaches in latest day verdict",
            f"readiness_score >= {score_threshold}",
        ],
        "currently_false": list(pass_conditions_remaining),
        "gating_conditions": [d["clearance_condition"] for d in promotion_blockers_detail if d.get("blocker_code") in root_blockers],
        "informational_conditions": [
            f"derived_blockers={len(derived_blockers)}",
            f"readiness_grade={readiness_grade}",
            f"score={score}/{score_threshold}",
        ],
    }

    produced_utc = f"{day}T00:00:00Z"
    policy_sha = _sha256_file(POLICY_PATH)

    out = {
        "schema_id": "C2_SLEEVE_LIVE_READINESS_V1",
        "schema_version": 1,
        "sleeve_id": sleeve_id,
        "mode": mode,
        "day_utc": day,
        "produced_utc": produced_utc,
        "authority_classification": "NON_CANONICAL_ADVISORY_ONLY",
        "control_decision_warning": "DO_NOT_USE_FOR_CONTROL_DECISIONS; canonical sleeve-edge control truth is sleeve_edge_snapshot_v1",
        "readiness_state": readiness_state,
        "readiness_summary": readiness_summary,
        "promotion_decision_basis": promotion_decision_basis,
        "readiness_score": int(score),
        "readiness_grade": readiness_grade,
        "score_threshold": int(score_threshold),
        "grade_band": readiness_grade,
        "promotion_candidate": bool(promotion_candidate),
        "promotion_blockers": reasons,
        "root_blockers": root_blockers,
        "derived_blockers": derived_blockers,
        "aggregate_blocker_summary": aggregate_blocker_summary,
        "promotion_blockers_detail": promotion_blockers_detail,
        "minimum_conditions_summary": minimum_conditions_summary,
        "current_vs_required": current_vs_required,
        "smallest_clearance_set": smallest_clearance_set,
        "blocker_dependency_order": blocker_dependency_order,
        "estimated_promotion_gate_sequence": estimated_promotion_gate_sequence,
        "top_blockers_ordered": top_blockers_ordered,
        "pass_conditions_remaining": pass_conditions_remaining,
        "recommended_next_actions": recommended_next_actions,
        "calibration_support": calibration_support,
        "promotion_checklist": promotion_checklist,
        "summary": (
            f"score={score} threshold={score_threshold} required_checks_ok={required_checks_ok} "
            f"freshness_ok={freshness_ok} pass_days={pass_days}/{min_pass_days}"
        ),
        "policy_ref": {
            "path": str(POLICY_PATH.relative_to(REPO_ROOT)),
            "sha256": policy_sha,
        },
        "checks": [
            {
                "check_id": c.check_id,
                "required": c.required,
                "status": c.status,
                "weight": c.weight,
                "score_awarded": c.score_awarded,
                "details": c.details,
            }
            for c in checks
        ],
        "reason_codes": reasons,
        "evidence_paths": sorted({str(e["path"]) for e in evidence}),
        "evidence": [
            {
                "path": e["path"],
                "sha256": e.get("sha256"),
                "note": e.get("note"),
            }
            for e in evidence
        ],
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
        "schema_id": "C2_SLEEVE_LIVE_READINESS_POINTER_V1",
        "schema_version": 1,
        "target_path": str(target_path),
        "target_sha256": _sha256_file(target_path),
    }
    _write_json_atomic(path, pointer)


def main() -> None:
    ap = argparse.ArgumentParser(prog="run_sleeve_live_readiness_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--sleeve_id", default="PRIMARY")
    ap.add_argument("--mode", default="PAPER")
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL_CLOSED: invalid day_utc: {day}")

    sleeve_id = str(args.sleeve_id).strip().upper()
    mode = str(args.mode).strip().upper()
    if mode != "PAPER":
        raise SystemExit("FAIL_CLOSED: sleeve_live_readiness_v1 currently supports PAPER mode only")

    sleeve_truth_root = _resolve_sleeve_truth_root(sleeve_id, mode, str(args.truth_root))

    out_obj = _run(day=day, sleeve_id=sleeve_id, mode=mode, sleeve_truth_root=sleeve_truth_root)

    out_path = (
        sleeve_truth_root
        / "readiness_v1"
        / "sleeve_live_readiness_v1"
        / day
        / "sleeve_live_readiness.v1.json"
    ).resolve()
    _write_json_atomic(out_path, out_obj)

    pointer_path = (
        sleeve_truth_root
        / "readiness_v1"
        / "sleeve_live_readiness_v1"
        / "latest_pointer.v1.json"
    ).resolve()
    _write_pointer(pointer_path, out_path)

    print(f"OK: SLEEVE_LIVE_READINESS_V1_WRITTEN day_utc={day} sleeve_id={sleeve_id} mode={mode} path={out_path}")
    print(f"OK: SLEEVE_LIVE_READINESS_V1_POINTER path={pointer_path}")


if __name__ == "__main__":
    main()
