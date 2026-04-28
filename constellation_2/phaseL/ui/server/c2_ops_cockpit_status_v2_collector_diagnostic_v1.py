#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


def _safe_read_json(path: Path) -> Tuple[Optional[Any], Optional[str]]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle), None
    except FileNotFoundError:
        return None, "FILE_NOT_FOUND"
    except json.JSONDecodeError:
        return None, "JSON_DECODE_ERROR"
    except Exception:
        return None, "READ_ERROR"


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> Optional[str]:
    try:
        return _sha256_bytes(path.read_bytes())
    except Exception:
        return None


def _mtime(path: Path) -> Optional[float]:
    try:
        return float(path.stat().st_mtime)
    except Exception:
        return None


def _coerce_state(s: Optional[str]) -> str:
    if not isinstance(s, str) or not s:
        return "UNKNOWN"
    u = s.upper()
    if u in ("PASS", "DEGRADED", "FAIL", "ABORTED", "UNKNOWN", "MISSING"):
        return u
    if u in ("OK", "PRESENT", "SUCCESS"):
        return "PASS"
    return "UNKNOWN"


def _top2_reason_codes(x: Any) -> List[str]:
    if isinstance(x, list):
        out = [str(a) for a in x if isinstance(a, (str, int, float)) and str(a).strip()]
        return out[:2]
    return []


def _is_day_str(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except Exception:
        return False


def _simple_gate_tile_dict(path: Path, tile_id: str) -> Tuple[Dict[str, Any], List[str], List[str]]:
    warnings: List[str] = []
    missing: List[str] = []

    if not path.exists():
        missing.append(str(path))
        return {
            "tile_id": tile_id,
            "state": "MISSING",
            "last_updated_utc": None,
            "reason_codes": ["MISSING_GATE_ARTIFACT"],
            "reason_human": [],
            "artifact_path": str(path),
            "artifact_sha256": None,
        }, warnings, missing

    obj, err = _safe_read_json(path)
    if not isinstance(obj, dict):
        warnings.append(f"GATE_UNREADABLE:{err}")
        return {
            "tile_id": tile_id,
            "state": "UNKNOWN",
            "last_updated_utc": None,
            "reason_codes": [f"GATE_UNREADABLE:{err}"],
            "reason_human": [],
            "artifact_path": str(path),
            "artifact_sha256": _sha256_file(path),
        }, warnings, missing

    state_obj = obj.get("state") or obj.get("status") or obj.get("verdict") or obj.get("run_verdict")
    if isinstance(state_obj, dict):
        state_obj = state_obj.get("state") or state_obj.get("status") or state_obj.get("run_verdict")
    reason_codes = obj.get("reason_codes") or obj.get("reason_codes_top") or []
    if isinstance(obj.get("verdict"), dict) and not reason_codes:
        verdict = obj["verdict"]
        reason_codes = verdict.get("reason_codes") or verdict.get("reason_codes_top") or []
    last = obj.get("generated_at_utc") or obj.get("generated_utc") or obj.get("produced_utc") or obj.get("asof_utc") or None
    return {
        "tile_id": tile_id,
        "state": _coerce_state(str(state_obj) if state_obj is not None else "UNKNOWN"),
        "last_updated_utc": str(last) if isinstance(last, str) and last else None,
        "reason_codes": _top2_reason_codes(reason_codes),
        "reason_human": [],
        "artifact_path": str(path),
        "artifact_sha256": _sha256_file(path),
    }, warnings, missing


def load_fixed_gate_tiles_v1(*, truth_root: Path, day: str) -> Dict[str, Any]:
    feed_path = (truth_root / "reports" / "feed_attestation_gate_v1" / day / "feed_attestation_gate.v1.json").resolve()
    liquidity_path = (truth_root / "reports" / "liquidity_slippage_gate_v1" / day / "liquidity_slippage_gate.v1.json").resolve()
    correlation_path = (truth_root / "reports" / "correlation_envelope_gate_v1" / day / "correlation_envelope_gate.v1.json").resolve()
    convex_path = (truth_root / "reports" / "convex_risk_assessment_v1" / day / "convex_risk_assessment.v1.json").resolve()
    capital_path = (truth_root / "reports" / "capital_risk_envelope_v2" / day / "capital_risk_envelope.v2.json").resolve()

    attest_tile, warn_att, miss_att = _simple_gate_tile_dict(feed_path, "feed_attestation")
    liquidity_tile, warn_liq, miss_liq = _simple_gate_tile_dict(liquidity_path, "liquidity_gate")
    corr_tile, warn_cor, miss_cor = _simple_gate_tile_dict(correlation_path, "correlation_gate")
    convex_tile, warn_cvx, miss_cvx = _simple_gate_tile_dict(convex_path, "convex_gate")
    capital_tile, warn_cap, miss_cap = _simple_gate_tile_dict(capital_path, "capital_risk_envelope")
    return {
        "attest_tile": attest_tile,
        "warn_att": warn_att,
        "miss_att": miss_att,
        "liquidity_tile": liquidity_tile,
        "warn_liq": warn_liq,
        "miss_liq": miss_liq,
        "corr_tile": corr_tile,
        "warn_cor": warn_cor,
        "miss_cor": miss_cor,
        "convex_tile": convex_tile,
        "warn_cvx": warn_cvx,
        "miss_cvx": miss_cvx,
        "capital_tile": capital_tile,
        "warn_cap": warn_cap,
        "miss_cap": miss_cap,
    }


def load_platform_readiness_policy_view_v1(*, policy_path: Path) -> Dict[str, Any]:
    obj, err = _safe_read_json(policy_path)
    base = {
        "present": False,
        "path": str(policy_path),
        "score_threshold_ready": None,
        "grade_bands": [],
        "weights": {},
        "hard_blockers": {},
        "reason_codes": ["ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE"],
    }
    if not isinstance(obj, dict):
        return base
    scoring = obj.get("scoring") if isinstance(obj.get("scoring"), dict) else {}
    hard_blockers = obj.get("hard_blockers") if isinstance(obj.get("hard_blockers"), dict) else {}
    weights = obj.get("weights") if isinstance(obj.get("weights"), dict) else {}
    return {
        **base,
        "present": True,
        "reason_codes": [],
        "score_threshold_ready": scoring.get("score_threshold_ready"),
        "grade_bands": scoring.get("grade_bands") if isinstance(scoring.get("grade_bands"), list) else [],
        "weights": weights,
        "hard_blockers": hard_blockers,
    }


def load_platform_readiness_history_v1(*, global_truth_root: Path) -> Dict[str, Any]:
    root = (global_truth_root / "readiness_v1" / "constellation_platform_readiness_v1").resolve()
    history: List[Dict[str, Any]] = []
    missing_paths: List[str] = []
    warnings: List[str] = []

    if not root.exists():
        return {
            "present": False,
            "root": str(root),
            "history": [],
            "date_range": None,
            "missing_paths": [str(root)],
            "warnings": ["PLATFORM_READINESS_HISTORY_ROOT_MISSING"],
            "authoritative_for_current_truth": False,
            "diagnostic_family_classification": "DIAGNOSTIC_ONLY_HISTORY_SERIES",
        }

    for day_dir in sorted((p for p in root.iterdir() if p.is_dir() and _is_day_str(p.name)), key=lambda p: p.name):
        artifact_path = (day_dir / "constellation_platform_readiness.v1.json").resolve()
        obj, err = _safe_read_json(artifact_path)
        if not isinstance(obj, dict):
            missing_paths.append(str(artifact_path))
            warnings.append(
                f"PLATFORM_READINESS_HISTORY_ARTIFACT_{'MISSING' if err == 'FILE_NOT_FOUND' else 'UNREADABLE'}:{day_dir.name}"
            )
            continue
        history.append(
            {
                "day": str(obj.get("day_utc") or day_dir.name),
                "score": obj.get("platform_readiness_score"),
                "grade": str(obj.get("platform_readiness_grade") or ""),
                "state": str(obj.get("platform_readiness_state") or "UNKNOWN"),
                "threshold": obj.get("score_threshold_ready"),
                "path": str(artifact_path),
                "produced_utc": str(obj.get("produced_utc") or ""),
            }
        )

    date_range = None
    if history:
        date_range = {"start": history[0]["day"], "end": history[-1]["day"]}

    comparison: Dict[str, Any]
    if len(history) >= 2:
        previous = history[-2]
        latest = history[-1]
        latest_score = latest.get("score")
        previous_score = previous.get("score")
        score_change = None
        if isinstance(latest_score, (int, float)) and isinstance(previous_score, (int, float)):
            score_change = latest_score - previous_score
        comparison = {
            "present": True,
            "latest_day": latest["day"],
            "previous_day": previous["day"],
            "score_change": score_change,
            "grade_change": {
                "from": previous.get("grade"),
                "to": latest.get("grade"),
            },
        }
    else:
        comparison = {
            "present": False,
            "latest_day": history[-1]["day"] if history else None,
            "previous_day": None,
            "score_change": None,
            "grade_change": {"from": None, "to": history[-1].get("grade") if history else None},
        }

    return {
        "present": bool(history),
        "root": str(root),
        "history": history,
        "date_range": date_range,
        "comparison": comparison,
        "missing_paths": missing_paths,
        "warnings": warnings,
        "authoritative_for_current_truth": False,
        "diagnostic_family_classification": "DIAGNOSTIC_ONLY_HISTORY_SERIES",
    }


def load_platform_bug_metrics_v1(*, global_truth_root: Path, day: str) -> Dict[str, Any]:
    root = (global_truth_root / "readiness_v1" / "constellation_bug_metrics_v1").resolve()
    path = (root / day / "constellation_bug_metrics.v1.json").resolve()
    obj, err = _safe_read_json(path)
    base = {
        "present": False,
        "path": str(path),
        "requested_day_path": str(path),
        "requested_day_present": False,
        "resolved_via_latest_pointer": False,
        "latest_pointer_path": str((root / "latest_pointer.v1.json").resolve()),
        "resolved_day": "",
        "produced_utc": None,
        "new_bug_events_today": None,
        "bug_velocity_7d_avg": None,
        "bug_velocity_14d_avg": None,
        "recurrence_rate": None,
        "diagnostic_stability_rate": None,
        "bug_velocity_trend": None,
        "recurring_bug_events": [],
        "event_counts_by_day": {},
        "metric_views": {},
        "calculation_summary": {},
        "unknown_fields": [],
        "evidence_paths": [],
        "reason_codes": [],
        "authoritative_for_family": False,
        "diagnostic_family_classification": "FALLBACK_OR_MISSING",
        "resolution_mode": "MISSING",
    }
    if not isinstance(obj, dict):
        pointer_path = (root / "latest_pointer.v1.json").resolve()
        pointer_obj, pointer_err = _safe_read_json(pointer_path)
        if isinstance(pointer_obj, dict):
            target_path_raw = str(pointer_obj.get("target_path") or "").strip()
            target_sha = str(pointer_obj.get("target_sha256") or "").strip().lower()
            if target_path_raw:
                target_path = Path(target_path_raw).expanduser().resolve()
                target_obj, target_err = _safe_read_json(target_path)
                if isinstance(target_obj, dict):
                    path_sha = ""
                    sha_ok = False
                    if target_path.exists() and target_path.is_file():
                        path_sha = (_sha256_file(target_path) or "").lower()
                        sha_ok = bool(target_sha and path_sha == target_sha)
                    event_counts = target_obj.get("event_counts_by_day")
                    return {
                        **base,
                        "present": True,
                        "path": str(target_path),
                        "requested_day_path": str(path),
                        "requested_day_present": False,
                        "resolved_via_latest_pointer": True,
                        "latest_pointer_path": str(pointer_path),
                        "latest_pointer_target_sha256": target_sha,
                        "latest_pointer_target_sha256_verified": sha_ok,
                        "resolved_day": str(target_obj.get("day_utc") or ""),
                        "produced_utc": target_obj.get("produced_utc"),
                        "new_bug_events_today": target_obj.get("new_bug_events_today"),
                        "bug_velocity_7d_avg": target_obj.get("bug_velocity_7d_avg"),
                        "bug_velocity_14d_avg": target_obj.get("bug_velocity_14d_avg"),
                        "recurrence_rate": target_obj.get("recurrence_rate"),
                        "diagnostic_stability_rate": target_obj.get("diagnostic_stability_rate"),
                        "bug_velocity_trend": target_obj.get("bug_velocity_trend"),
                        "recurring_bug_events": target_obj.get("recurring_bug_events") if isinstance(target_obj.get("recurring_bug_events"), list) else [],
                        "event_counts_by_day": event_counts if isinstance(event_counts, (dict, list)) else {},
                        "metric_views": target_obj.get("metric_views") if isinstance(target_obj.get("metric_views"), dict) else {},
                        "calculation_summary": target_obj.get("calculation_summary") if isinstance(target_obj.get("calculation_summary"), dict) else {},
                        "unknown_fields": target_obj.get("unknown_fields") if isinstance(target_obj.get("unknown_fields"), list) else [],
                        "evidence_paths": target_obj.get("evidence_paths") if isinstance(target_obj.get("evidence_paths"), list) else [],
                        "reason_codes": ["FALLBACK_TO_LATEST_POINTER"],
                        "fallback_source_reason_codes": ["ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE"],
                        "authoritative_for_family": False,
                        "diagnostic_family_classification": "LATEST_POINTER_FALLBACK_NON_AUTHORITATIVE",
                        "resolution_mode": "LATEST_POINTER_FALLBACK",
                    }
                return {
                    **base,
                    "reason_codes": [
                        "ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE",
                        "LATEST_POINTER_TARGET_MISSING" if target_err == "FILE_NOT_FOUND" else "LATEST_POINTER_TARGET_UNREADABLE",
                    ],
                }
        return {
            **base,
            "reason_codes": [
                "ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE",
                "LATEST_POINTER_MISSING" if pointer_err == "FILE_NOT_FOUND" else "LATEST_POINTER_UNREADABLE",
            ],
        }
    event_counts = obj.get("event_counts_by_day")
    return {
        **base,
        "present": True,
        "requested_day_present": True,
        "resolved_day": str(obj.get("day_utc") or day),
        "reason_codes": [],
        "produced_utc": obj.get("produced_utc"),
        "new_bug_events_today": obj.get("new_bug_events_today"),
        "bug_velocity_7d_avg": obj.get("bug_velocity_7d_avg"),
        "bug_velocity_14d_avg": obj.get("bug_velocity_14d_avg"),
        "recurrence_rate": obj.get("recurrence_rate"),
        "diagnostic_stability_rate": obj.get("diagnostic_stability_rate"),
        "bug_velocity_trend": obj.get("bug_velocity_trend"),
        "recurring_bug_events": obj.get("recurring_bug_events") if isinstance(obj.get("recurring_bug_events"), list) else [],
        "event_counts_by_day": event_counts if isinstance(event_counts, (dict, list)) else {},
        "metric_views": obj.get("metric_views") if isinstance(obj.get("metric_views"), dict) else {},
        "calculation_summary": obj.get("calculation_summary") if isinstance(obj.get("calculation_summary"), dict) else {},
        "unknown_fields": obj.get("unknown_fields") if isinstance(obj.get("unknown_fields"), list) else [],
        "evidence_paths": obj.get("evidence_paths") if isinstance(obj.get("evidence_paths"), list) else [],
        "authoritative_for_family": True,
        "diagnostic_family_classification": "REQUESTED_DAY_ARTIFACT",
        "resolution_mode": "REQUESTED_DAY_ARTIFACT",
    }


def load_signal_activity_v1(
    *,
    truth_root: Path,
    day: str,
    run_doc: Optional[Dict[str, Any]],
    engine_ids_from_active_engine_set_fn: Callable[[Path, str], Tuple[List[str], List[str], List[str]]],
    engine_ids_from_engine_linkage_fn: Callable[[Path, str], Tuple[List[str], List[str], List[str]]],
    load_sleeve_policy_fn: Callable[[], List[Dict[str, Any]]],
    engine_ids_from_heartbeat_fn: Callable[[Path, str], Tuple[List[str], List[str], List[str]]],
    load_gate_stack_day_state_fn: Callable[[Path, str], Dict[str, Any]],
    load_kill_switch_day_state_fn: Callable[[Path, str], Dict[str, Any]],
    compute_signal_frequency_30d_fn: Callable[[Path, str], Dict[str, Any]],
    count_phasec_veto_records_fn: Callable[[Path, str], Tuple[int, List[str]]],
    discover_phasec_identity_dirs_fn: Callable[[Path, str], List[Path]],
    attempt_stage_view_fn: Callable[[Optional[Dict[str, Any]], str], Dict[str, Any]],
    jsonl_same_day_presence_fn: Callable[[Path, str], bool],
    derive_trading_day_outcome_fn: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    expected_engine_ids, _miss_expected, _warn_expected = engine_ids_from_active_engine_set_fn(truth_root, day)
    if not expected_engine_ids:
        expected_engine_ids, _miss_linkage, _warn_linkage = engine_ids_from_engine_linkage_fn(truth_root, day)
    if not expected_engine_ids:
        policy_rows = load_sleeve_policy_fn()
        policy_engine_ids: List[str] = []
        for row in policy_rows:
            policy_engine_ids.extend(row.get("engine_ids") if isinstance(row.get("engine_ids"), list) else [])
        expected_engine_ids = sorted({str(eid).strip() for eid in policy_engine_ids if isinstance(eid, str) and str(eid).strip()})
    present_engine_ids, _miss_hb, _warn_hb = engine_ids_from_heartbeat_fn(truth_root, day)
    expected_set = sorted(set(expected_engine_ids))
    present_set = sorted(set(present_engine_ids))
    missing_engine_ids = [eid for eid in expected_set if eid not in set(present_set)]
    gate_stack = load_gate_stack_day_state_fn(truth_root, day)
    kill_switch = load_kill_switch_day_state_fn(truth_root, day)
    signal_frequency = compute_signal_frequency_30d_fn(truth_root, day)

    intents_root = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    intent_paths = sorted(intents_root.glob("*.exposure_intent.v1.json")) if intents_root.exists() and intents_root.is_dir() else []
    intent_count = len(intent_paths)

    veto_count, _ = count_phasec_veto_records_fn(truth_root, day)
    identity_dirs = discover_phasec_identity_dirs_fn(truth_root, day)
    released_identity_dir_count = len(identity_dirs)
    if released_identity_dir_count > 0 and veto_count > 0:
        phasec_label = "mixed"
        phasec_tone = "warning"
    elif released_identity_dir_count > 0:
        phasec_label = "released identities present"
        phasec_tone = "positive"
    elif veto_count > 0:
        phasec_label = "veto-only"
        phasec_tone = "warning"
    else:
        phasec_label = "no phaseC outputs"
        phasec_tone = "neutral"

    submit_stage = attempt_stage_view_fn(run_doc, "A7A_GOVERNED_SUBMIT_V5")
    submit_status = submit_stage.get("status")
    if released_identity_dir_count == 0 and veto_count > 0:
        submit_label = "governed abort / no identity"
        submit_tone = "warning"
    elif released_identity_dir_count == 0 and not submit_stage.get("present"):
        submit_label = "blocked upstream before governed submit"
        submit_tone = "negative"
    elif submit_status in ("OK", "PASS"):
        submit_label = "governed submit passed"
        submit_tone = "positive"
    elif submit_status == "SKIP":
        submit_label = "governed abort / no identity"
        submit_tone = "warning"
    elif submit_status in ("FAIL", "ABORTED"):
        submit_label = "governed submit failed"
        submit_tone = "negative"
    else:
        submit_label = "governed submit not reached"
        submit_tone = "neutral"

    symbols = ["IWM", "SPY", "QQQ"]
    upstream_rows: List[Dict[str, Any]] = []
    for symbol in symbols:
        md_path = (truth_root / "market_data_snapshot_v1" / symbol / f"{day[:4]}.jsonl").resolve()
        same_day_present = jsonl_same_day_presence_fn(md_path, day) if md_path.exists() and md_path.is_file() else False
        upstream_rows.append(
            {
                "symbol": symbol,
                "same_day_present": same_day_present,
                "path": str(md_path),
                "status": "PRESENT" if same_day_present else "MISSING",
                "tone": "positive" if same_day_present else "negative",
            }
        )

    upstream_summary = "upstream data ready" if all(row["same_day_present"] for row in upstream_rows) else "upstream data incomplete"
    upstream_tone = "positive" if all(row["same_day_present"] for row in upstream_rows) else "negative"
    if released_identity_dir_count == 0 and veto_count == 0 and intent_count == 0 and upstream_tone == "negative":
        submit_label = "blocked upstream before governed submit"
        submit_tone = "negative"

    outcome = derive_trading_day_outcome_fn(
        day=day,
        run_doc=run_doc,
        gate_stack=gate_stack,
        kill_switch=kill_switch,
        intent_count=intent_count,
        veto_count=veto_count,
        released_identity_dir_count=released_identity_dir_count,
        submit_stage=submit_stage,
        upstream_rows=upstream_rows,
        expected_heartbeat_count=len(expected_set),
        present_heartbeat_count=len(present_set),
    )

    return {
        "day_utc": day,
        "trading_day_outcome": outcome,
        "signal_frequency_30d": signal_frequency,
        "engine_heartbeats": {
            "expected_count": len(expected_set),
            "present_count": len(present_set),
            "expected_engine_ids": expected_set,
            "present_engine_ids": present_set,
            "missing_engine_ids": missing_engine_ids,
        },
        "intents": {
            "count": intent_count,
            "label": "No real intents produced" if intent_count == 0 else f"{intent_count} real intents produced",
            "path": str(intents_root),
        },
        "phasec_outcomes": {
            "veto_count": veto_count,
            "released_identity_dir_count": released_identity_dir_count,
            "label": phasec_label,
            "tone": phasec_tone,
            "path": str((truth_root / "phaseC_preflight_v1" / day).resolve()),
        },
        "governed_submit": {
            "stage_status": submit_status,
            "label": submit_label,
            "tone": submit_tone,
            "reason_codes": submit_stage.get("reason_codes") if isinstance(submit_stage.get("reason_codes"), list) else [],
        },
        "upstream_data_status": {
            "label": upstream_summary,
            "tone": upstream_tone,
            "symbols": upstream_rows,
        },
        "gate_stack": gate_stack,
        "kill_switch": kill_switch,
    }


def discover_attempts_v1(truth_root: Path, day: str) -> Tuple[List[str], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []
    attempts: List[str] = []

    v2_day_dir = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day).resolve()
    if not v2_day_dir.exists() or not v2_day_dir.is_dir():
        missing.append(str(v2_day_dir))
        warnings.append("ATTEMPTS_NOT_FOUND")
        return [], missing, [], {}, sorted(set(warnings))

    source_paths.append(str(v2_day_dir))
    mt = _mtime(v2_day_dir)
    if mt is not None:
        source_mtimes[str(v2_day_dir)] = mt

    for p in sorted([x for x in v2_day_dir.iterdir() if x.is_dir()], key=lambda x: x.name):
        name = p.name.strip()
        if name and "__A" in name:
            attempts.append(name)

    attempts = sorted(set(attempts))
    if not attempts:
        warnings.append("ATTEMPTS_NOT_FOUND")

    return attempts, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def _select_latest_attempt_v1(attempts: List[str]) -> Optional[str]:
    return attempts[-1] if attempts else None


def load_attempt_verdict_v1(truth_root: Path, day: str, attempt_id: str) -> Optional[Dict[str, Any]]:
    p = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day / attempt_id / "orchestrator_run_verdict.v2.json").resolve()
    obj, _err = _safe_read_json(p)
    if isinstance(obj, dict):
        return obj
    return None


def select_preferred_attempt_v1(truth_root: Path, day: str, attempts: List[str]) -> Optional[str]:
    latest = _select_latest_attempt_v1(attempts)
    if not isinstance(latest, str) or not latest:
        return None

    latest_doc = load_attempt_verdict_v1(truth_root, day, latest)
    latest_status = _coerce_state(str(latest_doc.get("status") or latest_doc.get("state") or "UNKNOWN")) if isinstance(latest_doc, dict) else "UNKNOWN"
    if latest_status != "ABORTED":
        return latest

    pass_attempts: List[Tuple[int, str]] = []
    for aid in attempts:
        doc = load_attempt_verdict_v1(truth_root, day, aid)
        if not isinstance(doc, dict):
            continue
        st = _coerce_state(str(doc.get("status") or doc.get("state") or "UNKNOWN"))
        if st != "PASS":
            continue
        seq = doc.get("attempt_seq")
        seq_i = int(seq) if isinstance(seq, int) else -1
        pass_attempts.append((seq_i, aid))
    if not pass_attempts:
        return latest

    pass_attempts.sort(key=lambda x: (x[0], x[1]))
    return pass_attempts[-1][1]


def _load_day_start_blocked_v1(day: str, *, global_truth_root: Path) -> Tuple[Optional[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    path = (global_truth_root / "reports" / "day_start_blocked_v1" / day / "day_start_blocked.v1.json").resolve()
    obj, err = _safe_read_json(path)
    if not isinstance(obj, dict):
        mt = _mtime(path)
        return (
            None,
            ([str(path)] if err == "FILE_NOT_FOUND" else []),
            ([] if err == "FILE_NOT_FOUND" else [str(path)]),
            ({str(path): mt} if mt is not None else {}),
            ([] if err == "FILE_NOT_FOUND" else [f"DAY_START_BLOCKED_UNREADABLE:{err}"]),
        )
    mt = _mtime(path)
    return obj, [], [str(path)], ({str(path): mt} if mt is not None else {}), []


def _load_trading_day_state_v1(day: str, *, global_truth_root: Path) -> Tuple[Optional[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    path = (global_truth_root / "reports" / "trading_day_state_v1" / day / "trading_day_state.v1.json").resolve()
    obj, err = _safe_read_json(path)
    if not isinstance(obj, dict):
        mt = _mtime(path)
        return (
            None,
            ([str(path)] if err == "FILE_NOT_FOUND" else []),
            ([] if err == "FILE_NOT_FOUND" else [str(path)]),
            ({str(path): mt} if mt is not None else {}),
            ([] if err == "FILE_NOT_FOUND" else [f"TRADING_DAY_STATE_UNREADABLE:{err}"]),
        )
    mt = _mtime(path)
    return obj, [], [str(path)], ({str(path): mt} if mt is not None else {}), []


def _grade_1_to_7_from_score_value(score: Any) -> Optional[int]:
    if not isinstance(score, (int, float)):
        return None
    clamped = max(0, min(100, int(score)))
    if clamped >= 95:
        return 7
    if clamped >= 85:
        return 6
    if clamped >= 75:
        return 5
    if clamped >= 65:
        return 4
    if clamped >= 50:
        return 3
    if clamped >= 30:
        return 2
    return 1


def load_sleeve_live_readiness_v1(truth_root: Path, day: str) -> Dict[str, Any]:
    p = (
        truth_root
        / "readiness_v1"
        / "sleeve_live_readiness_v1"
        / day
        / "sleeve_live_readiness.v1.json"
    ).resolve()
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return {
            "state": "UNKNOWN",
            "reason_codes": ["ARTIFACT_MISSING" if err == "FILE_NOT_FOUND" else "ARTIFACT_UNREADABLE"],
            "path": str(p),
            "present": False,
        }
    readiness_score = obj.get("readiness_score")
    score_threshold = obj.get("score_threshold")
    readiness_grade_1_to_7 = obj.get("readiness_grade_1_to_7")
    if not isinstance(readiness_grade_1_to_7, int):
        readiness_grade_1_to_7 = _grade_1_to_7_from_score_value(readiness_score)
    score_threshold_grade_1_to_7 = obj.get("score_threshold_grade_1_to_7")
    if not isinstance(score_threshold_grade_1_to_7, int):
        score_threshold_grade_1_to_7 = _grade_1_to_7_from_score_value(score_threshold)
    return {
        "state": str(obj.get("readiness_state") or "UNKNOWN"),
        "readiness_summary": str(obj.get("readiness_summary") or ""),
        "promotion_decision_basis": str(obj.get("promotion_decision_basis") or ""),
        "readiness_score": readiness_score,
        "score_threshold": score_threshold,
        "readiness_grade": obj.get("readiness_grade", obj.get("grade_band")),
        "readiness_grade_scale": obj.get("readiness_grade_scale") or "1_to_7",
        "readiness_grade_1_to_7": readiness_grade_1_to_7,
        "score_threshold_grade_1_to_7": score_threshold_grade_1_to_7,
        "grading_thresholds_1_to_7": obj.get("grading_thresholds_1_to_7") if isinstance(obj.get("grading_thresholds_1_to_7"), list) else [],
        "grade_band": obj.get("grade_band", obj.get("readiness_grade")),
        "promotion_candidate": obj.get("promotion_candidate"),
        "promotion_blockers": obj.get("promotion_blockers") if isinstance(obj.get("promotion_blockers"), list) else [],
        "root_blockers": obj.get("root_blockers") if isinstance(obj.get("root_blockers"), list) else [],
        "derived_blockers": obj.get("derived_blockers") if isinstance(obj.get("derived_blockers"), list) else [],
        "aggregate_blocker_summary": obj.get("aggregate_blocker_summary") if isinstance(obj.get("aggregate_blocker_summary"), dict) else {},
        "promotion_blockers_detail": obj.get("promotion_blockers_detail") if isinstance(obj.get("promotion_blockers_detail"), list) else [],
        "minimum_conditions_summary": obj.get("minimum_conditions_summary") if isinstance(obj.get("minimum_conditions_summary"), list) else [],
        "current_vs_required": obj.get("current_vs_required") if isinstance(obj.get("current_vs_required"), dict) else {},
        "smallest_clearance_set": obj.get("smallest_clearance_set") if isinstance(obj.get("smallest_clearance_set"), list) else [],
        "blocker_dependency_order": obj.get("blocker_dependency_order") if isinstance(obj.get("blocker_dependency_order"), list) else [],
        "estimated_promotion_gate_sequence": obj.get("estimated_promotion_gate_sequence") if isinstance(obj.get("estimated_promotion_gate_sequence"), list) else [],
        "top_blockers_ordered": obj.get("top_blockers_ordered") if isinstance(obj.get("top_blockers_ordered"), list) else [],
        "pass_conditions_remaining": obj.get("pass_conditions_remaining") if isinstance(obj.get("pass_conditions_remaining"), list) else [],
        "recommended_next_actions": obj.get("recommended_next_actions") if isinstance(obj.get("recommended_next_actions"), list) else [],
        "calibration_support": obj.get("calibration_support") if isinstance(obj.get("calibration_support"), dict) else {},
        "promotion_checklist": obj.get("promotion_checklist") if isinstance(obj.get("promotion_checklist"), dict) else {},
        "reason_codes": obj.get("reason_codes") if isinstance(obj.get("reason_codes"), list) else [],
        "evidence_paths": obj.get("evidence_paths") if isinstance(obj.get("evidence_paths"), list) else [],
        "path": str(p),
        "present": True,
    }


def load_run_scope_diagnostics_bundle_v1(*, run_tile_artifact_path: str | None, runtime_state_path: Path) -> Dict[str, Any]:
    run_doc: Optional[Dict[str, Any]] = None
    if isinstance(run_tile_artifact_path, str) and run_tile_artifact_path:
        obj, _err = _safe_read_json(Path(run_tile_artifact_path))
        if isinstance(obj, dict):
            run_doc = obj

    obj, _err = _safe_read_json(runtime_state_path)
    if not isinstance(obj, dict):
        scope_health = {
            "sleeve_execution_health": {"status": "UNKNOWN", "reason_codes": ["RUNTIME_STATE_MISSING"]},
            "system_monitoring_health": {"status": "UNKNOWN", "reason_codes": ["RUNTIME_STATE_MISSING"]},
            "overall": {"status": "UNKNOWN", "reason_codes": ["RUNTIME_STATE_MISSING"]},
            "source": {"path": str(runtime_state_path), "present": False},
        }
    else:
        scope = obj.get("scope_health") if isinstance(obj.get("scope_health"), dict) else {}
        scope_health = {
            "sleeve_execution_health": scope.get("sleeve_execution_health", {"status": "UNKNOWN"}),
            "system_monitoring_health": scope.get("system_monitoring_health", {"status": "UNKNOWN"}),
            "overall": scope.get("overall", {"status": "UNKNOWN"}),
            "source": {"path": str(runtime_state_path), "present": True},
        }

    return {
        "run_doc": run_doc,
        "scope_health": scope_health,
    }


def _candidate_activity_rollup_path(truth_root: Path, day: str) -> Path:
    return (truth_root / "monitoring_v1" / "activity_ledger_rollup_v1" / day / "activity_ledger_rollup.v1.json").resolve()


def _load_activity_rollup(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    path = _candidate_activity_rollup_path(truth_root, day)
    obj, err = _safe_read_json(path)
    if not isinstance(obj, dict):
        return None, [str(path)] if err == "FILE_NOT_FOUND" else [str(path)]
    return obj, []


def _load_activity_flow_diagnostics(
    truth_root: Path,
    day: str,
) -> Tuple[Optional[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    path = (truth_root / "reports" / "activity_flow_diagnostics_v1" / day / "activity_flow_diagnostics.v1.json").resolve()
    obj, err = _safe_read_json(path)
    if not isinstance(obj, dict):
        mt = _mtime(path)
        return (
            None,
            ([str(path)] if err == "FILE_NOT_FOUND" else []),
            ([] if err == "FILE_NOT_FOUND" else [str(path)]),
            ({str(path): mt} if mt is not None else {}),
            ([] if err == "FILE_NOT_FOUND" else [f"ACTIVITY_FLOW_DIAGNOSTICS_UNREADABLE:{err}"]),
        )
    mt = _mtime(path)
    return obj, [], [str(path)], ({str(path): mt} if mt is not None else {}), []


def _load_oms_terminal_dispositions(
    truth_root: Path,
    day: str,
) -> Tuple[List[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    root = (truth_root / "oms_decisions_v1" / "decisions" / day).resolve()
    if not root.exists() or not root.is_dir():
        return [], [str(root)], [], {}, []

    docs: List[Dict[str, Any]] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []
    for path in sorted(root.glob("*.oms_decision.v2.json"), key=lambda item: item.name):
        obj, err = _safe_read_json(path)
        if not isinstance(obj, dict):
            warnings.append(f"OMS_DECISION_UNREADABLE:{path.name}:{err}")
            continue
        if str(obj.get("schema_id") or "") != "C2_OMS_DECISION_V2":
            warnings.append(f"OMS_DECISION_SCHEMA_MISMATCH:{path.name}")
            continue
        if str(obj.get("day_utc") or "") != day:
            warnings.append(f"OMS_DECISION_DAY_MISMATCH:{path.name}")
            continue
        docs.append(obj)
        source_paths.append(str(path))
        mt = _mtime(path)
        if mt is not None:
            source_mtimes[str(path)] = mt
    return docs, [], source_paths, source_mtimes, sorted(set(warnings))


def _summarize_oms_terminal_dispositions(docs: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "present": bool(docs),
        "intent_count": len(docs),
        "terminal_state_counts": [],
        "stage_counts": [],
        "dominant_reason_codes": [],
        "by_engine": [],
    }
    if not docs:
        return summary

    term_counts: Dict[str, int] = {}
    stage_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    engine_counts: Dict[str, int] = {}
    for doc in docs:
        terminal_state = str(doc.get("terminal_state") or doc.get("decision_state") or "UNKNOWN").strip().upper()
        term_counts[terminal_state] = term_counts.get(terminal_state, 0) + 1
        stage_id = str(doc.get("stage_id") or "").strip()
        if stage_id:
            stage_counts[stage_id] = stage_counts.get(stage_id, 0) + 1
        engine_id = str(doc.get("engine_id") or "").strip()
        if engine_id:
            engine_counts[engine_id] = engine_counts.get(engine_id, 0) + 1
        for code in doc.get("reason_codes") or []:
            code_str = str(code).strip()
            if code_str:
                reason_counts[code_str] = reason_counts.get(code_str, 0) + 1

    summary["terminal_state_counts"] = [{"state": key, "count": term_counts[key]} for key in sorted(term_counts)]
    summary["stage_counts"] = [{"stage_id": key, "count": stage_counts[key]} for key in sorted(stage_counts)]
    summary["dominant_reason_codes"] = [
        {"reason_code": key, "count": count}
        for key, count in sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:5]
    ]
    summary["by_engine"] = [{"engine_id": key, "count": engine_counts[key]} for key in sorted(engine_counts)]
    return summary


def _extract_flow_from_activity_rollup(doc: Optional[Dict[str, Any]]) -> Dict[str, Optional[int]]:
    out: Dict[str, Optional[int]] = {
        "intents": None,
        "authorized": None,
        "submitted": None,
        "filled": None,
        "reconciled": None,
        "blocked_liquidity": None,
        "blocked_correlation": None,
        "blocked_attestation": None,
        "blocked_convex": None,
        "blocked_capital": None,
    }
    if not isinstance(doc, dict):
        return out

    totals = doc.get("totals") if isinstance(doc.get("totals"), dict) else None
    counts = doc.get("counts") if isinstance(doc.get("counts"), dict) else None
    source = totals or counts or doc

    def _get_int(keys: List[str]) -> Optional[int]:
        for key in keys:
            value = source.get(key) if isinstance(source, dict) else None
            if isinstance(value, int):
                return value
        return None

    out["intents"] = _get_int(["intents", "intent_count"])
    out["authorized"] = _get_int(["authorized", "authorized_count"])
    out["submitted"] = _get_int(["submitted", "submitted_count"])
    out["filled"] = _get_int(["filled", "filled_count"])
    out["reconciled"] = _get_int(["reconciled", "reconciled_count"])
    out["blocked_liquidity"] = _get_int(["blocked_liquidity", "liquidity_blocked"])
    out["blocked_correlation"] = _get_int(["blocked_correlation", "correlation_blocked"])
    out["blocked_attestation"] = _get_int(["blocked_attestation", "attestation_blocked"])
    out["blocked_convex"] = _get_int(["blocked_convex", "convex_blocked"])
    out["blocked_capital"] = _get_int(["blocked_capital", "capital_blocked"])
    return out


def load_flow_diagnostics_bundle_v1(*, truth_root: Path, day: str) -> Dict[str, Any]:
    rollup_doc, miss_roll = _load_activity_rollup(truth_root, day)
    activity_flow_doc, miss_afd, sp_afd, sm_afd, warn_afd = _load_activity_flow_diagnostics(truth_root, day)
    oms_docs, miss_oms, sp_oms, sm_oms, warn_oms = _load_oms_terminal_dispositions(truth_root, day)
    return {
        "rollup_doc": rollup_doc,
        "miss_roll": miss_roll,
        "activity_flow_doc": activity_flow_doc,
        "miss_afd": miss_afd,
        "sp_afd": sp_afd,
        "sm_afd": sm_afd,
        "warn_afd": warn_afd,
        "oms_docs": oms_docs,
        "miss_oms": miss_oms,
        "sp_oms": sp_oms,
        "sm_oms": sm_oms,
        "warn_oms": warn_oms,
        "oms_summary": _summarize_oms_terminal_dispositions(oms_docs),
        "flow": _extract_flow_from_activity_rollup(rollup_doc),
    }


def _attempt_stage_status(doc: Optional[Dict[str, Any]], stage_id: str) -> Optional[str]:
    if not isinstance(doc, dict):
        return None
    stages = doc.get("stages")
    if not isinstance(stages, list):
        return None
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        if str(stage.get("stage_id") or "") != stage_id:
            continue
        status = stage.get("status")
        if isinstance(status, str) and status.strip():
            return status.strip().upper()
        return None
    return None


def load_broker_connection_observer_tile_v1(
    *,
    truth_root: Path,
    day: str,
    c3_status: Optional[Dict[str, Any]],
    run_doc: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    broker_path_v2 = (truth_root / "reports" / "broker_reconciliation_v2" / day / "broker_reconciliation.v2.json").resolve()
    broker_path_v1 = (truth_root / "reports" / "broker_reconciliation_v1" / day / "broker_reconciliation.v1.json").resolve()

    if broker_path_v2.exists():
        broker_tile, warn_broker, miss_broker = _simple_gate_tile_dict(broker_path_v2, "broker_connection_observer")
    elif broker_path_v1.exists():
        broker_tile, warn_broker, miss_broker = _simple_gate_tile_dict(broker_path_v1, "broker_connection_observer")
    else:
        broker_state = "UNKNOWN"
        broker_last = None
        if isinstance(c3_status, dict):
            broker_reconciliation = (
                c3_status.get("broker_reconciliation") if isinstance(c3_status.get("broker_reconciliation"), dict) else None
            )
            if isinstance(broker_reconciliation, dict):
                broker_state = _coerce_state(str(broker_reconciliation.get("state") or "UNKNOWN"))
                broker_last = broker_reconciliation.get("generated_at_utc") or broker_reconciliation.get("generated_utc") or None
        if broker_state in ("UNKNOWN", "MISSING"):
            stage_status = _attempt_stage_status(run_doc, "A1_BROKER_RECONCILIATION_GATE_V2_CHECK")
            if stage_status in ("OK", "SKIP", "PASS"):
                broker_state = "PASS"
            elif stage_status in ("FAIL", "ABORTED"):
                broker_state = "ABORTED"

        broker_tile = {
            "tile_id": "broker_connection_observer",
            "state": _coerce_state(broker_state),
            "last_updated_utc": str(broker_last) if isinstance(broker_last, str) and broker_last else None,
            "reason_codes": [],
            "reason_human": [],
            "artifact_path": None,
            "artifact_sha256": None,
        }
        warn_broker = []
        miss_broker = [str(broker_path_v2), str(broker_path_v1)]

    return {
        "broker_tile": broker_tile,
        "warn_broker": warn_broker,
        "miss_broker": miss_broker,
    }


def _load_nav(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[str], Optional[str]]:
    p = (truth_root / "accounting_v2" / "nav" / day / "nav.v2.json").resolve()
    if not p.exists():
        return None, [str(p)], None, None
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)], str(err), str(p)
    return obj, [], None, str(p)


def _extract_portfolio_metrics(nav_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = {
        "nav_total": None,
        "pnl_today": None,
        "pnl_cumulative": None,
        "drawdown_pct": None,
        "cash_pct": None,
        "net_exposure_pct": None,
        "gross_exposure_pct": None,
        "asof_utc": None,
        "authority_basis": None,
        "authoritative": None,
        "authority_reason_codes": [],
        "nav_status": None,
    }
    if not isinstance(nav_doc, dict):
        return out

    out["asof_utc"] = nav_doc.get("asof_utc") or nav_doc.get("generated_at_utc") or nav_doc.get("generated_utc")

    nav = nav_doc.get("nav") if isinstance(nav_doc.get("nav"), dict) else None
    src = nav or nav_doc

    def _get_num(keys: List[str]) -> Optional[float]:
        for k in keys:
            v = src.get(k) if isinstance(src, dict) else None
            if isinstance(v, (int, float)):
                return float(v)
        return None

    out["nav_total"] = _get_num(["nav_total", "nav_end", "nav"])
    out["pnl_today"] = _get_num(["pnl_today", "pnl_day", "pnl_1d"])
    out["pnl_cumulative"] = _get_num(["pnl_cumulative", "pnl_total", "pnl_cum"])
    out["drawdown_pct"] = _get_num(["drawdown_pct", "dd_pct"])
    out["cash_pct"] = _get_num(["cash_pct"])
    out["net_exposure_pct"] = _get_num(["net_exposure_pct", "net_pct"])
    out["gross_exposure_pct"] = _get_num(["gross_exposure_pct", "gross_pct"])
    out["nav_status"] = str(nav_doc.get("status") or "UNKNOWN").upper()
    return out


def _load_positions_snapshot(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[str], Optional[str]]:
    p = (truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json").resolve()
    if not p.exists():
        return None, [str(p)], None, None
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)], str(err), str(p)
    return obj, [], None, str(p)


def _load_exposure_net(truth_root: Path, day: str) -> Tuple[Optional[Dict[str, Any]], List[str], Optional[str], Optional[str]]:
    p = (truth_root / "risk_v1" / "exposure_net_v1" / day / "exposure_net.v1.json").resolve()
    if not p.exists():
        return None, [str(p)], None, None
    obj, err = _safe_read_json(p)
    if not isinstance(obj, dict):
        return None, [str(p)], str(err), str(p)
    return obj, [], None, str(p)


def _extract_positions_exposure(positions_doc: Optional[Dict[str, Any]], exposure_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "asof_utc": None,
        "summary": {
            "positions_total": 0,
            "open_positions": 0,
            "portfolio_net_notional_usd": None,
            "portfolio_gross_notional_usd": None,
            "capital_at_risk_cents": None,
            "symbol_count": None,
        },
        "positions": [],
        "exposure_by_engine": [],
        "sources": {"positions_path": None, "exposure_path": None},
    }

    if isinstance(positions_doc, dict):
        out["asof_utc"] = positions_doc.get("produced_utc") if isinstance(positions_doc.get("produced_utc"), str) else None
        pos = positions_doc.get("positions") if isinstance(positions_doc.get("positions"), dict) else {}
        items = pos.get("items") if isinstance(pos.get("items"), list) else []
        rows: List[Dict[str, Any]] = []
        open_cnt = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            status = str(it.get("status") or "UNKNOWN")
            qty = it.get("qty")
            if status.upper() == "OPEN":
                open_cnt += 1
            rows.append(
                {
                    "position_id": it.get("position_id"),
                    "engine_id": it.get("engine_id"),
                    "qty": qty if isinstance(qty, (int, float)) else None,
                    "status": status,
                    "market_exposure_type": it.get("market_exposure_type"),
                }
            )
        out["positions"] = rows
        out["summary"]["positions_total"] = len(rows)
        out["summary"]["open_positions"] = open_cnt

    if isinstance(exposure_doc, dict):
        portfolio = exposure_doc.get("portfolio") if isinstance(exposure_doc.get("portfolio"), dict) else {}
        out["summary"]["portfolio_net_notional_usd"] = portfolio.get("net_notional_usd")
        out["summary"]["portfolio_gross_notional_usd"] = portfolio.get("gross_notional_usd")
        out["summary"]["capital_at_risk_cents"] = portfolio.get("capital_at_risk_cents")
        out["summary"]["symbol_count"] = portfolio.get("symbol_count")

        per_engine = exposure_doc.get("per_engine") if isinstance(exposure_doc.get("per_engine"), list) else []
        e_rows: List[Dict[str, Any]] = []
        for it in per_engine:
            if not isinstance(it, dict):
                continue
            e_rows.append(
                {
                    "engine_id": it.get("engine_id"),
                    "net_notional_usd": it.get("net_notional_usd"),
                    "gross_notional_usd": it.get("gross_notional_usd"),
                    "capital_at_risk_cents": it.get("capital_at_risk_cents"),
                }
            )
        e_rows.sort(key=lambda x: str(x.get("engine_id") or ""))
        out["exposure_by_engine"] = e_rows

    return out


def load_portfolio_positions_bundle_v1(*, truth_root: Path, day: str) -> Dict[str, Any]:
    nav_doc, miss_nav, nav_err, nav_path = _load_nav(truth_root, day)
    positions_doc, miss_pos, pos_err, pos_path = _load_positions_snapshot(truth_root, day)
    exposure_doc, miss_exp, exp_err, exp_path = _load_exposure_net(truth_root, day)
    return {
        "nav_doc": nav_doc,
        "miss_nav": miss_nav,
        "nav_err": nav_err,
        "nav_path": nav_path,
        "portfolio": _extract_portfolio_metrics(nav_doc),
        "positions_doc": positions_doc,
        "miss_pos": miss_pos,
        "pos_err": pos_err,
        "pos_path": pos_path,
        "exposure_doc": exposure_doc,
        "miss_exp": miss_exp,
        "exp_err": exp_err,
        "exp_path": exp_path,
        "positions_exposure": _extract_positions_exposure(positions_doc, exposure_doc),
    }
