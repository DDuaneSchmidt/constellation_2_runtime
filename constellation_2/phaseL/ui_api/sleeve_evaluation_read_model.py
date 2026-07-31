from __future__ import annotations

from collections import Counter
from functools import lru_cache
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .common import (
    GLOBAL_TRUTH_ROOT,
    REPO_ROOT,
    SLEEVE_TRUTH_ROOT,
    evidence_ref,
    freshness_state,
    latest_day_from_roots,
    latest_timestamp,
    read_json_dict,
)
from .dto import evidence_refs, markers, view_envelope
from .metadata_contract import validate_projection_envelope


def _string_or_none(value: Any) -> Optional[str]:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _float_from_text(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _usd_from_cents(value: Any) -> Optional[float]:
    if isinstance(value, int):
        return value / 100.0
    return None


def _int_or_none(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def _read_json_object(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _resolve_sleeve_day(day: Optional[str]) -> Optional[str]:
    text = _string_or_none(day)
    if text:
        return text
    evaluation_root = (SLEEVE_TRUTH_ROOT / "reports" / "evaluation_input_manifest_v1").resolve()
    measurement_root = (SLEEVE_TRUTH_ROOT / "reports" / "sleeve_edge_measurement_snapshot_v1").resolve()
    governance_root = (SLEEVE_TRUTH_ROOT / "reports" / "allocation_governance_snapshot_v1").resolve()
    latest_evaluation_day = latest_day_from_roots([evaluation_root, measurement_root, governance_root])
    if latest_evaluation_day:
        return latest_evaluation_day
    return latest_day_from_roots([SLEEVE_TRUTH_ROOT / "allocation_v1" / "capital_authority_allocation_v1"])


def _latest_day_for_sleeve_report_family(family: str, sleeve_id: str) -> Optional[str]:
    root = (SLEEVE_TRUTH_ROOT / "reports" / family).resolve()
    if not root.exists() or not root.is_dir():
        return None
    candidates: List[str] = []
    for day_dir in root.iterdir():
        if not day_dir.is_dir():
            continue
        day = _string_or_none(day_dir.name)
        if not day:
            continue
        artifact_dir = (day_dir / sleeve_id).resolve()
        if artifact_dir.exists() and artifact_dir.is_dir():
            candidates.append(day)
    if not candidates:
        return None
    return sorted(candidates)[-1]


def _legacy_snapshot_diagnostics(path_text: Any) -> Dict[str, Any]:
    path = _string_or_none(path_text)
    if not path:
        return {"reason_codes": [], "invalidity_reasons": [], "sample_count": None, "snapshot_path": None}
    snapshot_path = Path(path).resolve()
    doc, err = read_json_dict(snapshot_path)
    if not isinstance(doc, dict):
        return {
            "reason_codes": [],
            "invalidity_reasons": [],
            "sample_count": None,
            "snapshot_path": str(snapshot_path),
            "diagnostic_code": f"LEGACY_SNAPSHOT_{err or 'UNREADABLE'}",
        }
    qualification = doc.get("qualification") if isinstance(doc.get("qualification"), dict) else {}
    factual_metrics = doc.get("factual_metrics") if isinstance(doc.get("factual_metrics"), dict) else {}
    reason_codes = [str(code).strip() for code in qualification.get("reason_codes") or [] if str(code).strip()]
    invalidity_reasons = [str(code).strip() for code in factual_metrics.get("invalidity_reasons") or [] if str(code).strip()]
    return {
        "reason_codes": reason_codes,
        "invalidity_reasons": invalidity_reasons,
        "sample_count": _int_or_none(factual_metrics.get("sample_count")),
        "snapshot_path": str(snapshot_path),
    }


def _extract_broker_error_codes(raw_payload: Mapping[str, Any] | None) -> List[str]:
    if not isinstance(raw_payload, Mapping):
        return []
    ib_fields = raw_payload.get("ib_fields")
    if not isinstance(ib_fields, Mapping):
        return []
    args = ib_fields.get("args")
    if not isinstance(args, list):
        return []
    codes: List[str] = []
    for item in args:
        if not isinstance(item, Mapping):
            continue
        value = str(item.get("value") or "").strip()
        if not value or not value.startswith("errorCode="):
            continue
        code = value.split("=", 1)[1].strip()
        if code:
            codes.append(code)
    return codes


def _count_events_from_broker_log(log_path: Path) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not log_path.exists() or not log_path.is_file():
        return counts
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return counts
    for line in lines:
        text = str(line).strip()
        if not text:
            continue
        try:
            obj = json.loads(text)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        event_type = str(obj.get("event_type") or "").strip()
        if event_type:
            counts[event_type] += 1
    return counts


def _event_counts_from_manifest_doc(manifest_doc: Mapping[str, Any]) -> Dict[str, int]:
    log_payload = manifest_doc.get("log")
    if not isinstance(log_payload, Mapping):
        return {}
    event_counts = log_payload.get("event_type_counts")
    if not isinstance(event_counts, Mapping):
        return {}
    normalized: Dict[str, int] = {}
    for key, value in event_counts.items():
        event_type = str(key or "").strip()
        count = _int_or_none(value)
        if event_type and count is not None and count >= 0:
            normalized[event_type] = count
    return normalized


def _broker_event_counts_for_day_from_root(
    *,
    truth_root: Path,
    day: str,
) -> tuple[Dict[str, int], bool, Optional[Path]]:
    broker_events_root = (Path(truth_root).resolve() / "execution_evidence_v1" / "broker_events").resolve()
    day_dir = (broker_events_root / day).resolve()
    log_path = (day_dir / "broker_event_log.v1.jsonl").resolve()
    manifest_path = (day_dir / "broker_event_day_manifest.v1.json").resolve()
    manifest_doc, _ = read_json_dict(manifest_path)
    if isinstance(manifest_doc, dict):
        manifest_counts = _event_counts_from_manifest_doc(manifest_doc)
        if manifest_counts:
            log_doc = manifest_doc.get("log") if isinstance(manifest_doc.get("log"), Mapping) else {}
            manifest_log_path = (
                Path(str(log_doc.get("log_path")).strip()).resolve()
                if isinstance(log_doc, Mapping) and str(log_doc.get("log_path") or "").strip()
                else log_path
            )
            return manifest_counts, True, manifest_log_path
    if log_path.exists() and log_path.is_file():
        return dict(_count_events_from_broker_log(log_path)), True, log_path
    return {}, False, None


def _broker_error_codes_for_day_from_root(*, truth_root: Path, day: str) -> List[str]:
    log_path = (
        Path(truth_root).resolve()
        / "execution_evidence_v1"
        / "broker_events"
        / day
        / "broker_event_log.v1.jsonl"
    ).resolve()
    if not log_path.exists() or not log_path.is_file():
        return []
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    ordered: List[str] = []
    seen: set[str] = set()
    for line in lines:
        text = str(line).strip()
        if not text:
            continue
        try:
            obj = json.loads(text)
        except Exception:
            continue
        if not isinstance(obj, dict) or str(obj.get("event_type") or "").strip() != "error":
            continue
        for code in _extract_broker_error_codes(obj):
            if code not in seen:
                seen.add(code)
                ordered.append(code)
    return ordered


@lru_cache(maxsize=8)
def _broker_event_history_diagnostics(truth_root_text: str) -> Dict[str, Optional[str]]:
    truth_root = Path(str(truth_root_text)).resolve()
    broker_events_root = (truth_root / "execution_evidence_v1" / "broker_events").resolve()
    first_live_fill_day: Optional[str] = None
    first_live_order_day: Optional[str] = None
    first_live_day_with_order_and_fill: Optional[str] = None

    if broker_events_root.exists() and broker_events_root.is_dir():
        day_dirs = sorted(
            [
                child.name
                for child in broker_events_root.iterdir()
                if child.is_dir() and len(child.name) == 10
            ]
        )
        for day in day_dirs:
            event_counts, has_source, _ = _broker_event_counts_for_day_from_root(
                truth_root=truth_root,
                day=day,
            )
            if not has_source:
                continue
            order_count = int(event_counts.get("openOrder", 0)) + int(event_counts.get("orderStatus", 0))
            fill_count = int(event_counts.get("execDetails", 0)) + int(event_counts.get("commissionReport", 0))
            if fill_count > 0 and first_live_fill_day is None:
                first_live_fill_day = day
            if order_count > 0 and first_live_order_day is None:
                first_live_order_day = day
            if order_count > 0 and fill_count > 0 and first_live_day_with_order_and_fill is None:
                first_live_day_with_order_and_fill = day

    first_fixture_day_with_usable_order_fill_facts: Optional[str] = None
    audit_root = (truth_root / "reports" / "broker_fact_spine_audit_v1").resolve()
    if audit_root.exists() and audit_root.is_dir():
        for day_dir in sorted([child for child in audit_root.iterdir() if child.is_dir()]):
            day = str(day_dir.name).strip()
            audit_doc, _ = read_json_dict((day_dir / "broker_fact_spine_audit.v1.json").resolve())
            if not isinstance(audit_doc, dict):
                continue
            count_payload = (
                audit_doc.get("counts")
                if isinstance(audit_doc.get("counts"), Mapping)
                else audit_doc.get("fact_counts")
            )
            if not isinstance(count_payload, Mapping):
                continue
            order_count = _int_or_none(count_payload.get("observed_order_fact_count")) or 0
            fill_count = _int_or_none(count_payload.get("observed_fill_fact_count")) or 0
            source_ref = audit_doc.get("source_input_ref")
            source_path = (
                str(source_ref.get("artifact_path") or "").strip()
                if isinstance(source_ref, Mapping)
                else ""
            )
            if (
                order_count > 0
                and fill_count > 0
                and source_path
                and (
                    "/ops/fixtures/" in source_path
                    or "broker_fact_spine_sample_events_v1.jsonl" in source_path
                )
            ):
                first_fixture_day_with_usable_order_fill_facts = day
                break

    return {
        "first_fixture_day_with_usable_order_fill_facts": first_fixture_day_with_usable_order_fill_facts,
        "first_live_fill_day": first_live_fill_day,
        "first_live_order_day": first_live_order_day,
        "first_live_day_with_order_and_fill": first_live_day_with_order_and_fill,
    }


def _broker_callback_gap_diagnostic(day: Optional[str]) -> Dict[str, Any]:
    resolved_day = _string_or_none(day)
    if not resolved_day:
        return {
            "callbacks_absent": False,
            "order_callback_count": 0,
            "fill_callback_count": 0,
            "error_codes": [],
            "has_broker_source": False,
        }
    event_counts, has_source, _ = _broker_event_counts_for_day_from_root(
        truth_root=SLEEVE_TRUTH_ROOT,
        day=resolved_day,
    )
    order_callback_count = int(event_counts.get("openOrder", 0)) + int(event_counts.get("orderStatus", 0))
    fill_callback_count = int(event_counts.get("execDetails", 0)) + int(event_counts.get("commissionReport", 0))
    callbacks_absent = bool(has_source and order_callback_count == 0 and fill_callback_count == 0)
    error_codes = (
        _broker_error_codes_for_day_from_root(truth_root=SLEEVE_TRUTH_ROOT, day=resolved_day)
        if callbacks_absent
        else []
    )
    history = _broker_event_history_diagnostics(str(SLEEVE_TRUTH_ROOT.resolve()))
    return {
        "callbacks_absent": callbacks_absent,
        "order_callback_count": order_callback_count,
        "fill_callback_count": fill_callback_count,
        "error_codes": error_codes,
        "has_broker_source": bool(has_source),
        "first_fixture_day_with_usable_order_fill_facts": history.get(
            "first_fixture_day_with_usable_order_fill_facts"
        ),
        "first_live_fill_day": history.get("first_live_fill_day"),
        "first_live_order_day": history.get("first_live_order_day"),
        "first_live_day_with_order_and_fill": history.get("first_live_day_with_order_and_fill"),
    }


def _format_error_code_list(codes: List[str]) -> str:
    preferred_order = ["502", "504", "326"]
    unique = [str(code).strip() for code in codes if str(code).strip()]
    deduped: List[str] = []
    seen: set[str] = set()
    for code in unique:
        if code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    ordered: List[str] = [code for code in preferred_order if code in seen]
    ordered.extend([code for code in deduped if code not in preferred_order])
    return ", ".join(ordered)


def _measurement_invalid_reason(
    reason_codes: List[str],
    attribution_diagnostics: Mapping[str, Any] | None = None,
    evaluation_day: Optional[str] = None,
) -> tuple[str, str]:
    code_set = {str(code).strip() for code in reason_codes if str(code).strip()}
    if "SLEEVE_EDGE_NATIVE_ENGINE_ATTRIBUTION_UNAVAILABLE" in code_set:
        broker_callback_diag = _broker_callback_gap_diagnostic(evaluation_day)
        if broker_callback_diag.get("callbacks_absent"):
            reason = "No grade available. Reason: no order/fill broker events for this day."
            code_text = _format_error_code_list(
                list(broker_callback_diag.get("error_codes") or [])
            )
            if code_text:
                reason = f"{reason} Broker session errors detected: {code_text}."
            next_step = (
                "Rerun paper trading with healthy broker session and captured "
                "openOrder/orderStatus/execDetails callbacks."
            )
            fixture_day = _string_or_none(
                broker_callback_diag.get("first_fixture_day_with_usable_order_fill_facts")
            )
            if fixture_day:
                next_step = (
                    f"{next_step} First fixture day with usable order/fill facts: {fixture_day}."
                )
            first_live_fill_day = _string_or_none(
                broker_callback_diag.get("first_live_fill_day")
            )
            first_live_order_day = _string_or_none(
                broker_callback_diag.get("first_live_order_day")
            )
            first_live_day_with_order_and_fill = _string_or_none(
                broker_callback_diag.get("first_live_day_with_order_and_fill")
            )
            if first_live_fill_day and first_live_order_day and not first_live_day_with_order_and_fill:
                next_step = (
                    f"{next_step} Live stream has partial data only: fills on {first_live_fill_day}, "
                    f"orders on {first_live_order_day}, no day with both."
                )
            return reason, next_step

        diag = attribution_diagnostics if isinstance(attribution_diagnostics, Mapping) else {}
        source_fact_count = _int_or_none(diag.get("source_fact_count"))
        attributed_fact_count = _int_or_none(diag.get("attributed_fact_count"))
        unattributed_fact_count = _int_or_none(diag.get("unattributed_fact_count"))
        upstream_path = _string_or_none(diag.get("upstream_artifact_path"))
        lineage_requirement_diagnostic = _string_or_none(diag.get("lineage_requirement_diagnostic"))
        detail_parts = []
        if source_fact_count is not None:
            detail_parts.append(f"source_facts={source_fact_count}")
        if attributed_fact_count is not None:
            detail_parts.append(f"attributed_facts={attributed_fact_count}")
        if unattributed_fact_count is not None:
            detail_parts.append(f"unattributed_facts={unattributed_fact_count}")
        detail_suffix = f" ({', '.join(detail_parts)})" if detail_parts else ""
        if lineage_requirement_diagnostic:
            next_step = (
                "Produce order/fill attributed facts with order_id/perm_id/engine_id lineage, "
                "then rerun broker fact spine, reconciled trade state, and sleeve evaluation artifacts."
            )
            if upstream_path:
                next_step = f"{next_step} Source artifact: {upstream_path}"
            return (
                f"Measurement validity failed: {lineage_requirement_diagnostic}{detail_suffix}",
                next_step,
            )
        next_step = "Ensure reconciled trade attribution maps closed trades to sleeve engine IDs, then rerun sleeve evaluation artifacts."
        if upstream_path:
            next_step = f"{next_step} Source artifact: {upstream_path}"
        return (
            f"Measurement validity failed: native engine attribution unavailable{detail_suffix}",
            next_step,
        )
    if "OUTCOME_ATTRIBUTION_CONFIDENCE_INVALID" in code_set:
        return (
            "Measurement validity failed: outcome attribution confidence invalid",
            "Regenerate outcome attribution and sleeve edge measurement after attribution evidence is complete.",
        )
    if "SLEEVE_EDGE_SAMPLE_INSUFFICIENT" in code_set:
        return (
            "Measurement validity failed: insufficient closed-trade sample",
            "Accumulate sufficient closed native trades in the measurement window, then rerun sleeve evaluation artifacts.",
        )
    ordered = [code for code in reason_codes if code and code != "SLEEVE_EDGE_MEASUREMENT_INVALID"]
    if ordered:
        return (
            f"Measurement validity failed: {ordered[0]}",
            "Resolve the measurement blocker in sleeve edge inputs, then rerun sleeve evaluation artifacts.",
        )
    return (
        "Measurement invalid; grade unavailable",
        "Resolve measurement validity and rerun sleeve readiness.",
    )


def _load_sleeve_live_readiness(day: Optional[str]) -> tuple[Dict[str, Any], Optional[Path], List[str]]:
    if not day:
        return {
            "present": False,
            "diagnostic_code": "NO_EVALUATION_DAY",
            "diagnostic_reason": "No evaluation day resolved",
            "diagnostic_next_step": "Resolve sleeve evaluation day, then rerun sleeve live readiness.",
        }, None, []
    readiness_path = (
        SLEEVE_TRUTH_ROOT
        / "readiness_v1"
        / "sleeve_live_readiness_v1"
        / day
        / "sleeve_live_readiness.v1.json"
    ).resolve()
    readiness_doc, readiness_err = read_json_dict(readiness_path)
    if not isinstance(readiness_doc, dict):
        if readiness_err == "FILE_NOT_FOUND":
            return {
                "present": False,
                "artifact_path": str(readiness_path),
                "diagnostic_code": "NO_READINESS_ARTIFACT",
                "diagnostic_reason": "No readiness artifact found",
                "diagnostic_next_step": "Run sleeve live readiness for the active day.",
            }, readiness_path, []
        return {
            "present": False,
            "artifact_path": str(readiness_path),
            "diagnostic_code": "READINESS_RUN_FAILED",
            "diagnostic_reason": "Readiness run failed",
            "diagnostic_next_step": "Inspect readiness artifact generation logs and rerun.",
        }, readiness_path, []

    score_contribution = []
    calibration_support = readiness_doc.get("calibration_support")
    if isinstance(calibration_support, dict) and isinstance(calibration_support.get("score_contribution"), list):
        score_contribution = calibration_support.get("score_contribution") or []
    elif isinstance(readiness_doc.get("checks"), list):
        score_contribution = readiness_doc.get("checks") or []

    factors: List[str] = []
    for row in score_contribution:
        if not isinstance(row, dict):
            continue
        check_id = _string_or_none(row.get("check_id")) or "check"
        score_awarded = _int_or_none(row.get("score_awarded"))
        weight = _int_or_none(row.get("weight"))
        if score_awarded is not None and weight is not None:
            factors.append(f"{check_id}:{score_awarded}/{weight}")
        elif score_awarded is not None:
            factors.append(f"{check_id}:{score_awarded}")
        else:
            factors.append(check_id)

    readiness_grade_1_to_7 = _int_or_none(readiness_doc.get("readiness_grade_1_to_7"))
    score_threshold_grade_1_to_7 = _int_or_none(readiness_doc.get("score_threshold_grade_1_to_7"))
    readiness_grade_scale = _string_or_none(readiness_doc.get("readiness_grade_scale"))
    grading_thresholds_1_to_7 = (
        readiness_doc.get("grading_thresholds_1_to_7")
        if isinstance(readiness_doc.get("grading_thresholds_1_to_7"), list)
        else []
    )
    missing_grade_fields = (
        readiness_grade_1_to_7 is None
        or score_threshold_grade_1_to_7 is None
        or readiness_grade_scale != "1_to_7"
        or not grading_thresholds_1_to_7
    )

    return {
        "present": True,
        "artifact_path": str(readiness_path),
        "sleeve_id": _string_or_none(readiness_doc.get("sleeve_id")),
        "readiness_score": _int_or_none(readiness_doc.get("readiness_score")),
        "score_threshold": _int_or_none(readiness_doc.get("score_threshold")),
        "readiness_grade_1_to_7": readiness_grade_1_to_7,
        "score_threshold_grade_1_to_7": score_threshold_grade_1_to_7,
        "readiness_grade_scale": readiness_grade_scale,
        "grading_thresholds_1_to_7": grading_thresholds_1_to_7,
        "promotion_candidate": readiness_doc.get("promotion_candidate") if isinstance(readiness_doc.get("promotion_candidate"), bool) else None,
        "reason_codes": readiness_doc.get("reason_codes") if isinstance(readiness_doc.get("reason_codes"), list) else [],
        "recommended_next_actions": (
            readiness_doc.get("recommended_next_actions")
            if isinstance(readiness_doc.get("recommended_next_actions"), list)
            else []
        ),
        "factors": factors,
        "diagnostic_code": "ARTIFACT_MISSING_GRADE_FIELDS" if missing_grade_fields else "",
        "diagnostic_reason": "Artifact missing grade fields" if missing_grade_fields else "",
        "diagnostic_next_step": (
            "Regenerate sleeve live readiness artifact with 1-7 grade fields."
            if missing_grade_fields
            else ""
        ),
    }, readiness_path, []


def _load_capital_policy() -> tuple[List[Dict[str, Any]], Optional[Path], List[str]]:
    policy_path = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
    policy_doc, policy_err = read_json_dict(policy_path)
    warnings: List[str] = []
    if policy_doc is None:
        return [], None, [f"CAPITAL_AUTHORITY_POLICY_{policy_err or 'UNREADABLE'}"]
    sleeves = policy_doc.get("sleeves")
    if not isinstance(sleeves, list):
        return [], policy_path, ["CAPITAL_AUTHORITY_POLICY_SLEEVES_INVALID"]
    rows = [row for row in sleeves if isinstance(row, dict)]
    if not rows:
        warnings.append("CAPITAL_AUTHORITY_POLICY_SLEEVES_EMPTY")
    return rows, policy_path, warnings


def _load_bond_sleeve_registry_row() -> tuple[Optional[Dict[str, Any]], Optional[Path], List[str]]:
    registry_path = (REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
    registry_doc, registry_err = read_json_dict(registry_path)
    if not isinstance(registry_doc, dict):
        return None, registry_path, [f"SLEEVE_REGISTRY_{registry_err or 'UNREADABLE'}"]
    sleeves = registry_doc.get("sleeves")
    if not isinstance(sleeves, list):
        return None, registry_path, ["SLEEVE_REGISTRY_SLEEVES_INVALID"]
    for row in sleeves:
        if isinstance(row, dict) and str(row.get("sleeve_id") or "").strip().upper() == "BOND":
            return row, registry_path, []
    return None, registry_path, []


def _latest_bond_recommendation_for_ui(day: Optional[str]) -> Dict[str, Any]:
    day_text = _string_or_none(day)
    roots = [
        (GLOBAL_TRUTH_ROOT / "reports" / "bond_sleeve_recommendation_v2").resolve(),
        (SLEEVE_TRUTH_ROOT / "bond_decision_artifact_v1").resolve(),
    ]
    candidates: List[tuple[str, Path, str]] = []
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        for day_dir in root.iterdir():
            if not day_dir.is_dir() or not _string_or_none(day_dir.name):
                continue
            if root.name == "bond_sleeve_recommendation_v2":
                artifact = (day_dir / "bond_sleeve_recommendation.v2.json").resolve()
                source = "bond_sleeve_recommendation_v2"
            else:
                artifact = (day_dir / "bond_decision_artifact.v1.json").resolve()
                source = "bond_decision_artifact_v1"
            if artifact.exists() and artifact.is_file():
                candidates.append((day_dir.name, artifact, source))
    if not candidates:
        return {"present": False}
    same_day = [row for row in candidates if day_text and row[0] == day_text]
    selected_day, path, source = sorted(same_day or candidates, key=lambda row: (row[0], str(row[1])))[-1]
    doc, _err = read_json_dict(path)
    if not isinstance(doc, dict):
        return {"present": False, "artifact_path": str(path)}
    if source == "bond_sleeve_recommendation_v2":
        state = _string_or_none(doc.get("recommendation_state")) or _string_or_none(doc.get("action_state")) or "MANUAL_REVIEW"
        produced = _string_or_none(doc.get("produced_utc"))
        next_step = _string_or_none(doc.get("operator_next_step"))
    else:
        summary = doc.get("decision_summary") if isinstance(doc.get("decision_summary"), Mapping) else {}
        state = _string_or_none(summary.get("rebalance_state")) or _string_or_none(doc.get("authority_status")) or "MANUAL_REVIEW"
        produced = _string_or_none(doc.get("created_at"))
        next_step = "Review the bond recommendation manually; automated execution is not enabled."
    return {
        "present": True,
        "day_utc": selected_day,
        "artifact_path": str(path),
        "source": source,
        "produced_utc": produced,
        "recommendation_state": state.upper(),
        "operator_next_step": next_step,
    }


def _bond_manual_sleeve_row(day: Optional[str], registry_row: Optional[Dict[str, Any]], registry_ref: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(registry_row, dict):
        return None
    if str(registry_row.get("execution_mode") or "").strip().upper() != "MANUAL":
        return None
    if registry_row.get("ui_visible") is False or not bool(registry_row.get("enabled")):
        return None
    latest = _latest_bond_recommendation_for_ui(day)
    recommendation_state = _string_or_none(latest.get("recommendation_state")) if latest.get("present") else "unavailable"
    as_of_utc = _string_or_none(latest.get("produced_utc"))
    refs = evidence_refs(registry_ref)
    artifact_path = _string_or_none(latest.get("artifact_path"))
    if artifact_path:
        refs = evidence_refs(registry_ref, evidence_ref(Path(artifact_path), label="BOND Manual Recommendation", artifact_type=str(latest.get("source") or "bond_manual_recommendation")))
    return {
        "entity_id": "BOND",
        "sleeve_id": "BOND",
        "display_name": _string_or_none(registry_row.get("display_name")) or "Bond Sleeve",
        "engine_ids": [],
        "priority_rank": registry_row.get("priority_rank") if isinstance(registry_row.get("priority_rank"), int) else 900,
        "execution_scope_id": "MANUAL_ADVISORY",
        "mode": _string_or_none(registry_row.get("mode")) or "PAPER",
        "execution_mode": "MANUAL",
        "execution_label": "Manual execution",
        "advisory_label": "Advisory only",
        "manual_execution_only": True,
        "advisory_only": True,
        "broker_execution_allowed": False,
        "automated_execution_allowed": False,
        "asset_class": _string_or_none(registry_row.get("asset_class")) or "FIXED_INCOME",
        "target_allocation_pct": None,
        "target_allocation_status": "MANUAL_POLICY",
        "actual_allocation_pct": None,
        "actual_value_usd": None,
        "effective_budget_usd": None,
        "effective_budget_status": "MANUAL_ADVISORY_ONLY",
        "effective_risk_budget_usd": None,
        "used_risk_budget_usd": None,
        "headroom_usd": None,
        "performance_metrics": {"lookback_window": {}, "sample_trade_count": None, "warnings": []},
        "drawdown_metrics": {"max_drawdown_pct": None, "drawdown_status": "MANUAL_ADVISORY_ONLY", "warnings": []},
        "stability_metrics": {"drift_band": "MANUAL", "stability_state": "manual_advisory", "sample_sufficiency_band": "N/A", "confidence_state": "MANUAL", "execution_health_band": "MANUAL"},
        "qualification_state": "MANUAL_ADVISORY",
        "edge_band": "MANUAL_ADVISORY",
        "tax_efficiency_summary": {"status": "UNAVAILABLE", "warnings": []},
        "recommendation": {
            "recommendation_state": recommendation_state,
            "state_class": "MANUAL_ADVISORY",
            "alignment_state": "MANUAL_REVIEW",
            "control_state": "advisory_only",
            "backend_reason_codes": ["BOND_MANUAL_ADVISORY_ONLY"],
        },
        "latest_evaluation": latest,
        "evidence_summary": {"source_artifact_count": 1 if latest.get("present") else 0, "sample_trade_count": None, "closure_state": "MANUAL_ADVISORY", "first_blocker_code": ""},
        "policy_limits": {"max_capital_at_risk_usd": None, "max_symbols": None, "max_single_name_notional_pct": None, "max_sector_concentration_pct": None},
        "truth_state": "derived",
        "as_of_utc": as_of_utc,
        "freshness_state": freshness_state(as_of_utc),
        "source_authority": ["C2_SLEEVE_REGISTRY_V1", "bond_sleeve_recommendation_v2", "bond_decision_artifact_v1"],
        "provenance_refs": refs,
        "degradation_codes": [] if latest.get("present") else ["BOND_EVALUATION_NOT_AVAILABLE"],
        "readiness_grade_1_to_7": None,
        "score_threshold_grade_1_to_7": None,
        "readiness_grade_scale": None,
        "grading_thresholds_1_to_7": [],
        "readiness_score": None,
        "readiness_score_threshold": None,
        "readiness_promotion_candidate": False,
        "readiness_threshold_met": None,
        "readiness_contributing_factors": [],
        "readiness_reason_codes": [],
        "measurement_reason_codes": [],
        "measurement_attribution_diagnostics": {},
        "readiness_grade_reason": "Manual/advisory bond sleeve; automated readiness artifacts are not required.",
        "readiness_grade_next_step": _string_or_none(latest.get("operator_next_step")) or "Review bond sleeve manually when a recommendation is available.",
        "readiness_diagnostic_code": "MANUAL_ADVISORY_NO_AUTOMATED_READINESS_REQUIRED",
        "readiness_artifact_path": None,
    }


def build_sleeve_evaluation_view(day: Optional[str] = None) -> Dict[str, Any]:
    resolved_day = _resolve_sleeve_day(day)
    policy_rows, policy_path, policy_warnings = _load_capital_policy()
    bond_registry_row, bond_registry_path, bond_registry_warnings = _load_bond_sleeve_registry_row()
    sleeve_readiness, sleeve_readiness_path, sleeve_readiness_warnings = _load_sleeve_live_readiness(resolved_day)

    allocation_path = (
        SLEEVE_TRUTH_ROOT / "allocation_v1" / "capital_authority_allocation_v1" / (resolved_day or "UNKNOWN") / "capital_authority_allocation.v1.json"
    ).resolve()
    allocation_doc, allocation_err = read_json_dict(allocation_path)
    allocation_rows = allocation_doc.get("per_sleeve") if isinstance(allocation_doc, dict) and isinstance(allocation_doc.get("per_sleeve"), list) else []
    allocation_by_sleeve = {
        _string_or_none(row.get("sleeve_id")): row
        for row in allocation_rows
        if isinstance(row, dict) and _string_or_none(row.get("sleeve_id"))
    }

    policy_ref = evidence_ref(policy_path if policy_path else None, label="Capital Authority Policy", artifact_type="C2_CAPITAL_AUTHORITY_POLICY_V1")
    allocation_ref = evidence_ref(
        allocation_path if allocation_doc else None,
        label="Capital Authority Allocation",
        artifact_type="capital_authority_allocation_v1",
    )
    sleeve_readiness_ref = evidence_ref(
        sleeve_readiness_path if sleeve_readiness else None,
        label="Sleeve Live Readiness",
        artifact_type="C2_SLEEVE_LIVE_READINESS_V1",
    )
    bond_registry_ref = evidence_ref(
        bond_registry_path if bond_registry_path else None,
        label="Sleeve Registry",
        artifact_type="C2_SLEEVE_REGISTRY_V1",
    )

    rows: List[Dict[str, Any]] = []
    sleeve_warnings: List[str] = list(policy_warnings) + list(bond_registry_warnings) + list(sleeve_readiness_warnings)
    source_refs: List[Dict[str, Any]] = evidence_refs(policy_ref, allocation_ref, sleeve_readiness_ref, bond_registry_ref)
    top_as_of_candidates: List[str] = []

    for policy_row in policy_rows:
        sleeve_id = _string_or_none(policy_row.get("sleeve_id"))
        if not sleeve_id:
            continue
        display_name = _string_or_none(policy_row.get("display_name")) or sleeve_id
        engine_ids = [str(item).strip() for item in policy_row.get("engine_ids") or [] if str(item).strip()]
        limits = policy_row.get("limits") if isinstance(policy_row.get("limits"), dict) else {}
        allocation_row = allocation_by_sleeve.get(sleeve_id, {})

        evaluation_manifest_path = (
            SLEEVE_TRUTH_ROOT / "reports" / "evaluation_input_manifest_v1" / (resolved_day or "UNKNOWN") / sleeve_id / "evaluation_input_manifest.v1.json"
        ).resolve()
        measurement_path = (
            SLEEVE_TRUTH_ROOT
            / "reports"
            / "sleeve_edge_measurement_snapshot_v1"
            / (resolved_day or "UNKNOWN")
            / sleeve_id
            / "sleeve_edge_measurement_snapshot.v1.json"
        ).resolve()
        governance_path = (
            SLEEVE_TRUTH_ROOT / "reports" / "allocation_governance_snapshot_v1" / (resolved_day or "UNKNOWN") / sleeve_id / "allocation_governance_snapshot.v1.json"
        ).resolve()

        evaluation_manifest_doc, evaluation_manifest_err = read_json_dict(evaluation_manifest_path)
        measurement_doc, measurement_err = read_json_dict(measurement_path)
        governance_doc, governance_err = read_json_dict(governance_path)
        evaluation_day_latest = (
            _latest_day_for_sleeve_report_family("evaluation_input_manifest_v1", sleeve_id)
            if evaluation_manifest_doc is None
            else None
        )
        measurement_day_latest = (
            _latest_day_for_sleeve_report_family("sleeve_edge_measurement_snapshot_v1", sleeve_id)
            if measurement_doc is None
            else None
        )
        governance_day_latest = (
            _latest_day_for_sleeve_report_family("allocation_governance_snapshot_v1", sleeve_id)
            if governance_doc is None
            else None
        )
        legacy_diagnostics = _legacy_snapshot_diagnostics(
            allocation_row.get("qualification_snapshot_path") if isinstance(allocation_row, dict) else None
        )

        evaluation_manifest_ref = evidence_ref(
            evaluation_manifest_path if evaluation_manifest_doc else None,
            label=f"{sleeve_id} Evaluation Input",
            artifact_type="evaluation_input_manifest_v1",
        )
        measurement_ref = evidence_ref(
            measurement_path if measurement_doc else None,
            label=f"{sleeve_id} Sleeve Edge Measurement",
            artifact_type="sleeve_edge_measurement_snapshot_v1",
        )
        governance_ref = evidence_ref(
            governance_path if governance_doc else None,
            label=f"{sleeve_id} Allocation Governance",
            artifact_type="allocation_governance_snapshot_v1",
        )
        legacy_snapshot_path_text = _string_or_none(allocation_row.get("qualification_snapshot_path")) if isinstance(allocation_row, dict) else None
        legacy_snapshot_path = Path(legacy_snapshot_path_text).resolve() if legacy_snapshot_path_text else None
        legacy_snapshot_ref = evidence_ref(
            legacy_snapshot_path if legacy_snapshot_path and legacy_snapshot_path.exists() else None,
            label=f"{sleeve_id} Legacy Sleeve Edge Snapshot",
            artifact_type="sleeve_edge_snapshot_v1",
        )

        row_warnings: List[str] = []
        if evaluation_manifest_doc is None:
            row_warnings.append(f"EVALUATION_INPUT_MANIFEST_{evaluation_manifest_err or 'UNREADABLE'}")
        if measurement_doc is None:
            row_warnings.append(f"SLEEVE_EDGE_MEASUREMENT_{measurement_err or 'UNREADABLE'}")
        if governance_doc is None:
            row_warnings.append(f"ALLOCATION_GOVERNANCE_{governance_err or 'UNREADABLE'}")
        if not isinstance(allocation_row, dict) or not allocation_row:
            row_warnings.append("CAPITAL_AUTHORITY_ALLOCATION_ROW_MISSING")
        row_warnings.append("TARGET_ALLOCATION_POLICY_UNPROVEN")
        row_warnings.append("TAX_EFFICIENCY_SUMMARY_UNAVAILABLE")

        measurement_window = measurement_doc.get("measurement_window") if isinstance(measurement_doc, dict) and isinstance(measurement_doc.get("measurement_window"), dict) else {}
        evidence_summary = (
            evaluation_manifest_doc.get("evidence_summary")
            if isinstance(evaluation_manifest_doc, dict) and isinstance(evaluation_manifest_doc.get("evidence_summary"), dict)
            else {}
        )
        edge_measurement = (
            measurement_doc.get("edge_measurement")
            if isinstance(measurement_doc, dict) and isinstance(measurement_doc.get("edge_measurement"), dict)
            else {}
        )
        stability_measurement = (
            measurement_doc.get("stability_measurement")
            if isinstance(measurement_doc, dict) and isinstance(measurement_doc.get("stability_measurement"), dict)
            else {}
        )
        confidence_measurement = (
            measurement_doc.get("confidence_measurement")
            if isinstance(measurement_doc, dict) and isinstance(measurement_doc.get("confidence_measurement"), dict)
            else {}
        )
        current_control_context = (
            governance_doc.get("current_control_context")
            if isinstance(governance_doc, dict) and isinstance(governance_doc.get("current_control_context"), dict)
            else {}
        )

        row_as_of_utc = latest_timestamp(
            _string_or_none(evaluation_manifest_doc.get("produced_utc") if isinstance(evaluation_manifest_doc, dict) else None),
            _string_or_none(measurement_doc.get("produced_utc") if isinstance(measurement_doc, dict) else None),
            _string_or_none(governance_doc.get("produced_utc") if isinstance(governance_doc, dict) else None),
            _string_or_none(allocation_doc.get("produced_utc") if isinstance(allocation_doc, dict) else None),
        )
        if row_as_of_utc:
            top_as_of_candidates.append(row_as_of_utc)

        row_refs = evidence_refs(policy_ref, allocation_ref, evaluation_manifest_ref, measurement_ref, governance_ref, legacy_snapshot_ref)
        source_refs.extend(row_refs)
        execution_scope_id = _string_or_none(evaluation_manifest_doc.get("execution_sleeve_id") if isinstance(evaluation_manifest_doc, dict) else None)
        readiness_sleeve_id = _string_or_none(sleeve_readiness.get("sleeve_id"))
        readiness_present = bool(sleeve_readiness.get("present"))
        readiness_match = readiness_present and (
            not readiness_sleeve_id
            or readiness_sleeve_id == sleeve_id
            or (execution_scope_id and readiness_sleeve_id == execution_scope_id)
        )
        qualification_state = (
            _string_or_none(edge_measurement.get("qualification_state"))
            or _string_or_none(allocation_row.get("qualification_state") if isinstance(allocation_row, dict) else None)
            or "UNAVAILABLE"
        )
        measurement_invalid = qualification_state.upper() == "MEASUREMENT_INVALID"
        measurement_reason_codes = [
            str(code).strip()
            for code in (
                (measurement_doc.get("reason_codes") if isinstance(measurement_doc, dict) else None)
                or legacy_diagnostics.get("reason_codes")
                or legacy_diagnostics.get("invalidity_reasons")
                or []
            )
            if str(code).strip()
        ]
        measurement_reason_codes = list(dict.fromkeys(measurement_reason_codes))
        measurement_attribution_diagnostics = (
            dict(measurement_doc.get("attribution_diagnostics") or {})
            if isinstance(measurement_doc, dict) and isinstance(measurement_doc.get("attribution_diagnostics"), dict)
            else {}
        )

        readiness_grade_1_to_7 = sleeve_readiness.get("readiness_grade_1_to_7") if readiness_match and not measurement_invalid else None
        readiness_threshold_grade_1_to_7 = (
            sleeve_readiness.get("score_threshold_grade_1_to_7")
            if readiness_match and not measurement_invalid
            else None
        )
        readiness_grade_scale = sleeve_readiness.get("readiness_grade_scale") if readiness_match and not measurement_invalid else None
        grading_thresholds_1_to_7 = (
            list(sleeve_readiness.get("grading_thresholds_1_to_7") or [])
            if readiness_match and not measurement_invalid
            else []
        )
        readiness_reason = ""
        readiness_next_step = ""
        readiness_code = ""
        if evaluation_manifest_doc is None:
            if evaluation_day_latest and resolved_day and evaluation_day_latest != resolved_day:
                readiness_code = "EVALUATION_INPUT_DAY_MISMATCH"
                readiness_reason = "Evaluation input manifest day mismatch"
                readiness_next_step = (
                    f"Resolved day {resolved_day} has no manifest for {sleeve_id}; latest available day is {evaluation_day_latest}."
                )
            else:
                readiness_code = "EVALUATION_INPUT_MANIFEST_MISSING"
                readiness_reason = "Evaluation input manifest missing"
                readiness_next_step = "Generate evaluation input manifest for this sleeve/day."
        elif measurement_doc is None:
            if measurement_day_latest and resolved_day and measurement_day_latest != resolved_day:
                readiness_code = "SLEEVE_EDGE_MEASUREMENT_DAY_MISMATCH"
                readiness_reason = "Sleeve edge measurement snapshot day mismatch"
                readiness_next_step = (
                    f"Resolved day {resolved_day} has no measurement snapshot for {sleeve_id}; latest available day is {measurement_day_latest}."
                )
            else:
                readiness_code = "NO_SLEEVE_EDGE_MEASUREMENT_SNAPSHOT"
                readiness_reason = "No sleeve edge measurement snapshot found"
                readiness_next_step = "Generate sleeve edge measurement snapshot for this sleeve/day."
        elif governance_doc is None:
            if governance_day_latest and resolved_day and governance_day_latest != resolved_day:
                readiness_code = "ALLOCATION_GOVERNANCE_DAY_MISMATCH"
                readiness_reason = "Allocation governance snapshot day mismatch"
                readiness_next_step = (
                    f"Resolved day {resolved_day} has no allocation governance snapshot for {sleeve_id}; latest available day is {governance_day_latest}."
                )
            else:
                readiness_code = "ALLOCATION_GOVERNANCE_SNAPSHOT_MISSING"
                readiness_reason = "Allocation governance snapshot missing"
                readiness_next_step = "Generate allocation governance snapshot for this sleeve/day."
        elif measurement_invalid:
            readiness_code = "MEASUREMENT_INVALID"
            readiness_reason, readiness_next_step = _measurement_invalid_reason(
                measurement_reason_codes,
                measurement_attribution_diagnostics,
                evaluation_day=resolved_day,
            )
        elif not readiness_present:
            readiness_code = _string_or_none(sleeve_readiness.get("diagnostic_code")) or "NO_READINESS_ARTIFACT"
            readiness_reason = _string_or_none(sleeve_readiness.get("diagnostic_reason")) or "No readiness artifact found"
            readiness_next_step = (
                _string_or_none(sleeve_readiness.get("diagnostic_next_step"))
                or "Run sleeve live readiness for the active day."
            )
        elif not readiness_match:
            readiness_code = "READINESS_SLEEVE_ID_MISMATCH"
            readiness_reason = "Sleeve ID did not match readiness artifact"
            readiness_next_step = (
                f"Artifact sleeve_id={readiness_sleeve_id or 'n/a'} "
                f"did not match sleeve_id={sleeve_id} execution_scope_id={execution_scope_id or 'n/a'}."
            )
        elif _string_or_none(sleeve_readiness.get("diagnostic_code")) == "ARTIFACT_MISSING_GRADE_FIELDS":
            readiness_code = "ARTIFACT_MISSING_GRADE_FIELDS"
            readiness_reason = "Artifact missing grade fields"
            readiness_next_step = (
                _string_or_none(sleeve_readiness.get("diagnostic_next_step"))
                or "Regenerate sleeve live readiness artifact with 1-7 fields."
            )
        readiness_threshold_met = (
            isinstance(readiness_grade_1_to_7, int)
            and isinstance(readiness_threshold_grade_1_to_7, int)
            and readiness_grade_1_to_7 >= readiness_threshold_grade_1_to_7
        )

        rows.append(
            {
                "entity_id": sleeve_id,
                "sleeve_id": sleeve_id,
                "display_name": display_name,
                "engine_ids": engine_ids,
                "priority_rank": policy_row.get("priority_rank"),
                "execution_scope_id": execution_scope_id,
                "mode": _string_or_none(evaluation_manifest_doc.get("mode") if isinstance(evaluation_manifest_doc, dict) else None),
                "target_allocation_pct": None,
                "target_allocation_status": "UNAVAILABLE_NOT_PROVEN",
                "actual_allocation_pct": _float_from_text(allocation_row.get("actual_notional_pct")) if isinstance(allocation_row, dict) else None,
                "actual_value_usd": _usd_from_cents(allocation_row.get("actual_value_cents")) if isinstance(allocation_row, dict) else None,
                "effective_budget_usd": None,
                "effective_budget_status": "UNAVAILABLE_NON_RISK_BUDGET",
                "effective_risk_budget_usd": _usd_from_cents(allocation_row.get("allowed_capital_at_risk_cents")) if isinstance(allocation_row, dict) else None,
                "used_risk_budget_usd": _usd_from_cents(allocation_row.get("used_capital_at_risk_cents")) if isinstance(allocation_row, dict) else None,
                "headroom_usd": _usd_from_cents(allocation_row.get("headroom_cents")) if isinstance(allocation_row, dict) else None,
                "performance_metrics": {
                    "lookback_window": measurement_window,
                    "sample_trade_count": measurement_doc.get("sample_trade_count") if isinstance(measurement_doc, dict) else None,
                    "included_trade_count": evidence_summary.get("included_trade_count"),
                    "excluded_trade_count": evidence_summary.get("excluded_trade_count"),
                    "realized_return_pct": None,
                    "annualized_return_pct": None,
                    "warnings": ["REALIZED_PERFORMANCE_METRICS_UNAVAILABLE"],
                },
                "drawdown_metrics": {
                    "max_drawdown_pct": None,
                    "drawdown_status": "UNAVAILABLE",
                    "warnings": ["DRAWDOWN_METRICS_UNAVAILABLE"],
                },
                "stability_metrics": {
                    "drift_band": _string_or_none(stability_measurement.get("drift_band"))
                    or _string_or_none(allocation_row.get("drift_band") if isinstance(allocation_row, dict) else None)
                    or "UNKNOWN",
                    "stability_state": _string_or_none(stability_measurement.get("stability_state")) or "unavailable",
                    "sample_sufficiency_band": _string_or_none(confidence_measurement.get("sample_sufficiency_band"))
                    or _string_or_none(allocation_row.get("sample_sufficiency_band") if isinstance(allocation_row, dict) else None)
                    or "UNKNOWN",
                    "confidence_state": _string_or_none(confidence_measurement.get("confidence_state")) or "UNAVAILABLE",
                    "execution_health_band": _string_or_none(confidence_measurement.get("execution_health_band"))
                    or _string_or_none(allocation_row.get("execution_health_band") if isinstance(allocation_row, dict) else None)
                    or "UNKNOWN",
                },
                "qualification_state": qualification_state,
                "edge_band": _string_or_none(edge_measurement.get("edge_band"))
                or _string_or_none(allocation_row.get("edge_band") if isinstance(allocation_row, dict) else None)
                or "UNKNOWN",
                "tax_efficiency_summary": {
                    "status": "UNAVAILABLE",
                    "warnings": ["TAX_EFFICIENCY_SUMMARY_UNAVAILABLE"],
                },
                "recommendation": {
                    "recommendation_state": _string_or_none(governance_doc.get("recommended_action_state") if isinstance(governance_doc, dict) else None)
                    or "unavailable",
                    "state_class": _string_or_none(governance_doc.get("recommended_state_class") if isinstance(governance_doc, dict) else None)
                    or "UNAVAILABLE",
                    "alignment_state": _string_or_none(governance_doc.get("allocation_alignment_state") if isinstance(governance_doc, dict) else None)
                    or "UNAVAILABLE",
                    "control_state": _string_or_none(current_control_context.get("control_state")) or "unavailable",
                    "backend_reason_codes": (
                        governance_doc.get("recommendation_reason_codes")
                        if isinstance(governance_doc, dict) and isinstance(governance_doc.get("recommendation_reason_codes"), list)
                        else []
                    ),
                },
                "evidence_summary": {
                    "source_artifact_count": evidence_summary.get("source_artifact_count"),
                    "sample_trade_count": evidence_summary.get("sample_trade_count"),
                    "closure_state": _string_or_none(evaluation_manifest_doc.get("closure_state") if isinstance(evaluation_manifest_doc, dict) else None)
                    or _string_or_none(measurement_doc.get("closure_state") if isinstance(measurement_doc, dict) else None)
                    or _string_or_none(governance_doc.get("closure_state") if isinstance(governance_doc, dict) else None)
                    or "UNKNOWN",
                    "first_blocker_code": _string_or_none(evaluation_manifest_doc.get("first_blocker_code") if isinstance(evaluation_manifest_doc, dict) else None)
                    or _string_or_none(measurement_doc.get("first_blocker_code") if isinstance(measurement_doc, dict) else None)
                    or _string_or_none(governance_doc.get("first_blocker_code") if isinstance(governance_doc, dict) else None)
                    or "",
                },
                "policy_limits": {
                    "max_capital_at_risk_usd": _usd_from_cents(limits.get("max_capital_at_risk_cents")),
                    "max_symbols": limits.get("max_symbols"),
                    "max_single_name_notional_pct": _float_from_text(limits.get("max_single_name_notional_pct")),
                    "max_sector_concentration_pct": _float_from_text(limits.get("max_sector_concentration_pct")),
                },
                "truth_state": "derived" if row_warnings else "canonical",
                "as_of_utc": row_as_of_utc,
                "freshness_state": freshness_state(row_as_of_utc),
                "source_authority": [
                    "C2_CAPITAL_AUTHORITY_POLICY_V1",
                    "capital_authority_allocation_v1",
                    "evaluation_input_manifest_v1",
                    "sleeve_edge_measurement_snapshot_v1",
                    "allocation_governance_snapshot_v1",
                ],
                "provenance_refs": row_refs,
                "degradation_codes": row_warnings,
                "readiness_grade_1_to_7": readiness_grade_1_to_7,
                "score_threshold_grade_1_to_7": readiness_threshold_grade_1_to_7,
                "readiness_grade_scale": readiness_grade_scale,
                "grading_thresholds_1_to_7": grading_thresholds_1_to_7,
                "readiness_score": sleeve_readiness.get("readiness_score") if readiness_match and not measurement_invalid else None,
                "readiness_score_threshold": sleeve_readiness.get("score_threshold") if readiness_match and not measurement_invalid else None,
                "readiness_promotion_candidate": sleeve_readiness.get("promotion_candidate") if readiness_match and not measurement_invalid else None,
                "readiness_threshold_met": readiness_threshold_met if readiness_match and not measurement_invalid else None,
                "readiness_contributing_factors": list(sleeve_readiness.get("factors") or []) if readiness_match and not measurement_invalid else [],
                "readiness_reason_codes": list(sleeve_readiness.get("reason_codes") or []) if readiness_match else [],
                "measurement_reason_codes": measurement_reason_codes,
                "measurement_attribution_diagnostics": measurement_attribution_diagnostics,
                "readiness_grade_reason": readiness_reason,
                "readiness_grade_next_step": readiness_next_step,
                "readiness_diagnostic_code": readiness_code,
                "readiness_artifact_path": _string_or_none(sleeve_readiness.get("artifact_path")),
            }
        )

    bond_row = _bond_manual_sleeve_row(resolved_day, bond_registry_row, bond_registry_ref)
    if bond_row is not None:
        rows.append(bond_row)
        bond_as_of = _string_or_none(bond_row.get("as_of_utc"))
        if bond_as_of:
            top_as_of_candidates.append(bond_as_of)
        source_refs.extend(list(bond_row.get("provenance_refs") or []))

    if allocation_doc is None:
        sleeve_warnings.append(f"CAPITAL_AUTHORITY_ALLOCATION_{allocation_err or 'UNREADABLE'}")

    as_of_utc = latest_timestamp(*top_as_of_candidates)
    summary_diagnostic_reason = _string_or_none(sleeve_readiness.get("diagnostic_reason"))
    summary_diagnostic_next_step = _string_or_none(sleeve_readiness.get("diagnostic_next_step"))
    if not summary_diagnostic_reason:
        for row in rows:
            if _string_or_none(row.get("readiness_diagnostic_code")) != "MEASUREMENT_INVALID":
                continue
            row_reason = _string_or_none(row.get("readiness_grade_reason"))
            if not row_reason:
                continue
            summary_diagnostic_reason = row_reason
            summary_diagnostic_next_step = _string_or_none(row.get("readiness_grade_next_step"))
            break

    payload = view_envelope(
        view_name="sleeve_evaluation_state",
        as_of_utc=as_of_utc,
        freshness_state=freshness_state(as_of_utc),
        provenance_markers=markers("canonical", "derived"),
        source_refs=source_refs,
        surface_kind="projection",
        entity_scope="sleeves",
        truth_state="derived" if sleeve_warnings else "canonical",
        data_condition="degraded" if sleeve_warnings else "fresh",
        source_authority=[
            "C2_CAPITAL_AUTHORITY_POLICY_V1",
            "capital_authority_allocation_v1",
            "evaluation_input_manifest_v1",
            "sleeve_edge_measurement_snapshot_v1",
            "allocation_governance_snapshot_v1",
        ],
        contract_id="sleeve_evaluation_projection",
        contract_version="v1",
        provenance_refs=source_refs,
        degradation_codes=sleeve_warnings,
        current_day=resolved_day,
        sleeve_status="DEGRADED" if sleeve_warnings else "OK",
        sleeve_warnings=sleeve_warnings,
        sleeves=rows,
        sleeve_registry_summary={
            "total_sleeves": len(rows),
            "evaluated_sleeves": sum(1 for row in rows if row["recommendation"]["recommendation_state"] != "unavailable"),
            "source_refs": evidence_refs(policy_ref, allocation_ref, sleeve_readiness_ref, bond_registry_ref),
            "warnings": ["TARGET_ALLOCATION_POLICY_UNPROVEN"],
        },
        sleeve_live_readiness_summary={
            "present": bool(sleeve_readiness.get("present")),
            "artifact_path": _string_or_none(sleeve_readiness.get("artifact_path")),
            "sleeve_id": sleeve_readiness.get("sleeve_id"),
            "readiness_grade_1_to_7": sleeve_readiness.get("readiness_grade_1_to_7"),
            "score_threshold_grade_1_to_7": sleeve_readiness.get("score_threshold_grade_1_to_7"),
            "readiness_grade_scale": sleeve_readiness.get("readiness_grade_scale"),
            "grading_thresholds_1_to_7": list(sleeve_readiness.get("grading_thresholds_1_to_7") or []),
            "readiness_score": sleeve_readiness.get("readiness_score"),
            "score_threshold": sleeve_readiness.get("score_threshold"),
            "promotion_candidate": sleeve_readiness.get("promotion_candidate"),
            "contributing_factors": list(sleeve_readiness.get("factors") or []),
            "reason_codes": list(sleeve_readiness.get("reason_codes") or []),
            "diagnostic_code": (
                _string_or_none(sleeve_readiness.get("diagnostic_code"))
                or (
                    "MEASUREMENT_INVALID"
                    if summary_diagnostic_reason
                    and any(
                        _string_or_none(row.get("readiness_diagnostic_code")) == "MEASUREMENT_INVALID"
                        for row in rows
                    )
                    else None
                )
            ),
            "diagnostic_reason": summary_diagnostic_reason,
            "diagnostic_next_step": summary_diagnostic_next_step,
            "threshold_met": (
                sleeve_readiness.get("readiness_grade_1_to_7") >= sleeve_readiness.get("score_threshold_grade_1_to_7")
                if isinstance(sleeve_readiness.get("readiness_grade_1_to_7"), int)
                and isinstance(sleeve_readiness.get("score_threshold_grade_1_to_7"), int)
                else None
            ),
        },
    )
    validation = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation
    if not validation.get("ok"):
        payload["_metadata_errors"] = validation
    return payload
