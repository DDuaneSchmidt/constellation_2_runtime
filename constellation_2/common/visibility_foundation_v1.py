from __future__ import annotations

import json
import re
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.common.diagnostic_foundation_v1 import (
    _git_sha,
    _read_json_obj,
    _require_day_utc,
    _require_produced_utc,
    select_attempt_for_day,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    SchemaValidationError,
    validate_against_repo_schema_v1,
)
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import (
    RefreshWriteResultV1,
    write_day_artifact_refreshable_v1,
)


VISIBILITY_RULESET_ID = "BATCH2_VISIBILITY_RULESET"
VISIBILITY_RULESET_VERSION = 1
METRIC_REGISTRY_ID = "C2_VISIBILITY_METRIC_REGISTRY_V1"
METRIC_REGISTRY_VERSION = 1

REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_VISIBILITY_METRIC_REGISTRY_V1.json"
FUNNEL_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/funnel_metrics.v1.schema.json"
DRIFT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/drift_report.v1.schema.json"
SUMMARY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/daily_summary.v1.schema.json"
SNAPSHOT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/visibility_metric_snapshot.v1.schema.json"
LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/visibility_decision_ledger.v1.schema.json"

GATE_STACK_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/gate_stack_verdict.v1.schema.json"
ROOT_CAUSE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/root_cause.v1.schema.json"
INTENT_ABSENCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/intent_absence_analysis.v1.schema.json"
DIAGNOSTIC_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/diagnostic_decision_ledger.v1.schema.json"
FLOW_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/activity_flow_diagnostics.v1.schema.json"
REGRESSION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/runtime_regression_analytics.v1.schema.json"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class SourceArtifact:
    artifact_name: str
    path: Path
    schema_relpath: str
    required: bool
    present: bool
    valid: bool
    doc: Optional[Dict[str, Any]]
    error_code: str
    error_detail: str


@dataclass(frozen=True)
class EligibilityDecision:
    eligibility_status: str
    integrity_status: str
    reasons: Tuple[str, ...]


def _read_registry(repo_root: Path) -> Dict[str, Any]:
    return _read_json_obj((repo_root / REGISTRY_RELPATH).resolve())


def _registry_metrics_by_id(registry: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    metrics = registry.get("metrics")
    if not isinstance(metrics, list):
        raise SystemExit(f"FAIL: invalid metric registry metrics list: {REGISTRY_RELPATH}")
    out: Dict[str, Dict[str, Any]] = {}
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        metric_id = str(metric.get("metric_id") or "").strip()
        if not metric_id:
            continue
        out[metric_id] = metric
    return out


def _load_source(
    *,
    repo_root: Path,
    artifact_name: str,
    path: Path,
    schema_relpath: str,
    required: bool,
) -> SourceArtifact:
    if not path.exists() or not path.is_file():
        return SourceArtifact(
            artifact_name=artifact_name,
            path=path,
            schema_relpath=schema_relpath,
            required=required,
            present=False,
            valid=False,
            doc=None,
            error_code="MISSING_REQUIRED_INPUT" if required else "OPTIONAL_INPUT_MISSING",
            error_detail=str(path),
        )
    try:
        doc = _read_json_obj(path)
        validate_against_repo_schema_v1(doc, repo_root, schema_relpath)
    except SchemaValidationError as exc:
        return SourceArtifact(
            artifact_name=artifact_name,
            path=path,
            schema_relpath=schema_relpath,
            required=required,
            present=True,
            valid=False,
            doc=None,
            error_code="SCHEMA_MISMATCH",
            error_detail=f"{path}: {exc}",
        )
    except SystemExit as exc:
        return SourceArtifact(
            artifact_name=artifact_name,
            path=path,
            schema_relpath=schema_relpath,
            required=required,
            present=True,
            valid=False,
            doc=None,
            error_code="PARSE_OR_SHAPE_FAILURE",
            error_detail=f"{path}: {exc}",
        )
    return SourceArtifact(
        artifact_name=artifact_name,
        path=path,
        schema_relpath=schema_relpath,
        required=required,
        present=True,
        valid=True,
        doc=doc,
        error_code="",
        error_detail="",
    )


def _source_paths(truth_root: Path, day_utc: str) -> Dict[str, Path]:
    return {
        "gate_stack_verdict_v1": (truth_root / "reports" / "gate_stack_verdict_v1" / day_utc / "gate_stack_verdict.v1.json").resolve(),
        "root_cause_v1": (truth_root / "reports" / "root_cause_v1" / day_utc / "root_cause.v1.json").resolve(),
        "intent_absence_analysis_v1": (
            truth_root / "reports" / "intent_absence_analysis_v1" / day_utc / "intent_absence_analysis.v1.json"
        ).resolve(),
        "diagnostic_decision_ledger_v1": (
            truth_root / "reports" / "diagnostic_decision_ledger_v1" / day_utc / "diagnostic_decision_ledger.v1.json"
        ).resolve(),
        "activity_flow_diagnostics_v1": (
            truth_root / "reports" / "activity_flow_diagnostics_v1" / day_utc / "activity_flow_diagnostics.v1.json"
        ).resolve(),
        "runtime_regression_analytics_v1": (
            truth_root / "reports" / "runtime_regression_analytics_v1" / day_utc / "runtime_regression_analytics.v1.json"
        ).resolve(),
    }


def load_visibility_sources(repo_root: Path, truth_root: Path, day_utc: str) -> Dict[str, SourceArtifact]:
    paths = _source_paths(truth_root, day_utc)
    return {
        "gate_stack": _load_source(
            repo_root=repo_root,
            artifact_name="gate_stack_verdict_v1",
            path=paths["gate_stack_verdict_v1"],
            schema_relpath=GATE_STACK_SCHEMA,
            required=True,
        ),
        "root_cause": _load_source(
            repo_root=repo_root,
            artifact_name="root_cause_v1",
            path=paths["root_cause_v1"],
            schema_relpath=ROOT_CAUSE_SCHEMA,
            required=True,
        ),
        "intent_absence": _load_source(
            repo_root=repo_root,
            artifact_name="intent_absence_analysis_v1",
            path=paths["intent_absence_analysis_v1"],
            schema_relpath=INTENT_ABSENCE_SCHEMA,
            required=True,
        ),
        "diagnostic_ledger": _load_source(
            repo_root=repo_root,
            artifact_name="diagnostic_decision_ledger_v1",
            path=paths["diagnostic_decision_ledger_v1"],
            schema_relpath=DIAGNOSTIC_LEDGER_SCHEMA,
            required=True,
        ),
        "activity_flow": _load_source(
            repo_root=repo_root,
            artifact_name="activity_flow_diagnostics_v1",
            path=paths["activity_flow_diagnostics_v1"],
            schema_relpath=FLOW_SCHEMA,
            required=True,
        ),
        "runtime_regression": _load_source(
            repo_root=repo_root,
            artifact_name="runtime_regression_analytics_v1",
            path=paths["runtime_regression_analytics_v1"],
            schema_relpath=REGRESSION_SCHEMA,
            required=False,
        ),
    }


def evaluate_visibility_eligibility(sources: Dict[str, SourceArtifact]) -> EligibilityDecision:
    reasons: List[str] = []
    integrity_status = "OK"
    for key in ("gate_stack", "root_cause", "intent_absence", "diagnostic_ledger", "activity_flow"):
        source = sources[key]
        if not source.present or not source.valid:
            reasons.append(f"{source.artifact_name}:{source.error_code}:{source.error_detail}")
            integrity_status = "BOUNDED_INCOMPLETE"

    activity_flow = sources["activity_flow"].doc if sources["activity_flow"].valid else None
    if isinstance(activity_flow, dict):
        terminal_state = str(activity_flow.get("terminal_state") or "").strip()
        if terminal_state == "PARTIAL_OR_UNKNOWN":
            reasons.append("activity_flow_diagnostics_v1:PARTIAL_OR_UNKNOWN")
            integrity_status = "BOUNDED_INCOMPLETE"
        if str(activity_flow.get("integrity_status") or "").strip() == "EVIDENCE_INCONSISTENT":
            integrity_status = "EVIDENCE_INCONSISTENT"
            reasons.append("activity_flow_diagnostics_v1:EVIDENCE_INCONSISTENT")

    return EligibilityDecision(
        eligibility_status="ELIGIBLE" if not reasons else "BOUNDED_INCOMPLETE",
        integrity_status=integrity_status,
        reasons=tuple(reasons),
    )


def _derive_sleeve_scope(truth_root: Path) -> Dict[str, Optional[str]]:
    parts = list(truth_root.parts)
    if "truth_sleeves" in parts:
        idx = parts.index("truth_sleeves")
        if len(parts) > idx + 2:
            return {"sleeve_id": parts[idx + 1], "mode": parts[idx + 2]}
    return {"sleeve_id": None, "mode": None}


def _derive_run_scope(
    *,
    truth_root: Path,
    day_utc: str,
    sources: Dict[str, SourceArtifact],
) -> Dict[str, Any]:
    sleeve_scope = _derive_sleeve_scope(truth_root)
    attempt_id = None
    attempt_seq = None
    mode = sleeve_scope["mode"]
    engine_id = None
    try:
        attempt = select_attempt_for_day(truth_root, day_utc)
        attempt_id = str(attempt.pointer_entry.get("attempt_id") or "") or None
        attempt_seq = attempt.pointer_entry.get("attempt_seq")
        if isinstance(attempt.pointer_entry.get("mode"), str):
            mode = str(attempt.pointer_entry.get("mode"))
        manifest = attempt.attempt_manifest
        if isinstance(manifest.get("stages"), list):
            for stage in manifest["stages"]:
                if not isinstance(stage, dict):
                    continue
                candidate = stage.get("engine_id")
                if isinstance(candidate, str) and candidate.strip():
                    engine_id = candidate.strip()
                    break
    except Exception:
        attempt_id = None
        attempt_seq = None

    return {
        "day_utc": day_utc,
        "mode": mode,
        "sleeve_id": sleeve_scope["sleeve_id"],
        "engine_id": engine_id,
        "attempt_id": attempt_id,
        "attempt_seq": attempt_seq if isinstance(attempt_seq, int) else None,
        "truth_partition": str(truth_root),
    }


def _evidence_refs(sources: Dict[str, SourceArtifact]) -> List[str]:
    return sorted(str(source.path) for source in sources.values())


def _artifact_ref(truth_root: Path, family: str, day_utc: str, filename: str) -> str:
    return str((truth_root / "reports" / family / day_utc / filename).resolve())


def _stage_metric_map(activity_flow_doc: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(activity_flow_doc, dict):
        return out
    rows = activity_flow_doc.get("stage_metrics")
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        stage_name = str(row.get("stage_name") or "").strip()
        if stage_name:
            out[stage_name] = row
    return out


def _numeric_availability(
    *,
    value: Any,
    blocked_downstream: bool,
    integrity_failed: bool,
) -> Tuple[Any, str]:
    if integrity_failed:
        return None if value is None else value, "INTEGRITY_FAILED"
    if value is None:
        return None, "BLOCKED_DOWNSTREAM" if blocked_downstream else "UNAVAILABLE"
    if isinstance(value, int) and value == 0:
        return value, "ZERO"
    return value, "VALUE"


def _string_availability(value: Any) -> Tuple[Any, str]:
    if value is None:
        return None, "UNAVAILABLE"
    return value, "VALUE"


def _metric_source_ref(source_name: str, sources: Dict[str, SourceArtifact]) -> str:
    mapping = {
        "gate_stack_verdict_v1": "gate_stack",
        "root_cause_v1": "root_cause",
        "intent_absence_analysis_v1": "intent_absence",
        "diagnostic_decision_ledger_v1": "diagnostic_ledger",
        "activity_flow_diagnostics_v1": "activity_flow",
        "runtime_regression_analytics_v1": "runtime_regression",
    }
    return str(sources[mapping[source_name]].path)


def _registry_metric_value(
    *,
    metric_id: str,
    sources: Dict[str, SourceArtifact],
    run_scope: Dict[str, Any],
) -> Tuple[Any, str, str]:
    gate_stack = sources["gate_stack"].doc if sources["gate_stack"].valid else None
    root_cause = sources["root_cause"].doc if sources["root_cause"].valid else None
    intent_absence = sources["intent_absence"].doc if sources["intent_absence"].valid else None
    activity_flow = sources["activity_flow"].doc if sources["activity_flow"].valid else None
    flow_stage_map = _stage_metric_map(activity_flow)
    terminal_state = str(activity_flow.get("terminal_state") or "").strip() if isinstance(activity_flow, dict) else ""
    flow_integrity_failed = (
        isinstance(activity_flow, dict) and str(activity_flow.get("integrity_status") or "").strip() == "EVIDENCE_INCONSISTENT"
    )

    if metric_id == "readiness_status":
        return _string_availability(gate_stack.get("status") if isinstance(gate_stack, dict) else None) + ("OK",)
    if metric_id == "blocked_status":
        if not isinstance(gate_stack, dict):
            return None, "UNAVAILABLE", "UNAVAILABLE"
        return ("BLOCKED" if str(gate_stack.get("status") or "").strip().upper() != "PASS" else "NOT_BLOCKED"), "VALUE", "OK"
    if metric_id == "blocking_class":
        return _string_availability(gate_stack.get("blocking_class") if isinstance(gate_stack, dict) else None) + ("OK",)
    if metric_id == "root_cause_classification":
        return _string_availability(root_cause.get("deterministic_classification") if isinstance(root_cause, dict) else None) + ("OK",)
    if metric_id == "zero_intent_classification":
        return _string_availability(
            intent_absence.get("zero_intent_classification") if isinstance(intent_absence, dict) else None
        ) + ("OK",)
    if metric_id in {"intents", "authorized", "submitted", "filled", "reconciled", "rejected", "vetoed"}:
        counts = activity_flow.get("counts") if isinstance(activity_flow, dict) else None
        value = counts.get(metric_id) if isinstance(counts, dict) else None
        val, availability = _numeric_availability(
            value=value,
            blocked_downstream=terminal_state == "UPSTREAM_BLOCKED" and metric_id not in {"intents"},
            integrity_failed=flow_integrity_failed,
        )
        reconciliation = "EVIDENCE_INCONSISTENT" if flow_integrity_failed else (
            "BLOCKED_DOWNSTREAM" if availability == "BLOCKED_DOWNSTREAM" else (
                "UNAVAILABLE" if availability == "UNAVAILABLE" else "OK"
            )
        )
        return val, availability, reconciliation
    if metric_id.startswith("stage."):
        _, stage_name, field_name = metric_id.split(".", 2)
        stage_doc = flow_stage_map.get(stage_name)
        value = stage_doc.get(field_name) if isinstance(stage_doc, dict) else None
        blocked_downstream = terminal_state == "UPSTREAM_BLOCKED" and stage_name != "INTENTS"
        val, availability = _numeric_availability(
            value=value,
            blocked_downstream=blocked_downstream,
            integrity_failed=flow_integrity_failed,
        )
        reconciliation = "EVIDENCE_INCONSISTENT" if flow_integrity_failed else (
            "BLOCKED_DOWNSTREAM" if availability == "BLOCKED_DOWNSTREAM" else (
                "UNAVAILABLE" if availability == "UNAVAILABLE" else "OK"
            )
        )
        return val, availability, reconciliation
    raise SystemExit(f"FAIL: unknown registry metric_id: {metric_id}")


def build_visibility_metric_snapshot_doc(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    registry: Dict[str, Any],
    sources: Dict[str, SourceArtifact],
    eligibility: EligibilityDecision,
) -> Dict[str, Any]:
    metrics = registry.get("metrics") or []
    run_scope = _derive_run_scope(truth_root=truth_root, day_utc=day_utc, sources=sources)
    extracted_metrics: List[Dict[str, Any]] = []
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        metric_id = str(metric.get("metric_id") or "").strip()
        if not metric_id:
            continue
        value, availability_status, reconciliation_status = _registry_metric_value(
            metric_id=metric_id,
            sources=sources,
            run_scope=run_scope,
        )
        extracted_metrics.append(
            {
                "metric_id": metric_id,
                "metric_version": int(metric.get("metric_version") or 1),
                "value": value,
                "source_ref": _metric_source_ref(str(metric.get("source_artifact")), sources),
                "scope": run_scope,
                "availability_status": availability_status,
                "reconciliation_status": reconciliation_status,
            }
        )

    return {
        "schema_id": "visibility_metric_snapshot_v1",
        "schema_version": 1,
        "metric_registry_id": METRIC_REGISTRY_ID,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "run_scope": run_scope,
        "extracted_metrics": extracted_metrics,
        "evidence_refs": _evidence_refs(sources),
        "integrity_status": eligibility.integrity_status,
    }


def _snapshot_metric_map(snapshot_doc: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for metric in snapshot_doc.get("extracted_metrics") or []:
        if not isinstance(metric, dict):
            continue
        metric_id = str(metric.get("metric_id") or "").strip()
        if metric_id:
            out[metric_id] = metric
    return out


def _comparison_legitimacy_results(
    *,
    registry_by_id: Dict[str, Dict[str, Any]],
    snapshot_doc: Dict[str, Any],
    sources: Dict[str, SourceArtifact],
    run_scope: Dict[str, Any],
) -> List[Dict[str, Any]]:
    regression = sources["runtime_regression"]
    if not regression.valid or not isinstance(regression.doc, dict):
        return []
    regression_doc = regression.doc
    compared = regression_doc.get("compared_metrics")
    if not isinstance(compared, list):
        return []

    snapshot_map = _snapshot_metric_map(snapshot_doc)
    current_flow_ref = _artifact_ref(truth_root=Path(run_scope["truth_partition"]), family="activity_flow_diagnostics_v1", day_utc=run_scope["day_utc"], filename="activity_flow_diagnostics.v1.json")
    out: List[Dict[str, Any]] = []
    for row in compared:
        if not isinstance(row, dict):
            continue
        metric_id = str(row.get("metric_id") or "").strip()
        result = {
            "metric_id": metric_id,
            "comparison_mode": regression_doc.get("comparison_mode"),
            "comparison_status": row.get("comparison_status"),
            "comparability_status": "LEGITIMATE",
            "reason": "current snapshot and PRIOR_DAY regression are compatible",
        }
        if str(regression_doc.get("comparison_mode") or "") != "PRIOR_DAY":
            result["comparability_status"] = "COMPARISON_MODE_MISMATCH"
            result["reason"] = f"comparison_mode={regression_doc.get('comparison_mode')}"
        elif metric_id not in registry_by_id:
            result["comparability_status"] = "METRIC_NOT_REGISTERED"
            result["reason"] = metric_id
        elif str(regression_doc.get("current_activity_flow_ref") or "") != current_flow_ref:
            result["comparability_status"] = "SCOPE_MISMATCH"
            result["reason"] = f"current_activity_flow_ref={regression_doc.get('current_activity_flow_ref')}"
        else:
            snapshot_metric = snapshot_map.get(metric_id)
            if not isinstance(snapshot_metric, dict):
                result["comparability_status"] = "CURRENT_SNAPSHOT_MISSING"
                result["reason"] = metric_id
            else:
                current_value = row.get("current_value")
                snapshot_value = snapshot_metric.get("value")
                if current_value != snapshot_value:
                    result["comparability_status"] = "CURRENT_VALUE_MISMATCH"
                    result["reason"] = f"regression_current={current_value!r} snapshot={snapshot_value!r}"
        out.append(result)
    return out


def build_funnel_metrics_doc(
    *,
    truth_root: Path,
    day_utc: str,
    snapshot_doc: Dict[str, Any],
    sources: Dict[str, SourceArtifact],
    eligibility: EligibilityDecision,
) -> Dict[str, Any]:
    run_scope = snapshot_doc["run_scope"]
    activity_flow = sources["activity_flow"].doc if sources["activity_flow"].valid else {}
    stage_rows = activity_flow.get("stage_metrics") if isinstance(activity_flow, dict) else []
    rejection_breakdown = activity_flow.get("rejection_breakdown") if isinstance(activity_flow, dict) else []
    terminal_state = str(activity_flow.get("terminal_state") or "") if isinstance(activity_flow, dict) else ""

    stage_metrics: List[Dict[str, Any]] = []
    dominant_drop_stage = None
    dominant_drop_value = 0
    for row in stage_rows if isinstance(stage_rows, list) else []:
        if not isinstance(row, dict):
            continue
        availability = str(row.get("availability_status") or "UNAVAILABLE")
        reconciliation_status = "OK"
        if eligibility.integrity_status == "EVIDENCE_INCONSISTENT":
            reconciliation_status = "EVIDENCE_INCONSISTENT"
        elif availability == "UNAVAILABLE" and terminal_state == "UPSTREAM_BLOCKED" and str(row.get("stage_name")) != "INTENTS":
            reconciliation_status = "BLOCKED_DOWNSTREAM"
        elif availability == "UNAVAILABLE":
            reconciliation_status = "UNAVAILABLE"
        input_count = row.get("input_count")
        passed_count = row.get("passed_count")
        if isinstance(input_count, int) and isinstance(passed_count, int):
            drop = input_count - passed_count
            if drop > dominant_drop_value:
                dominant_drop_value = drop
                dominant_drop_stage = str(row.get("stage_name") or "")
        stage_metrics.append(
            {
                "stage_name": row.get("stage_name"),
                "input_count": input_count,
                "passed_count": passed_count,
                "rejected_count": row.get("rejected_count"),
                "emitted_count": row.get("emitted_count"),
                "reconciliation_status": reconciliation_status,
            }
        )

    dominant_reason = "NONE"
    if dominant_drop_stage:
        for item in rejection_breakdown if isinstance(rejection_breakdown, list) else []:
            if not isinstance(item, dict):
                continue
            if str(item.get("stage_name") or "") == dominant_drop_stage:
                dominant_reason = str(item.get("reason_classification") or "NONE")
                break
    elif terminal_state == "UPSTREAM_BLOCKED":
        dominant_reason = "UPSTREAM_BLOCKED"

    counts = activity_flow.get("counts") if isinstance(activity_flow, dict) else {}
    return {
        "schema_id": "funnel_metrics_v1",
        "schema_version": 1,
        "visibility_ruleset_id": VISIBILITY_RULESET_ID,
        "visibility_ruleset_version": VISIBILITY_RULESET_VERSION,
        "metric_registry_id": METRIC_REGISTRY_ID,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "run_scope": run_scope,
        "stage_metrics": stage_metrics,
        "total_universe_count": None,
        "total_signal_count": None,
        "total_intent_count": counts.get("intents") if isinstance(counts, dict) else None,
        "dominant_drop_stage": dominant_drop_stage,
        "dominant_drop_reason_classification": dominant_reason,
        "metric_snapshot_ref": _artifact_ref(truth_root, "visibility_metric_snapshot_v1", day_utc, "visibility_metric_snapshot.v1.json"),
        "decision_ledger_ref": _artifact_ref(truth_root, "visibility_decision_ledger_v1", day_utc, "visibility_decision_ledger.v1.json"),
        "evidence_refs": _evidence_refs(sources),
        "integrity_status": eligibility.integrity_status,
    }


def build_drift_report_doc(
    *,
    truth_root: Path,
    day_utc: str,
    registry_by_id: Dict[str, Dict[str, Any]],
    snapshot_doc: Dict[str, Any],
    sources: Dict[str, SourceArtifact],
    comparison_legitimacy_results: List[Dict[str, Any]],
    eligibility: EligibilityDecision,
) -> Dict[str, Any]:
    run_scope = snapshot_doc["run_scope"]
    regression = sources["runtime_regression"]
    compared_metrics: List[Dict[str, Any]] = []
    material_changes: List[Dict[str, Any]] = []
    drift_classifications: List[str] = []
    comparison_set_refs: List[str] = []
    comparison_window_definition: Dict[str, Any] = {
        "comparison_mode": "PRIOR_DAY",
        "comparison_day_utc": None,
        "source_artifact": "runtime_regression_analytics_v1",
    }
    integrity_status = eligibility.integrity_status

    legitimacy_by_metric = {row["metric_id"]: row for row in comparison_legitimacy_results if isinstance(row, dict) and "metric_id" in row}
    if not regression.valid or not isinstance(regression.doc, dict):
        integrity_status = "COMPARISON_UNAVAILABLE" if integrity_status == "OK" else integrity_status
        drift_classifications.append("PRIOR_DAY_UNAVAILABLE")
    else:
        doc = regression.doc
        comparison_window_definition["comparison_day_utc"] = doc.get("comparison_day_utc")
        comparison_set_refs = [str(doc.get("current_activity_flow_ref") or "")]
        if str(doc.get("comparison_activity_flow_ref") or "").strip():
            comparison_set_refs.append(str(doc.get("comparison_activity_flow_ref")))
        drift_classifications.append(str(doc.get("comparison_status") or "PRIOR_DAY_UNAVAILABLE"))
        if str(doc.get("integrity_status") or "").strip() == "COMPARISON_UNAVAILABLE" and integrity_status == "OK":
            integrity_status = "COMPARISON_UNAVAILABLE"
        for row in doc.get("compared_metrics") or []:
            if not isinstance(row, dict):
                continue
            metric_id = str(row.get("metric_id") or "")
            metric_def = registry_by_id.get(metric_id)
            legitimacy = legitimacy_by_metric.get(metric_id, {})
            comparability_status = str(legitimacy.get("comparability_status") or "NOT_COMPARABLE")
            metric_name = str(metric_def.get("metric_name") or metric_id) if isinstance(metric_def, dict) else metric_id
            legitimate = comparability_status == "LEGITIMATE"
            compared_metrics.append(
                {
                    "metric_id": metric_id,
                    "metric_version": int(metric_def.get("metric_version") or 1) if isinstance(metric_def, dict) else 1,
                    "metric_name": metric_name,
                    "current_value": row.get("current_value"),
                    "comparison_value": row.get("comparison_value"),
                    "absolute_change": row.get("absolute_change") if legitimate else None,
                    "relative_change": row.get("relative_change_bps") if legitimate else None,
                    "comparison_status": row.get("comparison_status") if legitimate else "NOT_COMPUTED",
                    "comparability_status": comparability_status,
                }
            )
        for row in doc.get("material_changes") or []:
            if not isinstance(row, dict):
                continue
            metric_id = str(row.get("metric_id") or "")
            legitimacy = legitimacy_by_metric.get(metric_id, {})
            if str(legitimacy.get("comparability_status") or "") == "LEGITIMATE":
                material_changes.append({"metric_id": metric_id, "absolute_change": row.get("absolute_change")})
    if any(row.get("comparability_status") != "LEGITIMATE" for row in compared_metrics):
        drift_classifications.append("COMPARABILITY_ISSUES_PRESENT")
    if material_changes:
        drift_classifications.append("MATERIAL_CHANGES_PRESENT")
    else:
        drift_classifications.append("NO_MATERIAL_CHANGES")

    return {
        "schema_id": "drift_report_v1",
        "schema_version": 1,
        "visibility_ruleset_id": VISIBILITY_RULESET_ID,
        "visibility_ruleset_version": VISIBILITY_RULESET_VERSION,
        "metric_registry_id": METRIC_REGISTRY_ID,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "run_scope": run_scope,
        "comparison_window_definition": comparison_window_definition,
        "comparison_set_refs": [ref for ref in comparison_set_refs if ref],
        "compared_metrics": compared_metrics,
        "material_changes": material_changes,
        "drift_classifications": drift_classifications,
        "metric_snapshot_ref": _artifact_ref(truth_root, "visibility_metric_snapshot_v1", day_utc, "visibility_metric_snapshot.v1.json"),
        "decision_ledger_ref": _artifact_ref(truth_root, "visibility_decision_ledger_v1", day_utc, "visibility_decision_ledger.v1.json"),
        "evidence_refs": _evidence_refs(sources),
        "integrity_status": integrity_status,
    }


def _count_entry(metric_id: str, snapshot_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    metric = snapshot_map.get(metric_id) or {}
    return {
        "metric_id": metric_id,
        "value": metric.get("value"),
        "availability_status": metric.get("availability_status"),
    }


def build_daily_summary_doc(
    *,
    truth_root: Path,
    day_utc: str,
    snapshot_doc: Dict[str, Any],
    drift_doc: Dict[str, Any],
    sources: Dict[str, SourceArtifact],
    eligibility: EligibilityDecision,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[str]]:
    run_scope = snapshot_doc["run_scope"]
    snapshot_map = _snapshot_metric_map(snapshot_doc)
    gate_stack = sources["gate_stack"].doc if sources["gate_stack"].valid else {}
    root_cause = sources["root_cause"].doc if sources["root_cause"].valid else {}
    intent_absence = sources["intent_absence"].doc if sources["intent_absence"].valid else {}

    readiness_status = str(gate_stack.get("status") or "UNAVAILABLE")
    blocked_status = "BLOCKED" if readiness_status != "PASS" else "NOT_BLOCKED"

    precedence_resolution: List[Dict[str, Any]] = []
    rejected_higher_priority_paths: List[str] = []
    summary_classification = "READY_STABLE"
    integrity_status = eligibility.integrity_status

    if blocked_status == "BLOCKED":
        summary_classification = "READINESS_BLOCKED"
        precedence_resolution.append({"priority": 1, "source": "gate_stack_verdict_v1", "outcome": summary_classification})
    else:
        rejected_higher_priority_paths.append("gate_stack_verdict_v1:status=PASS")
        if integrity_status in {"EVIDENCE_INCONSISTENT", "BOUNDED_INCOMPLETE", "INTERNAL_FAILURE"}:
            summary_classification = "INTEGRITY_FAILED"
            precedence_resolution.append({"priority": 2, "source": "integrity_failure_truth", "outcome": summary_classification})
        else:
            rejected_higher_priority_paths.append("integrity_failure_truth:integrity_status=OK")
            root_classification = str(root_cause.get("deterministic_classification") or "")
            zero_intent = str(intent_absence.get("zero_intent_classification") or "")
            if root_classification == "INVARIANT_VIOLATION" or zero_intent == "UPSTREAM_BLOCKED_NO_INTENT_PATH":
                summary_classification = "DIAGNOSTIC_ATTENTION_REQUIRED"
                precedence_resolution.append({"priority": 3, "source": "batch1_diagnostics", "outcome": summary_classification})
            else:
                rejected_higher_priority_paths.append("batch1_diagnostics:no_escalating_classification")
                total_intents = snapshot_map.get("intents", {}).get("value")
                if isinstance(total_intents, int) and total_intents == 0:
                    summary_classification = "READY_ZERO_INTENTS"
                    precedence_resolution.append({"priority": 4, "source": "current_day_metrics", "outcome": summary_classification})
                elif drift_doc.get("material_changes"):
                    summary_classification = "READY_WITH_MATERIAL_DRIFT"
                    precedence_resolution.append({"priority": 5, "source": "prior_day_drift", "outcome": summary_classification})
                else:
                    summary_classification = "READY_STABLE"
                    precedence_resolution.append({"priority": 4, "source": "current_day_metrics", "outcome": summary_classification})

    key_counts = [
        _count_entry("intents", snapshot_map),
        _count_entry("submitted", snapshot_map),
        _count_entry("filled", snapshot_map),
        _count_entry("rejected", snapshot_map),
    ]
    key_changes = [
        {
            "metric_id": row.get("metric_id"),
            "absolute_change": row.get("absolute_change"),
        }
        for row in drift_doc.get("material_changes") or []
        if isinstance(row, dict)
    ]
    key_risks: List[Dict[str, Any]] = []
    if blocked_status == "BLOCKED":
        key_risks.append({"risk_code": "BLOCKED_STATUS", "source": "gate_stack_verdict_v1", "detail": str(gate_stack.get("blocking_class") or readiness_status)})
    root_classification = str(root_cause.get("deterministic_classification") or "")
    if root_classification and root_classification != "NO_ROOT_CAUSE_IDENTIFIED":
        key_risks.append({"risk_code": "ROOT_CAUSE", "source": "root_cause_v1", "detail": root_classification})
    zero_intent = str(intent_absence.get("zero_intent_classification") or "")
    if zero_intent and zero_intent not in {"NON_ZERO_INTENTS", ""}:
        key_risks.append({"risk_code": "ZERO_INTENTS", "source": "intent_absence_analysis_v1", "detail": zero_intent})
    if integrity_status in {"EVIDENCE_INCONSISTENT", "BOUNDED_INCOMPLETE"}:
        key_risks.append({"risk_code": "VISIBILITY_INTEGRITY", "source": "batch2_visibility", "detail": integrity_status})

    doc = {
        "schema_id": "daily_summary_v1",
        "schema_version": 1,
        "visibility_ruleset_id": VISIBILITY_RULESET_ID,
        "visibility_ruleset_version": VISIBILITY_RULESET_VERSION,
        "metric_registry_id": METRIC_REGISTRY_ID,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "run_scope": run_scope,
        "readiness_status": readiness_status,
        "blocked_status": blocked_status,
        "summary_classification": summary_classification,
        "integrity_status": integrity_status,
        "key_counts": key_counts,
        "key_changes": key_changes,
        "key_risks": key_risks,
        "diagnostic_refs": [
            str(sources["root_cause"].path),
            str(sources["intent_absence"].path),
            str(sources["diagnostic_ledger"].path),
        ],
        "metric_snapshot_ref": _artifact_ref(truth_root, "visibility_metric_snapshot_v1", day_utc, "visibility_metric_snapshot.v1.json"),
        "decision_ledger_ref": _artifact_ref(truth_root, "visibility_decision_ledger_v1", day_utc, "visibility_decision_ledger.v1.json"),
        "evidence_refs": _evidence_refs(sources),
    }
    return doc, precedence_resolution, rejected_higher_priority_paths


def build_visibility_decision_ledger_doc(
    *,
    truth_root: Path,
    day_utc: str,
    snapshot_doc: Dict[str, Any],
    sources: Dict[str, SourceArtifact],
    comparison_legitimacy_results: List[Dict[str, Any]],
    precedence_resolution: List[Dict[str, Any]],
    rejected_higher_priority_paths: List[str],
    eligibility: EligibilityDecision,
) -> Dict[str, Any]:
    run_scope = snapshot_doc["run_scope"]
    rules_evaluated = [
        {"rule_id": "VISIBILITY_ELIGIBILITY_GATE", "result": eligibility.eligibility_status, "detail": list(eligibility.reasons)},
        {
            "rule_id": "DRIFT_COMPARISON_MODE",
            "result": "PRIOR_DAY",
            "detail": [str(sources["runtime_regression"].doc.get("comparison_mode"))] if sources["runtime_regression"].valid and isinstance(sources["runtime_regression"].doc, dict) else [],
        },
    ]
    return {
        "schema_id": "visibility_decision_ledger_v1",
        "schema_version": 1,
        "visibility_ruleset_id": VISIBILITY_RULESET_ID,
        "visibility_ruleset_version": VISIBILITY_RULESET_VERSION,
        "metric_registry_id": METRIC_REGISTRY_ID,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "run_scope": run_scope,
        "metric_snapshot_ref": _artifact_ref(truth_root, "visibility_metric_snapshot_v1", day_utc, "visibility_metric_snapshot.v1.json"),
        "comparison_legitimacy_results": comparison_legitimacy_results,
        "source_precedence_resolution": precedence_resolution,
        "rules_evaluated": rules_evaluated,
        "winning_derivation_paths": [
            "FUNNEL_FROM_ACTIVITY_FLOW_DIAGNOSTICS_V1",
            "DRIFT_FROM_RUNTIME_REGRESSION_ANALYTICS_V1",
            "SUMMARY_FROM_GATE_STACK_VERDICT_V1",
        ],
        "rejected_higher_priority_paths": rejected_higher_priority_paths,
        "evidence_refs": _evidence_refs(sources),
    }


def build_batch2_visibility_docs(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    force_internal_failure: bool = False,
) -> Dict[str, Dict[str, Any]]:
    _require_day_utc(day_utc)
    _require_produced_utc(day_utc, produced_utc)
    if force_internal_failure:
        raise RuntimeError("FORCED_INTERNAL_FAILURE")
    registry = _read_registry(repo_root)
    registry_by_id = _registry_metrics_by_id(registry)
    sources = load_visibility_sources(repo_root, truth_root, day_utc)
    eligibility = evaluate_visibility_eligibility(sources)
    snapshot_doc = build_visibility_metric_snapshot_doc(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        registry=registry,
        sources=sources,
        eligibility=eligibility,
    )
    comparison_legitimacy_results = _comparison_legitimacy_results(
        registry_by_id=registry_by_id,
        snapshot_doc=snapshot_doc,
        sources=sources,
        run_scope=snapshot_doc["run_scope"],
    )
    funnel_doc = build_funnel_metrics_doc(
        truth_root=truth_root,
        day_utc=day_utc,
        snapshot_doc=snapshot_doc,
        sources=sources,
        eligibility=eligibility,
    )
    drift_doc = build_drift_report_doc(
        truth_root=truth_root,
        day_utc=day_utc,
        registry_by_id=registry_by_id,
        snapshot_doc=snapshot_doc,
        sources=sources,
        comparison_legitimacy_results=comparison_legitimacy_results,
        eligibility=eligibility,
    )
    summary_doc, precedence_resolution, rejected_higher_priority_paths = build_daily_summary_doc(
        truth_root=truth_root,
        day_utc=day_utc,
        snapshot_doc=snapshot_doc,
        drift_doc=drift_doc,
        sources=sources,
        eligibility=eligibility,
    )
    ledger_doc = build_visibility_decision_ledger_doc(
        truth_root=truth_root,
        day_utc=day_utc,
        snapshot_doc=snapshot_doc,
        sources=sources,
        comparison_legitimacy_results=comparison_legitimacy_results,
        precedence_resolution=precedence_resolution,
        rejected_higher_priority_paths=rejected_higher_priority_paths,
        eligibility=eligibility,
    )
    return {
        "snapshot": snapshot_doc,
        "funnel": funnel_doc,
        "drift": drift_doc,
        "summary": summary_doc,
        "ledger": ledger_doc,
    }


def _write_refreshable_json(
    *,
    repo_root: Path,
    out_path: Path,
    doc: Dict[str, Any],
    schema_relpath: str,
    expected_schema_id: str,
    expected_schema_version: Any,
    expected_day_utc: str,
) -> RefreshWriteResultV1:
    validate_against_repo_schema_v1(doc, repo_root, schema_relpath)
    payload = canonical_json_bytes_v1(doc) + b"\n"
    return write_day_artifact_refreshable_v1(
        path=out_path,
        data=payload,
        expected_day_utc=expected_day_utc,
        expected_schema_id=expected_schema_id,
        expected_schema_version=expected_schema_version,
        preserve_statuses=(),
    )


def _internal_failure_docs(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    failure_detail: str,
) -> Dict[str, Dict[str, Any]]:
    run_scope = {
        "day_utc": day_utc,
        "mode": _derive_sleeve_scope(truth_root)["mode"],
        "sleeve_id": _derive_sleeve_scope(truth_root)["sleeve_id"],
        "engine_id": None,
        "attempt_id": None,
        "attempt_seq": None,
        "truth_partition": str(truth_root),
    }
    evidence_refs: List[str] = []
    snapshot_doc = {
        "schema_id": "visibility_metric_snapshot_v1",
        "schema_version": 1,
        "metric_registry_id": METRIC_REGISTRY_ID,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "run_scope": run_scope,
        "extracted_metrics": [],
        "evidence_refs": evidence_refs,
        "integrity_status": "INTERNAL_FAILURE",
    }
    ledger_doc = {
        "schema_id": "visibility_decision_ledger_v1",
        "schema_version": 1,
        "visibility_ruleset_id": VISIBILITY_RULESET_ID,
        "visibility_ruleset_version": VISIBILITY_RULESET_VERSION,
        "metric_registry_id": METRIC_REGISTRY_ID,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "run_scope": run_scope,
        "metric_snapshot_ref": _artifact_ref(truth_root, "visibility_metric_snapshot_v1", day_utc, "visibility_metric_snapshot.v1.json"),
        "comparison_legitimacy_results": [],
        "source_precedence_resolution": [{"priority": 1, "source": "batch2_internal_failure", "outcome": "INTERNAL_FAILURE"}],
        "rules_evaluated": [{"rule_id": "BATCH2_INTERNAL_FAILURE", "result": "INTERNAL_FAILURE", "detail": [failure_detail]}],
        "winning_derivation_paths": ["INTERNAL_FAILURE_FALLBACK"],
        "rejected_higher_priority_paths": [],
        "evidence_refs": evidence_refs,
    }
    funnel_doc = {
        "schema_id": "funnel_metrics_v1",
        "schema_version": 1,
        "visibility_ruleset_id": VISIBILITY_RULESET_ID,
        "visibility_ruleset_version": VISIBILITY_RULESET_VERSION,
        "metric_registry_id": METRIC_REGISTRY_ID,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "run_scope": run_scope,
        "stage_metrics": [],
        "total_universe_count": None,
        "total_signal_count": None,
        "total_intent_count": None,
        "dominant_drop_stage": None,
        "dominant_drop_reason_classification": "INTERNAL_FAILURE",
        "metric_snapshot_ref": _artifact_ref(truth_root, "visibility_metric_snapshot_v1", day_utc, "visibility_metric_snapshot.v1.json"),
        "decision_ledger_ref": _artifact_ref(truth_root, "visibility_decision_ledger_v1", day_utc, "visibility_decision_ledger.v1.json"),
        "evidence_refs": evidence_refs,
        "integrity_status": "INTERNAL_FAILURE",
    }
    drift_doc = {
        "schema_id": "drift_report_v1",
        "schema_version": 1,
        "visibility_ruleset_id": VISIBILITY_RULESET_ID,
        "visibility_ruleset_version": VISIBILITY_RULESET_VERSION,
        "metric_registry_id": METRIC_REGISTRY_ID,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "run_scope": run_scope,
        "comparison_window_definition": {"comparison_mode": "PRIOR_DAY", "comparison_day_utc": None, "source_artifact": "runtime_regression_analytics_v1"},
        "comparison_set_refs": [],
        "compared_metrics": [],
        "material_changes": [],
        "drift_classifications": ["INTERNAL_FAILURE"],
        "metric_snapshot_ref": _artifact_ref(truth_root, "visibility_metric_snapshot_v1", day_utc, "visibility_metric_snapshot.v1.json"),
        "decision_ledger_ref": _artifact_ref(truth_root, "visibility_decision_ledger_v1", day_utc, "visibility_decision_ledger.v1.json"),
        "evidence_refs": evidence_refs,
        "integrity_status": "INTERNAL_FAILURE",
    }
    summary_doc = {
        "schema_id": "daily_summary_v1",
        "schema_version": 1,
        "visibility_ruleset_id": VISIBILITY_RULESET_ID,
        "visibility_ruleset_version": VISIBILITY_RULESET_VERSION,
        "metric_registry_id": METRIC_REGISTRY_ID,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "run_scope": run_scope,
        "readiness_status": "UNAVAILABLE",
        "blocked_status": "UNKNOWN",
        "summary_classification": "INTERNAL_FAILURE",
        "integrity_status": "INTERNAL_FAILURE",
        "key_counts": [],
        "key_changes": [],
        "key_risks": [{"risk_code": "INTERNAL_FAILURE", "source": "batch2_visibility", "detail": failure_detail}],
        "diagnostic_refs": [],
        "metric_snapshot_ref": _artifact_ref(truth_root, "visibility_metric_snapshot_v1", day_utc, "visibility_metric_snapshot.v1.json"),
        "decision_ledger_ref": _artifact_ref(truth_root, "visibility_decision_ledger_v1", day_utc, "visibility_decision_ledger.v1.json"),
        "evidence_refs": evidence_refs,
    }
    return {
        "snapshot": snapshot_doc,
        "funnel": funnel_doc,
        "drift": drift_doc,
        "summary": summary_doc,
        "ledger": ledger_doc,
    }


def write_batch2_visibility(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
    force_internal_failure: bool = False,
) -> Dict[str, RefreshWriteResultV1]:
    try:
        docs = build_batch2_visibility_docs(
            repo_root=repo_root,
            truth_root=truth_root,
            day_utc=day_utc,
            produced_utc=produced_utc,
            force_internal_failure=force_internal_failure,
        )
    except Exception as exc:
        docs = _internal_failure_docs(
            repo_root=repo_root,
            truth_root=truth_root,
            day_utc=day_utc,
            failure_detail=" | ".join(traceback.format_exception_only(type(exc), exc)).strip(),
        )

    return {
        "snapshot": _write_refreshable_json(
            repo_root=repo_root,
            out_path=(truth_root / "reports" / "visibility_metric_snapshot_v1" / day_utc / "visibility_metric_snapshot.v1.json").resolve(),
            doc=docs["snapshot"],
            schema_relpath=SNAPSHOT_SCHEMA,
            expected_schema_id="visibility_metric_snapshot_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
        "funnel": _write_refreshable_json(
            repo_root=repo_root,
            out_path=(truth_root / "reports" / "funnel_metrics_v1" / day_utc / "funnel_metrics.v1.json").resolve(),
            doc=docs["funnel"],
            schema_relpath=FUNNEL_SCHEMA,
            expected_schema_id="funnel_metrics_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
        "drift": _write_refreshable_json(
            repo_root=repo_root,
            out_path=(truth_root / "reports" / "drift_report_v1" / day_utc / "drift_report.v1.json").resolve(),
            doc=docs["drift"],
            schema_relpath=DRIFT_SCHEMA,
            expected_schema_id="drift_report_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
        "summary": _write_refreshable_json(
            repo_root=repo_root,
            out_path=(truth_root / "reports" / "daily_summary_v1" / day_utc / "daily_summary.v1.json").resolve(),
            doc=docs["summary"],
            schema_relpath=SUMMARY_SCHEMA,
            expected_schema_id="daily_summary_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
        "ledger": _write_refreshable_json(
            repo_root=repo_root,
            out_path=(truth_root / "reports" / "visibility_decision_ledger_v1" / day_utc / "visibility_decision_ledger.v1.json").resolve(),
            doc=docs["ledger"],
            schema_relpath=LEDGER_SCHEMA,
            expected_schema_id="visibility_decision_ledger_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
    }
