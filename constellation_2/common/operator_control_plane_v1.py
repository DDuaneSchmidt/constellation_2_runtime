from __future__ import annotations

import re
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.common.diagnostic_foundation_v1 import (
    _git_sha,
    _read_json_obj,
    _require_day_utc,
    _require_produced_utc,
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


INTERFACE_RULESET_ID = "BATCH3_OPERATOR_INTERFACE_RULESET"
INTERFACE_RULESET_VERSION = 1

VIEW_REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_OPERATOR_VIEW_REGISTRY_V1.json"
HOME_POLICY_RELPATH = "governance/02_REGISTRIES/C2_OPERATOR_HOME_COMPOSITION_POLICY_V1.json"
QUERY_POLICY_RELPATH = "governance/02_REGISTRIES/C2_OPERATOR_QUERY_POLICY_REGISTRY_V1.json"
TEMPLATE_REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_OPERATOR_RESPONSE_TEMPLATE_REGISTRY_V1.json"

HOME_VIEW_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_home_view.v1.schema.json"
QUERY_RESPONSE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_query_response.v1.schema.json"
RETRIEVAL_MANIFEST_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_retrieval_manifest.v1.schema.json"
TRUST_PANEL_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_trust_panel.v1.schema.json"

GATE_STACK_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/gate_stack_verdict.v1.schema.json"
ROOT_CAUSE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/root_cause.v1.schema.json"
INTENT_ABSENCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/intent_absence_analysis.v1.schema.json"
DIAGNOSTIC_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/diagnostic_decision_ledger.v1.schema.json"
FLOW_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/activity_flow_diagnostics.v1.schema.json"
REGRESSION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/runtime_regression_analytics.v1.schema.json"
VISIBILITY_SNAPSHOT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/visibility_metric_snapshot.v1.schema.json"
FUNNEL_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/funnel_metrics.v1.schema.json"
DRIFT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/drift_report.v1.schema.json"
SUMMARY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/daily_summary.v1.schema.json"
VISIBILITY_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/visibility_decision_ledger.v1.schema.json"

DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
ATTEMPT_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}__A\d{4}\b")

ARTIFACT_SPECS: Dict[str, Tuple[str, str]] = {
    "gate_stack_verdict_v1": ("reports/gate_stack_verdict_v1/{day}/gate_stack_verdict.v1.json", GATE_STACK_SCHEMA),
    "root_cause_v1": ("reports/root_cause_v1/{day}/root_cause.v1.json", ROOT_CAUSE_SCHEMA),
    "intent_absence_analysis_v1": (
        "reports/intent_absence_analysis_v1/{day}/intent_absence_analysis.v1.json",
        INTENT_ABSENCE_SCHEMA,
    ),
    "diagnostic_decision_ledger_v1": (
        "reports/diagnostic_decision_ledger_v1/{day}/diagnostic_decision_ledger.v1.json",
        DIAGNOSTIC_LEDGER_SCHEMA,
    ),
    "activity_flow_diagnostics_v1": (
        "reports/activity_flow_diagnostics_v1/{day}/activity_flow_diagnostics.v1.json",
        FLOW_SCHEMA,
    ),
    "runtime_regression_analytics_v1": (
        "reports/runtime_regression_analytics_v1/{day}/runtime_regression_analytics.v1.json",
        REGRESSION_SCHEMA,
    ),
    "visibility_metric_snapshot_v1": (
        "reports/visibility_metric_snapshot_v1/{day}/visibility_metric_snapshot.v1.json",
        VISIBILITY_SNAPSHOT_SCHEMA,
    ),
    "funnel_metrics_v1": ("reports/funnel_metrics_v1/{day}/funnel_metrics.v1.json", FUNNEL_SCHEMA),
    "drift_report_v1": ("reports/drift_report_v1/{day}/drift_report.v1.json", DRIFT_SCHEMA),
    "daily_summary_v1": ("reports/daily_summary_v1/{day}/daily_summary.v1.json", SUMMARY_SCHEMA),
    "visibility_decision_ledger_v1": (
        "reports/visibility_decision_ledger_v1/{day}/visibility_decision_ledger.v1.json",
        VISIBILITY_LEDGER_SCHEMA,
    ),
}


@dataclass(frozen=True)
class SourceArtifact:
    artifact_name: str
    path: Path
    schema_relpath: str
    present: bool
    valid: bool
    doc: Optional[Dict[str, Any]]
    error_code: str
    error_detail: str


@dataclass(frozen=True)
class AccessDecision:
    access_status: str
    finalization_state: str
    integrity_state: str
    reasons: Tuple[str, ...]


@dataclass(frozen=True)
class NormalizedQuery:
    query_id: str
    original_query_text: str
    normalized_query_text: str
    requested_scope: Dict[str, Any]
    normalized_scope: Dict[str, Any]
    extracted_scope_fields: Tuple[str, ...]


def _read_registry(repo_root: Path, relpath: str) -> Dict[str, Any]:
    return _read_json_obj((repo_root / relpath).resolve())


def _validate_doc(doc: Dict[str, Any], repo_root: Path, schema_relpath: str) -> None:
    validate_against_repo_schema_v1(doc, repo_root, schema_relpath)


def _load_source(repo_root: Path, artifact_name: str, path: Path, schema_relpath: str) -> SourceArtifact:
    if not path.exists() or not path.is_file():
        return SourceArtifact(
            artifact_name=artifact_name,
            path=path,
            schema_relpath=schema_relpath,
            present=False,
            valid=False,
            doc=None,
            error_code="MISSING_REQUIRED_INPUT",
            error_detail=str(path),
        )
    try:
        doc = _read_json_obj(path)
        _validate_doc(doc, repo_root, schema_relpath)
    except SchemaValidationError as exc:
        return SourceArtifact(
            artifact_name=artifact_name,
            path=path,
            schema_relpath=schema_relpath,
            present=True,
            valid=False,
            doc=None,
            error_code="SCHEMA_MISMATCH",
            error_detail=str(exc),
        )
    except Exception as exc:
        return SourceArtifact(
            artifact_name=artifact_name,
            path=path,
            schema_relpath=schema_relpath,
            present=True,
            valid=False,
            doc=None,
            error_code="PARSE_OR_SHAPE_FAILURE",
            error_detail=str(exc),
        )
    return SourceArtifact(
        artifact_name=artifact_name,
        path=path,
        schema_relpath=schema_relpath,
        present=True,
        valid=True,
        doc=doc,
        error_code="",
        error_detail="",
    )


def _source_paths(truth_root: Path, day_utc: str) -> Dict[str, Path]:
    return {
        artifact_name: (truth_root / relpattern.format(day=day_utc)).resolve()
        for artifact_name, (relpattern, _) in ARTIFACT_SPECS.items()
    }


def load_operator_sources(repo_root: Path, truth_root: Path, day_utc: str) -> Dict[str, SourceArtifact]:
    out: Dict[str, SourceArtifact] = {}
    for artifact_name, path in _source_paths(truth_root, day_utc).items():
        schema_relpath = ARTIFACT_SPECS[artifact_name][1]
        out[artifact_name] = _load_source(repo_root, artifact_name, path, schema_relpath)
    return out


def _derive_scope_from_truth_root(truth_root: Path, day_utc: str) -> Dict[str, Any]:
    parts = list(truth_root.parts)
    sleeve_id = None
    mode = None
    if "truth_sleeves" in parts:
        idx = parts.index("truth_sleeves")
        if len(parts) > idx + 2:
            sleeve_id = parts[idx + 1]
            mode = parts[idx + 2]
    return {
        "day_utc": day_utc,
        "mode": mode,
        "sleeve_id": sleeve_id,
        "engine_id": None,
        "attempt_id": None,
        "attempt_seq": None,
        "truth_partition": str(truth_root),
    }


def _first_valid_doc(sources: Dict[str, SourceArtifact], names: List[str]) -> Optional[Dict[str, Any]]:
    for name in names:
        source = sources.get(name)
        if isinstance(source, SourceArtifact) and source.valid and isinstance(source.doc, dict):
            return source.doc
    return None


def derive_run_scope(truth_root: Path, day_utc: str, sources: Dict[str, SourceArtifact]) -> Dict[str, Any]:
    fallback = _derive_scope_from_truth_root(truth_root, day_utc)
    for artifact_name in [
        "daily_summary_v1",
        "visibility_metric_snapshot_v1",
        "funnel_metrics_v1",
        "drift_report_v1",
        "root_cause_v1",
        "intent_absence_analysis_v1",
        "diagnostic_decision_ledger_v1",
    ]:
        source = sources.get(artifact_name)
        if not isinstance(source, SourceArtifact) or not source.valid or not isinstance(source.doc, dict):
            continue
        run_scope = source.doc.get("run_scope")
        if isinstance(run_scope, dict):
            merged = dict(fallback)
            for key in merged.keys():
                if key in run_scope:
                    merged[key] = run_scope.get(key)
            return merged
        merged = dict(fallback)
        if isinstance(source.doc.get("selected_attempt_id"), str):
            merged["attempt_id"] = source.doc.get("selected_attempt_id")
        if isinstance(source.doc.get("day_utc"), str):
            merged["day_utc"] = source.doc.get("day_utc")
        return merged
    return fallback


def _evidence_refs(sources: Dict[str, SourceArtifact]) -> List[str]:
    out = []
    for source in sources.values():
        if source.present:
            out.append(str(source.path))
    return sorted(set(out))


def _artifact_ref(truth_root: Path, family: str, day_utc: str, filename: str) -> str:
    return str((truth_root / "reports" / family / day_utc / filename).resolve())


def _embedded_ref(family: str, object_id: str) -> str:
    return f"embedded://{family}/{object_id}"


def _integrity_from_sources(sources: Dict[str, SourceArtifact]) -> str:
    for artifact_name in ["daily_summary_v1", "drift_report_v1", "funnel_metrics_v1", "activity_flow_diagnostics_v1"]:
        source = sources.get(artifact_name)
        if not isinstance(source, SourceArtifact) or not source.valid or not isinstance(source.doc, dict):
            continue
        value = str(source.doc.get("integrity_status") or "").strip()
        if value in {"EVIDENCE_INCONSISTENT", "INTERNAL_FAILURE"}:
            return value
        if value in {"BOUNDED_INCOMPLETE", "COMPARISON_UNAVAILABLE"}:
            return "BOUNDED_INCOMPLETE"
    return "OK"


def evaluate_operator_access(
    view_or_query_id: str,
    required_source_names: List[str],
    sources: Dict[str, SourceArtifact],
) -> AccessDecision:
    reasons: List[str] = []
    finalization_state = "FINALIZED"
    access_status = "ALLOW"
    integrity_state = _integrity_from_sources(sources)

    gate = sources.get("gate_stack_verdict_v1")
    if not isinstance(gate, SourceArtifact) or not gate.present or not gate.valid or not isinstance(gate.doc, dict):
        finalization_state = "UNAVAILABLE"
        access_status = "ALLOW_BOUNDED_PARTIAL"
        integrity_state = "BOUNDED_INCOMPLETE" if integrity_state == "OK" else integrity_state
        reasons.append("gate_stack_verdict_v1:MISSING_OR_INVALID")
    else:
        status = str(gate.doc.get("status") or "").strip()
        if status not in {"PASS", "FAIL"}:
            finalization_state = "IN_FLIGHT_BLOCKED"
            access_status = "DENY_FINAL"
            reasons.append(f"gate_stack_verdict_v1:UNSTABLE_STATUS:{status}")

    for artifact_name in required_source_names:
        if artifact_name.startswith("operator_"):
            continue
        source = sources.get(artifact_name)
        if not isinstance(source, SourceArtifact) or not source.present or not source.valid:
            if finalization_state == "FINALIZED":
                finalization_state = "BOUNDED_PARTIAL"
            access_status = "ALLOW_BOUNDED_PARTIAL"
            integrity_state = "BOUNDED_INCOMPLETE" if integrity_state == "OK" else integrity_state
            detail = source.error_code if isinstance(source, SourceArtifact) else "MISSING"
            reasons.append(f"{artifact_name}:{detail}")

    if integrity_state in {"EVIDENCE_INCONSISTENT", "INTERNAL_FAILURE"}:
        access_status = "ALLOW_INTEGRITY_CONSTRAINED"

    return AccessDecision(
        access_status=access_status,
        finalization_state=finalization_state,
        integrity_state=integrity_state,
        reasons=tuple(sorted(set(reasons))),
    )


def _humanize_view_label(view_id: str) -> str:
    text = view_id.replace("_contract_v1", "").replace("_", " ")
    return " ".join(part.capitalize() for part in text.split())


def _normalize_text(text: str) -> str:
    lowered = str(text or "").strip().lower()
    lowered = re.sub(r"[^a-z0-9_\-\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def normalize_operator_query(query_text: str, default_scope: Dict[str, Any]) -> NormalizedQuery:
    original = str(query_text or "")
    normalized = _normalize_text(original)
    extracted: List[str] = []
    requested_scope = {
        "day_utc": default_scope.get("day_utc"),
        "mode": default_scope.get("mode"),
        "sleeve_id": default_scope.get("sleeve_id"),
        "attempt_id": default_scope.get("attempt_id"),
        "truth_partition": default_scope.get("truth_partition"),
    }
    normalized_scope = dict(requested_scope)

    dates = DATE_RE.findall(original)
    if dates:
        normalized_scope["day_utc"] = dates[0]
        extracted.append("day_utc")

    attempt_match = ATTEMPT_RE.search(original)
    if attempt_match:
        normalized_scope["attempt_id"] = attempt_match.group(0)
        extracted.append("attempt_id")

    lowered = original.lower()
    if " paper" in f" {lowered} ":
        normalized_scope["mode"] = "PAPER"
        extracted.append("mode")
    elif " live" in f" {lowered} ":
        normalized_scope["mode"] = "LIVE"
        extracted.append("mode")

    if "primary" in lowered:
        normalized_scope["sleeve_id"] = "PRIMARY"
        extracted.append("sleeve_id")

    day_component = str(normalized_scope.get("day_utc") or default_scope.get("day_utc") or "")
    query_id = f"{day_component}:{uuid.uuid5(uuid.NAMESPACE_URL, normalized or original).hex[:16]}"
    return NormalizedQuery(
        query_id=query_id,
        original_query_text=original,
        normalized_query_text=normalized,
        requested_scope=requested_scope,
        normalized_scope=normalized_scope,
        extracted_scope_fields=tuple(sorted(set(extracted))),
    )


def _query_classes(query_registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = query_registry.get("query_classes")
    if not isinstance(rows, list):
        raise SystemExit(f"FAIL: invalid query registry: {QUERY_POLICY_RELPATH}")
    return [row for row in rows if isinstance(row, dict)]


def _templates_by_id(template_registry: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    templates = template_registry.get("templates")
    if not isinstance(templates, list):
        raise SystemExit(f"FAIL: invalid template registry: {TEMPLATE_REGISTRY_RELPATH}")
    out: Dict[str, Dict[str, Any]] = {}
    for row in templates:
        if isinstance(row, dict) and isinstance(row.get("response_template_id"), str):
            out[str(row["response_template_id"])] = row
    return out


def classify_query(normalized: NormalizedQuery, query_registry: Dict[str, Any]) -> Dict[str, Any]:
    matches: List[Tuple[int, int, Dict[str, Any]]] = []
    for idx, policy in enumerate(_query_classes(query_registry)):
        query_class_id = str(policy.get("query_class_id") or "")
        if query_class_id == "unsupported_query_v1":
            continue
        patterns = policy.get("supported_intent_patterns")
        if not isinstance(patterns, list):
            continue
        matched_patterns = []
        best_len = 0
        for pattern in patterns:
            pattern_text = _normalize_text(str(pattern or ""))
            if pattern_text and pattern_text in normalized.normalized_query_text:
                matched_patterns.append(pattern_text)
                best_len = max(best_len, len(pattern_text))
        if matched_patterns:
            scored_policy = dict(policy)
            scored_policy["_matched_patterns"] = matched_patterns
            matches.append((best_len, -idx, scored_policy))

    if not matches:
        return next(policy for policy in _query_classes(query_registry) if policy.get("query_class_id") == "unsupported_query_v1")

    matches.sort(reverse=True)
    best = matches[0][2]
    if len(matches) > 1 and matches[0][0] == matches[1][0]:
        return next(policy for policy in _query_classes(query_registry) if policy.get("query_class_id") == "unsupported_query_v1")
    return best


def _source_matches_scope(source: SourceArtifact, normalized_scope: Dict[str, Any]) -> bool:
    if not source.valid or not isinstance(source.doc, dict):
        return False
    day_utc = normalized_scope.get("day_utc")
    if isinstance(day_utc, str):
        doc_day = source.doc.get("day_utc")
        run_scope = source.doc.get("run_scope")
        if isinstance(doc_day, str) and doc_day != day_utc:
            return False
        if isinstance(run_scope, dict) and isinstance(run_scope.get("day_utc"), str) and run_scope.get("day_utc") != day_utc:
            return False

    mode = normalized_scope.get("mode")
    if isinstance(mode, str):
        run_scope = source.doc.get("run_scope")
        if isinstance(run_scope, dict) and isinstance(run_scope.get("mode"), str) and run_scope.get("mode") != mode:
            return False

    sleeve_id = normalized_scope.get("sleeve_id")
    if isinstance(sleeve_id, str):
        run_scope = source.doc.get("run_scope")
        if isinstance(run_scope, dict) and isinstance(run_scope.get("sleeve_id"), str) and run_scope.get("sleeve_id") != sleeve_id:
            return False

    attempt_id = normalized_scope.get("attempt_id")
    if isinstance(attempt_id, str):
        run_scope = source.doc.get("run_scope")
        doc_attempt = None
        if isinstance(run_scope, dict):
            doc_attempt = run_scope.get("attempt_id")
        if doc_attempt is None:
            doc_attempt = source.doc.get("selected_attempt_id")
        if isinstance(doc_attempt, str) and doc_attempt != attempt_id:
            return False
    return True


def _build_retrieval_manifest(
    *,
    route_id: str,
    route_classification: str,
    requested_scope: Dict[str, Any],
    normalized_scope: Dict[str, Any],
    required_source_names: List[str],
    optional_source_names: List[str],
    precedence_rules: List[str],
    sources: Dict[str, SourceArtifact],
) -> Dict[str, Any]:
    retrieved_artifacts: List[Dict[str, Any]] = []
    rejected_artifacts: List[Dict[str, Any]] = []
    retrieval_status = "EXACT"

    all_names = [(name, True) for name in required_source_names] + [(name, False) for name in optional_source_names]
    for artifact_name, required in all_names:
        if artifact_name.startswith("operator_"):
            continue
        source = sources.get(artifact_name)
        if not isinstance(source, SourceArtifact):
            rejected_artifacts.append({"artifact_id": artifact_name, "reason": "UNREGISTERED_SOURCE"})
            retrieval_status = "UNAVAILABLE"
            continue
        if not source.present or not source.valid:
            if required:
                rejected_artifacts.append({"artifact_id": artifact_name, "reason": source.error_code or "MISSING_REQUIRED_INPUT"})
                retrieval_status = "BOUNDED_PARTIAL" if retrieval_status == "EXACT" else retrieval_status
            else:
                retrieved_artifacts.append(
                    {
                        "artifact_id": artifact_name,
                        "path": str(source.path),
                        "required": False,
                        "status": "OPTIONAL_MISSING",
                    }
                )
            continue
        if not _source_matches_scope(source, normalized_scope):
            rejected_artifacts.append({"artifact_id": artifact_name, "reason": "SCOPE_MISMATCH"})
            retrieval_status = "UNAVAILABLE" if required else retrieval_status
            continue
        retrieved_artifacts.append(
            {
                "artifact_id": artifact_name,
                "path": str(source.path),
                "required": required,
                "status": "RETRIEVED",
            }
        )

    return {
        "schema_id": "operator_retrieval_manifest_v1",
        "schema_version": 1,
        "day_utc": normalized_scope.get("day_utc"),
        "query_or_view_id": route_id,
        "route_classification": route_classification,
        "requested_scope": requested_scope,
        "normalized_scope": normalized_scope,
        "retrieved_artifacts": retrieved_artifacts,
        "rejected_artifacts": rejected_artifacts,
        "scope_filters_applied": [
            f"day_utc={normalized_scope.get('day_utc')}",
            f"mode={normalized_scope.get('mode')}",
            f"sleeve_id={normalized_scope.get('sleeve_id')}",
            f"attempt_id={normalized_scope.get('attempt_id')}",
        ],
        "precedence_rules_applied": precedence_rules,
        "retrieval_status": retrieval_status if rejected_artifacts or retrieved_artifacts else "UNAVAILABLE",
    }


def _trust_panel_for_route(
    *,
    route_id: str,
    access: AccessDecision,
    retrieval_manifest: Dict[str, Any],
    sources: Dict[str, SourceArtifact],
    suppression_rules: List[str],
    downgrade_rules: List[str],
) -> Dict[str, Any]:
    exactness = "EXACT"
    trust_classification = "TRUSTED"
    limitations = list(access.reasons)
    retrieval_status = str(retrieval_manifest.get("retrieval_status") or "UNAVAILABLE")

    if access.integrity_state in {"EVIDENCE_INCONSISTENT", "INTERNAL_FAILURE"}:
        exactness = "INTEGRITY_CONSTRAINED"
        trust_classification = "INTEGRITY_CONSTRAINED"
    elif access.finalization_state == "UNAVAILABLE" or retrieval_status == "UNAVAILABLE":
        exactness = "UNAVAILABLE"
        trust_classification = "UNAVAILABLE"
    elif access.finalization_state in {"BOUNDED_PARTIAL", "IN_FLIGHT_BLOCKED"} or retrieval_status == "BOUNDED_PARTIAL":
        exactness = "BOUNDED_PARTIAL"
        trust_classification = "LIMITED"

    if retrieval_manifest.get("rejected_artifacts"):
        for item in retrieval_manifest["rejected_artifacts"]:
            if isinstance(item, dict):
                limitations.append(f"{item.get('artifact_id')}:{item.get('reason')}")

    return {
        "schema_id": "operator_trust_panel_v1",
        "schema_version": 1,
        "day_utc": retrieval_manifest.get("day_utc"),
        "view_or_query_id": route_id,
        "trust_classification": trust_classification,
        "finalization_state": access.finalization_state,
        "integrity_state": access.integrity_state,
        "source_count": len(_evidence_refs(sources)),
        "source_refs": _evidence_refs(sources),
        "bounded_limitations": sorted(set(limitations)),
        "exactness_classification": exactness,
        "suppression_rules_applied": sorted(set(suppression_rules)),
        "downgrade_rules_applied": sorted(set(downgrade_rules)),
    }


def _validate_output_bundle(repo_root: Path, home_view: Dict[str, Any], trust_panel: Dict[str, Any], retrieval_manifest: Dict[str, Any]) -> None:
    _validate_doc(retrieval_manifest, repo_root, RETRIEVAL_MANIFEST_SCHEMA)
    _validate_doc(trust_panel, repo_root, TRUST_PANEL_SCHEMA)
    _validate_doc(home_view, repo_root, HOME_VIEW_SCHEMA)


def _validate_query_bundle(repo_root: Path, response_doc: Dict[str, Any], trust_panel: Dict[str, Any], retrieval_manifest: Dict[str, Any]) -> None:
    _validate_doc(retrieval_manifest, repo_root, RETRIEVAL_MANIFEST_SCHEMA)
    _validate_doc(trust_panel, repo_root, TRUST_PANEL_SCHEMA)
    _validate_doc(response_doc, repo_root, QUERY_RESPONSE_SCHEMA)


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
    _validate_doc(doc, repo_root, schema_relpath)
    payload = canonical_json_bytes_v1(doc) + b"\n"
    return write_day_artifact_refreshable_v1(
        path=out_path,
        data=payload,
        expected_day_utc=expected_day_utc,
        expected_schema_id=expected_schema_id,
        expected_schema_version=expected_schema_version,
        preserve_statuses=(),
    )


def _source_ref(sources: Dict[str, SourceArtifact], artifact_name: str) -> Optional[str]:
    source = sources.get(artifact_name)
    if not isinstance(source, SourceArtifact) or not source.present:
        return None
    return str(source.path)


def _panel(panel_id: str, title: str, state: str, lines: List[str], source_refs: List[str], suppressed: bool) -> Dict[str, Any]:
    return {
        "panel_id": panel_id,
        "title": title,
        "state": state,
        "content_lines": lines,
        "source_refs": sorted(set(source_refs)),
        "suppressed": suppressed,
    }


def _show_rule(rule: Dict[str, Any], *, blocked: bool, integrity_ok: bool, summary_classification: str, funnel_available: bool, drift_legit: bool) -> bool:
    condition = str(rule.get("show_when") or "ALWAYS")
    if condition == "ALWAYS":
        return True
    if condition == "INTEGRITY_NOT_OK":
        return not integrity_ok
    if condition == "BLOCKED_OR_DIAGNOSTIC_ATTENTION":
        return blocked or summary_classification == "DIAGNOSTIC_ATTENTION_REQUIRED"
    if condition == "NOT_BLOCKED_AND_FUNNEL_AVAILABLE":
        return (not blocked) and funnel_available
    if condition == "PRIOR_DAY_COMPARISON_LEGITIMATE":
        return drift_legit
    return False


def _build_navigation_options(view_registry: Dict[str, Any], sources: Dict[str, SourceArtifact]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for view in view_registry.get("views") or []:
        if not isinstance(view, dict):
            continue
        view_id = str(view.get("view_id") or "")
        required_sources = [name for name in (view.get("required_sources") or []) if isinstance(name, str) and not name.startswith("operator_")]
        missing = []
        for artifact_name in required_sources:
            source = sources.get(artifact_name)
            if not isinstance(source, SourceArtifact) or not source.present or not source.valid:
                missing.append(artifact_name)
        out.append(
            {
                "view_id": view_id,
                "label": _humanize_view_label(view_id),
                "enabled": not missing,
                "reason": "READY" if not missing else f"MISSING_REQUIRED:{','.join(missing)}",
            }
        )
    return out


def _drift_legitimate(drift_doc: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(drift_doc, dict):
        return False
    for row in drift_doc.get("compared_metrics") or []:
        if isinstance(row, dict) and str(row.get("comparability_status") or "") == "LEGITIMATE":
            return True
    return False


def _build_home_panels(
    *,
    policy: Dict[str, Any],
    sources: Dict[str, SourceArtifact],
    home_trust_ref: str,
    trust_panel_doc: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    gate_doc = _first_valid_doc(sources, ["gate_stack_verdict_v1"]) or {}
    summary_doc = _first_valid_doc(sources, ["daily_summary_v1"]) or {}
    root_doc = _first_valid_doc(sources, ["root_cause_v1"]) or {}
    intent_doc = _first_valid_doc(sources, ["intent_absence_analysis_v1"]) or {}
    funnel_doc = _first_valid_doc(sources, ["funnel_metrics_v1"]) or {}
    drift_doc = _first_valid_doc(sources, ["drift_report_v1"]) or {}

    blocked = str(summary_doc.get("blocked_status") or ("BLOCKED" if str(gate_doc.get("status") or "") != "PASS" else "NOT_BLOCKED")) == "BLOCKED"
    integrity_ok = str(trust_panel_doc.get("integrity_state") or "OK") == "OK"
    summary_classification = str(summary_doc.get("summary_classification") or "UNAVAILABLE")
    funnel_available = bool(funnel_doc)
    drift_legit = _drift_legitimate(drift_doc)
    suppression_rules: List[str] = []

    panels: List[Dict[str, Any]] = []
    rules = sorted((policy.get("panel_rules") or []), key=lambda row: int(row.get("precedence_rank") or 999))
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        panel_id = str(rule.get("panel_id") or "")
        show = _show_rule(
            rule,
            blocked=blocked,
            integrity_ok=integrity_ok,
            summary_classification=summary_classification,
            funnel_available=funnel_available,
            drift_legit=drift_legit,
        )
        suppressed = False
        for suppress_rule in rule.get("suppress_when") or []:
            if suppress_rule == "BLOCKED_STATUS" and blocked:
                suppressed = True
                suppression_rules.append(f"{panel_id}:BLOCKED_STATUS")
            if suppress_rule == "DRIFT_COMPARISON_UNAVAILABLE" and not drift_legit:
                suppressed = True
                suppression_rules.append(f"{panel_id}:DRIFT_COMPARISON_UNAVAILABLE")

        if not show and not bool(rule.get("mandatory")) and not suppressed:
            continue

        if panel_id == "readiness_panel":
            panels.append(
                _panel(
                    panel_id,
                    "Readiness",
                    str(gate_doc.get("status") or "UNAVAILABLE"),
                    [
                        f"status={gate_doc.get('status')}",
                        f"blocking_class={gate_doc.get('blocking_class')}",
                        f"reason_code={((gate_doc.get('reason_codes') or [None])[0])}",
                    ],
                    [ref for ref in [_source_ref(sources, "gate_stack_verdict_v1")] if ref],
                    suppressed,
                )
            )
        elif panel_id == "integrity_panel":
            panels.append(
                _panel(
                    panel_id,
                    "Integrity",
                    str(trust_panel_doc.get("integrity_state") or "OK"),
                    [
                        f"integrity_state={trust_panel_doc.get('integrity_state')}",
                        f"exactness={trust_panel_doc.get('exactness_classification')}",
                    ],
                    [ref for ref in [_source_ref(sources, "daily_summary_v1"), _source_ref(sources, "funnel_metrics_v1")] if ref],
                    suppressed,
                )
            )
        elif panel_id == "diagnostic_panel":
            panels.append(
                _panel(
                    panel_id,
                    "Diagnostics",
                    "ATTENTION" if blocked else "INFO",
                    [
                        f"root_cause={root_doc.get('deterministic_classification')}",
                        f"zero_intents={intent_doc.get('zero_intent_classification')}",
                        f"first_break={root_doc.get('first_break_cause')}",
                    ],
                    [
                        ref
                        for ref in [
                            _source_ref(sources, "root_cause_v1"),
                            _source_ref(sources, "intent_absence_analysis_v1"),
                            _source_ref(sources, "diagnostic_decision_ledger_v1"),
                        ]
                        if ref
                    ],
                    suppressed,
                )
            )
        elif panel_id == "summary_panel":
            count_lines = []
            for row in summary_doc.get("key_counts") or []:
                if isinstance(row, dict):
                    count_lines.append(f"{row.get('metric_id')}={row.get('value')} ({row.get('availability_status')})")
            panels.append(
                _panel(
                    panel_id,
                    "Daily Summary",
                    summary_classification,
                    [f"readiness={summary_doc.get('readiness_status')}", f"blocked={summary_doc.get('blocked_status')}"] + count_lines[:4],
                    [ref for ref in [_source_ref(sources, "daily_summary_v1")] if ref],
                    suppressed,
                )
            )
        elif panel_id == "funnel_panel":
            stage_lines = []
            for row in funnel_doc.get("stage_metrics") or []:
                if isinstance(row, dict):
                    stage_lines.append(
                        f"{row.get('stage_name')}: input={row.get('input_count')} pass={row.get('passed_count')} reject={row.get('rejected_count')}"
                    )
            panels.append(
                _panel(
                    panel_id,
                    "Funnel",
                    str(funnel_doc.get("integrity_status") or "UNAVAILABLE"),
                    [f"dominant_drop={funnel_doc.get('dominant_drop_stage')}", f"reason={funnel_doc.get('dominant_drop_reason_classification')}"] + stage_lines[:5],
                    [ref for ref in [_source_ref(sources, "funnel_metrics_v1")] if ref],
                    suppressed,
                )
            )
        elif panel_id == "drift_panel":
            drift_lines = []
            for row in drift_doc.get("material_changes") or []:
                if isinstance(row, dict):
                    drift_lines.append(f"{row.get('metric_id')} absolute_change={row.get('absolute_change')}")
            panels.append(
                _panel(
                    panel_id,
                    "Prior-Day Drift",
                    str((drift_doc.get("drift_classifications") or ["UNAVAILABLE"])[0]),
                    [f"comparison_day={((drift_doc.get('comparison_window_definition') or {}).get('comparison_day_utc'))}"] + drift_lines[:5],
                    [ref for ref in [_source_ref(sources, "drift_report_v1")] if ref],
                    suppressed,
                )
            )
        elif panel_id == "navigation_panel":
            panels.append(
                _panel(
                    panel_id,
                    "Navigation",
                    "READY",
                    ["Use the governed destinations listed in navigation_options."],
                    [],
                    suppressed,
                )
            )
        elif panel_id == "trust_panel":
            panels.append(
                _panel(
                    panel_id,
                    "Trust",
                    str(trust_panel_doc.get("exactness_classification") or "UNAVAILABLE"),
                    [
                        f"trust={trust_panel_doc.get('trust_classification')}",
                        f"finalization={trust_panel_doc.get('finalization_state')}",
                        f"limitations={len(trust_panel_doc.get('bounded_limitations') or [])}",
                    ],
                    [home_trust_ref],
                    suppressed,
                )
            )
    return panels, suppression_rules


def build_operator_home_bundle(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
) -> Dict[str, Any]:
    view_registry = _read_registry(repo_root, VIEW_REGISTRY_RELPATH)
    home_policy = _read_registry(repo_root, HOME_POLICY_RELPATH)
    view_contract = next(
        row for row in (view_registry.get("views") or []) if isinstance(row, dict) and row.get("view_id") == "home_view_contract_v1"
    )
    sources = load_operator_sources(repo_root, truth_root, day_utc)
    run_scope = derive_run_scope(truth_root, day_utc, sources)
    access = evaluate_operator_access("home_view_contract_v1", list(view_contract.get("required_sources") or []), sources)
    retrieval_manifest = _build_retrieval_manifest(
        route_id="home_view_contract_v1",
        route_classification="HOME_VIEW",
        requested_scope={
            "day_utc": day_utc,
            "mode": run_scope.get("mode"),
            "sleeve_id": run_scope.get("sleeve_id"),
            "attempt_id": run_scope.get("attempt_id"),
            "truth_partition": run_scope.get("truth_partition"),
        },
        normalized_scope={
            "day_utc": day_utc,
            "mode": run_scope.get("mode"),
            "sleeve_id": run_scope.get("sleeve_id"),
            "attempt_id": run_scope.get("attempt_id"),
            "truth_partition": run_scope.get("truth_partition"),
        },
        required_source_names=list(view_contract.get("required_sources") or []),
        optional_source_names=list(view_contract.get("optional_sources") or []),
        precedence_rules=list(view_contract.get("display_precedence") or []),
        sources=sources,
    )
    trust_panel_ref = _artifact_ref(truth_root, "operator_trust_panel_v1", day_utc, "operator_home_trust_panel.v1.json")
    retrieval_ref = _artifact_ref(
        truth_root, "operator_retrieval_manifest_v1", day_utc, "operator_home_retrieval_manifest.v1.json"
    )
    trust_panel = _trust_panel_for_route(
        route_id="home_view_contract_v1",
        access=access,
        retrieval_manifest=retrieval_manifest,
        sources=sources,
        suppression_rules=[],
        downgrade_rules=list(access.reasons),
    )
    rendered_panels, suppression_rules = _build_home_panels(
        policy=home_policy,
        sources=sources,
        home_trust_ref=trust_panel_ref,
        trust_panel_doc=trust_panel,
    )
    trust_panel["suppression_rules_applied"] = sorted(set(trust_panel["suppression_rules_applied"] + suppression_rules))
    navigation_options = _build_navigation_options(view_registry, sources)

    summary_doc = _first_valid_doc(sources, ["daily_summary_v1"]) or {}
    home_view = {
        "schema_id": "operator_home_view_v1",
        "schema_version": 1,
        "day_utc": day_utc,
        "interface_ruleset_id": INTERFACE_RULESET_ID,
        "interface_ruleset_version": INTERFACE_RULESET_VERSION,
        "view_contract_id": "home_view_contract_v1",
        "view_contract_version": 1,
        "run_scope": run_scope,
        "home_status_classification": str(summary_doc.get("summary_classification") or trust_panel.get("exactness_classification")),
        "readiness_ref": _artifact_ref(truth_root, "gate_stack_verdict_v1", day_utc, "gate_stack_verdict.v1.json"),
        "daily_summary_ref": _artifact_ref(truth_root, "daily_summary_v1", day_utc, "daily_summary.v1.json"),
        "diagnostic_refs": [
            _artifact_ref(truth_root, "root_cause_v1", day_utc, "root_cause.v1.json"),
            _artifact_ref(truth_root, "intent_absence_analysis_v1", day_utc, "intent_absence_analysis.v1.json"),
            _artifact_ref(truth_root, "diagnostic_decision_ledger_v1", day_utc, "diagnostic_decision_ledger.v1.json"),
        ],
        "visibility_refs": [
            _artifact_ref(truth_root, "visibility_metric_snapshot_v1", day_utc, "visibility_metric_snapshot.v1.json"),
            _artifact_ref(truth_root, "funnel_metrics_v1", day_utc, "funnel_metrics.v1.json"),
            _artifact_ref(truth_root, "drift_report_v1", day_utc, "drift_report.v1.json"),
            _artifact_ref(truth_root, "visibility_decision_ledger_v1", day_utc, "visibility_decision_ledger.v1.json"),
        ],
        "rendered_panels": rendered_panels,
        "navigation_options": navigation_options,
        "trust_panel_ref": trust_panel_ref,
        "evidence_refs": _evidence_refs(sources),
        "partiality_status": trust_panel["exactness_classification"],
        "integrity_status": trust_panel["integrity_state"],
    }

    _validate_output_bundle(repo_root, home_view, trust_panel, retrieval_manifest)
    return {
        "home_view": home_view,
        "trust_panel": trust_panel,
        "retrieval_manifest": retrieval_manifest,
        "trust_panel_ref": trust_panel_ref,
        "retrieval_manifest_ref": retrieval_ref,
        "sources": sources,
    }


def _template_version(template_registry: Dict[str, Any], template_id: str) -> int:
    template = _templates_by_id(template_registry).get(template_id) or {}
    return int(template.get("response_template_version") or 1)


def _render_query_answer_blocks(
    *,
    query_class_id: str,
    normalized_query: NormalizedQuery,
    sources: Dict[str, SourceArtifact],
    trust_panel: Dict[str, Any],
) -> List[Dict[str, Any]]:
    gate_doc = _first_valid_doc(sources, ["gate_stack_verdict_v1"]) or {}
    root_doc = _first_valid_doc(sources, ["root_cause_v1"]) or {}
    intent_doc = _first_valid_doc(sources, ["intent_absence_analysis_v1"]) or {}
    summary_doc = _first_valid_doc(sources, ["daily_summary_v1"]) or {}
    funnel_doc = _first_valid_doc(sources, ["funnel_metrics_v1"]) or {}
    drift_doc = _first_valid_doc(sources, ["drift_report_v1"]) or {}
    source_lines: List[Dict[str, Any]] = []

    if query_class_id == "readiness_state_query_v1":
        source_lines.append(
            {
                "block_id": "headline",
                "title": "Readiness State",
                "lines": [
                    f"status={gate_doc.get('status')}",
                    f"blocked_status={summary_doc.get('blocked_status')}",
                    f"blocking_class={gate_doc.get('blocking_class')}",
                ],
                "source_refs": [ref for ref in [_source_ref(sources, "gate_stack_verdict_v1"), _source_ref(sources, "daily_summary_v1")] if ref],
            }
        )
    elif query_class_id == "blocker_reason_query_v1":
        source_lines.append(
            {
                "block_id": "headline",
                "title": "Blocker Reason",
                "lines": [
                    f"blocking_class={gate_doc.get('blocking_class')}",
                    f"reason_code={((gate_doc.get('reason_codes') or [None])[0])}",
                    f"root_cause={root_doc.get('deterministic_classification')}",
                ],
                "source_refs": [ref for ref in [_source_ref(sources, "gate_stack_verdict_v1"), _source_ref(sources, "root_cause_v1")] if ref],
            }
        )
    elif query_class_id == "root_cause_query_v1":
        source_lines.append(
            {
                "block_id": "headline",
                "title": "Root Cause",
                "lines": [
                    f"classification={root_doc.get('deterministic_classification')}",
                    f"first_break_cause={root_doc.get('first_break_cause')}",
                    f"failing_stage={root_doc.get('failing_stage')}",
                ],
                "source_refs": [ref for ref in [_source_ref(sources, "root_cause_v1"), _source_ref(sources, "diagnostic_decision_ledger_v1")] if ref],
            }
        )
    elif query_class_id == "no_intents_query_v1":
        source_lines.append(
            {
                "block_id": "headline",
                "title": "No Intents",
                "lines": [
                    f"zero_intent_classification={intent_doc.get('zero_intent_classification')}",
                    f"dominant_elimination_stage={intent_doc.get('dominant_elimination_stage')}",
                    f"final_intent_count={intent_doc.get('final_intent_count')}",
                ],
                "source_refs": [ref for ref in [_source_ref(sources, "intent_absence_analysis_v1")] if ref],
            }
        )
    elif query_class_id == "daily_summary_query_v1":
        count_lines = []
        for row in summary_doc.get("key_counts") or []:
            if isinstance(row, dict):
                count_lines.append(f"{row.get('metric_id')}={row.get('value')} ({row.get('availability_status')})")
        source_lines.append(
            {
                "block_id": "headline",
                "title": "Daily Summary",
                "lines": [
                    f"summary_classification={summary_doc.get('summary_classification')}",
                    f"readiness_status={summary_doc.get('readiness_status')}",
                    f"blocked_status={summary_doc.get('blocked_status')}",
                ] + count_lines[:4],
                "source_refs": [ref for ref in [_source_ref(sources, "daily_summary_v1")] if ref],
            }
        )
    elif query_class_id == "funnel_query_v1":
        lines = [
            f"total_intent_count={funnel_doc.get('total_intent_count')}",
            f"dominant_drop_stage={funnel_doc.get('dominant_drop_stage')}",
            f"dominant_drop_reason={funnel_doc.get('dominant_drop_reason_classification')}",
        ]
        for row in funnel_doc.get("stage_metrics") or []:
            if isinstance(row, dict):
                lines.append(
                    f"{row.get('stage_name')}: input={row.get('input_count')} pass={row.get('passed_count')} reject={row.get('rejected_count')}"
                )
        source_lines.append(
            {
                "block_id": "headline",
                "title": "Funnel",
                "lines": lines[:8],
                "source_refs": [ref for ref in [_source_ref(sources, "funnel_metrics_v1")] if ref],
            }
        )
    elif query_class_id == "drift_query_v1":
        lines = [
            f"comparison_mode={((drift_doc.get('comparison_window_definition') or {}).get('comparison_mode'))}",
            f"comparison_day_utc={((drift_doc.get('comparison_window_definition') or {}).get('comparison_day_utc'))}",
        ]
        for row in drift_doc.get("material_changes") or []:
            if isinstance(row, dict):
                lines.append(f"{row.get('metric_id')} absolute_change={row.get('absolute_change')}")
        source_lines.append(
            {
                "block_id": "headline",
                "title": "Prior-Day Drift",
                "lines": lines[:8],
                "source_refs": [ref for ref in [_source_ref(sources, "drift_report_v1")] if ref],
            }
        )
    elif query_class_id == "trust_explanation_query_v1":
        source_lines.append(
            {
                "block_id": "headline",
                "title": "Trust Explanation",
                "lines": [
                    f"trust_classification={trust_panel.get('trust_classification')}",
                    f"finalization_state={trust_panel.get('finalization_state')}",
                    f"exactness_classification={trust_panel.get('exactness_classification')}",
                ] + [f"limitation={line}" for line in (trust_panel.get("bounded_limitations") or [])[:5]],
                "source_refs": [ref for ref in [_source_ref(sources, "gate_stack_verdict_v1"), _source_ref(sources, "daily_summary_v1")] if ref],
            }
        )
    else:
        source_lines.append(
            {
                "block_id": "headline",
                "title": "Unsupported Query",
                "lines": [
                    "unsupported_query=TRUE",
                    f"normalized_query_text={normalized_query.normalized_query_text}",
                ],
                "source_refs": [],
            }
        )
    return source_lines


def build_operator_query_bundle(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    query_text: str,
) -> Dict[str, Any]:
    try:
        query_registry = _read_registry(repo_root, QUERY_POLICY_RELPATH)
        template_registry = _read_registry(repo_root, TEMPLATE_REGISTRY_RELPATH)
        sources = load_operator_sources(repo_root, truth_root, day_utc)
        run_scope = derive_run_scope(truth_root, day_utc, sources)
        normalized = normalize_operator_query(query_text, run_scope)
        normalized_day = str(normalized.normalized_scope.get("day_utc") or day_utc)
        normalized_sources = load_operator_sources(repo_root, truth_root, normalized_day)
        normalized_run_scope = derive_run_scope(truth_root, normalized_day, normalized_sources)
        normalized_scope = {
            "day_utc": normalized.normalized_scope.get("day_utc"),
            "mode": normalized.normalized_scope.get("mode") or normalized_run_scope.get("mode"),
            "sleeve_id": normalized.normalized_scope.get("sleeve_id") or normalized_run_scope.get("sleeve_id"),
            "attempt_id": normalized.normalized_scope.get("attempt_id") or normalized_run_scope.get("attempt_id"),
            "truth_partition": normalized_run_scope.get("truth_partition"),
        }
        normalized = NormalizedQuery(
            query_id=normalized.query_id,
            original_query_text=normalized.original_query_text,
            normalized_query_text=normalized.normalized_query_text,
            requested_scope=normalized.requested_scope,
            normalized_scope=normalized_scope,
            extracted_scope_fields=normalized.extracted_scope_fields,
        )
        policy = classify_query(normalized, query_registry)
        query_class_id = str(policy.get("query_class_id") or "unsupported_query_v1")
        required_sources = list(policy.get("required_sources") or [])
        optional_sources = list(policy.get("optional_sources") or [])
        access = evaluate_operator_access(query_class_id, required_sources, normalized_sources)
        retrieval_manifest = _build_retrieval_manifest(
            route_id=normalized.query_id,
            route_classification=query_class_id,
            requested_scope=normalized.requested_scope,
            normalized_scope=normalized.normalized_scope,
            required_source_names=required_sources,
            optional_source_names=optional_sources,
            precedence_rules=list(policy.get("precedence_rules") or []),
            sources=normalized_sources,
        )
        unsupported = query_class_id == "unsupported_query_v1"
        downgrade_rules = list(access.reasons)
        if unsupported:
            retrieval_manifest["retrieval_status"] = "UNSUPPORTED"
            downgrade_rules.append("unsupported_query_class")
        trust_panel = _trust_panel_for_route(
            route_id=normalized.query_id,
            access=access,
            retrieval_manifest=retrieval_manifest,
            sources=normalized_sources,
            suppression_rules=[],
            downgrade_rules=downgrade_rules,
        )
        trust_ref = _embedded_ref("operator_trust_panel_v1", normalized.query_id)
        manifest_ref = _embedded_ref("operator_retrieval_manifest_v1", normalized.query_id)
        answer_blocks = _render_query_answer_blocks(
            query_class_id=query_class_id,
            normalized_query=normalized,
            sources=normalized_sources,
            trust_panel=trust_panel,
        )
        template_id = str(policy.get("allowed_answer_template") or "unsupported_template_v1")
        response_status = "UNSUPPORTED" if unsupported else trust_panel["exactness_classification"]
        integrity_status = trust_panel["integrity_state"]
        unsupported_reason = None if not unsupported else str(policy.get("unsupported_boundary") or "UNREGISTERED_INTENT")
        response_doc = {
            "schema_id": "operator_query_response_v1",
            "schema_version": 1,
            "day_utc": normalized_scope.get("day_utc"),
            "interface_ruleset_id": INTERFACE_RULESET_ID,
            "interface_ruleset_version": INTERFACE_RULESET_VERSION,
            "query_class_id": query_class_id,
            "query_class_version": int(policy.get("query_class_version") or 1),
            "response_template_id": template_id,
            "response_template_version": _template_version(template_registry, template_id),
            "query_id": normalized.query_id,
            "original_query_text": normalized.original_query_text,
            "normalized_query_text": normalized.normalized_query_text,
            "run_scope": normalized_run_scope,
            "response_status": response_status,
            "answer_blocks": answer_blocks,
            "source_refs": _evidence_refs(normalized_sources),
            "retrieval_manifest_ref": manifest_ref,
            "trust_panel_ref": trust_ref,
            "unsupported_reason": unsupported_reason,
            "partiality_status": trust_panel["exactness_classification"],
            "integrity_status": integrity_status,
        }
        _validate_query_bundle(repo_root, response_doc, trust_panel, retrieval_manifest)
        return {
            "query_response": response_doc,
            "trust_panel": trust_panel,
            "retrieval_manifest": retrieval_manifest,
            "sources": normalized_sources,
        }
    except Exception as exc:
        failure_day = _require_day_utc(day_utc)
        normalized_scope = {
            "day_utc": failure_day,
            "mode": None,
            "sleeve_id": None,
            "attempt_id": None,
            "truth_partition": str(truth_root),
        }
        query_id = f"{failure_day}:{uuid.uuid5(uuid.NAMESPACE_URL, str(query_text or '')).hex[:16]}"
        retrieval_manifest = {
            "schema_id": "operator_retrieval_manifest_v1",
            "schema_version": 1,
            "day_utc": failure_day,
            "query_or_view_id": query_id,
            "route_classification": "INTERNAL_FAILURE",
            "requested_scope": normalized_scope,
            "normalized_scope": normalized_scope,
            "retrieved_artifacts": [],
            "rejected_artifacts": [{"artifact_id": "batch3", "reason": f"INTERNAL_FAILURE:{type(exc).__name__}"}],
            "scope_filters_applied": [],
            "precedence_rules_applied": [],
            "retrieval_status": "UNAVAILABLE",
        }
        trust_panel = {
            "schema_id": "operator_trust_panel_v1",
            "schema_version": 1,
            "day_utc": failure_day,
            "view_or_query_id": query_id,
            "trust_classification": "INTEGRITY_CONSTRAINED",
            "finalization_state": "UNAVAILABLE",
            "integrity_state": "INTERNAL_FAILURE",
            "source_count": 0,
            "source_refs": [],
            "bounded_limitations": [f"INTERNAL_FAILURE:{type(exc).__name__}", traceback.format_exc(limit=1).strip()],
            "exactness_classification": "INTEGRITY_CONSTRAINED",
            "suppression_rules_applied": ["SUPPRESS_AUTHORITATIVE_PRESENTATION_ON_INTERNAL_FAILURE"],
            "downgrade_rules_applied": ["DOWNGRADE_INTERNAL_FAILURE"],
        }
        response_doc = {
            "schema_id": "operator_query_response_v1",
            "schema_version": 1,
            "day_utc": failure_day,
            "interface_ruleset_id": INTERFACE_RULESET_ID,
            "interface_ruleset_version": INTERFACE_RULESET_VERSION,
            "query_class_id": "unsupported_query_v1",
            "query_class_version": 1,
            "response_template_id": "unsupported_template_v1",
            "response_template_version": 1,
            "query_id": query_id,
            "original_query_text": str(query_text or ""),
            "normalized_query_text": _normalize_text(str(query_text or "")),
            "run_scope": {
                "day_utc": failure_day,
                "mode": None,
                "sleeve_id": None,
                "engine_id": None,
                "attempt_id": None,
                "attempt_seq": None,
                "truth_partition": str(truth_root),
            },
            "response_status": "UNAVAILABLE",
            "answer_blocks": [
                {
                    "block_id": "headline",
                    "title": "Batch 3 Internal Failure",
                    "lines": [f"error={type(exc).__name__}", str(exc)],
                    "source_refs": [],
                }
            ],
            "source_refs": [],
            "retrieval_manifest_ref": _embedded_ref("operator_retrieval_manifest_v1", query_id),
            "trust_panel_ref": _embedded_ref("operator_trust_panel_v1", query_id),
            "unsupported_reason": None,
            "partiality_status": "INTEGRITY_CONSTRAINED",
            "integrity_status": "INTERNAL_FAILURE",
        }
        _validate_query_bundle(repo_root, response_doc, trust_panel, retrieval_manifest)
        return {
            "query_response": response_doc,
            "trust_panel": trust_panel,
            "retrieval_manifest": retrieval_manifest,
            "sources": {},
        }


def write_batch3_operator_home(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    produced_utc: str,
) -> Dict[str, RefreshWriteResultV1]:
    _require_day_utc(day_utc)
    _require_produced_utc(day_utc, produced_utc)
    bundle = build_operator_home_bundle(repo_root=repo_root, truth_root=truth_root, day_utc=day_utc)
    home_view = bundle["home_view"]
    trust_panel = bundle["trust_panel"]
    retrieval_manifest = bundle["retrieval_manifest"]

    home_path = (truth_root / "reports" / "operator_home_view_v1" / day_utc / "operator_home_view.v1.json").resolve()
    trust_path = (
        truth_root / "reports" / "operator_trust_panel_v1" / day_utc / "operator_home_trust_panel.v1.json"
    ).resolve()
    retrieval_path = (
        truth_root / "reports" / "operator_retrieval_manifest_v1" / day_utc / "operator_home_retrieval_manifest.v1.json"
    ).resolve()

    home_view["trust_panel_ref"] = str(trust_path.resolve())
    trust_panel["view_or_query_id"] = "home_view_contract_v1"

    writes = {
        "retrieval_manifest": _write_refreshable_json(
            repo_root=repo_root,
            out_path=retrieval_path,
            doc=retrieval_manifest,
            schema_relpath=RETRIEVAL_MANIFEST_SCHEMA,
            expected_schema_id="operator_retrieval_manifest_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
        "trust_panel": _write_refreshable_json(
            repo_root=repo_root,
            out_path=trust_path,
            doc=trust_panel,
            schema_relpath=TRUST_PANEL_SCHEMA,
            expected_schema_id="operator_trust_panel_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
        "home_view": _write_refreshable_json(
            repo_root=repo_root,
            out_path=home_path,
            doc=home_view,
            schema_relpath=HOME_VIEW_SCHEMA,
            expected_schema_id="operator_home_view_v1",
            expected_schema_version=1,
            expected_day_utc=day_utc,
        ),
    }
    return writes


def build_batch3_home_docs(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
) -> Dict[str, Dict[str, Any]]:
    bundle = build_operator_home_bundle(repo_root=repo_root, truth_root=truth_root, day_utc=day_utc)
    return {
        "home_view": bundle["home_view"],
        "trust_panel": bundle["trust_panel"],
        "retrieval_manifest": bundle["retrieval_manifest"],
    }
