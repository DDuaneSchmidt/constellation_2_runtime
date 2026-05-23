from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_research_hypothesis_classification_v1"

CLASSIFICATIONS = {
    "REAL_OPERATOR_HYPOTHESIS",
    "SYSTEM_GENERATED_HYPOTHESIS",
    "TEST_FIXTURE",
    "LEGACY_DUPLICATE",
    "ARCHIVED",
    "REJECTED",
    "NEEDS_MANUAL_CLASSIFICATION",
    "UNKNOWN",
}

ACTIVE_STATUSES = {"IDEA", "PROPOSED", "RESEARCH_QUEUED", "DATA_NEEDED", "BACKTEST_READY", "PAPER_TEST_CANDIDATE", "SLEEVE_REVIEW_CANDIDATE"}
QUEUED_STATUSES = {"RESEARCH_QUEUED", "DATA_NEEDED", "BACKTEST_READY", "TEST_QUEUED", "QUEUED"}
VALIDATED_STATUSES = {"VALIDATED"}
COMPLETED_STATUSES = {"BACKTEST_COMPLETE", "RESULT_REVIEWED", "COMPLETED", "TESTED"}
REJECTED_STATUSES = {"REJECTED", "AUTO_REJECTED", "RETIRED", "INVALIDATED"}
ARCHIVED_STATUSES = {"AUTO_ARCHIVED", "ARCHIVED"}
OPERATOR_SOURCES = {"MANUAL", "OPERATOR", "USER", "HUMAN", "HUMAN_OPERATOR", "MANUAL_OPERATOR_ENTRY"}
SYSTEM_SOURCES = {"AI_RESEARCH", "CHATGPT_SEED", "SYSTEM", "DETERMINISTIC", "RESEARCH_LAB", "UNKNOWN"}


def build_research_hypothesis_classification_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    rows = _discover_rows(root=root, day_utc=day_utc)
    classified = _classify_rows(rows)
    return {
        "schema_id": "aegis_research_hypothesis_classification",
        "schema_version": "v1",
        "artifact_id": "aegis_research_hypothesis_classification_v1",
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "truth_root": str(root),
        "source_root": str(root.parent / "research_lab") if root.name == "truth" else str(root / "research_lab"),
        "classification_rules": {
            "fixture_or_test_naming": "TEST_FIXTURE",
            "legacy_with_canonical_counterpart": "LEGACY_DUPLICATE",
            "rejected_status": "REJECTED",
            "archived_status": "ARCHIVED",
            "manual_operator_source": "REAL_OPERATOR_HYPOTHESIS",
            "system_or_ai_seed_source": "SYSTEM_GENERATED_HYPOTHESIS",
            "orphaned_without_provenance": "NEEDS_MANUAL_CLASSIFICATION",
        },
        "summary_counts": _summary_counts(classified),
        "hypotheses": classified,
        "safety": {
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
            "unsupported_ai_claims_allowed": False,
        },
    }


def write_research_hypothesis_classification_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "research_hypothesis_classification.v1.json", payload)
    summary_path = out_dir / "research_hypothesis_classification.summary.txt"
    matrix_path = out_dir / "research_hypothesis_classification.matrix.csv"
    summary_path.write_text(render_research_hypothesis_classification_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_research_hypothesis_classification_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_research_hypothesis_classification_summary_v1(payload: dict[str, Any]) -> str:
    counts = payload.get("summary_counts") if isinstance(payload.get("summary_counts"), dict) else {}
    lines = [
        "AEGIS RESEARCH HYPOTHESIS CLASSIFICATION v1",
        f"day_utc: {payload.get('day_utc')}",
        f"total_hypotheses_found: {counts.get('total', 0)}",
        f"orphaned_hypotheses_count: {counts.get('orphaned', 0)}",
        f"real_operator_hypotheses_count: {counts.get('REAL_OPERATOR_HYPOTHESIS', 0)}",
        f"system_generated_hypotheses_count: {counts.get('SYSTEM_GENERATED_HYPOTHESIS', 0)}",
        f"test_fixture_count: {counts.get('TEST_FIXTURE', 0)}",
        f"duplicate_count: {counts.get('duplicates', 0)}",
        f"needs_manual_classification_count: {counts.get('NEEDS_MANUAL_CLASSIFICATION', 0)}",
        "",
        "classification_notes:",
        "- fixture/test naming is classified as TEST_FIXTURE and excluded from active Research UI.",
        "- legacy EDGE records with matching rh-edge canonical records are linked as LEGACY_DUPLICATE.",
        "- rejected and archived records remain available as collapsed history.",
        "- unknown orphaned records are surfaced under Needs Classification.",
        "- no source hypothesis artifact was deleted or rewritten.",
        "- broker_execution_allowed: false",
        "- autonomous_execution_allowed: false",
        "",
        "orphaned_rows:",
    ]
    orphaned = [row for row in payload.get("hypotheses") or [] if row.get("orphaned") is True]
    if not orphaned:
        lines.append("- none")
    for row in orphaned:
        lines.append(f"- {row.get('hypothesis_id')}: {row.get('classification')} | {row.get('orphan_reason')} | {row.get('path')}")
    lines.append("")
    return "\n".join(lines)


def render_research_hypothesis_classification_matrix_csv_v1(payload: dict[str, Any]) -> str:
    fieldnames = [
        "hypothesis_id",
        "path",
        "classification",
        "current_status",
        "normalized_status",
        "orphaned",
        "orphan_reason",
        "duplicate_of",
        "appears_in_research_ui",
        "recommended_action",
        "source_hash",
    ]
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    for row in payload.get("hypotheses") or []:
        writer.writerow({key: row.get(key, "") for key in fieldnames})
    return out.getvalue()


def _discover_rows(*, root: Path, day_utc: str) -> list[dict[str, Any]]:
    search_roots = [root / "research_lab"]
    if root.name == "truth":
        search_roots.append(root.parent / "research_lab")
    rows: list[dict[str, Any]] = []
    for base in search_roots:
        if not base.exists():
            continue
        for path in sorted(base.rglob("research_hypothesis.v1.json")):
            payload = read_json_v1(path)
            row = _normalize_canonical(path=path, payload=payload, day_utc=day_utc)
            if row:
                rows.append(row)
        for path in sorted(base.rglob("*.edge_hypothesis.v1.json")):
            payload = read_json_v1(path)
            row = _normalize_legacy(path=path, payload=payload, day_utc=day_utc)
            if row:
                rows.append(row)
    return sorted(rows, key=lambda row: (str(row.get("created_at_utc") or ""), str(row.get("hypothesis_id") or ""), str(row.get("path") or "")))


def _classify_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical_by_key: dict[str, dict[str, Any]] = {}
    first_by_idea: dict[str, dict[str, Any]] = {}
    for row in rows:
        idea_key = _idea_key(row)
        if idea_key and idea_key not in first_by_idea:
            first_by_idea[idea_key] = row
        if row.get("schema_type") == "research_hypothesis.v1":
            for key in _canonical_keys(row):
                canonical_by_key[key] = row

    classified: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        status = str(item.get("status") or "UNKNOWN").upper()
        source = str(item.get("source") or "UNKNOWN").upper()
        path_text = str(item.get("path") or "")
        id_text = str(item.get("hypothesis_id") or "")
        idea_key = _idea_key(item)
        duplicate = _matching_canonical(item, canonical_by_key)
        repeated = first_by_idea.get(idea_key) if idea_key else None

        classification = "UNKNOWN"
        evidence: list[str] = []
        orphaned = False
        orphan_reason = ""
        duplicate_of = ""
        appears_in_ui = True

        if _looks_like_fixture(path_text, id_text):
            classification = "TEST_FIXTURE"
            appears_in_ui = False
            orphaned = bool(item.get("legacy_source"))
            orphan_reason = "Fixture/test naming indicates a test fixture, not active operator research."
            evidence.append("fixture/test naming in hypothesis id or path")
        elif item.get("legacy_source") and duplicate:
            classification = "LEGACY_DUPLICATE"
            duplicate_of = str(duplicate.get("hypothesis_id") or duplicate.get("path") or "")
            orphaned = False
            orphan_reason = "Legacy edge_hypothesis has a matching canonical research_hypothesis.v1 record."
            evidence.append(f"matching canonical record: {duplicate_of}")
        elif status in REJECTED_STATUSES:
            classification = "REJECTED"
            evidence.append(f"status={status}")
        elif status in ARCHIVED_STATUSES:
            classification = "ARCHIVED"
            evidence.append(f"status={status}")
        elif source in OPERATOR_SOURCES:
            classification = "REAL_OPERATOR_HYPOTHESIS"
            evidence.append(f"source={source}")
        elif item.get("legacy_source") and not duplicate:
            classification = "NEEDS_MANUAL_CLASSIFICATION"
            orphaned = True
            orphan_reason = "Legacy edge_hypothesis has no matching canonical research_hypothesis.v1 record."
            evidence.append("legacy source without canonical counterpart")
        elif source in SYSTEM_SOURCES:
            classification = "SYSTEM_GENERATED_HYPOTHESIS"
            evidence.append(f"source={source}")
        else:
            classification = "UNKNOWN"
            evidence.append("source/status did not prove operator, system, fixture, rejected, archived, or duplicate class")

        if item.get("legacy_source") and repeated and repeated is not row and not duplicate_of:
            duplicate_of = str(repeated.get("hypothesis_id") or repeated.get("path") or "")
            evidence.append(f"repeated idea key: {idea_key}")

        normalized_status = _normalized_status(status=status, classification=classification)
        item.update(
            {
                "classification": classification,
                "current_status": status,
                "normalized_status": normalized_status,
                "orphaned": orphaned,
                "orphan_reason": orphan_reason,
                "duplicate_of": duplicate_of,
                "appears_in_research_ui": appears_in_ui,
                "recommended_action": _recommended_action(classification, normalized_status),
                "evidence": evidence,
            }
        )
        classified.append(item)
    return classified


def _normalize_canonical(*, path: Path, payload: dict[str, Any], day_utc: str) -> dict[str, Any] | None:
    if not payload:
        return None
    hypothesis_id = str(payload.get("hypothesis_id") or path.parent.name)
    source_idea_id = str(payload.get("source_idea_id") or payload.get("source_edge_id") or "")
    if not source_idea_id and hypothesis_id.startswith("rh-edge-"):
        source_idea_id = hypothesis_id.replace("rh-", "", 1).upper()
    status = str(payload.get("status") or payload.get("lifecycle_state") or "UNKNOWN").upper()
    title = str(payload.get("title") or payload.get("hypothesis_summary") or hypothesis_id)
    created_at = str(payload.get("created_at_utc") or payload.get("generated_at_utc") or payload.get("generated_at") or "")
    return {
        "hypothesis_id": hypothesis_id,
        "canonical_hypothesis_id": hypothesis_id,
        "source_idea_id": source_idea_id,
        "title": title,
        "hypothesis_summary": str(payload.get("hypothesis_summary") or title),
        "status": status,
        "edge_family": payload.get("edge_family") or "UNKNOWN",
        "source": payload.get("source") or "UNKNOWN",
        "created_at_utc": created_at,
        "updated_at_utc": str(payload.get("updated_at_utc") or payload.get("updated_utc") or ""),
        "schema_type": "research_hypothesis.v1",
        "legacy_source": False,
        "active": status in ACTIVE_STATUSES,
        "queued": status in QUEUED_STATUSES,
        "symbols": payload.get("symbols") or payload.get("instruments") or [],
        "instruments": payload.get("instruments") or payload.get("symbols") or [],
        "required_symbols": payload.get("required_symbols") or payload.get("symbols") or payload.get("instruments") or [],
        "missing_symbols": payload.get("missing_symbols") or [],
        "missing_datasets": payload.get("missing_datasets") or payload.get("required_data") or [],
        "required_data": payload.get("required_data") or [],
        "event_type": payload.get("event_type") or "",
        "readiness": payload.get("readiness") if isinstance(payload.get("readiness"), dict) else {},
        "blocking_items": payload.get("blocking_items") or [],
        "path": str(path),
        "source_artifact": str(path),
        "source_hash": _file_hash(path),
        "freshness_status": "CURRENT" if day_utc in path.parts or created_at.startswith(day_utc) else "HISTORICAL",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _normalize_legacy(*, path: Path, payload: dict[str, Any], day_utc: str) -> dict[str, Any] | None:
    if not payload:
        return None
    idea_id = str(payload.get("idea_id") or path.stem.replace(".edge_hypothesis.v1", ""))
    status = str(payload.get("status") or "UNKNOWN").upper()
    title = str(payload.get("title") or payload.get("hypothesis") or idea_id)
    created_at = str(payload.get("created_at_utc") or payload.get("created_utc") or "")
    canonical_hypothesis_id = f"rh-{idea_id.lower()}" if idea_id.upper().startswith("EDGE-") else ""
    return {
        "hypothesis_id": idea_id,
        "canonical_hypothesis_id": canonical_hypothesis_id,
        "source_idea_id": idea_id,
        "title": title,
        "hypothesis_summary": str(payload.get("hypothesis") or payload.get("thesis") or title),
        "status": status,
        "edge_family": payload.get("edge_family") or payload.get("edge_type") or "UNKNOWN",
        "source": payload.get("source") or "UNKNOWN",
        "created_at_utc": created_at,
        "updated_at_utc": str(payload.get("updated_at_utc") or payload.get("updated_utc") or ""),
        "schema_type": "edge_hypothesis.v1",
        "legacy_source": True,
        "active": status in {"PROPOSED", "VALIDATED", "TEST_QUEUED", "QUEUED"},
        "queued": "QUEUED" in status,
        "path": str(path),
        "source_artifact": str(path),
        "source_hash": _file_hash(path),
        "freshness_status": "CURRENT" if day_utc in path.parts or created_at.startswith(day_utc) else "HISTORICAL",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _canonical_keys(row: dict[str, Any]) -> set[str]:
    keys = {str(row.get("hypothesis_id") or "").lower()}
    source_idea_id = str(row.get("source_idea_id") or "").lower()
    if source_idea_id:
        keys.add(source_idea_id)
    if str(row.get("hypothesis_id") or "").lower().startswith("rh-"):
        keys.add(str(row.get("hypothesis_id") or "").lower().replace("rh-", "", 1))
    return {key for key in keys if key}


def _matching_canonical(row: dict[str, Any], canonical_by_key: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    for key in _canonical_keys(row):
        if key in canonical_by_key:
            return canonical_by_key[key]
    canonical_id = str(row.get("canonical_hypothesis_id") or "").lower()
    if canonical_id and canonical_id in canonical_by_key:
        return canonical_by_key[canonical_id]
    return None


def _idea_key(row: dict[str, Any]) -> str:
    return str(row.get("source_idea_id") or row.get("canonical_hypothesis_id") or row.get("hypothesis_id") or "").lower()


def _looks_like_fixture(path_text: str, id_text: str) -> bool:
    text = f"{path_text} {id_text}".lower()
    return "fixture" in text or "/tests/" in text or "test_fixture" in text


def _normalized_status(*, status: str, classification: str) -> str:
    if classification == "TEST_FIXTURE":
        return "TEST_FIXTURE"
    if classification == "LEGACY_DUPLICATE":
        return "DUPLICATE"
    if classification == "NEEDS_MANUAL_CLASSIFICATION":
        return "NEEDS_CLASSIFICATION"
    if classification == "REJECTED" or status in REJECTED_STATUSES:
        return "REJECTED"
    if classification == "ARCHIVED" or status in ARCHIVED_STATUSES:
        return "ARCHIVED"
    if status in VALIDATED_STATUSES:
        return "VALIDATED"
    if status in COMPLETED_STATUSES:
        return "COMPLETED"
    if status in QUEUED_STATUSES:
        return "QUEUED"
    if status in ACTIVE_STATUSES:
        return "ACTIVE"
    return "UNKNOWN"


def _recommended_action(classification: str, normalized_status: str) -> str:
    if classification == "TEST_FIXTURE":
        return "Exclude from active Research UI; preserve as source artifact."
    if classification == "LEGACY_DUPLICATE":
        return "Show under legacy duplicates; use canonical hypothesis as the active record."
    if normalized_status == "NEEDS_CLASSIFICATION":
        return "Operator should classify as real, archived, rejected, duplicate, or fixture."
    if normalized_status in {"REJECTED", "ARCHIVED"}:
        return "Keep collapsed in historical Research UI."
    return "Keep visible in Research workflow."


def _summary_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {key: 0 for key in sorted(CLASSIFICATIONS)}
    for row in rows:
        counts[str(row.get("classification") or "UNKNOWN")] = counts.get(str(row.get("classification") or "UNKNOWN"), 0) + 1
    counts["total"] = len(rows)
    counts["orphaned"] = sum(1 for row in rows if row.get("orphaned") is True)
    counts["duplicates"] = sum(1 for row in rows if row.get("duplicate_of"))
    counts["appears_in_research_ui"] = sum(1 for row in rows if row.get("appears_in_research_ui") is True)
    return counts


def append_manual_hypothesis_classification_v1(
    *,
    truth_root: Path,
    day_utc: str,
    hypothesis_id: str,
    classification: str,
    operator: str,
    reason: str,
    source_path: str = "",
    duplicate_of: str = "",
) -> Path:
    normalized = str(classification or "").upper()
    allowed = {"REAL_OPERATOR_HYPOTHESIS", "ARCHIVED", "REJECTED", "DUPLICATE", "TEST_FIXTURE"}
    if normalized not in allowed:
        raise ValueError(f"classification must be one of {sorted(allowed)}")
    if not hypothesis_id or not operator or not reason:
        raise ValueError("hypothesis_id, operator, and reason are required")
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / "aegis_research_hypothesis_classification_overrides_v1" / day_utc
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "research_hypothesis_classification_overrides.v1.jsonl"
    event = {
        "event_type": "RESEARCH_HYPOTHESIS_CLASSIFIED",
        "event_id": hashlib.sha256(f"{hypothesis_id}|{classification}|{operator}|{reason}|{datetime.now(UTC).isoformat()}".encode("utf-8")).hexdigest()[:24],
        "timestamp_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "hypothesis_id": hypothesis_id,
        "classification": normalized,
        "operator": operator,
        "reason": reason,
        "source_path": source_path,
        "duplicate_of": duplicate_of,
        "append_only": True,
        "source_artifact_preserved": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return path


def _file_hash(path: Path | None) -> str:
    if not path:
        return ""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""
