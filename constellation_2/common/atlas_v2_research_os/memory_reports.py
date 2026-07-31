from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .failure_patterns import get_repeated_failures, list_failure_patterns
from .governance import validate_memory_allowed, validate_memory_no_authority_escalation
from .mechanism_clusters import list_mechanism_clusters
from .memory_index import list_memory_objects, memory_root, validate_memory_integrity
from .regime_context import list_regime_contexts
from .semantic_deduplication import LIKELY_DUPLICATE_THRESHOLD, POTENTIAL_DUPLICATE_THRESHOLD, find_potential_duplicates


def build_memory_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    day_value = day or date.today().isoformat()
    memories = list_memory_objects(root)
    failures = list_failure_patterns(root)
    clusters = list_mechanism_clusters(root)
    regimes = list_regime_contexts(root)
    duplicates = find_potential_duplicates(root)
    integrity_ok, integrity_failures = validate_memory_integrity(root)
    governance_failures: list[str] = []
    for row in memories:
        try:
            validate_memory_allowed(row)
            validate_memory_no_authority_escalation(row)
        except Exception as exc:  # narrow value is less useful in report audit loop
            governance_failures.append(f"{row.get('memory_id')}: {exc}")
    report = {
        "schema_id": "atlas_v2_research_os_memory_report_v1",
        "schema_version": "v0.1",
        "day": day_value,
        "memory_object_counts_by_type": dict(Counter(row.get("memory_type") for row in memories)),
        "mechanism_cluster_counts": {"total": len(clusters), "by_tag": dict(Counter(tag for row in clusters for tag in row.get("mechanism_tags", [])))},
        "failure_pattern_counts": {"total": len(failures), "by_type": dict(Counter(row.get("failure_type") for row in failures))},
        "repeated_failure_counts": len(get_repeated_failures(root, 2)),
        "retired_knowledge_counts": sum(1 for row in memories if row.get("memory_type") == "RetiredKnowledge" or row.get("lifecycle_state") == "RETIRED"),
        "reopened_knowledge_counts": sum(1 for row in memories if row.get("memory_type") == "ReopenedKnowledge" or row.get("lifecycle_state") == "REOPENED"),
        "regime_context_coverage": {"total": len(regimes), "labels": dict(Counter(label for row in regimes for label in row.get("labels", [])))},
        "potential_duplicate_counts": sum(1 for row in duplicates if row["duplicate_score"] >= POTENTIAL_DUPLICATE_THRESHOLD),
        "likely_duplicate_counts": sum(1 for row in duplicates if row["duplicate_score"] >= LIKELY_DUPLICATE_THRESHOLD),
        "duplicate_samples": duplicates[:5],
        "governance_audit_result": "PASS" if not governance_failures else "FAIL",
        "governance_failures": governance_failures,
        "memory_integrity_result": "PASS" if integrity_ok else "FAIL",
        "memory_integrity_failures": integrity_failures,
        "open_blockers": governance_failures + integrity_failures,
    }
    return report


def write_memory_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    day_value = day or date.today().isoformat()
    report = build_memory_report(root, day=day_value)
    out_root = memory_root(root)
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "research_memory_report.json"
    md_path = out_dir / "research_memory_summary.md"
    latest_json = out_root / "latest.json"
    latest_md = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary = render_memory_summary(report)
    md_path.write_text(summary, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_memory_summary(report: dict[str, Any]) -> str:
    return "\n".join([
        "# Atlas V2 Research Memory Report",
        "",
        f"Day: {report['day']}",
        f"Governance audit: {report['governance_audit_result']}",
        f"Memory integrity: {report['memory_integrity_result']}",
        f"Memory objects: {json.dumps(report['memory_object_counts_by_type'], sort_keys=True)}",
        f"Mechanism clusters: {json.dumps(report['mechanism_cluster_counts'], sort_keys=True)}",
        f"Failure patterns: {json.dumps(report['failure_pattern_counts'], sort_keys=True)}",
        f"Repeated failures: {report['repeated_failure_counts']}",
        f"Retired knowledge: {report['retired_knowledge_counts']}",
        f"Reopened knowledge: {report['reopened_knowledge_counts']}",
        f"Regime coverage: {json.dumps(report['regime_context_coverage'], sort_keys=True)}",
        f"Potential duplicates: {report['potential_duplicate_counts']}",
        f"Likely duplicates: {report['likely_duplicate_counts']}",
        f"Open blockers: {json.dumps(report['open_blockers'], sort_keys=True)}",
        "",
    ])
