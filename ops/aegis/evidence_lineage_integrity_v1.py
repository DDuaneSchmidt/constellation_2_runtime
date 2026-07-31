from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.future_target_day_audit_guard_v1 import is_future_target_day_guarded_v1, read_future_target_day_audit_guard_v1

REPORT_FAMILY = "aegis_evidence_lineage_integrity_v1"
REPORT_FILENAME = "evidence_lineage_integrity.v1.json"
MARK_FAMILY = "aegis_mark_coverage_report_v1"
MARK_FILENAME = "mark_coverage_report.v1.json"
VALIDATION_FAMILY = "aegis_validation_integrity_report_v1"
VALIDATION_FILENAME = "validation_integrity_report.v1.json"

SAFETY = {
    "read_only": True,
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "live_trading_allowed": False,
}


def integrity_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def mark_coverage_report_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / MARK_FAMILY / str(day_utc) / MARK_FILENAME


def validation_integrity_report_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / VALIDATION_FAMILY / str(day_utc) / VALIDATION_FILENAME


def build_evidence_lineage_integrity_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    ledger = _read_report(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json")
    validation_samples = _read_report(root, "aegis_research_validation_samples_v1", day, "research_validation_samples.v1.json")
    validation_results = _read_report(root, "aegis_research_validation_result_v1", day, "research_validation_result.v1.json")
    analytics = _read_report(root, "aegis_sleeve_analytics_v1", day, "sleeve_analytics.v1.json")
    perf_truth = _read_report(root, "aegis_sleeve_performance_truth_v1", day, "sleeve_performance_truth.v1.json")

    candidate_rows = _candidate_rows(root, day)
    positions = [row for row in ledger.get("positions") or [] if isinstance(row, Mapping)]
    open_positions = [row for row in ledger.get("open_positions") or [] if isinstance(row, Mapping)]
    closed_positions = [row for row in ledger.get("closed_positions") or [] if isinstance(row, Mapping)]
    samples = [row for row in validation_samples.get("samples") or [] if isinstance(row, Mapping)]
    results = [row for row in validation_results.get("results") or [] if isinstance(row, Mapping)]

    stages = {
        "candidate_to_sleeve": _coverage(
            total=len(candidate_rows),
            linked=sum(1 for row in candidate_rows if _text(row.get("sleeve_id")) and _text(row.get("raw_signal_id")) and _created_at(row)),
            root_causes=_candidate_root_causes(candidate_rows),
        ),
        "sleeve_to_paper_position": _coverage(
            total=len(positions),
            linked=sum(1 for row in positions if _position_has_sleeve(row)),
            root_causes=_position_root_causes(positions),
        ),
        "paper_position_to_certified_mark": _coverage(
            total=len(open_positions),
            linked=sum(1 for row in open_positions if _has_certified_mark(row)),
            root_causes=_mark_root_causes(open_positions),
        ),
        "certified_mark_to_outcome": _coverage(
            total=len(closed_positions),
            linked=sum(1 for row in closed_positions if _text(row.get("realized_pnl")) or _text(row.get("exit_time"))),
            root_causes=_outcome_root_causes(closed_positions),
        ),
        "outcome_to_validation_sample": _coverage(
            total=len(samples),
            linked=sum(1 for row in samples if _sample_traceable(row)),
            root_causes=_sample_root_causes(samples),
        ),
        "validation_sample_to_validation_result": _coverage(
            total=len(results),
            linked=sum(1 for row in results if not _binding_mismatch(row)),
            root_causes=_validation_result_root_causes(results),
        ),
        "validation_result_to_scorecard": _coverage(
            total=len(results),
            linked=sum(1 for row in results if _scorecard_traceable(row, analytics, perf_truth)),
            root_causes=_scorecard_root_causes(results, analytics, perf_truth),
        ),
    }
    mark_report = build_mark_coverage_report_v1(truth_root=root, day_utc=day, ledger=ledger)
    validation_report = build_validation_integrity_report_v1(truth_root=root, day_utc=day, samples_payload=validation_samples, results_payload=validation_results, ledger=ledger)
    broken_chain_count = sum(int(stage["orphaned_records"] or 0) for stage in stages.values()) + int(mark_report["unmarked_positions"] or 0) + int(validation_report["mismatched_samples"] or 0)
    unknown_position_count = sum(1 for row in positions if not _position_has_sleeve(row))
    sample_binding_errors = sum(1 for row in results if _binding_mismatch(row))
    future_guard = read_future_target_day_audit_guard_v1(truth_root=root, day_utc=day)
    future_guarded = future_guard.get("guard_status") == "TARGET_DAY_IN_FUTURE_GUARDED" and future_guard.get("is_future_target_day") is True
    coverage_panel = {
        "candidate_coverage_pct": stages["candidate_to_sleeve"]["coverage_pct"],
        "sleeve_attribution_pct": stages["sleeve_to_paper_position"]["coverage_pct"],
        "mark_coverage_pct": mark_report["coverage_pct"],
        "validation_coverage_pct": validation_report["coverage_pct"],
        "broken_chain_count": broken_chain_count,
        "unknown_position_count": unknown_position_count,
        "sample_binding_errors": sample_binding_errors,
        "future_target_day_guarded": future_guarded,
        "future_target_day_guard_status": str(future_guard.get("guard_status") or ""),
        "current_integrity_status": "GUARDED_FUTURE_TARGET_DAY" if future_guarded else _integrity_status(broken_chain_count, unknown_position_count, sample_binding_errors, mark_report["coverage_pct"], validation_report["coverage_pct"]),
    }
    payload = {
        "schema_id": "aegis_evidence_lineage_integrity",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at": _now(),
        "stages": stages,
        "evidence_coverage_panel": coverage_panel,
        "summary": {
            **coverage_panel,
            "candidate_records": len(candidate_rows),
            "paper_positions": len(positions),
            "open_paper_positions": len(open_positions),
            "closed_paper_positions": len(closed_positions),
            "validation_samples": len(samples),
            "validation_results": len(results),
        },
        "source_artifacts": _source_refs(root, day),
        "safety": dict(SAFETY),
        **SAFETY,
    }
    return payload


def build_mark_coverage_report_v1(*, truth_root: Path | str, day_utc: str, ledger: Mapping[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    ledger_payload = dict(ledger or _read_report(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"))
    open_positions = [row for row in ledger_payload.get("open_positions") or [] if isinstance(row, Mapping)]
    marked = [row for row in open_positions if _has_certified_mark(row)]
    unmarked = [row for row in open_positions if not _has_certified_mark(row)]
    return {
        "schema_id": "aegis_mark_coverage_report",
        "schema_version": "v1",
        "artifact_id": MARK_FAMILY,
        "day_utc": day,
        "generated_at": _now(),
        "total_positions": len(open_positions),
        "marked_positions": len(marked),
        "unmarked_positions": len(unmarked),
        "coverage_pct": _pct(len(marked), len(open_positions)),
        "unmarked_position_ids": [str(row.get("position_id") or "") for row in unmarked],
        "source_artifact_paths": {"paper_position_ledger": str(root / "reports" / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json")},
        "safety": dict(SAFETY),
        **SAFETY,
    }


def build_validation_integrity_report_v1(*, truth_root: Path | str, day_utc: str, samples_payload: Mapping[str, Any] | None = None, results_payload: Mapping[str, Any] | None = None, ledger: Mapping[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    samples_payload = samples_payload or _read_report(root, "aegis_research_validation_samples_v1", day, "research_validation_samples.v1.json")
    results_payload = results_payload or _read_report(root, "aegis_research_validation_result_v1", day, "research_validation_result.v1.json")
    samples = [row for row in samples_payload.get("samples") or [] if isinstance(row, Mapping)]
    results = [row for row in results_payload.get("results") or [] if isinstance(row, Mapping)]
    valid_samples = [row for row in samples if _sample_traceable(row)]
    orphaned_samples = [row for row in samples if not _sample_traceable(row)]
    mismatched_results = [row for row in results if _binding_mismatch(row)]
    total = len(samples) + len(results)
    linked = len(valid_samples) + (len(results) - len(mismatched_results))
    return {
        "schema_id": "aegis_validation_integrity_report",
        "schema_version": "v1",
        "artifact_id": VALIDATION_FAMILY,
        "day_utc": day,
        "generated_at": _now(),
        "total_samples": len(samples),
        "valid_samples": len(valid_samples),
        "orphaned_samples": len(orphaned_samples),
        "mismatched_samples": len(mismatched_results),
        "coverage_pct": _pct(linked, total),
        "sample_traceability_required_fields": ["originating_hypothesis", "candidate", "sleeve", "outcome"],
        "mismatched_result_ids": [str(row.get("result_id") or row.get("hypothesis_id") or "") for row in mismatched_results],
        "source_artifact_paths": {
            "research_validation_samples": str(root / "reports" / "aegis_research_validation_samples_v1" / day / "research_validation_samples.v1.json"),
            "research_validation_result": str(root / "reports" / "aegis_research_validation_result_v1" / day / "research_validation_result.v1.json"),
        },
        "safety": dict(SAFETY),
        **SAFETY,
    }


def write_evidence_lineage_integrity_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_evidence_lineage_integrity_v1(truth_root=root, day_utc=day_utc))
    write_json_v1(mark_coverage_report_path_v1(truth_root=root, day_utc=day_utc), build_mark_coverage_report_v1(truth_root=root, day_utc=day_utc))
    write_json_v1(validation_integrity_report_path_v1(truth_root=root, day_utc=day_utc), build_validation_integrity_report_v1(truth_root=root, day_utc=day_utc))
    return write_json_v1(integrity_path_v1(truth_root=root, day_utc=day_utc), body)


def write_lineage_audit_doc_v1(*, truth_root: Path | str, day_utc: str, output_path: Path | str) -> Path:
    payload = build_evidence_lineage_integrity_v1(truth_root=truth_root, day_utc=day_utc)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    stages = payload["stages"]
    labels = [
        ("Candidate -> Sleeve Coverage", "candidate_to_sleeve"),
        ("Sleeve -> Paper Position Coverage", "sleeve_to_paper_position"),
        ("Paper Position -> Certified Mark Coverage", "paper_position_to_certified_mark"),
        ("Certified Mark -> Outcome Coverage", "certified_mark_to_outcome"),
        ("Outcome -> Validation Sample Coverage", "outcome_to_validation_sample"),
        ("Validation Sample -> Validation Result Coverage", "validation_sample_to_validation_result"),
        ("Validation Result -> Scorecard Coverage", "validation_result_to_scorecard"),
    ]
    lines = ["# Aegis Evidence Lineage Audit", "", f"Day UTC: `{day_utc}`", "", "This is a measurement artifact. It does not authorize trading, change sleeve logic, or replace existing analytics.", ""]
    for title, key in labels:
        row = stages[key]
        lines += [f"## {title}", "", f"- total records: {row['total_records']}", f"- linked records: {row['linked_records']}", f"- orphaned records: {row['orphaned_records']}", f"- coverage percentage: {row['coverage_pct']}%", f"- root causes: {', '.join(row['root_causes']) if row['root_causes'] else 'None'}", ""]
    out.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return out


def integrity_failures_v1(payload: Mapping[str, Any]) -> list[str]:
    panel = payload.get("evidence_coverage_panel") if isinstance(payload.get("evidence_coverage_panel"), Mapping) else {}
    failures: list[str] = []
    checks = {
        "candidate_lineage_integrity": panel.get("candidate_coverage_pct"),
        "paper_position_integrity": panel.get("sleeve_attribution_pct"),
        "mark_coverage_integrity": panel.get("mark_coverage_pct"),
        "validation_binding_integrity": panel.get("validation_coverage_pct"),
        "scorecard_traceability_integrity": (payload.get("stages") or {}).get("validation_result_to_scorecard", {}).get("coverage_pct") if isinstance(payload.get("stages"), Mapping) else None,
    }
    future_guarded = panel.get("future_target_day_guarded") is True or str(panel.get("future_target_day_guard_status") or "") == "TARGET_DAY_IN_FUTURE_GUARDED"
    for name, value in checks.items():
        if future_guarded and name == "mark_coverage_integrity":
            continue
        if float(value or 0.0) < 100.0:
            failures.append(f"{name}: {value}%")
    if int(panel.get("unknown_position_count") or 0) != 0:
        failures.append(f"unknown_position_count: {panel.get('unknown_position_count')}")
    if int(panel.get("sample_binding_errors") or 0) != 0:
        failures.append(f"sample_binding_errors: {panel.get('sample_binding_errors')}")
    return failures


def _candidate_rows(root: Path, day: str) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for path in sorted((root / "reports" / "aegis_candidate_contracts_v1").glob("*/candidate_contracts.v1.json")):
        if path.parent.name > day:
            continue
        payload = read_json_v1(path)
        for row in _walk_dicts(payload):
            candidate_id = _text(row.get("candidate_id") or row.get("candidate_contract_id"))
            if candidate_id:
                rows[candidate_id] = {**rows.get(candidate_id, {}), **row, "lineage_source_path": str(path)}
    return list(rows.values())


def _read_report(root: Path, family: str, day: str, filename: str) -> dict[str, Any]:
    return read_json_v1(root / "reports" / family / day / filename)


def _source_refs(root: Path, day: str) -> dict[str, str]:
    specs = {
        "paper_position_ledger": ("aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"),
        "sleeve_analytics": ("aegis_sleeve_analytics_v1", "sleeve_analytics.v1.json"),
        "sleeve_performance_truth": ("aegis_sleeve_performance_truth_v1", "sleeve_performance_truth.v1.json"),
        "research_validation_samples": ("aegis_research_validation_samples_v1", "research_validation_samples.v1.json"),
        "research_validation_result": ("aegis_research_validation_result_v1", "research_validation_result.v1.json"),
    }
    return {name: str(root / "reports" / family / day / filename) for name, (family, filename) in specs.items()}


def _coverage(*, total: int, linked: int, root_causes: list[str]) -> dict[str, Any]:
    return {"total_records": total, "linked_records": linked, "orphaned_records": max(total - linked, 0), "coverage_pct": _pct(linked, total), "root_causes": root_causes}


def _pct(linked: int, total: int) -> float:
    if total == 0:
        return 100.0
    return round((linked / total) * 100.0, 6)


def _position_has_sleeve(row: Mapping[str, Any]) -> bool:
    lineage = row.get("candidate_lineage") if isinstance(row.get("candidate_lineage"), Mapping) else {}
    return _text(row.get("sleeve_id")) not in {"", "UNKNOWN"} or _text(lineage.get("sleeve_id")) not in {"", "UNKNOWN"}


def _has_certified_mark(row: Mapping[str, Any]) -> bool:
    return bool(_text(row.get("current_certified_mark")) and _text(row.get("mark_timestamp_utc")) and _text(row.get("mark_source_path")) and _text(row.get("mark_certification_status")) == "CERTIFIED")


def _sample_traceable(row: Mapping[str, Any]) -> bool:
    if not row:
        return False
    return bool(_text(row.get("hypothesis_id")) and (_text(row.get("candidate_id")) or _text(row.get("source_test_result_path"))) and (_text(row.get("sleeve_id")) or _text(row.get("hypothesis_id"))) and (_text(row.get("outcome_id")) or _text(row.get("forward_return_1d"))))


def _binding_mismatch(row: Mapping[str, Any]) -> bool:
    failures = row.get("artifact_binding_failures") if isinstance(row.get("artifact_binding_failures"), list) else []
    binding = row.get("hypothesis_binding") if isinstance(row.get("hypothesis_binding"), Mapping) else {}
    return "HYPOTHESIS_SAMPLE_BINDING_MISMATCH" in failures or _text(binding.get("binding_status")) == "MISMATCH"


def _scorecard_traceable(row: Mapping[str, Any], analytics: Mapping[str, Any], perf_truth: Mapping[str, Any]) -> bool:
    if not row:
        return False
    return bool(analytics.get("summary") or perf_truth.get("sleeves")) and not _binding_mismatch(row)


def _candidate_root_causes(rows: list[Mapping[str, Any]]) -> list[str]:
    causes = []
    if any(not _text(row.get("sleeve_id")) for row in rows):
        causes.append("missing_candidate_sleeve_id")
    if any(not _text(row.get("raw_signal_id")) for row in rows):
        causes.append("missing_originating_signal")
    if any(not _created_at(row) for row in rows):
        causes.append("missing_creation_timestamp")
    return causes


def _position_root_causes(rows: list[Mapping[str, Any]]) -> list[str]:
    return ["unknown_sleeve_attribution"] if any(not _position_has_sleeve(row) for row in rows) else []


def _mark_root_causes(rows: list[Mapping[str, Any]]) -> list[str]:
    causes = []
    if any(not _text(row.get("current_certified_mark")) for row in rows):
        causes.append("missing_current_certified_mark")
    if any(not _text(row.get("mark_timestamp_utc")) for row in rows):
        causes.append("missing_mark_timestamp")
    if any(not _text(row.get("mark_source_path")) for row in rows):
        causes.append("missing_mark_source")
    return causes


def _outcome_root_causes(rows: list[Mapping[str, Any]]) -> list[str]:
    return ["no_closed_positions"] if not rows else (["missing_realized_outcome"] if any(not _text(row.get("realized_pnl")) and not _text(row.get("exit_time")) for row in rows) else [])


def _sample_root_causes(rows: list[Mapping[str, Any]]) -> list[str]:
    return ["no_validation_samples"] if not rows else (["orphaned_validation_samples"] if any(not _sample_traceable(row) for row in rows) else [])


def _validation_result_root_causes(rows: list[Mapping[str, Any]]) -> list[str]:
    return ["hypothesis_sample_binding_mismatch"] if any(_binding_mismatch(row) for row in rows) else []


def _scorecard_root_causes(rows: list[Mapping[str, Any]], analytics: Mapping[str, Any], perf_truth: Mapping[str, Any]) -> list[str]:
    causes = []
    if rows and not analytics.get("summary") and not perf_truth.get("sleeves"):
        causes.append("scorecard_artifact_missing")
    if any(_binding_mismatch(row) for row in rows):
        causes.append("validation_result_not_traceable")
    return causes


def _created_at(row: Mapping[str, Any]) -> str:
    return _text(
        row.get("candidate_snapshot_timestamp_utc")
        or row.get("candidate_snapshot_timestamp")
        or row.get("created_at_utc")
        or row.get("created_at")
        or row.get("entry_reference_price_timestamp_utc")
        or row.get("generated_at_utc")
        or row.get("generated_at")
    )


def _integrity_status(broken_chain_count: int, unknown_position_count: int, sample_binding_errors: int, mark_pct: float, validation_pct: float) -> str:
    if broken_chain_count == 0 and unknown_position_count == 0 and sample_binding_errors == 0 and mark_pct >= 100.0 and validation_pct >= 100.0:
        return "GREEN"
    if mark_pct < 100.0 or validation_pct < 100.0 or unknown_position_count:
        return "RED"
    return "YELLOW"


def _walk_dicts(value: Any):
    if isinstance(value, Mapping):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
