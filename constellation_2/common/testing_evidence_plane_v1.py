from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.integration_test_result_v1 import build_integration_test_result_v1
from constellation_2.common.paper_workflow_result_v1 import build_paper_workflow_result_v1
from constellation_2.common.replay_test_result_v1 import build_replay_test_result_v1
from constellation_2.common.runtime_truth_integrity_result_v1 import (
    build_runtime_truth_integrity_result_v1,
)
from constellation_2.common.scenario_test_result_v1 import build_scenario_test_result_v1
from constellation_2.common.workflow_restart_result_v1 import build_workflow_restart_result_v1


SOURCE_REPO_ROOT = Path(__file__).resolve().parents[2]

SCENARIO_CATALOG_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/scenario_catalog.v1.schema.json"
SCENARIO_TEST_RESULT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/scenario_test_result.v1.schema.json"
REPLAY_TEST_RESULT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/replay_test_result.v1.schema.json"
INTEGRATION_TEST_RESULT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/integration_test_result.v1.schema.json"
RUNTIME_TRUTH_INTEGRITY_RESULT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/runtime_truth_integrity_result.v1.schema.json"
WORKFLOW_RESTART_RESULT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/workflow_restart_result.v1.schema.json"
PAPER_WORKFLOW_RESULT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_workflow_result.v1.schema.json"
SUBSYSTEM_READINESS_REPORT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/subsystem_readiness_report.v1.schema.json"
PAPER_OPEN_READINESS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_open_readiness.v1.schema.json"

REQUIRED_SUBSYSTEMS = (
    "governance",
    "lifecycle",
    "runtime_truth",
    "bond",
    "advisory",
    "signal",
    "legacy_holdings",
    "operator_session_replay",
)

FAMILY_TO_FILENAME = {
    "scenario_catalog_v1": "scenario_catalog.v1.json",
    "scenario_test_result_v1": "scenario_test_result.v1.json",
    "replay_test_result_v1": "replay_test_result.v1.json",
    "integration_test_result_v1": "integration_test_result.v1.json",
    "runtime_truth_integrity_result_v1": "runtime_truth_integrity_result.v1.json",
    "workflow_restart_result_v1": "workflow_restart_result.v1.json",
    "paper_workflow_result_v1": "paper_workflow_result.v1.json",
    "subsystem_readiness_report_v1": "subsystem_readiness_report.v1.json",
    "paper_open_readiness_v1": "paper_open_readiness.v1.json",
}


def _normalize_string_list(values: Iterable[Any]) -> list[str]:
    return [str(value).strip() for value in values if str(value).strip()]


def _normalize_suite_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        suite_id = str(row.get("suite_id") or "").strip()
        subsystem_name = str(row.get("subsystem_name") or "").strip()
        status = str(row.get("status") or "").strip()
        if not suite_id or not subsystem_name or not status:
            raise ValueError(f"TESTING_EVIDENCE_SUITE_ROW_INVALID:row={row!r}")
        pass_count = int(row.get("pass_count") or 0)
        fail_count = int(row.get("fail_count") or 0)
        skip_count = int(row.get("skip_count") or 0)
        if min(pass_count, fail_count, skip_count) < 0:
            raise ValueError(f"TESTING_EVIDENCE_SUITE_COUNT_NEGATIVE:row={row!r}")
        normalized.append(
            {
                "suite_id": suite_id,
                "subsystem_name": subsystem_name,
                "scenario_ids": _normalize_string_list(row.get("scenario_ids") or []),
                "test_refs": _normalize_string_list(row.get("test_refs") or []),
                "status": status,
                "pass_count": pass_count,
                "fail_count": fail_count,
                "skip_count": skip_count,
                "failure_refs": _normalize_string_list(row.get("failure_refs") or []),
                "semantic_gaps": _normalize_string_list(row.get("semantic_gaps") or []),
            }
        )
    if not normalized:
        raise ValueError("TESTING_EVIDENCE_SUITE_ROWS_REQUIRED")
    return normalized


RESULT_ENTRY_BUILDERS: dict[str, Any] = {
    "scenario_test_result": build_scenario_test_result_v1,
    "replay_test_result": build_replay_test_result_v1,
    "integration_test_result": build_integration_test_result_v1,
    "runtime_truth_integrity_result": build_runtime_truth_integrity_result_v1,
    "workflow_restart_result": build_workflow_restart_result_v1,
    "paper_workflow_result": build_paper_workflow_result_v1,
}


def _severity_for_suite_row(row: Mapping[str, Any]) -> str:
    if int(row["fail_count"]) > 0:
        return "high"
    if row["semantic_gaps"]:
        return "review"
    return "informational"


def _notes_for_suite_row(row: Mapping[str, Any]) -> list[str]:
    notes = [
        f"suite_id:{row['suite_id']}",
        f"pass_count:{row['pass_count']}",
        f"fail_count:{row['fail_count']}",
        f"skip_count:{row['skip_count']}",
    ]
    notes.extend(f"scenario_id:{scenario_id}" for scenario_id in row["scenario_ids"])
    notes.extend(f"failure_ref:{failure_ref}" for failure_ref in row["failure_refs"])
    notes.extend(f"semantic_gap:{semantic_gap}" for semantic_gap in row["semantic_gaps"])
    return notes


def _divergence_summary_for_suite_row(row: Mapping[str, Any]) -> str:
    if row["failure_refs"]:
        return "Failure refs detected: " + ", ".join(row["failure_refs"])
    if row["semantic_gaps"]:
        return "Semantic gaps detected: " + ", ".join(row["semantic_gaps"])
    return "No divergence."


def build_scenario_catalog_v1(
    *,
    catalog_id: str,
    scenarios: Iterable[Mapping[str, Any]],
    created_at: str,
) -> dict[str, Any]:
    catalog_key = str(catalog_id or "").strip()
    created = str(created_at or "").strip()
    if not catalog_key or not created:
        raise ValueError("SCENARIO_CATALOG_REQUIRED_FIELDS_MISSING")
    scenario_rows: list[dict[str, Any]] = []
    for row in scenarios:
        scenario_id = str(row.get("scenario_id") or "").strip()
        domain_scope = str(row.get("domain_scope") or "").strip()
        scenario_type = str(row.get("scenario_type") or "").strip()
        purpose = str(row.get("purpose") or "").strip()
        status = str(row.get("status") or "").strip()
        if not all([scenario_id, domain_scope, scenario_type, purpose, status]):
            raise ValueError(f"SCENARIO_CATALOG_ROW_INVALID:row={row!r}")
        scenario_rows.append(
            {
                "scenario_id": scenario_id,
                "domain_scope": domain_scope,
                "scenario_type": scenario_type,
                "purpose": purpose,
                "input_artifact_refs": _normalize_string_list(row.get("input_artifact_refs") or []),
                "policy_refs": _normalize_string_list(row.get("policy_refs") or []),
                "expected_outputs": dict(row.get("expected_outputs") or {}),
                "expected_action_classes": _normalize_string_list(row.get("expected_action_classes") or []),
                "expected_precedence_notes": _normalize_string_list(row.get("expected_precedence_notes") or []),
                "status": status,
            }
        )
    if not scenario_rows:
        raise ValueError("SCENARIO_CATALOG_SCENARIOS_REQUIRED")
    payload = {
        "schema_id": "scenario_catalog",
        "schema_version": "v1",
        "catalog_id": catalog_key,
        "created_at": created,
        "scenarios": scenario_rows,
    }
    validate_against_repo_schema_v1(payload, SOURCE_REPO_ROOT, SCENARIO_CATALOG_SCHEMA)
    return payload


def build_suite_result_artifact_v1(
    *,
    schema_id: str,
    schema_relpath: str,
    result_id: str,
    suite_results: Iterable[Mapping[str, Any]],
    recorded_at: str,
) -> dict[str, Any]:
    result_key = str(result_id or "").strip()
    recorded = str(recorded_at or "").strip()
    if not result_key or not recorded:
        raise ValueError("TESTING_EVIDENCE_RESULT_REQUIRED_FIELDS_MISSING")
    normalized_rows = _normalize_suite_rows(suite_results)
    entry_builder = RESULT_ENTRY_BUILDERS.get(schema_id)
    if entry_builder is None:
        raise ValueError(f"TESTING_EVIDENCE_RESULT_SCHEMA_UNSUPPORTED:schema_id={schema_id}")
    results = []
    for row in normalized_rows:
        entry = entry_builder(
            result_id=f"{result_key}:{row['suite_id']}",
            source_test_id=row["suite_id"],
            subsystem_name=row["subsystem_name"],
            status=row["status"],
            executed_at=recorded,
            input_artifact_refs=row["test_refs"],
            observed_outputs_summary=(
                "Observed pytest suite counts: "
                f"pass={row['pass_count']}, fail={row['fail_count']}, skip={row['skip_count']}, "
                f"collected={len(row['scenario_ids'])}."
            ),
            expected_outputs_summary=(
                "Expected the collected suite to complete without failures or unmodeled semantic gaps."
            ),
            divergence_summary=_divergence_summary_for_suite_row(row),
            blocking=bool(row["failure_refs"]),
            severity=_severity_for_suite_row(row),
            notes=_notes_for_suite_row(row),
        ).to_dict()
        for field in ("schema_id", "schema_version", "result_id"):
            entry.pop(field, None)
        results.append(entry)
    payload = {
        "schema_id": schema_id,
        "schema_version": "v1",
        "result_id": result_key,
        "results": results,
        "recorded_at": recorded,
    }
    validate_against_repo_schema_v1(payload, SOURCE_REPO_ROOT, schema_relpath)
    return payload


def build_subsystem_readiness_report_v1(
    *,
    readiness_id: str,
    subsystem_rows: Iterable[Mapping[str, Any]],
    recorded_at: str,
) -> dict[str, Any]:
    readiness_key = str(readiness_id or "").strip()
    recorded = str(recorded_at or "").strip()
    if not readiness_key or not recorded:
        raise ValueError("SUBSYSTEM_READINESS_REQUIRED_FIELDS_MISSING")
    normalized_rows: list[dict[str, Any]] = []
    seen_subsystems: set[str] = set()
    for row in subsystem_rows:
        subsystem_name = str(row.get("subsystem_name") or "").strip()
        status = str(row.get("status") or "").strip()
        blocking_issues = _normalize_string_list(row.get("blocking_issues") or [])
        semantic_gaps = _normalize_string_list(row.get("semantic_gaps") or [])
        required_test_refs = _normalize_string_list(row.get("required_test_refs") or [])
        approved_for_next_gate = bool(row.get("approved_for_next_gate") is True)
        if not subsystem_name or not status:
            raise ValueError(f"SUBSYSTEM_READINESS_ROW_INVALID:row={row!r}")
        if approved_for_next_gate and (blocking_issues or semantic_gaps):
            raise ValueError(
                f"SUBSYSTEM_READINESS_APPROVAL_CONTRADICTION:subsystem_name={subsystem_name}"
            )
        normalized_rows.append(
            {
                "subsystem_name": subsystem_name,
                "status": status,
                "blocking_issues": blocking_issues,
                "semantic_gaps": semantic_gaps,
                "required_test_refs": required_test_refs,
                "approved_for_next_gate": approved_for_next_gate,
            }
        )
        seen_subsystems.add(subsystem_name)
    missing_subsystems = sorted(set(REQUIRED_SUBSYSTEMS) - seen_subsystems)
    if missing_subsystems:
        raise ValueError(
            f"SUBSYSTEM_READINESS_REQUIRED_SUBSYSTEMS_MISSING:subsystems={','.join(missing_subsystems)}"
        )
    aggregate_blockers = sorted(
        {
            issue
            for row in normalized_rows
            for issue in row["blocking_issues"]
        }
    )
    aggregate_gaps = sorted(
        {
            gap
            for row in normalized_rows
            for gap in row["semantic_gaps"]
        }
    )
    aggregate_test_refs = sorted(
        {
            test_ref
            for row in normalized_rows
            for test_ref in row["required_test_refs"]
        }
    )
    approved = all(bool(row["approved_for_next_gate"]) for row in normalized_rows)
    status = "READY" if approved and not aggregate_blockers and not aggregate_gaps else "BLOCKED"
    payload = {
        "schema_id": "subsystem_readiness_report",
        "schema_version": "v1",
        "readiness_id": readiness_key,
        "subsystem_name": "MULTI_SUBSYSTEM",
        "status": status,
        "blocking_issues": aggregate_blockers,
        "semantic_gaps": aggregate_gaps,
        "required_test_refs": aggregate_test_refs,
        "approved_for_next_gate": approved,
        "recorded_at": recorded,
        "subsystem_rows": normalized_rows,
    }
    validate_against_repo_schema_v1(payload, SOURCE_REPO_ROOT, SUBSYSTEM_READINESS_REPORT_SCHEMA)
    return payload


def build_paper_open_readiness_v1(
    *,
    readiness_id: str,
    subsystem_readiness_refs: Iterable[str],
    unresolved_blockers: Iterable[str],
    unresolved_semantic_gaps: Iterable[str],
    paper_open_allowed: bool,
    rationale: str,
    recorded_at: str,
    environment_name: str = "PAPER",
    authority_scope: str | None = None,
    paper_session_ledger_ref: str | None = None,
    ledger_authority_status: str | None = None,
) -> dict[str, Any]:
    readiness_key = str(readiness_id or "").strip()
    env_name = str(environment_name or "").strip()
    rationale_text = str(rationale or "").strip()
    recorded = str(recorded_at or "").strip()
    readiness_refs = _normalize_string_list(subsystem_readiness_refs)
    blockers = _normalize_string_list(unresolved_blockers)
    gaps = _normalize_string_list(unresolved_semantic_gaps)
    if not readiness_key or not env_name or not rationale_text or not recorded or not readiness_refs:
        raise ValueError("PAPER_OPEN_READINESS_REQUIRED_FIELDS_MISSING")
    if bool(paper_open_allowed) and (blockers or gaps):
        raise ValueError("PAPER_OPEN_ALLOWED_REQUIRES_NO_BLOCKERS_OR_GAPS")
    overall_status = "READY" if bool(paper_open_allowed) else "BLOCKED"
    payload = {
        "schema_id": "paper_open_readiness",
        "schema_version": "v1",
        "readiness_id": readiness_key,
        "environment_name": env_name,
        "subsystem_readiness_refs": readiness_refs,
        "unresolved_blockers": blockers,
        "unresolved_semantic_gaps": gaps,
        "overall_status": overall_status,
        "paper_open_allowed": bool(paper_open_allowed),
        "rationale": rationale_text,
        "recorded_at": recorded,
    }
    if authority_scope is not None:
        payload["authority_scope"] = str(authority_scope)
    if paper_session_ledger_ref is not None:
        payload["paper_session_ledger_ref"] = str(paper_session_ledger_ref)
    if ledger_authority_status is not None:
        payload["ledger_authority_status"] = str(ledger_authority_status)
    validate_against_repo_schema_v1(payload, SOURCE_REPO_ROOT, PAPER_OPEN_READINESS_SCHEMA)
    return payload


def canonical_testing_evidence_report_path(*, truth_root: Path, artifact_family: str, day_utc: str) -> Path:
    family = str(artifact_family or "").strip()
    filename = FAMILY_TO_FILENAME.get(family)
    if not filename:
        raise ValueError(f"TESTING_EVIDENCE_FAMILY_UNMAPPED:artifact_family={artifact_family!r}")
    day = str(day_utc or "").strip()
    if len(day) != 10:
        raise ValueError(f"TESTING_EVIDENCE_DAY_INVALID:day_utc={day_utc!r}")
    return (
        Path(truth_root).resolve()
        / "reports"
        / family
        / day
        / filename
    ).resolve()


def write_testing_evidence_report_v1(
    *,
    truth_root: Path,
    artifact_family: str,
    day_utc: str,
    payload: Mapping[str, Any],
    schema_relpath: str,
) -> Path:
    validate_against_repo_schema_v1(dict(payload), SOURCE_REPO_ROOT, schema_relpath)
    path = canonical_testing_evidence_report_path(
        truth_root=truth_root,
        artifact_family=artifact_family,
        day_utc=day_utc,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(raw)
    os.replace(str(tmp), str(path))
    return path


def read_testing_evidence_report_v1(
    *,
    truth_root: Path,
    artifact_family: str,
    day_utc: str,
    schema_relpath: str,
) -> dict[str, Any]:
    path = canonical_testing_evidence_report_path(
        truth_root=truth_root,
        artifact_family=artifact_family,
        day_utc=day_utc,
    )
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"TESTING_EVIDENCE_REPORT_NOT_OBJECT:path={path}")
    validate_against_repo_schema_v1(payload, SOURCE_REPO_ROOT, schema_relpath)
    return payload


def sha256_for_testing_evidence_report(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
