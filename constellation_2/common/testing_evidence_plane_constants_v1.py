from __future__ import annotations

from pathlib import Path
from typing import Any

READINESS_STATES_V1 = (
    "READY",
    "NOT_READY",
    "BLOCKED",
    "UNKNOWN",
)

TESTING_EVIDENCE_MINIMUM_SUBSYSTEMS_V1 = (
    "governance",
    "lifecycle",
    "runtime_truth",
    "bond",
    "advisory",
    "signal",
    "legacy_holdings",
    "operator_session_replay",
)

REPO_ROOT = Path(__file__).resolve().parents[2]

SCENARIO_CATALOG_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/scenario_catalog.v1.schema.json"
)
SCENARIO_TEST_RESULT_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/scenario_test_result.v1.schema.json"
)
REPLAY_TEST_RESULT_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/replay_test_result.v1.schema.json"
)
INTEGRATION_TEST_RESULT_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/integration_test_result.v1.schema.json"
)
RUNTIME_TRUTH_INTEGRITY_RESULT_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/runtime_truth_integrity_result.v1.schema.json"
)
WORKFLOW_RESTART_RESULT_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/workflow_restart_result.v1.schema.json"
)
PAPER_WORKFLOW_RESULT_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_workflow_result.v1.schema.json"
)
SUBSYSTEM_READINESS_REPORT_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/subsystem_readiness_report.v1.schema.json"
)
PAPER_OPEN_READINESS_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_open_readiness.v1.schema.json"
)


def validate_test_result_identity_v1(obj: dict[str, Any]) -> None:
    if not str(obj.get("scenario_id") or "").strip() and not str(obj.get("source_test_id") or "").strip():
        raise ValueError("SCENARIO_ID_OR_SOURCE_TEST_ID_REQUIRED")


def validate_test_result_entry_v1(obj: dict[str, Any], *, result_type: str) -> None:
    validate_test_result_identity_v1(obj)
    expected_keys = {
        "schema_id",
        "schema_version",
        "result_id",
        "subsystem_name",
        "result_type",
        "status",
        "executed_at",
        "input_artifact_refs",
        "observed_outputs_summary",
        "expected_outputs_summary",
        "divergence_summary",
        "blocking",
        "severity",
        "notes",
        "scenario_id",
        "source_test_id",
    }
    unexpected = sorted(set(obj) - expected_keys)
    if unexpected:
        raise ValueError(f"TEST_RESULT_ENTRY_UNEXPECTED_FIELDS:{','.join(unexpected)}")
    required_fields = (
        "schema_id",
        "schema_version",
        "result_id",
        "subsystem_name",
        "result_type",
        "status",
        "executed_at",
        "input_artifact_refs",
        "observed_outputs_summary",
        "expected_outputs_summary",
        "divergence_summary",
        "blocking",
        "severity",
        "notes",
    )
    missing = [field for field in required_fields if field not in obj]
    if missing:
        raise ValueError(f"TEST_RESULT_ENTRY_REQUIRED_FIELDS_MISSING:{','.join(missing)}")
    if str(obj["schema_id"]) != str(result_type):
        raise ValueError("TEST_RESULT_ENTRY_SCHEMA_ID_MISMATCH")
    if str(obj["result_type"]) != str(result_type):
        raise ValueError("TEST_RESULT_ENTRY_RESULT_TYPE_MISMATCH")
    string_fields = (
        "schema_version",
        "result_id",
        "subsystem_name",
        "status",
        "executed_at",
        "observed_outputs_summary",
        "expected_outputs_summary",
        "divergence_summary",
        "severity",
    )
    for field in string_fields:
        if not str(obj[field]).strip():
            raise ValueError(f"TEST_RESULT_ENTRY_STRING_FIELD_EMPTY:{field}")
    if not isinstance(obj["blocking"], bool):
        raise ValueError("TEST_RESULT_ENTRY_BLOCKING_NOT_BOOL")
    if not isinstance(obj["input_artifact_refs"], list) or not all(
        isinstance(item, str) and item.strip() for item in obj["input_artifact_refs"]
    ):
        raise ValueError("TEST_RESULT_ENTRY_INPUT_ARTIFACT_REFS_INVALID")
    if not isinstance(obj["notes"], list) or not all(
        isinstance(item, str) and item.strip() for item in obj["notes"]
    ):
        raise ValueError("TEST_RESULT_ENTRY_NOTES_INVALID")
