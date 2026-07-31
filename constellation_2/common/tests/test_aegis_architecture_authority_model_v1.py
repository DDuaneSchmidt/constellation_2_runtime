from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.architecture_authority_model_v1 import (  # noqa: E402
    RUNTIME_TRUTH_LITE_DEPENDENCIES,
    build_architecture_authority_model_v1,
    validate_architecture_authority_model_v1,
    write_architecture_authority_model_v1,
)


def test_authority_model_classifies_strategic_and_lite_layers(tmp_path: Path) -> None:
    payload = build_architecture_authority_model_v1(truth_root=tmp_path, day_utc="2026-05-30")

    assert payload["strategic_system_of_record"]["classification"] == "STRATEGIC_SYSTEM_OF_RECORD"
    assert payload["strategic_system_of_record"]["name"] == "Paper Trading + Hypothesis Validation Architecture"
    assert payload["legacy_compatibility_layers"][0]["classification"] == "LEGACY_COMPATIBILITY_LAYER"
    assert payload["legacy_compatibility_layers"][0]["name"] == "Aegis Lite"
    assert validate_architecture_authority_model_v1(payload)["ok"] is True


def test_every_runtime_truth_lite_dependency_has_migration_state_and_replacement(tmp_path: Path) -> None:
    payload = build_architecture_authority_model_v1(truth_root=tmp_path, day_utc="2026-05-30")
    rows = {row["artifact_name"]: row for row in payload["artifact_authorities"]}

    for artifact in RUNTIME_TRUTH_LITE_DEPENDENCIES:
        row = rows[artifact]
        assert row["authority_status"] == "COMPATIBILITY_AUTHORITY"
        assert row["migration_state"] == "LEGACY_COMPATIBILITY"
        assert row["replacement_artifact"]
        assert row["deprecation_criteria"]
    blockers = {row["artifact_name"] for row in payload["deprecation_blockers"]}
    assert RUNTIME_TRUTH_LITE_DEPENDENCIES.issubset(blockers)


def test_strategic_domains_have_authority_owners(tmp_path: Path) -> None:
    payload = build_architecture_authority_model_v1(truth_root=tmp_path, day_utc="2026-05-30")
    rows = {row["artifact_name"]: row for row in payload["artifact_authorities"]}
    for artifact in [
        "candidate_generation",
        "candidate_generation_diagnostics",
        "paper_position_ledger",
        "outcome_registry",
        "research_validation_samples",
        "statistical_sufficiency",
        "hypothesis_state_machine",
        "research_portfolio",
        "research_capital_allocation",
        "operator_action_model",
    ]:
        assert rows[artifact]["authority_status"] == "STRATEGIC_AUTHORITY"
        assert rows[artifact]["strategic_owner"]


def test_self_check_rejects_deprecating_runtime_truth_lite_dependency(tmp_path: Path) -> None:
    payload = build_architecture_authority_model_v1(truth_root=tmp_path, day_utc="2026-05-30")
    for row in payload["artifact_authorities"]:
        if row["artifact_name"] == "manual_trade_packet":
            row["authority_status"] = "DEPRECATED"
            row["migration_state"] = "DEPRECATED"
    result = validate_architecture_authority_model_v1(payload)
    assert result["ok"] is False
    assert any("lite_deprecated_while_runtime_truth_requires:manual_trade_packet" == item for item in result["failures"])


def test_write_authority_model_artifact(tmp_path: Path) -> None:
    payload = build_architecture_authority_model_v1(truth_root=tmp_path, day_utc="2026-05-30")
    path = write_architecture_authority_model_v1(truth_root=tmp_path, payload=payload)
    written = json.loads(path.read_text(encoding="utf-8"))
    assert path.name == "architecture_authority_model.v1.json"
    assert written["schema_id"] == "aegis_architecture_authority_model_v1"
    assert written["audit_runtime_truth_impact"]["runtime_dependencies_changed"] is False
