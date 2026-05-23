from __future__ import annotations

from research_lab.sleeves.sleeve_definition import build_sleeve_definition, validate_sleeve_definition, build_sleeve_version, validate_sleeve_version


def test_sleeve_definition_schema_validates() -> None:
    definition = build_sleeve_definition(
        sleeve_id="slv_fixture",
        name="Fixture Sleeve",
        hypothesis_id="hyp_fixture",
        created_at="2024-01-01T00:00:00Z",
    )

    validate_sleeve_definition(definition)
    assert definition["sleeve_type"] == "research_only"
    assert definition["status"] == "draft"


def test_sleeve_version_schema_validates() -> None:
    version = build_sleeve_version(
        sleeve_id="slv_fixture",
        version="v1",
        hypothesis_id="hyp_fixture",
        event_study_evidence_package_id="ev_event",
        backtest_evidence_package_id="ev_bt",
        candidate_batch_id="cb_fixture",
        dataset_snapshot_id="ds_fixture",
        regime_snapshot_id="rs_fixture",
        cost_model_snapshot_id="cm_fixture",
        created_at="2024-01-01T00:00:00Z",
    )

    validate_sleeve_version(version)
    assert version["sleeve_version_id"].startswith("slvv_slv_fixture_v1_")

