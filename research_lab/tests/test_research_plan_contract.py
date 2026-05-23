from __future__ import annotations

import pytest

from research_lab.contracts.schemas import validate_contract
from research_lab.events.event_definitions import validate_event_definition
from research_lab.research.research_plan import build_research_plan


def test_research_plan_schema_validates_required_fields() -> None:
    plan = build_research_plan(
        hypothesis_id="hyp_fixture",
        title="Fixture plan",
        dataset_snapshot_id="ds_fixture",
        universe_snapshot_id="us_fixture",
        symbols=["SPY"],
        start="2024-01-01",
        end="2024-01-10",
        event_definition={"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": -0.01}},
        forward_return_windows=[1, 2],
        created_at="2024-01-01T00:00:00Z",
    )

    validate_contract("research_plan", plan)
    assert plan["runner_name"] == "event_study_v1"
    assert plan["research_plan_id"].startswith("rp_hyp_fixture_")


def test_unsupported_event_type_fails() -> None:
    with pytest.raises(ValueError, match="Unsupported event definition"):
        validate_event_definition({"type": "freeform_dsl", "params": {}})

