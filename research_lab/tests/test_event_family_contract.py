from pathlib import Path

from research_lab.contracts.schemas import validate_contract
from research_lab.event_intake.event_family import build_event_family
from research_lab.event_intake.event_hypothesis_templates import event_family_spec


def test_event_family_schema_validates() -> None:
    family = build_event_family(event_family_spec("oil_shock"))
    validate_contract("event_family", family)
    assert family["event_family_id"] == "oil_shock"
    assert family["content_hash"] == build_event_family(event_family_spec("oil_shock"))["content_hash"]
