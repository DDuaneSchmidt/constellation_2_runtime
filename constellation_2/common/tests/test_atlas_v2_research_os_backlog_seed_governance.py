from constellation_2.common.atlas_v2_research_os.backlog_seed_governance import (
    validate_backlog_seed_allowed,
    validate_backlog_seed_no_authority_escalation,
    validate_backlog_seed_no_forbidden_artifacts,
)


def _seed(**overrides):
    row = {
        "item_type": "MECHANISM_VARIATION",
        "title": "Research opening range variation",
        "description": "Research-only backlog work.",
        "state": "READY",
        "metadata": {"research_only": True},
    }
    row.update(overrides)
    return row


def test_safe_backlog_seed_allowed():
    result = validate_backlog_seed_allowed(_seed())
    assert result["status"] == "PASS"


def test_authority_escalation_rejected():
    result = validate_backlog_seed_allowed(_seed(metadata={"research_only": True, "capital_authorized": True}))
    assert result["status"] == "FAIL"
    assert "capital_authorized" in result["violations"][0]


def test_forbidden_artifact_rejected():
    result = validate_backlog_seed_allowed(_seed(artifact_types=["LiveTrade"]))
    assert result["status"] == "FAIL"
    assert "LiveTrade" in result["violations"][0]


def test_trade_instruction_text_rejected():
    result = validate_backlog_seed_allowed(_seed(description="Trade this setup tomorrow."))
    assert result["status"] == "FAIL"
