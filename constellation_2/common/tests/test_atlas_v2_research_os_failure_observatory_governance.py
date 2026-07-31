from __future__ import annotations

import pytest

from constellation_2.common.atlas_v2_research_os.failure_observatory import record_failure
from constellation_2.common.atlas_v2_research_os.failure_observatory_governance import (
    validate_failure_observatory_allowed,
    validate_failure_observatory_no_authority_escalation,
    validate_failure_observatory_no_forbidden_recommendations,
)


def test_forbidden_recommendations_are_rejected() -> None:
    audit = validate_failure_observatory_no_forbidden_recommendations(["Trade this."])

    assert audit["status"] == "FAIL"
    assert audit["failures"]


def test_no_authority_escalation_is_possible(tmp_path) -> None:
    audit = validate_failure_observatory_no_authority_escalation({"action": "authorize live trading"})

    assert audit["status"] == "FAIL"
    assert validate_failure_observatory_allowed({"action": "record failures"})["status"] == "PASS"
    with pytest.raises(ValueError):
        record_failure(root=tmp_path, component="GOVERNANCE", severity="ERROR", error_type="Bad", error_message="bad", metadata={"action": "authorize capital use"})
