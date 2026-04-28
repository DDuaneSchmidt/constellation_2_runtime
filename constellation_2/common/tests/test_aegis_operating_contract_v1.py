from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_operating_contract_v1 import build_aegis_operating_contract_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


DAY = "2026-04-27"
SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_operating_contract.v1.schema.json"


def test_dry_run_manual_contract_is_valid() -> None:
    payload = build_aegis_operating_contract_v1(day_utc=DAY, mode="DRY_RUN", run_style="MANUAL")
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA)
    assert payload["mode"] == "DRY_RUN"
    assert payload["run_style"] == "MANUAL"


def test_paper_transmit_auto_contract_requires_submit_service() -> None:
    payload = build_aegis_operating_contract_v1(day_utc=DAY, mode="PAPER_TRANSMIT", run_style="AUTO")
    assert payload["required_services_by_mode_run_style"]["auto_runner"] is True
    assert payload["required_services_by_mode_run_style"]["submit_creator"] is True
    assert payload["run_style_policy"]["default_execution_mode"] == "DRY_RUN"


def test_invalid_mode_run_style_fails() -> None:
    with pytest.raises(ValueError):
        build_aegis_operating_contract_v1(day_utc=DAY, mode="SIM", run_style="MANUAL")
    with pytest.raises(ValueError):
        build_aegis_operating_contract_v1(day_utc=DAY, mode="DRY_RUN", run_style="BACKGROUND")


def test_no_silent_day_outcomes_are_mutually_exclusive() -> None:
    payload = build_aegis_operating_contract_v1(day_utc=DAY)
    rule = payload["no_silent_day_rule"]
    assert rule["mutually_exclusive"] is True
    assert rule["exactly_one_required"] is True
    assert len(rule["allowed_outcomes"]) == len(set(rule["allowed_outcomes"]))
