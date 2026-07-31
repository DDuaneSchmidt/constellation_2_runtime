from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.oil_shock_risk_policy_schema_repair_v1 import (
    build_oil_shock_risk_policy_schema_repair_v1,
    oil_shock_risk_policy_schema_repair_path_v1,
    write_oil_shock_risk_policy_schema_repair_v1,
)

DAY = "2026-06-02"
ROOT = Path(__file__).resolve().parents[3]
SOURCE = "C2_RISK_POLICY_REGISTRY_V1:C2_OIL_SHOCK_REVERSAL_V1.stop_loss_bps_default"


def test_oil_shock_risk_policy_registry_schema_validates() -> None:
    registry = json.loads((ROOT / "governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json").read_text(encoding="utf-8"))
    validate_against_repo_schema_v1(registry, ROOT, "governance/04_DATA/SCHEMAS/C2/RISK/c2_risk_policy_registry.v1.schema.json")
    policy = registry["policies"]["C2_OIL_SHOCK_REVERSAL_V1"]
    assert policy["stop_loss_bps_default"] == 1000
    assert policy["hard_stop"]["long_formula"] == "entry_reference_price * (1 - stop_loss_bps / 10000)"
    assert policy["broker_execution_allowed"] is False
    assert policy["paper_submit_allowed"] is False


def test_repair_artifact_reports_compatibility_without_behavior_change(tmp_path: Path) -> None:
    write_json_v1(
        tmp_path / "reports" / "aegis_generated_hypothesis_paper_construction_repair_v1" / DAY / "generated_hypothesis_paper_construction_repair.v1.json",
        {"summary": {"stop_price_source": SOURCE, "stop_price_status": "DERIVED", "stop_price_present": True}},
    )
    payload = build_oil_shock_risk_policy_schema_repair_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY)
    assert payload["schema_failures_after_repair"] == []
    assert payload["missing_fields_after_repair"] == []
    assert payload["stop_loss_bps_default_present"] is True
    assert payload["stop_price_source_still_valid"] is True
    assert payload["stop_price_source_day"] == DAY
    assert payload["strategy_behavior_changed"] is False
    assert payload["paper_construction_compatibility_status"] == "COMPATIBLE"
    assert payload["package_017_compatibility_status"] == "COMPATIBLE"
    assert payload["remaining_blocker"] == "NONE"
    path = write_oil_shock_risk_policy_schema_repair_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY, payload=payload)
    assert path == oil_shock_risk_policy_schema_repair_path_v1(truth_root=tmp_path, day_utc=DAY)


def test_future_day_reuses_latest_prior_package_017_stop_source(tmp_path: Path) -> None:
    write_json_v1(
        tmp_path / "reports" / "aegis_generated_hypothesis_paper_construction_repair_v1" / DAY / "generated_hypothesis_paper_construction_repair.v1.json",
        {"summary": {"stop_price_source": SOURCE, "stop_price_status": "DERIVED", "stop_price_present": True}},
    )
    payload = build_oil_shock_risk_policy_schema_repair_v1(truth_root=tmp_path, repo_root=ROOT, day_utc="2026-06-03")
    assert payload["stop_price_source_still_valid"] is True
    assert payload["stop_price_source"] == SOURCE
    assert payload["stop_price_source_day"] == DAY
    assert payload["package_017_compatibility_status"] == "COMPATIBLE"
    assert payload["strategy_behavior_changed"] is False
