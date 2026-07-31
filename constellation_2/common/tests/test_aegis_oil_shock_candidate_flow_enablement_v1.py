from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.oil_shock_candidate_flow_enablement_v1 import build_oil_shock_candidate_flow_enablement_v1, write_oil_shock_candidate_flow_enablement_v1

DAY = "2026-06-01"
HID = "ehp_cdbd8fe683acb622"


def _producer(root: Path, *, status: str, missing_market: list[str] | None = None, outputs: list[dict] | None = None) -> None:
    output_intents = outputs or []
    write_json_v1(root / "reports/aegis_oil_shock_candidate_producer_v1" / DAY / "oil_shock_candidate_producer.v1.json", {
        "hypothesis_id": HID,
        "hypothesis_name": "Oil shock reversals across energy ETFs",
        "producer_status": status,
        "exact_blocker": status if status != "VALID_CANDIDATE_SIGNAL" else "NONE",
        "candidate_count": len(output_intents),
        "raw_signal_count": len(output_intents),
        "output_intents": output_intents,
        "required_evidence_fields": ["hypothesis_statement", "instrument_universe", "entry_logic", "exit_logic", "data_availability", "sample_frequency"],
        "available_evidence_fields": ["hypothesis_statement", "instrument_universe", "entry_logic", "exit_logic", "data_availability", "sample_frequency"],
        "missing_evidence_fields": [],
        "required_market_symbols": ["DBC", "SPY", "USO", "XLE"],
        "available_market_symbols": ["DBC", "SPY", "XLE"],
        "missing_market_symbols": missing_market or [],
        "reason_codes": [status] + (["MISSING_MARKET_DATA"] if missing_market else []),
        "david_action_required": False,
        "broker_execution_allowed": False,
        "trade_advice_allowed": False,
    })


def test_enablement_reports_producer_status_and_system_data_missing(tmp_path: Path) -> None:
    _producer(tmp_path, status="SYSTEM_DATA_PIPELINE_REQUIRED", missing_market=["USO"])
    payload = build_oil_shock_candidate_flow_enablement_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY)
    row = payload["oil_shock"]
    assert row["producer_registered"] is True
    assert row["producer_run_status"] == "RAN"
    assert row["producer_status"] == "SYSTEM_DATA_PIPELINE_REQUIRED"
    assert row["candidate_flow_status"] == "SYSTEM_DATA_PIPELINE_REQUIRED"
    assert row["blocker_code"] == "SYSTEM_DATA_PIPELINE_REQUIRED"
    assert row["blocker_owner"] == "AEGIS_SYSTEM"
    assert row["david_action_required"] is False
    assert "USO" in row["missing_market_symbols"]


def test_missing_producer_is_separate_from_missing_data(tmp_path: Path) -> None:
    payload = build_oil_shock_candidate_flow_enablement_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY)
    row = payload["oil_shock"]
    assert row["producer_registered"] is True
    assert row["producer_run_status"] == "NOT_RUN"
    assert row["candidate_flow_status"] == "PRODUCER_MISSING"
    assert row["blocker_code"] == "PRODUCER_MISSING"
    assert row["david_action_required"] is False


def test_no_market_setup_is_separate_from_system_data_missing(tmp_path: Path) -> None:
    _producer(tmp_path, status="NO_MARKET_SETUP")
    row = build_oil_shock_candidate_flow_enablement_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY)["oil_shock"]
    assert row["candidate_flow_status"] == "WAITING_FOR_MARKET_CONDITIONS"
    assert row["blocker_code"] == "NO_MARKET_SETUP"
    assert row["blocker_owner"] == "MARKET_CONDITIONS"
    assert row["david_action_required"] is False
    assert row["candidate_count"] == 0
    assert row["raw_signal_count"] == 0


def test_valid_setup_reports_raw_signal_without_fabricating_candidate_or_paper(tmp_path: Path) -> None:
    _producer(tmp_path, status="VALID_CANDIDATE_SIGNAL", outputs=[{"raw_signal_id": "oil_raw_1", "symbol": "USO"}])
    payload = build_oil_shock_candidate_flow_enablement_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY)
    row = payload["oil_shock"]
    assert row["candidate_flow_status"] == "CANDIDATE_FLOW_STARTED"
    assert row["raw_signal_count"] == 1
    assert row["candidate_count"] == 0
    assert payload["candidate_created_by_this_artifact"] is False
    assert payload["paper_observation_created_by_this_artifact"] is False
    assert payload["no_broker_execution"] is True
    assert payload["trade_advice_allowed"] is False


def test_enablement_artifact_write_path_and_safety(tmp_path: Path) -> None:
    _producer(tmp_path, status="NO_MARKET_SETUP")
    payload = build_oil_shock_candidate_flow_enablement_v1(truth_root=tmp_path, repo_root=ROOT, day_utc=DAY)
    path = write_oil_shock_candidate_flow_enablement_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert path.exists()
    assert path.name == "oil_shock_candidate_flow_enablement.v1.json"
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["safety_gates_changed"] is False
