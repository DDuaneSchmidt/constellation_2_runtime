from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.dormant_sleeve_signal_generation_diagnostics_v1 import build_dormant_sleeve_signal_generation_diagnostics_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.missing_market_data_requirement_resolver_v1 import (
    build_missing_market_data_requirement_resolver_v1,
    missing_market_data_requirement_resolver_path_v1,
    write_missing_market_data_requirement_resolver_v1,
)

DAY = "2026-06-02"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed_t02_scope(root: Path, sleeves: list[str]) -> None:
    write_json_v1(_report(root, "aegis_dormant_sleeve_signal_generation_diagnostics_v1", "dormant_sleeve_signal_generation_diagnostics.v1.json"), {
        "dormant_sleeves": [
            {
                "sleeve_id": sid,
                "sleeve_name": sid.replace("_", " "),
                "hypothesis_id": f"H_{sid}",
                "dormant_reason_code": "MISSING_MARKET_DATA",
                "missing_inputs": [],
            }
            for sid in sleeves
        ],
        "summary": {"missing_market_data_count": len(sleeves)},
    })


def _seed_common(root: Path) -> None:
    sleeves = ["ROUTING", "INGEST", "STALE", "INCOMPLETE", "NOT_CONFIG", "UNDECLARED", "UNSUPPORTED", "DAVID", "AEGIS"]
    _seed_t02_scope(root, sleeves)
    write_json_v1(_report(root, "aegis_sleeve_input_contracts_v1", "sleeve_input_contracts.v1.json"), {
        "contracts": [
            {"sleeve_id": "STALE", "required_inputs": [{"data_item_id": "market.price.STALE"}]},
            {"sleeve_id": "NOT_CONFIG", "required_inputs": [{"data_item_id": "market.price.NOCONFIG"}]},
            {"sleeve_id": "UNSUPPORTED", "required_inputs": [{"data_item_id": "market.alternative.sentiment.X"}]},
            {"sleeve_id": "DAVID", "required_inputs": [{"data_item_id": "market.price.EXTERNAL", "expected_source_type": "EXTERNAL_SOURCE_REQUIRED", "expected_source": "vendor_or_file_required"}]},
            {"sleeve_id": "AEGIS", "required_inputs": [{"data_item_id": "market.price.AEGIS"}]},
        ]
    })
    write_json_v1(_report(root, "sleeve_evaluation_kernel_v1", "sleeve_evaluation_rollup.v1.json"), {
        "outcomes": [
            {"engine_id": "ROUTING", "active_symbol_universe": ["ROUTE"], "current_status": "BLOCKED"},
            {"engine_id": "INGEST", "active_symbol_universe": ["INGEST"], "current_status": "BLOCKED"},
            {"engine_id": "INCOMPLETE", "active_symbol_universe": ["MISSBAR"], "current_status": "NO_INTENT", "stdout_summary": 'OK {"reason_codes":["MISSING_BAR_FOR_DAY: symbol=MISSBAR day_utc=2026-06-02"]}'},
        ]
    })
    write_json_v1(_report(root, "market_data_inputs_v1", "market_data_inputs.v1.json"), {
        "input_records": [
            {"data_item_id": "market.price.ROUTE", "symbol": "ROUTE", "validation_status": "VALID", "source_vendor": "LOCAL_CACHE", "source_artifact_path": "route.json"},
            {"data_item_id": "market.price.STALE", "symbol": "STALE", "validation_status": "STALE", "source_vendor": "LOCAL_CACHE", "source_artifact_path": "stale.json"},
            {"data_item_id": "market.price.MISSBAR", "symbol": "MISSBAR", "validation_status": "VALID", "source_vendor": "YAHOO_CHART", "source_artifact_path": "missbar.json"},
            {"data_item_id": "market.price.AEGIS", "symbol": "AEGIS", "validation_status": "VALID", "source_vendor": "LOCAL_CACHE", "source_artifact_path": "aegis.json"},
        ]
    })
    write_json_v1(_report(root, "market_data_readiness_v1", "market_data_readiness.v1.json"), {
        "input_statuses": [
            {"data_item_id": "market.price.ROUTE", "symbol": "ROUTE", "validation_status": "VALID"},
            {"data_item_id": "market.price.STALE", "symbol": "STALE", "validation_status": "STALE"},
            {"data_item_id": "market.price.MISSBAR", "symbol": "MISSBAR", "validation_status": "VALID"},
            {"data_item_id": "market.price.AEGIS", "symbol": "AEGIS", "validation_status": "VALID"},
        ]
    })
    write_json_v1(_report(root, "aegis_market_data_demand_v1", "market_data_demand.v1.json"), {
        "demand_rows": [
            {"symbol": "AEGIS", "required_data_item": "market.price.AEGIS", "consumer_sleeves": ["AEGIS"]},
            {"symbol": "ROUTE", "required_data_item": "market.price.ROUTE", "consumer_sleeves": []},
        ]
    })
    write_json_v1(root / "market_data_snapshot_v1" / "dataset_manifest.json", {
        "symbols": ["ROUTE", "INGEST", "STALE", "MISSBAR", "AEGIS"],
        "symbols_requested": ["ROUTE", "INGEST", "STALE", "MISSBAR", "AEGIS"],
    })


def test_missing_market_data_resolver_classifies_requested_resolution_types(tmp_path: Path) -> None:
    _seed_common(tmp_path)
    payload = build_missing_market_data_requirement_resolver_v1(truth_root=tmp_path, day_utc=DAY)
    rows = {row["sleeve_id"]: row for row in payload["sleeves"]}

    assert rows["ROUTING"]["resolution_type"] == "SOURCE_AVAILABLE_ROUTING_MISSING"
    assert rows["INGEST"]["resolution_type"] == "SOURCE_AVAILABLE_INGESTION_MISSING"
    assert rows["STALE"]["resolution_type"] == "SOURCE_AVAILABLE_BUT_STALE"
    assert rows["INCOMPLETE"]["resolution_type"] == "SOURCE_AVAILABLE_BUT_INCOMPLETE"
    assert rows["NOT_CONFIG"]["resolution_type"] == "SOURCE_NOT_CONFIGURED"
    assert rows["UNDECLARED"]["resolution_type"] == "DATA_REQUIREMENT_UNDECLARED"
    assert rows["UNSUPPORTED"]["resolution_type"] == "UNSUPPORTED_DATA_DEPENDENCY"
    assert rows["DAVID"]["resolution_type"] == "SOURCE_NOT_AVAILABLE"
    assert rows["DAVID"]["owner"] == "DAVID"
    assert rows["DAVID"]["david_action_required"] is True
    assert rows["AEGIS"]["owner"] == "NONE"


def test_missing_market_data_resolver_read_only_and_writes_expected_artifact(tmp_path: Path) -> None:
    _seed_common(tmp_path)
    before = build_missing_market_data_requirement_resolver_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_missing_market_data_requirement_resolver_v1(truth_root=tmp_path, day_utc=DAY)
    after = build_missing_market_data_requirement_resolver_v1(truth_root=tmp_path, day_utc=DAY)

    assert path == missing_market_data_requirement_resolver_path_v1(truth_root=tmp_path, day_utc=DAY)
    assert before["summary"] == after["summary"]
    assert after["read_only"] is True
    assert after["no_market_data_mutation"] is True
    assert after["no_repair_performed"] is True
    assert all(row["blocker_from_t02"] == "MISSING_MARKET_DATA" for row in after["sleeves"])


def test_t02_links_to_t03_without_changing_diagnostic_behavior(tmp_path: Path) -> None:
    write_json_v1(_report(tmp_path, "aegis_sleeve_throughput_diagnostics_v1", "sleeve_throughput_diagnostics.v1.json"), {
        "sleeves": [{"sleeve_id": "LINK", "sleeve_name": "Link", "hypothesis_id": "H_LINK", "sleeve_status": "ACTIVE", "raw_signal_count": 0, "candidate_count": 0, "throughput_status": "DORMANT", "blocker_code": "NO_SIGNALS_GENERATED", "furthest_stage_reached": "HYPOTHESIS"}]
    })
    write_json_v1(_report(tmp_path, "aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json"), {"sleeves": [{"sleeve_id": "LINK", "enabled": True, "run_status": "BLOCKED", "producer_command": "python3 producer.py", "missing_inputs": ["market.price.LINK"], "thresholds_applied": ["runtime_ready"]}]})
    write_json_v1(_report(tmp_path, "sleeve_evaluation_kernel_v1", "sleeve_evaluation_rollup.v1.json"), {"outcomes": [{"engine_id": "LINK", "current_status": "BLOCKED", "producer_command": "python3 producer.py", "exit_code": 1, "reason_codes": ["MISSING_REQUIRED_INPUTS"], "stderr_summary": "MISSING_REQUIRED_INPUTS market.price.LINK"}]})
    write_json_v1(_report(tmp_path, "aegis_sleeve_input_contracts_v1", "sleeve_input_contracts.v1.json"), {"contracts": [{"sleeve_id": "LINK", "enabled": True, "required_inputs": [{"data_item_id": "market.price.LINK"}]}]})
    t02_before = build_dormant_sleeve_signal_generation_diagnostics_v1(truth_root=tmp_path, day_utc=DAY)
    write_missing_market_data_requirement_resolver_v1(truth_root=tmp_path, day_utc=DAY)
    t02_after = build_dormant_sleeve_signal_generation_diagnostics_v1(truth_root=tmp_path, day_utc=DAY)

    assert t02_before["summary"] == t02_after["summary"]
    assert t02_after["dormant_sleeves"][0]["dormant_reason_code"] == "MISSING_MARKET_DATA"
    assert t02_after["dormant_sleeves"][0]["downstream_diagnostics"]["artifact_id"] == "aegis_missing_market_data_requirement_resolver_v1"
