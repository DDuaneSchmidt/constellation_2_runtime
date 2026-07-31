from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.dormant_sleeve_signal_generation_diagnostics_v1 import (
    build_dormant_sleeve_signal_generation_diagnostics_v1,
    dormant_sleeve_signal_generation_diagnostics_path_v1,
    write_dormant_sleeve_signal_generation_diagnostics_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.sleeve_throughput_diagnostics_v1 import build_sleeve_throughput_diagnostics_v1

DAY = "2026-06-02"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed_base(root: Path) -> None:
    sleeves = [
        ("VALID", "H_VALID", "Valid No Signal"),
        ("UNREGISTERED", "H_UNREGISTERED", "Missing Producer Registration"),
        ("NOT_INVOKED", "H_NOT_INVOKED", "Producer Not Invoked"),
        ("MISSING_MARKET", "H_MISSING_MARKET", "Missing Market Data"),
        ("MISSING_EVENT", "H_MISSING_EVENT", "Missing Event Data"),
        ("MISSING_POLICY", "H_MISSING_POLICY", "Missing Trigger Policy"),
        ("THRESHOLD", "H_THRESHOLD", "Thresholds Not Met"),
        ("RUNTIME", "H_RUNTIME", "Runtime Error"),
    ]
    write_json_v1(_report(root, "aegis_sleeve_throughput_diagnostics_v1", "sleeve_throughput_diagnostics.v1.json"), {
        "sleeves": [
            {
                "sleeve_id": sid,
                "sleeve_name": name,
                "hypothesis_id": hid,
                "sleeve_status": "ACTIVE",
                "raw_signal_count": 0,
                "candidate_count": 0,
                "throughput_status": "DORMANT",
                "blocker_code": "NO_SIGNALS_GENERATED",
                "furthest_stage_reached": "HYPOTHESIS",
            }
            for sid, hid, name in sleeves
        ],
        "summary": {"sleeves_dormant": len(sleeves)},
    })
    write_json_v1(_report(root, "aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json"), {
        "sleeves": [
            {
                "sleeve_id": "VALID",
                "enabled": True,
                "run_status": "RAN",
                "evaluation_status": "NO_INTENT",
                "producer_command": "python3 valid.py",
                "raw_signal_count": 0,
                "reason_codes": ["NO_INTENT_DECLARED"],
                "thresholds_applied": ["runtime_ready", "trigger_detected"],
            },
            {
                "sleeve_id": "NOT_INVOKED",
                "enabled": True,
                "run_status": "NOT_EXECUTED",
                "evaluation_status": "READY",
                "producer_command": "python3 not_invoked.py",
                "thresholds_applied": ["runtime_ready"],
            },
            {
                "sleeve_id": "MISSING_MARKET",
                "enabled": True,
                "run_status": "BLOCKED",
                "evaluation_status": "BLOCKED",
                "producer_command": "python3 missing_market.py",
                "missing_inputs": ["market.price.SPY"],
                "thresholds_applied": ["runtime_ready"],
            },
            {
                "sleeve_id": "MISSING_EVENT",
                "enabled": True,
                "run_status": "BLOCKED",
                "evaluation_status": "BLOCKED",
                "producer_command": "python3 missing_event.py",
                "reason_codes": ["EVENT_CALENDAR_MISSING"],
                "thresholds_applied": ["runtime_ready"],
            },
            {
                "sleeve_id": "MISSING_POLICY",
                "enabled": True,
                "run_status": "RAN",
                "evaluation_status": "NO_INTENT",
                "producer_command": "python3 missing_policy.py",
                "reason_codes": ["NO_INTENT_DECLARED"],
            },
            {
                "sleeve_id": "THRESHOLD",
                "enabled": True,
                "run_status": "RAN",
                "evaluation_status": "NO_INTENT",
                "producer_command": "python3 threshold.py",
                "reason_codes": ["NO_INTENT_DECLARED"],
                "thresholds_applied": ["z_enter"],
            },
            {
                "sleeve_id": "RUNTIME",
                "enabled": True,
                "run_status": "RAN",
                "evaluation_status": "BLOCKED",
                "producer_command": "python3 runtime.py",
                "thresholds_applied": ["runtime_ready"],
            },
        ],
    })
    write_json_v1(_report(root, "sleeve_evaluation_kernel_v1", "sleeve_evaluation_rollup.v1.json"), {
        "outcomes": [
            {
                "sleeve_id": "VALID",
                "status": "NO_INTENT",
                "producer_command": "python3 valid.py",
                "exit_code": 0,
                "reason_codes": ["NO_INTENT_DECLARED"],
                "market_data_manifest_check": {"status": "PASS", "canonical_blocker": ""},
                "stdout_summary": '{"status":"NO_INTENT","rule":"NO_SETUP"}',
            },
            {
                "sleeve_id": "MISSING_MARKET",
                "status": "BLOCKED",
                "producer_command": "python3 missing_market.py",
                "exit_code": 1,
                "reason_codes": ["MISSING_REQUIRED_INPUTS"],
                "stderr_summary": "FAIL: MISSING_REQUIRED_INPUTS: /tmp/market_data_snapshot_v1/SPY.json",
            },
            {
                "sleeve_id": "THRESHOLD",
                "status": "NO_INTENT",
                "producer_command": "python3 threshold.py",
                "exit_code": 0,
                "reason_codes": ["NO_INTENT_DECLARED"],
                "market_data_manifest_check": {"status": "PASS", "canonical_blocker": ""},
                "stdout_summary": '{"status":"NO_INTENT","z_enter":2.0,"evaluations":[{"z":0.4}]}',
            },
            {
                "sleeve_id": "RUNTIME",
                "status": "BLOCKED",
                "producer_command": "python3 runtime.py",
                "exit_code": 1,
                "reason_codes": ["PRODUCER_NONZERO_RC"],
                "stderr_summary": "Traceback: ValueError: bad parse",
            },
        ],
    })
    write_json_v1(_report(root, "aegis_sleeve_input_contracts_v1", "sleeve_input_contracts.v1.json"), {
        "contracts": [
            {"sleeve_id": "VALID", "enabled": True, "contract_status": "OK", "required_inputs": [{"data_item_id": "market.price.SPY"}]},
            {"sleeve_id": "NOT_INVOKED", "enabled": True, "contract_status": "OK", "required_inputs": [{"data_item_id": "market.price.SPY"}]},
            {"sleeve_id": "MISSING_MARKET", "enabled": True, "contract_status": "OK", "required_inputs": [{"data_item_id": "market.price.SPY"}]},
            {"sleeve_id": "MISSING_EVENT", "enabled": True, "contract_status": "OK", "required_inputs": [{"data_item_id": "event.calendar"}]},
            {"sleeve_id": "MISSING_POLICY", "enabled": True, "contract_status": "OK", "required_inputs": [{"data_item_id": "market.price.SPY"}]},
            {"sleeve_id": "THRESHOLD", "enabled": True, "contract_status": "OK", "required_inputs": [{"data_item_id": "market.price.SPY"}]},
            {"sleeve_id": "RUNTIME", "enabled": True, "contract_status": "OK", "required_inputs": [{"data_item_id": "market.price.SPY"}]},
        ]
    })
    write_json_v1(_report(root, "aegis_macro_calendar_data_readiness_v1", "macro_calendar_data_readiness.v1.json"), {
        "status": "NEEDS_SOURCE",
        "macro_calendar_ready": False,
    })


def test_dormant_signal_diagnostics_classifies_requested_reasons(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    payload = build_dormant_sleeve_signal_generation_diagnostics_v1(truth_root=tmp_path, day_utc=DAY)
    rows = {row["sleeve_id"]: row for row in payload["dormant_sleeves"]}

    assert rows["VALID"]["dormant_reason_code"] == "VALID_NO_SIGNAL_CONDITIONS"
    assert rows["UNREGISTERED"]["dormant_reason_code"] == "PRODUCER_NOT_REGISTERED"
    assert rows["NOT_INVOKED"]["dormant_reason_code"] == "PRODUCER_NOT_INVOKED"
    assert rows["MISSING_MARKET"]["dormant_reason_code"] == "MISSING_MARKET_DATA"
    assert rows["MISSING_EVENT"]["dormant_reason_code"] == "MISSING_EVENT_DATA"
    assert rows["MISSING_POLICY"]["dormant_reason_code"] == "MISSING_TRIGGER_POLICY"
    assert rows["THRESHOLD"]["dormant_reason_code"] == "TRIGGER_THRESHOLDS_NOT_MET"
    assert rows["RUNTIME"]["dormant_reason_code"] == "PRODUCER_RUNTIME_ERROR"


def test_dormant_signal_diagnostics_preserves_t01_behavior_and_read_only(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    before = build_sleeve_throughput_diagnostics_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_dormant_sleeve_signal_generation_diagnostics_v1(truth_root=tmp_path, day_utc=DAY)
    after = build_sleeve_throughput_diagnostics_v1(truth_root=tmp_path, day_utc=DAY)
    payload = build_dormant_sleeve_signal_generation_diagnostics_v1(truth_root=tmp_path, day_utc=DAY)

    assert path == dormant_sleeve_signal_generation_diagnostics_path_v1(truth_root=tmp_path, day_utc=DAY)
    assert before["summary"] == after["summary"]
    before_behavior = {row["sleeve_id"]: (row["throughput_status"], row["blocker_code"], row["raw_signal_count"], row["candidate_count"]) for row in before["sleeves"]}
    after_behavior = {row["sleeve_id"]: (row["throughput_status"], row["blocker_code"], row["raw_signal_count"], row["candidate_count"]) for row in after["sleeves"]}
    assert before_behavior == after_behavior
    assert payload["read_only"] is True
    assert payload["no_signal_fabrication"] is True
    assert payload["no_forced_candidates"] is True
    assert all(row["t01_link"]["blocker_code"] == "NO_SIGNALS_GENERATED" for row in payload["dormant_sleeves"])
