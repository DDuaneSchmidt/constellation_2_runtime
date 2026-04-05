from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common import execution_truth_foundation_v1 as etf  # noqa: E402
from constellation_2.common.tests.test_diagnostic_foundation_v1 import _write_day_fixture  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _hx(char: str) -> str:
    return char * 64


def _producer_contract_by_id(rows: list[dict]) -> dict[str, dict]:
    return {row["producer_contract_id"]: row for row in rows}


def _prepare_sparse_truth(truth_root: Path, day: str = "2026-04-02") -> None:
    _write_day_fixture(
        truth_root,
        day,
        attempt_id=f"{day}__A0001",
        verdict_status="PASS",
        intents=3,
        submitted=2,
        filled=1,
        rejected=1,
        vetoed=0,
    )


def _prepare_complete_execution_truth(truth_root: Path, day: str = "2026-04-02") -> None:
    intent_hash = _hx("a")
    submission_id = _hx("b")
    binding_hash = _hx("c")
    event_hash = _hx("d")
    auth_hash = _hx("e")
    intent_id = "intent-000000000001"
    produced = f"{day}T00:00:00Z"

    _write_json(
        truth_root / "intents_v1" / "day_rollup" / day / "intents_day_rollup.v1.json",
        {
            "schema_id": "intents_day_rollup.v1",
            "day_utc": day,
            "produced_utc": produced,
            "producer": {"component": "fixture", "version": "v1", "git_sha": "fixture"},
            "inputs": {"market_data_snapshot_hashes": [_hx("f")], "market_calendar_hash": _hx("1"), "engine_config_hashes": [_hx("2")]},
            "engines": [
                {
                    "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                    "intent_type": "exposure_intent.v1",
                    "intent_hashes": [intent_hash],
                    "intent_count": 1,
                }
            ],
            "rollup_sha256": _hx("3"),
        },
    )
    _write_json(
        truth_root / "intents_v1" / "snapshots" / day / f"{intent_hash}.exposure_intent.v1.json",
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": intent_id,
            "created_at_utc": produced,
            "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "suite": "C2_HYBRID_V1", "mode": "PAPER"},
            "underlying": {"symbol": "SPY", "currency": "USD"},
            "exposure_type": "LONG_EQUITY",
            "target_notional_pct": "0.4",
            "expected_holding_days": 3,
            "risk_class": "TREND",
            "constraints": {"max_risk_pct": "0.01"},
            "canonical_json_hash": None,
        },
    )
    _write_json(
        truth_root / "engine_activity_v1" / "authorization_v1" / day / f"{intent_hash}.authorization.v1.json",
        {
            "schema_id": "C2_AUTHORIZATION_V1",
            "schema_version": 1,
            "produced_utc": produced,
            "day_utc": day,
            "producer": {"repo": "fixture", "git_sha": "abcdef1", "module": "fixture"},
            "status": "AUTHORIZED",
            "reason_codes": [],
            "input_manifest": [
                {
                    "type": "intent",
                    "path": str((truth_root / "intents_v1" / "snapshots" / day / f"{intent_hash}.exposure_intent.v1.json").resolve()),
                    "sha256": intent_hash,
                    "day_utc": day,
                    "producer": "fixture",
                }
            ],
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "intent_id": intent_id,
            "intent_hash": intent_hash,
            "authorization": {
                "decision": "AUTHORIZED",
                "authorized_quantity": 10,
                "constraints": [],
                "decision_hash": auth_hash,
            },
        },
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / day / "submission_0"
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": submission_id,
            "submitted_at_utc": produced,
            "binding_hash": binding_hash,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": "FILLED",
            "broker_ids": {"order_id": 101, "perm_id": 202},
            "canonical_json_hash": None,
        },
    )
    _write_json(
        subdir / "execution_event_record.v1.json",
        {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "created_at_utc": produced,
            "event_time_utc": produced,
            "binding_hash": binding_hash,
            "broker_submission_hash": _hx("4"),
            "broker_order_id": "101",
            "perm_id": "202",
            "status": "FILLED",
            "filled_qty": 10,
            "avg_price": "500.25",
            "canonical_json_hash": None,
            "upstream_hash": None,
            "raw_broker_status": None,
            "raw_payload_digest": None,
            "sequence_num": 1,
        },
    )
    _write_json(
        subdir / "equity_order_plan.v1.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v1",
            "plan_id": "plan-0000000000001",
            "created_at_utc": produced,
            "intent_hash": intent_hash,
            "structure": "EQUITY_SPOT",
            "symbol": "SPY",
            "currency": "USD",
            "action": "BUY",
            "qty_shares": 10,
            "order_terms": {"order_type": "MARKET", "limit_price": None, "time_in_force": "DAY"},
            "risk_proof": None,
            "canonical_json_hash": None,
        },
    )
    _write_json(
        truth_root / "execution_stream_v1" / day / f"{event_hash}.execution_event_stream_record.v1.json",
        {
            "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
            "schema_version": 1,
            "produced_utc": produced,
            "day_utc": day,
            "producer": {"repo": "fixture", "git_sha": "abcdef1", "module": "fixture"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": intent_id,
            "intent_sha256": intent_hash,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "event_type": "EXEC_DETAILS",
            "event_time_utc": produced,
            "observed_at_utc": produced,
            "broker_ids": {"order_id": 101, "perm_id": 202},
            "order_state": {"status": "FILLED", "filled_qty": 10, "remaining_qty": 0, "avg_fill_price": "500.25"},
            "fill": {"fill_qty": 10, "fill_price": "500.25", "commission": "1.00", "currency": "USD"},
            "canonical_json_hash": _hx("5"),
        },
    )
    _write_json(
        truth_root / "fill_ledger_v1" / day / f"{submission_id}.fill_ledger.v1.json",
        {
            "schema_id": "C2_FILL_LEDGER_V1",
            "schema_version": 1,
            "produced_utc": produced,
            "day_utc": day,
            "producer": {"repo": "fixture", "git_sha": "abcdef1", "module": "fixture"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": intent_id,
            "intent_sha256": intent_hash,
            "order_qty": 10,
            "filled_qty": 10,
            "remaining_qty": 0,
            "avg_fill_price_weighted": "500.25",
            "lifecycle_status": "FILLED",
            "event_hashes": [event_hash],
            "canonical_json_hash": _hx("6"),
        },
    )


def test_dependency_readiness_classification(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_sparse_truth(truth_root)
    facts = etf.collect_execution_truth_facts(REPO_ROOT, truth_root, "2026-04-02")
    statuses = etf.build_dependency_statuses(REPO_ROOT, truth_root, "2026-04-02", facts)
    by_id = {row["dependency_id"]: row for row in statuses}
    assert by_id["market_data_snapshot_for_real_intent_generation"]["status"] == "MISSING_BLOCKING_DEPENDENCY"
    assert by_id["submission_identity_bundle"]["status"] == "PARTIAL_DEPENDENCY_SET"


def test_payload_completeness_failure_classification(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_sparse_truth(truth_root)
    facts = etf.collect_execution_truth_facts(REPO_ROOT, truth_root, "2026-04-02")
    probes = etf.build_payload_probes(REPO_ROOT, truth_root, "2026-04-02", facts)
    assert probes["authorization"].completeness_status == "SCHEMA_MISMATCH"
    assert probes["intents_day_rollup_linkage"].completeness_status == "AGGREGATE_ONLY_NO_LINKAGE"


def test_producer_contract_violation_classification(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_sparse_truth(truth_root)
    facts = etf.collect_execution_truth_facts(REPO_ROOT, truth_root, "2026-04-02")
    probes = etf.build_payload_probes(REPO_ROOT, truth_root, "2026-04-02", facts)
    contract_results = etf.build_producer_contract_results(REPO_ROOT, probes)
    by_id = {row.producer_contract_id: row for row in contract_results}
    assert by_id["intent_snapshot_producer_contract_v1"].contract_status == "MISSING_ARTIFACT"
    assert by_id["authorization_payload_contract_v1"].contract_status == "PRESENT_BUT_INCOMPLETE"


def test_lifecycle_progression_blocked_by_sparse_evidence(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_sparse_truth(truth_root)
    docs = etf.build_execution_truth_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z")
    assert docs["lifecycle"]["first_break_stage"] == "INTENT_EMITTED"
    assert docs["lifecycle"]["execution_truth_status"] == "EXECUTION_TRUTH_INCOMPLETE"
    assert docs["lifecycle"]["stages"][0]["stage_status"] == "BLOCKED_MISSING_DEPENDENCY"
    assert docs["lifecycle"]["stages"][0]["producer_contract_failures"] == ["intent_snapshot_producer_contract_v1"]


def test_lifecycle_progression_advances_only_when_prerequisites_met(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_complete_execution_truth(truth_root)
    docs = etf.build_execution_truth_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z")
    assert all(row["stage_status"] == "COMPLETE" for row in docs["lifecycle"]["stages"])
    assert docs["lifecycle"]["execution_truth_status"] == "EXECUTION_TRUTH_COMPLETE"


def test_economic_finalization_blocked_without_required_inputs(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_complete_execution_truth(truth_root)
    docs = etf.build_execution_truth_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z")
    assert docs["economic"]["economic_finalization_status"] == "READY_FOR_POSITIONS"
    assert docs["economic"]["execution_truth_status"] == "EXECUTION_TRUTH_COMPLETE"


def test_gap_report_identifies_first_break_and_blocked_writers(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_sparse_truth(truth_root)
    docs = etf.build_execution_truth_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z")
    assert docs["gap"]["first_break_stage"] == "INTENT_EMITTED"
    contract_rows = _producer_contract_by_id(docs["gap"]["producer_contract_results"])
    assert contract_rows["intent_snapshot_producer_contract_v1"]["contract_status"] == "MISSING_ARTIFACT"
    assert "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py" in docs["gap"]["blocked_writers"]


def test_replay_same_inputs_same_outputs(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_sparse_truth(truth_root)
    docs1 = etf.build_execution_truth_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z")
    docs2 = etf.build_execution_truth_docs(REPO_ROOT, truth_root, "2026-04-02", "2026-04-02T00:00:00Z")
    assert canonical_json_bytes_v1(docs1["gap"]) == canonical_json_bytes_v1(docs2["gap"])
    assert canonical_json_bytes_v1(docs1["normalization"]) == canonical_json_bytes_v1(docs2["normalization"])


def test_internal_self_failure_path(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _prepare_sparse_truth(truth_root)

    def _boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(etf, "build_dependency_statuses", _boom)
    writes = etf.write_execution_truth_plane(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-02",
        produced_utc="2026-04-02T00:00:00Z",
    )
    assert writes["gap"].action == "WROTE"
    gap_doc = json.loads(
        (truth_root / "reports" / "execution_completion_gap_report_v1" / "2026-04-02" / "execution_completion_gap_report.v1.json").read_text(encoding="utf-8")
    )
    assert gap_doc["integrity_status"] == "INTERNAL_FAILURE"
    assert gap_doc["first_break_stage"] == "INTERNAL_FAILURE"
    assert gap_doc["execution_truth_status"] == "INTERNAL_FAILURE"
