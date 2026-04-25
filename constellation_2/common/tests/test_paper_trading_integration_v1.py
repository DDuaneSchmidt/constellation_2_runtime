from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import mock


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_path_alignment_v1 import (  # noqa: E402
    resolve_canonical_lifecycle_closure_path,
    resolve_paper_session_bootstrap_path,
    resolve_paper_submit_smoke_test_report_path,
    resolve_paper_trading_integration_path,
    resolve_runtime_ledger_path,
)
import ops.tools.run_canonical_lifecycle_closure_day_v1 as closure_tool  # noqa: E402
import ops.tools.run_execution_reconciliation_day_v1 as exec_recon  # noqa: E402
import ops.tools.run_paper_trading_integration_v1 as integration_tool  # noqa: E402


DAY = "2026-04-14"
SUBMISSION_ID = "03baac64479f27f2137331d879f8bcf5fb8663e5a77f33cc15f8238844357b92"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_sleeve_submission(sleeve_truth: Path, *, advanced_status: bool) -> Path:
    subdir = sleeve_truth / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": SUBMISSION_ID,
            "submitted_at_utc": f"{DAY}T14:35:00Z",
            "binding_hash": "a" * 64,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": "PRESUBMITTED",
            "broker_ids": {"order_id": 85, "perm_id": 332310280},
            "error": None,
            "canonical_json_hash": "b" * 64,
        },
    )
    _write_json(
        subdir / "execution_event_record.v1.json",
        {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "created_at_utc": f"{DAY}T14:35:00Z",
            "event_time_utc": f"{DAY}T14:36:00Z",
            "binding_hash": "a" * 64,
            "broker_submission_hash": "b" * 64,
            "broker_order_id": "85",
            "perm_id": "332310280",
            "status": "ACKNOWLEDGED" if advanced_status else "UNKNOWN",
            "filled_qty": 0,
            "avg_price": "0",
            "raw_broker_status": "Submitted" if advanced_status else None,
            "raw_payload_digest": None,
            "sequence_num": None,
            "canonical_json_hash": "c" * 64,
            "upstream_hash": None,
        },
    )
    _write_json(
        subdir / "equity_order_plan.v2.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "plan_id": "c2_trend_eq_spy_2026-04-14_v1",
            "created_at_utc": f"{DAY}T14:30:00Z",
            "intent_hash": "d" * 64,
            "intent_sha256": "e" * 64,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": "c2_trend_eq_spy_2026-04-14_v1",
            "lineage_envelope_ref": {"path": "lineage_envelope.v1.json", "sha256": "f" * 64},
            "structure": "EQUITY_SPOT",
            "symbol": "SPY",
            "currency": "USD",
            "action": "BUY",
            "qty_shares": 1,
            "order_terms": {"order_type": "LIMIT", "limit_price": "679.91", "time_in_force": "DAY"},
            "risk_proof": None,
            "canonical_json_hash": "1" * 64,
        },
    )
    _write_json(
        subdir / "binding_record.v2.json",
        {
            "schema_id": "binding_record",
            "schema_version": "v2",
            "submission_id": SUBMISSION_ID,
            "intent_id": "c2_trend_eq_spy_2026-04-14_v1",
            "intent_hash": "d" * 64,
            "canonical_json_hash": "a" * 64,
        },
    )
    _write_json(
        subdir / "mapping_ledger_record.v2.json",
        {
            "schema_id": "mapping_ledger_record",
            "schema_version": "v2",
            "submission_id": SUBMISSION_ID,
            "canonical_json_hash": "2" * 64,
        },
    )
    return subdir


def _seed_integration_context(canonical_truth: Path, sleeve_truth: Path, *, advanced_status: bool) -> None:
    sleeve_subdir = _seed_sleeve_submission(sleeve_truth, advanced_status=advanced_status)
    (canonical_truth / "fill_ledger_v1" / DAY).mkdir(parents=True, exist_ok=True)
    _write_json(
        resolve_paper_session_bootstrap_path(truth_root=canonical_truth, day_utc=DAY),
        {
            "bootstrap_status": "READY",
            "bootstrap_semantic_status": "READY_PAPER_ONLY",
            "schema_id": "paper_session_bootstrap",
            "schema_version": "v1",
        },
    )
    request_path = sleeve_truth / "operator_inputs" / "paper_submit_smoke_test_v1" / DAY / "paper_submit_smoke_test_request.v1.json"
    _write_json(request_path, {"request_nonce": "integration-test"})
    _write_json(
        resolve_paper_submit_smoke_test_report_path(sleeve_truth_root=sleeve_truth, day_utc=DAY),
        {
            "schema_id": "paper_submit_smoke_test",
            "schema_version": "v1",
            "request_path": str(request_path),
            "request_nonce": "integration-test",
            "submission_id": SUBMISSION_ID,
            "broker_submission_record_path": str(sleeve_subdir / "broker_submission_record.v2.json"),
            "kernel_run_outcome": "SUBMITTED",
        },
    )
    assert closure_tool.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth),
            "--source_truth_root",
            str(sleeve_truth),
        ]
    ) == 0
    with mock.patch.object(
        sys,
        "argv",
        [
            "run_execution_reconciliation_day_v1.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth),
        ],
    ):
        assert exec_recon.main() == 0
    assert resolve_canonical_lifecycle_closure_path(
        truth_root=canonical_truth,
        day_utc=DAY,
        submission_id=SUBMISSION_ID,
    ).exists()


def test_paper_trading_integration_passes_when_canonical_closure_and_status_advancement_exist(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _seed_integration_context(canonical_truth, sleeve_truth, advanced_status=True)

    rc = integration_tool.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth),
            "--source_truth_root",
            str(sleeve_truth),
        ]
    )
    assert rc == 0
    report = json.loads(resolve_paper_trading_integration_path(truth_root=canonical_truth, day_utc=DAY).read_text(encoding="utf-8"))
    assert report["integration_verdict"] == "PASS"
    assert report["submission_id"] == SUBMISSION_ID
    assert report["order_id"] == "85"
    assert report["perm_id"] == "332310280"
    assert report["canonical_lifecycle_status"] == "CANONICALIZED"
    assert report["broker_status_advanced_observed"] is True
    assert report["broker_observation_basis"] == "EXECUTION_EVENT_ONLY"
    assert report["fill_observation_basis"] == "EXECUTION_EVENT_ONLY"
    assert report["propagation_state"]["status"] == "PROPAGATION_PENDING"
    assert report["canonical_reconciliation_ref"]["path"] != report["canonical_reconciliation_alias_ref"]["path"]
    assert Path(report["canonical_reconciliation_ref"]["path"]).parent.name != DAY
    assert Path(report["canonical_reconciliation_alias_ref"]["path"]).parent.name == DAY
    assert report["audit_guidance"]["immutable_snapshot_refs_preferred"] is True
    assert report["verdict_basis"]["canonical_reconciliation_ref_is_versioned"] is True
    assert report["verdict_basis"]["broker_observation_explicit"] is True
    assert report["verdict_basis"]["fill_observation_explicit"] is True
    assert report["verdict_basis"]["propagation_state_explicit"] is True
    assert report["verdict_basis"]["broker_status_advanced_observed"] is True
    assert report["verdict_basis"]["propagation_complete"] is False
    assert report["runtime_ledger_projection"]["derived_from_runtime_ledger"] is True
    assert "INTEGRATION_PASS" in report["runtime_ledger_projection"]["event_types"]
    ledger_rows = [
        json.loads(line)
        for line in resolve_runtime_ledger_path(truth_root=canonical_truth, day_utc=DAY).read_text(encoding="utf-8").splitlines()
    ]
    assert ledger_rows[-1]["event_type"] == "INTEGRATION_PASS"
    assert ledger_rows[-1]["submission_id"] == SUBMISSION_ID


def test_paper_trading_integration_fails_honestly_when_no_broker_status_advancement_is_observed(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _seed_integration_context(canonical_truth, sleeve_truth, advanced_status=False)

    rc = integration_tool.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth),
            "--source_truth_root",
            str(sleeve_truth),
        ]
    )
    assert rc == 2
    report = json.loads(resolve_paper_trading_integration_path(truth_root=canonical_truth, day_utc=DAY).read_text(encoding="utf-8"))
    assert report["integration_verdict"] == "FAIL"
    assert "BROKER_STATUS_ADVANCEMENT_NOT_OBSERVED" in report["blocker_chain"]
    assert report["broker_observation_state"] == "BROKER_STATUS_NOT_YET_OBSERVED"
    assert report["fill_observation_state"] == "FILL_NOT_YET_OBSERVED"
    assert report["propagation_state"]["status"] == "PROPAGATION_PENDING"
    assert "INTEGRATION_FAIL" in report["runtime_ledger_projection"]["event_types"]
    ledger_rows = [
        json.loads(line)
        for line in resolve_runtime_ledger_path(truth_root=canonical_truth, day_utc=DAY).read_text(encoding="utf-8").splitlines()
    ]
    assert ledger_rows[-1]["event_type"] == "INTEGRATION_FAIL"
