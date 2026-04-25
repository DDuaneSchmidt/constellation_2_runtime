from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import mock

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_path_alignment_v1 import (  # noqa: E402
    resolve_canonical_lifecycle_closure_path,
    resolve_runtime_ledger_path,
)
import ops.tools.run_canonical_lifecycle_closure_day_v1 as closure_tool  # noqa: E402
import ops.tools.run_execution_reconciliation_day_v1 as exec_recon  # noqa: E402


DAY = "2026-04-14"
SUBMISSION_ID = "03baac64479f27f2137331d879f8bcf5fb8663e5a77f33cc15f8238844357b92"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_day_open_attempt_with_runtime_lifecycle_ref(truth_root: Path) -> None:
    _write_json(
        truth_root / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json",
        {
            "schema_id": "day_open_attempt",
            "schema_version": "v1",
            "day_utc": DAY,
            "attempt_sequence": 1,
            "trigger_kind": "INITIAL_BOD_TRIGGER",
            "started_at_utc": f"{DAY}T13:30:00Z",
            "finished_at_utc": f"{DAY}T13:30:10Z",
            "trigger_ref": "/tmp/day_open_trigger.v1.json",
            "consumed_trigger_key": "trigger-key",
            "actor_identity": {"actor_name": "test", "actor_path": "/tmp/test"},
            "open_command_executed": True,
            "submit_stage_entered": True,
            "submit_stage_owner": "test",
            "result_code": "OPEN_SUCCEEDED",
            "reason_codes": [],
            "consumed_authority_refs": {
                "paper_session_ledger_v1": "/tmp/paper_session_ledger.v1.json",
                "day_open_trigger_v1": "/tmp/day_open_trigger.v1.json",
            },
            "direct_evidence_refs": {
                "sleeve_rollup_v1": "/tmp/sleeve_rollup.v1.json",
                "orchestrator_run_verdict_v2": [],
                "run_pointer_v1": [],
            },
            "runtime_lifecycle_ref": {
                "run_id": "20260418T150000Z__c2_paper_day_orchestrator_service__pid5150",
                "runtime_identity_contract_path": "/tmp/active_runtime_contract.v1.json",
                "runtime_identity_contract_sha256": "d" * 64,
                "startup_identity_receipt_path": "/tmp/runtime_startup_identity.v1.json",
                "lifecycle_start_receipt_path": "/tmp/runtime_lifecycle_receipt.v1.json",
            },
            "final_classification": "OPEN_SUCCEEDED",
            "attempt_history": [],
            "environment": "PAPER",
            "open_policy": {
                "policy_mode": "PAPER_READY_WHEN_GRANTED_UNBOUNDED_SAME_DAY",
                "environment": "PAPER",
                "max_successful_opens_per_day": 0,
                "max_late_open_attempts_per_day": 0,
                "same_day_open_cap_enforced": False,
                "time_window_enforced": False,
                "successful_open_already_recorded": False,
                "prior_success_ref": "",
                "initial_open_consumed": True,
                "late_open_available": False,
                "late_open_consumed": False,
                "late_open_consumed_ref": "",
                "open_terminal": False,
                "terminal_reason_code": "",
                "policy_status": "PAPER_REPEAT_OPEN_ALLOWED_WHEN_GRANTED",
                "attempt_count": 1,
                "latest_attempt_kind": "INITIAL_BOD_TRIGGER",
                "latest_attempt_result": "OPEN_SUCCEEDED",
                "prior_trigger_kind": "INITIAL_BOD_TRIGGER",
                "prior_trigger_status": "EMITTED",
            },
            "producer": {"repo": "constellation", "module": "test.module", "git_sha": "a" * 40},
        },
    )


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


def _seed_downstream_propagation_artifacts(canonical_truth: Path, *, positions_items: list[dict], nav_total: str) -> None:
    _write_json(
        canonical_truth / "fill_ledger_v1" / DAY / f"{SUBMISSION_ID}.fill_ledger.v1.json",
        {
            "schema_id": "fill_ledger",
            "schema_version": "v1",
            "submission_id": SUBMISSION_ID,
            "status": "OK",
        },
    )
    snapshot_path = canonical_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v4.json"
    _write_json(
        snapshot_path,
        {
            "schema_id": "positions_snapshot",
            "schema_version": "v4",
            "day_utc": DAY,
            "status": "OK",
            "positions": {"items": positions_items},
        },
    )
    _write_json(
        canonical_truth / "positions_v1" / "effective_v1" / "days" / DAY / "positions_effective_pointer.v1.json",
        {
            "schema_id": "positions_effective_pointer",
            "schema_version": "v1",
            "selection": {"selected_schema_version": 4},
            "pointers": {"snapshot_path": str(snapshot_path)},
        },
    )
    _write_json(
        canonical_truth / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "status": "ACTIVE",
            "nav_total": nav_total,
        },
    )


def _seed_broker_fact_observations(
    sleeve_truth: Path,
    *,
    order_id: str = "85",
    perm_id: str = "332310280",
    status: str = "Submitted",
    fill_qty: str = "0",
    fill_price: str = "0",
) -> None:
    ledger_dir = sleeve_truth / "broker_fact_spine_v1" / "fact_ledger" / DAY
    ledger_dir.mkdir(parents=True, exist_ok=True)
    status_row = {
        "schema_id": "observed_order_status_fact",
        "schema_version": "v1",
        "order_id": order_id,
        "perm_id": perm_id,
        "status": status,
        "quality_status": "OK",
        "quality_reason_codes": [],
        "attribution_status": "ATTRIBUTED",
        "attribution_reason_codes": [],
        "observed_utc": f"{DAY}T14:37:00Z",
        "journal_sequence_number": 18,
        "filled_quantity": fill_qty,
        "avg_fill_price": fill_price,
        "fact_record_id": "status-fact-1",
    }
    fill_row = {
        "schema_id": "observed_fill_fact",
        "schema_version": "v1",
        "order_id": order_id,
        "perm_id": perm_id,
        "execution_id": "fill-1",
        "fill_quantity": fill_qty,
        "fill_price": fill_price,
        "quality_status": "OK",
        "quality_reason_codes": [],
        "attribution_status": "ATTRIBUTED",
        "attribution_reason_codes": [],
        "observed_utc": f"{DAY}T14:37:01Z",
        "journal_sequence_number": 19,
        "fact_record_id": "fill-fact-1",
    }
    (ledger_dir / "observed_order_status_fact.v1.jsonl").write_text(
        json.dumps(status_row, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    (ledger_dir / "observed_fill_fact.v1.jsonl").write_text(
        json.dumps(fill_row, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def test_canonical_lifecycle_closure_materializes_canonical_visibility_and_reconciliation(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    sleeve_subdir = _seed_sleeve_submission(sleeve_truth, advanced_status=False)
    (canonical_truth / "execution_stream_v1" / DAY).mkdir(parents=True, exist_ok=True)
    (canonical_truth / "fill_ledger_v1" / DAY).mkdir(parents=True, exist_ok=True)

    rc = closure_tool.main(
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

    closure_path = resolve_canonical_lifecycle_closure_path(
        truth_root=canonical_truth,
        day_utc=DAY,
        submission_id=SUBMISSION_ID,
    )
    closure = json.loads(closure_path.read_text(encoding="utf-8"))
    assert closure["submission_id"] == SUBMISSION_ID
    assert closure["order_id"] == "85"
    assert closure["perm_id"] == "332310280"
    assert closure["canonical_lifecycle_status"] == "CANONICALIZED"
    assert closure["lifecycle_states"] == [
        "SUBMISSION_RECORDED",
        "BROKER_STATUS_NOT_YET_OBSERVED",
        "FILL_NOT_YET_OBSERVED",
        "PROPAGATION_PENDING",
    ]
    assert closure["broker_observation_state"] == "BROKER_STATUS_NOT_YET_OBSERVED"
    assert closure["fill_observation_state"] == "FILL_NOT_YET_OBSERVED"
    assert closure["broker_observation_basis"] == "EXECUTION_EVENT_ONLY"
    assert closure["fill_observation_basis"] == "EXECUTION_EVENT_ONLY"
    assert closure["broker_observation_match_count"] == 0
    assert closure["fill_observation_match_count"] == 0
    assert closure["broker_observation_ref"]["path"].endswith("execution_event_record.v1.json")
    assert closure["fill_observation_ref"]["path"].endswith("execution_event_record.v1.json")
    assert closure["propagation_state"]["status"] == "PROPAGATION_PENDING"
    assert closure["propagation_state"]["blocker_chain"] == ["FILL_NOT_YET_OBSERVED"]
    assert closure["runtime_ledger_projection"]["derived_from_runtime_ledger"] is True
    assert closure["runtime_ledger_projection"]["event_types"] == [
        "BROKER_STATUS_NOT_YET_OBSERVED",
        "FILL_NOT_YET_OBSERVED",
        "PROPAGATION_PENDING",
        "SUBMISSION_RECORDED",
    ]
    ledger_rows = [
        json.loads(line)
        for line in resolve_runtime_ledger_path(truth_root=canonical_truth, day_utc=DAY).read_text(encoding="utf-8").splitlines()
    ]
    assert [row["event_type"] for row in ledger_rows] == [
        "SUBMISSION_RECORDED",
        "BROKER_STATUS_NOT_YET_OBSERVED",
        "FILL_NOT_YET_OBSERVED",
        "PROPAGATION_PENDING",
    ]
    assert all(row["submission_id"] == SUBMISSION_ID for row in ledger_rows)

    canonical_subdir = canonical_truth / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    assert (canonical_subdir / "broker_submission_record.v2.json").read_bytes() == (
        sleeve_subdir / "broker_submission_record.v2.json"
    ).read_bytes()
    assert (canonical_subdir / "execution_event_record.v1.json").read_bytes() == (
        sleeve_subdir / "execution_event_record.v1.json"
    ).read_bytes()

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
        rc = exec_recon.main()
    assert rc == 0
    recon = json.loads(
        (
            canonical_truth
            / "reports"
            / "execution_reconciliation_v1"
            / DAY
            / "execution_reconciliation.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert recon["status"] == "PASS"
    assert recon["semantic_status"] == "FULLY_OBSERVED_AND_CONFIRMED"
    assert "NO_SUBMISSIONS_FOUND" not in recon["reason_codes"]
    assert recon["runtime_ledger_projection"]["derived_from_runtime_ledger"] is True


def test_execution_reconciliation_marks_missing_upstream_inputs_as_not_yet_materialized(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    subdir = canonical_truth / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
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
    (canonical_truth / "fill_ledger_v1" / DAY).mkdir(parents=True, exist_ok=True)

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
        rc = exec_recon.main()
    assert rc == 0

    recon = json.loads(
        (
            canonical_truth
            / "reports"
            / "execution_reconciliation_v1"
            / DAY
            / "execution_reconciliation.v1.json"
        ).read_text(encoding="utf-8")
    )

    assert recon["status"] == "FAIL"
    assert recon["semantic_status"] == "NOT_YET_MATERIALIZED"
    assert "UPSTREAM_NOT_MATERIALIZED" in recon["reason_codes"]


def test_execution_reconciliation_emits_runtime_lifecycle_ref_when_day_open_attempt_carries_it(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    subdir = canonical_truth / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
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
    (canonical_truth / "execution_stream_v1" / DAY).mkdir(parents=True, exist_ok=True)
    (canonical_truth / "fill_ledger_v1" / DAY).mkdir(parents=True, exist_ok=True)
    (canonical_truth / "reports" / "canonical_lifecycle_closure_v1" / DAY).mkdir(parents=True, exist_ok=True)
    _write_day_open_attempt_with_runtime_lifecycle_ref(canonical_truth)
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
    ), mock.patch.object(
        exec_recon,
        "runtime_ledger_ref_v1",
        return_value={
            "path": canonical_truth / "runtime_ledger_v1" / DAY / "canonical_runtime_ledger.v1.jsonl",
            "sha256": "a" * 64,
            "exists": True,
        },
    ), mock.patch.object(
        exec_recon,
        "read_runtime_ledger_events_v1",
        return_value=[{"event_id": "a" * 64, "event_type": "SUBMISSION_RECORDED", "submission_id": SUBMISSION_ID}],
    ):
        rc = exec_recon.main()
    assert rc == 0

    recon = json.loads(
        (
            canonical_truth
            / "reports"
            / "execution_reconciliation_v1"
            / DAY
            / "execution_reconciliation.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert recon["runtime_lifecycle_ref"]["run_id"] == "20260418T150000Z__c2_paper_day_orchestrator_service__pid5150"
    assert any(item["type"] == "day_open_attempt_v1" for item in recon["input_manifest"])


def test_canonical_lifecycle_closure_marks_propagation_complete_when_fill_and_downstream_artifacts_exist(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _seed_sleeve_submission(sleeve_truth, advanced_status=True)
    _write_json(
        sleeve_truth / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID / "execution_event_record.v1.json",
        {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "created_at_utc": f"{DAY}T14:35:00Z",
            "event_time_utc": f"{DAY}T14:36:00Z",
            "binding_hash": "a" * 64,
            "broker_submission_hash": "b" * 64,
            "broker_order_id": "85",
            "perm_id": "332310280",
            "status": "FILLED",
            "filled_qty": 1,
            "avg_price": "679.91",
            "raw_broker_status": "Filled",
            "raw_payload_digest": None,
            "sequence_num": None,
            "canonical_json_hash": "c" * 64,
            "upstream_hash": None,
        },
    )
    _seed_downstream_propagation_artifacts(
        canonical_truth,
        positions_items=[{"instrument": {"kind": "EQUITY", "symbol": "SPY"}}],
        nav_total="100000.00",
    )

    rc = closure_tool.main(
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

    closure = json.loads(
        resolve_canonical_lifecycle_closure_path(
            truth_root=canonical_truth,
            day_utc=DAY,
            submission_id=SUBMISSION_ID,
        ).read_text(encoding="utf-8")
    )
    assert closure["canonical_lifecycle_status"] == "CANONICALIZED"
    assert "TERMINAL_STATE_OBSERVED" in closure["lifecycle_states"]
    assert "BROKER_STATUS_OBSERVED" in closure["lifecycle_states"]
    assert "FILL_OBSERVED" in closure["lifecycle_states"]
    assert "PROPAGATION_COMPLETE" in closure["lifecycle_states"]
    assert closure["fill_observed"] is True
    assert closure["propagation_state"]["status"] == "PROPAGATION_COMPLETE"
    assert closure["propagation_state"]["positions_item_count"] == 1
    assert closure["propagation_state"]["nav_total_observed"] is True
    assert closure["propagation_state"]["blocker_chain"] == []
    ledger_rows = [
        json.loads(line)
        for line in resolve_runtime_ledger_path(truth_root=canonical_truth, day_utc=DAY).read_text(encoding="utf-8").splitlines()
    ]
    assert [row["event_type"] for row in ledger_rows] == [
        "SUBMISSION_RECORDED",
        "BROKER_STATUS_OBSERVED",
        "FILL_OBSERVED",
        "PROPAGATION_COMPLETE",
    ]


def test_canonical_lifecycle_closure_uses_exact_match_broker_fact_observation_when_present(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _seed_sleeve_submission(sleeve_truth, advanced_status=False)
    _seed_broker_fact_observations(
        sleeve_truth,
        status="Submitted",
        fill_qty="1",
        fill_price="679.91",
    )
    (canonical_truth / "fill_ledger_v1" / DAY).mkdir(parents=True, exist_ok=True)

    rc = closure_tool.main(
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

    closure = json.loads(
        resolve_canonical_lifecycle_closure_path(
            truth_root=canonical_truth,
            day_utc=DAY,
            submission_id=SUBMISSION_ID,
        ).read_text(encoding="utf-8")
    )
    assert closure["broker_status_observed"] == "Submitted"
    assert closure["broker_observation_state"] == "BROKER_STATUS_OBSERVED"
    assert closure["broker_observation_basis"] == "BROKER_FACT_AND_EXECUTION_EVENT"
    assert closure["broker_observation_match_count"] == 1
    assert closure["broker_observation_ref"]["path"].endswith("observed_order_status_fact.v1.jsonl")
    assert closure["fill_observation_state"] == "FILL_OBSERVED"
    assert closure["fill_observation_basis"] == "BROKER_FACT_AND_EXECUTION_EVENT"
    assert closure["fill_observation_match_count"] == 1
    assert closure["fill_observation_ref"]["path"].endswith("observed_fill_fact.v1.jsonl")
    assert closure["fill_status_observed"] == {"filled_qty": 1, "avg_price": "679.91"}
    assert "BROKER_STATUS_OBSERVED" in closure["lifecycle_states"]
    assert "FILL_OBSERVED" in closure["lifecycle_states"]
    assert closure["propagation_state"]["status"] == "PROPAGATION_PENDING"
    assert "POSITIONS_SNAPSHOT_MISSING" in closure["propagation_state"]["blocker_chain"]
    assert "NAV_ARTIFACT_MISSING" in closure["propagation_state"]["blocker_chain"]


def test_canonical_lifecycle_closure_surfaces_empty_positions_after_fill_as_explicit_pending_reason(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _seed_sleeve_submission(sleeve_truth, advanced_status=False)
    _seed_broker_fact_observations(
        sleeve_truth,
        status="Filled",
        fill_qty="1",
        fill_price="679.91",
    )
    _seed_downstream_propagation_artifacts(
        canonical_truth,
        positions_items=[],
        nav_total="100000.00",
    )

    rc = closure_tool.main(
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

    closure = json.loads(
        resolve_canonical_lifecycle_closure_path(
            truth_root=canonical_truth,
            day_utc=DAY,
            submission_id=SUBMISSION_ID,
        ).read_text(encoding="utf-8")
    )
    assert closure["broker_observation_state"] == "TERMINAL_STATE_OBSERVED"
    assert closure["fill_observation_state"] == "FILL_OBSERVED"
    assert closure["propagation_state"]["status"] == "PROPAGATION_PENDING"
    assert "POSITIONS_EMPTY_AFTER_FILL_NOT_EXPLAINED" in closure["propagation_state"]["blocker_chain"]


def test_canonical_lifecycle_closure_fails_closed_when_submission_filter_missing(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _seed_sleeve_submission(sleeve_truth, advanced_status=False)
    (canonical_truth / "fill_ledger_v1" / DAY).mkdir(parents=True, exist_ok=True)

    with pytest.raises(SystemExit, match="FILTERED_SUBMISSION_NOT_FOUND"):
        closure_tool.main(
            [
                "--day_utc",
                DAY,
                "--truth_root",
                str(canonical_truth),
                "--source_truth_root",
                str(sleeve_truth),
                "--submission_id",
                "missing-submission-id",
            ]
        )
