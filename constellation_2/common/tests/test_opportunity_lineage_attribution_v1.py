from __future__ import annotations

import json
from pathlib import Path

import pytest

import constellation_2.common.opportunity_lineage_attribution_v1 as attribution_mod


DAY = "2026-04-23"
ENV = "PAPER"
SLEEVE = "C2_TREND_EQ_PRIMARY_V1"
OPPORTUNITY = "c2_trend_eq_spy_2026-04-23_v1"


def _event(
    *,
    stage: str,
    status: str = "PASS",
    reason_code: str | None = None,
    observed_at_utc: str = f"{DAY}T13:30:00Z",
    metrics: dict[str, object] | None = None,
) -> dict[str, object]:
    return attribution_mod._build_lineage_event_payload(  # noqa: SLF001
        day_utc=DAY,
        environment=ENV,
        sleeve_id=SLEEVE,
        opportunity_id=OPPORTUNITY,
        lineage_stage=stage,
        stage_status=status,
        observed_at_utc=observed_at_utc,
        reason_code=reason_code or f"TEST_{stage.upper()}",
        reason_detail=f"test:{stage}",
        evidence_refs=[],
        upstream_refs=[],
        emitted_by="test",
        stage_metrics=metrics or {},
    )


def test_ran_no_signal_classification() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[_event(stage="sleeve_run"), _event(stage="signal_eval")]
    )
    assert result["final_classification"] == "RAN_NO_SIGNAL"


def test_signal_filtered_classification() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[
            _event(stage="sleeve_run"),
            _event(stage="signal_eval"),
            _event(stage="signal_filtered", reason_code="RISK_FILTER_EXPOSURE_CAP"),
        ]
    )
    assert result["final_classification"] == "RAN_SIGNAL_FILTERED"


def test_intent_created_not_submitted_classification() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[_event(stage="intent_created")]
    )
    assert result["final_classification"] == "INTENT_CREATED_NOT_SUBMITTED"


def test_intent_submitted_no_order_classification() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[_event(stage="intent_created"), _event(stage="submit_attempted")]
    )
    assert result["final_classification"] == "INTENT_SUBMITTED_NO_ORDER"


def test_order_accepted_no_fill_classification() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[
            _event(stage="intent_created"),
            _event(stage="submit_attempted"),
            _event(stage="order_ack"),
        ]
    )
    assert result["final_classification"] == "ORDER_ACCEPTED_NO_FILL"


def test_partial_fill_classification() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[
            _event(stage="intent_created"),
            _event(stage="submit_attempted"),
            _event(stage="order_ack"),
            _event(stage="fill_event", metrics={"filled_qty": 1, "order_qty": 2}),
        ]
    )
    assert result["final_classification"] == "PARTIAL_FILL"


def test_filled_classification() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[
            _event(stage="intent_created"),
            _event(stage="submit_attempted"),
            _event(stage="order_ack"),
            _event(stage="fill_event", metrics={"filled_qty": 2, "order_qty": 2}),
        ]
    )
    assert result["final_classification"] == "FILLED"


def test_order_rejected_classification() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[
            _event(stage="intent_created"),
            _event(stage="submit_attempted"),
            _event(stage="order_rejected", reason_code="BROKER_PRE_ACK_REJECT"),
        ]
    )
    assert result["final_classification"] == "ORDER_REJECTED"


def test_order_cancelled_classification() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[
            _event(stage="intent_created"),
            _event(stage="submit_attempted"),
            _event(stage="order_ack"),
            _event(stage="order_cancelled"),
        ]
    )
    assert result["final_classification"] == "ORDER_CANCELLED"


def test_contradictory_evidence_classifies_incomplete() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[
            _event(stage="submit_attempted"),
            _event(stage="order_rejected"),
            _event(stage="fill_event", metrics={"filled_qty": 1, "order_qty": 1}),
        ]
    )
    assert result["final_classification"] == "ATTRIBUTION_INCOMPLETE"
    assert result["completeness_status"] == "CONTRADICTORY"


def test_precedence_is_deterministic_for_permuted_events() -> None:
    events = [
        _event(stage="submit_attempted"),
        _event(stage="order_cancelled", observed_at_utc=f"{DAY}T14:00:00Z"),
        _event(stage="fill_event", observed_at_utc=f"{DAY}T14:01:00Z", metrics={"filled_qty": 1, "order_qty": 2}),
    ]
    first = attribution_mod.classify_opportunity_from_lineage_events_v1(events=events)
    second = attribution_mod.classify_opportunity_from_lineage_events_v1(events=list(reversed(events)))
    assert first["final_classification"] == "PARTIAL_FILL"
    assert first["final_classification"] == second["final_classification"]


def test_completeness_status_partial_for_submit_without_intent() -> None:
    result = attribution_mod.classify_opportunity_from_lineage_events_v1(
        events=[_event(stage="submit_attempted")]
    )
    assert result["completeness_status"] == "PARTIAL"


def test_lineage_event_id_is_stable_for_same_payload() -> None:
    event_a = _event(stage="intent_created")
    event_b = _event(stage="intent_created")
    assert event_a["event_id"] == event_b["event_id"]


def test_report_uses_lineage_spine_not_operator_summary(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)

    event = _event(stage="sleeve_run")
    path = attribution_mod._write_lineage_event_immutable(  # noqa: SLF001
        truth_root=truth_root,
        payload=event,
    )
    assert path.exists()

    fake_operator_summary = (
        truth_root
        / "reports"
        / "operator_day_authority_summary_v1"
        / DAY
        / "operator_day_authority_summary.v1.json"
    )
    fake_operator_summary.parent.mkdir(parents=True, exist_ok=True)
    fake_operator_summary.write_text(
        json.dumps({"startup_open_status": "OPEN_WITH_FILLS"}, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    result = attribution_mod.write_sleeve_intent_trade_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    report_payload = json.loads(Path(result.report_path).read_text(encoding="utf-8"))
    assert report_payload["evidence_status"]["primary_evidence_only"] is True
    assert report_payload["evidence_status"]["derived_sources_used"] == []
    assert report_payload["day_summary"]["classification_counts"]["RAN_NO_SIGNAL"] == 1


def test_materialize_lineage_from_no_intent_report(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    report_path = (
        truth_root
        / "reports"
        / "trading_day_intent_generation_v1"
        / DAY
        / "trading_day_intent_generation.v1.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_payload = {
        "day_utc": DAY,
        "produced_at_utc": f"{DAY}T13:30:00Z",
        "producer_results": [
            {
                "engine_id": SLEEVE,
                "status": "NO_INTENT",
                "reason_codes": ["NO_ENTRY_CONDITION_MET"],
            }
        ],
        "producer_topology": [{"engine_id": SLEEVE}],
    }
    report_path.write_text(json.dumps(report_payload, sort_keys=True) + "\n", encoding="utf-8")

    result = attribution_mod.materialize_opportunity_lineage_events_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    assert result.event_count >= 2
    loaded = attribution_mod.read_lineage_events_v1(truth_root=truth_root, day_utc=DAY)
    stages = {str(row["lineage_stage"]) for row in loaded}
    assert "sleeve_run" in stages
    assert "signal_eval" in stages


def test_materialize_lineage_and_write_attribution_is_idempotent(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    report_path = (
        truth_root
        / "reports"
        / "trading_day_intent_generation_v1"
        / DAY
        / "trading_day_intent_generation.v1.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_payload = {
        "day_utc": DAY,
        "produced_at_utc": f"{DAY}T13:30:00Z",
        "producer_results": [
            {
                "engine_id": SLEEVE,
                "status": "NO_INTENT",
                "reason_codes": ["NO_ENTRY_CONDITION_MET"],
            }
        ],
        "producer_topology": [{"engine_id": SLEEVE}],
    }
    report_path.write_text(json.dumps(report_payload, sort_keys=True) + "\n", encoding="utf-8")

    _, first = attribution_mod.materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    _, second = attribution_mod.materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    assert first.report_sha256 == second.report_sha256


def test_classification_precedence_constant_is_stable() -> None:
    assert attribution_mod.FINAL_CLASSIFICATION_PRECEDENCE == (
        "FILLED",
        "PARTIAL_FILL",
        "ORDER_CANCELLED",
        "ORDER_REJECTED",
        "ORDER_ACCEPTED_NO_FILL",
        "INTENT_SUBMITTED_NO_ORDER",
        "INTENT_CREATED_NOT_SUBMITTED",
        "RAN_SIGNAL_FILTERED",
        "RAN_NO_SIGNAL",
        "SLEEVE_NOT_RUN",
        "ATTRIBUTION_INCOMPLETE",
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _intent_payload(*, intent_id: str = OPPORTUNITY) -> dict[str, object]:
    return {
        "intent_id": intent_id,
        "created_at_utc": f"{DAY}T13:30:00Z",
        "engine": {"engine_id": SLEEVE},
    }


def _plan_payload(*, intent_id: str = OPPORTUNITY) -> dict[str, object]:
    return {
        "schema_id": "equity_order_plan",
        "schema_version": "v2",
        "engine_id": SLEEVE,
        "source_intent_id": intent_id,
        "plan_id": intent_id,
        "intent_hash": "hash",
        "qty_shares": 1,
        "protective_stop": {
            "order_type": "STOP",
            "stop_price": "611.92",
            "time_in_force": "DAY",
            "basis": "ENTRY_REFERENCE_PRICE",
            "stop_loss_bps": 1000,
        },
    }


def _bsr_payload(
    *,
    submission_id: str,
    status: str = "SUBMITTED",
    order_id: int | None = 1001,
    perm_id: int | None = 2002,
    error: dict[str, object] | None = None,
    protection_status: str = "UNKNOWN",
    order_linkage: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_id": "broker_submission_record",
        "schema_version": "v2",
        "submission_id": submission_id,
        "submitted_at_utc": f"{DAY}T14:00:00Z",
        "status": status,
        "broker_ids": {"order_id": order_id, "perm_id": perm_id},
        "error": error,
        "protection_status": protection_status,
    }
    if order_linkage is not None:
        payload["order_linkage"] = dict(order_linkage)
    return payload


def _execution_event_payload(*, status: str = "SUBMITTED") -> dict[str, object]:
    return {
        "schema_id": "execution_event_record",
        "schema_version": "v1",
        "event_time_utc": f"{DAY}T14:01:00Z",
        "status": status,
        "source_intent_id": OPPORTUNITY,
        "engine_id": SLEEVE,
    }


def _veto_payload(
    *,
    reason_code: str = "SUBMISSION_BOUNDARY_DENIED",
    reason_detail: str = "blocked",
    pointers: list[str] | None = None,
    intent_hash: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "boundary": "SUBMIT",
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "observed_at_utc": f"{DAY}T14:00:00Z",
    }
    if pointers:
        payload["pointers"] = list(pointers)
    if intent_hash:
        payload["inputs"] = {"intent_hash": intent_hash}
    return payload


def _authorization_payload(
    *,
    authorized_quantity: int = 0,
    reason_codes: list[str] | None = None,
    constraints: list[str] | None = None,
) -> dict[str, object]:
    return {
        "schema_id": "C2_AUTHORIZATION_V1",
        "schema_version": 1,
        "status": "REJECTED" if authorized_quantity <= 0 else "AUTHORIZED",
        "authorization": {
            "decision": "REJECTED" if authorized_quantity <= 0 else "AUTHORIZED",
            "authorized_quantity": authorized_quantity,
            "constraints": list(constraints or []),
        },
        "reason_codes": list(reason_codes or []),
    }


def _seed_submit_context(truth_root: Path) -> None:
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {
            "boundary_status": "AUTHORIZED",
            "submission_authorized": True,
        },
    )
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        {
            "control_state": {
                "authority_status": "GRANTED",
                "submission_authorized": True,
            }
        },
    )


def test_submit_decision_trace_attempted(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(subdir / "broker_submission_record.v2.json", _bsr_payload(submission_id="submission-1"))

    result = attribution_mod.materialize_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    assert result.trace_count == 1
    rows = attribution_mod.read_submit_decision_traces_v1(truth_root=truth_root, day_utc=DAY)
    assert rows[0]["submit_decision"] == "ATTEMPTED"
    assert rows[0]["submit_decision_reason_code"] == "SUBMISSION_ATTEMPT_EVIDENCE_PRESENT"


def test_submit_decision_trace_attempted_protected_submission(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(
        subdir / "broker_submission_record.v2.json",
        _bsr_payload(
            submission_id="submission-1",
            protection_status="PROTECTED",
            order_linkage={
                "parent_order_id": 1001,
                "parent_perm_id": 2002,
                "stop_order_id": 1002,
                "stop_perm_id": 2003,
                "take_profit_order_id": None,
                "take_profit_perm_id": None,
                "bracket_linkage": "PARENT_STOP",
                "oca_group": None,
            },
        ),
    )

    attribution_mod.materialize_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    rows = attribution_mod.read_submit_decision_traces_v1(truth_root=truth_root, day_utc=DAY)
    assert rows[0]["submit_decision"] == "ATTEMPTED"
    assert rows[0]["submit_decision_reason_code"] == "SUBMISSION_ATTEMPT_EVIDENCE_PRESENT_PROTECTED"


def test_submit_decision_trace_attempted_dry_run_no_broker_id(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(
        subdir / "broker_submission_record.v2.json",
        _bsr_payload(
            submission_id="submission-1",
            order_id=None,
            perm_id=None,
            error={
                "code": "DRY_RUN_NO_BROKER_ID",
                "message": "Governed submit dry-run mode recorded submission without broker order identifiers.",
            },
        ),
    )

    attribution_mod.materialize_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    rows = attribution_mod.read_submit_decision_traces_v1(truth_root=truth_root, day_utc=DAY)
    assert rows[0]["submit_decision"] == "ATTEMPTED"
    assert rows[0]["submit_decision_reason_code"] == "SUBMITTED_DRY_RUN_NO_BROKER_ID"


def test_submit_decision_trace_skipped_from_veto(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(subdir / "veto_record.v1.json", _veto_payload(reason_code="SUBMISSION_BOUNDARY_DENIED"))

    attribution_mod.materialize_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    rows = attribution_mod.read_submit_decision_traces_v1(truth_root=truth_root, day_utc=DAY)
    assert rows[0]["submit_decision"] == "SKIPPED"
    assert rows[0]["submit_decision_reason_code"] == "SUBMISSION_BOUNDARY_DENIED"


def test_submit_decision_trace_skipped_authz_veto_uses_specific_authorization_reason(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    auth_path = (
        truth_root
        / "engine_activity_v1"
        / "authorization_v1"
        / DAY
        / "hash.authorization.v1.json"
    )
    _write_json(
        auth_path,
        _authorization_payload(
            authorized_quantity=0,
            reason_codes=[
                "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS",
                "BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN",
                "BUNDLE_B_REQUESTED_QUANTITY_ZERO",
            ],
        ),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(
        subdir / "veto_record.v1.json",
        _veto_payload(
            reason_code="GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
            reason_detail="generic authz veto",
            pointers=[str(auth_path.resolve())],
            intent_hash="hash",
        ),
    )

    attribution_mod.materialize_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    rows = attribution_mod.read_submit_decision_traces_v1(truth_root=truth_root, day_utc=DAY)
    assert rows[0]["submit_decision"] == "SKIPPED"
    assert rows[0]["submit_decision_reason_code"] == "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS"
    assert "BUNDLE_B_REQUESTED_QUANTITY_ZERO" in rows[0]["submit_decision_reason_detail"]
    assert any(
        str(ref.get("artifact_path") or "") == str(auth_path.resolve())
        for ref in rows[0]["evidence_refs"]
    )


def test_submit_decision_trace_headroom_rejection_includes_required_vs_available_detail(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    auth_path = (
        truth_root
        / "engine_activity_v1"
        / "authorization_v1"
        / DAY
        / "hash.authorization.v1.json"
    )
    _write_json(
        auth_path,
        _authorization_payload(
            authorized_quantity=0,
            reason_codes=[
                "BUNDLE_B_HEADROOM_REJECTED",
            ],
            constraints=[
                "HEADROOM_REQUESTED_QUANTITY=1",
                "HEADROOM_AUTHORIZED_QUANTITY=0",
                "HEADROOM_REJECTED_QUANTITY=1",
                "HEADROOM_RISK_PER_UNIT_CENTS=100000",
                "HEADROOM_REQUIRED_RISK_CENTS=100000",
                "HEADROOM_AVAILABLE_CENTS=0",
                "HEADROOM_ALLOWED_CAPITAL_AT_RISK_CENTS=0",
                "HEADROOM_AVAILABLE_SLEEVE_CENTS=0",
                "HEADROOM_AVAILABLE_PORTFOLIO_CENTS=0",
            ],
        ),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(
        subdir / "veto_record.v1.json",
        _veto_payload(
            reason_code="GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
            reason_detail="generic authz veto",
            pointers=[str(auth_path.resolve())],
            intent_hash="hash",
        ),
    )

    attribution_mod.materialize_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    rows = attribution_mod.read_submit_decision_traces_v1(truth_root=truth_root, day_utc=DAY)
    assert rows[0]["submit_decision"] == "SKIPPED"
    assert rows[0]["submit_decision_reason_code"] == "BUNDLE_B_HEADROOM_REJECTED"
    assert "HEADROOM_REQUIRED_RISK_CENTS=100000" in rows[0]["submit_decision_reason_detail"]
    assert "HEADROOM_AVAILABLE_CENTS=0" in rows[0]["submit_decision_reason_detail"]


def test_submit_decision_trace_unknown_when_no_attempt_or_skip(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    attribution_mod.materialize_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    rows = attribution_mod.read_submit_decision_traces_v1(truth_root=truth_root, day_utc=DAY)
    assert rows[0]["submit_decision"] == "UNKNOWN"
    assert rows[0]["submit_decision_reason_code"] == "NO_SUBMIT_DECISION_EVIDENCE"


def test_submit_decision_trace_attempt_precedence_over_skip_evidence(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(subdir / "broker_submission_record.v2.json", _bsr_payload(submission_id="submission-1"))
    _write_json(subdir / "veto_record.v1.json", _veto_payload(reason_code="SUBMISSION_BOUNDARY_DENIED"))

    attribution_mod.materialize_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    rows = attribution_mod.read_submit_decision_traces_v1(truth_root=truth_root, day_utc=DAY)
    assert rows[0]["submit_decision"] == "ATTEMPTED"
    assert rows[0]["submit_decision_reason_code"] == "SUBMISSION_ATTEMPT_EVIDENCE_PRESENT"
    assert "historical skip evidence" in rows[0]["submit_decision_reason_detail"]


def test_submit_decision_resolver_prefers_attempted_over_stale_skipped() -> None:
    resolved = attribution_mod._resolve_submit_decision_trace_for_opportunity(
        [
            {
                "submit_decision": "SKIPPED",
                "submit_decision_reason_code": "GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
                "submit_decision_reason_detail": "older veto evidence",
                "observed_at_utc": f"{DAY}T13:00:00Z",
                "decision_id": "a",
                "_source_path": "/tmp/skipped.json",
            },
            {
                "submit_decision": "ATTEMPTED",
                "submit_decision_reason_code": "SUBMISSION_ATTEMPT_EVIDENCE_PRESENT",
                "submit_decision_reason_detail": "new submit evidence",
                "observed_at_utc": f"{DAY}T14:00:00Z",
                "decision_id": "b",
                "_source_path": "/tmp/attempted.json",
            },
        ]
    )
    assert resolved["submit_decision"] == "ATTEMPTED"
    assert resolved["submit_decision_reason_code"] == "SUBMISSION_ATTEMPT_EVIDENCE_PRESENT"
    assert resolved["trace_conflict"] is False
    assert resolved["trace_refs"] == ["/tmp/attempted.json"]


def test_submit_decision_resolver_prefers_concrete_decision_over_stale_unknown() -> None:
    resolved = attribution_mod._resolve_submit_decision_trace_for_opportunity(
        [
            {
                "submit_decision": "UNKNOWN",
                "submit_decision_reason_code": "NO_SUBMIT_DECISION_EVIDENCE",
                "submit_decision_reason_detail": "missing",
                "observed_at_utc": f"{DAY}T13:00:00Z",
                "decision_id": "a",
                "_source_path": "/tmp/unknown.json",
            },
            {
                "submit_decision": "SKIPPED",
                "submit_decision_reason_code": "GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
                "submit_decision_reason_detail": "explicit veto",
                "observed_at_utc": f"{DAY}T14:00:00Z",
                "decision_id": "b",
                "_source_path": "/tmp/skipped.json",
            },
        ]
    )
    assert resolved["submit_decision"] == "SKIPPED"
    assert resolved["submit_decision_reason_code"] == "GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED"
    assert resolved["trace_conflict"] is False


def test_submit_decision_resolver_prefers_specific_skipped_reason_over_generic() -> None:
    resolved = attribution_mod._resolve_submit_decision_trace_for_opportunity(
        [
            {
                "submit_decision": "SKIPPED",
                "submit_decision_reason_code": "GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
                "submit_decision_reason_detail": "generic veto",
                "observed_at_utc": f"{DAY}T14:00:00Z",
                "decision_id": "z",
                "_source_path": "/tmp/generic.json",
            },
            {
                "submit_decision": "SKIPPED",
                "submit_decision_reason_code": "BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN",
                "submit_decision_reason_detail": "specific authz cause",
                "observed_at_utc": f"{DAY}T13:00:00Z",
                "decision_id": "a",
                "_source_path": "/tmp/specific.json",
            },
        ]
    )
    assert resolved["submit_decision"] == "SKIPPED"
    assert resolved["submit_decision_reason_code"] == "BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN"
    assert resolved["trace_conflict"] is False
    assert resolved["trace_refs"] == ["/tmp/specific.json"]


def test_submit_decision_resolver_prefers_authz_reason_over_bundle_generic() -> None:
    resolved = attribution_mod._resolve_submit_decision_trace_for_opportunity(
        [
            {
                "submit_decision": "SKIPPED",
                "submit_decision_reason_code": "BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN",
                "submit_decision_reason_detail": "bundle reason",
                "observed_at_utc": f"{DAY}T14:00:00Z",
                "decision_id": "z",
                "_source_path": "/tmp/bundle.json",
            },
            {
                "submit_decision": "SKIPPED",
                "submit_decision_reason_code": "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS",
                "submit_decision_reason_detail": "authz reason",
                "observed_at_utc": f"{DAY}T13:00:00Z",
                "decision_id": "a",
                "_source_path": "/tmp/authz.json",
            },
        ]
    )
    assert resolved["submit_decision"] == "SKIPPED"
    assert resolved["submit_decision_reason_code"] == "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS"
    assert resolved["trace_conflict"] is False
    assert resolved["trace_refs"] == ["/tmp/authz.json"]


def test_attribution_uses_submit_decision_trace_for_precision(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(subdir / "broker_submission_record.v2.json", _bsr_payload(submission_id="submission-1"))
    _write_json(subdir / "execution_event_record.v1.json", _execution_event_payload(status="SUBMITTED"))
    _write_json(
        truth_root / "reports" / "trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json",
        {
            "day_utc": DAY,
            "produced_at_utc": f"{DAY}T13:30:00Z",
            "producer_topology": [{"engine_id": SLEEVE}],
            "producer_results": [{"engine_id": SLEEVE, "status": "INTENT_WRITTEN", "reason_codes": ["INTENT_OUTPUT_CREATED"]}],
        },
    )

    _, attribution = attribution_mod.materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    payload = json.loads(Path(attribution.report_path).read_text(encoding="utf-8"))
    row = next(item for item in payload["opportunities"] if item["opportunity_id"] == OPPORTUNITY)
    assert row["submit_decision"] == "ATTEMPTED"
    assert row["final_classification"] == "ORDER_ACCEPTED_NO_FILL"


def test_attribution_uses_specific_authz_reason_for_skipped_submit(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    _write_json(
        truth_root / "reports" / "trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json",
        {
            "day_utc": DAY,
            "produced_at_utc": f"{DAY}T13:30:00Z",
            "producer_topology": [{"engine_id": SLEEVE}],
            "producer_results": [{"engine_id": SLEEVE, "status": "INTENT_WRITTEN", "reason_codes": ["INTENT_OUTPUT_CREATED"]}],
        },
    )
    auth_path = (
        truth_root
        / "engine_activity_v1"
        / "authorization_v1"
        / DAY
        / "hash.authorization.v1.json"
    )
    _write_json(
        auth_path,
        _authorization_payload(
            authorized_quantity=0,
            reason_codes=[
                "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS",
                "BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN",
                "BUNDLE_B_REQUESTED_QUANTITY_ZERO",
            ],
        ),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(
        subdir / "veto_record.v1.json",
        _veto_payload(
            reason_code="GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
            reason_detail="generic authz veto",
            pointers=[str(auth_path.resolve())],
            intent_hash="hash",
        ),
    )

    _, attribution = attribution_mod.materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    payload = json.loads(Path(attribution.report_path).read_text(encoding="utf-8"))
    row = next(item for item in payload["opportunities"] if item["opportunity_id"] == OPPORTUNITY)
    assert row["submit_decision"] == "SKIPPED"
    assert row["submit_decision_reason_code"] == "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS"
    assert row["root_reason_code"] == "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS"


def test_attribution_uses_headroom_reason_for_skipped_submit(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    _write_json(
        truth_root / "reports" / "trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json",
        {
            "day_utc": DAY,
            "produced_at_utc": f"{DAY}T13:30:00Z",
            "producer_topology": [{"engine_id": SLEEVE}],
            "producer_results": [{"engine_id": SLEEVE, "status": "INTENT_WRITTEN", "reason_codes": ["INTENT_OUTPUT_CREATED"]}],
        },
    )
    auth_path = (
        truth_root
        / "engine_activity_v1"
        / "authorization_v1"
        / DAY
        / "hash.authorization.v1.json"
    )
    _write_json(
        auth_path,
        _authorization_payload(
            authorized_quantity=0,
            reason_codes=["BUNDLE_B_HEADROOM_REJECTED"],
            constraints=[
                "HEADROOM_REQUESTED_QUANTITY=1",
                "HEADROOM_AUTHORIZED_QUANTITY=0",
                "HEADROOM_REJECTED_QUANTITY=1",
                "HEADROOM_RISK_PER_UNIT_CENTS=100000",
                "HEADROOM_REQUIRED_RISK_CENTS=100000",
                "HEADROOM_AVAILABLE_CENTS=0",
                "HEADROOM_ALLOWED_CAPITAL_AT_RISK_CENTS=0",
                "HEADROOM_AVAILABLE_SLEEVE_CENTS=0",
                "HEADROOM_AVAILABLE_PORTFOLIO_CENTS=0",
            ],
        ),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(
        subdir / "veto_record.v1.json",
        _veto_payload(
            reason_code="GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
            reason_detail="generic authz veto",
            pointers=[str(auth_path.resolve())],
            intent_hash="hash",
        ),
    )

    _, attribution = attribution_mod.materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    payload = json.loads(Path(attribution.report_path).read_text(encoding="utf-8"))
    row = next(item for item in payload["opportunities"] if item["opportunity_id"] == OPPORTUNITY)
    assert row["submit_decision"] == "SKIPPED"
    assert row["submit_decision_reason_code"] == "BUNDLE_B_HEADROOM_REJECTED"
    assert row["root_reason_code"] == "BUNDLE_B_HEADROOM_REJECTED"
    assert "HEADROOM_REQUIRED_RISK_CENTS=100000" in row["submit_decision_reason_detail"]


def test_attribution_records_paper_discovery_mode_reason_when_authorization_carries_it(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    _write_json(
        truth_root / "reports" / "trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json",
        {
            "day_utc": DAY,
            "produced_at_utc": f"{DAY}T13:30:00Z",
            "producer_topology": [{"engine_id": SLEEVE}],
            "producer_results": [{"engine_id": SLEEVE, "status": "INTENT_WRITTEN", "reason_codes": ["INTENT_OUTPUT_CREATED"]}],
        },
    )
    auth_path = (
        truth_root
        / "engine_activity_v1"
        / "authorization_v1"
        / DAY
        / "hash.authorization.v1.json"
    )
    _write_json(
        auth_path,
        _authorization_payload(
            authorized_quantity=0,
            reason_codes=[
                "BUNDLE_B_HEADROOM_REJECTED",
                "PAPER_DISCOVERY_MODE_ACTIVE",
                "SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED",
                "PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED",
                "PAPER_DISCOVERY_MIN_EXECUTABLE_HEADROOM_ALLOWED",
            ],
            constraints=[
                "HEADROOM_REQUESTED_QUANTITY=1",
                "HEADROOM_AUTHORIZED_QUANTITY=0",
                "HEADROOM_REJECTED_QUANTITY=1",
                "HEADROOM_RISK_PER_UNIT_CENTS=100000",
                "HEADROOM_REQUIRED_RISK_CENTS=100000",
                "HEADROOM_AVAILABLE_CENTS=0",
            ],
        ),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(
        subdir / "veto_record.v1.json",
        _veto_payload(
            reason_code="GOV_SUBMIT_AUTHZ_NOT_AUTHORIZED",
            reason_detail="generic authz veto",
            pointers=[str(auth_path.resolve())],
            intent_hash="hash",
        ),
    )

    _, attribution = attribution_mod.materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    payload = json.loads(Path(attribution.report_path).read_text(encoding="utf-8"))
    row = next(item for item in payload["opportunities"] if item["opportunity_id"] == OPPORTUNITY)
    assert row["submit_decision"] == "SKIPPED"
    assert row["submit_decision_reason_code"] == "BUNDLE_B_HEADROOM_REJECTED"
    assert row["root_reason_code"] == "BUNDLE_B_HEADROOM_REJECTED"
    assert "PAPER_DISCOVERY_MODE_ACTIVE" in row["submit_decision_reason_detail"]
    assert "SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED" in row["submit_decision_reason_detail"]
    assert "PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED" in row["submit_decision_reason_detail"]
    assert "PAPER_DISCOVERY_MIN_EXECUTABLE_HEADROOM_ALLOWED" in row["submit_decision_reason_detail"]


def test_attempted_without_ack_is_intent_submitted_no_order(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(subdir / "broker_submission_record.v2.json", _bsr_payload(submission_id="submission-1", status="UNKNOWN"))

    _, attribution = attribution_mod.materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    payload = json.loads(Path(attribution.report_path).read_text(encoding="utf-8"))
    row = next(item for item in payload["opportunities"] if item["opportunity_id"] == OPPORTUNITY)
    assert row["submit_decision"] == "ATTEMPTED"
    assert row["final_classification"] == "INTENT_SUBMITTED_NO_ORDER"


def test_attempted_dry_run_no_broker_id_has_explicit_root_reason(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(
        subdir / "broker_submission_record.v2.json",
        _bsr_payload(
            submission_id="submission-1",
            order_id=None,
            perm_id=None,
            error={
                "code": "DRY_RUN_NO_BROKER_ID",
                "message": "Governed submit dry-run mode recorded submission without broker order identifiers.",
            },
        ),
    )

    _, attribution = attribution_mod.materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    payload = json.loads(Path(attribution.report_path).read_text(encoding="utf-8"))
    row = next(item for item in payload["opportunities"] if item["opportunity_id"] == OPPORTUNITY)
    assert row["submit_decision"] == "ATTEMPTED"
    assert row["submit_decision_reason_code"] == "SUBMITTED_DRY_RUN_NO_BROKER_ID"
    assert row["final_classification"] == "INTENT_SUBMITTED_NO_ORDER"
    assert row["root_reason_code"] == "SUBMITTED_DRY_RUN_NO_BROKER_ID"


def test_reconciliation_derived_signal_does_not_override_primary_submit_trace(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(subdir / "broker_submission_record.v2.json", _bsr_payload(submission_id="submission-1"))
    _write_json(
        truth_root / "reports" / "execution_reconciliation_v1" / DAY / "execution_reconciliation.v1.json",
        {"status": "NO_SUBMISSIONS_FOUND"},
    )

    attribution_mod.materialize_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    rows = attribution_mod.read_submit_decision_traces_v1(truth_root=truth_root, day_utc=DAY)
    assert rows[0]["submit_decision"] == "ATTEMPTED"


def test_attempted_rejected_maps_to_order_rejected(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    _write_json(subdir / "equity_order_plan.v2.json", _plan_payload())
    _write_json(subdir / "broker_submission_record.v2.json", _bsr_payload(submission_id="submission-1", status="REJECTED"))

    _, attribution = attribution_mod.materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    payload = json.loads(Path(attribution.report_path).read_text(encoding="utf-8"))
    row = next(item for item in payload["opportunities"] if item["opportunity_id"] == OPPORTUNITY)
    assert row["submit_decision"] == "ATTEMPTED"
    assert row["final_classification"] == "ORDER_REJECTED"


def test_unknown_submit_decision_keeps_intent_created_not_submitted(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_submit_context(truth_root)
    _write_json(
        truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json",
        _intent_payload(),
    )
    _, attribution = attribution_mod.materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=DAY,
        environment=ENV,
        emitted_by="test",
    )
    payload = json.loads(Path(attribution.report_path).read_text(encoding="utf-8"))
    row = next(item for item in payload["opportunities"] if item["opportunity_id"] == OPPORTUNITY)
    assert row["submit_decision"] == "UNKNOWN"
    assert row["final_classification"] == "INTENT_CREATED_NOT_SUBMITTED"
