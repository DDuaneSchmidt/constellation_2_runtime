from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from constellation_2.common.reconciled_trade_state_v1 import (
    OWN_AMBIGUOUS,
    OWN_CONSTELLATION,
    OWN_FOREIGN,
    RC_CONFLICTING_INCORPORATED_FILLS,
    RC_ENGINE_ATTRIBUTION_AMBIGUOUS,
    RC_ENGINE_ATTRIBUTION_LINEAGE_MISSING,
    RC_ENGINE_ATTRIBUTION_UNRESOLVED,
    RC_FOREIGN_MANUAL_ACTIVITY_SUSPECTED,
    RC_MIXED_OWNERSHIP_EVIDENCE,
    RC_POSITION_ORDER_FILL_INCONSISTENCY,
    derive_reconciled_trade_description_v1,
    derive_reconciliation_health_v1,
    materialize_reconciled_trade_state_v1,
)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _governed_identity() -> SimpleNamespace:
    return SimpleNamespace(
        authority_owner="execution_identity_binding_v1",
        sleeve_id="PRIMARY",
        environment="PAPER",
        account_id="DUO847203",
        client_id_orders=7,
        client_id_observer=179,
        host="127.0.0.1",
        port=4002,
    )


def _governed_roots(execution_root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        authority_owner="sleeve_execution_root_v1",
        execution_root_path=execution_root,
        sleeve_id="PRIMARY",
        mode="PAPER",
    )


def _contract_identity(symbol: str = "SPY") -> dict[str, str]:
    return {
        "symbol": symbol,
        "sec_type": "STK",
        "exchange": "SMART",
        "currency": "USD",
        "raw_summary": f"{symbol}-STK-SMART-USD",
    }


def _base_fact(
    *,
    schema_id: str,
    seq: int,
    observed_utc: str,
    account_id: str = "DUO847203",
    attribution_status: str = "ATTRIBUTED",
    attribution_reason_codes: list[str] | None = None,
) -> dict[str, object]:
    return {
        "schema_id": schema_id,
        "schema_version": "v1",
        "fact_record_id": _sha(f"{schema_id}:{seq}:fact"),
        "canonical_event_identity": _sha(f"{schema_id}:{seq}:canonical"),
        "duplicate_classification": "UNIQUE",
        "observed_utc": observed_utc,
        "observed_utc_source": "TEST",
        "normalized_utc": observed_utc,
        "raw_record_id": _sha(f"{schema_id}:{seq}:raw"),
        "journal_sequence_number": seq,
        "source_session_id": "session-a",
        "source_event_type": schema_id,
        "environment": "PAPER",
        "account_id": account_id,
        "client_id_observer": "179",
        "sleeve_id": "PRIMARY",
        "attribution_status": attribution_status,
        "attribution_reason_codes": attribution_reason_codes or [],
        "quality_status": "OK",
        "quality_reason_codes": [],
    }


def _order_fact(
    *,
    seq: int,
    observed_utc: str,
    order_id: str = "101",
    perm_id: str = "555001",
    action: str = "BUY",
    total_quantity: str = "10",
    attribution_status: str = "ATTRIBUTED",
) -> dict[str, object]:
    row = _base_fact(
        schema_id="observed_order_fact",
        seq=seq,
        observed_utc=observed_utc,
        attribution_status=attribution_status,
    )
    row.update(
        {
            "order_id": order_id,
            "perm_id": perm_id,
            "contract_identity": _contract_identity(),
            "order_summary": {
                "action": action,
                "total_quantity": total_quantity,
                "order_type": "LMT",
                "limit_price": "500.25",
                "raw_order": f"{action} {total_quantity}",
                "raw_order_state": "Submitted",
            },
        }
    )
    return row


def _order_status_fact(
    *,
    seq: int,
    observed_utc: str,
    order_id: str = "101",
    perm_id: str = "555001",
    status: str = "Submitted",
    filled_quantity: str = "0",
    remaining_quantity: str = "10",
) -> dict[str, object]:
    row = _base_fact(schema_id="observed_order_status_fact", seq=seq, observed_utc=observed_utc)
    row.update(
        {
            "order_id": order_id,
            "perm_id": perm_id,
            "status": status,
            "filled_quantity": filled_quantity,
            "remaining_quantity": remaining_quantity,
            "avg_fill_price": "0",
            "last_fill_price": "0",
        }
    )
    return row


def _fill_fact(
    *,
    seq: int,
    observed_utc: str,
    execution_id: str,
    order_id: str = "101",
    perm_id: str = "555001",
    fill_quantity: str = "10",
    fill_price: str = "500.30",
    side: str = "BOT",
    attribution_status: str = "ATTRIBUTED",
) -> dict[str, object]:
    row = _base_fact(
        schema_id="observed_fill_fact",
        seq=seq,
        observed_utc=observed_utc,
        attribution_status=attribution_status,
    )
    row.update(
        {
            "execution_id": execution_id,
            "order_id": order_id,
            "perm_id": perm_id,
            "contract_identity": _contract_identity(),
            "fill_quantity": fill_quantity,
            "fill_price": fill_price,
            "side": side,
            "commission": "1.25",
            "currency": "USD",
        }
    )
    return row


def _position_fact(
    *,
    seq: int,
    observed_utc: str,
    position_quantity: str,
    average_cost: str = "500.30",
    attribution_status: str = "ATTRIBUTED",
) -> dict[str, object]:
    row = _base_fact(
        schema_id="observed_position_fact",
        seq=seq,
        observed_utc=observed_utc,
        attribution_status=attribution_status,
    )
    row.update(
        {
            "contract_identity": _contract_identity(),
            "position_quantity": position_quantity,
            "average_cost": average_cost,
            "market_price": "501.00",
        }
    )
    return row


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _health_payload(
    *,
    execution_root: Path,
    day_utc: str,
    fact_counts: dict[str, int],
    current_state: str = "TRUSTED",
    downstream_trust_verdict: str = "TRUSTED",
    freshness_status: str = "FRESH",
    freshness_age_seconds: int = 0,
    blocker_codes: list[str] | None = None,
    degraded_codes: list[str] | None = None,
) -> dict[str, object]:
    raw_path = execution_root / "broker_fact_spine_v1" / "raw_journal" / day_utc / "broker_raw_evidence_envelope.v1.jsonl"
    fact_root = execution_root / "broker_fact_spine_v1" / "fact_ledger" / day_utc
    return {
        "schema_id": "broker_observation_health",
        "schema_version": "v1",
        "generated_utc": f"{day_utc}T14:31:00Z",
        "day_utc": day_utc,
        "evaluation_utc": f"{day_utc}T14:31:00Z",
        "authority_owner": "broker_observation_health_v1",
        "trust_rule_owner": "broker_observation_health_v1",
        "execution_root_path": str(execution_root),
        "raw_journal_ref": {
            "artifact_path": str(raw_path),
            "artifact_sha256": _sha(raw_path.read_text(encoding="utf-8")),
        },
        "fact_ledger_refs": {
            "observation_session_fact": {
                "artifact_path": str(fact_root / "observation_session_fact.v1.jsonl"),
                "artifact_sha256": _sha((fact_root / "observation_session_fact.v1.jsonl").read_text(encoding="utf-8")),
            },
            "observed_order_fact": {
                "artifact_path": str(fact_root / "observed_order_fact.v1.jsonl"),
                "artifact_sha256": _sha((fact_root / "observed_order_fact.v1.jsonl").read_text(encoding="utf-8")),
            },
            "observed_order_status_fact": {
                "artifact_path": str(fact_root / "observed_order_status_fact.v1.jsonl"),
                "artifact_sha256": _sha((fact_root / "observed_order_status_fact.v1.jsonl").read_text(encoding="utf-8")),
            },
            "observed_fill_fact": {
                "artifact_path": str(fact_root / "observed_fill_fact.v1.jsonl"),
                "artifact_sha256": _sha((fact_root / "observed_fill_fact.v1.jsonl").read_text(encoding="utf-8")),
            },
            "observed_position_fact": {
                "artifact_path": str(fact_root / "observed_position_fact.v1.jsonl"),
                "artifact_sha256": _sha((fact_root / "observed_position_fact.v1.jsonl").read_text(encoding="utf-8")),
            },
        },
        "current_state": current_state,
        "downstream_trust_verdict": downstream_trust_verdict,
        "downstream_consumption_posture": (
            "NORMAL_CONSUMPTION"
            if downstream_trust_verdict == "TRUSTED"
            else "DEGRADED_ONLY"
            if downstream_trust_verdict == "DEGRADED"
            else "FAIL_CLOSED"
        ),
        "may_consume_normally": downstream_trust_verdict == "TRUSTED",
        "may_consume_with_degraded_posture": downstream_trust_verdict in {"TRUSTED", "DEGRADED"},
        "must_fail_closed": downstream_trust_verdict == "BLOCKED",
        "freshness_status": freshness_status,
        "freshness_age_seconds": freshness_age_seconds,
        "session_status": "HEALTHY",
        "sequence_status": "OK",
        "replay_status": "NOT_OBSERVED",
        "reconnect_status": "NONE",
        "gap_status": "NONE",
        "attribution_status": "ATTRIBUTED",
        "event_identity_rule_version": "broker_fact_identity_v1",
        "blocker_codes": blocker_codes or [],
        "degraded_codes": degraded_codes or [],
        "counts": {
            "raw_record_count": sum(fact_counts.values()),
            "observation_session_fact_count": 0,
            "observed_order_fact_count": fact_counts["observed_order_fact"],
            "observed_order_status_fact_count": fact_counts["observed_order_status_fact"],
            "observed_fill_fact_count": fact_counts["observed_fill_fact"],
            "observed_position_fact_count": fact_counts["observed_position_fact"],
            "attributed_raw_record_count": sum(fact_counts.values()),
            "partial_raw_record_count": 0,
            "ambiguous_raw_record_count": 0,
            "foreign_raw_record_count": 0,
            "unresolved_raw_record_count": 0,
            "duplicate_fact_count": 0,
            "replay_overlap_fact_count": 0,
            "conflicting_duplicate_fact_count": 0,
            "replay_session_fact_count": 0,
            "replay_uncertain_fact_count": 0,
        },
        "summary": "synthetic test payload",
        "recommended_operator_action": "No operator action required.",
    }


def _prepare_core1(
    *,
    execution_root: Path,
    day_utc: str,
    rows_by_schema: dict[str, list[dict[str, object]]],
    health_overrides: dict[str, object] | None = None,
) -> None:
    raw_path = execution_root / "broker_fact_spine_v1" / "raw_journal" / day_utc / "broker_raw_evidence_envelope.v1.jsonl"
    _write_jsonl(raw_path, [{"schema_id": "broker_raw_evidence_envelope", "raw_record_id": _sha("raw-1")}])
    fact_root = execution_root / "broker_fact_spine_v1" / "fact_ledger" / day_utc
    _write_jsonl(fact_root / "observation_session_fact.v1.jsonl", [])
    for schema_id in (
        "observed_order_fact",
        "observed_order_status_fact",
        "observed_fill_fact",
        "observed_position_fact",
    ):
        _write_jsonl(fact_root / f"{schema_id}.v1.jsonl", rows_by_schema.get(schema_id, []))
    counts = {
        "observed_order_fact": len(rows_by_schema.get("observed_order_fact", [])),
        "observed_order_status_fact": len(rows_by_schema.get("observed_order_status_fact", [])),
        "observed_fill_fact": len(rows_by_schema.get("observed_fill_fact", [])),
        "observed_position_fact": len(rows_by_schema.get("observed_position_fact", [])),
    }
    payload = _health_payload(execution_root=execution_root, day_utc=day_utc, fact_counts=counts)
    for key, value in (health_overrides or {}).items():
        payload[key] = value
    health_path = execution_root / "reports" / "broker_observation_health_v1" / day_utc / "broker_observation_health.v1.json"
    _write_json(health_path, payload)


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_execution_stream_record(
    *,
    execution_root: Path,
    day_utc: str,
    filename: str,
    engine_id: str,
    source_intent_id: str,
    order_id: int | None,
    perm_id: int | None,
) -> None:
    payload = {
        "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
        "schema_version": 1,
        "engine_id": engine_id,
        "source_intent_id": source_intent_id,
        "intent_sha256": "a" * 64,
        "broker_ids": {"order_id": order_id, "perm_id": perm_id},
    }
    _write_json(execution_root / "execution_stream_v1" / day_utc / filename, payload)


def _write_submission_evidence(
    *,
    execution_root: Path,
    day_utc: str,
    submission_id: str,
    engine_id: str,
    source_intent_id: str,
    order_id: int,
    perm_id: int,
) -> None:
    submission_dir = execution_root / "execution_evidence_v1" / "submissions" / day_utc / submission_id
    _write_json(
        submission_dir / "equity_order_plan.v1.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v1",
            "engine_id": engine_id,
            "source_intent_id": source_intent_id,
            "intent_sha256": "b" * 64,
        },
    )
    _write_json(
        submission_dir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": submission_id,
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
        },
    )


def test_trade_identity_and_incorporated_state_are_deterministic(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    day_utc = "2026-04-14"
    _prepare_core1(
        execution_root=execution_root,
        day_utc=day_utc,
        rows_by_schema={
            "observed_order_fact": [_order_fact(seq=1, observed_utc="2026-04-14T14:30:03Z")],
            "observed_order_status_fact": [_order_status_fact(seq=2, observed_utc="2026-04-14T14:30:04Z")],
            "observed_fill_fact": [_fill_fact(seq=3, observed_utc="2026-04-14T14:30:05Z", execution_id="E-1")],
            "observed_position_fact": [_position_fact(seq=4, observed_utc="2026-04-14T14:30:06Z", position_quantity="10")],
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    first = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc="2026-04-14T14:31:00Z",
    )
    second = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc="2026-04-14T14:31:00Z",
    )

    assert first.materialization_set_id == second.materialization_set_id
    assert len(first.trade_artifacts) == 1
    assert first.trade_artifacts[0].trade_identity_id == second.trade_artifacts[0].trade_identity_id
    state = _load_json(first.trade_artifacts[0].incorporated_state_path)
    assert state["ownership_classification"] == OWN_CONSTELLATION
    assert state["current_quantity"] == "10"
    assert state["side"] == "LONG"
    assert first.summary["counts_by_health"]["TRUSTED"] == 1


def test_engine_attribution_is_persisted_from_execution_stream_lineage(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    day_utc = "2026-04-14"
    _prepare_core1(
        execution_root=execution_root,
        day_utc=day_utc,
        rows_by_schema={
            "observed_order_fact": [_order_fact(seq=1, observed_utc="2026-04-14T14:30:03Z")],
            "observed_order_status_fact": [_order_status_fact(seq=2, observed_utc="2026-04-14T14:30:04Z")],
            "observed_fill_fact": [_fill_fact(seq=3, observed_utc="2026-04-14T14:30:05Z", execution_id="E-1")],
            "observed_position_fact": [_position_fact(seq=4, observed_utc="2026-04-14T14:30:06Z", position_quantity="10")],
        },
    )
    _write_execution_stream_record(
        execution_root=execution_root,
        day_utc=day_utc,
        filename="record.execution_event_stream_record.v1.json",
        engine_id="C2_TREND_EQ_PRIMARY_V1",
        source_intent_id="intent-123",
        order_id=101,
        perm_id=555001,
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialization = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc="2026-04-14T14:31:00Z",
    )

    identity = _load_json(materialization.trade_artifacts[0].trade_identity_path)
    state = _load_json(materialization.trade_artifacts[0].incorporated_state_path)
    assert identity["native_engine_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert identity["strategy_engine_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert identity["source_intent_id"] == "intent-123"
    assert identity["attribution_diagnostics"]["status"] == "RESOLVED"
    assert state["engine_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert state["attribution_diagnostics"]["status"] == "RESOLVED"


def test_engine_attribution_is_resolved_from_submission_evidence_alias(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    day_utc = "2026-04-14"
    _prepare_core1(
        execution_root=execution_root,
        day_utc=day_utc,
        rows_by_schema={
            "observed_order_fact": [_order_fact(seq=1, observed_utc="2026-04-14T14:30:03Z")],
            "observed_order_status_fact": [_order_status_fact(seq=2, observed_utc="2026-04-14T14:30:04Z")],
            "observed_fill_fact": [_fill_fact(seq=3, observed_utc="2026-04-14T14:30:05Z", execution_id="E-1")],
            "observed_position_fact": [_position_fact(seq=4, observed_utc="2026-04-14T14:30:06Z", position_quantity="10")],
        },
    )
    _write_submission_evidence(
        execution_root=execution_root,
        day_utc=day_utc,
        submission_id="s" * 64,
        engine_id="C2_TREND_EQ_PRIMARY_V1",
        source_intent_id="intent-submission",
        order_id=101,
        perm_id=555001,
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialization = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        evaluation_utc="2026-04-14T14:31:00Z",
    )
    identity = _load_json(materialization.trade_artifacts[0].trade_identity_path)
    assert identity["engine_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert identity["source_intent_id"] == "intent-submission"


def test_missing_engine_attribution_remains_unresolved_without_fake_defaults(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    day_utc = "2026-04-16"
    _prepare_core1(
        execution_root=execution_root,
        day_utc=day_utc,
        rows_by_schema={
            "observed_position_fact": [
                _position_fact(seq=1, observed_utc="2026-04-16T05:15:07Z", position_quantity="3"),
            ],
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialization = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        evaluation_utc="2026-04-16T14:31:00Z",
    )
    identity = _load_json(materialization.trade_artifacts[0].trade_identity_path)
    assert identity["engine_id"] == ""
    assert identity["native_engine_id"] == ""
    assert identity["strategy_engine_id"] == ""
    reason_codes = set(identity["attribution_diagnostics"]["reason_codes"])
    assert RC_ENGINE_ATTRIBUTION_UNRESOLVED in reason_codes
    assert RC_ENGINE_ATTRIBUTION_LINEAGE_MISSING in reason_codes


def test_ambiguous_engine_attribution_does_not_pick_default(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    day_utc = "2026-04-14"
    _prepare_core1(
        execution_root=execution_root,
        day_utc=day_utc,
        rows_by_schema={
            "observed_order_fact": [_order_fact(seq=1, observed_utc="2026-04-14T14:30:03Z")],
            "observed_order_status_fact": [_order_status_fact(seq=2, observed_utc="2026-04-14T14:30:04Z")],
            "observed_fill_fact": [_fill_fact(seq=3, observed_utc="2026-04-14T14:30:05Z", execution_id="E-1")],
            "observed_position_fact": [_position_fact(seq=4, observed_utc="2026-04-14T14:30:06Z", position_quantity="10")],
        },
    )
    _write_execution_stream_record(
        execution_root=execution_root,
        day_utc=day_utc,
        filename="record-a.execution_event_stream_record.v1.json",
        engine_id="C2_TREND_EQ_PRIMARY_V1",
        source_intent_id="intent-a",
        order_id=101,
        perm_id=555001,
    )
    _write_execution_stream_record(
        execution_root=execution_root,
        day_utc=day_utc,
        filename="record-b.execution_event_stream_record.v1.json",
        engine_id="C2_INTENT_SIMULATOR_V1",
        source_intent_id="intent-b",
        order_id=101,
        perm_id=555001,
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialization = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        evaluation_utc="2026-04-14T14:31:00Z",
    )
    identity = _load_json(materialization.trade_artifacts[0].trade_identity_path)
    assert identity["engine_id"] == ""
    assert RC_ENGINE_ATTRIBUTION_AMBIGUOUS in set(identity["attribution_diagnostics"]["reason_codes"])


def test_foreign_manual_path_is_explicit_and_blocked(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    day_utc = "2026-04-14"
    _prepare_core1(
        execution_root=execution_root,
        day_utc=day_utc,
        rows_by_schema={
            "observed_fill_fact": [
                _fill_fact(
                    seq=1,
                    observed_utc="2026-04-14T14:30:05Z",
                    execution_id="E-FOREIGN",
                    attribution_status="FOREIGN_OR_MANUAL_SUSPECTED",
                )
            ],
            "observed_position_fact": [
                _position_fact(
                    seq=2,
                    observed_utc="2026-04-14T14:30:06Z",
                    position_quantity="5",
                    attribution_status="FOREIGN_OR_MANUAL_SUSPECTED",
                )
            ],
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialization = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        evaluation_utc="2026-04-14T14:31:00Z",
    )

    state = _load_json(materialization.trade_artifacts[0].incorporated_state_path)
    health = _load_json(materialization.trade_artifacts[0].health_path)
    assert state["ownership_classification"] == OWN_FOREIGN
    assert RC_FOREIGN_MANUAL_ACTIVITY_SUSPECTED in state["blocker_codes"]
    assert health["current_state"] == "BLOCKED"


def test_ambiguous_ownership_path_is_explicit(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    day_utc = "2026-04-14"
    _prepare_core1(
        execution_root=execution_root,
        day_utc=day_utc,
        rows_by_schema={
            "observed_order_fact": [_order_fact(seq=1, observed_utc="2026-04-14T14:30:03Z")],
            "observed_fill_fact": [
                _fill_fact(
                    seq=2,
                    observed_utc="2026-04-14T14:30:04Z",
                    execution_id="E-MIXED",
                    attribution_status="FOREIGN_OR_MANUAL_SUSPECTED",
                )
            ],
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialization = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        evaluation_utc="2026-04-14T14:31:00Z",
    )

    identity = _load_json(materialization.trade_artifacts[0].trade_identity_path)
    health = _load_json(materialization.trade_artifacts[0].health_path)
    assert identity["ownership_classification"] == OWN_AMBIGUOUS
    assert RC_MIXED_OWNERSHIP_EVIDENCE in identity["blocker_codes"]
    assert health["current_state"] == "BLOCKED"


def test_conflicting_facts_block_current_truth_and_preserve_provenance(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    day_utc = "2026-04-14"
    _prepare_core1(
        execution_root=execution_root,
        day_utc=day_utc,
        rows_by_schema={
            "observed_order_fact": [_order_fact(seq=1, observed_utc="2026-04-14T14:30:03Z")],
            "observed_fill_fact": [_fill_fact(seq=2, observed_utc="2026-04-14T14:30:04Z", execution_id="E-DRIFT")],
            "observed_position_fact": [_position_fact(seq=3, observed_utc="2026-04-14T14:30:05Z", position_quantity="5")],
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialization = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        evaluation_utc="2026-04-14T14:31:00Z",
    )

    state = _load_json(materialization.trade_artifacts[0].incorporated_state_path)
    health = _load_json(materialization.trade_artifacts[0].health_path)
    provenance = _load_json(materialization.trade_artifacts[0].provenance_path)
    assert RC_POSITION_ORDER_FILL_INCONSISTENCY in state["blocker_codes"]
    assert health["current_state"] == "BLOCKED"
    assert provenance["prior_state_change_summary"]["change_status"] == "INITIAL_MATERIALIZATION"
    assert provenance["incorporated_fact_refs"]
    assert provenance["upstream_core1_evidence_refs"]["health_ref"]["artifact_path"]


def test_open_close_continuity_creates_new_trade_identity(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    day_utc = "2026-04-14"
    _prepare_core1(
        execution_root=execution_root,
        day_utc=day_utc,
        rows_by_schema={
            "observed_fill_fact": [
                _fill_fact(seq=1, observed_utc="2026-04-14T14:30:01Z", execution_id="E-OPEN", fill_quantity="10", side="BOT"),
                _fill_fact(seq=2, observed_utc="2026-04-14T14:30:02Z", execution_id="E-CLOSE", fill_quantity="10", side="SLD"),
                _fill_fact(seq=3, observed_utc="2026-04-14T14:30:03Z", execution_id="E-REOPEN", fill_quantity="5", side="BOT"),
            ],
            "observed_position_fact": [
                _position_fact(seq=4, observed_utc="2026-04-14T14:30:02.500000Z", position_quantity="0"),
                _position_fact(seq=5, observed_utc="2026-04-14T14:30:05Z", position_quantity="5"),
            ],
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialization = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        evaluation_utc="2026-04-14T14:31:00Z",
    )

    assert len(materialization.trade_artifacts) == 2
    identities = [_load_json(item.trade_identity_path) for item in materialization.trade_artifacts]
    segment_indexes = sorted(identity["open_close_continuity"]["segment_index"] for identity in identities)
    assert segment_indexes == [1, 2]


def test_description_is_derived_only_and_no_action_logic_leaks() -> None:
    state_payload = {
        "materialization_set_id": _sha("set"),
        "day_utc": "2026-04-14",
        "evaluation_utc": "2026-04-14T14:31:00Z",
        "trade_identity_ref": {"trade_identity_id": _sha("trade")},
        "current_quantity": "10",
        "current_working_orders": [
            {
                "order_key": "201|777001",
                "order_id": "201",
                "perm_id": "777001",
                "status": "Submitted",
                "action": "SELL",
                "total_quantity": "10",
                "filled_quantity": "0",
                "remaining_quantity": "10",
                "observed_utc": "2026-04-14T14:30:10Z",
                "fact_record_ids": [_sha("working-order")],
            }
        ],
        "incorporated_fills": [{"fact_record_ids": [_sha("fill-a")]}],
        "ambiguity_state": "NONE",
        "blocker_codes": [],
        "degraded_codes": [],
        "drift_basis": {"reason_codes": []},
        "freshness_basis": {"age_seconds": 0},
    }
    health_payload = derive_reconciliation_health_v1(state_payload)
    description = derive_reconciled_trade_description_v1(state_payload=state_payload, health_payload=health_payload)

    assert description["derived_from_incorporated_state"] is True
    assert description["lifecycle_status"] == "OPEN_LONG_WITH_WORKING_EXIT"
    assert description["downstream_posture"] == "SAFE"
    joined = json.dumps(description, sort_keys=True)
    assert "authorize" not in joined
    assert "submit" not in joined
    assert RC_CONFLICTING_INCORPORATED_FILLS not in joined


def test_freshness_age_clamps_to_zero_when_evaluation_precedes_latest_fact(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    day_utc = "2026-04-08"
    _prepare_core1(
        execution_root=execution_root,
        day_utc=day_utc,
        rows_by_schema={
            "observed_order_fact": [_order_fact(seq=1, observed_utc="2026-04-08T20:29:10Z")],
            "observed_order_status_fact": [_order_status_fact(seq=2, observed_utc="2026-04-08T20:29:11Z")],
            "observed_fill_fact": [_fill_fact(seq=3, observed_utc="2026-04-08T20:29:12Z", execution_id="E-LATE")],
            "observed_position_fact": [_position_fact(seq=4, observed_utc="2026-04-08T20:29:13Z", position_quantity="10")],
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.reconciled_trade_state_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialization = materialize_reconciled_trade_state_v1(
        repo_root=Path("/home/node/constellation"),
        day_utc=day_utc,
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc="2026-04-08T16:00:00Z",
    )

    state = _load_json(materialization.trade_artifacts[0].incorporated_state_path)
    health = _load_json(materialization.trade_artifacts[0].health_path)
    assert state["freshness_basis"]["age_seconds"] == 0
    assert health["freshness_status"] == "FRESH"
