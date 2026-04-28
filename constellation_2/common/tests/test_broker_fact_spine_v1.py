from __future__ import annotations

import json
import runpy
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.broker_fact_spine_v1 import (
    BrokerRawEvidenceJournalWriterV1,
    DOWNSTREAM_DEGRADED_ONLY,
    DOWNSTREAM_FAIL_CLOSED,
    DOWNSTREAM_NORMAL,
    DUPLICATE_CONFLICT,
    DUPLICATE_EVENT,
    DUPLICATE_REPLAY_OVERLAP,
    GAP_NONE,
    GAP_SUSPECTED,
    LEGACY_BOUNDARY_HARDENED,
    LEGACY_BOUNDARY_LEGACY,
    RAW_ATTRIBUTION_AMBIGUOUS,
    RAW_ATTRIBUTION_ATTRIBUTED,
    RAW_ATTRIBUTION_FOREIGN,
    RAW_ATTRIBUTION_PARTIAL,
    RAW_ATTRIBUTION_UNRESOLVED,
    RC_CORE1_LEGACY_RUNTIME_DAY_NON_CONSUMABLE,
    RC_CORE2_REQUIRED_ARTIFACT_MISSING,
    RC_FOREIGN_MANUAL_SUSPECTED,
    RC_RECONNECT_RECOVERED,
    RC_REPLAY_IN_PROGRESS,
    READINESS_BLOCKED,
    READINESS_READY,
    RECONNECT_RECOVERED,
    REPLAYING,
    REPLAY_CLASSIFICATION_DUPLICATE_CALLBACK,
    REPLAY_CLASSIFICATION_REPLAY_OVERLAP,
    TRUST_BLOCKED,
    TRUST_DEGRADED,
    TRUST_TRUSTED,
    build_core1_pre_core2_readiness_payload_v1,
    build_raw_evidence_envelope_from_payload_v1,
    materialize_broker_fact_spine_v1,
    materialize_core1_pre_core2_readiness_v1,
    normalize_broker_fact_records_v1,
    resolve_broker_fact_spine_audit_path,
    resolve_broker_raw_journal_path,
    resolve_fact_ledger_path,
)


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


def _legacy_event(
    *,
    event_type: str,
    received_utc: str = "",
    args: list[str],
    client_id: int = 179,
    environment: str = "PAPER",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_id": "BROKER_EVENT_RAW",
        "schema_version": 1,
        "sequence_number": 1,
        "broker": {
            "client_id": client_id,
            "environment": environment,
            "name": "INTERACTIVE_BROKERS",
        },
        "event_type": event_type,
        "ib_fields": {"args": [{"value": item} for item in args]},
        "sha256": "a" * 64,
    }
    if received_utc:
        payload["received_utc"] = received_utc
    return payload


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def test_broker_raw_evidence_journal_writer_is_append_only(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )

    writer = BrokerRawEvidenceJournalWriterV1(
        repo_root=Path("/home/node/constellation"),
        execution_root_path=execution_root,
        day_utc="2026-04-14",
        environment="PAPER",
        sleeve_id="PRIMARY",
        source_adapter_name="ib_execution_observer_v1",
        source_session_id="session-1",
        source_path="observer://interactive_brokers/PRIMARY/PAPER",
    )
    writer.write_payload(
        _legacy_event(
            event_type="starting",
            received_utc="2026-04-14T14:30:00Z",
            args=["host=127.0.0.1", "port=4002", "clientId=179"],
        )
    )
    writer.write_payload(
        _legacy_event(
            event_type="nextValidId",
            received_utc="2026-04-14T14:30:01Z",
            args=["orderId=101"],
        )
    )
    writer.close()

    journal_path = execution_root / "broker_fact_spine_v1" / "raw_journal" / "2026-04-14" / "broker_raw_evidence_envelope.v1.jsonl"
    rows = [json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [row["journal_sequence_number"] for row in rows] == [1, 2]
    assert rows[0]["source_event_type"] == "starting"
    assert rows[0]["source_event_identity"]
    assert rows[0]["ordering_basis"] == "OBSERVED_UTC"


def test_materialize_broker_fact_spine_derives_normalized_fact_ledgers_and_trust_dependency(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    source_path = tmp_path / "source.jsonl"
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=30)
    day_utc = base.date().isoformat()
    stamps = [(base + timedelta(seconds=index)).isoformat().replace("+00:00", "Z") for index in range(11)]
    _write_jsonl(
        source_path,
        [
            _legacy_event(event_type="starting", received_utc=stamps[0], args=["host=127.0.0.1", "port=4002", "clientId=179"]),
            _legacy_event(event_type="nextValidId", received_utc=stamps[1], args=["orderId=101"]),
            _legacy_event(event_type="reqAllOpenOrders", received_utc=stamps[2], args=["reqAllOpenOrders()"]),
            _legacy_event(
                event_type="openOrder",
                received_utc=stamps[3],
                args=[
                    "orderId=101", "permId=555001", "symbol=SPY", "secType=STK", "exchange=SMART", "currency=USD",
                    "contract=SPY-STK-SMART-USD", "action=BUY", "totalQuantity=10", "orderType=LMT",
                    "lmtPrice=500.25", "order=BUY 10 LMT 500.25", "orderState=Submitted",
                ],
            ),
            _legacy_event(
                event_type="orderStatus",
                received_utc=stamps[4],
                args=[
                    "orderId=101", "status=Submitted", "filled=0", "remaining=10", "avgFillPrice=0",
                    "permId=555001", "parentId=0", "lastFillPrice=0", "clientId=7", "whyHeld=", "mktCapPrice=0",
                ],
            ),
            _legacy_event(
                event_type="execDetails",
                received_utc=stamps[5],
                args=[
                    "reqId=9001", "orderId=101", "permId=555001", "execId=E-PRIMARY-0001", "shares=10",
                    "price=500.30", "side=BOT", "symbol=SPY", "secType=STK", "exchange=SMART",
                    "currency=USD", "contract=SPY-STK-SMART-USD",
                ],
            ),
            _legacy_event(
                event_type="commissionReport",
                received_utc=stamps[6],
                args=[
                    "orderId=101", "permId=555001", "execId=E-PRIMARY-0001", "commission=1.25",
                    "currency=USD", "symbol=SPY", "secType=STK", "exchange=SMART", "contract=SPY-STK-SMART-USD",
                ],
            ),
            _legacy_event(
                event_type="position",
                received_utc=stamps[7],
                args=[
                    "account=DUO847203", "symbol=SPY", "secType=STK", "exchange=SMART", "currency=USD",
                    "contract=SPY-STK-SMART-USD", "position=10", "avgCost=500.30", "marketPrice=501.00",
                ],
            ),
            _legacy_event(event_type="openOrderEnd", received_utc=stamps[8], args=["openOrderEnd()"]),
            _legacy_event(event_type="execDetailsEnd", received_utc=stamps[9], args=["reqId=9001"]),
            _legacy_event(event_type="bootstrapHandshakeComplete", received_utc=stamps[10], args=["timeoutSeconds=15"]),
        ],
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    bundle = materialize_broker_fact_spine_v1(
        repo_root=Path("/home/node/constellation"),
        truth_root=tmp_path / "truth",
        day_utc=day_utc,
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc=stamps[10],
        source_path=source_path,
    )
    second_bundle = materialize_broker_fact_spine_v1(
        repo_root=Path("/home/node/constellation"),
        truth_root=tmp_path / "truth",
        day_utc=day_utc,
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc=stamps[10],
        source_path=source_path,
    )

    raw_rows = [json.loads(line) for line in bundle.raw_journal_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    order_rows = [json.loads(line) for line in second_bundle.fact_ledger_paths["observed_order_fact"].read_text(encoding="utf-8").splitlines() if line.strip()]
    fill_rows = [json.loads(line) for line in second_bundle.fact_ledger_paths["observed_fill_fact"].read_text(encoding="utf-8").splitlines() if line.strip()]
    health = json.loads(bundle.health_path.read_text(encoding="utf-8"))
    trust_dependency = json.loads(bundle.trust_dependency_path.read_text(encoding="utf-8"))
    audit = json.loads(bundle.audit_path.read_text(encoding="utf-8"))

    assert len(raw_rows) == 11
    assert len(order_rows) == 1
    assert len(fill_rows) == 2
    assert health["current_state"] == TRUST_TRUSTED
    assert health["downstream_consumption_posture"] == DOWNSTREAM_NORMAL
    assert trust_dependency["may_consume_normally"] is True
    assert trust_dependency["must_fail_closed"] is False
    assert audit["derived_only"] is True
    assert audit["trust_dependency_ref"]["artifact_path"].endswith("broker_observation_trust_dependency.v1.json")
    assert second_bundle.summary["raw_records_skipped_existing"] == 11
    assert bundle.summary["trust_verdict"] == TRUST_TRUSTED
    assert "lifecycle_state" not in json.dumps(order_rows[0], sort_keys=True)
    assert "action_authorized" not in json.dumps(health, sort_keys=True)


def test_run_broker_fact_spine_entrypoint_bootstraps_repo_import_and_writes_audit(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    source_path = tmp_path / "source.jsonl"
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=30)
    day_utc = base.date().isoformat()
    stamps = [(base + timedelta(seconds=index)).isoformat().replace("+00:00", "Z") for index in range(11)]
    _write_jsonl(
        source_path,
        [
            _legacy_event(event_type="starting", received_utc=stamps[0], args=["host=127.0.0.1", "port=4002", "clientId=179"]),
            _legacy_event(event_type="nextValidId", received_utc=stamps[1], args=["orderId=101"]),
            _legacy_event(event_type="reqAllOpenOrders", received_utc=stamps[2], args=["reqAllOpenOrders()"]),
            _legacy_event(
                event_type="openOrder",
                received_utc=stamps[3],
                args=[
                    "orderId=101", "permId=555001", "symbol=SPY", "secType=STK", "exchange=SMART", "currency=USD",
                    "contract=SPY-STK-SMART-USD", "action=BUY", "totalQuantity=10", "orderType=LMT",
                    "lmtPrice=500.25", "order=BUY 10 LMT 500.25", "orderState=Submitted",
                ],
            ),
            _legacy_event(
                event_type="orderStatus",
                received_utc=stamps[4],
                args=[
                    "orderId=101", "status=Submitted", "filled=0", "remaining=10", "avgFillPrice=0",
                    "permId=555001", "parentId=0", "lastFillPrice=0", "clientId=7", "whyHeld=", "mktCapPrice=0",
                ],
            ),
            _legacy_event(
                event_type="execDetails",
                received_utc=stamps[5],
                args=[
                    "reqId=9001", "orderId=101", "permId=555001", "execId=E-PRIMARY-0001", "shares=10",
                    "price=500.30", "side=BOT", "symbol=SPY", "secType=STK", "exchange=SMART",
                    "currency=USD", "contract=SPY-STK-SMART-USD",
                ],
            ),
            _legacy_event(
                event_type="commissionReport",
                received_utc=stamps[6],
                args=[
                    "orderId=101", "permId=555001", "execId=E-PRIMARY-0001", "commission=1.25",
                    "currency=USD", "symbol=SPY", "secType=STK", "exchange=SMART", "contract=SPY-STK-SMART-USD",
                ],
            ),
            _legacy_event(
                event_type="position",
                received_utc=stamps[7],
                args=[
                    "account=DUO847203", "symbol=SPY", "secType=STK", "exchange=SMART", "currency=USD",
                    "contract=SPY-STK-SMART-USD", "position=10", "avgCost=500.30", "marketPrice=501.00",
                ],
            ),
            _legacy_event(event_type="openOrderEnd", received_utc=stamps[8], args=["openOrderEnd()"]),
            _legacy_event(event_type="execDetailsEnd", received_utc=stamps[9], args=["reqId=9001"]),
            _legacy_event(event_type="bootstrapHandshakeComplete", received_utc=stamps[10], args=["timeoutSeconds=15"]),
        ],
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    script_path = Path("/home/node/constellation/ops/tools/run_broker_fact_spine_v1.py")
    argv = [
        str(script_path),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(tmp_path / "truth"),
        "--environment",
        "PAPER",
        "--sleeve_id",
        "PRIMARY",
        "--evaluation_utc",
        stamps[10],
        "--source_path",
        str(source_path),
        "--json",
    ]
    with patch.object(sys, "argv", argv):
        try:
            runpy.run_path(str(script_path), run_name="__main__")
        except SystemExit as exc:
            assert exc.code == 0

    audit_path = resolve_broker_fact_spine_audit_path(
        execution_root_path=execution_root,
        day_utc=day_utc,
    )
    assert audit_path.exists()
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    assert audit["schema_id"] == "broker_fact_spine_audit"
    assert audit["day_utc"] == day_utc


def test_normalize_broker_fact_records_is_deterministic_across_duplicate_and_replay_inputs() -> None:
    identity = _governed_identity()
    raw_one = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(
            event_type="orderStatus",
            received_utc="2026-04-14T14:30:04Z",
            args=["orderId=101", "status=Submitted", "filled=0", "remaining=10", "permId=555001"],
        ),
        source_adapter_name="legacy_ib_execution_observer_v1",
        source_session_id="session-a",
        source_path="/tmp/source.jsonl",
        source_line_number=1,
        governed_identity=identity,
    )
    raw_two = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(
            event_type="orderStatus",
            received_utc="2026-04-14T14:30:05Z",
            args=["orderId=101", "status=Submitted", "filled=0", "remaining=10", "permId=555001"],
        ),
        source_adapter_name="legacy_ib_execution_observer_v1",
        source_session_id="session-a",
        source_path="/tmp/source.jsonl",
        source_line_number=2,
        governed_identity=identity,
    )
    raw_three = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(
            event_type="orderStatus",
            received_utc="2026-04-14T14:30:06Z",
            args=["orderId=101", "status=Submitted", "filled=0", "remaining=10", "permId=555001"],
        ),
        source_adapter_name="legacy_ib_execution_observer_v1",
        source_session_id="session-b",
        source_path="/tmp/source.jsonl",
        source_line_number=3,
        governed_identity=identity,
    )
    for index, row in enumerate((raw_one, raw_two, raw_three), start=1):
        row["journal_sequence_number"] = index

    normalized_a = normalize_broker_fact_records_v1(raw_rows=[raw_one, raw_two, raw_three])
    normalized_b = normalize_broker_fact_records_v1(raw_rows=[raw_one, raw_two, raw_three])
    rows_a = normalized_a["observed_order_status_fact"]
    rows_b = normalized_b["observed_order_status_fact"]

    assert [row["canonical_event_identity"] for row in rows_a] == [row["canonical_event_identity"] for row in rows_b]
    assert len({row["canonical_event_identity"] for row in rows_a}) == 1
    assert [row["duplicate_classification"] for row in rows_a] == ["UNIQUE", DUPLICATE_EVENT, DUPLICATE_REPLAY_OVERLAP]
    assert rows_a[1]["replay_classification"] == REPLAY_CLASSIFICATION_DUPLICATE_CALLBACK
    assert rows_a[2]["replay_classification"] == REPLAY_CLASSIFICATION_REPLAY_OVERLAP


def test_normalize_broker_fact_records_handles_missing_timestamps_deterministically() -> None:
    identity = _governed_identity()
    raw_one = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(event_type="starting", args=["host=127.0.0.1"]),
        source_adapter_name="legacy",
        source_session_id="session-1",
        source_path="/tmp/source.jsonl",
        source_line_number=1,
        governed_identity=identity,
    )
    raw_two = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(event_type="starting", args=["host=127.0.0.1"]),
        source_adapter_name="legacy",
        source_session_id="session-1",
        source_path="/tmp/source.jsonl",
        source_line_number=2,
        governed_identity=identity,
    )
    raw_one["journal_sequence_number"] = 1
    raw_two["journal_sequence_number"] = 2

    normalized = normalize_broker_fact_records_v1(raw_rows=[raw_one, raw_two])
    session_rows = normalized["observation_session_fact"]

    assert raw_one["observed_utc"] == "1970-01-01T00:00:00Z"
    assert raw_one["ordering_basis"] == "SOURCE_SEQUENCE_NUMBER"
    assert session_rows[0]["canonical_event_identity"] == session_rows[1]["canonical_event_identity"]


def test_position_fact_preserves_available_lineage_fields() -> None:
    identity = _governed_identity()
    raw_row = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(
            event_type="position",
            received_utc="2026-04-14T14:30:01Z",
            args=[
                "account=DUO847203",
                "contract=SPY-STK-SMART-USD",
                "position=10",
                "avgCost=500.30",
                "marketPrice=501.00",
                "orderId=101",
                "permId=555001",
                "native_engine_id=C2_NATIVE_ENGINE_V1",
                "engine_id=C2_NATIVE_ENGINE_V1",
                "strategy_engine_id=C2_TREND_EQ_PRIMARY_V1",
            ],
        ),
        source_adapter_name="legacy",
        source_session_id="session-1",
        source_path="/tmp/source.jsonl",
        source_line_number=1,
        governed_identity=identity,
    )
    raw_row["journal_sequence_number"] = 1

    normalized = normalize_broker_fact_records_v1(raw_rows=[raw_row])
    position_row = normalized["observed_position_fact"][0]

    assert position_row["order_id"] == "101"
    assert position_row["perm_id"] == "555001"
    assert position_row["native_engine_id"] == "C2_NATIVE_ENGINE_V1"
    assert position_row["engine_id"] == "C2_NATIVE_ENGINE_V1"
    assert position_row["strategy_engine_id"] == "C2_TREND_EQ_PRIMARY_V1"


def test_position_fact_without_lineage_does_not_synthesize_attribution() -> None:
    identity = _governed_identity()
    raw_row = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(
            event_type="position",
            received_utc="2026-04-14T14:30:01Z",
            args=[
                "account=DUO847203",
                "contract=SPY-STK-SMART-USD",
                "position=10",
                "avgCost=500.30",
                "marketPrice=501.00",
            ],
        ),
        source_adapter_name="legacy",
        source_session_id="session-1",
        source_path="/tmp/source.jsonl",
        source_line_number=1,
        governed_identity=identity,
    )
    raw_row["journal_sequence_number"] = 1

    normalized = normalize_broker_fact_records_v1(raw_rows=[raw_row])
    position_row = normalized["observed_position_fact"][0]

    assert position_row["order_id"] == ""
    assert position_row["perm_id"] == ""
    assert position_row["native_engine_id"] == ""
    assert position_row["engine_id"] == ""
    assert position_row["strategy_engine_id"] == ""
    assert position_row["engine_id"] != position_row["sleeve_id"]


def test_materialize_broker_fact_spine_blocks_foreign_manual_observation(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    source_path = tmp_path / "foreign.jsonl"
    _write_jsonl(
        source_path,
        [
            _legacy_event(
                event_type="starting",
                received_utc="2026-04-14T14:30:00Z",
                args=["host=127.0.0.1", "port=4002", "clientId=999"],
                client_id=999,
            )
        ],
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    bundle = materialize_broker_fact_spine_v1(
        repo_root=Path("/home/node/constellation"),
        truth_root=tmp_path / "truth",
        day_utc="2026-04-14",
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc="2026-04-14T14:30:05Z",
        source_path=source_path,
    )

    health = json.loads(bundle.health_path.read_text(encoding="utf-8"))
    trust_dependency = json.loads(bundle.trust_dependency_path.read_text(encoding="utf-8"))
    raw_rows = [json.loads(line) for line in bundle.raw_journal_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert raw_rows[0]["attribution_status"] == RAW_ATTRIBUTION_FOREIGN
    assert health["attribution_status"] == RAW_ATTRIBUTION_FOREIGN
    assert health["current_state"] == TRUST_BLOCKED
    assert health["downstream_consumption_posture"] == DOWNSTREAM_FAIL_CLOSED
    assert RC_FOREIGN_MANUAL_SUSPECTED in health["blocker_codes"]
    assert trust_dependency["must_fail_closed"] is True


def test_build_raw_evidence_envelope_reports_strict_attribution_states() -> None:
    identity = _governed_identity()
    attributed = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(event_type="starting", received_utc="2026-04-14T14:30:00Z", args=["host=127.0.0.1"]),
        source_adapter_name="legacy",
        source_session_id="session-1",
        source_path="/tmp/source.jsonl",
        source_line_number=1,
        governed_identity=identity,
    )
    partial = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(event_type="position", received_utc="2026-04-14T14:30:01Z", args=["symbol=SPY"]),
        source_adapter_name="legacy",
        source_session_id="session-1",
        source_path="/tmp/source.jsonl",
        source_line_number=2,
        governed_identity=identity,
    )
    ambiguous = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload={
            **_legacy_event(event_type="starting", received_utc="2026-04-14T14:30:02Z", args=["host=127.0.0.1"]),
            "explicit_sleeve_id": "OTHER",
        },
        source_adapter_name="legacy",
        source_session_id="session-1",
        source_path="/tmp/source.jsonl",
        source_line_number=3,
        governed_identity=identity,
    )
    unresolved = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload={
            "schema_id": "BROKER_EVENT_RAW",
            "schema_version": 1,
            "received_utc": "2026-04-14T14:30:03Z",
            "broker": {"environment": "PAPER", "name": "INTERACTIVE_BROKERS"},
            "event_type": "starting",
            "ib_fields": {"args": [{"value": "host=127.0.0.1"}]},
        },
        source_adapter_name="legacy",
        source_session_id="session-1",
        source_path="/tmp/source.jsonl",
        source_line_number=4,
        governed_identity=identity,
    )
    foreign = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(event_type="starting", received_utc="2026-04-14T14:30:04Z", args=["host=127.0.0.1"], client_id=999),
        source_adapter_name="legacy",
        source_session_id="session-1",
        source_path="/tmp/source.jsonl",
        source_line_number=5,
        governed_identity=identity,
    )

    assert attributed["attribution_status"] == RAW_ATTRIBUTION_ATTRIBUTED
    assert partial["attribution_status"] == RAW_ATTRIBUTION_PARTIAL
    assert ambiguous["attribution_status"] == RAW_ATTRIBUTION_AMBIGUOUS
    assert unresolved["attribution_status"] == RAW_ATTRIBUTION_UNRESOLVED
    assert foreign["attribution_status"] == RAW_ATTRIBUTION_FOREIGN


def test_materialize_broker_fact_spine_reports_reconnect_recovered_as_degraded(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    source_path = tmp_path / "reconnect.jsonl"
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=5)
    stamps = [(base + timedelta(seconds=index)).isoformat().replace("+00:00", "Z") for index in range(5)]
    _write_jsonl(
        source_path,
        [
            _legacy_event(event_type="starting", received_utc=stamps[0], args=["host=127.0.0.1"]),
            _legacy_event(event_type="nextValidId", received_utc=stamps[1], args=["orderId=101"]),
            _legacy_event(event_type="error", received_utc=stamps[2], args=["errorCode=1100", "errorString=Connectivity between IB and TWS has been lost"]),
            _legacy_event(event_type="nextValidId", received_utc=stamps[3], args=["orderId=102"]),
            _legacy_event(event_type="bootstrapHandshakeComplete", received_utc=stamps[4], args=["timeoutSeconds=15"]),
        ],
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    bundle = materialize_broker_fact_spine_v1(
        repo_root=Path("/home/node/constellation"),
        truth_root=tmp_path / "truth",
        day_utc=base.date().isoformat(),
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc=stamps[4],
        source_path=source_path,
    )

    health = json.loads(bundle.health_path.read_text(encoding="utf-8"))
    trust_dependency = json.loads(bundle.trust_dependency_path.read_text(encoding="utf-8"))
    assert health["current_state"] == TRUST_DEGRADED
    assert health["reconnect_status"] == RECONNECT_RECOVERED
    assert health["gap_status"] == GAP_NONE
    assert RC_RECONNECT_RECOVERED in health["degraded_codes"]
    assert trust_dependency["downstream_consumption_posture"] == DOWNSTREAM_DEGRADED_ONLY
    assert trust_dependency["may_consume_with_degraded_posture"] is True


def test_materialize_broker_fact_spine_blocks_during_replay_gap(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    source_path = tmp_path / "replay_gap.jsonl"
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=3)
    stamps = [(base + timedelta(seconds=index)).isoformat().replace("+00:00", "Z") for index in range(3)]
    _write_jsonl(
        source_path,
        [
            _legacy_event(event_type="starting", received_utc=stamps[0], args=["host=127.0.0.1"]),
            _legacy_event(event_type="nextValidId", received_utc=stamps[1], args=["orderId=101"]),
            _legacy_event(event_type="reqAllOpenOrders", received_utc=stamps[2], args=["reqAllOpenOrders()"]),
        ],
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    bundle = materialize_broker_fact_spine_v1(
        repo_root=Path("/home/node/constellation"),
        truth_root=tmp_path / "truth",
        day_utc=base.date().isoformat(),
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc=stamps[2],
        source_path=source_path,
    )

    health = json.loads(bundle.health_path.read_text(encoding="utf-8"))
    trust_dependency = json.loads(bundle.trust_dependency_path.read_text(encoding="utf-8"))
    assert health["current_state"] == TRUST_BLOCKED
    assert health["replay_status"] == REPLAYING
    assert health["gap_status"] == GAP_SUSPECTED
    assert RC_REPLAY_IN_PROGRESS in health["blocker_codes"]
    assert trust_dependency["downstream_consumption_posture"] == DOWNSTREAM_FAIL_CLOSED
    assert trust_dependency["must_fail_closed"] is True


def test_normalize_broker_fact_records_is_stable_under_reversed_inputs_and_conflicts() -> None:
    identity = _governed_identity()
    raw_one = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload={
            **_legacy_event(
                event_type="orderStatus",
                received_utc="2026-04-14T14:30:05Z",
                args=["orderId=101", "status=Submitted", "filled=0", "remaining=10", "permId=555001"],
            ),
            "sequence_number": 1,
        },
        source_adapter_name="legacy",
        source_session_id="session-a",
        source_path="/tmp/source.jsonl",
        source_line_number=1,
        governed_identity=identity,
    )
    raw_two = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload={
            **_legacy_event(
                event_type="orderStatus",
                received_utc="2026-04-14T14:30:04Z",
                args=["orderId=101", "status=Submitted", "filled=0", "remaining=10", "permId=555001"],
            ),
            "sequence_number": 2,
        },
        source_adapter_name="legacy",
        source_session_id="session-a",
        source_path="/tmp/source.jsonl",
        source_line_number=2,
        governed_identity=identity,
    )
    raw_three = build_raw_evidence_envelope_from_payload_v1(
        day_utc="2026-04-14",
        raw_payload=_legacy_event(
            event_type="orderStatus",
            received_utc="2026-04-14T14:30:06Z",
            args=["orderId=101", "status=Submitted", "filled=0", "remaining=10", "permId=555001"],
        ),
        source_adapter_name="legacy",
        source_session_id="session-b",
        source_path="/tmp/source.jsonl",
        source_line_number=3,
        governed_identity=identity,
    )
    for index, row in enumerate((raw_one, raw_two, raw_three), start=1):
        row["journal_sequence_number"] = index

    forward = normalize_broker_fact_records_v1(raw_rows=[raw_one, raw_two, raw_three])
    reversed_rows = normalize_broker_fact_records_v1(raw_rows=[raw_three, raw_two, raw_one])
    forward_rows = forward["observed_order_status_fact"]
    reversed_rows_out = reversed_rows["observed_order_status_fact"]

    assert [row["canonical_event_identity"] for row in forward_rows] == [row["canonical_event_identity"] for row in reversed_rows_out]
    assert [row["duplicate_classification"] for row in forward_rows] == [row["duplicate_classification"] for row in reversed_rows_out]
    assert DUPLICATE_CONFLICT in [row["duplicate_classification"] for row in forward_rows]


def test_materialize_core1_pre_core2_readiness_reports_trusted_day(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    source_path = tmp_path / "trusted.jsonl"
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=4)
    stamps = [(base + timedelta(seconds=index)).isoformat().replace("+00:00", "Z") for index in range(4)]
    _write_jsonl(
        source_path,
        [
            _legacy_event(event_type="starting", received_utc=stamps[0], args=["host=127.0.0.1", "port=4002", "clientId=179"]),
            _legacy_event(event_type="nextValidId", received_utc=stamps[1], args=["orderId=101"]),
            _legacy_event(event_type="orderStatus", received_utc=stamps[2], args=["orderId=101", "status=Submitted", "filled=0", "remaining=10", "permId=555001"]),
            _legacy_event(event_type="bootstrapHandshakeComplete", received_utc=stamps[3], args=["timeoutSeconds=15"]),
        ],
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialize_broker_fact_spine_v1(
        repo_root=Path("/home/node/constellation"),
        truth_root=tmp_path / "truth",
        day_utc=base.date().isoformat(),
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc=stamps[3],
        source_path=source_path,
    )
    readiness = materialize_core1_pre_core2_readiness_v1(
        repo_root=Path("/home/node/constellation"),
        truth_root=tmp_path / "truth",
        day_utc=base.date().isoformat(),
        environment="PAPER",
        sleeve_id="PRIMARY",
    )

    assert readiness.payload["current_state"] == READINESS_READY
    assert readiness.payload["safe_for_core2_consumption"] is True
    assert readiness.payload["downstream_consumption_posture"] == DOWNSTREAM_NORMAL
    assert readiness.payload["legacy_boundary_status"] == LEGACY_BOUNDARY_HARDENED
    assert readiness.payload["identity_stability_status"] == "STABLE"
    assert "broker_raw_evidence_envelope.v1" not in json.dumps(readiness.payload["core2_allowed_input_refs"], sort_keys=True)
    assert "reconciled_trade_state" not in json.dumps(readiness.payload, sort_keys=True)


def test_materialize_core1_pre_core2_readiness_blocks_replay_gap_day(tmp_path: Path, monkeypatch) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    source_path = tmp_path / "replay_gap.jsonl"
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=3)
    stamps = [(base + timedelta(seconds=index)).isoformat().replace("+00:00", "Z") for index in range(3)]
    _write_jsonl(
        source_path,
        [
            _legacy_event(event_type="starting", received_utc=stamps[0], args=["host=127.0.0.1"]),
            _legacy_event(event_type="nextValidId", received_utc=stamps[1], args=["orderId=101"]),
            _legacy_event(event_type="reqAllOpenOrders", received_utc=stamps[2], args=["reqAllOpenOrders()"]),
        ],
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(execution_root),
    )

    materialize_broker_fact_spine_v1(
        repo_root=Path("/home/node/constellation"),
        truth_root=tmp_path / "truth",
        day_utc=base.date().isoformat(),
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc=stamps[2],
        source_path=source_path,
    )
    readiness = materialize_core1_pre_core2_readiness_v1(
        repo_root=Path("/home/node/constellation"),
        truth_root=tmp_path / "truth",
        day_utc=base.date().isoformat(),
        environment="PAPER",
        sleeve_id="PRIMARY",
    )

    assert readiness.payload["current_state"] == READINESS_BLOCKED
    assert readiness.payload["downstream_consumption_posture"] == DOWNSTREAM_FAIL_CLOSED
    assert readiness.payload["safe_for_core2_consumption"] is False
    assert RC_REPLAY_IN_PROGRESS in readiness.payload["blocker_codes"]


def test_build_core1_pre_core2_readiness_hard_cuts_legacy_runtime_day(tmp_path: Path) -> None:
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    execution_root.mkdir(parents=True)
    raw_journal_path = resolve_broker_raw_journal_path(execution_root_path=execution_root, day_utc="2026-04-11")
    raw_journal_path.parent.mkdir(parents=True, exist_ok=True)
    raw_journal_path.write_text(json.dumps({"schema_id": "BROKER_EVENT_RAW", "received_utc": "2026-04-11T14:30:00Z"}) + "\n", encoding="utf-8")
    fact_ledger_paths = {
        schema_id: resolve_fact_ledger_path(execution_root_path=execution_root, day_utc="2026-04-11", schema_id=schema_id)
        for schema_id in ("observation_session_fact", "observed_order_fact", "observed_order_status_fact", "observed_fill_fact", "observed_position_fact")
    }
    readiness = build_core1_pre_core2_readiness_payload_v1(
        execution_root_path=execution_root,
        day_utc="2026-04-11",
        raw_journal_path=raw_journal_path,
        fact_ledger_paths=fact_ledger_paths,
        health_path=execution_root / "reports" / "broker_observation_health_v1" / "2026-04-11" / "broker_observation_health.v1.json",
        trust_dependency_path=execution_root / "reports" / "broker_observation_trust_dependency_v1" / "2026-04-11" / "broker_observation_trust_dependency.v1.json",
    )

    assert readiness["current_state"] == READINESS_BLOCKED
    assert readiness["legacy_boundary_status"] == LEGACY_BOUNDARY_LEGACY
    assert RC_CORE1_LEGACY_RUNTIME_DAY_NON_CONSUMABLE in readiness["blocker_codes"]
    assert RC_CORE2_REQUIRED_ARTIFACT_MISSING in readiness["blocker_codes"]
    assert readiness["safe_for_core2_consumption"] is False
