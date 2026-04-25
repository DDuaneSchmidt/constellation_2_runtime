from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from ops.tools import run_c2_paper_day_orchestrator_v2 as orch_v2
from ops.tools import run_capital_authority_allocation_day_v1 as capalloc
import constellation_2.common.sleeve_edge_measurement_v1 as sleeve_edge
from constellation_2.common.sleeve_edge_measurement_v1 import (
    allocator_action_from_qualification_v1,
    materialize_sleeve_edge_snapshot_v1,
    read_sleeve_edge_snapshot_for_day_v1,
)


DAY = "2026-04-15"
ENGINE_ID = "C2_TREND_EQ_PRIMARY_V1"
SLEEVE_ID = "C2_TREND_EQ_PRIMARY"

POLICY = {
    "schema_id": "C2_SLEEVE_EDGE_QUALIFICATION_POLICY_V1",
    "schema_version": 1,
    "policy_version": "v1",
    "metric_windows": {"recent_trade_count": 2, "baseline_trade_count": 3},
  "sample_sufficiency": {"limited_min_sample_count": 2, "sufficient_min_sample_count": 5},
    "unknown_attribution_thresholds": {"max_unknown_trade_count": 1},
    "edge_band_thresholds": {
        "weak_positive_min_net_expectancy": "0.01",
        "qualified_positive_min_net_expectancy": "25.00",
        "strong_positive_min_net_expectancy": "75.00",
    },
    "drift_band_thresholds": {
        "improving_min_delta_net_expectancy": "10.00",
        "deteriorating_max_delta_net_expectancy": "-10.00",
    },
    "allocator_compatibility": {"allowed_calculation_versions": ["sleeve_edge_measurement_v1"]},
    "allocator_actions": {
        "MEASUREMENT_INVALID": {"capital_multiplier_bp": 0, "reason_code": "CAPAUTH_SLEEVE_MEASUREMENT_INVALID"},
        "INSUFFICIENT_DATA": {"capital_multiplier_bp": 0, "reason_code": "CAPAUTH_SLEEVE_INSUFFICIENT_DATA"},
        "QUALIFIED": {"capital_multiplier_bp": 10000, "reason_code": "CAPAUTH_SLEEVE_QUALIFIED"},
        "WATCHLIST": {"capital_multiplier_bp": 5000, "reason_code": "CAPAUTH_SLEEVE_WATCHLIST_THROTTLE"},
        "THROTTLED": {"capital_multiplier_bp": 2500, "reason_code": "CAPAUTH_SLEEVE_POLICY_THROTTLED"},
        "DISABLED": {"capital_multiplier_bp": 0, "reason_code": "CAPAUTH_SLEEVE_DISABLED"},
    },
}


def _policy_copy() -> dict[str, object]:
    return json.loads(json.dumps(POLICY))


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _execution_record(path: Path, *, order_id: int, perm_id: int, engine_id: str) -> None:
    _write_json(
        path,
        {
            "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
            "schema_version": 1,
            "produced_utc": f"{DAY}T16:00:00Z",
            "day_utc": DAY,
            "producer": {"repo": "constellation", "git_sha": "0" * 40, "module": "test"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": _sha(f"submission:{order_id}"),
            "binding_hash": _sha(f"binding:{order_id}"),
            "engine_id": engine_id,
            "source_intent_id": f"intent-{order_id}",
            "intent_sha256": _sha(f"intent-sha:{order_id}"),
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "event_type": "EXEC_DETAILS",
            "event_time_utc": f"{DAY}T15:00:00Z",
            "observed_at_utc": f"{DAY}T15:00:00Z",
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
            "order_state": {"status": "FILLED", "filled_qty": 1, "remaining_qty": 0, "avg_fill_price": "100"},
            "fill": {"fill_qty": 1, "fill_price": "100", "commission": "0", "currency": "USD"},
            "canonical_json_hash": _sha(f"canonical:{order_id}"),
        },
    )


def _trade_bundle(
    root: Path,
    *,
    materialization_set_id: str,
    trade_identity_id: str,
    ownership: str,
    continuity_status: str,
    order_id: int,
    perm_id: int,
    buy_price: str,
    sell_price: str,
    close_utc: str,
) -> dict[str, str]:
    trade_dir = root / "reconciled_trade_state_v1" / "materializations" / DAY / materialization_set_id / "trades" / trade_identity_id
    trade_identity_path = trade_dir / "trade_identity.v1.json"
    state_path = trade_dir / "incorporated_broker_trade_state.v1.json"
    provenance_path = trade_dir / "reconciliation_provenance.v1.json"
    _write_json(
        trade_identity_path,
        {
            "trade_identity_id": trade_identity_id,
            "lineage_attachment_refs": {
                "order_ids": [str(order_id)],
                "perm_ids": [str(perm_id)],
                "execution_ids": [f"exec-{order_id}"],
                "fact_record_ids": [_sha(f"fact:{order_id}:open"), _sha(f"fact:{order_id}:close")],
            },
            "open_close_continuity": {
                "continuity_key": f"continuity-{trade_identity_id}",
                "segment_index": 1,
                "continuity_status": continuity_status,
                "opened_by_fact_record_id": _sha(f"fact:{order_id}:open"),
                "closed_by_fact_record_id": _sha(f"fact:{order_id}:close"),
            },
        },
    )
    _write_json(
        state_path,
        {
            "trade_identity_ref": {"trade_identity_id": trade_identity_id, "artifact_path": str(trade_identity_path), "artifact_sha256": _sha("trade-id")},
            "ownership_classification": ownership,
            "current_quantity": "0",
            "average_cost": buy_price,
            "side": "FLAT",
            "incorporated_fills": [
                {
                    "execution_id": f"exec-open-{order_id}",
                    "order_id": str(order_id),
                    "perm_id": str(perm_id),
                    "fill_quantity": "1",
                    "fill_price": buy_price,
                    "side": "BOT",
                    "commission": "0",
                    "currency": "USD",
                    "observed_utc": close_utc.replace("15:30:00Z", "09:30:00Z"),
                    "fact_record_ids": [_sha(f"fact:{order_id}:open")],
                },
                {
                    "execution_id": f"exec-close-{order_id}",
                    "order_id": str(order_id),
                    "perm_id": str(perm_id),
                    "fill_quantity": "1",
                    "fill_price": sell_price,
                    "side": "SLD",
                    "commission": "0",
                    "currency": "USD",
                    "observed_utc": close_utc,
                    "fact_record_ids": [_sha(f"fact:{order_id}:close")],
                },
            ],
        },
    )
    _write_json(
        provenance_path,
        {
            "incorporated_fact_refs": [{"schema_id": "observed_fill_fact", "fact_record_id": _sha(f"fact:{order_id}:open"), "canonical_event_identity": _sha(f"event:{order_id}:open"), "reason_code": "USED"}],
            "ignored_fact_refs": [],
            "blocked_fact_refs": [],
        },
    )
    return {
        "trade_identity_id": trade_identity_id,
        "ownership_classification": ownership,
        "incorporated_state_path": str(state_path),
        "description_path": str(trade_dir / "reconciled_trade_description.v1.json"),
        "health_path": str(trade_dir / "reconciliation_health.v1.json"),
        "provenance_path": str(provenance_path),
    }


def _summary(root: Path, *, materialization_set_id: str, evaluation_utc: str, trade_refs: list[dict[str, str]]) -> None:
    _write_json(
        root / "reports" / "reconciled_trade_state_summary_v1" / DAY / materialization_set_id / "reconciled_trade_state_summary.v1.json",
        {
            "materialization_set_id": materialization_set_id,
            "day_utc": DAY,
            "evaluation_utc": evaluation_utc,
            "sleeve_id": "PRIMARY",
            "execution_root_path": str(root),
            "trade_refs": trade_refs,
        },
    )


def _materialize(
    root: Path,
    *,
    policy: dict[str, object] | None = None,
    revision_type: str = "",
    revision_reason: str = "",
) -> dict[str, object]:
    result = materialize_sleeve_edge_snapshot_v1(
        repo_root=Path("/home/node/constellation"),
        truth_root=root,
        day_utc=DAY,
        sleeve_id=SLEEVE_ID,
        strategy_family="Trend Sleeve",
        engine_ids=[ENGINE_ID],
        qualification_policy=policy or POLICY,
        revision_type=revision_type,
        revision_reason=revision_reason,
    )
    return result.snapshot


def test_native_and_adopted_trades_do_not_mix(tmp_path: Path) -> None:
    mat = "a" * 64
    native = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="1" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=1001,
        perm_id=9001,
        buy_price="100",
        sell_price="140",
        close_utc=f"{DAY}T15:30:00Z",
    )
    adopted = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="2" * 64,
        ownership="FOREIGN_MANUAL",
        continuity_status="CLOSED",
        order_id=2001,
        perm_id=9901,
        buy_price="100",
        sell_price="120",
        close_utc=f"{DAY}T15:31:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[native, adopted])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "native.execution_event_stream_record.v1.json", order_id=1001, perm_id=9001, engine_id=ENGINE_ID)
    snapshot = _materialize(tmp_path)
    metrics = snapshot["factual_metrics"]
    assert metrics["native_trade_count"] == 1
    assert metrics["adopted_trade_count"] == 0
    assert snapshot["included_trade_ids"] == ["1" * 64]
    excluded = {row["trade_identity_id"]: row["reason_codes"] for row in snapshot["exclusion_details"]}
    assert "2" * 64 in excluded
    assert "SLEEVE_EDGE_ADOPTED_ENGINE_ATTRIBUTION_UNAVAILABLE" in excluded["2" * 64]


def test_missing_native_engine_bridge_yields_measurement_invalid(tmp_path: Path) -> None:
    mat = "b" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="3" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=3001,
        perm_id=9301,
        buy_price="100",
        sell_price="130",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    snapshot = _materialize(tmp_path)
    qualification = snapshot["qualification"]
    assert qualification["qualification_state"] == "MEASUREMENT_INVALID"
    assert "SLEEVE_EDGE_NATIVE_ENGINE_ATTRIBUTION_UNAVAILABLE" in qualification["reason_codes"]


def test_insufficient_sample_yields_insufficient_data_and_reason_codes(tmp_path: Path) -> None:
    mat = "c" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="4" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=4001,
        perm_id=9401,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "trade.execution_event_stream_record.v1.json", order_id=4001, perm_id=9401, engine_id=ENGINE_ID)
    snapshot = _materialize(tmp_path)
    assert snapshot["qualification"]["qualification_state"] == "INSUFFICIENT_DATA"
    assert snapshot["qualification"]["reason_codes"] == ["SLEEVE_EDGE_SAMPLE_INSUFFICIENT"]


def test_deteriorating_drift_throttles_when_policy_requires(tmp_path: Path) -> None:
    mat = "d" * 64
    trade_refs = []
    pnl_pairs = [("100", "200"), ("100", "200"), ("100", "200"), ("100", "90"), ("100", "90")]
    for idx, (buy_price, sell_price) in enumerate(pnl_pairs, start=1):
        trade_refs.append(
            _trade_bundle(
                tmp_path,
                materialization_set_id=mat,
                trade_identity_id=str(idx) * 64,
                ownership="CONSTELLATION_OWNED",
                continuity_status="CLOSED",
                order_id=5000 + idx,
                perm_id=9500 + idx,
                buy_price=buy_price,
                sell_price=sell_price,
                close_utc=f"{DAY}T15:3{idx}:00Z",
            )
        )
        _execution_record(
            tmp_path / "execution_stream_v1" / DAY / f"{idx}.execution_event_stream_record.v1.json",
            order_id=5000 + idx,
            perm_id=9500 + idx,
            engine_id=ENGINE_ID,
        )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=trade_refs)
    snapshot = _materialize(tmp_path)
    qualification = snapshot["qualification"]
    assert qualification["drift_band"] == "DETERIORATING"
    assert qualification["qualification_state"] == "THROTTLED"
    assert "SLEEVE_EDGE_DRIFT_DETERIORATING" in qualification["reason_codes"]


def test_allocator_action_consumes_qualification_surface_only() -> None:
    qualification = {
        "qualification_state": "WATCHLIST",
        "edge_band": "WEAK_POSITIVE",
        "execution_health_band": "DEGRADED",
        "sample_sufficiency_band": "LIMITED",
        "drift_band": "UNKNOWN",
        "reason_codes": ["SLEEVE_EDGE_SAMPLE_LIMITED"],
        "native_net_expectancy": "999999",
        "allocation_merit_score": "100",
    }
    action = allocator_action_from_qualification_v1(POLICY, qualification)
    assert action == {
        "qualification_state": "WATCHLIST",
        "capital_multiplier_bp": 5000,
        "reason_code": "CAPAUTH_SLEEVE_WATCHLIST_THROTTLE",
    }


def test_snapshot_is_frozen_and_historical_reads_do_not_recompute(tmp_path: Path) -> None:
    mat = "e" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="5" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=6001,
        perm_id=9601,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "history.execution_event_stream_record.v1.json", order_id=6001, perm_id=9601, engine_id=ENGINE_ID)
    first = _materialize(tmp_path)
    summary_path = tmp_path / "reports" / "reconciled_trade_state_summary_v1" / DAY / mat / "reconciled_trade_state_summary.v1.json"
    summary_path.unlink()
    historical = read_sleeve_edge_snapshot_for_day_v1(truth_root=tmp_path, sleeve_id=SLEEVE_ID, day_utc=DAY)
    assert historical["snapshot_id"] == first["snapshot_id"]
    assert historical["calculation_version"] == "sleeve_edge_measurement_v1"


def test_initial_snapshot_has_initial_publish_lineage(tmp_path: Path) -> None:
    mat = "f" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="6" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=7001,
        perm_id=9701,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "immutable.execution_event_stream_record.v1.json", order_id=7001, perm_id=9701, engine_id=ENGINE_ID)
    snapshot = _materialize(tmp_path)
    lineage = snapshot["snapshot_lineage"]
    assert lineage["revision_type"] == "INITIAL_PUBLISH"
    assert lineage["previous_snapshot_id"] is None
    assert lineage["revision_reason"] == "INITIAL_PUBLISH"


def test_revised_publish_links_previous_snapshot_id_and_preserves_reason(tmp_path: Path) -> None:
    mat = "f" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="6" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=7001,
        perm_id=9701,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "immutable.execution_event_stream_record.v1.json", order_id=7001, perm_id=9701, engine_id=ENGINE_ID)
    first = _materialize(tmp_path)
    _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="6" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=7001,
        perm_id=9701,
        buy_price="100",
        sell_price="170",
        close_utc=f"{DAY}T15:30:00Z",
    )
    revised = _materialize(
        tmp_path,
        revision_type="DATA_CORRECTION",
        revision_reason="FACT_INPUT_HASH_CHANGED",
    )
    assert revised["snapshot_id"] != first["snapshot_id"]
    assert first["qualification"]["qualification_state"] == "INSUFFICIENT_DATA"
    assert revised["snapshot_lineage"]["previous_snapshot_id"] == first["snapshot_id"]
    assert revised["snapshot_lineage"]["revision_type"] == "DATA_CORRECTION"
    assert revised["snapshot_lineage"]["revision_reason"] == "FACT_INPUT_HASH_CHANGED"
    historical = read_sleeve_edge_snapshot_for_day_v1(truth_root=tmp_path, sleeve_id=SLEEVE_ID, day_utc=DAY)
    assert historical["snapshot_id"] == revised["snapshot_id"]


def test_policy_version_boundary_starts_new_lineage(tmp_path: Path) -> None:
    mat = "b" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="b" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8051,
        perm_id=9851,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "lineage-policy.execution_event_stream_record.v1.json", order_id=8051, perm_id=9851, engine_id=ENGINE_ID)
    first = _materialize(tmp_path)
    policy_v2 = _policy_copy()
    policy_v2["policy_version"] = "v2"
    second = _materialize(tmp_path, policy=policy_v2)
    assert second["snapshot_lineage"]["previous_snapshot_id"] is None
    assert second["snapshot_lineage"]["revision_type"] == "INITIAL_PUBLISH"
    assert second["snapshot_lineage"]["revision_reason"] == "LINEAGE_RESET_POLICY_VERSION_BOUNDARY:v1->v2"
    assert first["snapshot_id"] != second["snapshot_id"]


def test_calculation_version_boundary_starts_new_lineage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mat = "c" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="c" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8061,
        perm_id=9861,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "lineage-calc.execution_event_stream_record.v1.json", order_id=8061, perm_id=9861, engine_id=ENGINE_ID)
    first = _materialize(tmp_path)
    monkeypatch.setattr(sleeve_edge, "CALCULATION_VERSION", "sleeve_edge_measurement_v2")
    second = _materialize(tmp_path)
    assert second["snapshot_lineage"]["previous_snapshot_id"] is None
    assert second["snapshot_lineage"]["revision_type"] == "INITIAL_PUBLISH"
    assert second["snapshot_lineage"]["revision_reason"] == "LINEAGE_RESET_CALCULATION_VERSION_BOUNDARY:sleeve_edge_measurement_v1->sleeve_edge_measurement_v2"
    assert first["snapshot_id"] != second["snapshot_id"]


def test_versions_and_unsupported_metrics_are_preserved(tmp_path: Path) -> None:
    mat = "1" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="7" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8001,
        perm_id=9801,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "versions.execution_event_stream_record.v1.json", order_id=8001, perm_id=9801, engine_id=ENGINE_ID)
    snapshot = _materialize(tmp_path)
    metrics = snapshot["factual_metrics"]
    assert snapshot["calculation_version"] == "sleeve_edge_measurement_v1"
    assert snapshot["policy_version"] == "v1"
    assert snapshot["unavailable_metrics"] == [
        "adopted_capital_efficiency",
        "measured_slippage_drag",
        "native_budget_utilization",
        "native_capital_efficiency",
        "native_expectancy_per_unit_risk",
        "native_recent_vs_baseline_drift",
    ]
    assert metrics["native_expectancy_per_unit_risk"]["status"] == "UNAVAILABLE"
    assert metrics["native_budget_utilization"]["reason_codes"] == ["SLEEVE_EDGE_BUDGET_TRUTH_UNAVAILABLE"]
    assert "included_trade_ids" in snapshot and "excluded_trade_ids" in snapshot


def test_snapshot_fact_input_hash_is_bound_to_fact_ledger(tmp_path: Path) -> None:
    mat = "1" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="b" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8011,
        perm_id=9811,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "binding.execution_event_stream_record.v1.json", order_id=8011, perm_id=9811, engine_id=ENGINE_ID)
    snapshot = _materialize(tmp_path)
    fact_ledger = json.loads(Path(str(snapshot["fact_ledger_ref"]["artifact_path"])).read_text(encoding="utf-8"))
    assert snapshot["fact_input_hash"] == fact_ledger["fact_input_hash"]
    assert snapshot["fact_input_hash"] == sleeve_edge._fact_input_hash_from_fact_ledger(fact_ledger)


def test_unknown_attribution_within_threshold_does_not_auto_invalidate(tmp_path: Path) -> None:
    mat = "2" * 64
    native = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="8" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8101,
        perm_id=9811,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    unknown = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="d" * 64,
        ownership="BROKER_IMPORTED_UNKNOWN",
        continuity_status="CLOSED",
        order_id=8102,
        perm_id=9812,
        buy_price="100",
        sell_price="140",
        close_utc=f"{DAY}T15:31:00Z",
    )
    relaxed_policy = _policy_copy()
    relaxed_policy["sample_sufficiency"] = {"limited_min_sample_count": 1, "sufficient_min_sample_count": 1}
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[native, unknown])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "unknown-threshold.execution_event_stream_record.v1.json", order_id=8101, perm_id=9811, engine_id=ENGINE_ID)
    snapshot = _materialize(tmp_path, policy=relaxed_policy)
    metrics = snapshot["factual_metrics"]
    excluded = {row["trade_identity_id"]: row["reason_codes"] for row in snapshot["exclusion_details"]}
    assert metrics["unknown_attribution_count"] == 1
    assert metrics["native_trade_count"] == 1
    assert metrics["adopted_trade_count"] == 0
    assert snapshot["qualification"]["qualification_state"] != "MEASUREMENT_INVALID"
    assert "SLEEVE_EDGE_UNKNOWN_ATTRIBUTION_THRESHOLD_EXCEEDED" not in snapshot["qualification"]["reason_codes"]
    assert excluded["d" * 64] == ["SLEEVE_EDGE_UNKNOWN_ATTRIBUTION_EXCLUDED"]


def test_unknown_attribution_beyond_threshold_invalidates_deterministically(tmp_path: Path) -> None:
    mat = "2" * 64
    native = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="8" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8101,
        perm_id=9811,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    unknown = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="e" * 64,
        ownership="BROKER_IMPORTED_UNKNOWN",
        continuity_status="CLOSED",
        order_id=8102,
        perm_id=9812,
        buy_price="100",
        sell_price="140",
        close_utc=f"{DAY}T15:31:00Z",
    )
    strict_policy = _policy_copy()
    strict_policy["sample_sufficiency"] = {"limited_min_sample_count": 1, "sufficient_min_sample_count": 1}
    strict_policy["unknown_attribution_thresholds"] = {"max_unknown_trade_count": 0}
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[native, unknown])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "unknown-threshold-strict.execution_event_stream_record.v1.json", order_id=8101, perm_id=9811, engine_id=ENGINE_ID)
    snapshot = _materialize(tmp_path, policy=strict_policy)
    assert snapshot["factual_metrics"]["unknown_attribution_count"] == 1
    assert snapshot["qualification"]["qualification_state"] == "MEASUREMENT_INVALID"
    assert "SLEEVE_EDGE_UNKNOWN_ATTRIBUTION_THRESHOLD_EXCEEDED" in snapshot["qualification"]["reason_codes"]


def test_allocator_reads_existing_snapshot_without_publishing_one(tmp_path: Path) -> None:
    mat = "3" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="9" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8201,
        perm_id=9821,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "allocator.execution_event_stream_record.v1.json", order_id=8201, perm_id=9821, engine_id=ENGINE_ID)
    published = _materialize(tmp_path)
    before = sorted((tmp_path / "reports" / "sleeve_edge_snapshot_v1" / DAY / SLEEVE_ID).glob("*/sleeve_edge_snapshot.v1.json"))
    meta = capalloc._load_sleeve_edge_allocator_meta(
        truth_root=tmp_path,
        day_utc=DAY,
        sleeve_id=SLEEVE_ID,
        sleeve_edge_policy=POLICY,
        canonical_sequence_owner=capalloc.CANONICAL_SEQUENCE_OWNER,
    )
    after = sorted((tmp_path / "reports" / "sleeve_edge_snapshot_v1" / DAY / SLEEVE_ID).glob("*/sleeve_edge_snapshot.v1.json"))
    assert meta["snapshot_path"] == published["artifact_path"]
    assert meta["qualification_state"] == published["qualification"]["qualification_state"]
    assert before == after


def test_allocator_fails_closed_when_snapshot_is_missing(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="SLEEVE_EDGE_SNAPSHOT_DEPENDENCY"):
        capalloc._load_sleeve_edge_allocator_meta(
            truth_root=tmp_path,
            day_utc=DAY,
            sleeve_id=SLEEVE_ID,
            sleeve_edge_policy=POLICY,
            canonical_sequence_owner=capalloc.CANONICAL_SEQUENCE_OWNER,
        )


def test_policy_version_lock_match_and_mismatch(tmp_path: Path) -> None:
    mat = "4" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="a" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8301,
        perm_id=9831,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "policy.execution_event_stream_record.v1.json", order_id=8301, perm_id=9831, engine_id=ENGINE_ID)
    _materialize(tmp_path)
    matched = capalloc._load_sleeve_edge_allocator_meta(
        truth_root=tmp_path,
        day_utc=DAY,
        sleeve_id=SLEEVE_ID,
        sleeve_edge_policy=POLICY,
        canonical_sequence_owner=capalloc.CANONICAL_SEQUENCE_OWNER,
    )
    assert matched["snapshot_policy_version"] == "v1"
    mismatched_policy = _policy_copy()
    mismatched_policy["policy_version"] = "v2"
    with pytest.raises(SystemExit, match="SLEEVE_EDGE_POLICY_VERSION_MISMATCH"):
        capalloc._load_sleeve_edge_allocator_meta(
            truth_root=tmp_path,
            day_utc=DAY,
            sleeve_id=SLEEVE_ID,
            sleeve_edge_policy=mismatched_policy,
            canonical_sequence_owner=capalloc.CANONICAL_SEQUENCE_OWNER,
        )


def test_calculation_version_lock_match_and_mismatch(tmp_path: Path) -> None:
    mat = "5" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="f" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8401,
        perm_id=9841,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "calc.execution_event_stream_record.v1.json", order_id=8401, perm_id=9841, engine_id=ENGINE_ID)
    matched_snapshot = _materialize(tmp_path)
    matched = capalloc._load_sleeve_edge_allocator_meta(
        truth_root=tmp_path,
        day_utc=DAY,
        sleeve_id=SLEEVE_ID,
        sleeve_edge_policy=POLICY,
        canonical_sequence_owner=capalloc.CANONICAL_SEQUENCE_OWNER,
    )
    assert matched["snapshot_calculation_version"] == matched_snapshot["calculation_version"]
    mismatched_policy = _policy_copy()
    mismatched_policy["allocator_compatibility"] = {"allowed_calculation_versions": ["sleeve_edge_measurement_v2"]}
    with pytest.raises(SystemExit, match="SLEEVE_EDGE_CALCULATION_VERSION_MISMATCH"):
        capalloc._load_sleeve_edge_allocator_meta(
            truth_root=tmp_path,
            day_utc=DAY,
            sleeve_id=SLEEVE_ID,
            sleeve_edge_policy=mismatched_policy,
            canonical_sequence_owner=capalloc.CANONICAL_SEQUENCE_OWNER,
        )


def test_snapshot_integrity_failure_fails_allocator_closed(tmp_path: Path) -> None:
    mat = "6" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="0" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8501,
        perm_id=9851,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "integrity.execution_event_stream_record.v1.json", order_id=8501, perm_id=9851, engine_id=ENGINE_ID)
    published = _materialize(tmp_path)
    fact_ledger_path = Path(str(published["fact_ledger_ref"]["artifact_path"]))
    fact_ledger_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="SLEEVE_EDGE_SNAPSHOT_INTEGRITY_FAILURE"):
        capalloc._load_sleeve_edge_allocator_meta(
            truth_root=tmp_path,
            day_utc=DAY,
            sleeve_id=SLEEVE_ID,
            sleeve_edge_policy=POLICY,
            canonical_sequence_owner=capalloc.CANONICAL_SEQUENCE_OWNER,
        )


def test_fact_input_hash_mismatch_fails_allocator_closed(tmp_path: Path) -> None:
    mat = "6" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="1" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8511,
        perm_id=9861,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "fact-hash.execution_event_stream_record.v1.json", order_id=8511, perm_id=9861, engine_id=ENGINE_ID)
    published = _materialize(tmp_path)
    snapshot_path = Path(str(published["artifact_path"]))
    snapshot_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot_payload["fact_input_hash"] = "f" * 64
    _write_json(snapshot_path, snapshot_payload)
    with pytest.raises(SystemExit, match="SLEEVE_EDGE_SNAPSHOT_INTEGRITY_FAILURE:FACT_INPUT_HASH_MISMATCH"):
        capalloc._load_sleeve_edge_allocator_meta(
            truth_root=tmp_path,
            day_utc=DAY,
            sleeve_id=SLEEVE_ID,
            sleeve_edge_policy=POLICY,
            canonical_sequence_owner=capalloc.CANONICAL_SEQUENCE_OWNER,
        )


def test_allocator_requires_canonical_sequence_owner(tmp_path: Path) -> None:
    mat = "7" * 64
    trade = _trade_bundle(
        tmp_path,
        materialization_set_id=mat,
        trade_identity_id="2" * 64,
        ownership="CONSTELLATION_OWNED",
        continuity_status="CLOSED",
        order_id=8521,
        perm_id=9871,
        buy_price="100",
        sell_price="160",
        close_utc=f"{DAY}T15:30:00Z",
    )
    _summary(tmp_path, materialization_set_id=mat, evaluation_utc=f"{DAY}T16:00:00Z", trade_refs=[trade])
    _execution_record(tmp_path / "execution_stream_v1" / DAY / "sequence.execution_event_stream_record.v1.json", order_id=8521, perm_id=9871, engine_id=ENGINE_ID)
    _materialize(tmp_path)
    with pytest.raises(SystemExit, match="SLEEVE_EDGE_CANONICAL_SEQUENCE_OWNER_REQUIRED"):
        capalloc._load_sleeve_edge_allocator_meta(
            truth_root=tmp_path,
            day_utc=DAY,
            sleeve_id=SLEEVE_ID,
            sleeve_edge_policy=POLICY,
            canonical_sequence_owner="",
        )
    with pytest.raises(SystemExit, match="SLEEVE_EDGE_CANONICAL_SEQUENCE_OWNER_MISMATCH"):
        capalloc._load_sleeve_edge_allocator_meta(
            truth_root=tmp_path,
            day_utc=DAY,
            sleeve_id=SLEEVE_ID,
            sleeve_edge_policy=POLICY,
            canonical_sequence_owner="ops/tools/run_paper_session_bootstrap_v1.py",
        )


def test_policy_registry_validation_rejects_malformed_structure(tmp_path: Path) -> None:
    registry_path = tmp_path / "governance" / "02_REGISTRIES" / "C2_SLEEVE_EDGE_QUALIFICATION_POLICY_V1.json"
    malformed = _policy_copy()
    malformed.pop("allocator_actions")
    _write_json(registry_path, malformed)
    with pytest.raises(ValueError, match="SLEEVE_EDGE_POLICY_REGISTRY_INVALID:ALLOCATOR_ACTIONS_INVALID"):
        sleeve_edge.load_sleeve_edge_policy_v1(tmp_path)


def test_policy_registry_rejects_unknown_fraction_threshold_until_governed(tmp_path: Path) -> None:
    registry_path = tmp_path / "governance" / "02_REGISTRIES" / "C2_SLEEVE_EDGE_QUALIFICATION_POLICY_V1.json"
    malformed = _policy_copy()
    malformed["unknown_attribution_thresholds"] = {
        "max_unknown_trade_count": 1,
        "max_unknown_trade_fraction": "0.10",
    }
    _write_json(registry_path, malformed)
    with pytest.raises(ValueError, match="SLEEVE_EDGE_POLICY_UNKNOWN_ATTRIBUTION_THRESHOLDS_UNSUPPORTED_KEYS"):
        sleeve_edge.load_sleeve_edge_policy_v1(tmp_path)


def test_orchestrator_stage_order_places_sleeve_edge_before_governed_eval_before_allocation() -> None:
    stages = orch_v2._build_stage_defs(
        truth=Path("/tmp/sleeve-edge-ordering"),
        day=DAY,
        input_day=DAY,
        ib_account="DUO847203",
        git_sha="deadbeef",
        attempt_id="attempt-1",
    )
    stage_ids = [stage.stage_id for stage in stages]
    assert stage_ids.index(orch_v2.SLEEVE_EDGE_PUBLICATION_STAGE_ID) < stage_ids.index(orch_v2.GOVERNED_EVALUATION_STAGE_ID)
    assert stage_ids.index(orch_v2.GOVERNED_EVALUATION_STAGE_ID) < stage_ids.index(orch_v2.CAPITAL_AUTHORITY_STAGE_ID)
    orch_v2._validate_sleeve_edge_publication_order(stages)
    governed_eval_stage = next(stage for stage in stages if stage.stage_id == orch_v2.GOVERNED_EVALUATION_STAGE_ID)
    assert "ops/tools/run_governed_evaluation_day_v1.py" in governed_eval_stage.cmd
    governed_sleeves = [
        governed_eval_stage.cmd[index + 1]
        for index, value in enumerate(governed_eval_stage.cmd[:-1])
        if value == "--sleeve_id"
    ]
    assert governed_sleeves == [
        "C2_TREND_EQ_PRIMARY",
        "C2_VOL_INCOME_DEFINED_RISK",
        "C2_MEAN_REVERSION_EQ",
        "C2_EVENT_DISLOCATION",
        "C2_DEFENSIVE_TAIL",
    ]
    assert "C2_CROSS_ASSET_TREND" not in governed_sleeves
    assert "C2_MARKET_NEUTRAL_SPREAD" not in governed_sleeves
    allocation_stage = next(stage for stage in stages if stage.stage_id == orch_v2.CAPITAL_AUTHORITY_STAGE_ID)
    assert "--canonical_sequence_owner" in allocation_stage.cmd
    assert "ops/tools/run_c2_paper_day_orchestrator_v2.py" in allocation_stage.cmd


def test_orchestrator_stops_after_sleeve_edge_publication_failure() -> None:
    assert orch_v2._should_stop_after_sleeve_edge_publication_failure([]) is False
    assert orch_v2._should_stop_after_sleeve_edge_publication_failure(
        [{"stage_id": orch_v2.SLEEVE_EDGE_PUBLICATION_STAGE_ID, "status": "FAIL"}]
    ) is True
