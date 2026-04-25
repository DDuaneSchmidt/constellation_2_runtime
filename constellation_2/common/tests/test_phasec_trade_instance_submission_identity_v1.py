from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.execution_identity_authority_v1 import (  # noqa: E402
    derive_submission_id_v1,
    derive_trade_instance_id_v1,
)
from constellation_2.phaseC.tools.c2_submit_preflight_offline_v2 import main as preflight_main  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _build_intent() -> dict[str, object]:
    return {
        "schema_id": "equity_intent",
        "schema_version": "v1",
        "intent_id": "c2_intent_20260214_trend_spy_long_open_v1",
        "created_at_utc": "2026-02-14T00:00:00Z",
        "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "suite": "C2_HYBRID_V1", "mode": "PAPER"},
        "underlying": {"symbol": "SPY", "currency": "USD"},
        "intent_type": "EQUITY_LONG_OPEN",
        "sizing": {"target_notional_pct": "0.10", "max_risk_pct": "0.01"},
        "exit_policy": {"policy_id": "c2_equity_time_exit_10d_v1", "time_exit": {"enabled": True, "max_holding_days": 10}},
        "canonical_json_hash": None,
    }


def _build_plan(*, intent_hash: str, intent_sha256: str) -> dict[str, object]:
    return {
        "schema_id": "equity_order_plan",
        "schema_version": "v2",
        "plan_id": "plan-0000000000000001",
        "created_at_utc": "2026-02-14T00:00:00Z",
        "intent_hash": intent_hash,
        "structure": "EQUITY_SPOT",
        "symbol": "SPY",
        "currency": "USD",
        "action": "BUY",
        "qty_shares": 1,
        "order_terms": {"order_type": "LIMIT", "limit_price": "500.00", "time_in_force": "DAY"},
        "risk_proof": None,
        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
        "source_intent_id": "c2_intent_20260214_trend_spy_long_open_v1",
        "intent_sha256": intent_sha256,
        "canonical_json_hash": None,
    }


def _run_preflight(tmp_path: Path, trade_instance_id: str) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    intent = _build_intent()
    intent_path = tmp_path / "intent.json"
    _write_json(intent_path, intent)
    intent_sha256 = hashlib.sha256(intent_path.read_bytes()).hexdigest()
    intent_hash = canonical_hash_for_c2_artifact_v1(intent)
    plan = _build_plan(intent_hash=intent_hash, intent_sha256=intent_sha256)
    plan_path = tmp_path / "plan.json"
    _write_json(plan_path, plan)
    out_dir = tmp_path / trade_instance_id

    rc = preflight_main([
        "--intent", str(intent_path),
        "--equity_order_plan", str(plan_path),
        "--eval_time_utc", "2026-04-14T00:00:00Z",
        "--trade_instance_id", trade_instance_id,
        "--out_dir", str(out_dir),
    ])
    assert rc == 0

    binding = json.loads((out_dir / "binding_record.v2.json").read_text(encoding="utf-8"))
    mapping = json.loads((out_dir / "mapping_ledger_record.v2.json").read_text(encoding="utf-8"))
    decision = json.loads((out_dir / "submit_preflight_decision.v1.json").read_text(encoding="utf-8"))
    return binding, mapping, decision


def test_phasec_preflight_writer_keeps_same_submission_id_for_same_trade_instance(tmp_path: Path) -> None:
    trade_instance_id = derive_trade_instance_id_v1(
        day_utc="2026-04-14",
        attempt_id="A0010",
        sleeve_id="PRIMARY",
        environment="PAPER",
        intent_id="c2_intent_20260214_trend_spy_long_open_v1",
        intent_hash="1" * 64,
    )

    binding_1, mapping_1, decision_1 = _run_preflight(tmp_path / "run1", trade_instance_id)
    binding_2, mapping_2, decision_2 = _run_preflight(tmp_path / "run2", trade_instance_id)

    assert binding_1["submission_id"] == binding_2["submission_id"]
    assert binding_1["trade_instance_id"] == trade_instance_id
    assert mapping_1["trade_instance_id"] == trade_instance_id
    assert decision_1["trade_instance_id"] == trade_instance_id
    assert decision_1["submission_id"] == binding_1["submission_id"]
    assert decision_2["submission_id"] == binding_2["submission_id"]


def test_phasec_preflight_writer_generates_new_submission_id_for_new_trade_instance_same_plan(tmp_path: Path) -> None:
    trade_instance_1 = derive_trade_instance_id_v1(
        day_utc="2026-04-14",
        attempt_id="A0010",
        sleeve_id="PRIMARY",
        environment="PAPER",
        intent_id="c2_intent_20260214_trend_spy_long_open_v1",
        intent_hash="1" * 64,
    )
    trade_instance_2 = derive_trade_instance_id_v1(
        day_utc="2026-04-14",
        attempt_id="A0011",
        sleeve_id="PRIMARY",
        environment="PAPER",
        intent_id="c2_intent_20260214_trend_spy_long_open_v1",
        intent_hash="1" * 64,
    )

    binding_1, _, _ = _run_preflight(tmp_path / "run1", trade_instance_1)
    binding_2, _, _ = _run_preflight(tmp_path / "run2", trade_instance_2)

    assert binding_1["trade_instance_id"] != binding_2["trade_instance_id"]
    assert binding_1["plan_hash"] == binding_2["plan_hash"]
    assert binding_1["submission_id"] == derive_submission_id_v1(
        intent_id="c2_intent_20260214_trend_spy_long_open_v1",
        plan_hash=str(binding_1["plan_hash"]),
        trade_instance_id=trade_instance_1,
    )
    assert binding_2["submission_id"] == derive_submission_id_v1(
        intent_id="c2_intent_20260214_trend_spy_long_open_v1",
        plan_hash=str(binding_2["plan_hash"]),
        trade_instance_id=trade_instance_2,
    )
    assert binding_1["submission_id"] != binding_2["submission_id"]
