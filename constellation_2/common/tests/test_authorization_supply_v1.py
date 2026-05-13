from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_aegis_bod_prepare_v1 as bod  # noqa: E402
import ops.tools.run_aegis_day_v1 as day_run  # noqa: E402
import ops.tools.run_authorization_supply_v1 as auth  # noqa: E402
import ops.tools.run_intent_arbitration_v1 as arbitration  # noqa: E402
import ops.tools.run_structure_decision_supply_v1 as structure_supply  # noqa: E402

DAY = "2026-04-29"


def _ctx(tmp_path: Path, day: str = DAY) -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(
        day_utc=day,
        environment="PAPER",
        truth_root=truth,
        execution_root=execution,
        runtime_root=runtime,
        operator_input_root=operator,
        ib_account="DUO847203",
    )


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _intent(ctx: bod.BodContext, *, intent_id: str = "intent-1", structure: bool = True, day: str | None = None) -> Path:
    payload = {
        "day_utc": day or ctx.day_utc,
        "intent_id": intent_id,
        "intent_hash": "a" * 64,
        "underlying": {"symbol": "SPY"},
        "intent_type": "DEFINED_RISK_OPTIONS",
        "requires_defined_risk": True,
        "engine": {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "suite": "C2_HYBRID_V1", "mode": "PAPER"},
        "exposure_type": "SHORT_VOL_DEFINED",
        "option": {"direction": "SELL", "structure": "PUT"},
        "expected_holding_days": 7,
    }
    if structure:
        payload["option_structure"] = {"selected_structure": "VERTICAL_SPREAD", "legs": [{"right": "CALL"}, {"right": "CALL"}]}
    return _write(ctx.execution_root / "intents_v1" / "snapshots" / (day or ctx.day_utc) / f"{'a' * 64}.exposure_intent.v1.json", payload)


def _market_supply(ctx: bod.BodContext, status: str = "PASS", blocker: str = "") -> Path:
    return _write(
        ctx.truth_root / "reports" / "market_data_supply_v1" / ctx.day_utc / "market_data_supply.v1.json",
        {"day_utc": ctx.day_utc, "status": status, "canonical_blocker": blocker, "operator_next_action": "fix market data"},
    )


def _risk_budget(ctx: bod.BodContext, status: str = "PASS", blocker: str = "") -> Path:
    return _write(
        ctx.truth_root / "reports" / "risk_budget_supply_v1" / ctx.day_utc / "risk_budget_supply.v1.json",
        {
            "day_utc": ctx.day_utc,
            "status": status,
            "canonical_blocker": blocker,
            "operator_next_action": "fix risk budget",
            "intent_budgets": [{"intent_id": "intent-1", "instrument": "SPY", "allowed_risk_cents": 25000}],
        },
    )


def _strategy(ctx: bod.BodContext, state: str = "INTENT_CREATED") -> Path:
    return _write(
        ctx.truth_root / "reports" / "strategy_decision_authority_v1" / ctx.day_utc / "strategy_decision_authority.v1.json",
        {"day_utc": ctx.day_utc, "status": "PASS", "strategy_decision_state": state, "intent_count": 1},
    )


def _order_plan(
    ctx: bod.BodContext,
    *,
    day: str | None = None,
    defined: bool = True,
    max_loss_usd: object = "73.00",
    width_points: object = "1",
    contracts: object = 1,
    risk_proof: dict | None | object = None,
) -> Path:
    identity_dir = ctx.execution_root / "phaseC_preflight_v1" / (day or ctx.day_utc) / "attempt_000001" / ("a" * 64)
    if risk_proof is None:
        risk_payload = {
            "defined_risk_proven": defined,
            "max_loss_usd": max_loss_usd,
            "width_points": width_points,
            "contracts": contracts,
            "multiplier": 100,
        }
    elif isinstance(risk_proof, dict):
        risk_payload = risk_proof
    else:
        risk_payload = None
    payload = {
        "schema_id": "order_plan",
        "schema_version": "v1",
        "structure": "VERTICAL_SPREAD",
        "legs": [
            {"action": "SELL", "expiry_utc": "2026-04-30T00:00:00Z", "right": "PUT", "strike": "706.00", "ratio": 1, "ib_conId": 1},
            {"action": "BUY", "expiry_utc": "2026-04-30T00:00:00Z", "right": "PUT", "strike": "705.00", "ratio": 1, "ib_conId": 2},
        ],
    }
    if risk_payload is not None:
        payload["risk_proof"] = risk_payload
    return _write(identity_dir / "order_plan.v1.json", payload)


def _identity(
    ctx: bod.BodContext,
    *,
    defined: bool = True,
    day: str | None = None,
    order_plan: Path | None = None,
    order_plan_sha: str | None = None,
    include_order_plan_ref: bool = True,
    identity_risk_fields: bool = False,
) -> Path:
    if order_plan is None and include_order_plan_ref:
        order_plan = _order_plan(ctx, defined=defined, day=day)
    source_refs = []
    if include_order_plan_ref and order_plan is not None:
        source_refs.append({"ref_type": "order_plan_ref", "path": str(order_plan), "sha256": order_plan_sha or _sha256(order_plan)})
    payload = {
        "schema_id": "execution_identity_record",
        "day_utc": day or ctx.day_utc,
        "intent_id": "intent-1",
        "source_refs": source_refs,
    }
    if identity_risk_fields:
        payload["risk_proof"] = {"defined_risk_proven": defined}
    return _write(
        ctx.execution_root / "phaseC_preflight_v1" / (day or ctx.day_utc) / "attempt_000001" / ("a" * 64) / "execution_identity_record.v1.json",
        payload,
    )


def _authorization(ctx: bod.BodContext, *, status: str = "AUTHORIZED", day: str | None = None) -> Path:
    decision = "AUTHORIZED" if status == "AUTHORIZED" else "REJECTED"
    return _write(
        ctx.execution_root / "engine_activity_v1" / "authorization_v1" / (day or ctx.day_utc) / f"{'a' * 64}.authorization.v1.json",
        {
            "schema_id": "C2_AUTHORIZATION_V1",
            "day_utc": day or ctx.day_utc,
            "intent_id": "intent-1",
            "status": status,
            "authorization": {"decision": decision},
            "reason_codes": [] if status == "AUTHORIZED" else ["REJECTED"],
        },
    )


def _structure_supply(ctx: bod.BodContext, *, day: str | None = None, intent_id: str = "intent-1") -> Path:
    day_utc = day or ctx.day_utc
    return _write(
        ctx.truth_root / "reports" / "structure_decision_supply_v1" / day_utc / "structure_decision_supply.v1.json",
        {
            "day_utc": day_utc,
            "status": "PASS",
            "canonical_blocker": "",
            "structure_export": {
                "usable_for_authorization_supply": True,
                "decision_count": 1,
                "decisions": [{"intent_id": intent_id, "selected_structure": "VERTICAL_SPREAD", "legs": [{"right": "PUT"}, {"right": "PUT"}]}],
            },
        },
    )


def _selected_pointer(ctx: bod.BodContext, intent_path: Path) -> Path:
    return _write(
        arbitration.selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc),
        {
            "schema_id": "selected_intent_pointer",
            "schema_version": "v1",
            "day_utc": ctx.day_utc,
            "environment": "PAPER",
            "status": "SELECTED",
            "canonical_blocker": "",
            "selected_intent": {
                "intent_id": "intent-1",
                "intent_hash": "a" * 64,
                "intent_path": str(intent_path),
                "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                "symbol": "SPY",
            },
        },
    )


def _market_gate_with_snapshot(ctx: bod.BodContext, *, quotes: bool = True) -> Path:
    snap_root = ctx.execution_root / "options_chain_snapshot_v1" / ctx.day_utc / "capture"
    snap_root.mkdir(parents=True, exist_ok=True)
    contracts = [
        {"right": "PUT", "strike": "105.00", "bid": "3.10" if quotes else "", "ask": "3.15" if quotes else "", "expiry_utc": f"{ctx.day_utc}T00:00:00Z"},
        {"right": "PUT", "strike": "100.00", "bid": "0.55" if quotes else "", "ask": "0.60" if quotes else "", "expiry_utc": f"{ctx.day_utc}T00:00:00Z"},
    ]
    snap = _write(
        snap_root / "options_chain_snapshot.v1.json",
        {
            "schema_id": "options_chain_snapshot",
            "schema_version": 1,
            "as_of_utc": f"{ctx.day_utc}T14:30:00Z",
            "underlying": {"symbol": "SPY", "spot_price": "110.00", "spot_as_of_utc": f"{ctx.day_utc}T14:30:00Z"},
            "contracts": contracts,
            "provenance": {"source": "TEST"},
        },
    )
    cert = _write(snap_root / "freshness_certificate.v1.json", {"valid_until_utc": "2099-01-01T00:00:00Z"})
    return _write(
        ctx.truth_root / "reports" / "market_open_data_gate_v1" / ctx.day_utc / "market_open_data_gate.v1.json",
        {"day_utc": ctx.day_utc, "status": "PASS", "snapshot_path": str(snap), "freshness_certificate_path": str(cert)},
    )


def _no_external(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(auth, "_run_command", lambda *_args, **_kwargs: {"exit_code": 0})
    monkeypatch.setattr(auth, "_run_phasec_identity_materializer", lambda *_args, **_kwargs: {"exit_code": 0})
    monkeypatch.setattr(auth, "_run_authorization_artifacts", lambda *_args, **_kwargs: {"exit_code": 0})


def test_no_active_intent_blocks(tmp_path: Path) -> None:
    payload = auth.build_authorization_supply_v1(_ctx(tmp_path))
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "ACTIVE_INTENT_MISSING"


def test_market_data_supply_blocked_prevents_phasec(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx, "BLOCKED", "OPTIONS_MARKET_DATA_PERMISSION_DENIED")
    called = False

    def _phasec(_ctx):  # noqa: ANN001
        nonlocal called
        called = True

    monkeypatch.setattr(auth, "_run_phasec_identity_materializer", _phasec)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "MARKET_DATA_SUPPLY_BLOCKED"
    assert payload["phasec_defined_risk"]["status"] == "SKIPPED"
    assert called is False


def test_market_data_supply_skipped_does_not_satisfy_active_intent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx, "SKIPPED", "")
    called = False

    def _phasec(_ctx):  # noqa: ANN001
        nonlocal called
        called = True

    monkeypatch.setattr(auth, "_run_phasec_identity_materializer", _phasec)
    payload = auth.build_authorization_supply_v1(ctx)

    assert payload["canonical_blocker"] == "MARKET_DATA_SUPPLY_BLOCKED"
    assert payload["market_data_input"]["status"] == "SKIPPED"
    assert called is False


def test_risk_budget_supply_blocked_prevents_phasec(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx, "BLOCKED", "NAV_BASIS_INVALID")
    called = False

    def _phasec(_ctx):  # noqa: ANN001
        nonlocal called
        called = True

    monkeypatch.setattr(auth, "_run_phasec_identity_materializer", _phasec)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "RISK_BUDGET_SUPPLY_BLOCKED"
    assert called is False


def test_missing_strategy_decision_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "STRATEGY_DECISION_MISSING"


def test_missing_structure_decision_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx, structure=False)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "STRUCTURE_DECISION_MISSING"
    assert payload["structure_decision"]["producer_command"].startswith("python3 ops/tools/run_structure_decision_supply_v1.py")


def test_valid_current_day_structure_decision_allows_phasec(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx, structure=False)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    _structure_supply(ctx)
    _identity(ctx)
    _authorization(ctx)

    payload = auth.build_authorization_supply_v1(ctx)

    assert payload["status"] == "PASS"
    assert payload["structure_decision"]["status"] == "PASS"
    assert payload["phasec_defined_risk"]["status"] == "PASS"


def test_wrong_day_structure_decision_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx, structure=False)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    _structure_supply(ctx, day="2026-04-28")

    payload = auth.build_authorization_supply_v1(ctx)

    assert payload["canonical_blocker"] == "STRUCTURE_DECISION_MISSING"


def test_structure_selection_uses_current_day_options_snapshot(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    intent_path = _intent(ctx, structure=False)
    _selected_pointer(ctx, intent_path)
    _risk_budget(ctx)
    gate = _market_gate_with_snapshot(ctx)

    payload = structure_supply.build_structure_decision_supply_v1(ctx)

    assert payload["status"] == "PASS"
    assert payload["market_open_data"]["market_open_data_gate_path"] == str(gate)
    decision = payload["structure_decisions"][0]
    assert decision["pricing_inputs"]["snapshot_path"].endswith("options_chain_snapshot.v1.json")
    assert decision["option_structure"]["legs"][0]["action"] == "SELL"
    assert decision["option_structure"]["legs"][1]["action"] == "BUY"


def test_structure_selection_no_eligible_option_structure_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    intent_path = _intent(ctx, structure=False)
    _selected_pointer(ctx, intent_path)
    _risk_budget(ctx)
    _market_gate_with_snapshot(ctx, quotes=False)

    payload = structure_supply.build_structure_decision_supply_v1(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "NO_ELIGIBLE_OPTION_STRUCTURE"
    diagnostics = payload["structure_diagnostics"][0]
    assert diagnostics["intent_id"] == "intent-1"
    assert diagnostics["selected_symbol"] == "SPY"
    assert diagnostics["option_chain_snapshot_path"].endswith("options_chain_snapshot.v1.json")
    assert diagnostics["candidates_seen"] == 0
    assert diagnostics["candidates_eligible"] == 0
    assert diagnostics["rejected_by_reason"]["NO_LIQUID_CONTRACTS"] == 2


def test_blocked_selected_intent_pointer_supersedes_stale_raw_intent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx, structure=False)
    pointer_path = arbitration.selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    _write(
        pointer_path,
        {
            "schema_id": "selected_intent_pointer",
            "schema_version": "v1",
            "day_utc": ctx.day_utc,
            "environment": "PAPER",
            "status": "BLOCKED",
            "canonical_blocker": "ALLOWED_SYMBOL_MISMATCH",
            "selected_intent": {},
            "cycle_id": "scan_test",
        },
    )

    payload = auth.build_authorization_supply_v1(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "ALLOWED_SYMBOL_MISMATCH"
    assert payload["active_intents"] == []
    assert payload["strategy_decision"]["selected_intent_pointer"]["path"] == str(pointer_path)


def test_missing_execution_identity_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "PHASEC_EXECUTION_IDENTITY_MISSING"


def test_skip_phasec_materialization_policy_does_not_run_materializer(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    called = False

    def _phasec(_ctx):  # noqa: ANN001
        nonlocal called
        called = True
        return {"exit_code": 0}

    monkeypatch.setenv("AEGIS_SKIP_PHASEC_MATERIALIZATION", "YES")
    monkeypatch.setattr(auth, "_run_command", lambda *_args, **_kwargs: {"exit_code": 0})
    monkeypatch.setattr(auth, "_run_phasec_identity_materializer", _phasec)

    payload = auth.build_authorization_supply_v1(ctx)

    assert payload["canonical_blocker"] == "PHASEC_EXECUTION_IDENTITY_MISSING"
    assert payload["phasec_defined_risk"]["materialization_skipped_by_policy"] is True
    assert called is False


def test_defined_risk_false_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    _identity(ctx, defined=False)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "PHASEC_DEFINED_RISK_NOT_PROVEN"
    assert payload["phasec_defined_risk"]["risk_proof"]["blocker"] == "ORDER_PLAN_DEFINED_RISK_NOT_TRUE"


def test_phasec_order_plan_ref_risk_proof_passes_without_identity_risk_fields(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    _identity(ctx, identity_risk_fields=False)
    _authorization(ctx)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["status"] == "PASS"
    assert payload["phasec_defined_risk"]["status"] == "PASS"
    assert payload["phasec_defined_risk"]["defined_risk_proven"] is True


def test_missing_order_plan_ref_blocks_defined_risk(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    _identity(ctx, include_order_plan_ref=False, identity_risk_fields=True)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "PHASEC_DEFINED_RISK_NOT_PROVEN"
    assert payload["phasec_defined_risk"]["risk_proof"]["blocker"] == "ORDER_PLAN_REF_MISSING"


def test_missing_order_plan_file_blocks_defined_risk(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    order_plan = _order_plan(ctx)
    order_plan_hash = _sha256(order_plan)
    order_plan.unlink()
    _identity(ctx, order_plan=order_plan, order_plan_sha=order_plan_hash)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "PHASEC_DEFINED_RISK_NOT_PROVEN"
    assert payload["phasec_defined_risk"]["risk_proof"]["blocker"] == "ORDER_PLAN_REF_UNREADABLE"


def test_order_plan_hash_mismatch_blocks_defined_risk(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    order_plan = _order_plan(ctx)
    _identity(ctx, order_plan=order_plan, order_plan_sha="0" * 64)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "PHASEC_DEFINED_RISK_NOT_PROVEN"
    assert payload["phasec_defined_risk"]["risk_proof"]["blocker"] == "ORDER_PLAN_REF_HASH_MISMATCH"


def test_missing_order_plan_risk_proof_blocks_defined_risk(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    order_plan = _order_plan(ctx, risk_proof={})
    _identity(ctx, order_plan=order_plan)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "PHASEC_DEFINED_RISK_NOT_PROVEN"
    assert payload["phasec_defined_risk"]["risk_proof"]["blocker"] == "ORDER_PLAN_RISK_PROOF_MISSING"


@pytest.mark.parametrize(
    ("field", "value", "blocker"),
    [
        ("max_loss_usd", "0", "max_loss_usd_INVALID"),
        ("max_loss_usd", "not-a-number", "max_loss_usd_INVALID"),
        ("width_points", "0", "width_points_INVALID"),
        ("width_points", "not-a-number", "width_points_INVALID"),
        ("contracts", 0, "contracts_INVALID"),
    ],
)
def test_invalid_order_plan_risk_proof_numeric_fields_block_defined_risk(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    field: str,
    value: object,
    blocker: str,
) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    kwargs = {"max_loss_usd": "73.00", "width_points": "1", "contracts": 1}
    kwargs[field] = value
    _identity(ctx, order_plan=_order_plan(ctx, **kwargs))
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "PHASEC_DEFINED_RISK_NOT_PROVEN"
    assert payload["phasec_defined_risk"]["risk_proof"]["blocker"] == blocker


def test_missing_authorization_evidence_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    _identity(ctx)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "MISSING_EVIDENCE"


def test_authorization_rejected_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    _identity(ctx)
    _authorization(ctx, status="REJECTED")
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "AUTHORIZATION_REJECTED"


def test_stale_authorization_artifact_blocks_as_stale_not_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    stale_auth = _authorization(ctx, status="REJECTED")
    _identity(ctx)
    identity_path = next((ctx.execution_root / "phaseC_preflight_v1" / ctx.day_utc).glob("attempt_*/*/execution_identity_record.v1.json"))
    # Force the rejected authorization to be older than the PhaseC identity it depends on.
    os.utime(stale_auth, (1000, 1000))
    os.utime(identity_path, (2000, 2000))

    payload = auth.build_authorization_supply_v1(ctx)

    assert payload["canonical_blocker"] == "STALE_ARTIFACT"
    assert payload["authorization"]["artifact_freshness"]["stale_dependency_path"]


def test_valid_authorization_supply_passes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    _identity(ctx)
    _authorization(ctx)
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["status"] == "PASS"
    assert payload["authorization_export"]["usable_for_submit_readiness"] is True


def test_wrong_day_phasec_and_authorization_do_not_satisfy_current_day(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx)
    _risk_budget(ctx)
    _strategy(ctx)
    _identity(ctx, day="2026-04-28")
    _authorization(ctx, day="2026-04-28")
    payload = auth.build_authorization_supply_v1(ctx)
    assert payload["canonical_blocker"] == "PHASEC_EXECUTION_IDENTITY_MISSING"


def test_run_authorization_supply_writes_only_runtime_artifact(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _no_external(monkeypatch)
    ctx = _ctx(tmp_path)
    _intent(ctx)
    _market_supply(ctx, "BLOCKED", "OPTIONS_MARKET_DATA_PERMISSION_DENIED")
    monkeypatch.setattr(auth.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    path, payload = auth.run_authorization_supply_v1(ctx.day_utc, ctx.environment)
    assert path == ctx.truth_root / "reports" / "authorization_supply_v1" / ctx.day_utc / "authorization_supply.v1.json"
    assert payload["canonical_blocker"] == "MARKET_DATA_SUPPLY_BLOCKED"
    assert REPO_ROOT not in path.parents


def test_day_ledger_does_not_run_risk_sizing_before_authorization_pass(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    phase_ctx = day_run.PhaseContext(
        day_utc=DAY,
        environment="PAPER",
        truth_root=tmp_path / "truth",
        execution_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        runtime_root=tmp_path / "runtime",
        operator_input_root=tmp_path / "operator",
        ib_account="DUO847203",
    )
    for path in (phase_ctx.truth_root, phase_ctx.execution_root, phase_ctx.runtime_root, phase_ctx.operator_input_root):
        path.mkdir(parents=True, exist_ok=True)
    _write(phase_ctx.truth_root / "reports" / "capital_supply_v1" / DAY / "capital_supply.v1.json", {"day_utc": DAY, "status": "PASS"})
    _write(phase_ctx.truth_root / "reports" / "risk_budget_supply_v1" / DAY / "risk_budget_supply.v1.json", {"day_utc": DAY, "status": "PASS"})
    _write(phase_ctx.truth_root / "reports" / "authorization_supply_v1" / DAY / "authorization_supply.v1.json", {"day_utc": DAY, "status": "BLOCKED", "canonical_blocker": "AUTHORIZATION_EVIDENCE_MISSING"})
    called: list[str] = []

    def _fake_run_steps(_phase, commands, **_kwargs):  # noqa: ANN001, ANN002
        step = commands[0][0]
        called.append(step)
        blocker = "AUTHORIZATION_EVIDENCE_MISSING" if step == "authorization_supply" else ""
        status = "BLOCKED" if blocker else "PASS"
        return ([{"step_name": step, "status": status, "blocker": blocker, "duration_ms": 1}], [], [blocker] if blocker else [])

    monkeypatch.setattr(day_run, "_run_steps", _fake_run_steps)
    row = day_run._phase_authorization_final(phase_ctx, {})
    assert row["canonical_blocker"] == "AUTHORIZATION_EVIDENCE_MISSING"
    assert "risk_sizing_authority" not in called
