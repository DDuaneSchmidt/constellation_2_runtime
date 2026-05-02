from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from constellation_2.common.advisory import execution_package_builder_from_execution_intent_v1 as builder_module
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from ops.tools import run_execution_package_from_authorized_intent_v1 as tool
from ops.tools import run_risk_definition_contract_v1 as risk_tool


DAY = "2026-05-01"
TREND_ID = "c2_trend_eq_spy_2026-05-01_v1"
VOL_ID = "c2_vol_income_iwm_2026-05-01_v1"
TREND_HASH = "a" * 64
VOL_HASH = "b" * 64


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed_capital(root: Path, rows: list[dict]) -> None:
    _write(
        root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json",
        {"decision_chain": {"authorized_trade_intents": rows}},
    )


def _trend_row(*, approved: bool = True) -> dict:
    return {
        "intent_id": TREND_ID,
        "authorized_trade_intent_id": "trend-authz",
        "intent_hash": TREND_HASH,
        "authorization_outcome": "APPROVED" if approved else "REJECTED",
        "authorized_quantity": 1 if approved else 0,
        "execution_sleeve_id": "PRIMARY",
        "account_id": "DUO847203",
        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
        "symbol": "SPY",
        "action_type": "OPEN",
        "reason_codes": [] if approved else ["BUNDLE_B_HEADROOM_REJECTED"],
    }


def _vol_row(*, approved: bool = False) -> dict:
    return {
        "intent_id": VOL_ID,
        "authorized_trade_intent_id": "vol-authz",
        "intent_hash": VOL_HASH,
        "authorization_outcome": "APPROVED" if approved else "REJECTED",
        "authorized_quantity": 1 if approved else 0,
        "execution_sleeve_id": "PRIMARY",
        "account_id": "DUO847203",
        "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
        "symbol": "IWM",
        "action_type": "OPEN",
        "reason_codes": [
            "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE",
            "BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN",
            "BUNDLE_B_REQUESTED_QUANTITY_ZERO",
        ],
    }


def _seed_intents(root: Path) -> None:
    _write(
        root / "intents_v1" / "snapshots" / DAY / f"{TREND_HASH}.exposure_intent.v1.json",
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": TREND_ID,
            "exposure_type": "LONG_EQUITY",
            "underlying": {"symbol": "SPY", "currency": "USD"},
            "target_notional_pct": "0.01",
            "constraints": {"max_risk_pct": "0.01", "stop_loss_bps": 1000},
        },
    )
    _write(
        root / "intents_v1" / "snapshots" / DAY / f"{VOL_HASH}.exposure_intent.v1.json",
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": VOL_ID,
            "exposure_type": "SHORT_VOL_DEFINED",
            "underlying": {"symbol": "IWM", "currency": "USD"},
        },
    )


def _seed_contract(root: Path, *, intent_hash: str, intent_id: str, risk_type: str) -> None:
    git_sha = risk_tool._git_sha()  # noqa: SLF001 - fixtures must match governed loader freshness checks.
    payload = {
        "schema_id": "risk_definition_contract_v1",
        "schema_version": "v1",
        "contract_id": "risk-contract-" + intent_hash[:16],
        "day_utc": DAY,
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1" if risk_type == "STOP_BASED" else "C2_VOL_INCOME_DEFINED_RISK_V1",
        "intent_id": intent_id,
        "intent_hash": intent_hash,
        "instrument": {"kind": "EQUITY" if risk_type == "STOP_BASED" else "OPTION_STRATEGY", "symbol": "SPY" if risk_type == "STOP_BASED" else "IWM", "currency": "USD"},
        "contract_type": risk_type,
        "risk_type": risk_type,
        "source_intent_path": str((root / "intents_v1" / "snapshots" / DAY / f"{intent_hash}.exposure_intent.v1.json").resolve()),
        "generated_at": f"{DAY}T00:00:00Z",
        "git_commit": git_sha,
        "truth_root": str(root.resolve()),
        "producer": {"repo": "constellation", "module": "ops/tools/run_risk_definition_contract_v1.py", "git_sha": git_sha},
        "validation_status": "PASS",
        "blockers": [],
        "stop_loss_bps": 1000 if risk_type == "STOP_BASED" else None,
        "stop_loss_price": "450.00" if risk_type == "STOP_BASED" else None,
        "reference_price_source": "/tmp/SPY.market_data_snapshot.v1.json" if risk_type == "STOP_BASED" else None,
        "reference_price": "500.00" if risk_type == "STOP_BASED" else None,
        "risk_per_unit": 5000 if risk_type == "STOP_BASED" else 40000,
        "quantity_basis": {"basis": "ONE_UNIT_STOP_RISK_BOOTSTRAP" if risk_type == "STOP_BASED" else "DEFINED_RISK_CONTRACTS", "quantity": 1},
        "structure_type": None if risk_type == "STOP_BASED" else "VERTICAL_SPREAD",
        "strikes": None if risk_type == "STOP_BASED" else [{"action": "SELL", "right": "PUT", "strike": "190.00", "expiry": "2026-05-15T00:00:00Z"}],
        "expiry": None if risk_type == "STOP_BASED" else "2026-05-15T00:00:00Z",
        "max_loss": None if risk_type == "STOP_BASED" else 40000,
        "max_gain": None,
        "breakeven": None,
        "options_chain_ref": None if risk_type == "STOP_BASED" else "/tmp/options_chain_snapshot.v1.json",
        "order_plan_ref": None if risk_type == "STOP_BASED" else {"path": "/tmp/order_plan.v1.json", "plan_id": "plan-1234567890abcdef", "sha256": "a" * 64},
        "defined_risk_proof": None if risk_type == "STOP_BASED" else {"source": "order_plan.risk_proof", "defined_risk_proven": True, "max_loss_usd": "400.00", "contracts": 1, "width_points": "5.00", "multiplier": 100},
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    _write(root / "risk_definition_contract_v1" / DAY / intent_hash / "risk_definition_contract.v1.json", payload)


def test_approved_trend_stage_carries_source_stop_into_selected_plan(monkeypatch, tmp_path: Path) -> None:
    _seed_capital(tmp_path, [_trend_row()])
    _seed_intents(tmp_path)
    _seed_contract(tmp_path, intent_hash=TREND_HASH, intent_id=TREND_ID, risk_type="STOP_BASED")
    _write(
        tmp_path / "intents_v1" / "snapshots" / DAY / f"{TREND_HASH}.exposure_intent.v1.json",
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": TREND_ID,
            "exposure_type": "LONG_EQUITY",
            "underlying": {"symbol": "SPY", "currency": "USD"},
            "target_notional_pct": "0.01",
            "expected_holding_days": 20,
            "constraints": {"max_risk_pct": "0.01", "stop_loss_bps": 1000},
        },
    )
    _write(
        tmp_path / "market_data_snapshot_v1" / "snapshots" / DAY / "SPY.market_data_snapshot.v1.json",
        {"schema_id": "C2_MARKET_DATA_SNAPSHOT_V1", "day_utc": DAY, "symbol": "SPY", "close": "500.00"},
    )
    monkeypatch.setattr(builder_module, "resolve_truth_sleeves_root", lambda: (tmp_path / "truth_sleeves").resolve())
    monkeypatch.setattr(builder_module, "resolve_governed_account_binding", lambda **kwargs: SimpleNamespace(ib_account="DUO847203"))
    package_path = tmp_path / "execution_package_v1" / DAY / "submission" / "execution_package.v1.json"
    build_path = tmp_path / "reports" / "execution_build_v1" / DAY / "submission" / "execution_build.v1.json"
    observed: dict[str, object] = {}

    def fake_build(**kwargs):
        candidate_path = Path(kwargs["candidate_path"]).resolve()
        observed["plan"] = json.loads((candidate_path / "equity_order_plan.v2.json").read_text(encoding="utf-8"))
        return {
            "build_obj": {"closure_status": "COMPLETE"},
            "package_path": str(package_path),
            "build_path": str(build_path),
        }

    monkeypatch.setattr(tool, "run_execution_build_authority_v1", fake_build)

    result = tool.build_execution_package_from_authorized_intent_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        intent_id=TREND_ID,
    )

    plan = observed["plan"]
    assert result["status"] == "PASS"
    assert plan["order_terms"] == {"limit_price": "500.00", "order_type": "LIMIT", "time_in_force": "DAY"}
    assert plan["protective_stop"] == {
        "basis": "ENTRY_REFERENCE_PRICE",
        "order_type": "STOP",
        "stop_loss_bps": 1000,
        "stop_price": "450.00",
        "time_in_force": "DAY",
    }
    assert plan["bracket"]["enabled"] is True


def test_approved_trend_uses_governed_build_authority(monkeypatch, tmp_path: Path) -> None:
    _seed_capital(tmp_path, [_trend_row()])
    _seed_intents(tmp_path)
    _seed_contract(tmp_path, intent_hash=TREND_HASH, intent_id=TREND_ID, risk_type="STOP_BASED")
    package_path = tmp_path / "execution_package_v1" / DAY / "submission" / "execution_package.v1.json"
    build_path = tmp_path / "reports" / "execution_build_v1" / DAY / "submission" / "execution_build.v1.json"
    calls: dict[str, object] = {}

    def fake_stage(*, repo_root: Path, execution_intent):
        calls["execution_intent_id"] = execution_intent.execution_intent_id
        calls["quantity_shares"] = execution_intent.quantity_shares
        candidate = tmp_path / "candidate.json"
        _write(candidate, {"candidate": True})
        return {"candidate_path": str(candidate)}

    def fake_build(**kwargs):
        calls["materialize"] = kwargs["materialize"]
        calls["emit_package"] = kwargs["emit_package"]
        return {
            "build_obj": {"closure_status": "COMPLETE"},
            "package_path": str(package_path),
            "build_path": str(build_path),
        }

    monkeypatch.setattr(tool, "stage_candidate_from_execution_intent_v1", fake_stage)
    monkeypatch.setattr(tool, "run_execution_build_authority_v1", fake_build)

    result = tool.build_execution_package_from_authorized_intent_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        intent_id=TREND_ID,
    )

    assert result["status"] == "PASS"
    assert result["package_path"] == str(package_path)
    assert calls == {
        "execution_intent_id": TREND_ID,
        "quantity_shares": 1,
        "materialize": True,
        "emit_package": True,
    }


def test_approved_trend_without_risk_contract_cannot_build_package(tmp_path: Path) -> None:
    _seed_capital(tmp_path, [_trend_row()])
    _seed_intents(tmp_path)

    result = tool.build_execution_package_from_authorized_intent_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        intent_id=TREND_ID,
    )

    assert result["status"] == "BLOCKED"
    assert result["blocker"] == "RISK_DEFINITION_CONTRACT_MISSING"
    assert result["package_path"] is None


def test_rejected_vol_cannot_produce_package_and_exposes_defined_risk_recovery(tmp_path: Path) -> None:
    _seed_capital(tmp_path, [_vol_row()])
    _seed_intents(tmp_path)

    result = tool.build_execution_package_from_authorized_intent_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        intent_id=VOL_ID,
    )

    assert result["status"] == "BLOCKED"
    assert result["blocker"] == "AUTHORIZED_INTENT_NOT_APPROVED"
    assert result["package_path"] is None
    assert result["details"]["defined_risk_evidence_required"] is True
    assert result["details"]["missing_fields"] == [
        "structure type",
        "strikes",
        "expiry",
        "max loss",
        "quantity basis",
        "options-chain ref",
    ]
    assert "run_options_chain_snapshot_required_day_v1.py" in result["details"]["recovery_command"]


def test_approved_defined_risk_intent_still_requires_governed_options_evidence(tmp_path: Path) -> None:
    _seed_capital(tmp_path, [_vol_row(approved=True)])
    _seed_intents(tmp_path)
    _seed_contract(tmp_path, intent_hash=VOL_HASH, intent_id=VOL_ID, risk_type="DEFINED_RISK")

    result = tool.build_execution_package_from_authorized_intent_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        intent_id=VOL_ID,
    )

    assert result["status"] == "BLOCKED"
    assert result["blocker"] == "DEFINED_RISK_EXECUTION_PACKAGE_REQUIRES_GOVERNED_OPTIONS_EVIDENCE"
    assert result["package_path"] is None


def test_blocked_trend_build_exposes_execution_chain_map(monkeypatch, tmp_path: Path) -> None:
    _seed_capital(tmp_path, [_trend_row()])
    _seed_intents(tmp_path)
    _seed_contract(tmp_path, intent_hash=TREND_HASH, intent_id=TREND_ID, risk_type="STOP_BASED")
    _write(
        tmp_path / "target_day_admission_v1" / f"{DAY}.json",
        {
            "admission_status": "BLOCKED",
            "blocking_reason_codes": ["REQUIRED_GATE_FAIL"],
            "blocker_chain": [{"artifact_id": "startup_materialization_input_convergence_v1", "blocker_code": "REQUIRED_GATE_FAIL"}],
        },
    )
    build_path = tmp_path / "reports" / "execution_build_v1" / DAY / "submission" / "execution_build.v1.json"

    def fake_stage(*, repo_root: Path, execution_intent):
        candidate = tmp_path / "candidate.json"
        _write(candidate, {"candidate": True})
        return {"candidate_path": str(candidate)}

    def fake_build(**kwargs):
        return {
            "build_obj": {
                "closure_status": "BLOCKED",
                "submission_id": "submission",
                "candidate_ref": {
                    "canonical_truth_root": str(tmp_path),
                    "execution_truth_root": str(tmp_path),
                    "sleeve_id": "PRIMARY",
                    "environment": "PAPER",
                },
                "first_real_blocker": {
                    "dependency_id": "global_context_package_v1",
                    "path": str(tmp_path / "global_context_package.v1.json"),
                },
                "blocking_chain": [{"dependency_id": "global_context_package_v1", "status": "FAILED"}],
                "materializable_now": ["global_context_package_v1"],
            },
            "build_path": str(build_path),
            "package_path": "",
        }

    monkeypatch.setattr(tool, "stage_candidate_from_execution_intent_v1", fake_stage)
    monkeypatch.setattr(tool, "run_execution_build_authority_v1", fake_build)

    result = tool.build_execution_package_from_authorized_intent_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        intent_id=TREND_ID,
    )

    assert result["status"] == "BLOCKED"
    chain = result["details"]["chain_map"]
    assert [node["artifact"] for node in chain] == [
        "target_day_admission_v1",
        "day_activation_package_v1",
        "global_context_package_v1",
        "execution_build_v1",
        "execution_package_v1",
    ]
    assert chain[0]["status"] == "BLOCKED"
    assert chain[0]["blocker"] == "REQUIRED_GATE_FAIL"
    assert "run_session_authority_v1.py" in chain[0]["recovery_command"]
    assert chain[-1]["status"] == "MISSING"
    assert chain[-1]["blocker"] == "EXECUTION_BUILD_NOT_COMPLETE"
