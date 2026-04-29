from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.aegis_chatgpt_packet as packet  # noqa: E402
import ops.tools.run_aegis_day_v1 as day_run  # noqa: E402
import ops.tools.run_risk_budget_supply_v1 as supply  # noqa: E402
from ops.tools import run_aegis_bod_prepare_v1 as bod  # noqa: E402


def _ctx(tmp_path: Path, day: str = "2026-04-29") -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(day, "PAPER", truth, execution, runtime, operator, "DU123456")


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _capital_supply(
    ctx: bod.BodContext,
    *,
    status: str = "PASS",
    blocker: str = "",
    nav: int | None = 100_000_00,
    cash: int | None = 90_000_00,
    day: str | None = None,
) -> Path:
    day_utc = day or ctx.day_utc
    return _write_json(
        ctx.truth_root / "reports" / "capital_supply_v1" / day_utc / "capital_supply.v1.json",
        {
            "day_utc": day_utc,
            "environment": "PAPER",
            "status": status,
            "canonical_blocker": blocker,
            "selected_source": {
                "source_type": "BROKER_ACCOUNT",
                "cash_total_cents": cash,
                "net_liquidation_cents": nav,
                "trust_level": "HIGH",
                "freshness_utc": f"{day_utc}T14:30:00Z",
            },
        },
    )


def _intent(ctx: bod.BodContext, *, target: str = "0.01") -> Path:
    return _write_json(
        ctx.execution_root / "intents_v1" / "snapshots" / ctx.day_utc / "i1.exposure_intent.v1.json",
        {
            "intent_id": "intent_1",
            "target_notional_pct": target,
            "underlying": {"symbol": "SPY"},
        },
    )


def _pass_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        supply,
        "_run_capital_risk_envelope",
        lambda ctx: (
            {
                "status": "PASS",
                "reason_codes": [],
                "path": str(ctx.execution_root / "reports" / "capital_risk_envelope_v2" / ctx.day_utc / "capital_risk_envelope.v2.json"),
            },
            "",
        ),
    )


def test_valid_capital_supply_broker_nav_plus_policy_passes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    _intent(ctx)
    _pass_envelope(monkeypatch)

    payload = supply.build_risk_budget_supply(ctx)

    assert payload["status"] == "PASS"
    assert payload["nav_basis"]["source"] == "BROKER_SUPPLY"
    assert payload["risk_sizing_export"]["usable_for_risk_sizing"] is True


def test_missing_capital_supply_blocks(tmp_path: Path) -> None:
    payload = supply.build_risk_budget_supply(_ctx(tmp_path))
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "CAPITAL_SUPPLY_MISSING"


def test_blocked_capital_supply_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx, status="BLOCKED", blocker="NAV_TOTAL_MISSING_OR_INVALID")
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["canonical_blocker"] == "CAPITAL_SUPPLY_BLOCKED"
    assert payload["capital_supply_result"]["canonical_blocker"] == "NAV_TOTAL_MISSING_OR_INVALID"


def test_missing_nav_basis_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_json(ctx.truth_root / "reports" / "capital_supply_v1" / ctx.day_utc / "capital_supply.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "selected_source": {}})
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["canonical_blocker"] == "NAV_BASIS_MISSING"


def test_invalid_null_nav_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx, nav=None)
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["canonical_blocker"] == "NAV_BASIS_INVALID"


def test_missing_budget_policy_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    monkeypatch.setattr(supply, "POLICY_SOURCE", tmp_path / "missing_policy.md")
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["canonical_blocker"] == "RISK_BUDGET_POLICY_MISSING"


def test_active_current_day_intent_receives_computed_budget(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx, nav=200_000_00)
    _intent(ctx, target="0.01")
    _pass_envelope(monkeypatch)
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["intent_budgets"][0]["allowed_risk_cents"] == 200_000
    assert payload["intent_budgets"][0]["instrument"] == "SPY"


def test_intent_budget_compute_failure_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    _intent(ctx, target="0.05")
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["canonical_blocker"] == "INTENT_BUDGET_COMPUTE_FAILED"


def test_capital_risk_envelope_failure_preserves_reason_codes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    _intent(ctx)
    monkeypatch.setattr(supply, "_run_capital_risk_envelope", lambda ctx: ({"status": "FAIL", "reason_codes": ["B2_NAV_TOTAL_MISSING_OR_INVALID"]}, "CAPITAL_RISK_ENVELOPE_BLOCKED"))
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["canonical_blocker"] == "CAPITAL_RISK_ENVELOPE_BLOCKED"
    assert payload["capital_risk_envelope"]["reason_codes"] == ["B2_NAV_TOTAL_MISSING_OR_INVALID"]


def test_risk_sizing_export_only_when_budget_and_envelope_pass(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    _intent(ctx)
    monkeypatch.setattr(supply, "_run_capital_risk_envelope", lambda ctx: ({"status": "FAIL", "reason_codes": ["X"]}, "CAPITAL_RISK_ENVELOPE_BLOCKED"))
    blocked = supply.build_risk_budget_supply(ctx)
    assert blocked["risk_sizing_export"]["usable_for_risk_sizing"] is False
    _pass_envelope(monkeypatch)
    passed = supply.build_risk_budget_supply(ctx)
    assert passed["risk_sizing_export"]["usable_for_risk_sizing"] is True


def test_day_ledger_does_not_run_risk_sizing_before_risk_budget_supply_passes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = day_run.PhaseContext("2026-04-29", "PAPER", tmp_path / "truth", tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER", tmp_path / "runtime", tmp_path / "operator", "DU123456")
    for path in (ctx.truth_root, ctx.execution_root, ctx.runtime_root, ctx.operator_input_root):
        path.mkdir(parents=True, exist_ok=True)
    _write_json(ctx.truth_root / "reports" / "capital_supply_v1" / ctx.day_utc / "capital_supply.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", "selected_source": {"source_type": "BROKER_ACCOUNT", "trust_level": "HIGH"}})
    risk_path = ctx.truth_root / "reports" / "risk_budget_supply_v1" / ctx.day_utc / "risk_budget_supply.v1.json"
    _write_json(risk_path, {"day_utc": ctx.day_utc, "status": "BLOCKED", "canonical_blocker": "CAPITAL_RISK_ENVELOPE_BLOCKED", "nav_basis": {"source": "BROKER_SUPPLY", "net_liquidation_cents": 100}})
    calls: list[str] = []

    def fake_steps(_phase, commands, env):  # noqa: ANN001
        del env
        calls.extend(str(cmd[1]) for _, cmd, _ in commands)
        if any("run_risk_budget_supply_v1.py" in str(cmd[1]) for _, cmd, _ in commands):
            return ([{"status": "BLOCKED", "blocker": "CAPITAL_RISK_ENVELOPE_BLOCKED", "duration_ms": 1}], [str(risk_path)], ["CAPITAL_RISK_ENVELOPE_BLOCKED"])
        return ([{"status": "PASS", "duration_ms": 1} for _ in commands], [], [])

    monkeypatch.setattr(day_run, "_run_steps", fake_steps)
    row = day_run._phase_strategy_and_risk(ctx, {})

    assert row["canonical_blocker"] == "CAPITAL_RISK_ENVELOPE_BLOCKED"
    assert any("run_risk_budget_supply_v1.py" in item for item in calls)
    assert not any("run_risk_sizing_authority_v1.py" in item for item in calls)


def test_packet_displays_risk_budget_supply_details(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    path = truth / "reports" / "risk_budget_supply_v1" / "2026-04-29" / "risk_budget_supply.v1.json"
    _write_json(
        path,
        {
            "day_utc": "2026-04-29",
            "status": "BLOCKED",
            "canonical_blocker": "CAPITAL_RISK_ENVELOPE_BLOCKED",
            "nav_basis": {"source": "BROKER_SUPPLY", "net_liquidation_cents": 100},
            "budget_policy": {"policy_id": "P"},
            "intent_budgets": [{"intent_id": "i"}],
            "capital_risk_envelope": {"status": "FAIL", "reason_codes": ["X"]},
            "risk_sizing_export": {"usable_for_risk_sizing": False},
            "operator_next_action": "fix envelope",
        },
    )
    roots = packet.RootResolution(truth, tmp_path / "exec", tmp_path / "truth_sleeves", "x", "NONE", "")
    status = packet._load_risk_budget_supply_status(roots, "2026-04-29")
    assert status.canonical_blocker == "CAPITAL_RISK_ENVELOPE_BLOCKED"
    assert status.nav_basis["source"] == "BROKER_SUPPLY"


def test_wrong_day_capital_supply_cannot_satisfy_current_day(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx, day="2026-04-28")
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["canonical_blocker"] == "CAPITAL_SUPPLY_MISSING"


def test_risk_budget_supply_writes_only_runtime_artifact(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    _intent(ctx)
    _pass_envelope(monkeypatch)
    monkeypatch.setattr(supply.bod, "_resolve_context", lambda day, env, truth: ctx)
    path, payload = supply.run_risk_budget_supply_v1(ctx.day_utc, ctx.environment)
    assert payload["status"] == "PASS"
    assert path == ctx.truth_root / "reports" / "risk_budget_supply_v1" / ctx.day_utc / "risk_budget_supply.v1.json"
    assert str(path).startswith(str(ctx.truth_root))
