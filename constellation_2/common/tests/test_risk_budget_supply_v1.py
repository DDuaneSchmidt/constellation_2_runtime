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
    freshness_utc: str | None = None,
    carry_forward_source_used: str = "",
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
                "freshness_utc": freshness_utc or f"{day_utc}T14:30:00Z",
            },
            "carry_forward_source_used": carry_forward_source_used,
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


def _selected_pointer(ctx: bod.BodContext, *, status: str = "SELECTED", blocker: str = "", intent_id: str = "intent_1", intent_path: Path | None = None) -> Path:
    selected = {}
    if status == "SELECTED":
        selected = {"intent_id": intent_id}
        if intent_path is not None:
            selected["intent_path"] = str(intent_path)
    return _write_json(
        ctx.truth_root / "pointers" / "selected_intent_pointer.v1.json",
        {
            "day_utc": ctx.day_utc,
            "status": status,
            "canonical_blocker": blocker,
            "selected_intent": selected,
        },
    )


def _positions_v2(ctx: bod.BodContext) -> Path:
    return _write_json(
        ctx.execution_root / "positions_v1" / "snapshots" / ctx.day_utc / "positions_snapshot.v2.json",
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
            "schema_version": 2,
            "produced_utc": f"{ctx.day_utc}T00:00:00Z",
            "day_utc": ctx.day_utc,
            "producer": {"repo": "constellation", "git_sha": "a" * 40, "module": "test"},
            "status": "OK",
            "reason_codes": ["TEST"],
            "input_manifest": [{"type": "other", "path": str(ctx.execution_root), "sha256": "0" * 64, "day_utc": ctx.day_utc, "producer": "test"}],
            "positions": {"currency": "USD", "asof_utc": f"{ctx.day_utc}T00:00:00Z", "items": [], "notes": []},
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


def test_valid_risk_budget_nav_materializes_capital_risk_adapter_input(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx, nav=101_283_531, cash=100_766_465)
    _intent(ctx)
    _pass_envelope(monkeypatch)

    payload = supply.build_risk_budget_supply(ctx)
    adapter_path = Path(payload["capital_risk_envelope_adapter"]["path"])
    adapter = json.loads(adapter_path.read_text(encoding="utf-8"))
    nav = json.loads((ctx.execution_root / "accounting_v2" / "nav" / ctx.day_utc / "nav.v2.json").read_text(encoding="utf-8"))

    assert adapter["nav_total_cents"] == 101_283_531
    assert adapter["cash_total_cents"] == 100_766_465
    assert nav["nav"]["nav_total_cents"] == 101_283_531


def test_valid_intent_budget_materializes_exposure_budget_adapter_fields(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx, nav=101_283_531)
    _intent(ctx, target="0.01")
    _pass_envelope(monkeypatch)

    payload = supply.build_risk_budget_supply(ctx)
    adapter = json.loads(Path(payload["capital_risk_envelope_adapter"]["path"]).read_text(encoding="utf-8"))

    assert adapter["intent_budgets"][0]["allowed_risk_cents"] == 1_012_835
    assert adapter["intent_budgets"][0]["nav_total_cents"] == 101_283_531
    assert adapter["intent_budgets"][0]["target_pct"] == "0.01"


def test_missing_capital_supply_blocks(tmp_path: Path) -> None:
    payload = supply.build_risk_budget_supply(_ctx(tmp_path))
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "CAPITAL_SUPPLY_MISSING"


def test_missing_capital_supply_blocks_before_capital_risk_envelope_runs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    called = False

    def _unexpected(ctx):  # noqa: ANN001
        nonlocal called
        called = True
        return {"status": "FAIL"}, "CAPITAL_RISK_ENVELOPE_BLOCKED"

    monkeypatch.setattr(supply, "_run_capital_risk_envelope", _unexpected)
    payload = supply.build_risk_budget_supply(_ctx(tmp_path))

    assert payload["canonical_blocker"] == "CAPITAL_SUPPLY_MISSING"
    assert called is False


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
    intent_path = _intent(ctx, target="0.01")
    _selected_pointer(ctx, intent_path=intent_path)
    _pass_envelope(monkeypatch)
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["intent_budgets"][0]["allowed_risk_cents"] == 200_000
    assert payload["intent_budgets"][0]["instrument"] == "SPY"


def test_no_executable_intent_pointer_ignores_stale_day_snapshots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    _intent(ctx, target="0.05")
    _selected_pointer(ctx, status="NO_EXECUTABLE_INTENT", blocker="NO_EXECUTABLE_INTENT")
    _pass_envelope(monkeypatch)

    payload = supply.build_risk_budget_supply(ctx)

    assert payload["status"] == "PASS"
    assert payload["intent_budgets"] == []
    assert payload["risk_sizing_export"]["usable_for_risk_sizing"] is True


def test_blocked_empty_selected_intent_pointer_does_not_budget_all_snapshots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    _intent(ctx, target="0.10")
    _selected_pointer(ctx, status="BLOCKED", blocker="MISSING_REQUIRED_INPUTS", intent_id="")
    _pass_envelope(monkeypatch)

    payload = supply.build_risk_budget_supply(ctx)

    assert payload["status"] == "PASS"
    assert payload["intent_budgets"] == []
    assert payload["risk_sizing_export"]["usable_for_risk_sizing"] is True


def test_selected_intent_pointer_limits_budgeting_to_selected_snapshot(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    selected_path = _intent(ctx, target="0.01")
    _write_json(
        ctx.execution_root / "intents_v1" / "snapshots" / ctx.day_utc / "stale.exposure_intent.v1.json",
        {
            "intent_id": "stale_intent",
            "target_notional_pct": "0.05",
            "underlying": {"symbol": "DBC"},
        },
    )
    _selected_pointer(ctx, intent_path=selected_path)
    _pass_envelope(monkeypatch)

    payload = supply.build_risk_budget_supply(ctx)

    assert payload["status"] == "PASS"
    assert [row["intent_id"] for row in payload["intent_budgets"]] == ["intent_1"]


def test_execution_root_selected_pointer_limits_canonical_risk_budgeting(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    selected_path = _intent(ctx, target="0.01")
    _write_json(
        ctx.execution_root / "intents_v1" / "snapshots" / ctx.day_utc / "nonselected.exposure_intent.v1.json",
        {
            "intent_id": "nonselected_intent",
            "target_notional_pct": "0.10",
            "underlying": {"symbol": "DBC"},
        },
    )
    _write_json(
        ctx.truth_root / "pointers" / "selected_intent_pointer.v1.json",
        {"day_utc": "2026-04-28", "status": "BLOCKED", "canonical_blocker": "MISSING_REQUIRED_INPUTS", "selected_intent": {}},
    )
    _write_json(
        ctx.execution_root / "pointers" / "selected_intent_pointer.v1.json",
        {
            "day_utc": ctx.day_utc,
            "status": "SELECTED",
            "canonical_blocker": "",
            "selected_intent": {"intent_id": "intent_1", "intent_path": str(selected_path)},
        },
    )
    _pass_envelope(monkeypatch)

    payload = supply.build_risk_budget_supply(ctx)

    assert payload["status"] == "PASS"
    assert [row["intent_id"] for row in payload["intent_budgets"]] == ["intent_1"]
    assert payload["risk_sizing_export"]["usable_for_risk_sizing"] is True
    diagnostics = payload["non_selected_intent_diagnostics"]
    assert [row["intent_id"] for row in diagnostics] == ["nonselected_intent"]
    assert diagnostics[0]["status"] == "DIAGNOSTIC_ONLY"
    assert diagnostics[0]["blocker"] == "INTENT_BUDGET_COMPUTE_FAILED"


def test_execution_root_selected_intent_budget_failure_still_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    selected_path = _intent(ctx, target="0.10")
    _write_json(
        ctx.execution_root / "pointers" / "selected_intent_pointer.v1.json",
        {
            "day_utc": ctx.day_utc,
            "status": "SELECTED",
            "canonical_blocker": "",
            "selected_intent": {"intent_id": "intent_1", "intent_path": str(selected_path)},
        },
    )

    payload = supply.build_risk_budget_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "INTENT_BUDGET_COMPUTE_FAILED"
    assert payload["intent_budgets"][0]["intent_id"] == "intent_1"


def test_intent_budget_compute_failure_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    intent_path = _intent(ctx, target="0.05")
    _selected_pointer(ctx, intent_path=intent_path)
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["canonical_blocker"] == "INTENT_BUDGET_COMPUTE_FAILED"


def test_capital_risk_envelope_failure_preserves_reason_codes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx)
    _intent(ctx)
    monkeypatch.setattr(supply, "_run_capital_risk_envelope", lambda ctx: ({"status": "FAIL", "reason_codes": ["B2_PORTFOLIO_CAPITAL_AT_RISK_EXCEEDS_ENVELOPE"]}, "CAPITAL_RISK_ENVELOPE_BLOCKED"))
    payload = supply.build_risk_budget_supply(ctx)
    assert payload["canonical_blocker"] == "CAPITAL_RISK_ENVELOPE_BLOCKED"
    assert payload["capital_risk_envelope"]["reason_codes"] == ["B2_PORTFOLIO_CAPITAL_AT_RISK_EXCEEDS_ENVELOPE"]
    assert "B2_NAV_TOTAL_MISSING_OR_INVALID" not in payload["capital_risk_envelope"]["reason_codes"]


def test_capital_risk_envelope_no_longer_reports_missing_nav_with_valid_risk_budget(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _capital_supply(ctx, nav=101_283_531, cash=100_766_465)
    _intent(ctx)
    _positions_v2(ctx)

    payload = supply.build_risk_budget_supply(ctx)

    assert payload["capital_risk_envelope"]["status"] == "PASS"
    assert "B2_NAV_TOTAL_MISSING_OR_INVALID" not in payload["capital_risk_envelope"]["reason_codes"]
    assert "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS" not in payload["capital_risk_envelope"]["reason_codes"]
    assert payload["risk_sizing_export"]["usable_for_risk_sizing"] is True


def test_preopen_build_allows_t_minus_1_capital_supply_freshness(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("C2_TRADING_DAY_READINESS_NOW_UTC", "2026-04-30T22:00:00Z")
    ctx = _ctx(tmp_path, day="2026-05-01")
    _capital_supply(
        ctx,
        nav=101_300_246,
        cash=100_766_465,
        freshness_utc="2026-04-30T22:50:29Z",
        carry_forward_source_used="T_MINUS_1",
    )
    _intent(ctx)
    _pass_envelope(monkeypatch)

    payload = supply.build_risk_budget_supply(ctx)

    assert payload["status"] == "PASS"
    assert payload["readiness_mode"] == "PREOPEN_BUILD"
    assert payload["nav_basis"]["freshness_status"] == "CARRY_FORWARD_T_MINUS_1"
    assert payload["nav_basis"]["carry_forward_allowed"] is True


def test_intraday_submit_does_not_allow_t_minus_1_capital_supply_freshness(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("C2_TRADING_DAY_READINESS_NOW_UTC", "2026-05-01T15:00:00Z")
    ctx = _ctx(tmp_path, day="2026-05-01")
    _capital_supply(
        ctx,
        nav=101_300_246,
        cash=100_766_465,
        freshness_utc="2026-04-30T22:50:29Z",
        carry_forward_source_used="T_MINUS_1",
    )

    payload = supply.build_risk_budget_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "NAV_BASIS_INVALID"
    assert payload["readiness_mode"] == "INTRADAY_SUBMIT_READY"
    assert payload["nav_basis"]["carry_forward_allowed"] is False


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


def test_wrong_day_existing_adapter_cannot_feed_current_day_envelope(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    adapter_path = supply.capital_risk_envelope_adapter_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    _write_json(adapter_path, {"day_utc": "2026-04-28", "nav_total_cents": 1})
    _capital_supply(ctx, nav=101_283_531)
    _intent(ctx)
    _pass_envelope(monkeypatch)

    payload = supply.build_risk_budget_supply(ctx)
    adapter = json.loads(adapter_path.read_text(encoding="utf-8"))

    assert payload["capital_risk_envelope_adapter"]["status"] == "PASS"
    assert adapter["day_utc"] == ctx.day_utc
    assert adapter["nav_total_cents"] == 101_283_531


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
