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
import ops.tools.run_capital_supply_v1 as supply  # noqa: E402
from ops.tools import run_aegis_bod_prepare_v1 as bod  # noqa: E402


def _ctx(tmp_path: Path, day: str = "2026-04-29") -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(day, "PAPER", truth, execution, runtime, operator, "DU123456")


def _broker(ctx: bod.BodContext, *, day: str | None = None, nav: int | None = 100_000_00, cash: int | None = 100_000_00) -> Path:
    day_utc = day or ctx.day_utc
    path = ctx.execution_root / "broker_account_snapshot_v1" / day_utc / "broker_account_snapshot.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "account_id": ctx.ib_account,
                "currency": "USD",
                "cash_total_cents": cash,
                "net_liquidation_cents": nav,
                "observed_at_utc": f"{day_utc}T14:30:00Z",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _operator(ctx: bod.BodContext, *, nlv: str = "100000.00", cash: str = "100000.00") -> Path:
    path = supply.resolve_operator_statement_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "account_id": ctx.ib_account,
                "currency": "USD",
                "cash_total": cash,
                "nlv_total": nlv,
                "observed_at_utc": f"{ctx.day_utc}T00:00:00Z",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _seed(ctx: bod.BodContext, *, nlv: str = "100000.00", cash: str = "100000.00") -> Path:
    path = supply.resolve_paper_capital_seed_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "day_utc": ctx.day_utc,
                "environment": "PAPER",
                "ib_account": ctx.ib_account,
                "currency": "USD",
                "cash_total": cash,
                "nlv_total": nlv,
                "produced_utc": f"{ctx.day_utc}T00:00:00Z",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _nav(ctx: bod.BodContext, *, day: str | None = None, nav_total: int = 100_000) -> Path:
    day_utc = day or ctx.day_utc
    path = ctx.execution_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"day_utc": day_utc, "nav": {"nav_total": nav_total, "cash_total": nav_total, "currency": "USD"}}), encoding="utf-8")
    return path


def _budget(ctx: bod.BodContext, *, nav_total_cents: int | None = 100_000_00) -> Path:
    path = ctx.execution_root / "allocation_v1" / "summary" / ctx.day_utc / "summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"day_utc": ctx.day_utc, "allocation": {"nav_total_cents": nav_total_cents}}
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def _pass_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        supply,
        "_run_capital_risk_envelope",
        lambda ctx: ({"status": "PASS", "reason_codes": [], "path": str(ctx.execution_root / "reports" / "capital_risk_envelope_v2" / ctx.day_utc / "capital_risk_envelope.v2.json")}, ""),
    )


def test_valid_broker_nav_cash_passes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _broker(ctx)
    _nav(ctx)
    _budget(ctx)
    _pass_envelope(monkeypatch)
    payload = supply.build_capital_supply(ctx)
    assert payload["status"] == "PASS"
    assert payload["selected_source"]["source_type"] == "BROKER_ACCOUNT"


def test_operator_statement_valid_is_degraded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _operator(ctx)
    _nav(ctx)
    _budget(ctx)
    _pass_envelope(monkeypatch)
    payload = supply.build_capital_supply(ctx)
    assert payload["status"] == "DEGRADED"
    assert payload["selected_source"]["source_type"] == "OPERATOR_STATEMENT"


def test_bootstrap_seed_only_is_degraded_with_blocker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _seed(ctx)
    _nav(ctx)
    _budget(ctx)
    _pass_envelope(monkeypatch)
    payload = supply.build_capital_supply(ctx)
    assert payload["status"] == "DEGRADED"
    assert payload["canonical_blocker"] == "BOOTSTRAP_CAPITAL_ONLY"


def test_null_nav_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _broker(ctx, nav=None)
    payload = supply.build_capital_supply(ctx)
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "NAV_TOTAL_MISSING_OR_INVALID"


def test_wrong_day_nav_cannot_satisfy_current_day(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _broker(ctx)
    _nav(ctx, day="2026-04-28")
    _budget(ctx)
    payload = supply.build_capital_supply(ctx)
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "BROKER_NAV_EVIDENCE_MISSING"


def test_capital_supply_no_longer_owns_missing_exposure_budget(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _broker(ctx)
    _nav(ctx)
    _pass_envelope(monkeypatch)
    payload = supply.build_capital_supply(ctx)
    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert payload["exposure_budget"] == {}


def test_capital_supply_no_longer_reports_exposure_budget_nav_missing_when_broker_nav_valid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _broker(ctx)
    _nav(ctx)
    _budget(ctx, nav_total_cents=None)
    _pass_envelope(monkeypatch)
    payload = supply.build_capital_supply(ctx)
    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] != "EXPOSURE_BUDGET_NAV_MISSING"
    assert payload["selected_source"]["net_liquidation_cents"] == 100_000_00


def test_capital_supply_no_longer_owns_capital_risk_envelope_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _broker(ctx)
    _nav(ctx)
    _budget(ctx)
    monkeypatch.setattr(supply, "_run_capital_risk_envelope", lambda ctx: ({"status": "FAIL", "reason_codes": ["B2_NAV_TOTAL_MISSING_OR_INVALID"]}, "CAPITAL_RISK_ENVELOPE_BLOCKED"))
    payload = supply.build_capital_supply(ctx)
    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert payload["capital_risk_envelope"] == {}


def test_day_ledger_skips_risk_sizing_when_capital_supply_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = day_run.PhaseContext("2026-04-29", "PAPER", tmp_path / "truth", tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER", tmp_path / "runtime", tmp_path / "operator", "DU123456")
    for path in (ctx.truth_root, ctx.execution_root, ctx.runtime_root, ctx.operator_input_root):
        path.mkdir(parents=True, exist_ok=True)
    capital_path = ctx.truth_root / "reports" / "capital_supply_v1" / ctx.day_utc / "capital_supply.v1.json"
    capital_path.parent.mkdir(parents=True, exist_ok=True)
    capital_path.write_text(json.dumps({"day_utc": ctx.day_utc, "status": "BLOCKED", "canonical_blocker": "NAV_TOTAL_MISSING_OR_INVALID", "selected_source": {"source_type": "BROKER_ACCOUNT", "trust_level": "HIGH"}}), encoding="utf-8")

    calls: list[str] = []

    def fake_steps(_phase, commands, env):  # noqa: ANN001
        del env
        calls.extend(str(cmd[1]) for _, cmd, _ in commands)
        if any("run_capital_supply_v1.py" in str(cmd[1]) for _, cmd, _ in commands):
            return ([{"status": "BLOCKED", "blocker": "NAV_TOTAL_MISSING_OR_INVALID", "duration_ms": 1}], [str(capital_path)], ["NAV_TOTAL_MISSING_OR_INVALID"])
        return ([{"status": "PASS", "duration_ms": 1} for _ in commands], [], [])

    monkeypatch.setattr(day_run, "_run_steps", fake_steps)
    row = day_run._phase_strategy_and_risk(ctx, {})
    assert row["canonical_blocker"] == "NAV_TOTAL_MISSING_OR_INVALID"
    assert not any("run_risk_sizing_authority_v1.py" in item for item in calls)


def test_packet_displays_capital_supply_details(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    execution = tmp_path / "exec"
    cap = truth / "reports" / "capital_supply_v1" / "2026-04-29" / "capital_supply.v1.json"
    cap.parent.mkdir(parents=True)
    cap.write_text(
        json.dumps(
            {
                "day_utc": "2026-04-29",
                "status": "BLOCKED",
                "canonical_blocker": "NAV_TOTAL_MISSING_OR_INVALID",
                "selected_source": {"source_type": "BROKER_ACCOUNT", "trust_level": "HIGH", "cash_total_cents": 1, "net_liquidation_cents": None},
                "exposure_budget": {"status": "VALID"},
                "capital_risk_envelope": {"status": "MISSING", "reason_codes": ["X"]},
                "operator_next_action": "fix nav",
            }
        ),
        encoding="utf-8",
    )
    roots = packet.RootResolution(truth, execution, tmp_path / "truth_sleeves", "x", "NONE", "")
    status = packet._load_capital_supply_status(roots, "2026-04-29")
    assert status.canonical_blocker == "NAV_TOTAL_MISSING_OR_INVALID"
    assert status.selected_source["source_type"] == "BROKER_ACCOUNT"


def test_capital_supply_writes_only_runtime_artifact(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _broker(ctx)
    _nav(ctx)
    _budget(ctx)
    _pass_envelope(monkeypatch)
    monkeypatch.setattr(supply.bod, "_resolve_context", lambda day, env, truth: ctx)
    path, payload = supply.run_capital_supply_v1(ctx.day_utc, ctx.environment)
    assert path == ctx.truth_root / "reports" / "capital_supply_v1" / ctx.day_utc / "capital_supply.v1.json"
    assert payload["status"] == "PASS"
    assert str(path).startswith(str(ctx.truth_root))
