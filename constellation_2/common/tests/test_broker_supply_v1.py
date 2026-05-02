from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.aegis_chatgpt_packet as packet  # noqa: E402
import ops.tools.run_aegis_day_v1 as day_run  # noqa: E402
import ops.tools.run_broker_supply_v1 as broker_supply  # noqa: E402
import ops.tools.run_capital_supply_v1 as capital_supply  # noqa: E402
from ops.tools import run_aegis_bod_prepare_v1 as bod  # noqa: E402
from ops.tools import run_ib_broker_event_probe_v1 as probe  # noqa: E402


DAY = "2026-04-29"


def _ctx(tmp_path: Path) -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(DAY, "PAPER", truth, execution, runtime, operator, "DUO847203")


def _ctx_for_day(tmp_path: Path, day: str) -> bod.BodContext:
    base = _ctx(tmp_path)
    return bod.BodContext(day, base.environment, base.truth_root, base.execution_root, base.runtime_root, base.operator_input_root, base.ib_account)


def _config(ctx: bod.BodContext) -> probe.ProbeConfig:
    return probe.ProbeConfig(
        day_utc=ctx.day_utc,
        environment=ctx.environment,
        expected_account=ctx.ib_account,
        host="127.0.0.1",
        port=4002,
        observer_client_id=179,
        probe_client_id=180,
        broker_event_log_path=ctx.execution_root / "execution_evidence_v1" / "broker_events" / ctx.day_utc / "broker_event_log.v1.jsonl",
        timeout_seconds=12.0,
        freshness_seconds=300.0,
        request_open_orders=False,
    )


def _patch_config(monkeypatch: pytest.MonkeyPatch, ctx: bod.BodContext) -> None:
    monkeypatch.setattr(broker_supply.probe, "_resolve_config", lambda *_args, **_kwargs: (_config(ctx), ctx.truth_root))


def _write_probe(ctx: bod.BodContext, status: str = "PASS", blocker: str = "") -> None:
    path = probe.probe_artifact_path_v1(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"status": status, "canonical_blocker": blocker, "connection": {"connected": status == "PASS"}}), encoding="utf-8")


def _legacy_event(event_type: str, args: list[str] | None = None) -> dict:
    return {
        "schema_id": "BROKER_EVENT_RAW",
        "schema_version": 1,
        "event_type": event_type,
        "received_utc": f"{DAY}T13:00:00Z",
        "sequence_number": 1,
        "broker": {"client_id": 179, "environment": "PAPER", "name": "INTERACTIVE_BROKERS"},
        "ib_fields": {"args": [{"value": value} for value in (args or [])]},
        "sha256": "test",
    }


def _write_log(
    ctx: bod.BodContext,
    *,
    account: str = "DUO847203",
    nav: str | None = "100000",
    cash: str | None = "95000",
    position_end: bool = True,
) -> Path:
    path = _config(ctx).broker_event_log_path
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        _legacy_event("nextValidId", ["orderId=44"]),
        _legacy_event("currentTime", ["time=1770000000"]),
        _legacy_event("managedAccounts", [f"accounts={account}"]),
    ]
    if nav is not None:
        rows.append(_legacy_event("accountSummary", [f"account={account}", "tag=NetLiquidation", f"value={nav}", "currency=USD"]))
    if cash is not None:
        rows.append(_legacy_event("accountSummary", [f"account={account}", "tag=TotalCashValue", f"value={cash}", "currency=USD"]))
    rows.append(_legacy_event("accountSummaryEnd", ["reqId=9101"]))
    if position_end:
        rows.append(_legacy_event("positionEnd", ["positionEnd()"]))
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")
    return path


def test_valid_event_log_with_account_values_passes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _patch_config(monkeypatch, ctx)
    _write_probe(ctx)
    _write_log(ctx)
    payload = broker_supply.build_broker_supply(ctx)
    assert payload["status"] == "PASS"
    assert payload["truth_root"] == str(ctx.truth_root.resolve())
    assert payload["account_values"]["net_liquidation_cents"] == 10_000_000
    assert payload["account_values"]["total_cash_value_cents"] == 9_500_000
    assert payload["capital_supply_export"]["usable_for_capital_supply"] is True


def test_missing_event_log_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _patch_config(monkeypatch, ctx)
    _write_probe(ctx)
    payload = broker_supply.build_broker_supply(ctx)
    assert payload["canonical_blocker"] == "BROKER_EVENT_LOG_MISSING"


def test_stale_event_log_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _patch_config(monkeypatch, ctx)
    _write_probe(ctx)
    path = _write_log(ctx)
    stale = time.time() - 3600
    os.utime(path, (stale, stale))
    payload = broker_supply.build_broker_supply(ctx, freshness_seconds=1.0)
    assert payload["canonical_blocker"] == "BROKER_EVENT_LOG_STALE"


def test_account_mismatch_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _patch_config(monkeypatch, ctx)
    _write_probe(ctx)
    _write_log(ctx, account="DU999999")
    payload = broker_supply.build_broker_supply(ctx)
    assert payload["canonical_blocker"] == "BROKER_ACCOUNT_MISMATCH"


def test_missing_net_liquidation_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _patch_config(monkeypatch, ctx)
    _write_probe(ctx)
    _write_log(ctx, nav=None)
    payload = broker_supply.build_broker_supply(ctx)
    assert payload["canonical_blocker"] == "BROKER_NAV_EVIDENCE_MISSING"


def test_missing_total_cash_value_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _patch_config(monkeypatch, ctx)
    _write_probe(ctx)
    _write_log(ctx, cash=None)
    payload = broker_supply.build_broker_supply(ctx)
    assert payload["canonical_blocker"] == "BROKER_CASH_EVIDENCE_MISSING"


def test_invalid_account_values_block(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _patch_config(monkeypatch, ctx)
    _write_probe(ctx)
    _write_log(ctx, nav="not-a-number")
    payload = broker_supply.build_broker_supply(ctx)
    assert payload["canonical_blocker"] == "BROKER_VALUES_INVALID"


def test_missing_position_end_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _patch_config(monkeypatch, ctx)
    _write_probe(ctx)
    _write_log(ctx, position_end=False)
    payload = broker_supply.build_broker_supply(ctx)
    assert payload["canonical_blocker"] == "BROKER_POSITION_SNAPSHOT_MISSING"


def test_capital_supply_consumes_broker_supply_export(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    broker_path = broker_supply.broker_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    broker_path.parent.mkdir(parents=True, exist_ok=True)
    broker_path.write_text(
        json.dumps(
            {
                "day_utc": ctx.day_utc,
                "status": "PASS",
                "canonical_blocker": "",
                "account": ctx.ib_account,
                "account_values": {"currency": "USD"},
                "capital_supply_export": {
                    "usable_for_capital_supply": True,
                    "cash_total_cents": 9_500_000,
                    "net_liquidation_cents": 10_000_000,
                    "trust_level": "HIGH",
                },
            }
        ),
        encoding="utf-8",
    )
    row = capital_supply._load_broker_source(ctx)
    assert row["source_path"] == str(broker_path)
    assert row["status"] == "VALID"
    assert row["cash_total_cents"] == 9_500_000


def test_day_ledger_broker_health_blocks_on_broker_supply(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = day_run.PhaseContext(DAY, "PAPER", tmp_path / "truth", tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER", tmp_path / "runtime", tmp_path / "operator", "DUO847203")
    for path in (ctx.truth_root, ctx.execution_root, ctx.runtime_root, ctx.operator_input_root):
        path.mkdir(parents=True, exist_ok=True)
    broker_path = ctx.truth_root / "reports" / "broker_supply_v1" / DAY / "broker_supply.v1.json"
    broker_path.parent.mkdir(parents=True, exist_ok=True)
    broker_path.write_text(json.dumps({"day_utc": DAY, "status": "BLOCKED", "canonical_blocker": "BROKER_NAV_EVIDENCE_MISSING"}), encoding="utf-8")
    monkeypatch.setattr(day_run, "_run_steps", lambda *_args, **_kwargs: ([{"status": "BLOCKED", "blocker": "BROKER_NAV_EVIDENCE_MISSING", "duration_ms": 1}], [str(broker_path)], ["BROKER_NAV_EVIDENCE_MISSING"]))
    row = day_run._phase_broker_health(ctx, {})
    assert row["canonical_blocker"] == "BROKER_NAV_EVIDENCE_MISSING"
    assert row["status"] == "BLOCKED"


def test_packet_displays_broker_supply_details(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    execution = tmp_path / "exec"
    path = truth / "reports" / "broker_supply_v1" / DAY / "broker_supply.v1.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "day_utc": DAY,
                "status": "PASS",
                "canonical_blocker": "",
                "account_identity": {"match": True, "observed_accounts": ["DUO847203"]},
                "account_values": {"net_liquidation_cents": 10_000_000, "total_cash_value_cents": 9_500_000},
                "event_log": {"status": "PRESENT"},
                "capital_supply_export": {"usable_for_capital_supply": True},
                "execution_readiness_export": {"usable_for_execution_readiness": True},
            }
        ),
        encoding="utf-8",
    )
    roots = packet.RootResolution(truth, execution, tmp_path / "truth_sleeves", "x", "NONE", "")
    status = packet._load_broker_supply_status(roots, DAY)
    assert status.status == "PASS"
    assert status.account_identity["match"] is True
    assert status.account_values["net_liquidation_cents"] == 10_000_000


def test_broker_supply_writes_only_runtime_artifact(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _patch_config(monkeypatch, ctx)
    _write_probe(ctx)
    _write_log(ctx)
    monkeypatch.setattr(broker_supply.bod, "_resolve_context", lambda day, env, truth: ctx)
    path, payload = broker_supply.run_broker_supply_v1(ctx.day_utc, ctx.environment)
    assert path == ctx.truth_root / "reports" / "broker_supply_v1" / ctx.day_utc / "broker_supply.v1.json"
    assert payload["status"] == "PASS"
    assert str(path).startswith(str(ctx.truth_root))


def test_future_target_day_uses_t_minus_1_broker_event_carry_forward(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = _ctx_for_day(tmp_path, "2026-05-01")
    prior = _ctx_for_day(tmp_path, "2026-04-30")
    monkeypatch.setenv("C2_TRADING_DAY_READINESS_NOW_UTC", "2026-04-30T22:00:00Z")
    _patch_config(monkeypatch, target)
    _write_probe(target)
    _write_log(prior)

    payload = broker_supply.build_broker_supply(target)

    assert payload["status"] == "PASS"
    assert payload["readiness_mode"] == "PREOPEN_BUILD"
    assert payload["broker_event_source"] == "CARRY_FORWARD"
    assert payload["broker_event_day_used"] == "2026-04-30"
    assert payload["carry_forward_allowed"] is True


def test_intraday_target_day_requires_same_day_broker_event_log(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = _ctx_for_day(tmp_path, "2026-05-01")
    prior = _ctx_for_day(tmp_path, "2026-04-30")
    monkeypatch.setenv("C2_TRADING_DAY_READINESS_NOW_UTC", "2026-05-01T15:00:00Z")
    _patch_config(monkeypatch, target)
    _write_probe(target)
    _write_log(prior)

    payload = broker_supply.build_broker_supply(target)

    assert payload["status"] == "BLOCKED"
    assert payload["readiness_mode"] == "INTRADAY_SUBMIT_READY"
    assert payload["broker_event_source"] == "MISSING"
    assert payload["canonical_blocker"] == "BROKER_EVENT_LOG_MISSING"
