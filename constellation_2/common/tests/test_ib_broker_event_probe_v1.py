from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_aegis_bod_prepare_v1 as bod
import ops.tools.run_aegis_pre_open_verify_v1 as preopen
import ops.tools.run_ib_broker_event_probe_v1 as probe


DAY = "2026-04-30"


def _config() -> probe.ProbeConfig:
    return probe.ProbeConfig(
        day_utc=DAY,
        environment="PAPER",
        expected_account="DUO847203",
        host="127.0.0.1",
        port=4002,
        client_id=179,
        timeout_seconds=12.0,
        request_open_orders=False,
    )


def _ctx(tmp_path: Path) -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    for path in (truth, execution, runtime):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(
        day_utc=DAY,
        environment="PAPER",
        truth_root=truth,
        execution_root=execution,
        runtime_root=runtime,
        operator_input_root=runtime,
        ib_account="DUO847203",
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _valid_events(account: str = "DUO847203") -> list[dict]:
    return [
        {"event_type": "nextValidId", "order_id": 44},
        {"event_type": "currentTime", "ib_time": 1},
        {"event_type": "managedAccounts", "accounts": [account]},
        {"event_type": "accountSummary", "account": account, "tag": "NetLiquidation", "value": "100000", "currency": "USD"},
        {"event_type": "accountSummaryEnd", "req_id": 9101},
        {"event_type": "positionEnd"},
    ]


def test_socket_connected_without_account_events_blocks_specific_reason() -> None:
    payload = probe.evaluate_probe_v1(_config(), {"connected": True, "server_version": 178, "events": [{"event_type": "nextValidId"}]})

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] in {"IB_ACCOUNT_UPDATES_MISSING", "IB_EVENT_TIMEOUT", "IB_MANAGED_ACCOUNTS_MISSING"}


def test_managed_account_mismatch_blocks_specific_reason() -> None:
    payload = probe.evaluate_probe_v1(_config(), {"connected": True, "server_version": 178, "events": _valid_events("DU999999")})

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "IB_ACCOUNT_MISMATCH"


def test_valid_probe_evidence_clears_broker_events_missing_in_bod(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write(probe.probe_artifact_path_v1(truth_root=ctx.truth_root, day_utc=DAY), {"status": "PASS"})
    _write(ctx.truth_root / "reports/paper_trading_day_authority_v1" / DAY / "paper_trading_day_authority.v1.json", {"canonical_blocker": "C2_KILL_SWITCH_ACTIVE"})
    steps = [
        bod._internal_step("pre_open_bundle", status="BLOCKED", blocker="BROKER_EVENTS_MISSING"),
        bod._internal_step("paper_session_bootstrap", status="BLOCKED", blocker="IB_API_HANDSHAKE_NOT_OK"),
        bod._internal_step("paper_trading_day_authority", status="BLOCKED", blocker="C2_KILL_SWITCH_ACTIVE"),
    ]

    assert bod._canonical_blocker(steps, ctx) == "C2_KILL_SWITCH_ACTIVE"


def test_market_data_failure_does_not_make_probe_broker_missing() -> None:
    events = [*_valid_events(), {"event_type": "error", "error_code": 354, "error_string": "market data permission denied"}]
    payload = probe.evaluate_probe_v1(_config(), {"connected": True, "server_version": 178, "events": events})

    assert payload["canonical_blocker"] != "BROKER_EVENTS_MISSING"
    assert payload["status"] == "PASS"


def test_probe_artifact_path_is_runtime_only(tmp_path: Path) -> None:
    path = probe.probe_artifact_path_v1(truth_root=tmp_path / "truth", day_utc=DAY)

    assert not path.resolve().is_relative_to(SOURCE_ROOT)
    assert str(path).endswith("/truth/reports/ib_broker_event_probe_v1/2026-04-30/ib_broker_event_probe.v1.json")


def test_pre_open_verify_consumes_probe_specific_blocker(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write(ctx.truth_root / "reports/paper_session_authority_v1" / DAY / "paper_session_authority.v1.json", {"authority_status": "DENIED", "canonical_blocker": "BROKER_EVENTS_MISSING"})
    _write(ctx.truth_root / "reports/pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json", {"materialization_state": "BLOCKED", "canonical_blocker": "BROKER_EVENTS_MISSING"})
    _write(probe.probe_artifact_path_v1(truth_root=ctx.truth_root, day_utc=DAY), {"status": "BLOCKED", "canonical_blocker": "IB_ACCOUNT_UPDATES_MISSING"})
    _write(ctx.truth_root / "reports/execution_mode_authority_v1" / DAY / "execution_mode_authority.v1.json", {"mode_state": "DRY_RUN_LOCKED"})
    _write(ctx.truth_root / "reports/runtime_service_authority_v1" / DAY / "runtime_service_authority.v1.json", {"service_state": "MANUAL_MODE_READY"})
    _write(ctx.operator_input_root / "operator_inputs/paper_capital_seed_v1" / DAY / "paper_capital_seed.v1.json", {"ok": True})
    _write(ctx.operator_input_root / "operator_inputs/cash_ledger_operator_statements" / DAY / "operator_statement.v1.json", {"ok": True})
    monkeypatch.setattr(preopen.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    assert preopen.main(["--day_utc", DAY]) == 2
    payload = json.loads((ctx.truth_root / "reports/aegis_pre_open_verify_v1" / DAY / "aegis_pre_open_verify.v1.json").read_text(encoding="utf-8"))

    blockers = [row["blocker"] for row in payload["checks"] if row["name"] in {"ib_broker_event_probe", "ib_connection"}]
    assert blockers == ["IB_ACCOUNT_UPDATES_MISSING", "IB_ACCOUNT_UPDATES_MISSING"]


def test_client_id_in_use_is_specific_blocker() -> None:
    payload = probe.evaluate_probe_v1(
        _config(),
        {
            "connected": True,
            "server_version": 178,
            "events": [{"event_type": "error", "error_code": 326, "error_string": "client id is already in use"}],
        },
    )

    assert payload["canonical_blocker"] == "IB_CLIENT_ID_IN_USE"
