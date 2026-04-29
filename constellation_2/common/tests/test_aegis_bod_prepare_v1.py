from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_aegis_bod_prepare_v1 as bod


DAY = "2026-04-29"
PRIOR_DAY = "2026-04-28"


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


def _write(path: Path, payload: dict | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload or {"ok": True}, sort_keys=True) + "\n", encoding="utf-8")


def test_prior_ready_artifacts_are_not_deleted_or_overwritten_by_current_bod(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    prior_path = ctx.truth_root / "reports" / "paper_session_authority_v1" / PRIOR_DAY / "paper_session_authority.v1.json"
    _write(prior_path, {"authority_status": "GRANTED"})
    before = prior_path.read_text(encoding="utf-8")

    monkeypatch.setattr(bod, "_source_integrity_step", lambda: bod._internal_step("source_integrity_gate", status="PASS"))
    monkeypatch.setattr(bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    monkeypatch.setattr(bod, "resolve_market_calendar_record_v1", lambda **_kwargs: {"record": {"is_trading_session": True}, "path": "calendar"})
    monkeypatch.setattr(bod, "_run_sequence", lambda _ctx: [bod._internal_step("paper_session_bootstrap", status="PASS")])

    assert bod.main(["--day_utc", DAY, "--environment", "PAPER"]) == 0

    assert prior_path.exists()
    assert prior_path.read_text(encoding="utf-8") == before


def test_wrong_day_artifacts_do_not_satisfy_current_day_comparison(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    prior_path = ctx.truth_root / "reports" / "paper_session_authority_v1" / PRIOR_DAY / "paper_session_authority.v1.json"
    _write(prior_path)

    rows = bod._build_artifact_comparison(ctx, PRIOR_DAY)
    row = next(item for item in rows if item["artifact_name"] == "paper_session_authority")

    assert row["prior_status"] == "EXISTS"
    assert row["current_status"] == "MISSING"
    assert row["carry_forward_allowed"] is False


def test_bod_manifest_reports_operator_required_inputs(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    blocked_seed = bod._internal_step(
        "paper_capital_seed",
        status="BLOCKED",
        blocker="PAPER_CAPITAL_SEED_MISSING",
        artifact_path=str(ctx.operator_input_root / "operator_inputs/paper_capital_seed_v1" / DAY / "paper_capital_seed.v1.json"),
    )

    monkeypatch.setattr(bod, "_source_integrity_step", lambda: bod._internal_step("source_integrity_gate", status="PASS"))
    monkeypatch.setattr(bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    monkeypatch.setattr(bod, "resolve_market_calendar_record_v1", lambda **_kwargs: {"record": {"is_trading_session": True}, "path": "calendar"})
    monkeypatch.setattr(bod, "_run_sequence", lambda _ctx: [blocked_seed])

    assert bod.main(["--day_utc", DAY, "--environment", "PAPER"]) == 2
    manifest = json.loads((ctx.truth_root / "reports/aegis_bod_prepare_v1" / DAY / "aegis_bod_prepare.v1.json").read_text(encoding="utf-8"))

    assert manifest["canonical_blocker"] == "PAPER_CAPITAL_SEED_MISSING"
    assert manifest["operator_actions"]
    assert "paper_capital_seed" in manifest["operator_actions"][0]["action"]


def test_bod_command_is_idempotent_for_manifest_path(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    monkeypatch.setattr(bod, "_source_integrity_step", lambda: bod._internal_step("source_integrity_gate", status="PASS"))
    monkeypatch.setattr(bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    monkeypatch.setattr(bod, "resolve_market_calendar_record_v1", lambda **_kwargs: {"record": {"is_trading_session": True}, "path": "calendar"})
    monkeypatch.setattr(bod, "_run_sequence", lambda _ctx: [bod._internal_step("paper_session_bootstrap", status="PASS")])

    assert bod.main(["--day_utc", DAY, "--environment", "PAPER"]) == 0
    assert bod.main(["--day_utc", DAY, "--environment", "PAPER"]) == 0

    manifests = list((ctx.truth_root / "reports/aegis_bod_prepare_v1" / DAY).glob("*.json"))
    assert manifests == [ctx.truth_root / "reports/aegis_bod_prepare_v1" / DAY / "aegis_bod_prepare.v1.json"]


def test_bod_default_outputs_are_outside_source_repo(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)

    assert not bod._manifest_path(ctx).resolve().is_relative_to(SOURCE_ROOT)
    for row in bod._build_artifact_comparison(ctx, PRIOR_DAY):
        if row["artifact_name"] in {"paper_capital_seed", "operator_statement"}:
            assert not Path(row["current_expected_path"]).resolve().is_relative_to(SOURCE_ROOT)


def test_child_step_timeout_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("AEGIS_BOD_STEP_TIMEOUT_SECONDS", "1")

    row = bod._run_child("timeout_probe", [sys.executable, "-c", "import time; time.sleep(5)"])

    assert row["status"] == "TIMEOUT"
    assert row["exit_code"] == 124
    assert row["blocker"] == "BOD_STEP_TIMEOUT"


def test_session_denied_makes_options_snapshot_diagnostic_skip(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    seen: list[str] = []

    def fake_run_child(name: str, cmd: list[str], **_kwargs):
        seen.append(name)
        return bod._internal_step(name, status="PASS", command=cmd)

    monkeypatch.setattr(bod, "_run_child", fake_run_child)
    monkeypatch.setattr(bod, "_session_denied", lambda _ctx: True)

    steps = bod._run_sequence(ctx)
    options = next(row for row in steps if row["step_name"] == "options_chain_snapshot")

    assert options["status"] == "SKIPPED"
    assert options["blocker"] == "SESSION_DENIED_OPTIONS_DIAGNOSTIC_ONLY"
    assert "options_chain_snapshot" not in seen


def test_session_granted_with_active_option_intent_requires_options_snapshot(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    calls: list[tuple[str, list[str]]] = []

    def fake_run_child(name: str, cmd: list[str], **_kwargs):
        calls.append((name, cmd))
        return bod._internal_step(name, status="PASS", command=cmd)

    monkeypatch.setattr(bod, "_run_child", fake_run_child)
    monkeypatch.setattr(bod, "_session_denied", lambda _ctx: False)

    bod._run_sequence(ctx)
    options = [cmd for name, cmd in calls if name == "options_chain_snapshot"]

    assert options
    assert "ops/tools/run_options_chain_snapshot_required_day_v1.py" in options[0]
    assert "--symbols_from_intents" in options[0]


def test_missing_defined_risk_phasec_evidence_reports_expected_path_and_command(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    intent_path = ctx.execution_root / "intents_v1" / "snapshots" / DAY / (("a" * 64) + ".exposure_intent.v1.json")
    _write(
        intent_path,
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": "intent_spy",
            "exposure_type": "SHORT_VOL_DEFINED",
            "option": {"structure": "PUT"},
            "underlying": {"symbol": "SPY"},
        },
    )

    rows = bod._defined_risk_expected(ctx)

    assert rows
    assert "phaseC_preflight_v1" in rows[0]["expected_path"]
    assert "run_phasec_identity_materializer_day_v1.py" in rows[0]["creation_command"]


def test_bod_sequence_includes_non_external_readiness_producers(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    calls: list[str] = []

    def fake_run_child(name: str, cmd: list[str], **_kwargs):
        calls.append(name)
        return bod._internal_step(name, status="PASS", command=cmd)

    monkeypatch.setattr(bod, "_run_child", fake_run_child)
    monkeypatch.setattr(bod, "_session_denied", lambda _ctx: True)

    bod._run_sequence(ctx)

    for expected in {
        "paper_capital_seed",
        "operator_statement",
        "pre_open_bundle",
        "paper_session_bootstrap",
        "paper_trading_day_authority",
        "market_data_authority",
        "strategy_decision_authority",
        "portfolio_account_authority",
        "risk_sizing_authority",
        "execution_mode_authority",
        "runtime_service_authority",
        "trade_submit_readiness",
        "submit_boundary_status",
        "trading_day_control_plane",
        "aegis_operating_contract",
        "aegis_authority_graph",
        "aegis_daily_operator_summary",
        "aegis_chatgpt_packet",
    }:
        assert expected in calls
