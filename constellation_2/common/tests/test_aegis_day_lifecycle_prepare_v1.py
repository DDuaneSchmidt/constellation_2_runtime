from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.aegis_day_lifecycle_v1 import read_lifecycle_state_v1, write_lifecycle_transition_v1
import ops.tools.run_aegis_bod_prepare_v1 as bod
import ops.tools.run_aegis_market_and_strategy_prepare_v1 as market
import ops.tools.run_aegis_paper_ready_v1 as paper
import ops.tools.run_aegis_pre_open_verify_v1 as preopen
import ops.tools.run_aegis_prepare_for_trading_day_v1 as master


DAY = "2026-04-30"
PRIOR_DAY = "2026-04-29"


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


def _seed_pre_open_inputs(ctx: bod.BodContext, *, session: str = "GRANTED", pre_open: str = "COMPLETE", operator: bool = True) -> None:
    _write(
        ctx.truth_root / "reports/paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json",
        {"authority_status": session, "submission_authorized": session == "GRANTED", "canonical_blocker": "SESSION_AUTHORITY_DENIED" if session != "GRANTED" else ""},
    )
    _write(
        ctx.truth_root / "reports/pre_open_bundle_v1" / ctx.day_utc / "pre_open_bundle.v1.json",
        {"materialization_state": pre_open, "canonical_blocker": "IB_API_HANDSHAKE_NOT_OK" if pre_open != "COMPLETE" else ""},
    )
    _write(
        ctx.truth_root / "reports/execution_mode_authority_v1" / ctx.day_utc / "execution_mode_authority.v1.json",
        {"mode_state": "DRY_RUN_LOCKED"},
    )
    _write(
        ctx.truth_root / "reports/runtime_service_authority_v1" / ctx.day_utc / "runtime_service_authority.v1.json",
        {"service_state": "MANUAL_MODE_READY"},
    )
    _write(ctx.operator_input_root / "operator_inputs/paper_capital_seed_v1" / ctx.day_utc / "paper_capital_seed.v1.json", {"seed_usd": "0.00"})
    if operator:
        _write(ctx.operator_input_root / "operator_inputs/cash_ledger_operator_statements" / ctx.day_utc / "operator_statement.v1.json", {"cash_total": "0.00"})


def _seed_paper_ready_inputs(ctx: bod.BodContext, *, risk_state: str = "PASS") -> None:
    _write(ctx.truth_root / "reports/market_data_authority_v1" / ctx.day_utc / "market_data_authority.v1.json", {"market_data_state": "READY"})
    _write(ctx.truth_root / "reports/strategy_decision_authority_v1" / ctx.day_utc / "strategy_decision_authority.v1.json", {"strategy_decision_state": "READY"})
    _write(
        ctx.truth_root / "reports/risk_sizing_authority_v1" / ctx.day_utc / "risk_sizing_authority.v1.json",
        {"risk_sizing_state": risk_state, "canonical_blocker": "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE" if risk_state != "PASS" else ""},
    )
    _write(
        ctx.truth_root / "reports/submit_boundary_status_v1" / ctx.day_utc / "submit_boundary_status.v1.json",
        {"boundary_status": "AUTHORIZED", "submission_authorized": True},
    )


def test_full_lifecycle_from_bod_to_paper_ready_passes_when_inputs_present(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _seed_pre_open_inputs(ctx)
    _seed_paper_ready_inputs(ctx)
    monkeypatch.setattr(preopen.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    monkeypatch.setattr(paper.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    assert preopen.main(["--day_utc", DAY]) == 0
    assert paper.main(["--day_utc", DAY]) == 0

    lifecycle = read_lifecycle_state_v1(truth_root=ctx.truth_root, day_utc=DAY)
    assert lifecycle["state"] == "PAPER_READY"


def test_ib_unavailable_blocks_pre_open(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _seed_pre_open_inputs(ctx, pre_open="BLOCKED")
    monkeypatch.setattr(preopen.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    assert preopen.main(["--day_utc", DAY]) == 2
    payload = json.loads((ctx.truth_root / "reports/aegis_pre_open_verify_v1" / DAY / "aegis_pre_open_verify.v1.json").read_text(encoding="utf-8"))
    assert payload["status"] == "PRE_OPEN_BLOCKED"
    assert payload["canonical_blocker"] == "IB_API_HANDSHAKE_NOT_OK"


def test_missing_operator_statement_blocks_pre_open(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _seed_pre_open_inputs(ctx, operator=False)
    monkeypatch.setattr(preopen.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    assert preopen.main(["--day_utc", DAY]) == 2
    payload = json.loads((ctx.truth_root / "reports/aegis_pre_open_verify_v1" / DAY / "aegis_pre_open_verify.v1.json").read_text(encoding="utf-8"))
    assert payload["canonical_blocker"] == "OPERATOR_STATEMENT_MISSING"


def test_missing_options_data_reports_market_data_unavailable(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)

    def fake_child(name: str, cmd: list[str], **_kwargs):
        if name == "options_chain_snapshot":
            return bod._internal_step(name, status="BLOCKED", blocker="OPTIONS_CHAIN_SNAPSHOT_MISSING", command=cmd)
        return bod._internal_step(name, status="PASS", command=cmd)

    monkeypatch.setattr(market.bod, "_run_child_with_retries", fake_child)
    payload = market.run_market_and_strategy_prepare_v1(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["failure_classification"] == "MARKET_DATA_UNAVAILABLE"


def test_missing_phasec_evidence_blocks_paper_ready(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _seed_pre_open_inputs(ctx)
    _seed_paper_ready_inputs(ctx, risk_state="BLOCKED")
    write_lifecycle_transition_v1(truth_root=ctx.truth_root, day_utc=DAY, state="PRE_OPEN_READY", producer="test")
    monkeypatch.setattr(paper.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    assert paper.main(["--day_utc", DAY]) == 2
    payload = json.loads((ctx.truth_root / "reports/aegis_paper_ready_v1" / DAY / "aegis_paper_ready.v1.json").read_text(encoding="utf-8"))
    assert payload["status"] == "NOT_READY"
    assert payload["canonical_blocker"] == "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE"


def test_rerunning_full_pipeline_is_idempotent_for_master_manifest(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)

    monkeypatch.setattr(master.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    monkeypatch.setattr(master.bod, "_run_child", lambda name, cmd, **_kwargs: bod._internal_step(name, status="PASS", command=cmd))

    assert master.main(["--day_utc", DAY]) == 0
    assert master.main(["--day_utc", DAY]) == 0

    manifests = list((ctx.truth_root / "reports/aegis_day_prepare_v1" / DAY).glob("*.json"))
    assert manifests == [ctx.truth_root / "reports/aegis_day_prepare_v1" / DAY / "aegis_day_prepare.v1.json"]


def test_pipeline_outputs_do_not_target_source_repo(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)

    assert not master._manifest_path(ctx).resolve().is_relative_to(SOURCE_ROOT)
    assert not preopen._manifest_path(ctx).resolve().is_relative_to(SOURCE_ROOT)
    assert not market._manifest_path(ctx).resolve().is_relative_to(SOURCE_ROOT)
    assert not paper._manifest_path(ctx).resolve().is_relative_to(SOURCE_ROOT)


def test_prior_day_lifecycle_artifacts_are_preserved(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    prior_path = write_lifecycle_transition_v1(truth_root=ctx.truth_root, day_utc=PRIOR_DAY, state="EOD_COMPLETE", producer="test")
    before = prior_path.read_text(encoding="utf-8")

    write_lifecycle_transition_v1(truth_root=ctx.truth_root, day_utc=DAY, state="BOD_PENDING", producer="test")

    assert prior_path.read_text(encoding="utf-8") == before
