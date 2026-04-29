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
import ops.tools.run_market_data_supply_v1 as supply  # noqa: E402
from ops.tools import run_aegis_bod_prepare_v1 as bod  # noqa: E402


def _ctx(tmp_path: Path, day: str = "2026-04-29") -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(day, "PAPER", truth, execution, runtime, operator, "DU123456")


def _requirement(ctx: bod.BodContext, *, source_type: str = "ACTIVE_INTENT", day: str | None = None) -> Path:
    day_utc = day or ctx.day_utc
    path = ctx.truth_root / "reports" / "aegis_requirement_graph_v1" / day_utc / "requirement_graph.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "day_utc": day_utc,
        "requirements": [
            {
                "owner_phase": "MARKET_DATA",
                "requirement_id": "MARKET_DATA:intent_spy:SPY:BID_ASK_QUOTES",
                "source_type": source_type,
                "source_id": "intent_spy" if source_type == "ACTIVE_INTENT" else "",
                "instrument": "SPY",
                "required_artifact": "bid_ask_quotes",
            }
        ],
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def _diag(ctx: bod.BodContext, errors: list[int], *, valid_quotes: int = 0, spot: bool = False, contracts: int = 0) -> Path:
    path = (
        ctx.execution_root
        / "reports"
        / "options_chain_capture_ib_day_v1"
        / ctx.day_utc
        / "ib_capture_SPY_20260429T010203Z_test"
        / "options_chain_capture_diagnostic.v1.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": "FAIL" if errors else "OK",
        "attempts": [
            {
                "market_data_type_requested": 3,
                "valid_quote_count": valid_quotes,
                "contract_details_count": contracts,
                "spot_snapshot": {"last": "500.00"} if spot else {},
                "ib_errors": [
                    {"error_code": code, "error_detail": f"IB error {code}", "req_id": code}
                    for code in errors
                ],
            }
        ],
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def _snapshot(ctx: bod.BodContext, *, day: str | None = None, fresh: bool = True, quotes: bool = True) -> tuple[Path, Path]:
    day_utc = day or ctx.day_utc
    root = ctx.execution_root / "options_chain_snapshot_v1" / day_utc / "capture"
    root.mkdir(parents=True, exist_ok=True)
    snap = root / "options_chain_snapshot.v1.json"
    cert = root / "freshness_certificate.v1.json"
    contract = {"bid": "1.00", "ask": "1.10"} if quotes else {"strike": "500"}
    snap.write_text(
        json.dumps(
            {
                "schema_id": "options_chain_snapshot",
                "schema_version": 1,
                "as_of_utc": f"{day_utc}T14:30:00Z",
                "underlying": {"symbol": "SPY", "spot_price": "500.00"},
                "contracts": [contract],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    cert.write_text(
        json.dumps({"valid_until_utc": "2099-01-01T00:00:00Z" if fresh else "2020-01-01T00:00:00Z"}),
        encoding="utf-8",
    )
    return snap, cert


def test_owned_spy_active_intent_creates_market_data_requirements(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _diag(ctx, [10091], spot=True, contracts=1)
    payload = supply.build_market_data_supply(ctx)

    assert payload["requirements"]
    assert payload["requirements"][0]["source_type"] == "ACTIVE_INTENT"
    assert payload["requirements"][0]["instrument"] == "SPY"


def test_unowned_default_requirement_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx, source_type="LIFECYCLE_PHASE")
    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "MARKET_DATA_REQUIREMENT_UNOWNED"


@pytest.mark.parametrize("code", [10089, 10091, 10167])
def test_ib_permission_errors_block_with_permission_denied(tmp_path: Path, code: int) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _diag(ctx, [code], spot=True, contracts=1)
    payload = supply.build_market_data_supply(ctx)

    assert payload["canonical_blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
    assert any(row["blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED" for row in payload["provider_checks"])


def test_permission_denied_blocks_before_generic_snapshot_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _diag(ctx, [10091], spot=True, contracts=1)
    monkeypatch.setattr(supply, "_run_capture", lambda *_args, **_kwargs: pytest.fail("capture should not run"))

    payload = supply.build_market_data_supply(ctx)

    assert payload["canonical_blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
    assert payload["capture_attempts"] == []


def test_valid_current_day_snapshot_and_freshness_pass(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _diag(ctx, [], valid_quotes=1, spot=True, contracts=1)
    _snapshot(ctx)
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(supply, "_run_market_data_authority", lambda _ctx: ({"exit_code": 0}, ""))

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""


def test_wrong_day_snapshot_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _diag(ctx, [], valid_quotes=1, spot=True, contracts=1)
    _snapshot(ctx, day="2026-04-28")
    monkeypatch.setattr(supply, "_run_capture", lambda *_args, **_kwargs: {"instrument": "SPY", "status": "PASS", "blocker": "", "snapshot_path": "", "freshness_certificate_path": ""})

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_CAPTURE_FAILED"


def test_stale_snapshot_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _diag(ctx, [], valid_quotes=1, spot=True, contracts=1)
    _snapshot(ctx, fresh=False)
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)

    payload = supply.build_market_data_supply(ctx)

    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_STALE"


def test_missing_quote_fields_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _diag(ctx, [], valid_quotes=1, spot=True, contracts=1)
    _snapshot(ctx, quotes=False)
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)

    payload = supply.build_market_data_supply(ctx)

    assert payload["canonical_blocker"] == "OPTIONS_QUOTES_MISSING"


def test_day_ledger_market_data_phase_consumes_supply_only(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = day_run.PhaseContext("2026-04-29", "PAPER", tmp_path / "truth", tmp_path / "execution", tmp_path / "runtime", tmp_path / "operator", "DU123456")
    supply_path = day_run._market_data_supply_path(ctx)
    supply_path.parent.mkdir(parents=True, exist_ok=True)
    supply_path.write_text('{"status":"BLOCKED","canonical_blocker":"OPTIONS_MARKET_DATA_PERMISSION_DENIED","requirements":[{"requirement_id":"REQ1","instrument":"SPY"}],"provider_checks":[{"provider":"IBKR","capability":"OPTIONS_BID_ASK_QUOTES","status":"UNAVAILABLE","blocker":"OPTIONS_MARKET_DATA_PERMISSION_DENIED"}]}\n', encoding="utf-8")

    def _fake_run_steps(_phase, commands, **_kwargs):  # noqa: ANN001
        assert len(commands) == 1
        assert commands[0][0] == "market_data_supply"
        assert commands[0][1][1] == "ops/tools/run_market_data_supply_v1.py"
        return ([{"step_name": "market_data_supply", "status": "BLOCKED", "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED", "duration_ms": 1}], [str(supply_path)], ["OPTIONS_MARKET_DATA_PERMISSION_DENIED"])

    monkeypatch.setattr(day_run, "_run_steps", _fake_run_steps)
    row = day_run._phase_market_data(ctx, {})

    assert row["status"] == "BLOCKED"
    assert row["canonical_blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"


def test_packet_displays_market_data_supply_root_details(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    execution = tmp_path / "execution"
    ledger = truth / "reports" / "aegis_day_run_v1" / "2026-04-29" / "day_run.v1.json"
    req = truth / "reports" / "aegis_requirement_graph_v1" / "2026-04-29" / "requirement_graph.v1.json"
    mds = truth / "reports" / "market_data_supply_v1" / "2026-04-29" / "market_data_supply.v1.json"
    for path in (ledger, req, mds):
        path.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text('{"day_utc":"2026-04-29","environment":"PAPER","final_status":"NOT_READY","canonical_phase":"MARKET_DATA","canonical_blocker":"OPTIONS_MARKET_DATA_PERMISSION_DENIED"}', encoding="utf-8")
    req.write_text('{"day_utc":"2026-04-29","root_requirement":{}}', encoding="utf-8")
    mds.write_text(
        json.dumps(
            {
                "day_utc": "2026-04-29",
                "status": "BLOCKED",
                "canonical_blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
                "requirements": [{"requirement_id": "REQ1", "source_id": "intent_spy", "instrument": "SPY"}],
                "provider_checks": [{"provider": "IBKR", "status": "UNAVAILABLE", "capability": "OPTIONS_BID_ASK_QUOTES", "evidence": [{"ib_error_code": 10091}], "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED"}],
                "operator_next_action": "Enable IBKR API market-data permissions",
            }
        ),
        encoding="utf-8",
    )
    roots = packet.RootResolution(truth, execution, tmp_path / "truth_sleeves", "test", "test", "")
    monkeypatch.setattr(packet, "_build_current_calendar_day_runtime_status", lambda _roots: packet.CurrentCalendarDayStatus("2026-04-29", "true", False, "x", "GRANTED", "NOT_READY", "LEGACY", "legacy", "legacy"))
    monkeypatch.setattr(packet, "_build_latest_trading_day_evidence_status", lambda _roots: packet.LatestTradingDayEvidenceStatus("2026-04-29", "NOT_READY", "LEGACY", "x", "x", "x", "x", "x", "x", "x", "x", "x", "x", "x", "MANUAL_MODE_READY", "MISSING_REQUIRED_DATA", "PRE_SUBMIT_BLOCKER", "MARKET_DATA_BLOCKED", "1", "", "OK", "0", "0", "BLOCKED", "{}", "DRY_RUN_LOCKED", "PAPER", "false", "NO_TRADES_CLOSED", "x", "x", "legacy"))

    section = packet._build_paper_status(roots).section

    assert "- market_data_supply_path: " in section
    assert "- market_data_supply_requirement_id: REQ1" in section
    assert "- market_data_supply_provider: IBKR" in section
    assert "10091" in section


def test_market_data_supply_writes_only_runtime_artifact(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    payload = supply.build_market_data_supply(ctx)
    path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    supply._write_json(path, payload)

    assert path.exists()
    assert str(path).startswith(str(ctx.truth_root))
    assert not str(path).startswith(str(REPO_ROOT))
