from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.aegis_chatgpt_packet as packet  # noqa: E402
import ops.tools.run_aegis_day_v1 as day_run  # noqa: E402
import ops.tools.run_ib_market_data_entitlement_probe_v1 as entitlement  # noqa: E402
import ops.tools.run_aegis_requirement_graph_v1 as req_graph  # noqa: E402
import ops.tools.run_market_open_data_gate_v1 as open_gate  # noqa: E402
import ops.tools.run_market_data_supply_v1 as supply  # noqa: E402
import ops.tools.run_options_chain_snapshot_required_day_v1 as options_required  # noqa: E402
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


def _same_day_options_readiness(monkeypatch: pytest.MonkeyPatch, ctx: bod.BodContext, *, session_state: str = "REGULAR") -> None:
    path = ctx.truth_root / "reports" / "trading_day_readiness_authority_v1" / ctx.day_utc / "trading_day_readiness_authority.v1.json"
    payload = {
        "status": "PASS",
        "readiness_mode": "MARKET_OPEN",
        "requires_same_day_options_snapshot": True,
        "session_state": session_state,
        "operator_next_action": "",
        "evidence_policy": {},
    }
    monkeypatch.setattr(open_gate, "read_or_evaluate_trading_day_readiness_authority_v1", lambda **_kwargs: (path, payload))


def _stale_preopen_readiness(monkeypatch: pytest.MonkeyPatch, ctx: bod.BodContext) -> None:
    path = ctx.truth_root / "reports" / "trading_day_readiness_authority_v1" / ctx.day_utc / "trading_day_readiness_authority.v1.json"
    payload = {
        "status": "PASS",
        "readiness_mode": "PREOPEN_ADMISSION",
        "requires_same_day_options_snapshot": False,
        "session_state": "PRE_MARKET",
        "operator_next_action": "preopen",
        "evidence_policy": {"policy_id": "PREOPEN_CARRY_FORWARD_V1"},
    }
    monkeypatch.setattr(open_gate, "read_or_evaluate_trading_day_readiness_authority_v1", lambda **_kwargs: (path, payload))


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


def _entitlement(
    ctx: bod.BodContext,
    *,
    status: str = "BLOCKED",
    codes: list[int] | None = None,
    live: bool = False,
    delayed: bool = False,
) -> Path:
    path = supply._entitlement_probe_path(ctx, "SPY")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "symbol": "SPY",
        "status": status,
        "canonical_blocker": "" if status == "PASS" else "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
        "path": str(path),
        "tested_data_types": [
            {"market_data_type": 1, "name": "LIVE", "readiness_eligible": True},
            {"market_data_type": 2, "name": "FROZEN", "readiness_eligible": False},
            {"market_data_type": 3, "name": "DELAYED", "readiness_eligible": False},
            {"market_data_type": 4, "name": "DELAYED_FROZEN", "readiness_eligible": False},
        ],
        "live_data_available": live,
        "delayed_data_available": delayed,
        "ib_error_codes": codes or [],
        "ib_errors": [
            {"error_code": code, "error_message": f"IB error {code}", "req_id": code}
            for code in (codes or [])
        ],
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def _policy(ctx: bod.BodContext, *, allow: bool = True, mode: str = "PAPER") -> Path:
    path = ctx.truth_root / "governance" / "paper_market_data_policy_v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "mode": mode,
                "allow_delayed_data": allow,
                "allowed_data_types": ["DELAYED", "DELAYED_FROZEN"],
                "requirements": [
                    "underlying spot present",
                    "option chain present",
                    "bid/ask or equivalent quote fields present",
                    "quote timestamp available",
                ],
                "accepted_quote_fields": ["bid", "ask", "last", "close", "mark", "midpoint"],
                "minimum_acceptable_mode": "BID_ASK_REQUIRED",
                "reason": "paper trading requires testable data path when IB live API entitlement is unavailable",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _snapshot(
    ctx: bod.BodContext,
    *,
    day: str | None = None,
    symbol: str = "SPY",
    fresh: bool = True,
    quotes: bool = True,
    timestamps: bool = True,
    market_data_type: int = 1,
) -> tuple[Path, Path]:
    day_utc = day or ctx.day_utc
    root = ctx.execution_root / "options_chain_snapshot_v1" / day_utc / "capture"
    root.mkdir(parents=True, exist_ok=True)
    snap = root / "options_chain_snapshot.v1.json"
    cert = root / "freshness_certificate.v1.json"
    contract = {"bid": "1.00", "ask": "1.10"} if quotes else {"strike": "500"}
    underlying = {"symbol": symbol, "spot_price": "500.00"}
    if timestamps:
        underlying["spot_as_of_utc"] = f"{day_utc}T14:30:00Z"
    payload = {
        "schema_id": "options_chain_snapshot",
        "schema_version": 1,
        "underlying": underlying,
        "contracts": [contract],
        "provenance": {"market_data_type": market_data_type},
    }
    if timestamps:
        payload["as_of_utc"] = f"{day_utc}T14:30:00Z"
    snap.write_text(
        json.dumps(payload, sort_keys=True),
        encoding="utf-8",
    )
    cert.write_text(
        json.dumps({"valid_until_utc": "2099-01-01T00:00:00Z" if fresh else "2020-01-01T00:00:00Z"}),
        encoding="utf-8",
    )
    return snap, cert


def _add_dte_coverage(snapshot_path: Path, *, omitted: bool = False) -> None:
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload["provenance"]["capture_method"] = "IBKR_SNAPSHOT_DAY_ANCHORED"
    payload["derived"] = {
        "derivation_policy": {
            "dte_method": "CALENDAR_DAYS_UTC",
            "dte_window_policy": {
                "policy_dte_min": 1,
                "policy_dte_max": 7,
                "max_expiries_to_capture": 7,
                "expiries_available": [
                    {"dte": 1, "expiry_yyyymmdd": "20260430"},
                    {"dte": 2, "expiry_yyyymmdd": "20260501"},
                ],
                "expiries_evaluated": [{"dte": 1, "expiry_yyyymmdd": "20260430"}],
                "expiries_omitted": (
                    [{"dte": 2, "expiry_yyyymmdd": "20260501", "reason": "CAPTURE_TIME_BUDGET_EXHAUSTED"}]
                    if omitted
                    else []
                ),
                "omission_reason": "CAPTURE_TIME_BUDGET_EXHAUSTED" if omitted else "",
            },
        }
    }
    snapshot_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _selected_pointer(ctx: bod.BodContext, *, symbol: str = "SPY") -> Path:
    intent_path = ctx.execution_root / "intents_v1" / "snapshots" / ctx.day_utc / f"{symbol.lower()}.exposure_intent.v1.json"
    intent_path.parent.mkdir(parents=True, exist_ok=True)
    intent_path.write_text(
        json.dumps(
            {
                "schema_id": "exposure_intent",
                "schema_version": "v1",
                "intent_id": f"intent_{symbol.lower()}",
                "underlying": {"symbol": symbol},
                "option": {"structure": "PUT"},
                "exposure_type": "SHORT_VOL_DEFINED",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    pointer = open_gate.selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text(
        json.dumps(
            {
                "schema_id": "selected_intent_pointer",
                "schema_version": "v1",
                "day_utc": ctx.day_utc,
                "environment": "PAPER",
                "status": "SELECTED",
                "canonical_blocker": "",
                "selected_intent": {"intent_id": f"intent_{symbol.lower()}", "intent_path": str(intent_path), "symbol": symbol},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return pointer


def _selected_long_equity_pointer(ctx: bod.BodContext, *, symbol: str = "QQQ") -> Path:
    intent_path = ctx.execution_root / "intents_v1" / "snapshots" / ctx.day_utc / f"{symbol.lower()}_long.exposure_intent.v1.json"
    intent_path.parent.mkdir(parents=True, exist_ok=True)
    intent_path.write_text(
        json.dumps(
            {
                "schema_id": "exposure_intent",
                "schema_version": "v1",
                "day_utc": ctx.day_utc,
                "intent_id": f"intent_{symbol.lower()}_long",
                "underlying": {"symbol": symbol},
                "symbol": symbol,
                "exposure_type": "LONG_EQUITY",
                "target_notional_pct": "0.10",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    pointer = open_gate.selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text(
        json.dumps(
            {
                "schema_id": "selected_intent_pointer",
                "schema_version": "v1",
                "day_utc": ctx.day_utc,
                "environment": "PAPER",
                "status": "SELECTED",
                "canonical_blocker": "",
                "selected_intent": {"intent_id": f"intent_{symbol.lower()}_long", "intent_path": str(intent_path), "symbol": symbol},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return pointer


def test_owned_spy_active_intent_creates_market_data_requirements(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _diag(ctx, [10091], spot=True, contracts=1)
    payload = supply.build_market_data_supply(ctx)

    assert payload["requirements"]
    assert payload["requirements"][0]["source_type"] == "ACTIVE_INTENT"
    assert payload["requirements"][0]["instrument"] == "SPY"


def test_long_equity_selected_intent_creates_equity_market_data_requirements(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _selected_long_equity_pointer(ctx, symbol="QQQ")

    payload = req_graph.build_requirement_graph(ctx)

    rows = [
        row for row in payload["requirements"]
        if row.get("owner_phase") == "MARKET_DATA"
        and row.get("source_type") == "ACTIVE_INTENT"
        and row.get("instrument") == "QQQ"
    ]
    assert [row["required_artifact"] for row in rows] == [
        "underlying_spot",
        "bid_ask_quotes",
        "freshness_certificate",
    ]
    assert payload["active_intents"][0]["requires_equity_market_data"] is True


def test_missing_requirement_graph_blocks_when_selected_intent_is_active(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _selected_long_equity_pointer(ctx, symbol="QQQ")

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "MARKET_DATA_REQUIREMENT_GRAPH_MISSING"
    assert payload["active_selected_intent"]["symbol"] == "QQQ"


def test_empty_requirement_graph_blocks_for_active_selected_intent(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _selected_long_equity_pointer(ctx, symbol="QQQ")
    path = ctx.truth_root / "reports" / "aegis_requirement_graph_v1" / ctx.day_utc / "requirement_graph.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"day_utc": ctx.day_utc, "requirements": []}), encoding="utf-8")

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "MARKET_DATA_REQUIREMENTS_EMPTY_FOR_ACTIVE_INTENT"


def test_valid_qqq_long_equity_market_evidence_allows_supply_pass(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _selected_long_equity_pointer(ctx, symbol="QQQ")
    graph = req_graph.build_requirement_graph(ctx)
    graph_path = ctx.truth_root / "reports" / "aegis_requirement_graph_v1" / ctx.day_utc / "requirement_graph.v1.json"
    graph_path.parent.mkdir(parents=True, exist_ok=True)
    graph_path.write_text(json.dumps(graph, sort_keys=True), encoding="utf-8")
    _diag(ctx, [], valid_quotes=1, spot=True, contracts=1)
    _snapshot(ctx, symbol="QQQ")
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "_run_capture", lambda _ctx, instrument: {"instrument": instrument, "status": "PASS", "blocker": "", "snapshot_path": "x", "freshness_certificate_path": "y"})
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(supply, "_run_market_data_authority", lambda _ctx: ({"exit_code": 0}, ""))

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert {row["instrument"] for row in payload["requirements"]} == {"QQQ"}


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


@pytest.mark.parametrize(
    ("code", "meaning"),
    [
        (10089, "API_MARKET_DATA_SUBSCRIPTION_MISSING"),
        (10091, "PARTIAL_MARKET_DATA_SUBSCRIPTION_MISSING"),
        (10167, "MARKET_DATA_NOT_SUBSCRIBED_DELAYED_AVAILABLE"),
    ],
)
def test_entitlement_probe_maps_ib_market_data_errors(code: int, meaning: str) -> None:
    config = entitlement.EntitlementProbeConfig(
        day_utc="2026-04-29",
        environment="PAPER",
        symbol="SPY",
        host="127.0.0.1",
        port=4002,
        client_id=181,
        account="DU123456",
        timeout_seconds=1.0,
    )
    payload = entitlement.evaluate_entitlement_probe_v1(
        config,
        {
            "connected": True,
            "requests": [
                {
                    "requested_market_data_type": 1,
                    "has_any_price": False,
                    "errors": [{"error_code": code, "error_message": "err", "req_id": 1}],
                }
            ],
        },
    )

    assert payload["canonical_blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
    assert {"ib_error_code": code, "meaning": meaning} in payload["error_mapping"]


def test_ib_10167_records_delayed_available_but_not_accepted_without_policy(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _entitlement(ctx, codes=[10167], live=False, delayed=True)

    payload = supply.build_market_data_supply(ctx)

    assert payload["canonical_blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
    assert payload["delayed_data_available"] is True
    assert payload["delayed_data_accepted_by_policy"] is False
    assert payload["delayed_data_policy_result"] == "DELAYED_DATA_AVAILABLE_NOT_ACCEPTED"


def test_delayed_data_cannot_satisfy_without_explicit_governed_policy(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _entitlement(ctx, status="BLOCKED", codes=[10167], live=False, delayed=True)
    _snapshot(ctx)

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
    assert payload["delayed_data_policy"]["policy_decision"] == "DELAYED_DATA_POLICY_MISSING"


def test_live_market_data_available_lets_supply_attempt_capture(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _entitlement(ctx, status="PASS", codes=[], live=True, delayed=False)
    calls: list[str] = []

    def _capture(_ctx, instrument):  # noqa: ANN001
        calls.append(instrument)
        _snapshot(ctx)
        return {"instrument": instrument, "status": "PASS", "blocker": "", "snapshot_path": "x", "freshness_certificate_path": "y"}

    monkeypatch.setattr(supply, "_run_capture", _capture)
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(supply, "_run_market_data_authority", lambda _ctx: ({"exit_code": 0}, ""))

    payload = supply.build_market_data_supply(ctx)

    assert calls == ["SPY"]
    assert payload["status"] == "PASS"


def test_live_unavailable_delayed_available_policy_enabled_passes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    policy = _policy(ctx)
    _entitlement(ctx, status="BLOCKED", codes=[10167], live=False, delayed=True)
    _snapshot(ctx, market_data_type=3)
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(supply, "_run_market_data_authority", lambda _ctx: ({"exit_code": 0}, ""))

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert payload["delayed_data_used"] is True
    assert payload["market_data_mode"] == "DELAYED"
    assert payload["policy_source_path"] == str(policy)


def test_delayed_policy_makes_supply_continue_to_capture(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _policy(ctx)
    _entitlement(ctx, status="BLOCKED", codes=[10167], live=False, delayed=True)
    calls: list[str] = []

    def _capture(_ctx, instrument):  # noqa: ANN001
        calls.append(instrument)
        _snapshot(ctx, market_data_type=3)
        return {"instrument": instrument, "status": "PASS", "blocker": "", "snapshot_path": "x", "freshness_certificate_path": "y"}

    monkeypatch.setattr(supply, "_run_capture", _capture)
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(supply, "_run_market_data_authority", lambda _ctx: ({"exit_code": 0}, ""))

    payload = supply.build_market_data_supply(ctx)

    assert calls == ["SPY"]
    assert payload["status"] == "PASS"
    assert payload["capture_attempts"][0]["data_mode"] == "DELAYED"


def test_delayed_underlying_chain_zero_option_callbacks_reports_not_returned(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _policy(ctx)
    _entitlement(ctx, status="BLOCKED", codes=[10167], live=False, delayed=True)
    diag = _diag(ctx, [], spot=True, contracts=23)
    payload = json.loads(diag.read_text(encoding="utf-8"))
    payload["attempts"][0]["market_data_type_requested"] = 3
    payload["attempts"][0]["quote_request_count"] = 23
    payload["attempts"][0]["quote_samples"] = [{"observed_tick_types": [], "option_computation": {}, "valid_quote": False}]
    diag.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    def _capture(_ctx, instrument):  # noqa: ANN001
        return {
            "instrument": instrument,
            "status": "BLOCKED",
            "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
            "snapshot_path": "",
            "freshness_certificate_path": "",
            "stdout_summary": "market_data_type=3:NO_VALID_OPTION_QUOTES_CAPTURED",
            "stderr_summary": "",
        }

    monkeypatch.setattr(supply, "_run_capture", _capture)
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "_inside_regular_us_options_hours", lambda: True)

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB"
    assert payload["capture_attempts"][0]["blocker"] == "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB"


def test_delayed_callbacks_with_close_only_rejected_by_bid_ask_policy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _policy(ctx)
    _entitlement(ctx, status="BLOCKED", codes=[10167], live=False, delayed=True)
    diag = _diag(ctx, [], spot=True, contracts=23)
    payload = json.loads(diag.read_text(encoding="utf-8"))
    payload["attempts"][0]["market_data_type_requested"] = 3
    payload["attempts"][0]["quote_samples"] = [{"observed_tick_types": [75], "delayed_close": "0.64", "valid_quote": False}]
    diag.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    monkeypatch.setattr(
        supply,
        "_run_capture",
        lambda _ctx, instrument: {
            "instrument": instrument,
            "status": "BLOCKED",
            "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
            "snapshot_path": "",
            "freshness_certificate_path": "",
            "stdout_summary": "market_data_type=3:NO_VALID_OPTION_QUOTES_CAPTURED",
            "stderr_summary": "",
        },
    )
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "_inside_regular_us_options_hours", lambda: True)

    result = supply.build_market_data_supply(ctx)

    assert result["canonical_blocker"] == "OPTIONS_MARKET_DATA_POLICY_REJECTED_QUOTE_TYPE"


def test_outside_us_options_hours_reports_specific_blocker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _policy(ctx)
    _entitlement(ctx, status="BLOCKED", codes=[10167], live=False, delayed=True)

    monkeypatch.setattr(
        supply,
        "_run_capture",
        lambda _ctx, instrument: {
            "instrument": instrument,
            "status": "BLOCKED",
            "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
            "snapshot_path": "",
            "freshness_certificate_path": "",
            "stdout_summary": "market_data_type=3:NO_VALID_OPTION_QUOTES_CAPTURED",
            "stderr_summary": "",
        },
    )
    monkeypatch.setattr(supply, "_market_session_state", lambda: "AFTER_HOURS")
    monkeypatch.setattr(supply, "_inside_regular_us_options_hours", lambda: False)

    result = supply.build_market_data_supply(ctx)

    assert result["canonical_blocker"] == "OPTIONS_QUOTES_UNAVAILABLE_OUTSIDE_MARKET_HOURS"


def test_delayed_data_requires_quotes_and_timestamps(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _policy(ctx)
    _entitlement(ctx, status="BLOCKED", codes=[10167], live=False, delayed=True)
    _snapshot(ctx, market_data_type=3, timestamps=False)
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_STALE"


def test_live_mode_blocks_when_only_delayed_data_exists(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    live_ctx = bod.BodContext(ctx.day_utc, "LIVE", ctx.truth_root, ctx.execution_root, ctx.runtime_root, ctx.operator_input_root, ctx.ib_account)
    _requirement(live_ctx)
    _policy(live_ctx)
    _entitlement(live_ctx, status="BLOCKED", codes=[10167], live=False, delayed=True)
    _snapshot(live_ctx, market_data_type=3)
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)

    payload = supply.build_market_data_supply(live_ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
    assert payload["delayed_data_accepted_by_policy"] is False


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
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
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
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "_run_capture", lambda *_args, **_kwargs: {"instrument": "SPY", "status": "PASS", "blocker": "", "snapshot_path": "", "freshness_certificate_path": ""})

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_CAPTURE_FAILED"


def test_stale_snapshot_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _diag(ctx, [], valid_quotes=1, spot=True, contracts=1)
    _snapshot(ctx, fresh=False)
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)

    payload = supply.build_market_data_supply(ctx)

    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_STALE"


def test_missing_quote_fields_blocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _diag(ctx, [], valid_quotes=1, spot=True, contracts=1)
    _snapshot(ctx, quotes=False)
    monkeypatch.setattr(supply, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(supply, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)

    payload = supply.build_market_data_supply(ctx)

    assert payload["canonical_blocker"] == "OPTIONS_QUOTES_MISSING_BID_ASK"


def test_pre_market_missing_bid_ask_returns_pending(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _policy(ctx)
    _entitlement(ctx, status="BLOCKED", codes=[10167], live=False, delayed=True)
    monkeypatch.setattr(supply, "_market_session_state", lambda: "PRE_MARKET")
    monkeypatch.setattr(
        supply,
        "_run_capture",
        lambda _ctx, instrument: {
            "instrument": instrument,
            "status": "BLOCKED",
            "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
            "snapshot_path": "",
            "freshness_certificate_path": "",
        },
    )

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "PRE_MARKET_PENDING"
    assert payload["canonical_blocker"] == "MARKET_OPEN_DATA_PENDING"


def test_pre_market_entitlement_denied_still_blocks(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    _entitlement(ctx, status="BLOCKED", codes=[10089], live=False, delayed=False)

    payload = supply.build_market_data_supply(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"


def test_market_open_gate_before_open_is_pending(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx, session_state="PRE_MARKET")
    _selected_pointer(ctx)
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "PRE_MARKET")
    monkeypatch.setattr(open_gate, "_run_capture", lambda *_args, **_kwargs: pytest.fail("capture should not run before open"))

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PENDING"
    assert payload["canonical_blocker"] == "MARKET_NOT_OPEN"
    assert payload["capture_attempted_by_gate"] is False


def test_market_open_gate_skips_capture_without_selected_intent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    pointer = open_gate.selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text(
        json.dumps(
            {
                "schema_id": "selected_intent_pointer",
                "schema_version": "v1",
                "day_utc": ctx.day_utc,
                "environment": "PAPER",
                "status": "NO_EXECUTABLE_INTENT",
                "canonical_blocker": "NO_EXECUTABLE_INTENT",
                "selected_intent": {},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: pytest.fail("market data supply should not run without selected intent"))
    monkeypatch.setattr(open_gate, "_run_capture", lambda *_args, **_kwargs: pytest.fail("capture should not run without selected intent"))

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert payload["capture_attempted_by_gate"] is False


def test_market_open_gate_prioritizes_no_intent_over_after_hours(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx, session_state="AFTER_HOURS")
    pointer = open_gate.selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text(
        json.dumps(
            {
                "schema_id": "selected_intent_pointer",
                "schema_version": "v1",
                "day_utc": ctx.day_utc,
                "environment": "PAPER",
                "status": "NO_EXECUTABLE_INTENT",
                "canonical_blocker": "NO_EXECUTABLE_INTENT",
                "selected_intent": {},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "AFTER_HOURS")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: pytest.fail("market data supply should not run without selected intent"))
    monkeypatch.setattr(open_gate, "_run_capture", lambda *_args, **_kwargs: pytest.fail("capture should not run without selected intent"))

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert payload["market_session_state"] == "AFTER_HOURS"
    assert payload["capture_attempted_by_gate"] is False


def test_market_open_gate_passes_during_market_with_valid_supply(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx)
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    mds_path.parent.mkdir(parents=True, exist_ok=True)
    mds_path.write_text('{"status":"PASS","canonical_blocker":"","artifacts":[]}\n', encoding="utf-8")
    _snapshot(ctx)
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: {"exit_code": 0})

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""


def test_market_open_gate_passes_with_fresh_snapshot_when_supply_has_no_requirements(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx, symbol="IWM")
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    mds_path.parent.mkdir(parents=True, exist_ok=True)
    mds_path.write_text('{"status":"SKIPPED","canonical_blocker":"","requirements":[],"artifacts":[]}\n', encoding="utf-8")
    _snapshot(ctx, symbol="IWM")
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: {"exit_code": 0})

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert payload["diagnostics"]["quote_completeness_result"]["result"] == "PASS"


def test_market_open_gate_regular_hours_ignores_stale_preopen_readiness(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _stale_preopen_readiness(monkeypatch, ctx)
    _selected_pointer(ctx, symbol="IWM")
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    mds_path.parent.mkdir(parents=True, exist_ok=True)
    mds_path.write_text('{"status":"PASS","canonical_blocker":"","requirements":[{"instrument":"IWM"}],"artifacts":[]}\n', encoding="utf-8")
    _snapshot(ctx, symbol="IWM")
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: {"exit_code": 0})

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert payload["readiness_mode"] == "PREOPEN_ADMISSION"
    assert payload["diagnostics"]["market_session_result"] == "REGULAR"
    assert payload["diagnostics"]["readiness_session_result"] == "PRE_MARKET"


def test_market_open_gate_closed_session_fails_closed_even_with_valid_data(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx, session_state="AFTER_HOURS")
    _selected_pointer(ctx)
    _snapshot(ctx)
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "AFTER_HOURS")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: pytest.fail("market data supply should not run after hours"))

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PENDING"
    assert payload["canonical_blocker"] == "MARKET_NOT_OPEN"
    assert payload["diagnostics"]["specific_fail_closed_reason"] == "MARKET_CLOSED"


def test_market_open_gate_missing_evidence_fails_closed_with_specific_regular_session_blocker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx)
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    mds_path.parent.mkdir(parents=True, exist_ok=True)
    mds_path.write_text('{"status":"BLOCKED","canonical_blocker":"OPTIONS_SNAPSHOT_STALE","requirements":[{"instrument":"SPY"}]}\n', encoding="utf-8")
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: {"exit_code": 2})
    monkeypatch.setattr(
        open_gate,
        "_run_capture",
        lambda _ctx, instrument: {"instrument": instrument, "status": "BLOCKED", "blocker": "OPTIONS_SNAPSHOT_STALE", "snapshot_path": "", "freshness_certificate_path": ""},
    )

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PENDING"
    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_STALE"
    assert payload["diagnostics"]["specific_fail_closed_reason"] == "OPTIONS_SNAPSHOT_STALE"
    assert payload["diagnostics"]["quote_completeness_result"]["missing_symbols"] == ["SPY"]
    assert payload.get("submit_allowed") is not True
    assert payload.get("submission_authorized") is not True


def test_market_open_gate_stale_evidence_fails_closed_with_specific_regular_session_blocker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx)
    _snapshot(ctx, fresh=False)
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    mds_path.parent.mkdir(parents=True, exist_ok=True)
    mds_path.write_text('{"status":"BLOCKED","canonical_blocker":"OPTIONS_SNAPSHOT_STALE","requirements":[{"instrument":"SPY"}]}\n', encoding="utf-8")
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: {"exit_code": 2})
    monkeypatch.setattr(
        open_gate,
        "_run_capture",
        lambda _ctx, instrument: {"instrument": instrument, "status": "BLOCKED", "blocker": "OPTIONS_SNAPSHOT_STALE", "snapshot_path": "", "freshness_certificate_path": ""},
    )

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PENDING"
    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_STALE"
    assert payload["diagnostics"]["specific_fail_closed_reason"] == "OPTIONS_SNAPSHOT_STALE"
    assert payload["diagnostics"]["quote_completeness_result"]["stale_symbols"] == ["SPY"]


def test_market_open_gate_incomplete_quote_evidence_fails_closed_with_specific_regular_session_blocker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx)
    _snapshot(ctx, quotes=False)
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    mds_path.parent.mkdir(parents=True, exist_ok=True)
    mds_path.write_text('{"status":"BLOCKED","canonical_blocker":"OPTIONS_QUOTES_MISSING_BID_ASK","requirements":[{"instrument":"SPY"}]}\n', encoding="utf-8")
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: {"exit_code": 2})
    monkeypatch.setattr(
        open_gate,
        "_run_capture",
        lambda _ctx, instrument: {"instrument": instrument, "status": "BLOCKED", "blocker": "OPTIONS_QUOTES_MISSING_BID_ASK", "snapshot_path": "", "freshness_certificate_path": ""},
    )

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PENDING"
    assert payload["canonical_blocker"] == "OPTIONS_QUOTES_MISSING_BID_ASK"
    assert payload["diagnostics"]["specific_fail_closed_reason"] == "OPTIONS_QUOTES_MISSING_BID_ASK"
    assert payload["diagnostics"]["quote_completeness_result"]["incomplete_quote_fields"] == ["bid", "ask"]


def test_market_open_gate_uses_selected_intent_symbol(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx, symbol="IWM")
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    mds_path.parent.mkdir(parents=True, exist_ok=True)
    mds_path.write_text('{"status":"PASS","canonical_blocker":"","requirements":[{"instrument":"IWM"}],"artifacts":[]}\n', encoding="utf-8")
    _snapshot(ctx, symbol="IWM")
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: {"exit_code": 0})

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert "/options_chain_snapshot_v1/" in payload["snapshot_path"]


def test_market_open_gate_passes_with_explicit_dte_coverage_and_omissions(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx, symbol="IWM")
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    mds_path.parent.mkdir(parents=True, exist_ok=True)
    mds_path.write_text('{"status":"PASS","canonical_blocker":"","requirements":[{"instrument":"IWM"}],"artifacts":[]}\n', encoding="utf-8")
    snapshot_path, _cert_path = _snapshot(ctx, symbol="IWM")
    _add_dte_coverage(snapshot_path, omitted=True)
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: {"exit_code": 0})

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PASS"
    coverage = payload["diagnostics"]["dte_coverage"]
    assert coverage["expiries_evaluated"] == [{"dte": 1, "expiry_yyyymmdd": "20260430"}]
    assert coverage["expiries_omitted"] == [{"dte": 2, "expiry_yyyymmdd": "20260501", "reason": "CAPTURE_TIME_BUDGET_EXHAUSTED"}]


def test_market_open_gate_rejects_real_capture_snapshot_without_dte_coverage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx, symbol="IWM")
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    mds_path.parent.mkdir(parents=True, exist_ok=True)
    mds_path.write_text('{"status":"PASS","canonical_blocker":"","requirements":[{"instrument":"IWM"}],"artifacts":[]}\n', encoding="utf-8")
    snapshot_path, _cert_path = _snapshot(ctx, symbol="IWM")
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload["provenance"]["capture_method"] = "IBKR_SNAPSHOT_DAY_ANCHORED"
    snapshot_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: {"exit_code": 0})

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PENDING"
    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
    assert payload["diagnostics"]["quote_completeness_result"]["incomplete_quote_fields"] == [
        "derived.derivation_policy.dte_window_policy"
    ]


def test_market_open_gate_stale_snapshot_attempts_capture(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx)
    _snapshot(ctx, fresh=False)
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    calls = {"mds": 0, "capture": 0}

    def _mds(_ctx):  # noqa: ANN001
        calls["mds"] += 1
        mds_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"status": "BLOCKED", "canonical_blocker": "OPTIONS_SNAPSHOT_STALE", "requirements": [{"instrument": "SPY"}]}
        if calls["mds"] > 1:
            payload = {"status": "PASS", "canonical_blocker": "", "requirements": [{"instrument": "SPY"}]}
        mds_path.write_text(json.dumps(payload), encoding="utf-8")
        return {"exit_code": 0 if calls["mds"] > 1 else 2}

    def _capture(_ctx, instrument):  # noqa: ANN001
        calls["capture"] += 1
        _snapshot(ctx, fresh=True)
        return {"instrument": instrument, "status": "PASS", "blocker": "", "snapshot_path": "x", "freshness_certificate_path": "y"}

    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", _mds)
    monkeypatch.setattr(open_gate, "_run_capture", _capture)

    payload = open_gate.build_market_open_data_gate(ctx)

    assert calls["capture"] == 1
    assert payload["capture_attempted_by_gate"] is True
    assert payload["status"] == "PASS"


def test_market_open_gate_failed_capture_reports_specific_blocker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx)
    _snapshot(ctx, fresh=False)
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)

    def _mds(_ctx):  # noqa: ANN001
        mds_path.parent.mkdir(parents=True, exist_ok=True)
        mds_path.write_text('{"status":"BLOCKED","canonical_blocker":"OPTIONS_SNAPSHOT_STALE","requirements":[{"instrument":"SPY"}]}\n', encoding="utf-8")
        return {"exit_code": 2}

    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", _mds)
    monkeypatch.setattr(
        open_gate,
        "_run_capture",
        lambda _ctx, instrument: {"instrument": instrument, "status": "BLOCKED", "blocker": "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB", "snapshot_path": "", "freshness_certificate_path": ""},
    )

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PENDING"
    assert payload["canonical_blocker"] == "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB"
    assert payload["diagnostics"]["specific_fail_closed_reason"] == "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB"


def test_market_open_gate_missing_freshness_certificate_is_specific(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx)
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    calls = {"mds": 0}

    def _snapshot_without_cert() -> None:
        snap, cert = _snapshot(ctx, fresh=True)
        cert.unlink()
        assert snap.exists()

    def _mds(_ctx):  # noqa: ANN001
        calls["mds"] += 1
        mds_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"status": "BLOCKED", "canonical_blocker": "OPTIONS_SNAPSHOT_STALE", "requirements": [{"instrument": "SPY"}]}
        if calls["mds"] > 1:
            payload = {"status": "PASS", "canonical_blocker": "", "requirements": [{"instrument": "SPY"}]}
        mds_path.write_text(json.dumps(payload), encoding="utf-8")
        return {"exit_code": 0}

    def _capture(_ctx, instrument):  # noqa: ANN001
        _snapshot_without_cert()
        return {"instrument": instrument, "status": "PASS", "blocker": "", "snapshot_path": "x", "freshness_certificate_path": ""}

    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", _mds)
    monkeypatch.setattr(open_gate, "_run_capture", _capture)

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PENDING"
    assert payload["canonical_blocker"] == "OPTIONS_FRESHNESS_CERTIFICATE_MISSING"
    assert payload["diagnostics"]["specific_fail_closed_reason"] == "OPTIONS_FRESHNESS_CERTIFICATE_MISSING"


def test_market_open_gate_regular_session_capture_timeout_reports_data_blocker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _same_day_options_readiness(monkeypatch, ctx)
    _selected_pointer(ctx, symbol="IWM")
    _snapshot(ctx, symbol="IWM", fresh=False)
    mds_path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)

    def _mds(_ctx):  # noqa: ANN001
        mds_path.parent.mkdir(parents=True, exist_ok=True)
        mds_path.write_text('{"status":"BLOCKED","canonical_blocker":"OPTIONS_SNAPSHOT_STALE","requirements":[{"instrument":"IWM"}]}\n', encoding="utf-8")
        return {"exit_code": 2}

    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", _mds)
    monkeypatch.setattr(
        open_gate,
        "_run_capture",
        lambda _ctx, instrument: {
            "instrument": instrument,
            "status": "BLOCKED",
            "blocker": "OPTIONS_CAPTURE_TIMEOUT",
            "snapshot_path": str(ctx.execution_root / "options_chain_snapshot_v1" / ctx.day_utc),
            "freshness_certificate_path": "",
            "timeout_seconds": 180,
        },
    )

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PENDING"
    assert payload["canonical_blocker"] == "OPTIONS_CAPTURE_TIMEOUT"
    assert payload["diagnostics"]["market_session_result"] == "REGULAR"
    assert payload["diagnostics"]["specific_fail_closed_reason"] == "OPTIONS_CAPTURE_TIMEOUT"
    assert payload["capture_attempted_by_gate"] is True
    assert payload.get("submit_allowed") is not True
    assert payload.get("submission_authorized") is not True


def test_market_open_gate_passes_sleeve_truth_root_to_market_data_supply(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    seen: dict[str, object] = {}

    def _run(cmd, **kwargs):  # noqa: ANN001
        seen["cmd"] = cmd
        seen["env"] = kwargs.get("env")
        return SimpleNamespace(returncode=0, stdout='{"status":"SKIPPED","canonical_blocker":""}\n', stderr="")

    monkeypatch.setattr(open_gate.subprocess, "run", _run)

    result = open_gate._run_market_data_supply(ctx)

    cmd = seen["cmd"]
    assert isinstance(cmd, list)
    assert "--truth_root" in cmd
    assert cmd[cmd.index("--truth_root") + 1] == str(ctx.truth_root)
    env = seen["env"]
    assert isinstance(env, dict)
    assert env["C2_TRUTH_ROOT"] == str(ctx.truth_root)
    assert result["exit_code"] == 0


def test_options_snapshot_wrapper_writes_timeout_diagnostic(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    truth_root.mkdir(parents=True)
    cmd = [
        sys.executable,
        "ops/tools/run_options_chain_capture_ib_day_v1.py",
        "--day_utc",
        "2026-05-06",
        "--symbol",
        "IWM",
    ]

    def _timeout(*_args, **_kwargs):  # noqa: ANN001
        raise options_required.subprocess.TimeoutExpired(cmd=cmd, timeout=180, output="", stderr="")

    monkeypatch.setenv("C2_OPTIONS_SNAPSHOT_STEP_TIMEOUT_SECONDS", "180")
    monkeypatch.setattr(options_required.subprocess, "run", _timeout)

    result = options_required._run_tool(cmd, truth_root=truth_root)

    diagnostic_path = Path(str(result["diagnostic_path"]))
    diagnostic = json.loads(diagnostic_path.read_text(encoding="utf-8"))
    assert result["return_code"] == 124
    assert result["timed_out"] is True
    assert diagnostic["reason_code"] == "OPTIONS_CAPTURE_TIMEOUT"
    assert diagnostic["symbol"] == "IWM"
    assert diagnostic["timeout_seconds"] == 180
    assert diagnostic["last_known_stage"] == "SUBPROCESS_TIMEOUT"
    assert "options_chain_snapshot_v1/2026-05-06" in diagnostic["attempted_output_artifacts"]["snapshot_root"]


def test_day_ledger_market_data_phase_consumes_supply_only(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = day_run.PhaseContext("2026-04-29", "PAPER", tmp_path / "truth", tmp_path / "execution", tmp_path / "runtime", tmp_path / "operator", "DU123456")
    monkeypatch.setattr(day_run.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="", stderr=""))
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
                "entitlement_probe_path": "/tmp/probe.json",
                "entitlement_status": "BLOCKED",
                "tested_data_types": [{"market_data_type": 1, "name": "LIVE"}],
                "live_data_available": False,
                "delayed_data_available": True,
                "delayed_data_accepted_by_policy": False,
                "delayed_data_used": False,
                "market_data_mode": "UNKNOWN",
                "policy_source_path": "/tmp/paper_market_data_policy_v1.json",
                "delayed_data_policy": {"policy_decision": "DELAYED_DATA_POLICY_MISSING"},
                "ib_error_codes": [10091],
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
    assert "- market_data_supply_entitlement_probe_path: /tmp/probe.json" in section
    assert "- market_data_supply_ib_error_codes: [10091]" in section
    assert "- market_data_supply_market_data_mode: UNKNOWN" in section
    assert "- market_data_supply_policy_source_path: /tmp/paper_market_data_policy_v1.json" in section
    assert "10091" in section


def test_market_data_supply_includes_entitlement_probe_path_and_tested_data_types(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _requirement(ctx)
    path = _entitlement(ctx, codes=[10089], delayed=True)

    payload = supply.build_market_data_supply(ctx)

    assert payload["entitlement_probe_path"] == str(path)
    assert payload["entitlement_status"] == "BLOCKED"
    assert {row["market_data_type"] for row in payload["tested_data_types"]} == {1, 2, 3, 4}


def test_market_data_supply_writes_only_runtime_artifact(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    payload = supply.build_market_data_supply(ctx)
    path = supply.market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    supply._write_json(path, payload)

    assert path.exists()
    assert str(path).startswith(str(ctx.truth_root))
    assert not str(path).startswith(str(REPO_ROOT))
