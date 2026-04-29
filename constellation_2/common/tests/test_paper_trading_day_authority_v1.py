from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_aegis_paper_preflight_v1 as preflight_module
import ops.tools.run_options_chain_snapshot_required_day_v1 as options_required_module
import ops.tools.run_paper_trading_day_authority_v1 as day_authority_module
import ops.tools.run_submit_boundary_status_v1 as submit_boundary_module


def _configure_day_authority_runtime(monkeypatch, tmp_path: Path):
    truth_root = (tmp_path / "truth").resolve()
    execution_truth_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    execution_truth_root.mkdir(parents=True, exist_ok=True)
    (truth_root / "target_day_build_v1").mkdir(parents=True, exist_ok=True)
    (truth_root / "target_day_admission_v1").mkdir(parents=True, exist_ok=True)
    for fixture_day in ("2026-04-26", "2026-04-27"):
        (truth_root / "reports" / "session_promotion_decision_v1" / fixture_day).mkdir(parents=True, exist_ok=True)
        (truth_root / "target_day_build_v1" / f"{fixture_day}.json").write_text(
            json.dumps(
                {
                    "target_day": fixture_day,
                    "build_status": "COMPLETE",
                    "completeness_result": "COMPLETE",
                    "closure_status": "CLOSED",
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        (truth_root / "target_day_admission_v1" / f"{fixture_day}.json").write_text(
            json.dumps(
                {
                    "target_day": fixture_day,
                    "admission_status": "ADMIT",
                    "binding": True,
                    "blocking_reason_codes": [],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        (
            truth_root
            / "reports"
            / "session_promotion_decision_v1"
            / fixture_day
            / "session_promotion_decision.v1.json"
        ).write_text(
            json.dumps(
                {
                    "target_day": fixture_day,
                    "promotion_state": "PROMOTED",
                    "blocked_reason_codes": [],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    captured: dict[str, object] = {}

    monkeypatch.setattr(day_authority_module, "require_authoritative_repo_runtime_v1", lambda _repo: None)
    monkeypatch.setattr(
        day_authority_module,
        "resolve_decision_truth_root_bridge_v1",
        lambda *_args, **_kwargs: truth_root,
    )
    monkeypatch.setattr(
        day_authority_module,
        "resolve_single_paper_ib_account_from_sleeve_registry",
        lambda _repo: "DU123456",
    )
    monkeypatch.setattr(
        day_authority_module,
        "resolve_sleeve_execution_root_v1",
        lambda **_kwargs: SimpleNamespace(execution_root_path=execution_truth_root),
    )
    monkeypatch.setattr(day_authority_module, "_active_session_open_for_day", lambda _truth, _day: True)
    monkeypatch.setattr(day_authority_module, "repo_git_sha_v1", lambda: "a" * 40)
    monkeypatch.setattr(day_authority_module, "now_utc_iso_v1", lambda: "2026-04-27T14:00:00Z")

    def _fake_atomic_write(*, path: Path, payload: dict, schema_relpath: str, volatile_field_names=()):
        del schema_relpath, volatile_field_names
        captured["path"] = path
        captured["payload"] = dict(payload)
        return SimpleNamespace(path=path, sha256="b" * 64)

    monkeypatch.setattr(day_authority_module, "atomic_write_idempotent_validated_json_v1", _fake_atomic_write)
    return truth_root, execution_truth_root, captured


def _logical_name_from_path(path: Path) -> str:
    text = str(path)
    if "/correlation_envelope_gate_v1/" in text:
        return "correlation_envelope_gate_v1"
    if "/replay_certification_gate_v1/" in text:
        return "replay_certification_gate_v1"
    if "/authorization_gate_verdict_v1/" in text:
        return "authorization_gate_verdict_v1"
    if text.endswith("/global_kill_switch_state.v1.json"):
        return "global_kill_switch_state_v1"
    if "/options_chain_snapshot_v1/" in text:
        return "options_chain_snapshot_v1"
    if "/paper_session_authority_v1/" in text:
        return "paper_session_authority_v1"
    if "/submit_boundary_status_v1/" in text:
        return "submit_boundary_status_v1"
    if "/trading_day_control_plane_v1/" in text:
        return "trading_day_control_plane_v1"
    if "/market_data_authority_v1/" in text:
        return "market_data_authority_v1"
    return ""


def _pass_payloads() -> dict[str, dict]:
    return {
        "correlation_envelope_gate_v1": {"status": "PASS", "reason_codes": []},
        "replay_certification_gate_v1": {"status": "PASS", "reason_codes": []},
        "authorization_gate_verdict_v1": {"status": "PASS", "reason_codes": []},
        "global_kill_switch_state_v1": {"state": "INACTIVE", "allow_entries": True, "reason_codes": []},
        "paper_session_authority_v1": {
            "authority_status": "GRANTED",
            "submission_authorized": True,
            "blocking_reason_codes": [],
        },
        "submit_boundary_status_v1": {
            "boundary_status": "AUTHORIZED",
            "submission_authorized": True,
            "reason_codes": [],
            "blocking_codes": [],
        },
        "trading_day_control_plane_v1": {
            "final_start_decision": "READY_NOW",
            "blocking_codes": [],
        },
    }


def _write_option_intent(execution_truth_root: Path, *, day: str = "2026-04-27", symbol: str = "SPY") -> None:
    _write_path = (
        execution_truth_root
        / "intents_v1"
        / "snapshots"
        / day
        / f"{symbol.lower()}_option_intent.exposure_intent.v1.json"
    )
    _write_path.parent.mkdir(parents=True, exist_ok=True)
    _write_path.write_text(
        json.dumps(
            {
                "schema_id": "exposure_intent",
                "schema_version": "v1",
                "intent_id": f"test_{symbol.lower()}_option_intent",
                "underlying": {"symbol": symbol},
                "option": {"direction": "SELL", "structure": "PUT"},
                "exposure_type": "SHORT_VOL_DEFINED",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_equity_intent(execution_truth_root: Path, *, day: str = "2026-04-27", symbol: str = "SPY") -> None:
    path = (
        execution_truth_root
        / "intents_v1"
        / "snapshots"
        / day
        / f"{symbol.lower()}_equity_intent.exposure_intent.v1.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_id": "exposure_intent",
                "schema_version": "v1",
                "intent_id": f"test_{symbol.lower()}_equity_intent",
                "underlying": {"symbol": symbol},
                "exposure_type": "LONG_EQUITY",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_options_snapshot(execution_truth_root: Path, *, day: str = "2026-04-27", symbol: str = "SPY") -> None:
    capture_dir = execution_truth_root / "options_chain_snapshot_v1" / day / "capture_test"
    capture_dir.mkdir(parents=True, exist_ok=True)
    (capture_dir / "freshness_certificate.v1.json").write_text("{}\n", encoding="utf-8")
    (capture_dir / "options_chain_snapshot.v1.json").write_text(
        json.dumps(
            {
                "schema_id": "options_chain_snapshot",
                "schema_version": "v1",
                "as_of_utc": f"{day}T14:00:00Z",
                "underlying": {
                    "symbol": symbol,
                    "spot_price": "500.00",
                    "spot_as_of_utc": f"{day}T14:00:00Z",
                },
                "contracts": [
                    {
                        "contract_key": f"{symbol}|2026-05-01|PUT|490.00",
                        "expiry_utc": "2026-05-01T00:00:00Z",
                        "strike": "490.00",
                        "right": "PUT",
                        "bid": "1.00",
                        "ask": "1.10",
                        "open_interest": 1,
                        "volume": 1,
                        "ib": {
                            "conId": 1,
                            "localSymbol": f"{symbol} TEST",
                            "tradingClass": symbol,
                            "exchange": "SMART",
                            "currency": "USD",
                            "multiplier": 100,
                        },
                    }
                ],
                "provenance": {
                    "source": "test",
                    "capture_method": "test",
                    "capture_host": "test",
                    "capture_run_id": "capture_test",
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_yesterday_ready_does_not_imply_today_ready(monkeypatch, tmp_path: Path) -> None:
    _truth_root, _execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)

    ready_payloads = _pass_payloads()
    blocked_payloads = _pass_payloads()
    blocked_payloads["correlation_envelope_gate_v1"] = None

    def _fake_read_validated(path: Path, _schema_relpath: str):
        text = str(path)
        logical_name = _logical_name_from_path(path)
        if "/2026-04-26/" in text:
            payload = ready_payloads.get(logical_name)
        elif "/2026-04-27/" in text:
            payload = blocked_payloads.get(logical_name)
        else:
            payload = None
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc_yesterday = day_authority_module.main(["--day_utc", "2026-04-26"])
    yesterday_payload = dict(captured["payload"])
    rc_today = day_authority_module.main(["--day_utc", "2026-04-27"])
    today_payload = dict(captured["payload"])

    assert rc_yesterday == 0
    assert yesterday_payload["state"] == "OPEN_READY"
    assert yesterday_payload["can_submit_paper_orders"] is True

    assert rc_today == 2
    assert today_payload["state"] == "PREFLIGHT_REQUIRED"
    assert today_payload["can_submit_paper_orders"] is False
    assert today_payload["canonical_blocker"] == "CORRELATION_ENVELOPE_GATE_V1_MISSING"


def test_canonical_kill_switch_invalidates_day_authority(monkeypatch, tmp_path: Path) -> None:
    _truth_root, _execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)

    payloads = _pass_payloads()
    payloads["global_kill_switch_state_v1"] = {
        "state": "ACTIVE",
        "allow_entries": False,
        "reason_codes": ["C2_KILL_SWITCH_ACTIVE"],
    }

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["state"] == "INVALIDATED"
    assert payload["canonical_blocker"] == "C2_KILL_SWITCH_ACTIVE"
    assert payload["can_submit_paper_orders"] is False


def test_stale_market_data_before_submit_blocks_open_ready(monkeypatch, tmp_path: Path) -> None:
    _truth_root, _execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)

    payloads = _pass_payloads()
    payloads["market_data_authority_v1"] = {
        "schema_id": "C2_MARKET_DATA_AUTHORITY_V1",
        "schema_version": 1,
        "day_utc": "2026-04-27",
        "produced_utc": "2026-04-27T14:00:00Z",
        "authority_scope": "ACTIVE_INTENT_MARKET_DATA",
        "status": "FAIL",
        "market_data_state": "STALE",
        "phase_context": "PRE_SUBMIT",
        "operator_impact": "PRE_SUBMIT_BLOCKER",
        "active_intent_count": 1,
        "active_options_intent_count": 1,
        "required_symbols": ["SPY"],
        "coverage": [],
        "first_blocker": "OPTIONS_CHAIN_SNAPSHOT_STALE",
        "input_evidence": [],
        "canonical_json_hash": "a" * 64,
    }

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["state"] == "PREFLIGHT_BLOCKED"
    assert payload["can_submit_paper_orders"] is False
    assert payload["canonical_blocker"] == "OPTIONS_CHAIN_SNAPSHOT_STALE"
    assert payload["input_status"]["market_data_authority_v1"]["operator_impact"] == "PRE_SUBMIT_BLOCKER"


def test_successful_same_day_evidence_bundle_produces_open_ready(monkeypatch, tmp_path: Path) -> None:
    _truth_root, _execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)

    payloads = _pass_payloads()

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 0
    assert payload["state"] == "OPEN_READY"
    assert payload["status"] == "READY"
    assert payload["can_paper_trade_today"] is True
    assert payload["can_submit_paper_orders"] is True
    assert payload["canonical_blocker"] == ""


def test_granted_session_without_submit_authorization_reports_root_blocker(monkeypatch, tmp_path: Path) -> None:
    truth_root, _execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    bootstrap_path = (
        truth_root
        / "reports"
        / "paper_session_bootstrap_v1"
        / "2026-04-27"
        / "paper_session_bootstrap.v1.json"
    )
    bootstrap_path.parent.mkdir(parents=True, exist_ok=True)
    bootstrap_path.write_text(
        json.dumps(
            {
                "runtime_prerequisite_verification": {
                    "earliest_failing_prerequisite": {
                        "prerequisite_id": "target_day_admission_v1",
                        "reason_codes": ["PARTIAL_BUILD"],
                    }
                },
                "activation_phase": {"blocker_chain": ["DAY_ACTIVATION_NOT_READY"]},
                "session_bootstrap": {
                    "promotion_gate": {
                        "blocked_reason_codes": ["HIDDEN_DEPENDENCY_DETECTED", "PARTIAL_BUILD"]
                    },
                    "admission": {"reason_codes": ["HIDDEN_DEPENDENCY_DETECTED"]},
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    payloads = _pass_payloads()
    payloads["paper_session_authority_v1"] = {
        "authority_status": "GRANTED",
        "submission_authorized": False,
        "blocking_reason_codes": [],
        "upstream_refs": {"paper_session_bootstrap_v1": str(bootstrap_path)},
    }

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["state"] == "PREFLIGHT_BLOCKED"
    assert payload["can_submit_paper_orders"] is False
    assert payload["canonical_blocker"] == "TARGET_DAY_ADMISSION_NOT_READY"
    session_row = payload["input_status"]["paper_session_authority_v1"]
    assert session_row["status"] == "FAIL"
    assert session_row["reason_codes"] == [
        "TARGET_DAY_ADMISSION_NOT_READY",
        "PARTIAL_BUILD",
        "PAPER_SESSION_SUBMISSION_NOT_AUTHORIZED",
    ]


def test_active_option_intent_missing_options_snapshot_blocks_day_authority(monkeypatch, tmp_path: Path) -> None:
    _truth_root, execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    _write_option_intent(execution_root)
    payloads = _pass_payloads()

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["state"] == "PREFLIGHT_REQUIRED"
    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_ROOT_MISSING"
    options_row = payload["input_status"]["options_chain_snapshot_v1"]
    assert options_row["required_or_diagnostic"] == "required"
    assert options_row["status"] == "MISSING"
    assert options_row["required_symbols"] == ["SPY"]
    assert payload["missing_or_stale_inputs"][0]["logical_name"] == "options_chain_snapshot_v1"


def test_session_denied_precedes_missing_options_snapshot(monkeypatch, tmp_path: Path) -> None:
    _truth_root, execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    _write_option_intent(execution_root)
    payloads = _pass_payloads()
    payloads["paper_session_authority_v1"] = {
        "authority_status": "DENIED",
        "submission_authorized": False,
        "blocking_reason_codes": ["PAPER_CAPITAL_SEED_MISSING"],
    }

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["state"] == "PREFLIGHT_BLOCKED"
    assert payload["canonical_blocker"] == "SESSION_AUTHORITY_DENIED"
    session_row = payload["input_status"]["paper_session_authority_v1"]
    assert session_row["status"] == "FAIL"
    assert session_row["reason_codes"] == ["SESSION_AUTHORITY_DENIED", "PAPER_CAPITAL_SEED_MISSING"]
    options_row = payload["input_status"]["options_chain_snapshot_v1"]
    assert options_row["status"] == "MISSING"
    assert options_row["required_or_diagnostic"] == "diagnostic"
    assert options_row["readiness_role"] == "diagnostic"
    assert "options_chain_snapshot_v1" not in [
        str(row.get("logical_name") or "") for row in payload["missing_or_stale_inputs"]
    ]


def test_active_option_intent_valid_options_snapshot_allows_open_ready(monkeypatch, tmp_path: Path) -> None:
    _truth_root, execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    _write_option_intent(execution_root)
    _write_options_snapshot(execution_root)
    payloads = _pass_payloads()

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 0
    assert payload["state"] == "OPEN_READY"
    options_row = payload["input_status"]["options_chain_snapshot_v1"]
    assert options_row["required_or_diagnostic"] == "required"
    assert options_row["status"] == "PASS"
    assert options_row["covered_symbols"] == ["SPY"]


def test_no_active_option_intent_does_not_require_options_snapshot(monkeypatch, tmp_path: Path) -> None:
    _truth_root, execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    _write_equity_intent(execution_root)
    payloads = _pass_payloads()

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 0
    assert payload["state"] == "OPEN_READY"
    assert payload["input_status"]["options_chain_snapshot_v1"]["required_or_diagnostic"] == "diagnostic"


def test_wrong_day_options_snapshot_does_not_satisfy_current_day(monkeypatch, tmp_path: Path) -> None:
    _truth_root, execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    _write_option_intent(execution_root, day="2026-04-27")
    _write_options_snapshot(execution_root, day="2026-04-26")
    payloads = _pass_payloads()

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_ROOT_MISSING"
    assert payload["input_status"]["options_chain_snapshot_v1"]["required_or_diagnostic"] == "required"
    assert payload["input_status"]["options_chain_snapshot_v1"]["status"] == "MISSING"


def test_options_snapshot_capture_failure_is_not_reported_as_missing_root(monkeypatch, tmp_path: Path, capsys) -> None:
    truth_root = (tmp_path / "execution_truth").resolve()
    truth_root.mkdir(parents=True)

    monkeypatch.setattr(
        options_required_module,
        "_capture_symbol",
        lambda **_kwargs: {
            "cmd": ["capture"],
            "return_code": 2,
            "stdout": "",
            "stderr": "IB_UNAVAILABLE",
        },
    )

    rc = options_required_module.main(
        [
            "--day_utc",
            "2026-04-27",
            "--truth_root",
            str(truth_root),
            "--symbol",
            "SPY",
        ]
    )
    out = json.loads(capsys.readouterr().out)

    assert rc == 2
    assert out["status"] == "BLOCKED_VALID"
    assert out["results"][0]["reason_code"] == "OPTIONS_SNAPSHOT_CAPTURE_FAILED"


def test_options_snapshot_required_defaults_to_current_day_intents(monkeypatch, tmp_path: Path, capsys) -> None:
    truth_root = (tmp_path / "execution_truth").resolve()
    _write_option_intent(truth_root)

    monkeypatch.setattr(
        options_required_module,
        "_capture_symbol",
        lambda **_kwargs: {
            "cmd": ["capture"],
            "return_code": 2,
            "stdout": "",
            "stderr": "IB_UNAVAILABLE",
        },
    )

    rc = options_required_module.main(
        [
            "--day_utc",
            "2026-04-27",
            "--truth_root",
            str(truth_root),
        ]
    )
    out = json.loads(capsys.readouterr().out)

    assert rc == 2
    assert out["required_symbols"] == ["SPY"]
    assert out["results"][0]["reason_code"] == "OPTIONS_SNAPSHOT_CAPTURE_FAILED"


def test_submit_boundary_refuses_authority_with_required_options_snapshot_failure() -> None:
    assert (
        submit_boundary_module._day_authority_has_manifest_required_inputs_v1(
            {
                "input_status": {
                    "correlation_envelope_gate_v1": {
                        "required_or_diagnostic": "required",
                        "readiness_role": "authority_input",
                        "status": "PASS",
                    },
                    "options_chain_snapshot_v1": {
                        "required_or_diagnostic": "required",
                        "readiness_role": "authority_input",
                        "status": "MISSING",
                    },
                }
            }
        )
        is False
    )


def test_target_day_admission_partial_build_blocks_with_explicit_code(monkeypatch, tmp_path: Path) -> None:
    truth_root, _execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    (truth_root / "target_day_build_v1" / "2026-04-27.json").write_text(
        json.dumps(
            {
                "target_day": "2026-04-27",
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (truth_root / "target_day_admission_v1" / "2026-04-27.json").write_text(
        json.dumps(
            {
                "target_day": "2026-04-27",
                "admission_status": "BLOCKED",
                "binding": True,
                "blocking_reason_codes": ["PARTIAL_BUILD"],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    payloads = _pass_payloads()

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["state"] == "PREFLIGHT_BLOCKED"
    assert payload["canonical_blocker"] == "TARGET_DAY_ADMISSION_NOT_READY"
    assert payload["input_status"]["target_day_build_v1"]["required_or_diagnostic"] == "required"
    assert payload["input_status"]["target_day_build_v1"]["status"] == "PASS"
    assert payload["input_status"]["target_day_admission_v1"]["required_or_diagnostic"] == "required"
    assert payload["input_status"]["target_day_admission_v1"]["status"] == "FAIL"
    assert payload["blocker_tree"][0]["logical_name"] == "target_day_admission_v1"


def test_missing_target_day_build_blocks_with_explicit_upstream_code(monkeypatch, tmp_path: Path) -> None:
    truth_root, _execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    (truth_root / "target_day_build_v1" / "2026-04-27.json").unlink()
    payloads = _pass_payloads()

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["state"] == "PREFLIGHT_REQUIRED"
    assert payload["canonical_blocker"] == "TARGET_DAY_BUILD_MISSING"
    assert payload["input_status"]["target_day_build_v1"]["required_or_diagnostic"] == "required"
    assert payload["input_status"]["target_day_build_v1"]["status"] == "MISSING"
    assert payload["blocker_tree"][0]["logical_name"] == "target_day_build_v1"
    assert "PARTIAL_BUILD" not in payload["reason_codes"]


def test_existing_dry_run_complete_closes_day_instead_of_surprise_session_stale(monkeypatch, tmp_path: Path) -> None:
    truth_root, execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    session_path = (
        truth_root
        / "reports"
        / "paper_session_authority_v1"
        / "2026-04-27"
        / "paper_session_authority.v1.json"
    )
    session_path.parent.mkdir(parents=True, exist_ok=True)
    session_path.write_text(json.dumps(_pass_payloads()["paper_session_authority_v1"], sort_keys=True), encoding="utf-8")
    os.utime(session_path, (1_000_000, 1_000_000))
    os.utime(truth_root / "target_day_build_v1" / "2026-04-27.json", (1_000_010, 1_000_010))
    submission_dir = execution_root / "execution_evidence_v1" / "submissions" / "2026-04-27" / ("d" * 64)
    submission_dir.mkdir(parents=True, exist_ok=True)
    (submission_dir / "broker_submit_attempt_v1.json").write_text(
        json.dumps({"submission_id": "d" * 64, "dry_run": True}, sort_keys=True),
        encoding="utf-8",
    )
    (submission_dir / "broker_submission_record.v2.json").write_text(
        json.dumps(
            {
                "submission_id": "d" * 64,
                "error": {"code": "DRY_RUN_NO_BROKER_ID"},
                "broker_ids": {"order_id": None, "perm_id": None},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    payloads = _pass_payloads()
    payloads["submit_boundary_status_v1"] = {
        "boundary_status": "DRY_RUN_COMPLETE",
        "submission_authorized": False,
        "reason_codes": [],
        "blocking_codes": [],
    }

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["state"] == "CLOSED"
    assert payload["submit_mode_status"] == "DRY_RUN_COMPLETE"
    assert payload["can_submit_paper_orders"] is False
    assert payload["canonical_blocker"] == "DRY_RUN_COMPLETE_ALREADY_SUBMITTED"
    assert payload["blocker_tree"][0]["code"] == "DRY_RUN_COMPLETE_ALREADY_SUBMITTED"
    assert payload["input_status"]["target_day_build_v1"]["status"] == "PASS"
    assert payload["input_status"]["target_day_admission_v1"]["status"] == "PASS"
    assert payload["input_status"]["session_promotion_decision_v1"]["status"] == "PASS"
    assert payload["input_status"]["paper_session_authority_v1"]["status"] == "STALE"
    assert "PARTIAL_BUILD" not in payload["reason_codes"]
    assert payload["canonical_blocker"] != "SESSION_AUTHORITY_STALE"


def test_preflight_reset_moves_only_dry_run_submission_evidence(tmp_path: Path) -> None:
    execution_root = tmp_path / "execution"
    dry_run_dir = execution_root / "execution_evidence_v1" / "submissions" / "2026-04-27" / ("d" * 64)
    live_dir = execution_root / "execution_evidence_v1" / "submissions" / "2026-04-27" / ("e" * 64)
    dry_run_kernel_dir = execution_root / "execution_kernel_v1" / "submission_records" / "2026-04-27" / ("d" * 64)
    dry_run_fill = execution_root / "fill_ledger_v1" / "2026-04-27" / f"{'d' * 64}.fill_ledger.v1.json"
    dry_run_manifest = execution_root / "execution_evidence_v1" / "manifests" / "2026-04-27" / f"{'d' * 64}.manifest.json"
    live_kernel_dir = execution_root / "execution_kernel_v1" / "submission_records" / "2026-04-27" / ("e" * 64)
    dry_run_dir.mkdir(parents=True, exist_ok=True)
    live_dir.mkdir(parents=True, exist_ok=True)
    dry_run_kernel_dir.mkdir(parents=True, exist_ok=True)
    live_kernel_dir.mkdir(parents=True, exist_ok=True)
    dry_run_fill.parent.mkdir(parents=True, exist_ok=True)
    dry_run_manifest.parent.mkdir(parents=True, exist_ok=True)
    (dry_run_kernel_dir / "submission_record.v1.json").write_text(
        json.dumps({"submission_id": "d" * 64, "status": "READY_TO_SUBMIT"}, sort_keys=True),
        encoding="utf-8",
    )
    (dry_run_fill).write_text(json.dumps({"submission_id": "d" * 64}, sort_keys=True), encoding="utf-8")
    (dry_run_manifest).write_text(json.dumps({"submission_id": "d" * 64}, sort_keys=True), encoding="utf-8")
    (live_kernel_dir / "submission_record.v1.json").write_text(
        json.dumps({"submission_id": "e" * 64, "status": "READY_TO_SUBMIT"}, sort_keys=True),
        encoding="utf-8",
    )
    (dry_run_dir / "broker_submit_attempt_v1.json").write_text(
        json.dumps({"submission_id": "d" * 64, "dry_run": True}, sort_keys=True),
        encoding="utf-8",
    )
    (dry_run_dir / "broker_submission_record.v2.json").write_text(
        json.dumps(
            {
                "submission_id": "d" * 64,
                "error": {"code": "DRY_RUN_NO_BROKER_ID"},
                "broker_ids": {"order_id": None, "perm_id": None},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (live_dir / "broker_submission_record.v2.json").write_text(
        json.dumps(
            {
                "submission_id": "e" * 64,
                "broker_ids": {"order_id": 123, "perm_id": 456},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    result = preflight_module.reset_dry_run_submission_evidence_v1(
        execution_truth_root=execution_root,
        day_utc="2026-04-27",
    )

    assert result["status"] == "PASS"
    assert result["mode"] == "PAPER"
    assert result["moved_count"] == 1
    assert result["sidecar_moved_count"] == 3
    assert result["skipped_count"] == 1
    assert not dry_run_dir.exists()
    assert not dry_run_kernel_dir.exists()
    assert not dry_run_fill.exists()
    assert not dry_run_manifest.exists()
    assert live_dir.exists()
    assert live_kernel_dir.exists()
    moved_to = Path(result["moved"][0]["to"])
    assert moved_to.exists()
    assert "superseded_dry_run_submissions_v1" in str(moved_to)
    sidecar_paths = [Path(row["to"]) for row in result["sidecars_moved"]]
    assert any("superseded_dry_run_submission_records_v1" in str(path) for path in sidecar_paths)
    assert any("superseded_dry_run_fill_ledger_v1" in str(path) for path in sidecar_paths)
    assert any("superseded_dry_run_submission_manifests_v1" in str(path) for path in sidecar_paths)


def test_preflight_reset_does_not_move_live_or_nonpaper_evidence(tmp_path: Path) -> None:
    execution_root = tmp_path / "execution"
    live_dir = execution_root / "execution_evidence_v1" / "submissions" / "2026-04-27" / ("e" * 64)
    live_dir.mkdir(parents=True, exist_ok=True)
    (live_dir / "broker_submit_attempt_v1.json").write_text(
        json.dumps({"submission_id": "e" * 64, "dry_run": True, "environment": "LIVE"}, sort_keys=True),
        encoding="utf-8",
    )
    (live_dir / "broker_submission_record.v2.json").write_text(
        json.dumps(
            {
                "submission_id": "e" * 64,
                "broker": {"environment": "LIVE", "name": "INTERACTIVE_BROKERS"},
                "error": {"code": "DRY_RUN_NO_BROKER_ID"},
                "broker_ids": {"order_id": None, "perm_id": None},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    result = preflight_module.reset_dry_run_submission_evidence_v1(
        execution_truth_root=execution_root,
        day_utc="2026-04-27",
    )

    assert result["moved_count"] == 0
    assert result["sidecar_moved_count"] == 0
    assert result["skipped_count"] == 1
    assert live_dir.exists()


def test_reset_dry_run_submission_evidence_allows_open_ready_again(monkeypatch, tmp_path: Path) -> None:
    _truth_root, execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    submission_dir = execution_root / "execution_evidence_v1" / "submissions" / "2026-04-27" / ("d" * 64)
    submission_dir.mkdir(parents=True, exist_ok=True)
    (submission_dir / "broker_submit_attempt_v1.json").write_text(
        json.dumps({"submission_id": "d" * 64, "dry_run": True}, sort_keys=True),
        encoding="utf-8",
    )
    (submission_dir / "broker_submission_record.v2.json").write_text(
        json.dumps(
            {
                "submission_id": "d" * 64,
                "error": {"code": "DRY_RUN_NO_BROKER_ID"},
                "broker_ids": {"order_id": None, "perm_id": None},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    payloads = _pass_payloads()

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc_before = day_authority_module.main(["--day_utc", "2026-04-27"])
    before = dict(captured["payload"])
    reset = preflight_module.reset_dry_run_submission_evidence_v1(
        execution_truth_root=execution_root,
        day_utc="2026-04-27",
    )
    rc_after = day_authority_module.main(["--day_utc", "2026-04-27"])
    after = dict(captured["payload"])

    assert rc_before == 2
    assert before["state"] == "CLOSED"
    assert before["canonical_blocker"] == "DRY_RUN_COMPLETE_ALREADY_SUBMITTED"
    assert reset["moved_count"] == 1
    assert rc_after == 0
    assert after["state"] == "OPEN_READY"
    assert after["can_submit_paper_orders"] is True
    assert after["canonical_blocker"] == ""
    assert after["submit_mode_status"] == "NO_SUBMIT_ATTEMPT"


def test_session_promotion_failure_blocks_with_explicit_code(monkeypatch, tmp_path: Path) -> None:
    truth_root, _execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    (
        truth_root
        / "reports"
        / "session_promotion_decision_v1"
        / "2026-04-27"
        / "session_promotion_decision.v1.json"
    ).write_text(
        json.dumps(
            {
                "target_day": "2026-04-27",
                "promotion_state": "BLOCKED",
                "blocked_reason_codes": ["PARTIAL_BUILD"],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    payloads = _pass_payloads()

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["state"] == "PREFLIGHT_BLOCKED"
    assert payload["canonical_blocker"] == "SESSION_PROMOTION_DECISION_NOT_READY"
    assert payload["input_status"]["session_promotion_decision_v1"]["required_or_diagnostic"] == "required"
    assert payload["input_status"]["session_promotion_decision_v1"]["status"] == "FAIL"


def test_stale_required_authorization_verdict_blocks_with_manifest_metadata(monkeypatch, tmp_path: Path) -> None:
    _truth_root, _execution_root, captured = _configure_day_authority_runtime(monkeypatch, tmp_path)
    payloads = _pass_payloads()

    def _fake_read_validated(path: Path, _schema_relpath: str):
        logical_name = _logical_name_from_path(path)
        payload = payloads.get(logical_name)
        if payload is None:
            return None, "MISSING"
        return dict(payload), ""

    monkeypatch.setattr(day_authority_module, "_read_validated", _fake_read_validated)
    monkeypatch.setattr(
        day_authority_module,
        "_dependency_newer",
        lambda path, _deps: "/tmp/correlation_envelope_gate.v1.json"
        if "authorization_gate_verdict_v1" in str(path)
        else "",
    )

    rc = day_authority_module.main(["--day_utc", "2026-04-27"])
    payload = dict(captured["payload"])

    assert rc == 2
    assert payload["state"] == "PREFLIGHT_REQUIRED"
    assert payload["canonical_blocker"] == "AUTHORIZATION_GATE_VERDICT_STALE"
    stale = payload["missing_or_stale_inputs"][0]
    assert stale["logical_name"] == "authorization_gate_verdict_v1"
    assert stale["missing_or_stale"] == "STALE"
    assert stale["required_producer_command"]
    assert stale["operator_actionable"] is True
    assert stale["causal_parent"] == "/tmp/correlation_envelope_gate.v1.json"


def _configure_preflight_runtime(monkeypatch, tmp_path: Path):
    truth_root = (tmp_path / "truth").resolve()
    execution_truth_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    execution_truth_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(preflight_module, "require_authoritative_repo_runtime_v1", lambda _repo: None)
    monkeypatch.setattr(
        preflight_module,
        "resolve_decision_truth_root_bridge_v1",
        lambda *_args, **_kwargs: truth_root,
    )
    monkeypatch.setattr(
        preflight_module,
        "resolve_single_paper_ib_account_from_sleeve_registry",
        lambda _repo: "DU123456",
    )
    monkeypatch.setattr(
        preflight_module,
        "resolve_sleeve_execution_root_v1",
        lambda **_kwargs: SimpleNamespace(execution_root_path=execution_truth_root),
    )
    monkeypatch.setattr(
        preflight_module,
        "_run_step",
        lambda name, cmd: {
            "name": name,
            "cmd": cmd,
            "return_code": 0,
            "stdout": "",
            "stderr": "",
        },
    )
    monkeypatch.setattr(
        preflight_module,
        "write_gate_authority_plane",
        lambda **_kwargs: {
            "authorization_verdict": SimpleNamespace(
                action="REFRESHED",
                path=execution_truth_root / "reports" / "authorization_gate_verdict_v1" / "2026-04-27" / "authorization_gate_verdict.v1.json",
                sha256="f" * 64,
            )
        },
    )
    return truth_root


def _write_authority_payload(truth_root: Path, payload: dict) -> Path:
    path = (
        truth_root
        / "reports"
        / "paper_trading_day_authority_v1"
        / "2026-04-27"
        / "paper_trading_day_authority.v1.json"
    ).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def test_preflight_blocks_early_with_missing_correlation_evidence(monkeypatch, tmp_path: Path, capsys) -> None:
    truth_root = _configure_preflight_runtime(monkeypatch, tmp_path)
    authority_path = _write_authority_payload(
        truth_root,
        {
            "state": "PREFLIGHT_BLOCKED",
            "canonical_blocker": "CORRELATION_ENVELOPE_GATE_V1_MISSING",
            "missing_or_stale_inputs": [
                {
                    "path": "/tmp/correlation_envelope_gate.v1.json",
                    "owning_gate": "correlation_envelope_gate_v1",
                    "required_producer_command": "python3 ops/tools/run_correlation_envelope_gate_v1.py --day_utc 2026-04-27",
                    "operator_actionable": True,
                }
            ],
            "blocker_tree": [],
        },
    )

    rc = preflight_module.main(["--day_utc", "2026-04-27", "--truth_root", str(truth_root)])
    out = json.loads(capsys.readouterr().out.strip())

    assert rc == 2
    assert out["status"] == "PREFLIGHT_BLOCKED"
    assert out["canonical_blocker"] == "CORRELATION_ENVELOPE_GATE_V1_MISSING"
    assert out["missing_artifact_path"] == "/tmp/correlation_envelope_gate.v1.json"
    assert out["owning_gate"] == "correlation_envelope_gate_v1"
    assert out["required_producer_command"].startswith("python3 ops/tools/run_correlation_envelope_gate_v1.py")
    assert out["operator_actionable"] is True
    assert out["authority_path"] == str(authority_path)
    assert all(step["name"] != "paper_session_bootstrap_v1" for step in out["steps"])


def test_preflight_blocks_early_with_missing_replay_certification(monkeypatch, tmp_path: Path, capsys) -> None:
    truth_root = _configure_preflight_runtime(monkeypatch, tmp_path)
    authority_path = _write_authority_payload(
        truth_root,
        {
            "state": "PREFLIGHT_BLOCKED",
            "canonical_blocker": "REPLAY_CERTIFICATION_GATE_V1_MISSING",
            "missing_or_stale_inputs": [
                {
                    "path": "/tmp/replay_certification_gate.v1.json",
                    "owning_gate": "replay_certification_gate_v1",
                    "required_producer_command": "python3 ops/tools/run_replay_certification_gate_v1.py --day_utc 2026-04-27",
                    "operator_actionable": True,
                }
            ],
            "blocker_tree": [],
        },
    )

    rc = preflight_module.main(["--day_utc", "2026-04-27", "--truth_root", str(truth_root)])
    out = json.loads(capsys.readouterr().out.strip())

    assert rc == 2
    assert out["status"] == "PREFLIGHT_BLOCKED"
    assert out["canonical_blocker"] == "REPLAY_CERTIFICATION_GATE_V1_MISSING"
    assert out["missing_artifact_path"] == "/tmp/replay_certification_gate.v1.json"
    assert out["owning_gate"] == "replay_certification_gate_v1"
    assert out["required_producer_command"].startswith("python3 ops/tools/run_replay_certification_gate_v1.py")
    assert out["operator_actionable"] is True
    assert out["authority_path"] == str(authority_path)
    assert all(step["name"] != "paper_session_bootstrap_v1" for step in out["steps"])


def test_preflight_recomputes_downstream_when_only_session_authority_is_missing(monkeypatch, tmp_path: Path, capsys) -> None:
    truth_root = _configure_preflight_runtime(monkeypatch, tmp_path)
    authority_path = (
        truth_root
        / "reports"
        / "paper_trading_day_authority_v1"
        / "2026-04-27"
        / "paper_trading_day_authority.v1.json"
    ).resolve()
    authority_path.parent.mkdir(parents=True, exist_ok=True)

    def _write_precheck_session_missing() -> None:
        authority_path.write_text(
            json.dumps(
                {
                    "state": "PREFLIGHT_REQUIRED",
                    "canonical_blocker": "SESSION_AUTHORITY_MISSING",
                    "missing_or_stale_inputs": [
                        {
                            "logical_name": "paper_session_authority_v1",
                            "path": "/tmp/paper_session_authority.v1.json",
                            "owning_gate": "paper_session_authority_v1",
                            "required_producer_command": "python3 ops/tools/run_paper_session_bootstrap_v1.py --day_utc 2026-04-27",
                            "operator_actionable": True,
                        }
                    ],
                    "blocker_tree": [
                        {
                            "code": "SESSION_AUTHORITY_MISSING",
                            "source_path": "/tmp/paper_session_authority.v1.json",
                            "owning_gate": "paper_session_authority_v1",
                            "required_producer_command": "python3 ops/tools/run_paper_session_bootstrap_v1.py --day_utc 2026-04-27",
                            "operator_actionable": True,
                        }
                    ],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def _write_final_open_ready() -> None:
        authority_path.write_text(
            json.dumps(
                {
                    "state": "OPEN_READY",
                    "canonical_blocker": "",
                    "missing_or_stale_inputs": [],
                    "blocker_tree": [],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def _run_step(name: str, cmd: list[str]) -> dict:
        if name == "paper_trading_day_authority_v1.precheck":
            _write_precheck_session_missing()
            return {"name": name, "cmd": cmd, "return_code": 2, "stdout": "", "stderr": ""}
        if name == "paper_trading_day_authority_v1.final":
            _write_final_open_ready()
            return {"name": name, "cmd": cmd, "return_code": 0, "stdout": "", "stderr": ""}
        return {"name": name, "cmd": cmd, "return_code": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(preflight_module, "_run_step", _run_step)

    rc = preflight_module.main(["--day_utc", "2026-04-27", "--truth_root", str(truth_root)])
    out = json.loads(capsys.readouterr().out.strip())

    assert rc == 0
    assert out["status"] == "OK"
    assert out["authority_state"] == "OPEN_READY"
    assert out["canonical_blocker"] == ""
    step_names = [step["name"] for step in out["steps"]]
    expected_order = [
        "trading_day_intent_generation_v1",
        "correlation_envelope_gate_v1",
        "replay_certification_gate_v1",
        "gate_stack_verdict_v1",
        "authorization_gate_verdict_v1",
        "global_kill_switch_v1",
        "options_chain_snapshot_v1",
        "session_readiness_refresh_v1",
        "session_authority_v1.refresh_target_day",
        "paper_trading_day_authority_v1.precheck",
        "paper_session_bootstrap_v1",
        "submit_boundary_status_v1",
        "trading_day_control_plane_v1",
        "c2_daily_operator_gate_v1",
        "paper_trading_day_authority_v1.final",
    ]
    indices = [step_names.index(name) for name in expected_order]
    assert indices == sorted(indices)
