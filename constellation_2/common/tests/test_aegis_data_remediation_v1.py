from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.data_remediation_v1 import (
    PLAYBOOK_REFRESH_REQUIRED_SYMBOL,
    VIX_ALIAS_ORDER,
    alias_order_for_symbol_v1,
    blocker_registry_v1,
    classify_data_blockers_v1,
    latest_remediation_v1,
    provider_attempt_order_v1,
    read_remediation_events_v1,
    remediation_playbook_registry_v1,
    run_data_remediation_v1,
    validate_remediated_data_v1,
)
from ops.aegis.operator_command_v1 import system_diagnostic_projection_v1

DAY = "2026-05-18"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    _write_json(repo / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json", {"sleeves": []})
    _write_json(
        repo / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json",
        {
            "engines": [
                {
                    "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                    "activation_status": "ACTIVE",
                    "engine_runner_path": "vol.py",
                    "allowed_symbols": ["IWM"],
                }
            ]
        },
    )
    _write_json(
        repo / "ops/config/aegis_runtime_universe.json",
        {"runtime_universe_mode": "sleeve_required_only", "production_scan_dataset_id": ""},
    )
    return repo


def _market_report(root: Path, *, status: str = "MISSING", observed_session: str = "") -> None:
    symbols = {}
    if observed_session:
        symbols["VIX"] = {
            "symbol": "VIX",
            "canonical_symbol": "VIX",
            "provider": "LOCAL_CACHE",
            "provider_symbol": "VIX",
            "last_price": 18.0,
            "close": 18.0,
            "market_session_date": observed_session,
            "data_timestamp_utc": f"{observed_session}T15:55:00Z",
            "source_hash": "stale-hash",
            "freshness_status": "STALE",
        }
    _write_json(
        root / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json",
        {
            "day_utc": DAY,
            "status": "STALE" if status == "STALE" else "PARTIAL",
            "provider_config": {"configured": True, "primary": "LOCAL_CACHE", "fallback": ""},
            "requested_symbols": ["VIX"],
            "symbols": symbols,
            "missing_symbols": ["VIX"] if status == "MISSING" else [],
            "stale_symbols": ["VIX"] if status == "STALE" else [],
            "fetched_symbols": ["VIX"] if status == "STALE" else [],
            "mapping_missing_symbols": [],
            "provider_failed_symbols": [],
            "usable_for_candidate_generation": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        },
    )


def _readiness(root: Path) -> None:
    _write_json(
        root / "reports/aegis_sleeve_readiness_v1" / DAY / "sleeve_readiness.v1.json",
        {
            "day_utc": DAY,
            "ready_count": 0,
            "ready_with_warnings_count": 0,
            "blocked_count": 1,
            "sleeves": [
                {
                    "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                    "readiness": "BLOCKED",
                    "blocking_inputs": ["market.volatility.VIX"],
                }
            ],
        },
    )


def _config(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "market_data.env"
    config.write_text("AEGIS_MARKET_DATA_PROVIDER_PRIMARY=LOCAL_CACHE\nAEGIS_MARKET_DATA_REQUIRE_CURRENT_SESSION=true\n", encoding="utf-8")
    monkeypatch.setenv("AEGIS_MARKET_DATA_CONFIG", str(config))
    monkeypatch.setenv("AEGIS_RUNTIME_UNIVERSE_MODE", "sleeve_required_only")


def _cache_vix(root: Path, *, session: str) -> None:
    path = root / "market_data_snapshot_v1" / "VIX" / f"{DAY[:4]}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"symbol": "VIX", "timestamp_utc": f"{session}T15:55:00Z", "open": 18.0, "high": 18.5, "low": 17.8, "close": 18.2, "volume": 0}) + "\n", encoding="utf-8")


def test_vix_stale_blocker_detected(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _market_report(root, status="STALE", observed_session="2026-05-17")
    _readiness(root)

    payload = classify_data_blockers_v1(truth_root=root, repo_root=repo, day_utc=DAY)

    blocker = payload["blockers"][0]
    assert blocker["blocker_type"] == "stale_required_symbol"
    assert blocker["affected_symbol"] == "VIX"
    assert blocker["affected_sleeve_id"] == "C2_VOL_INCOME_DEFINED_RISK_V1"
    assert blocker["remediation_playbook_id"] == PLAYBOOK_REFRESH_REQUIRED_SYMBOL
    assert "stale_required_symbol" in blocker_registry_v1()["blocker_types"]


def test_vix_remediation_alias_order_and_playbook_registry(tmp_path: Path, monkeypatch) -> None:
    _config(tmp_path, monkeypatch)
    assert alias_order_for_symbol_v1("VIX", {}, "LOCAL_CACHE") == VIX_ALIAS_ORDER
    assert provider_attempt_order_v1() == ["LOCAL_CACHE"]
    registry = remediation_playbook_registry_v1()
    assert PLAYBOOK_REFRESH_REQUIRED_SYMBOL in {row["playbook_id"] for row in registry["playbooks"]}
    assert registry["ai_generated_playbooks_allowed"] is False


def test_stale_vix_rejected_and_blocker_remains(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _config(tmp_path, monkeypatch)
    _market_report(root, status="MISSING")
    _readiness(root)
    _cache_vix(root, session="2026-05-17")

    payload = run_data_remediation_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    events = read_remediation_events_v1(truth_root=root, day_utc=DAY)

    assert any(row["event_type"] == "DATA_VALIDATION_FAILED" for row in events)
    assert any(row["event_type"] == "REMEDIATION_STILL_BLOCKED" for row in events)
    assert payload["governance"]["fake_data_allowed"] is False
    assert payload["governance"]["stale_override_allowed"] is False


def test_current_session_valid_vix_accepted_and_ledger_append_only(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "truth"
    repo = _repo(tmp_path)
    _config(tmp_path, monkeypatch)
    _market_report(root, status="MISSING")
    _readiness(root)
    _cache_vix(root, session=DAY)

    first = run_data_remediation_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    first_events = read_remediation_events_v1(truth_root=root, day_utc=DAY)
    second = run_data_remediation_v1(truth_root=root, repo_root=repo, day_utc=DAY)
    second_events = read_remediation_events_v1(truth_root=root, day_utc=DAY)

    assert any(row["event_type"] == "DATA_VALIDATION_PASSED" for row in first_events)
    assert any(row["event_type"] == "REMEDIATION_HEALED_BLOCKER" for row in first_events)
    assert (root / "market_data_snapshot_v1" / "VIX" / f"{DAY[:4]}.jsonl").exists()
    vix_blocker_id = next(row["blocker_id"] for row in first_events if row["event_type"] == "REMEDIATION_HEALED_BLOCKER")
    first_vix_events = [row for row in first_events if row.get("blocker_id") == vix_blocker_id]
    second_vix_events = [row for row in second_events if row.get("blocker_id") == vix_blocker_id]
    assert len(second_vix_events) == len(first_vix_events)
    assert not any(row.get("blocker_id") == vix_blocker_id and row.get("status") != "REUSED_PRIOR_RESULT" for row in second["attempts"])
    assert latest_remediation_v1(truth_root=root, day_utc=DAY)["attempt_count"] >= 1


def test_validation_gate_rejects_schema_and_session_failures() -> None:
    stale = validate_remediated_data_v1(symbol="VIX", provider="LOCAL_CACHE", row={"canonical_symbol": "VIX", "market_session_date": "2026-05-17", "freshness_status": "STALE", "close": 18.0, "source_hash": "x"}, expected_session=DAY)
    bad_provider = validate_remediated_data_v1(symbol="VIX", provider="UNAPPROVED", row={"canonical_symbol": "VIX", "market_session_date": DAY, "freshness_status": "CURRENT", "close": 18.0, "source_hash": "x"}, expected_session=DAY)

    assert stale["status"] == "FAIL"
    assert "SESSION_MISMATCH" in stale["errors"]
    assert "FRESHNESS_NOT_CURRENT" in stale["errors"]
    assert bad_provider["status"] == "FAIL"
    assert "PROVIDER_NOT_APPROVED" in bad_provider["errors"]


def test_system_diagnostics_reflect_remediation_state() -> None:
    cockpit = {
        "runtime": {"runtime_truth_classification": "REAL_RUNTIME", "highest_readiness_layer": "ADVISORY_ONLY"},
        "opportunities": {
            "market_data_summary": {"missing_symbols": ["VIX"], "stale_symbols": []},
            "sleeve_run_summary": [{"sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "run_status": "BLOCKED", "blocking_inputs": ["VIX"]}],
            "data_remediation": {
                "blockers": [{"blocker_id": "b1", "affected_symbol": "VIX"}],
                "events": [
                    {"blocker_id": "b1", "event_type": "DATA_VALIDATION_FAILED", "playbook_id": PLAYBOOK_REFRESH_REQUIRED_SYMBOL, "provider": "LOCAL_CACHE", "validation_result": "FAIL"},
                    {"blocker_id": "b1", "event_type": "REMEDIATION_STILL_BLOCKED", "playbook_id": PLAYBOOK_REFRESH_REQUIRED_SYMBOL, "provider": "LOCAL_CACHE", "validation_result": "STILL_BLOCKED"},
                ],
                "attempts": [],
            },
        },
        "top_candidates": [],
        "candidate_decisions_corrections": {},
        "source_paths": {},
    }

    diagnostics = system_diagnostic_projection_v1(cockpit)["diagnostics"]
    vix = next(row for row in diagnostics if row["diagnostic_type"] == "missing_market_data")

    assert vix["remediation_attempted"] is True
    assert vix["remediation_playbook_id"] == PLAYBOOK_REFRESH_REQUIRED_SYMBOL
    assert vix["remediation_providers_tried"] == ["LOCAL_CACHE"]
    assert vix["remediation_still_blocked"] is True
    assert vix["remediation_message"] == "Still blocked after approved remediation. No fake or stale data was accepted."


def test_no_fake_or_broker_paths_exist_in_remediation_source() -> None:
    source = (ROOT / "ops/aegis/data_remediation_v1.py").read_text(encoding="utf-8")
    forbidden = ["submit_order", "route_order", "live_trading_allowed\": True", "synthetic_values_allowed\": True", "fake_data_allowed\": True", "stale_override_allowed\": True"]
    assert all(item not in source for item in forbidden)
