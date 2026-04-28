from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from constellation_2.phaseL.ui_api import command_overview_read_model as read_model


DAY = "2026-04-27"


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _source_path(root: Path, sleeve_root: Path, source: Dict[str, str], day: str = DAY) -> Path:
    base = sleeve_root if source["root"] == "sleeve" else root
    return base / source["rel"].format(day=day)


def _payload_for(name: str) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "day_utc": DAY,
        "produced_utc": "2026-04-28T01:00:00Z",
        "status": "PASS",
    }
    if name == "aegis_daily_operator_summary_v1":
        return {
            **base,
            "mode": "DRY_RUN",
            "run_style": "MANUAL",
            "no_silent_day_outcome": "SUCCESS_DRY_RUN",
            "first_blocker": {},
            "submissions_created": True,
            "intents_created": True,
        }
    if name == "aegis_operating_contract_v1":
        return {
            **base,
            "mode": "DRY_RUN",
            "run_style": "MANUAL",
            "required_authorities_by_phase": {"PRE_MARKET": ["market_data_authority_v1"], "CLOSURE": ["trading_day_closure_authority_v1"]},
            "allowed_diagnostic_states": ["WARN"],
            "success_states": ["SUCCESS_DRY_RUN"],
            "fail_states": [],
        }
    if name == "aegis_authority_graph_v1":
        return {
            **base,
            "blocking_nodes": [],
            "authority_nodes": [
                {
                    "authority_name": "paper_trading_day_authority_v1",
                    "phase": "PRE_MARKET",
                    "observed_state": "PASS",
                    "status": "PRESENT",
                    "artifact_path": "/tmp/paper_trading_day_authority.v1.json",
                }
            ],
        }
    if name == "aegis_day_evidence_ledger_v1":
        return {
            **base,
            "final_daily_outcome": "SUCCESS_DRY_RUN",
            "finished_utc": "2026-04-28T01:01:00Z",
            "blockers": [],
            "diagnostics": [],
            "commands": [{"name": "packet", "exit_code": 0, "outputs": ["/tmp/packet.md"]}],
            "artifact_paths": {"summary": "/tmp/summary.json"},
        }
    if name == "paper_trading_day_authority_v1":
        return {**base, "state": "OPEN_READY"}
    if name == "strategy_decision_authority_v1":
        return {**base, "strategy_decision_state": "INTENT_CREATED", "intent_count": 1}
    if name == "market_data_authority_v1":
        return {**base, "market_data_state": "READY", "operator_impact": "", "first_blocker": ""}
    if name == "risk_sizing_authority_v1":
        return {**base, "risk_sizing_state": "SIZED", "intent_count": 1}
    if name == "portfolio_account_authority_v1":
        return {**base, "account_state": "READY"}
    if name == "execution_mode_authority_v1":
        return {**base, "mode_state": "DRY_RUN", "mode": "DRY_RUN", "broker_transmit_enabled": False}
    if name == "execution_lifecycle_authority_v1":
        return {**base, "current_lifecycle_state": "DRY_RUN_COMPLETE", "first_blocker_or_gap": ""}
    if name == "runtime_service_authority_v1":
        return {**base, "service_state": "MANUAL_MODE_READY"}
    if name == "trading_day_closure_authority_v1":
        return {**base, "closure_state": "DRY_RUN_CLOSED", "submission_count": 1}
    return base


def _patch_roots(monkeypatch: Any, tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "truth"
    sleeve_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    monkeypatch.setattr(read_model, "GLOBAL_TRUTH_ROOT", root)
    monkeypatch.setattr(read_model, "SLEEVE_TRUTH_ROOT", sleeve_root)
    return root, sleeve_root


def _write_all_sources(root: Path, sleeve_root: Path) -> None:
    for source in read_model.AUTHORITY_SOURCES:
        _write_json(_source_path(root, sleeve_root, source), _payload_for(source["name"]))


def _write_market_calendar(root: Path, rows: list[tuple[str, bool]]) -> None:
    path = root / "market_calendar_v1" / "NYSE" / "2026.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps({"day_utc": day, "is_trading_session": is_session}) for day, is_session in rows) + "\n",
        encoding="utf-8",
    )


def test_command_overview_uses_real_authorities_over_mock_fixtures(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)

    payload = read_model.build_command_overview_view(DAY)

    assert payload["data_source_state"] == "REAL"
    assert payload["fallback_badge"] == ""
    assert payload["summary"][0]["value"] == "SUCCESS_DRY_RUN"
    assert "All Systems Operational" not in json.dumps(payload)
    assert payload["readiness"]["tiles"][0]["value"] == "OPEN_READY"
    assert "DAILY_OUTCOME_SOURCE_MISMATCH" not in json.dumps(payload)


def test_command_overview_missing_artifacts_are_unavailable_not_fake_healthy(monkeypatch: Any, tmp_path: Path) -> None:
    _patch_roots(monkeypatch, tmp_path)

    payload = read_model.build_command_overview_view(DAY)

    assert payload["data_source_state"] == "UNAVAILABLE"
    assert payload["fallback_badge"] == "MOCK / UNAVAILABLE"
    assert payload["summary"][0]["value"] == "UNAVAILABLE"
    assert "All Systems Operational" not in json.dumps(payload)
    assert all(ref["status"] == "UNAVAILABLE" for ref in payload["source_refs"])


def test_command_overview_mixed_authority_state_is_explicitly_labeled(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    first_source = read_model.AUTHORITY_SOURCES[0]
    _write_json(_source_path(root, sleeve_root, first_source), _payload_for(first_source["name"]))

    payload = read_model.build_command_overview_view(DAY)

    assert payload["data_source_state"] == "MIXED / UNAVAILABLE"
    assert payload["fallback_badge"] == "MIXED / UNAVAILABLE"
    assert payload["context"]["dataSourceState"] == "MIXED / UNAVAILABLE"
    assert any(ref["status"] == "UNAVAILABLE" for ref in payload["source_refs"])


def test_command_overview_shows_success_dry_run_for_proven_day(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)

    payload = read_model.build_command_overview_view(DAY)

    assert payload["day_utc"] == DAY
    assert payload["summary"][0]["value"] == "SUCCESS_DRY_RUN"
    assert any(decision["status"] == "DRY_RUN_CLOSED" for decision in payload["decisions"])


def test_matching_operator_summary_and_ledger_show_no_daily_outcome_mismatch(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)

    payload = read_model.build_command_overview_view(DAY)

    assert [item["title"] for item in payload["exceptions"]] == ["No Active Blockers"]


def test_stale_ledger_vs_fresh_summary_shows_diagnostic_with_paths(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)
    for source in read_model.AUTHORITY_SOURCES:
        if source["name"] == "aegis_day_evidence_ledger_v1":
            payload = _payload_for(source["name"])
            payload["final_daily_outcome"] = "NO_INTENT_EXPECTED"
            payload["finished_utc"] = "2026-04-28T01:00:00Z"
            _write_json(_source_path(root, sleeve_root, source), payload)
        if source["name"] == "aegis_daily_operator_summary_v1":
            payload = _payload_for(source["name"])
            payload["no_silent_day_outcome"] = "SUCCESS_DRY_RUN"
            payload["produced_utc"] = "2026-04-28T01:02:00Z"
            _write_json(_source_path(root, sleeve_root, source), payload)

    payload = read_model.build_command_overview_view(DAY)
    mismatch = next(item for item in payload["exceptions"] if item["title"] == "DAILY_OUTCOME_SOURCE_MISMATCH")

    assert payload["summary"][0]["value"] == "NO_INTENT_EXPECTED"
    assert mismatch["source_a"]["value"] == "NO_INTENT_EXPECTED"
    assert mismatch["source_b"]["value"] == "SUCCESS_DRY_RUN"
    assert "aegis_day_evidence_ledger_v1" in mismatch["source_a"]["path"]
    assert "aegis_daily_operator_summary_v1" in mismatch["source_b"]["path"]
    assert "summary projection is newer" in mismatch["operator_impact"]
    assert mismatch["fix_command"] == f"npm run aegis:paper:daily -- --day_utc {DAY}"


def test_stale_summary_vs_fresh_ledger_shows_diagnostic_with_paths(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)
    for source in read_model.AUTHORITY_SOURCES:
        if source["name"] == "aegis_day_evidence_ledger_v1":
            payload = _payload_for(source["name"])
            payload["final_daily_outcome"] = "SUCCESS_DRY_RUN"
            payload["produced_utc"] = "2026-04-28T01:02:00Z"
            payload["finished_utc"] = "2026-04-28T01:02:00Z"
            _write_json(_source_path(root, sleeve_root, source), payload)
        if source["name"] == "aegis_daily_operator_summary_v1":
            payload = _payload_for(source["name"])
            payload["no_silent_day_outcome"] = "NO_INTENT_EXPECTED"
            payload["produced_utc"] = "2026-04-28T01:00:00Z"
            _write_json(_source_path(root, sleeve_root, source), payload)

    payload = read_model.build_command_overview_view(DAY)
    mismatch = next(item for item in payload["exceptions"] if item["title"] == "DAILY_OUTCOME_SOURCE_MISMATCH")

    assert payload["summary"][0]["value"] == "SUCCESS_DRY_RUN"
    assert mismatch["source_a"]["value"] == "SUCCESS_DRY_RUN"
    assert mismatch["source_b"]["value"] == "NO_INTENT_EXPECTED"
    assert "operator summary projection is stale" in mismatch["operator_impact"]


def test_canonical_final_outcome_is_deterministic(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)
    for source in read_model.AUTHORITY_SOURCES:
        if source["name"] == "aegis_day_evidence_ledger_v1":
            payload = _payload_for(source["name"])
            payload["final_daily_outcome"] = "SUCCESS_DRY_RUN"
            _write_json(_source_path(root, sleeve_root, source), payload)
        if source["name"] == "aegis_daily_operator_summary_v1":
            payload = _payload_for(source["name"])
            payload["no_silent_day_outcome"] = "NO_INTENT_EXPECTED"
            _write_json(_source_path(root, sleeve_root, source), payload)

    first = read_model.build_command_overview_view(DAY)
    second = read_model.build_command_overview_view(DAY)

    assert first["summary"][0]["value"] == "SUCCESS_DRY_RUN"
    assert second["summary"][0]["value"] == "SUCCESS_DRY_RUN"


def test_what_matters_now_only_uses_summary_graph_and_ledger_sources(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)
    for source in read_model.AUTHORITY_SOURCES:
        if source["name"] == "market_data_authority_v1":
            payload = _payload_for(source["name"])
            payload["first_blocker"] = "SHOULD_NOT_SURFACE_DIRECTLY"
            payload["operator_impact"] = "This belongs behind the authority graph or evidence ledger."
            _write_json(_source_path(root, sleeve_root, source), payload)
        if source["name"] == "aegis_day_evidence_ledger_v1":
            payload = _payload_for(source["name"])
            payload["blockers"] = [{"code": "LEDGER_BLOCKER", "reason": "Ledger-owned blocker"}]
            payload["diagnostics"] = [{"code": "LEDGER_DIAGNOSTIC", "reason": "Ledger-owned diagnostic"}]
            _write_json(_source_path(root, sleeve_root, source), payload)

    payload = read_model.build_command_overview_view(DAY)
    rendered = json.dumps(payload)

    assert "LEDGER_BLOCKER" in rendered
    assert "LEDGER_DIAGNOSTIC" in rendered
    assert "SHOULD_NOT_SURFACE_DIRECTLY" not in rendered


def test_next_review_uses_market_calendar_after_end_of_market_day(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)
    _write_market_calendar(
        root,
        [
            ("2026-04-17", True),
            ("2026-04-18", False),
            ("2026-04-19", False),
            ("2026-04-20", True),
        ],
    )
    for source in read_model.AUTHORITY_SOURCES:
        if source["name"] == "aegis_day_evidence_ledger_v1":
            payload = _payload_for(source["name"])
            payload["finished_utc"] = "2026-04-18T03:58:00Z"
            payload["produced_utc"] = "2026-04-18T03:58:00Z"
            _write_json(_source_path(root, sleeve_root, source), payload)
        if source["name"] == "market_data_authority_v1":
            payload = _payload_for(source["name"])
            payload["produced_utc"] = "2026-04-18T03:58:00Z"
            _write_json(_source_path(root, sleeve_root, source), payload)

    payload = read_model.build_command_overview_view(DAY)
    next_review = next(item for item in payload["summary"] if item["label"] == "Next Review")

    assert next_review["raw_value"] == "2026-04-20"
    assert next_review["value"] == "Apr 20"
    assert "America/New_York" in next_review["detail"]
    assert "NYSE" in next_review["detail"]


def test_exception_cards_expose_actionable_handlers(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)
    for source in read_model.AUTHORITY_SOURCES:
        if source["name"] == "aegis_day_evidence_ledger_v1":
            payload = _payload_for(source["name"])
            payload["blockers"] = [{"code": "LEDGER_BLOCKER", "reason": "Ledger-owned blocker"}]
            _write_json(_source_path(root, sleeve_root, source), payload)

    payload = read_model.build_command_overview_view(DAY)
    exception = next(item for item in payload["exceptions"] if item["title"] == "LEDGER_BLOCKER")
    actions = {action["id"]: action for action in exception["actions"]}

    assert exception["primary_action"] in {"view_evidence", "open_workflow"}
    assert {"view_evidence", "acknowledge", "create_decision", "open_workflow"} <= set(actions)
    assert actions["acknowledge"]["kind"] == "local"
    assert actions["create_decision"]["route"] == "/advisory"
    assert actions["open_workflow"]["route"] == "/control"


def test_policy_runtime_counts_do_not_treat_fail_states_as_expired(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)

    payload = read_model.build_command_overview_view(DAY)
    policy = {item["label"]: item for item in payload["policy"]}

    assert policy["Active"]["value"] == 2
    assert policy["Pending"]["value"] == 1
    assert policy["Draft"]["value"] == 1
    assert policy["Expired"]["value"] == 0
    assert payload["policy_runtime"]["expired"] is False
    assert payload["policy_runtime"]["state"] == "MANUAL_MODE_READY"


def test_policy_runtime_expired_state_surfaces_reason_and_remediation(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)
    for source in read_model.AUTHORITY_SOURCES:
        if source["name"] == "runtime_service_authority_v1":
            payload = _payload_for(source["name"])
            payload["service_state"] = "EXPIRED"
            payload["expires_utc"] = "2026-04-28T00:30:00Z"
            _write_json(_source_path(root, sleeve_root, source), payload)

    payload = read_model.build_command_overview_view(DAY)
    policy = {item["label"]: item for item in payload["policy"]}

    assert policy["Expired"]["value"] == 1
    assert policy["Expired"]["tone"] == "error"
    assert payload["policy_runtime"]["expired"] is True
    assert "expired" in payload["policy_runtime"]["reason"].lower()
    assert "Refresh or regenerate" in payload["policy_runtime"]["remediation"]


def test_context_rail_source_refs_include_paths_and_timestamps(monkeypatch: Any, tmp_path: Path) -> None:
    root, sleeve_root = _patch_roots(monkeypatch, tmp_path)
    _write_all_sources(root, sleeve_root)

    payload = read_model.build_command_overview_view(DAY)

    assert "aegis_daily_operator_summary_v1" in payload["context"]["sourceOfTruth"]
    assert "updated 2026-04-28T01:00:00Z" in payload["context"]["sourceOfTruth"]
    assert payload["context"]["sourceRefs"]
    assert all(ref["path"].endswith(".json") for ref in payload["context"]["sourceRefs"])
    assert all(ref["last_update_utc"] for ref in payload["context"]["sourceRefs"])
