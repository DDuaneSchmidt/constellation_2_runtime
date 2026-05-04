from __future__ import annotations

import json
from pathlib import Path

from constellation_2.aegis_truth.evidence_event_v1 import build_event
from constellation_2.aegis_truth.evidence_ledger_v1 import append_event, ledger_path, read_events
from constellation_2.aegis_truth.protected_mutation_v1 import record_mutation_check, wrong_repo_guard
from constellation_2.aegis_truth.truth_resolver_v1 import resolve_truth_state
from ops.tools.run_aegis_alert_sink_v1 import build_alerts
from ops.tools.run_portal_availability_probe_v1 import probe


class Args:
    target_day = "2026-05-04"
    truth_root = ""
    environment = "PAPER"
    portal_url = "https://portal.schmidtvault.com/healthz"
    origin_health_url = ""
    timeout_seconds = 1.0
    projection_max_age_seconds = 60
    simulate_status = 502


def _event(event_type: str, status: str = "OK", owner: str = "test", severity: str = "INFO", blocker: str | None = None) -> dict:
    return build_event(event_type=event_type, producer="test", target_day="2026-05-04", environment="PAPER", status=status, blocker=blocker, owner=owner, severity=severity, payload={"event_type": event_type, "status": status}, next_action="test action")


def _write_required_inputs(root: Path, day: str = "2026-05-04") -> None:
    for rel in [
        "reports/truth_day_run_ledger_v1/{day}/x.json",
        "reports/aegis_requirement_graph_v1/{day}/x.json",
        "reports/broker_supply_v1/{day}/x.json",
        "reports/market_data_supply_v1/{day}/x.json",
        "reports/capital_supply_v1/{day}/x.json",
        "reports/risk_budget_supply_v1/{day}/x.json",
        "reports/authorization_supply_v1/{day}/x.json",
        "reports/submit_boundary_status_v1/{day}/x.json",
    ]:
        path = root / rel.format(day=day)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"status":"OK"}\n', encoding="utf-8")


def test_portal_502_simulation_writes_critical_event_alert_and_resolver_degrades(tmp_path: Path) -> None:
    args = Args()
    args.truth_root = str(tmp_path)
    event = probe(args)
    append_event(event, truth_root=tmp_path)
    _write_required_inputs(tmp_path)
    state = resolve_truth_state(target_day=args.target_day, truth_root=tmp_path, environment="PAPER")
    alerts = build_alerts(target_day=args.target_day, truth_root=tmp_path)
    assert event["event_type"] == "PORTAL_ORIGIN_UNREACHABLE"
    assert event["severity"] == "CRITICAL"
    assert state["final_status"] == "DEGRADED"
    assert alerts and alerts[0]["status"] == "OPEN"


def test_day_run_blocked_keeps_primary_and_portal_is_secondary(tmp_path: Path) -> None:
    _write_required_inputs(tmp_path)
    append_event(_event("TRADING_SAFETY_BLOCKED", "BLOCKED", "trading_safety", "ERROR", "trading safety blocked"), truth_root=tmp_path)
    append_event(_event("PORTAL_ORIGIN_UNREACHABLE", "BLOCKED", "portal", "CRITICAL", "portal 502"), truth_root=tmp_path)
    state = resolve_truth_state(target_day="2026-05-04", truth_root=tmp_path, environment="PAPER")
    assert state["final_status"] == "BLOCKED"
    assert state["owner"] == "trading_safety"
    assert state["secondary_conditions"]


def test_stale_projection_event_alerts_after_threshold(tmp_path: Path) -> None:
    event = _event("PORTAL_PROJECTION_STALE", "BLOCKED", "portal", "CRITICAL", "portal projection stale")
    append_event(event, truth_root=tmp_path)
    alerts = build_alerts(target_day="2026-05-04", truth_root=tmp_path)
    assert alerts[0]["severity"] == "CRITICAL"


def test_later_projection_refresh_clears_stale_projection_condition(tmp_path: Path) -> None:
    _write_required_inputs(tmp_path)
    stale = _event("PORTAL_PROJECTION_STALE", "STALE", "portal", "WARN", "portal projection stale")
    clear = _event("PROJECTION_GENERATED", "OK", "projection", "INFO")
    append_event(stale, truth_root=tmp_path)
    append_event(clear, truth_root=tmp_path)
    state = resolve_truth_state(target_day="2026-05-04", truth_root=tmp_path, environment="PAPER")
    assert "PORTAL_PROJECTION_STALE" not in state["stale_inputs"]


def test_missing_ledger_returns_unknown_never_ready(tmp_path: Path) -> None:
    state = resolve_truth_state(target_day="2026-05-04", truth_root=tmp_path, environment="PAPER")
    assert state["final_status"] == "UNKNOWN"
    assert "evidence_ledger" in state["missing_inputs"]


def test_contradictory_surfaces_prevent_ready(tmp_path: Path) -> None:
    _write_required_inputs(tmp_path)
    append_event(_event("PORTAL_AVAILABILITY_OBSERVED", "OK", "portal", "INFO"), truth_root=tmp_path)
    append_event(_event("PORTAL_AVAILABILITY_OBSERVED", "BLOCKED", "portal", "CRITICAL", "portal blocked"), truth_root=tmp_path)
    state = resolve_truth_state(target_day="2026-05-04", truth_root=tmp_path, environment="PAPER")
    assert state["final_status"] != "READY"
    assert state["contradictions"]


def test_event_immutability_append_only(tmp_path: Path) -> None:
    first = _event("ONE")
    second = _event("TWO")
    append_event(first, truth_root=tmp_path)
    path = ledger_path(tmp_path, "2026-05-04")
    before = path.read_text(encoding="utf-8")
    append_event(second, truth_root=tmp_path)
    after = path.read_text(encoding="utf-8")
    assert after.startswith(before)
    assert len(read_events(truth_root=tmp_path, target_day="2026-05-04")) == 2


def test_protected_mutation_and_wrong_repo_guard_evented(tmp_path: Path) -> None:
    event = record_mutation_check(truth_root=tmp_path, path="/home/node/constellation/x", operation="rm", target_day="2026-05-04", environment="PAPER")
    wrong = wrong_repo_guard(expected_root="/home/node/constellation", actual_root="/home/node/projects/wardrobe_system", target_day="2026-05-04", environment="PAPER", truth_root=tmp_path)
    assert event["severity"] == "CRITICAL"
    assert event["status"] == "FORBIDDEN"
    assert wrong is not None and wrong["event_type"] == "CODEX_WRONG_REPO_GUARD_TRIGGERED"
