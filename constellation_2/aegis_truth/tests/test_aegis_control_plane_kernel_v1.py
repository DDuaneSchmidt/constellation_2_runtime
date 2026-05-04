from __future__ import annotations

import json
from pathlib import Path

from constellation_2.aegis_truth.evidence_event_v1 import build_event
from constellation_2.aegis_truth.evidence_ledger_v1 import append_event
from constellation_2.aegis_truth.state_machines.alert_lifecycle_state_v1 import build_alert_lifecycle_state
from constellation_2.aegis_truth.state_machines.portal_surface_state_v1 import build_portal_surface_state
from constellation_2.aegis_truth.state_machines.projection_freshness_state_v1 import build_projection_freshness_state
from constellation_2.aegis_truth.state_machines.repo_protection_state_v1 import build_repo_protection_state
from constellation_2.aegis_truth.state_machines.trading_readiness_state_v1 import AUTHORITATIVE_INPUTS, build_trading_readiness_state
from ops.tools.run_aegis_control_plane_kernel_v1 import build_control_plane_kernel, write_control_plane_kernel
from ops.tools.run_aegis_truth_resolver_v1 import write_compatible_truth_state

DAY = "2026-05-04"


def _event(event_type: str, status: str = "OK", owner: str = "test", severity: str = "INFO", blocker: str | None = None, payload: dict | None = None, observed: str | None = None) -> dict:
    return build_event(event_type=event_type, producer="test", target_day=DAY, environment="PAPER", status=status, blocker=blocker, owner=owner, severity=severity, payload=payload or {"event_type": event_type, "status": status}, next_action="test action", observed_at_utc=observed)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_required_inputs(root: Path, status: str = "OK") -> None:
    for name, pattern in AUTHORITATIVE_INPUTS.items():
        _write_json(root / pattern.format(day=DAY) / "state.json", {"status": status, "severity": "INFO"})


def _write_repo_protected(root: Path) -> None:
    _write_json(root / "repo_protection_v1" / "status.json", {"status": "PROTECTED", "protected": True})


def _write_alert(root: Path, *, title: str, source_event_id: str, severity: str = "CRITICAL", message: str = "alert") -> None:
    path = root / "alerts" / "aegis_alerts_v1" / DAY / "alerts.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    alert = {"schema_version": "aegis_alert.v1", "alert_id": title.lower().replace(" ", "_") + source_event_id[:8], "source_event_id": source_event_id, "severity": severity, "title": title, "message": message, "created_at_utc": "2026-05-04T00:00:00Z", "ack_required": True, "status": "OPEN"}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(alert, sort_keys=True) + "\n")


def _green_base(root: Path) -> None:
    _write_required_inputs(root)
    _write_repo_protected(root)
    append_event(_event("PORTAL_AVAILABILITY_OBSERVED", "OK", "portal", "INFO"), truth_root=root)
    append_event(_event("PROJECTION_GENERATED", "OK", "projection", "INFO"), truth_root=root)


def test_historical_simulated_502_recovers_alert_and_portal_current_ok(tmp_path: Path) -> None:
    down = _event("PORTAL_ORIGIN_UNREACHABLE", "BLOCKED", "portal", "CRITICAL", "portal returned HTTP 502", {"simulated": True, "http_status": 502}, "2026-05-04T00:00:01Z")
    append_event(down, truth_root=tmp_path)
    _write_alert(tmp_path, title="Portal Origin Unreachable", source_event_id=down["event_id"], message="portal returned HTTP 502")
    append_event(_event("PORTAL_AVAILABILITY_OBSERVED", "OK", "portal", "INFO", observed="2026-05-04T00:00:02Z"), truth_root=tmp_path)
    portal = build_portal_surface_state(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    lifecycle = build_alert_lifecycle_state(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    assert portal["status"] == "OK"
    assert portal["recovery_observed"] is True
    assert lifecycle["alerts"][0]["lifecycle_state"] == "HISTORICAL_TEST"
    assert lifecycle["active_alerts"] == []


def test_active_portal_outage_degrades_kernel_when_trading_ready(tmp_path: Path) -> None:
    _write_required_inputs(tmp_path)
    _write_repo_protected(tmp_path)
    append_event(_event("PROJECTION_GENERATED", "OK", "projection", "INFO", observed="2026-05-04T00:00:01Z"), truth_root=tmp_path)
    down = _event("PORTAL_ORIGIN_UNREACHABLE", "BLOCKED", "portal", "CRITICAL", "portal timeout", {"http_status": 504}, "2026-05-04T00:00:02Z")
    append_event(down, truth_root=tmp_path)
    _write_alert(tmp_path, title="Portal Origin Unreachable", source_event_id=down["event_id"], message="portal timeout")
    portal = build_portal_surface_state(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    lifecycle = build_alert_lifecycle_state(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    kernel = build_control_plane_kernel(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    assert portal["status"] == "DOWN"
    assert lifecycle["active_alerts"]
    assert kernel["final_status"] == "DEGRADED"


def test_missing_trading_inputs_make_trading_and_kernel_unknown(tmp_path: Path) -> None:
    _write_repo_protected(tmp_path)
    append_event(_event("PORTAL_AVAILABILITY_OBSERVED", "OK", "portal", "INFO"), truth_root=tmp_path)
    append_event(_event("PROJECTION_GENERATED", "OK", "projection", "INFO"), truth_root=tmp_path)
    trading = build_trading_readiness_state(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    kernel = build_control_plane_kernel(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    assert trading["status"] == "UNKNOWN"
    assert kernel["final_status"] == "UNKNOWN"
    assert "missing authoritative trading inputs" in kernel["primary_blocker"]


def test_trading_forbidden_beats_portal_ok(tmp_path: Path) -> None:
    _green_base(tmp_path)
    append_event(_event("TRANSMIT_FORBIDDEN", "FORBIDDEN", "submit_boundary", "CRITICAL", "transmit forbidden", observed="2026-05-04T00:00:04Z"), truth_root=tmp_path)
    kernel = build_control_plane_kernel(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    assert kernel["final_status"] == "FORBIDDEN"
    assert kernel["owner"] == "trading_readiness"


def test_projection_stale_degrades_kernel_unless_trading_worse(tmp_path: Path) -> None:
    _green_base(tmp_path)
    append_event(_event("PROJECTION_STALE", "STALE", "projection", "WARN", "packet stale"), truth_root=tmp_path)
    projection = build_projection_freshness_state(target_day=DAY, truth_root=tmp_path, environment="PAPER", max_age_seconds=999999999)
    kernel = build_control_plane_kernel(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    assert projection["status"] == "STALE"
    assert kernel["final_status"] == "DEGRADED"


def test_repo_breach_blocks_kernel(tmp_path: Path) -> None:
    _green_base(tmp_path)
    append_event(_event("PROTECTED_MUTATION_BREACH_ATTEMPT", "FORBIDDEN", "repo_protection", "CRITICAL", "unexpected mutation", observed="2026-05-04T00:00:04Z"), truth_root=tmp_path)
    repo = build_repo_protection_state(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    kernel = build_control_plane_kernel(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    assert repo["status"] == "BREACH_ATTEMPT"
    assert kernel["final_status"] == "BLOCKED"


def test_all_green_kernel_ready(tmp_path: Path) -> None:
    _green_base(tmp_path)
    kernel = build_control_plane_kernel(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    assert kernel["final_status"] == "READY"
    assert kernel["primary_blocker"] is None
    assert kernel["active_alerts"] == []



def test_legacy_truth_resolver_uses_kernel_compatibility_when_kernel_exists(tmp_path: Path) -> None:
    _green_base(tmp_path)
    append_event(_event("TRADING_SAFETY_BLOCKED", "BLOCKED", "trading_safety", "ERROR", "safety blocked"), truth_root=tmp_path)
    write_control_plane_kernel(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    _path, state, source = write_compatible_truth_state(target_day=DAY, truth_root=tmp_path, environment="PAPER")
    assert source == "aegis_control_plane_kernel.v1"
    assert state["final_status"] == "BLOCKED"
    assert state["compatibility_note"].startswith("Derived from aegis_control_plane_kernel")
