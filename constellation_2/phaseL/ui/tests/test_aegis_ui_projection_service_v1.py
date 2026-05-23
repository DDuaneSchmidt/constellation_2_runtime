from __future__ import annotations

import json
from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1
from types import SimpleNamespace

import pytest

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server
from ops.tools import run_ui_service_authority_v1 as authority


ROOT = Path(__file__).resolve().parents[4]


class _ReadyzHandler:
    SHELL_ROUTES = server.OpsHandler.SHELL_ROUTES

    def _route_status_payload(self) -> dict:
        return server.OpsHandler._route_status_payload(self)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def test_package_exposes_canonical_aegis_ui_service_commands() -> None:
    package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
    scripts = package["scripts"]

    assert scripts["aegis:ui:start"] == "python3 ops/runtime/supervisor.py start"
    assert scripts["aegis:ui:stop"] == "python3 ops/runtime/supervisor.py stop"
    assert scripts["aegis:ui:restart"] == "python3 ops/runtime/supervisor.py restart"
    assert scripts["aegis:ui:status"] == "python3 ops/runtime/supervisor.py status"


def test_runtime_manifest_uses_canonical_host_port_and_healthz() -> None:
    manifest = (ROOT / "ops/runtime/runtime_manifest.yaml").read_text(encoding="utf-8")

    assert "host: 127.0.0.1" in manifest
    assert "port: 8787" in manifest
    assert "entrypoint: constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py" in manifest
    assert "health_url: http://127.0.0.1:8787/healthz" in manifest


def test_supervisor_duplicate_start_returns_already_ready() -> None:
    source = (ROOT / "ops/runtime/supervisor.py").read_text(encoding="utf-8")

    assert 'if pre["state"] == STATE_READY:' in source
    assert '"action": "already_ready"' in source


def test_healthz_payload_includes_process_status() -> None:
    handler = SimpleNamespace(server=SimpleNamespace(server_address=("127.0.0.1", 8787)))

    payload = server.OpsHandler._health_payload(handler)

    assert payload["process_alive"] is True
    assert isinstance(payload["pid"], int)
    assert payload["host"] == "127.0.0.1"
    assert payload["port"] == 8787
    assert payload["started_at_utc"]


def test_readyz_passes_when_truth_projection_and_routes_exist(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    runtime_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    packet = tmp_path / "packet.md"
    truth.mkdir(parents=True)
    runtime_truth.mkdir(parents=True)
    packet.write_text("packet", encoding="utf-8")
    monkeypatch.setattr(server, "_canonical_truth_root", lambda: truth)
    monkeypatch.setattr(server, "_runtime_truth_root", lambda: runtime_truth)
    monkeypatch.setattr(server, "_latest_packet_path", lambda: packet)
    handler = _ReadyzHandler()

    payload = server.OpsHandler._readyz_payload(handler)

    assert payload["status"] == "PASS"
    assert payload["checks"]["truth_root_resolved"] is True
    assert payload["checks"]["runtime_truth_available"] is True
    assert payload["route_status"]["routes"]["/aegis-runtime"] is True


def test_readyz_fails_when_truth_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(server, "_canonical_truth_root", lambda: tmp_path / "missing_truth")
    monkeypatch.setattr(server, "_runtime_truth_root", lambda: tmp_path / "missing_runtime_truth")
    monkeypatch.setattr(server, "_latest_packet_path", lambda: tmp_path / "missing_packet.md")
    handler = _ReadyzHandler()

    payload = server.OpsHandler._readyz_payload(handler)

    assert payload["status"] == "FAIL"
    assert payload["checks"]["truth_root_resolved"] is False


def test_runtime_status_projection_reads_current_canonical_artifacts_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    runtime_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    packet = tmp_path / "packet.md"
    runtime_truth.mkdir(parents=True)
    packet.write_text("packet", encoding="utf-8")
    _write_json(
        truth / "reports/aegis_day_run_v1" / day / "day_run.v1.json",
        {
            "day_utc": day,
            "final_status": "NOT_READY",
            "canonical_phase": "MARKET_OPEN_DATA_GATE",
            "canonical_blocker": "MARKET_CLOSED",
            "operator_next_action": "wait",
            "source_repo_status": {"source_reproducibility_status": "REPRODUCIBLE"},
        },
    )
    _write_json(
        truth / "pointers/selected_intent_pointer.v1.json",
        {"status": "SELECTED", "selected_intent_id": "intent_iwm"},
    )
    _write_json(truth / "reports/portfolio_state_v1" / day / "portfolio_state.v1.json", {"day_utc": day, "status": "PASS"})
    _write_json(truth / "reports/portfolio_scoring_v1" / day / "portfolio_scoring.v1.json", {"day_utc": day, "status": "PASS"})
    _write_json(truth / "reports/decision_ledger_v1" / day / "decision_ledger.v1.json", {"day_utc": day, "status": "PASS"})
    monkeypatch.setattr(
        server,
        "build_readiness_kernel_v1",
        lambda _day: {
            "truth_root": str(truth),
            "overall_status": "BLOCKED",
            "current_phase": "MARKET_OPEN_DATA_GATE",
            "canonical_blocker": "MARKET_CLOSED",
            "operator_next_action": "wait",
            "runtime_mode": "PRODUCTION",
            "production_version_status": "ACTIVE",
            "promoted_commit": "a" * 40,
            "submit_status": "BLOCKED",
            "submit_canonical_blocker": "DAY_RUN_LEDGER_NOT_READY",
            "primary_ui_authority": "aegis_control_plane_v1",
        },
    )
    monkeypatch.setattr(server, "_canonical_truth_root", lambda: truth)
    monkeypatch.setattr(server, "_runtime_truth_root", lambda: runtime_truth)
    monkeypatch.setattr(server, "_latest_packet_path", lambda: packet)

    payload = server._runtime_status_projection(day)

    assert payload["status"] == "PASS"
    assert payload["truth_root"] == str(truth)
    assert payload["final_status"] == "BLOCKED"
    assert payload["runtime_mode"] == "PRODUCTION"
    assert payload["production_version_status"] == "ACTIVE"
    assert payload["canonical_blocker"] == "MARKET_CLOSED"
    assert payload["selected_intent_id"] == "intent_iwm"


def test_runtime_status_projection_degrades_when_projection_artifact_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    runtime_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime_truth.mkdir(parents=True)
    monkeypatch.setattr(
        server,
        "build_readiness_kernel_v1",
        lambda _day: {
            "truth_root": str(truth),
            "overall_status": "UNKNOWN",
            "canonical_blocker": "CONTROL_PLANE_MISSING",
            "primary_ui_authority": "aegis_control_plane_v1",
        },
    )
    monkeypatch.setattr(server, "_canonical_truth_root", lambda: truth)
    monkeypatch.setattr(server, "_runtime_truth_root", lambda: runtime_truth)

    payload = server._runtime_status_projection(day)

    assert payload["status"] == "FAIL"
    assert "AEGIS_CONTROL_PLANE_MISSING" in payload["reason_codes"]


def test_runtime_status_projection_reads_nested_selected_intent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    truth = tmp_path / "truth"
    _write_json(
        truth / "pointers/selected_intent_pointer.v1.json",
        {"status": "SELECTED", "selected_intent": {"intent_id": "nested_iwm_intent"}},
    )
    monkeypatch.setattr(server, "_canonical_truth_root", lambda: truth)

    selected_intent_id, status, _path = server._selected_intent_projection()

    assert selected_intent_id == "nested_iwm_intent"
    assert status == "SELECTED"


def test_aegis_runtime_route_and_offline_guidance_are_present() -> None:
    pages = pages_source_v1(ROOT)
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    api_client = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/api_client/index.js").read_text(encoding="utf-8")

    assert "/aegis-runtime" in server.OpsHandler.SHELL_ROUTES
    assert "fetchRuntimeStatus" in pages
    assert "Phase-Controlled Readiness" in pages
    assert "BACKEND_UNAVAILABLE" in pages
    assert "npm run aegis:ui:restart" in pages
    assert "aegis.runtime.lastKnownTruth.v1" in pages
    assert "CONNECTED" in main and "RECONNECTING" in main
    assert "aegis:connection-state" in api_client


def test_ui_service_authority_records_service_status(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    truth.mkdir()
    service = SimpleNamespace(name="ops_dashboard", log_path=tmp_path / "ops_dashboard.log")
    monkeypatch.setattr(authority, "resolve_canonical_truth_root", lambda: truth)
    monkeypatch.setattr(authority.supervisor, "load_manifest", lambda _path: [service])
    monkeypatch.setattr(
        authority.supervisor,
        "probe_service",
        lambda _service: {
            "pid": 12345,
            "pid_running": True,
            "route_status": {"status": "PASS", "routes": {"/aegis-runtime": {"ok": True}}},
        },
    )
    monkeypatch.setattr(
        authority,
        "_read_json_url",
        lambda url, timeout_seconds=2.0: {
            "ok": True,
            "http_status": 200,
            "url": url,
            "payload": {
                "status": "READY" if url.endswith("/healthz") else "PASS",
                "started_at_utc": "2026-04-30T12:00:00Z",
            },
        },
    )

    payload = authority.build_ui_service_authority(day_utc="2026-04-30", environment="PAPER")

    assert payload["status"] == "PASS"
    assert payload["host"] == "127.0.0.1"
    assert payload["port"] == 8787
    assert payload["pid"] == 12345
    assert payload["healthz_status"] == "PASS"
    assert payload["readyz_status"] == "PASS"
    assert payload["truth_root"] == str(truth)
