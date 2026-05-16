from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseL.ui_api.aegis_lite_execution_queue_read_model import (
    aegis_ui_runtime_status_path_v1,
    build_aegis_lite_execution_queue_view,
    build_aegis_lite_ui_health_view,
)


DAY = "2026-05-15"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _status_path(root: Path) -> Path:
    return root / "reports/aegis_lite_operating_status_v1" / DAY / "aegis_lite_operating_status.v1.json"


def _release_path(root: Path) -> Path:
    return root / "reports/aegis_release_integrity_status_v1/current/aegis_release_integrity_status.v1.json"


def _report_path(root: Path) -> Path:
    return root / "reports/aegis_lite_eod_report_v1" / DAY / "run" / "aegis_lite_eod_report.v1.json"


def _queue_path(root: Path) -> Path:
    return root / "reports/operator_execution_queue_v1" / DAY / "run" / "operator_execution_queue.v1.json"


def _write_release(root: Path, match: str = "MATCH") -> None:
    _write_json(
        _release_path(root),
        {
            "release_match_status": match,
            "active_release_id": "release-demo",
            "active_release_commit": "abc123",
            "active_release_path": "/home/node/constellation_releases/release-demo",
        },
    )


def _write_valid_report(root: Path) -> None:
    _write_json(
        _report_path(root),
        {
            "day_utc": DAY,
            "run_id": "run",
            "generated_at_utc": "2026-05-15T20:00:00Z",
            "manual_execution_status": "READY_FOR_MANUAL_ENTRY",
            "readiness_classification": "READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING",
            "data_freshness_status": {"status": "PASS"},
            "governance_status": {"status": "PASS"},
            "operating_model": {"ib_automation_status": "DEFERRED"},
            "edge_overlap_summary": {"distinct_edge_count": 1, "portfolio_concentration_warnings": []},
            "missing_stop_warnings": [],
            "warnings": [],
            "runtime_truth_classification": "REAL_RUNTIME",
            "alert_transport_status": "GATE_ONLY_NO_TRANSPORT",
            "selected_trade_candidates": [
                {
                    "candidate_id": "candidate-1",
                    "sleeve_id": "demo-sleeve",
                    "sleeve_ownership": "demo-owner",
                    "promotion_status": "promoted",
                    "executable_status": "EXECUTABLE",
                    "trade_class": "LONG_EQUITY",
                    "symbol": "SPY",
                    "direction": "LONG",
                    "suggested_quantity": 1,
                    "entry_reference_price": "520.10",
                    "stop_price": "514.90",
                    "risk_per_trade": "5.20",
                    "edge_overlap": {"governance_recommendation": "approve"},
                    "execution_confidence_badges": ["GOVERNED_READY"],
                    "runtime_truth_classification": "REAL_RUNTIME",
                }
            ],
        },
    )


def _write_valid_queue(root: Path) -> None:
    _write_json(
        _queue_path(root),
        {
            "day_utc": DAY,
            "execution_queue": [
                {
                    "candidate_id": "candidate-1",
                    "execution_order": 1,
                    "priority_rank": 1,
                    "trade_class": "LONG_EQUITY",
                    "edge_cluster_id": "EDGE-1",
                    "queue_status": "READY_FOR_MANUAL_ENTRY",
                    "reason_codes": [],
                    "manual_execution_recipe": "side=BUY symbol=SPY quantity=1 entry_instruction=520.10 stop_order_type=STP stop_price=514.90 stop_quantity=1",
                }
            ],
        },
    )


def _write_status(root: Path, match: str = "MATCH", *, queue_path: Path | None = None) -> None:
    _write_json(
        _status_path(root),
        {
            "lite_eod_latest_report_path": str(_report_path(root)),
            "lite_eod_latest_queue_path": str(queue_path or _queue_path(root)),
            "release_repo_match_status": match,
            "readiness_classification": "READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING",
            "current_blockers": [],
            "warnings": [],
            "runtime_truth_classification": "REAL_RUNTIME",
            "alert_transport_status": "GATE_ONLY_NO_TRANSPORT",
            "legacy_paper_runtime_status": {
                "lite_operational_spine_active": True,
                "legacy_runtime_active": False,
            },
        },
    )


def test_missing_lite_report_degrades_to_not_ready_and_writes_runtime_status(tmp_path: Path) -> None:
    payload = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert payload["readiness_classification"] == "NOT_READY"
    assert "NO_CURRENT_LITE_REPORT" in payload["current_blockers"]
    status_path = aegis_ui_runtime_status_path_v1(truth_root=tmp_path)
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["schema_id"] == "aegis_ui_runtime_status"
    assert status["health_status"] == "WARN"
    assert status["ui_process_alive"] is True
    assert status["backend_api_alive"] is True


def test_missing_queue_degrades_to_not_ready(tmp_path: Path) -> None:
    _write_release(tmp_path)
    _write_valid_report(tmp_path)
    _write_status(tmp_path, queue_path=tmp_path / "missing_queue.json")

    payload = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert payload["readiness_classification"] == "NOT_READY"
    assert "NO_CURRENT_LITE_QUEUE" in payload["current_blockers"]
    assert payload["executable_trades"] == []


def test_malformed_queue_degrades_without_exception(tmp_path: Path) -> None:
    _write_release(tmp_path)
    _write_valid_report(tmp_path)
    _queue_path(tmp_path).parent.mkdir(parents=True, exist_ok=True)
    _queue_path(tmp_path).write_text("{not-json", encoding="utf-8")
    _write_status(tmp_path)

    payload = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert payload["readiness_classification"] == "NOT_READY"
    assert "NO_CURRENT_LITE_QUEUE" in payload["current_blockers"]
    assert "JSON_DECODE_ERROR" in payload["current_blockers"]


def test_release_mismatch_downgrades_ready_queue_to_advisory(tmp_path: Path) -> None:
    _write_release(tmp_path, match="MISMATCH")
    _write_valid_report(tmp_path)
    _write_valid_queue(tmp_path)
    _write_status(tmp_path, match="MISMATCH")

    payload = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert payload["readiness_classification"] == "ADVISORY_ONLY"
    assert payload["manual_execution_status"] == "NOT_READY"
    assert payload["executable_trades"] == []
    assert payload["blocked_or_advisory_trades"][0]["do_not_trade_blockers"] == ["ACTIVE_RELEASE_REPO_MISMATCH"]


def test_valid_queue_can_render_and_health_passes(tmp_path: Path) -> None:
    _write_release(tmp_path)
    _write_valid_report(tmp_path)
    _write_valid_queue(tmp_path)
    _write_status(tmp_path)

    payload = build_aegis_lite_execution_queue_view(DAY, tmp_path)
    health = build_aegis_lite_ui_health_view(tmp_path, DAY)
    status = json.loads(aegis_ui_runtime_status_path_v1(truth_root=tmp_path).read_text(encoding="utf-8"))

    assert payload["readiness_classification"] == "READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING"
    assert payload["runtime_truth_classification"] == "REAL_RUNTIME"
    assert payload["alert_transport_status"] == "GATE_ONLY_NO_TRANSPORT"
    assert payload["queue_summary"]["executable_trades_count"] == 1
    assert health["status"] == "PASS"
    assert health["queue_availability"]["available"] is True
    assert status["readiness_classification"] == "READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING"


def test_demo_runtime_truth_candidate_cannot_appear_executable(tmp_path: Path) -> None:
    _write_release(tmp_path)
    _write_valid_report(tmp_path)
    report = json.loads(_report_path(tmp_path).read_text(encoding="utf-8"))
    report["selected_trade_candidates"][0]["runtime_truth_classification"] = "DEMO_ONLY"
    _write_json(_report_path(tmp_path), report)
    _write_valid_queue(tmp_path)
    _write_status(tmp_path)

    payload = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert payload["executable_trades"] == []
    assert payload["blocked_or_advisory_trades"]
    card = payload["blocked_or_advisory_trades"][0]
    assert card["runtime_truth_classification"] == "DEMO_ONLY"
    assert "DEMO_ONLY_NOT_ACTIONABLE" in card["do_not_trade_blockers"]


def test_health_endpoint_is_wired_to_structured_lite_health() -> None:
    root = Path(__file__).resolve().parents[4]
    server = (root / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")

    assert '"/healthz/aegis-lite-ui"' in server
    assert '"/aegis-lite"' in server
    assert "build_aegis_lite_ui_health_view" in server
