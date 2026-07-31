from __future__ import annotations

import json
from pathlib import Path

from ops.tools import repair_aegis_runtime_readiness_v1 as repair

DAY = "2026-05-26"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def test_runtime_readiness_repair_creates_required_artifacts(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"

    def fake_run(cmd: list[str], *, env: dict[str, str] | None = None) -> dict[str, object]:
        command = " ".join(cmd)
        if "build_aegis_market_context_provider_health_v1.py" in command:
            _write_json(truth_root / "reports" / "aegis_market_context_provider_health_v1" / DAY / "provider_health.v1.json", {"day_utc": DAY})
        elif "build_aegis_market_context_demand_v1.py" in command:
            _write_json(truth_root / "reports" / "aegis_market_context_demand_v1" / DAY / "market_context_demand.v1.json", {"day_utc": DAY})
        elif "build_event_market_snapshot_v1.py" in command:
            _write_json(truth_root / "reports" / "event_market_snapshot_v1" / DAY / "event_market_snapshot.v1.json", {"day_utc": DAY})
        elif "run_aegis_event_monitor_v1.py" in command:
            _write_json(truth_root / "reports" / "event_monitoring_status_v1" / DAY / "run" / "event_monitoring_status.v1.json", {"day_utc": DAY})
            _write_json(truth_root / "reports" / "event_rules_registry_v1" / DAY / "run" / "event_rules_registry.v1.json", {"day_utc": DAY})
        elif "write_event_validity_evidence_v1.py" in command:
            _write_json(truth_root / "reports" / "event_validity_gate_v1" / DAY / "index" / "event_validity_gate.v1.json", {"day_utc": DAY, "validity_status": "NO_EVENT_PACKET"})
        elif "write_alert_transport_proof_v1.py" in command:
            _write_json(truth_root / "reports" / "alert_transport_proof_v1" / DAY / "index" / "alert_transport_proof.v1.json", {"day_utc": DAY, "result": "GATE_ONLY"})
        elif "run_research_test_queue_v1.py" in command:
            _write_json(truth_root / "research_lab" / "research_task_queue_v1" / DAY / "index" / "research_task_queue.v1.json", {"day_utc": DAY, "tasks": []})
        elif "audit_research_dataset_bindings_v1.py" in command:
            _write_json(truth_root / "reports" / "research_dataset_gap_v1" / DAY / "research_dataset_gap.v1.json", {"day_utc": DAY, "dataset_gaps": []})
        elif "build_ai_eod_feedback_review_v1.py" in command:
            _write_json(truth_root / "reports" / "ai_feedback_review_v1" / "EOD" / DAY / "ai_feedback_review.v1.json", {"period_end": DAY, "ai_used": False})
        elif "run_aegis_lite_eod_pipeline_v1.py" in command:
            _write_json(truth_root / "reports" / "aegis_lite_operating_status_v1" / DAY / "aegis_lite_operating_status.v1.json", {"day_utc": DAY})
            _write_json(truth_root / "reports" / "aegis_lite_eod_report_v1" / DAY / "run" / "aegis_lite_eod_report.v1.json", {"day_utc": DAY})
            _write_json(truth_root / "reports" / "operator_execution_queue_v1" / DAY / "run" / "operator_execution_queue.v1.json", {"day_utc": DAY})
            _write_json(truth_root / "reports" / "manual_trade_packet_v1" / DAY / "run" / "manual_trade_packet.v1.json", {"date": DAY})
        return {"command": command, "exit_code": 0, "stdout": "", "stderr": "", "started_at_utc": DAY, "completed_at_utc": DAY}

    monkeypatch.setattr(repair, "_run", fake_run)
    payload = repair.build_runtime_readiness_repair_v1(truth_root=truth_root, day_utc=DAY)

    by_id = {row["artifact_id"]: row for row in payload["artifact_statuses"]}
    assert by_id["aegis_lite_operating_status"]["status"] == "PRESENT"
    assert by_id["aegis_lite_eod_report"]["status"] == "PRESENT"
    assert by_id["operator_execution_queue"]["status"] == "PRESENT"
    assert by_id["manual_trade_packet"]["status"] == "PRESENT"
    assert by_id["event_monitoring_status"]["status"] == "PRESENT"
    assert by_id["event_rules_registry"]["status"] == "PRESENT"
    assert by_id["market_context_provider_health"]["status"] == "PRESENT"
    assert by_id["market_context_demand"]["status"] == "PRESENT"
    assert by_id["event_market_snapshot"]["status"] == "PRESENT"
    assert by_id["alert_transport_proof"]["status"] == "PRESENT"
    assert by_id["ai_feedback_review"]["status"] == "PRESENT"
    assert by_id["research_dataset_binding"]["status"] == "PRESENT"
    assert by_id["research_task_queue"]["status"] == "PRESENT"
    assert payload["trade_advice_allowed"] is False
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
