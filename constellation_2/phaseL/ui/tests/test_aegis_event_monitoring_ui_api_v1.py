from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.aegis_event_monitoring_v1 import run_event_monitor_v1
from constellation_2.phaseL.ui_api.aegis_event_monitoring_read_model import build_aegis_event_monitoring_view


DAY = "2026-05-15"
NOW = "2026-05-15T19:00:00Z"


def test_aegis_event_monitoring_ui_api_exposes_rules_ledger_and_packet(tmp_path: Path) -> None:
    run_event_monitor_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        monitor_run_id="ui-event-monitor",
        timestamp_utc=NOW,
        market_snapshot={
            "generated_at_utc": "2026-05-15T18:45:00Z",
            "data_snapshot_refs": ["fixture://ui-event-snapshot"],
            "symbols": ["SPY"],
            "current_prices": {"SPY": "500.10"},
            "promoted_sleeves": [{"event_type": "PANIC_EXHAUSTION", "sleeve_id": "panic-demo"}],
            "inputs": {
                "index_return_pct": "-2.0",
                "vix_change_pct": "10.0",
                "breadth_down_pct": "80.0",
                "late_session_stabilization": True,
            },
            "tactical_packet": {
                "symbol": "SPY",
                "side": "BUY",
                "instrument_type": "ETF",
                "entry_reference_price": "500.00",
                "quantity_or_sizing_guidance": "1 share",
                "stop_price": "494.00",
                "stop_logic": "protective stop below event low",
                "risk_per_trade": "6.00",
                "execution_sensitivity": "MEDIUM",
                "valid_until": "2026-05-15T19:45:00Z",
                "max_entry_slippage": "50bps",
                "invalidation_conditions": ["new intraday low"],
                "inclusion_reason": "panic exhaustion rule passed",
            },
        },
    )

    payload = build_aegis_event_monitoring_view(DAY, tmp_path)

    assert payload["ok"] is True
    assert payload["event_rules"]
    assert payload["event_ledger"]["events"]
    assert payload["actionable_packets"]
    assert payload["blocked_packets"] == []
    assert payload["manual_execution_only"] is True
    assert payload["broker_submit_required"] is False
    assert payload["canonical_eod_state_mutated"] is False


def test_aegis_event_monitoring_ui_api_degrades_without_artifacts(tmp_path: Path) -> None:
    payload = build_aegis_event_monitoring_view(DAY, tmp_path)

    assert payload["ok"] is True
    assert payload["event_rules"]
    assert payload["monitor_status"]["status"] == "UNAVAILABLE"
    assert payload["event_ledger"]["status"] == "UNAVAILABLE"
    assert payload["email_transport_status"] == "GATE_ONLY_NO_TRANSPORT"
    assert payload["blocked_packets"] == []
