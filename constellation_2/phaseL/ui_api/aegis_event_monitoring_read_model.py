from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Optional

from constellation_2.common.aegis_event_monitoring_v1 import build_event_monitoring_operator_surface_v1
from constellation_2.phaseL.ui_api.common import GLOBAL_TRUTH_ROOT


def build_aegis_event_monitoring_view(day: Optional[str] = None, truth_root: Optional[Path] = None) -> Dict[str, Any]:
    requested_day = day or datetime.now(UTC).strftime("%Y-%m-%d")
    try:
        return build_event_monitoring_operator_surface_v1(
            truth_root=Path(truth_root or GLOBAL_TRUTH_ROOT).resolve(),
            day_utc=requested_day,
        )
    except Exception as exc:
        return {
            "ok": True,
            "schema_id": "event_monitoring_operator_surface",
            "schema_version": "v1",
            "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "day_utc": requested_day,
            "readiness": "DEGRADED",
            "reason_codes": ["EVENT_MONITORING_SURFACE_DEGRADED", f"{type(exc).__name__}:{exc}"],
            "event_rules": [],
            "monitor_status": {"status": "UNAVAILABLE"},
            "event_ledger": {"status": "UNAVAILABLE"},
            "actionable_packets": [],
            "blocked_packets": [],
            "advisory_packets": [],
            "expired_packets": [],
            "research_only_packets": [],
            "alert_ledgers": [],
            "email_transport_status": "GATE_ONLY_NO_TRANSPORT",
            "sms_transport_status": "GATE_ONLY_NO_TRANSPORT",
            "manual_execution_only": True,
            "broker_submit_required": False,
            "canonical_eod_state_mutated": False,
        }
