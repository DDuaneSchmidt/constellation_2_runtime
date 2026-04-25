from __future__ import annotations

from typing import Any, Dict, Optional

from .policy_evolution_state_read_model import build_policy_evolution_view


def build_system_summary_view(day: Optional[str] = None) -> Dict[str, Any]:
    payload = build_policy_evolution_view(day)
    payload["view_name"] = "system_summary"
    return payload
