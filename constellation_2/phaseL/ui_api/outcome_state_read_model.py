from __future__ import annotations

from typing import Any, Dict, Optional

from . import value_state_read_model as _value_state_read_model


GLOBAL_TRUTH_ROOT = _value_state_read_model.GLOBAL_TRUTH_ROOT


def build_outcome_state_view(day: Optional[str] = None) -> Dict[str, Any]:
    _value_state_read_model.GLOBAL_TRUTH_ROOT = GLOBAL_TRUTH_ROOT
    value_view = _value_state_read_model.build_value_state_view(day)
    return {
        **value_view,
        "view_name": "outcomes",
        "outcome_day": value_view.get("value_day"),
        "outcome_rows": list(value_view.get("value_rows") or []),
        "outcome_warnings": list(value_view.get("value_warnings") or []),
    }
