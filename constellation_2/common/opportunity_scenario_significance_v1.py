from __future__ import annotations

from typing import Any, Mapping, Sequence


def evaluate_opportunity_scenario_significance_v1(
    *,
    scenario_payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(scenario_payload, Mapping):
        return {"state": "not_evaluated", "reason_id": "SCENARIO_NOT_EVALUATED"}
    support_status = str(scenario_payload.get("support_status") or "").strip().lower()
    summary = str(scenario_payload.get("summary") or "").strip()
    scenario_ids = [str(item).strip() for item in (scenario_payload.get("scenario_ids") or []) if str(item).strip()]
    outcomes = [str(item).strip() for item in (scenario_payload.get("outcomes") or []) if str(item).strip()]
    if support_status != "fully_supported" or summary == "stub_only_view" or "no_financial_math" in set(outcomes):
        return {"state": "basis_unavailable", "reason_id": "SCENARIO_BASIS_UNAVAILABLE"}
    if len(scenario_ids) > 1 and outcomes:
        return {"state": "review_now", "reason_id": "SCENARIO_COMPARISON_MATERIAL"}
    return {"state": "monitor_only", "reason_id": "SCENARIO_MONITOR_ONLY"}
