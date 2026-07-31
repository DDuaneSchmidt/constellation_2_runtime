from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.hypothesis_outcome_ledger_v1 import build_hypothesis_outcome_ledger_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1

REPORT_FAMILY = "aegis_statistical_sufficiency_v1"
REPORT_FILENAME = "statistical_sufficiency.v1.json"
CONFIG_VERSION = "STATISTICAL_SUFFICIENCY_DEFAULTS_V1"
CONFIG = {"minimum_required_samples": 30, "validation_ready_samples": 10, "max_drawdown_limit": -0.15, "validated_min_expectancy": 0.0, "disproven_max_expectancy": -0.02, "minimum_independent_sleeves_for_strong_independence": 2}
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False}


def statistical_sufficiency_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def build_statistical_sufficiency_v1(*, truth_root: Path | str, day_utc: str, ledger: Mapping[str, Any] | None = None) -> dict[str, Any]:
    ledger_payload = dict(ledger or build_hypothesis_outcome_ledger_v1(truth_root=truth_root, day_utc=day_utc))
    rows = [_row(r) for r in ledger_payload.get("hypotheses") or [] if isinstance(r, Mapping)]
    payload = {"schema_id": "aegis_statistical_sufficiency", "schema_version": "v1", "artifact_id": REPORT_FAMILY, "day_utc": str(day_utc), "generated_at": _now(), "config_version": CONFIG_VERSION, "config": dict(CONFIG), "hypotheses": rows, "summary": {"hypothesis_count": len(rows), "underpowered": sum(1 for r in rows if r["sufficiency_state"] == "UNDERPOWERED"), "accumulating": sum(1 for r in rows if r["sufficiency_state"] == "ACCUMULATING"), "validation_ready": sum(1 for r in rows if r["sufficiency_state"] == "VALIDATION_READY"), "validated": sum(1 for r in rows if r["sufficiency_state"] == "VALIDATED"), "disproven": sum(1 for r in rows if r["sufficiency_state"] == "DISPROVEN")}, "safety": dict(SAFETY), **SAFETY}
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_statistical_sufficiency_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(statistical_sufficiency_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_statistical_sufficiency_v1(truth_root=truth_root, day_utc=day_utc))


def _row(row: Mapping[str, Any]) -> dict[str, Any]:
    n = int(row.get("usable_validation_sample_count") or 0); expectancy = row.get("expectancy"); drawdown = row.get("max_drawdown")
    reasons=[]; blockers=[]
    if n < CONFIG["minimum_required_samples"]: reasons.append("INSUFFICIENT_CLOSED_SAMPLES")
    sample_status = "PASS" if n >= CONFIG["minimum_required_samples"] else "LOW"
    independence = "WEAK" if n < CONFIG["minimum_required_samples"] else "PASS"
    regime = "LOW" if n < CONFIG["minimum_required_samples"] else "PASS"
    expectancy_status = "UNKNOWN" if expectancy is None else "POSITIVE" if expectancy > 0 else "NEGATIVE" if expectancy < 0 else "FLAT"
    if expectancy_status == "POSITIVE": reasons.append("POSITIVE_EXPECTANCY")
    if expectancy_status == "NEGATIVE": reasons.append("NEGATIVE_EXPECTANCY")
    drawdown_status = "UNKNOWN" if drawdown is None else "PASS" if drawdown >= CONFIG["max_drawdown_limit"] else "BREACH"
    if drawdown_status == "BREACH": reasons.append("DRAWDOWN_LIMIT_BREACH")
    benchmark = "UNKNOWN" if expectancy is None else "EXCESS_RETURN_POSITIVE" if expectancy > 0 else "EXCESS_RETURN_NEGATIVE" if expectancy < 0 else "FLAT"
    if benchmark in {"EXCESS_RETURN_POSITIVE", "EXCESS_RETURN_NEGATIVE"}: reasons.append(benchmark)
    if regime == "LOW": reasons.append("REGIME_COVERAGE_LOW")
    if independence == "WEAK": reasons.append("SAMPLE_INDEPENDENCE_WEAK")
    if n == 0:
        state = "UNDERPOWERED"
    elif n < CONFIG["validation_ready_samples"]:
        state = "ACCUMULATING"
    elif n < CONFIG["minimum_required_samples"]:
        state = "VALIDATION_READY"; reasons.append("VALIDATION_READY_THRESHOLD_MET")
    elif expectancy is not None and expectancy >= CONFIG["validated_min_expectancy"] and drawdown_status != "BREACH":
        state = "VALIDATED"; reasons.append("VALIDATED_THRESHOLD_MET")
    elif expectancy is not None and expectancy <= CONFIG["disproven_max_expectancy"]:
        state = "DISPROVEN"; reasons.append("DISPROOF_THRESHOLD_MET")
    else:
        state = "DEGRADED"
    return {"hypothesis_id": row.get("hypothesis_id"), "thesis_id": row.get("thesis_id"), "sufficiency_state": state, "usable_sample_count": n, "minimum_required_samples": CONFIG["minimum_required_samples"], "sample_independence_status": independence, "regime_coverage_status": regime, "expectancy_status": expectancy_status, "drawdown_status": drawdown_status, "benchmark_comparison_status": benchmark, "confidence_status": "SUFFICIENT" if n >= CONFIG["minimum_required_samples"] else "INSUFFICIENT", "blocker_reasons": blockers, "state_reason_codes": reasons or ["NO_REASON_CODE"], "expectancy": expectancy, "excess_return": expectancy, "max_drawdown": drawdown, "next_evidence_needed": max(0, CONFIG["minimum_required_samples"] - n)}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
