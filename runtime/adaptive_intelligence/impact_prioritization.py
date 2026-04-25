from __future__ import annotations

from typing import Any

from ..meta_governance.store import ArtifactStore
from .schemas import content_hash
from .types import ImpactAssessment


IMPACT_ORDER = {
    "low": 0,
    "moderate": 1,
    "high": 2,
    "critical": 3,
}


def _classify_dimension(value: float, thresholds: dict[str, float]) -> str:
    if value >= float(thresholds["critical"]):
        return "critical"
    if value >= float(thresholds["high"]):
        return "high"
    if value >= float(thresholds["moderate"]):
        return "moderate"
    return "low"


def _impact_from_thresholds(
    *,
    capital: float,
    risk: float,
    tax: float,
    execution: float,
    trust: float,
    thresholds: dict[str, Any],
) -> str:
    required = ("capital", "risk", "tax", "execution", "trust")
    missing = [key for key in required if key not in thresholds]
    if missing:
        raise ValueError("IMPACT_THRESHOLDS_REQUIRED")
    classes = [
        _classify_dimension(capital, thresholds["capital"]),
        _classify_dimension(risk, thresholds["risk"]),
        _classify_dimension(tax, thresholds["tax"]),
        _classify_dimension(execution, thresholds["execution"]),
        _classify_dimension(trust, thresholds["trust"]),
    ]
    return max(classes, key=lambda item: IMPACT_ORDER[item])


def _derive_from_drift(signal: dict[str, Any]) -> tuple[float, float, float, float, float]:
    magnitude = abs(float(signal["delta_value"]))
    family = signal["signal_family"]
    capital = magnitude if family in {"turnover_drift", "model_expectation_drift"} else 0.0
    risk = magnitude if family in {"model_expectation_drift", "valuation_drift", "pnl_drift"} else 0.0
    tax = magnitude if family == "tax_drift" else 0.0
    execution = magnitude if family == "execution_drift" else 0.0
    trust = {"informational": 0.0, "minor": 1.0, "material": 2.0, "critical": 3.0}[signal["severity"]]
    return capital, risk, tax, execution, trust


def _derive_from_regime(signal: dict[str, Any]) -> tuple[float, float, float, float, float]:
    metrics = signal["supporting_metrics"]
    execution = float(metrics.get("critical_mismatch_windows", 0) * 2 + metrics.get("material_mismatch_windows", 0))
    trust = {"informational": 0.0, "minor": 1.0, "material": 2.0, "critical": 3.0}[signal["severity"]]
    return 0.0, 0.0, 0.0, execution, trust


def _derive_from_reconciliation(result: dict[str, Any]) -> tuple[float, float, float, float, float]:
    position_count = float(len(result["position_mismatches"]))
    tax_count = float(len(result["taxlot_mismatches"]))
    execution_count = float(len(result["execution_mismatches"]))
    valuation_count = float(len(result["valuation_mismatches"]))
    pnl_count = float(len(result["pnl_mismatches"]))
    capital = position_count
    risk = valuation_count + pnl_count
    tax = tax_count + pnl_count
    execution = execution_count
    trust = {"informational": 0.0, "minor": 1.0, "material": 2.0, "critical": 3.0}[result["overall_severity"]]
    return capital, risk, tax, execution, trust


def assess_impacts(
    store: ArtifactStore,
    *,
    source_refs: tuple[str, ...],
    thresholds: dict[str, Any],
    exposure_overrides: dict[str, dict[str, float]] | None = None,
    require_explicit_exposure: bool = False,
) -> tuple[ImpactAssessment, ...]:
    overrides = exposure_overrides or {}
    assessments: list[ImpactAssessment] = []
    for ref in sorted(source_refs):
        if store.exists("drift_signals", ref):
            capital, risk, tax, execution, trust = _derive_from_drift(store.read("drift_signals", ref)["record"])
        elif store.exists("regime_signals", ref):
            capital, risk, tax, execution, trust = _derive_from_regime(store.read("regime_signals", ref)["record"])
        elif store.exists("reconciliation_results", ref):
            capital, risk, tax, execution, trust = _derive_from_reconciliation(store.read("reconciliation_results", ref)["record"])
        else:
            raise FileNotFoundError(f"IMPACT_SOURCE_UNSUPPORTED:{ref}")
        if ref in overrides:
            override = overrides[ref]
            capital = float(override.get("capital_exposure_estimate", capital))
            risk = float(override.get("risk_exposure_estimate", risk))
            tax = float(override.get("tax_exposure_estimate", tax))
            execution = float(override.get("execution_exposure_estimate", execution))
            trust = float(override.get("operator_trust_impact", trust))
        elif require_explicit_exposure:
            raise ValueError(f"EXPOSURE_INPUT_REQUIRED:{ref}")
        impact_class = _impact_from_thresholds(
            capital=capital,
            risk=risk,
            tax=tax,
            execution=execution,
            trust=trust,
            thresholds=thresholds,
        )
        payload = {
            "source_refs": (ref,),
            "capital_exposure_estimate": capital,
            "risk_exposure_estimate": risk,
            "tax_exposure_estimate": tax,
            "execution_exposure_estimate": execution,
            "operator_trust_impact": trust,
            "impact_class": impact_class,
        }
        artifact_hash = content_hash(payload)
        assessments.append(
            ImpactAssessment(
                impact_assessment_id=f"impact-{artifact_hash[:12]}",
                source_refs=(ref,),
                capital_exposure_estimate=float(capital),
                risk_exposure_estimate=float(risk),
                tax_exposure_estimate=float(tax),
                execution_exposure_estimate=float(execution),
                operator_trust_impact=float(trust),
                impact_class=impact_class,
                artifact_hash=artifact_hash,
            )
        )
    return tuple(assessments)
