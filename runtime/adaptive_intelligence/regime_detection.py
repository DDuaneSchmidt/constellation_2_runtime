from __future__ import annotations

from typing import Any

from ..meta_governance.store import ArtifactStore
from .schemas import content_hash
from .types import RegimeSignal


SUPPORTED_REGIME_FAMILIES = {"execution_quality_regime"}


def _build_signal(
    *,
    regime_family: str,
    source_refs: tuple[str, ...],
    supporting_metrics: dict[str, Any],
    prior_regime: str,
    candidate_regime: str,
    confidence_class: str,
    severity: str,
    explanation: str,
) -> RegimeSignal:
    payload = {
        "regime_family": regime_family,
        "source_refs": source_refs,
        "supporting_metrics": supporting_metrics,
        "prior_regime": prior_regime,
        "candidate_regime": candidate_regime,
        "confidence_class": confidence_class,
        "severity": severity,
        "explanation": explanation,
    }
    artifact_hash = content_hash(payload)
    return RegimeSignal(
        regime_signal_id=f"regime-{artifact_hash[:12]}",
        regime_family=regime_family,
        source_refs=source_refs,
        supporting_metrics=supporting_metrics,
        prior_regime=prior_regime,
        candidate_regime=candidate_regime,
        confidence_class=confidence_class,
        severity=severity,
        explanation=explanation,
        artifact_hash=artifact_hash,
    )


def detect_regime_signals(
    store: ArtifactStore,
    *,
    drift_signal_refs: tuple[str, ...] = (),
    reconciliation_result_refs: tuple[str, ...] = (),
    requested_regime_families: tuple[str, ...],
    thresholds: dict[str, Any],
) -> tuple[RegimeSignal, ...]:
    signals: list[RegimeSignal] = []
    drift_signals = [store.read("drift_signals", ref)["record"] for ref in drift_signal_refs]
    reconciliation_results = [store.read("reconciliation_results", ref)["record"] for ref in reconciliation_result_refs]
    for family in sorted(requested_regime_families):
        if family not in SUPPORTED_REGIME_FAMILIES:
            signals.append(
                _build_signal(
                    regime_family=family,
                    source_refs=(),
                    supporting_metrics={"status": "insufficient_evidence"},
                    prior_regime="unknown",
                    candidate_regime="insufficient_evidence",
                    confidence_class="insufficient_evidence",
                    severity="informational",
                    explanation=f"{family} is not supported by tracked runtime evidence surfaces",
                )
            )
            continue
        required_keys = (
            "execution_quality_min_evidence",
            "execution_quality_critical_mismatch_threshold",
            "execution_quality_material_mismatch_threshold",
        )
        missing = [key for key in required_keys if key not in thresholds]
        if missing:
            raise ValueError(f"REGIME_THRESHOLDS_REQUIRED:{family}")
        execution_counts = [
            len(result["execution_mismatches"])
            for result in reconciliation_results
            if result["execution_mismatches"]
        ]
        execution_drift = [
            signal
            for signal in drift_signals
            if signal["signal_family"] == "execution_drift"
        ]
        evidence_count = len(execution_counts) + len(execution_drift)
        if evidence_count < int(thresholds["execution_quality_min_evidence"]):
            signals.append(
                _build_signal(
                    regime_family=family,
                    source_refs=tuple(sorted(drift_signal_refs + reconciliation_result_refs)),
                    supporting_metrics={"evidence_count": evidence_count},
                    prior_regime="stable",
                    candidate_regime="insufficient_evidence",
                    confidence_class="insufficient_evidence",
                    severity="informational",
                    explanation="execution quality regime lacks enough pinned evidence",
                )
            )
            continue
        critical_counts = sum(1 for count in execution_counts if count >= int(thresholds["execution_quality_critical_mismatch_threshold"]))
        material_counts = sum(1 for count in execution_counts if count >= int(thresholds["execution_quality_material_mismatch_threshold"]))
        persistent_execution_drift = any(signal["drift_classification"] == "persistent_systematic" for signal in execution_drift)
        if critical_counts > 0 or persistent_execution_drift:
            candidate_regime = "degraded_execution_quality"
            confidence_class = "high"
            severity = "critical"
        elif material_counts > 0:
            candidate_regime = "stressed_execution_quality"
            confidence_class = "moderate"
            severity = "material"
        else:
            candidate_regime = "stable_execution_quality"
            confidence_class = "moderate"
            severity = "informational"
        signals.append(
            _build_signal(
                regime_family=family,
                source_refs=tuple(sorted(drift_signal_refs + reconciliation_result_refs)),
                supporting_metrics={
                    "evidence_count": evidence_count,
                    "critical_mismatch_windows": critical_counts,
                    "material_mismatch_windows": material_counts,
                    "persistent_execution_drift": persistent_execution_drift,
                },
                prior_regime="stable_execution_quality",
                candidate_regime=candidate_regime,
                confidence_class=confidence_class,
                severity=severity,
                explanation=f"execution quality regime derived from {evidence_count} pinned observations",
            )
        )
    return tuple(signals)
