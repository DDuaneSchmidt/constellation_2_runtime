from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError
from ops.atlas.v2_learning_estimator import _correlation

LABEL_INDEPENDENCE_STATUSES = ("INDEPENDENT", "PARTIALLY_SHARED", "HIGHLY_SHARED", "CIRCULAR")
DEFAULT_CREATED_AT = "2026-06-04T00:00:00Z"
DEFAULT_REPORT_ID = "atlas-v2-label-integrity-report-v1"


@dataclass(frozen=True)
class LabelPair:
    record_id: str
    expected_value: float
    actual_value: float
    expected_source_fields: tuple[str, ...]
    actual_source_fields: tuple[str, ...]
    expected_provenance: tuple[str, ...] = ()
    actual_provenance: tuple[str, ...] = ()


class AtlasV2LabelIntegrityAuditor:
    def __init__(self, ledger: AtlasV2Ledger | None = None) -> None:
        self.ledger = ledger

    def build_report(
        self,
        *,
        dataset_name: str = "atlas_v2_learning_estimate_evaluations",
        report_id: str = DEFAULT_REPORT_ID,
        created_at: str = DEFAULT_CREATED_AT,
        label_pairs: list[LabelPair] | None = None,
        append: bool = True,
    ) -> dict[str, Any]:
        pairs = label_pairs if label_pairs is not None else self.label_pairs_from_ledger()
        payload = self.report_payload(
            pairs,
            dataset_name=dataset_name,
            report_id=report_id,
            created_at=created_at,
        )
        if append:
            if self.ledger is None:
                raise AtlasV2ValidationError("append requires AtlasV2Ledger")
            return self.ledger.create_record(
                "LabelIntegrityReport",
                payload,
                reason="learning label integrity audited without authority expansion",
                triggering_object="LearningEstimateEvaluation:*",
            )
        return {"object_type": "LabelIntegrityReport", **payload}

    def label_pairs_from_ledger(self) -> list[LabelPair]:
        if self.ledger is None:
            raise AtlasV2ValidationError("ledger is required to derive label pairs")
        estimates = {record["estimate_id"]: record for record in self.ledger.records("LearningEstimate")}
        pairs: list[LabelPair] = []
        for evaluation in self.ledger.records("LearningEstimateEvaluation"):
            estimate = estimates.get(str(evaluation.get("estimate_id") or ""))
            if estimate is None:
                continue
            expected_fields, expected_provenance = self._expected_label_provenance(estimate)
            actual_fields, actual_provenance = self._actual_label_provenance(evaluation)
            pairs.append(
                LabelPair(
                    record_id=str(evaluation["evaluation_id"]),
                    expected_value=float(evaluation.get("expected_learning_value", estimate.get("expected_learning_value", 0.0))),
                    actual_value=float(evaluation["actual_learning_value"]),
                    expected_source_fields=tuple(expected_fields),
                    actual_source_fields=tuple(actual_fields),
                    expected_provenance=tuple(expected_provenance),
                    actual_provenance=tuple(actual_provenance),
                )
            )
        return pairs

    def report_payload(self, pairs: list[LabelPair], *, dataset_name: str, report_id: str, created_at: str) -> dict[str, Any]:
        expected_values = [pair.expected_value for pair in pairs]
        actual_values = [pair.actual_value for pair in pairs]
        expected_fields = sorted({field for pair in pairs for field in pair.expected_source_fields})
        actual_fields = sorted({field for pair in pairs for field in pair.actual_source_fields})
        shared_fields = sorted(set(expected_fields) & set(actual_fields))
        expected_provenance = [set(pair.expected_provenance) for pair in pairs]
        actual_provenance = [set(pair.actual_provenance) for pair in pairs]
        shared_provenance_count = sum(1 for left, right in zip(expected_provenance, actual_provenance) if left and right and left & right)
        direct_copy_count = sum(1 for pair in pairs if _same_number(pair.expected_value, pair.actual_value))
        deterministic_count = _deterministic_transform_count(expected_values, actual_values)
        correlation = _correlation(expected_values, actual_values)
        correlation_meaningless = correlation is None and len(pairs) >= 2 and (_low_variance(expected_values) or _low_variance(actual_values))
        expected_entropy = _entropy(expected_values)
        actual_entropy = _entropy(actual_values)
        expected_distinct = len({_round_label(value) for value in expected_values})
        actual_distinct = len({_round_label(value) for value in actual_values})
        label_overlap_ratio = _overlap_ratio(expected_fields, actual_fields)
        shared_provenance_ratio = round(shared_provenance_count / len(pairs), 6) if pairs else 0.0
        direct_copy_ratio = round(direct_copy_count / len(pairs), 6) if pairs else 0.0
        deterministic_ratio = round(deterministic_count / len(pairs), 6) if pairs else 0.0
        perfect_corr = correlation is not None and abs(abs(correlation) - 1.0) <= 1e-6
        low_variance = _low_variance(expected_values) or _low_variance(actual_values)
        homogeneous = _homogeneous(expected_values) or _homogeneous(actual_values)
        circularity_score = _circularity_score(
            label_overlap_ratio=label_overlap_ratio,
            shared_provenance_ratio=shared_provenance_ratio,
            direct_copy_ratio=direct_copy_ratio,
            deterministic_ratio=deterministic_ratio,
            perfect_correlation=perfect_corr,
            low_variance=low_variance,
            homogeneous=homogeneous,
        )
        warnings = _warnings(
            pairs=pairs,
            shared_fields=shared_fields,
            direct_copy_ratio=direct_copy_ratio,
            deterministic_ratio=deterministic_ratio,
            perfect_correlation=perfect_corr,
            low_variance=low_variance,
            homogeneous=homogeneous,
            shared_provenance_ratio=shared_provenance_ratio,
            correlation_meaningless=correlation_meaningless,
            expected_distinct=expected_distinct,
            actual_distinct=actual_distinct,
        )
        status = _status(circularity_score, shared_fields, direct_copy_ratio, perfect_corr, shared_provenance_ratio)
        return {
            "report_id": report_id,
            "created_at": created_at,
            "dataset_name": dataset_name,
            "source_ledger_root": self.ledger.root.resolve().as_posix() if self.ledger is not None else "label_pairs",
            "record_count": len(pairs),
            "expected_label_source_fields": expected_fields,
            "actual_label_source_fields": actual_fields,
            "shared_source_fields": shared_fields,
            "circularity_score": circularity_score,
            "correlation_expected_actual": correlation if correlation is not None else 0.0,
            "label_independence_status": status,
            "warnings": warnings,
            "recommendations": _recommendations(status, warnings),
            "expected_distinct_values": expected_distinct,
            "actual_distinct_values": actual_distinct,
            "expected_entropy": expected_entropy,
            "actual_entropy": actual_entropy,
            "label_overlap_ratio": label_overlap_ratio,
            "shared_provenance_ratio": shared_provenance_ratio,
            "authority_boundary_acknowledged": True,
        }

    def _expected_label_provenance(self, estimate: dict[str, Any]) -> tuple[list[str], list[str]]:
        fields = ["LearningEstimate.expected_learning_value"]
        provenance = [f"LearningEstimate:{estimate['estimate_id']}:expected_learning_value"]
        source_type = str(estimate.get("source_object_type") or "")
        source_id = str(estimate.get("source_object_id") or "")
        if source_type == "ExperienceEvent":
            try:
                event = self.ledger.latest("ExperienceEvent", source_id) if self.ledger else None
                if event:
                    decision = self.ledger.latest("AttentionDecision", str(event["source_decision_id"]))
                    fields.append("AttentionDecision.expected_learning_value")
                    provenance.append(f"AttentionDecision:{decision['decision_id']}:expected_learning_value")
                    if _same_number(float(decision.get("expected_learning_value", 0.0)), float(event.get("experience_quality_score", -1.0))):
                        fields.append("ExperienceEvent.experience_quality_score")
                        provenance.append(f"ExperienceEvent:{event['experience_id']}:experience_quality_score")
                    historical_record_id = decision.get("historical_record_id") or event.get("historical_record_id")
                    if historical_record_id:
                        record = self.ledger.latest("HistoricalExperienceRecord", str(historical_record_id))
                        if record.get("expected_learning_source_fields"):
                            fields.extend(f"HistoricalExperienceRecord.{field}" for field in record["expected_learning_source_fields"])
                        fields.append("HistoricalExperienceRecord.expected_learning_value_pre_outcome")
                        provenance.append(f"HistoricalExperienceRecord:{historical_record_id}:expected_learning_value_pre_outcome")
            except AtlasV2ValidationError:
                pass
        return fields, provenance

    def _actual_label_provenance(self, evaluation: dict[str, Any]) -> tuple[list[str], list[str]]:
        fields = ["LearningEstimateEvaluation.actual_learning_value"]
        provenance = [f"LearningEstimateEvaluation:{evaluation['evaluation_id']}:actual_learning_value"]
        source_type = str(evaluation.get("source_object_type") or "")
        source_id = str(evaluation.get("source_object_id") or "")
        if source_type == "ExperienceEvent":
            try:
                event = self.ledger.latest("ExperienceEvent", source_id) if self.ledger else None
                if event:
                    if "actual_learning_value_post_outcome" in event:
                        fields.append("ExperienceEvent.actual_learning_value_post_outcome")
                        provenance.append(f"ExperienceEvent:{event['experience_id']}:actual_learning_value_post_outcome")
                    else:
                        fields.append("ExperienceEvent.experience_quality_score")
                        provenance.append(f"ExperienceEvent:{event['experience_id']}:experience_quality_score")
                    historical_record_id = event.get("historical_record_id")
                    if historical_record_id:
                        record = self.ledger.latest("HistoricalExperienceRecord", str(historical_record_id))
                        if record.get("actual_learning_source_fields"):
                            fields.extend(f"HistoricalExperienceRecord.{field}" for field in record["actual_learning_source_fields"])
                        fields.append("HistoricalExperienceRecord.actual_learning_value_post_outcome")
                        provenance.append(f"HistoricalExperienceRecord:{historical_record_id}:actual_learning_value_post_outcome")
            except AtlasV2ValidationError:
                pass
        return fields, provenance


def _same_number(left: float, right: float) -> bool:
    return abs(left - right) <= 1e-9


def _round_label(value: float) -> float:
    return round(float(value), 6)


def _entropy(values: list[float]) -> float:
    if not values:
        return 0.0
    counts = Counter(_round_label(value) for value in values)
    total = len(values)
    return round(-sum((count / total) * math.log2(count / total) for count in counts.values()), 6)


def _overlap_ratio(expected_fields: list[str], actual_fields: list[str]) -> float:
    if not expected_fields or not actual_fields:
        return 0.0
    return round(len(set(expected_fields) & set(actual_fields)) / min(len(set(expected_fields)), len(set(actual_fields))), 6)


def _deterministic_transform_count(expected_values: list[float], actual_values: list[float]) -> int:
    if len(expected_values) != len(actual_values) or not expected_values:
        return 0
    mapping: dict[float, set[float]] = defaultdict(set)
    for expected, actual in zip(expected_values, actual_values):
        mapping[_round_label(expected)].add(_round_label(actual))
    if len(mapping) == len(expected_values):
        return 0
    if all(len(actuals) == 1 for actuals in mapping.values()):
        return len(expected_values)
    return 0


def _low_variance(values: list[float]) -> bool:
    if len(values) < 3:
        return False
    distinct = len({_round_label(value) for value in values})
    if distinct <= 2:
        return True
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return variance < 1e-6


def _homogeneous(values: list[float]) -> bool:
    if len(values) < 3:
        return False
    counts = Counter(_round_label(value) for value in values)
    return max(counts.values()) / len(values) >= 0.9


def _circularity_score(*, label_overlap_ratio: float, shared_provenance_ratio: float, direct_copy_ratio: float, deterministic_ratio: float, perfect_correlation: bool, low_variance: bool, homogeneous: bool) -> float:
    score = (
        label_overlap_ratio * 0.35
        + shared_provenance_ratio * 0.2
        + direct_copy_ratio * 0.25
        + deterministic_ratio * 0.1
        + (0.05 if perfect_correlation else 0.0)
        + (0.025 if low_variance else 0.0)
        + (0.025 if homogeneous else 0.0)
    )
    return round(min(1.0, score), 6)


def _warnings(*, pairs: list[LabelPair], shared_fields: list[str], direct_copy_ratio: float, deterministic_ratio: float, perfect_correlation: bool, low_variance: bool, homogeneous: bool, shared_provenance_ratio: float, correlation_meaningless: bool, expected_distinct: int, actual_distinct: int) -> list[str]:
    warnings: list[str] = []
    if not pairs:
        warnings.append("no label pairs available for integrity audit")
    if shared_fields:
        warnings.append("expected and actual labels share source fields")
    if direct_copy_ratio >= 0.5:
        warnings.append("expected and actual labels appear to be direct copies for most records")
    if deterministic_ratio >= 0.8:
        warnings.append("actual labels appear to be deterministic transformations of expected labels")
    if perfect_correlation:
        warnings.append("perfect correlation detected between expected and actual labels")
    if low_variance:
        warnings.append("low label variance detected")
    if expected_distinct <= 2 or actual_distinct <= 2:
        warnings.append("low distinct-value count detected")
    if correlation_meaningless:
        warnings.append("correlation is not meaningful because labels are constant or near-constant")
    if homogeneous:
        warnings.append("homogeneous label distribution detected")
    if shared_provenance_ratio >= 0.5:
        warnings.append("duplicated label provenance detected")
    return warnings


def _status(circularity_score: float, shared_fields: list[str], direct_copy_ratio: float, perfect_correlation: bool, shared_provenance_ratio: float) -> str:
    if circularity_score >= 0.85 or (shared_fields and direct_copy_ratio >= 0.8 and perfect_correlation):
        return "CIRCULAR"
    if circularity_score >= 0.5 or shared_provenance_ratio >= 0.75 or (shared_fields and circularity_score >= 0.35):
        return "HIGHLY_SHARED"
    if circularity_score >= 0.2 or shared_fields:
        return "PARTIALLY_SHARED"
    return "INDEPENDENT"


def _recommendations(status: str, warnings: list[str]) -> list[str]:
    if status == "INDEPENDENT":
        return ["Learning correlation may be interpreted, subject to sample size and domain checks."]
    recommendations = [
        "Do not interpret correlation_expected_actual as predictive learning strength until labels are independently sourced.",
        "Separate expected label construction from actual outcome measurement and preserve field-level provenance for both labels.",
    ]
    if any("variance" in warning or "homogeneous" in warning for warning in warnings):
        recommendations.append("Increase label diversity before treating aggregate learning metrics as stable.")
    if any("provenance" in warning or "source fields" in warning for warning in warnings):
        recommendations.append("Use actual outcome fields that are not derived from experience_quality_score or the paired expected label source.")
    return recommendations


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit Atlas V2 learning label independence.")
    parser.add_argument("--ledger-root", type=Path, required=True)
    parser.add_argument("--dataset-name", default="atlas_v2_learning_estimate_evaluations")
    parser.add_argument("--report-id", default=DEFAULT_REPORT_ID)
    parser.add_argument("--created-at", default=DEFAULT_CREATED_AT)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    auditor = AtlasV2LabelIntegrityAuditor(AtlasV2Ledger(args.ledger_root))
    report = auditor.build_report(
        dataset_name=args.dataset_name,
        report_id=args.report_id,
        created_at=args.created_at,
        append=not args.dry_run,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
