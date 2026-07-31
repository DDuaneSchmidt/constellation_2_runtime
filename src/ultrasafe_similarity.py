from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from statistics import mean
from typing import Iterable, Sequence


SIMILARITY_COMPONENTS = (
    "factor_family_overlap",
    "rule_overlap",
    "concept_overlap",
    "sector_similarity",
    "market_cap_similarity",
    "return_behavior_similarity",
    "drawdown_similarity",
)


@dataclass(frozen=True)
class UltraSafeProfile:
    candidate_id: str
    factor_families: tuple[str, ...] = ()
    rules: tuple[str, ...] = ()
    concepts: tuple[str, ...] = ()
    sectors: tuple[str, ...] = ()
    market_cap: float | None = None
    returns: tuple[float, ...] = ()
    drawdowns: tuple[float, ...] = ()


@dataclass(frozen=True)
class SimilarityExplanation:
    component: str
    score: float
    reason: str


@dataclass(frozen=True)
class UltraSafeSimilarityResult:
    candidate_id: str
    reference_id: str
    factor_family_overlap: float
    rule_overlap: float
    concept_overlap: float
    sector_similarity: float
    market_cap_similarity: float
    return_behavior_similarity: float
    drawdown_similarity: float
    ultrasafe_similarity_score: float
    differentiation_classification: str
    explanations: tuple[SimilarityExplanation, ...]

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["explanations"] = [asdict(item) for item in self.explanations]
        return payload


def default_ultrasafe_profile() -> UltraSafeProfile:
    return UltraSafeProfile(
        candidate_id="UltraSafe",
        factor_families=("quality", "low_volatility", "momentum", "risk_control"),
        rules=("monthly_rebalance", "drawdown_guard", "broad_diversification", "position_cap"),
        concepts=("capital_preservation", "compounded_growth", "portfolio_core"),
        sectors=("broad_market", "defensive", "technology", "healthcare", "consumer_staples"),
        market_cap=250_000_000_000.0,
        returns=(0.012, 0.008, -0.018, 0.021, 0.006, -0.011, 0.017, 0.009, 0.004, 0.014, -0.007, 0.016),
        drawdowns=(-0.02, -0.03, -0.08, -0.04, -0.03, -0.07, -0.04, -0.02, -0.02, -0.01, -0.03, -0.01),
    )


def evaluate_ultrasafe_similarity(
    candidate: UltraSafeProfile,
    ultrasafe: UltraSafeProfile | None = None,
) -> UltraSafeSimilarityResult:
    reference = ultrasafe or default_ultrasafe_profile()
    component_scores = {
        "factor_family_overlap": _jaccard(candidate.factor_families, reference.factor_families),
        "rule_overlap": _jaccard(candidate.rules, reference.rules),
        "concept_overlap": _jaccard(candidate.concepts, reference.concepts),
        "sector_similarity": _jaccard(candidate.sectors, reference.sectors),
        "market_cap_similarity": _market_cap_similarity(candidate.market_cap, reference.market_cap),
        "return_behavior_similarity": _return_behavior_similarity(candidate.returns, reference.returns),
        "drawdown_similarity": _drawdown_similarity(candidate.drawdowns, reference.drawdowns),
    }
    similarity_score = _round_score(mean(component_scores.values()))
    return UltraSafeSimilarityResult(
        candidate_id=candidate.candidate_id,
        reference_id=reference.candidate_id,
        factor_family_overlap=component_scores["factor_family_overlap"],
        rule_overlap=component_scores["rule_overlap"],
        concept_overlap=component_scores["concept_overlap"],
        sector_similarity=component_scores["sector_similarity"],
        market_cap_similarity=component_scores["market_cap_similarity"],
        return_behavior_similarity=component_scores["return_behavior_similarity"],
        drawdown_similarity=component_scores["drawdown_similarity"],
        ultrasafe_similarity_score=similarity_score,
        differentiation_classification=classify_similarity_score(similarity_score),
        explanations=_build_explanations(candidate, reference, component_scores),
    )


def classify_similarity_score(score: float) -> str:
    if score <= 0.30:
        return "highly differentiated"
    if score <= 0.60:
        return "partially differentiated"
    return "likely redundant"


def _normalize_tokens(values: Iterable[str]) -> set[str]:
    return {value.strip().casefold().replace(" ", "_") for value in values if value and value.strip()}


def _jaccard(left: Iterable[str], right: Iterable[str]) -> float:
    left_set = _normalize_tokens(left)
    right_set = _normalize_tokens(right)
    if not left_set and not right_set:
        return 1.0
    if not left_set or not right_set:
        return 0.0
    return _round_score(len(left_set & right_set) / len(left_set | right_set))


def _market_cap_similarity(candidate_market_cap: float | None, reference_market_cap: float | None) -> float:
    if candidate_market_cap is None or reference_market_cap is None:
        return 0.0
    if candidate_market_cap <= 0.0 or reference_market_cap <= 0.0:
        return 0.0
    log_gap = abs(math.log10(candidate_market_cap) - math.log10(reference_market_cap))
    return _round_score(max(0.0, 1.0 - (log_gap / 3.0)))


def _return_behavior_similarity(candidate_returns: Sequence[float], reference_returns: Sequence[float]) -> float:
    pairs = _aligned_pairs(candidate_returns, reference_returns)
    if len(pairs) < 2:
        return 0.0
    corr = _pearson([left for left, _ in pairs], [right for _, right in pairs])
    if corr is None:
        return _level_similarity([left for left, _ in pairs], [right for _, right in pairs])
    return _round_score((corr + 1.0) / 2.0)


def _drawdown_similarity(candidate_drawdowns: Sequence[float], reference_drawdowns: Sequence[float]) -> float:
    pairs = _aligned_pairs(candidate_drawdowns, reference_drawdowns)
    if not pairs:
        return 0.0
    gaps = []
    for candidate_value, reference_value in pairs:
        candidate_abs = abs(candidate_value)
        reference_abs = abs(reference_value)
        denominator = max(candidate_abs, reference_abs, 0.01)
        gaps.append(abs(candidate_abs - reference_abs) / denominator)
    return _round_score(max(0.0, 1.0 - mean(gaps)))


def _aligned_pairs(left: Sequence[float], right: Sequence[float]) -> list[tuple[float, float]]:
    return [(float(left_value), float(right_value)) for left_value, right_value in zip(left, right)]


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = mean(left)
    right_mean = mean(right)
    left_centered = [value - left_mean for value in left]
    right_centered = [value - right_mean for value in right]
    left_energy = math.sqrt(sum(value * value for value in left_centered))
    right_energy = math.sqrt(sum(value * value for value in right_centered))
    if left_energy == 0.0 or right_energy == 0.0:
        return None
    return max(-1.0, min(1.0, sum(a * b for a, b in zip(left_centered, right_centered)) / (left_energy * right_energy)))


def _level_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    left_avg = mean(left)
    right_avg = mean(right)
    denominator = max(abs(left_avg), abs(right_avg), 0.01)
    return _round_score(max(0.0, 1.0 - abs(left_avg - right_avg) / denominator))


def _build_explanations(
    candidate: UltraSafeProfile,
    reference: UltraSafeProfile,
    component_scores: dict[str, float],
) -> tuple[SimilarityExplanation, ...]:
    reasons = {
        "factor_family_overlap": _set_reason(candidate.factor_families, reference.factor_families, "factor families"),
        "rule_overlap": _set_reason(candidate.rules, reference.rules, "rules"),
        "concept_overlap": _set_reason(candidate.concepts, reference.concepts, "concepts"),
        "sector_similarity": _set_reason(candidate.sectors, reference.sectors, "sectors"),
        "market_cap_similarity": _market_cap_reason(candidate.market_cap, reference.market_cap),
        "return_behavior_similarity": _series_reason(candidate.returns, reference.returns, "return path"),
        "drawdown_similarity": _series_reason(candidate.drawdowns, reference.drawdowns, "drawdown path"),
    }
    return tuple(
        SimilarityExplanation(component=component, score=component_scores[component], reason=reasons[component])
        for component in SIMILARITY_COMPONENTS
    )


def _set_reason(candidate_values: Iterable[str], reference_values: Iterable[str], label: str) -> str:
    candidate_set = _normalize_tokens(candidate_values)
    reference_set = _normalize_tokens(reference_values)
    shared = sorted(candidate_set & reference_set)
    candidate_only = sorted(candidate_set - reference_set)
    if shared:
        shared_text = ", ".join(shared)
        if candidate_only:
            return f"Shares {label}: {shared_text}; candidate-only {label}: {', '.join(candidate_only)}."
        return f"Shares {label}: {shared_text}."
    if candidate_only:
        return f"No shared {label}; candidate-only {label}: {', '.join(candidate_only)}."
    return f"No candidate {label} supplied."


def _market_cap_reason(candidate_market_cap: float | None, reference_market_cap: float | None) -> str:
    if candidate_market_cap is None or reference_market_cap is None:
        return "Market cap similarity is unavailable because one side is missing market cap."
    if candidate_market_cap <= 0.0 or reference_market_cap <= 0.0:
        return "Market cap similarity is unavailable because one side has non-positive market cap."
    ratio = candidate_market_cap / reference_market_cap
    return f"Candidate market cap is {ratio:.2f}x the UltraSafe reference market cap."


def _series_reason(candidate_values: Sequence[float], reference_values: Sequence[float], label: str) -> str:
    pair_count = min(len(candidate_values), len(reference_values))
    if pair_count == 0:
        return f"No overlapping {label} observations supplied."
    return f"Compared {pair_count} aligned {label} observation(s)."


def _round_score(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 6)
