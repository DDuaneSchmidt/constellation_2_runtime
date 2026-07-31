from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


DEFAULT_WEIGHTS: dict[str, float] = {
    "performance": 0.24,
    "drawdown": 0.18,
    "stability": 0.18,
    "diversification": 0.16,
    "correlation_to_ultrasafe": 0.12,
    "ultrasafe_similarity_score": 0.06,
    "overfit_risk": 0.06,
}

REQUIRED_METRICS = tuple(DEFAULT_WEIGHTS)


@dataclass(frozen=True)
class ChallengerScore:
    candidate_id: str
    score: float
    rank: int | None
    components: dict[str, float]
    weighted_components: dict[str, float]
    penalties: dict[str, float]
    explanation: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "score": self.score,
            "rank": self.rank,
            "components": dict(self.components),
            "weighted_components": dict(self.weighted_components),
            "penalties": dict(self.penalties),
            "explanation": self.explanation,
        }


def _metric(candidate: Mapping[str, Any], name: str) -> float:
    if name not in candidate:
        raise ValueError(f"Missing required candidate metric: {name}")
    try:
        value = float(candidate[name])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Candidate metric {name} must be numeric") from exc
    if value != value:
        raise ValueError(f"Candidate metric {name} must not be NaN")
    return value


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _drawdown_quality(value: float) -> float:
    drawdown = abs(value)
    if drawdown > 1.0:
        drawdown = drawdown / 100.0
    return 1.0 - _clamp(drawdown)


def _low_is_good(value: float) -> float:
    return 1.0 - _clamp(abs(value))


def _components(candidate: Mapping[str, Any]) -> dict[str, float]:
    return {
        "performance": _clamp(_metric(candidate, "performance")),
        "drawdown": _drawdown_quality(_metric(candidate, "drawdown")),
        "stability": _clamp(_metric(candidate, "stability")),
        "diversification": _clamp(_metric(candidate, "diversification")),
        "correlation_to_ultrasafe": _low_is_good(_metric(candidate, "correlation_to_ultrasafe")),
        "ultrasafe_similarity_score": _low_is_good(_metric(candidate, "ultrasafe_similarity_score")),
        "overfit_risk": _low_is_good(_metric(candidate, "overfit_risk")),
    }


def _penalties(candidate: Mapping[str, Any], components: Mapping[str, float]) -> dict[str, float]:
    stability = _metric(candidate, "stability")
    similarity = _metric(candidate, "ultrasafe_similarity_score")
    overfit = _metric(candidate, "overfit_risk")
    correlation = abs(_metric(candidate, "correlation_to_ultrasafe"))
    drawdown_quality = components["drawdown"]

    return {
        "unstable_behavior": 0.08 * max(0.0, 0.45 - stability) / 0.45,
        "high_similarity": 0.07 * max(0.0, similarity - 0.70) / 0.30,
        "high_overfit_risk": 0.08 * max(0.0, overfit - 0.60) / 0.40,
        "high_ultrasafe_correlation": 0.04 * max(0.0, correlation - 0.70) / 0.30,
        "excess_drawdown": 0.06 * max(0.0, 0.45 - drawdown_quality) / 0.45,
    }


def _candidate_id(candidate: Mapping[str, Any]) -> str:
    raw = candidate.get("candidate_id") or candidate.get("id") or candidate.get("label") or "candidate"
    return str(raw)


def _explanation(candidate_id: str, score: float, components: Mapping[str, float], penalties: Mapping[str, float], rank: int | None) -> str:
    strengths = [
        name
        for name in ("performance", "drawdown", "stability", "diversification", "correlation_to_ultrasafe")
        if components[name] >= 0.70
    ]
    weaknesses = [
        name
        for name in ("performance", "drawdown", "stability", "diversification", "correlation_to_ultrasafe")
        if components[name] < 0.45
    ]
    active_penalties = [name for name, value in penalties.items() if value > 0.0]

    rank_text = f"rank {rank}" if rank is not None else "unranked"
    strength_text = ", ".join(strengths) if strengths else "no dominant strength"
    weakness_text = ", ".join(weaknesses) if weaknesses else "no major weak component"
    penalty_text = ", ".join(active_penalties) if active_penalties else "no threshold penalties"
    return (
        f"{candidate_id} is {rank_text} with portfolio discovery score {score:.4f}: "
        f"strengths={strength_text}; weaknesses={weakness_text}; penalties={penalty_text}."
    )


def challenger_score(candidate: Mapping[str, Any], *, weights: Mapping[str, float] | None = None, rank: int | None = None) -> dict[str, Any]:
    """Score one candidate for portfolio diversification value.

    Inputs are normalized 0..1 except drawdown, which accepts either a positive/negative
    fraction or percent. Lower UltraSafe correlation, lower UltraSafe similarity, and
    lower overfit risk improve the score.
    """

    active_weights = dict(DEFAULT_WEIGHTS if weights is None else weights)
    missing_weights = sorted(set(REQUIRED_METRICS) - set(active_weights))
    if missing_weights:
        raise ValueError(f"Missing score weights: {', '.join(missing_weights)}")

    components = _components(candidate)
    weighted = {name: components[name] * float(active_weights[name]) for name in REQUIRED_METRICS}
    penalties = {name: _clamp(value, 0.0, 1.0) for name, value in _penalties(candidate, components).items()}
    score = _clamp(sum(weighted.values()) - sum(penalties.values()))
    candidate_id = _candidate_id(candidate)

    return ChallengerScore(
        candidate_id=candidate_id,
        score=round(score, 6),
        rank=rank,
        components={name: round(value, 6) for name, value in components.items()},
        weighted_components={name: round(value, 6) for name, value in weighted.items()},
        penalties={name: round(value, 6) for name, value in penalties.items()},
        explanation=_explanation(candidate_id, score, components, penalties, rank),
    ).as_dict()


def rank_challengers(candidates: Sequence[Mapping[str, Any]], *, weights: Mapping[str, float] | None = None) -> list[dict[str, Any]]:
    scored = [challenger_score(candidate, weights=weights) for candidate in candidates]
    scored.sort(key=lambda item: (-float(item["score"]), str(item["candidate_id"])))
    ranked: list[dict[str, Any]] = []
    for index, item in enumerate(scored, 1):
        candidate = dict(item)
        candidate["rank"] = index
        candidate["explanation"] = _explanation(
            str(candidate["candidate_id"]),
            float(candidate["score"]),
            candidate["components"],
            candidate["penalties"],
            index,
        )
        ranked.append(candidate)
    return ranked
