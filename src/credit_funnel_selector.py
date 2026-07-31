from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping


DEFAULT_SIMILARITY_THRESHOLD = 0.82
DEFAULT_MIN_DISCOVERY_SCORE = 0.0
DEFAULT_MAX_CANDIDATES = 10
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "by",
    "for",
    "from",
    "in",
    "is",
    "of",
    "on",
    "or",
    "the",
    "to",
    "when",
    "with",
}


@dataclass(frozen=True)
class CreditFunnelCandidate:
    candidate_id: str
    title: str = ""
    thesis: str = ""
    mechanism: str = ""
    universe: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    novelty_score: float | None = None
    economic_logic_score: float | None = None
    diversification_score: float | None = None
    non_redundancy_score: float | None = None
    complexity_penalty: float | None = None
    overfit_risk_penalty: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DiscoveryScore:
    novelty_score: float
    economic_logic_score: float
    diversification_score: float
    non_redundancy_score: float
    complexity_penalty: float
    overfit_risk_penalty: float
    discovery_score: float

    def to_dict(self) -> dict[str, float]:
        return asdict(self)


@dataclass(frozen=True)
class RankedCandidate:
    candidate: CreditFunnelCandidate
    score: DiscoveryScore
    rank: int
    status: str
    selection_reason: str

    @property
    def candidate_id(self) -> str:
        return self.candidate.candidate_id

    @property
    def discovery_score(self) -> float:
        return self.score.discovery_score

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate": asdict(self.candidate),
            "score": self.score.to_dict(),
            "rank": self.rank,
            "status": self.status,
            "selection_reason": self.selection_reason,
        }


@dataclass(frozen=True)
class RejectedCandidate:
    candidate: CreditFunnelCandidate
    score: DiscoveryScore
    rejection_reason: str
    rejection_stage: str
    failed_criteria: tuple[str, ...]
    similar_to_candidate_id: str | None = None
    similarity_score: float | None = None

    @property
    def candidate_id(self) -> str:
        return self.candidate.candidate_id

    def to_graveyard_record(self) -> dict[str, Any]:
        return {
            "archive": "Candidate Graveyard",
            "candidate_id": self.candidate.candidate_id,
            "candidate_title": self.candidate.title,
            "rejection_reason": self.rejection_reason,
            "rejection_stage": self.rejection_stage,
            "failed_criteria": list(self.failed_criteria),
            "discovery_score": self.score.to_dict(),
            "similar_to_candidate_id": self.similar_to_candidate_id,
            "similarity_score": self.similarity_score,
            "lessons_learned": explain_rejection(self),
            "reopening_conditions": "Reopen only with materially new thesis, evidence, mechanism, or lower-risk specification.",
        }


@dataclass(frozen=True)
class CreditFunnelResult:
    ranked_candidates: tuple[RankedCandidate, ...]
    selected_candidates: tuple[RankedCandidate, ...]
    rejected_candidates: tuple[RejectedCandidate, ...]
    candidate_graveyard: tuple[Mapping[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ranked_candidates": [row.to_dict() for row in self.ranked_candidates],
            "selected_candidates": [row.to_dict() for row in self.selected_candidates],
            "rejected_candidates": [asdict(row) for row in self.rejected_candidates],
            "candidate_graveyard": [dict(row) for row in self.candidate_graveyard],
        }


def rank_candidates(
    candidates: Iterable[CreditFunnelCandidate | Mapping[str, Any]],
    *,
    existing_candidates: Iterable[CreditFunnelCandidate | Mapping[str, Any]] = (),
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
) -> CreditFunnelResult:
    """Score, deduplicate, similarity-filter, and rank candidates before paid testing."""

    incoming = [_coerce_candidate(row) for row in candidates]
    existing = [_coerce_candidate(row) for row in existing_candidates]
    scored = [(candidate, _score_candidate(candidate, existing)) for candidate in incoming]
    survivors, rejected = _remove_exact_duplicates(scored)
    survivors, similarity_rejections = _remove_highly_similar(survivors, threshold=similarity_threshold)
    rejected.extend(similarity_rejections)

    ordered = sorted(
        survivors,
        key=lambda item: (
            -item[1].discovery_score,
            -item[1].economic_logic_score,
            -item[1].novelty_score,
            item[0].candidate_id,
        ),
    )
    ranked = tuple(
        RankedCandidate(
            candidate=candidate,
            score=score,
            rank=index,
            status="RANKED_FOR_TESTING_REVIEW",
            selection_reason=_selection_reason(score),
        )
        for index, (candidate, score) in enumerate(ordered, 1)
    )
    graveyard = tuple(row.to_graveyard_record() for row in rejected)
    return CreditFunnelResult(
        ranked_candidates=ranked,
        selected_candidates=(),
        rejected_candidates=tuple(rejected),
        candidate_graveyard=graveyard,
    )


def select_candidates_for_testing(
    candidates: Iterable[CreditFunnelCandidate | Mapping[str, Any]],
    *,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    min_discovery_score: float = DEFAULT_MIN_DISCOVERY_SCORE,
    existing_candidates: Iterable[CreditFunnelCandidate | Mapping[str, Any]] = (),
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
) -> CreditFunnelResult:
    """Return only candidates worth spending Portfolio123 credits on."""

    if max_candidates < 0:
        raise ValueError("max_candidates must be non-negative")

    ranked_result = rank_candidates(
        candidates,
        existing_candidates=existing_candidates,
        similarity_threshold=similarity_threshold,
    )
    selected: list[RankedCandidate] = []
    rejected = list(ranked_result.rejected_candidates)

    for row in ranked_result.ranked_candidates:
        if row.discovery_score < min_discovery_score:
            rejected.append(
                _reject_ranked(
                    row,
                    reason="DISCOVERY_SCORE_BELOW_TESTING_FLOOR",
                    stage="CREDIT_FUNNEL_SCORE_FLOOR",
                    failed=("discovery_score",),
                )
            )
        elif len(selected) >= max_candidates:
            rejected.append(
                _reject_ranked(
                    row,
                    reason="NOT_IN_TOP_CREDIT_BUDGET",
                    stage="CREDIT_FUNNEL_CAPACITY_CUTOFF",
                    failed=("portfolio123_credit_budget",),
                )
            )
        else:
            selected.append(
                RankedCandidate(
                    candidate=row.candidate,
                    score=row.score,
                    rank=len(selected) + 1,
                    status="SELECTED_FOR_TESTING",
                    selection_reason=explain_selection(row),
                )
            )

    graveyard = tuple(row.to_graveyard_record() for row in rejected)
    return CreditFunnelResult(
        ranked_candidates=ranked_result.ranked_candidates,
        selected_candidates=tuple(selected),
        rejected_candidates=tuple(rejected),
        candidate_graveyard=graveyard,
    )


def explain_selection(candidate: RankedCandidate | Mapping[str, Any]) -> str:
    row = _coerce_ranked(candidate)
    score = row.score
    return (
        f"{row.candidate.candidate_id} advances because discovery_score={score.discovery_score:.3f} "
        f"with novelty={score.novelty_score:.3f}, economic_logic={score.economic_logic_score:.3f}, "
        f"diversification={score.diversification_score:.3f}, non_redundancy={score.non_redundancy_score:.3f}; "
        f"complexity_penalty={score.complexity_penalty:.3f} and overfit_risk_penalty={score.overfit_risk_penalty:.3f} "
        "remain within the internal pre-testing filter."
    )


def explain_rejection(candidate: RejectedCandidate | Mapping[str, Any]) -> str:
    row = _coerce_rejected(candidate)
    detail = (
        f"{row.candidate.candidate_id} rejected at {row.rejection_stage}: {row.rejection_reason}. "
        f"Failed criteria: {', '.join(row.failed_criteria)}. "
        f"discovery_score={row.score.discovery_score:.3f}."
    )
    if row.similar_to_candidate_id:
        detail += f" Similar to {row.similar_to_candidate_id} at similarity={row.similarity_score:.3f}."
    return detail


def discovery_score(candidate: CreditFunnelCandidate | Mapping[str, Any]) -> DiscoveryScore:
    return _score_candidate(_coerce_candidate(candidate), [])


def _score_candidate(candidate: CreditFunnelCandidate, existing: list[CreditFunnelCandidate]) -> DiscoveryScore:
    novelty = _component(candidate.novelty_score, _heuristic_novelty(candidate, existing))
    economic = _component(candidate.economic_logic_score, _heuristic_economic_logic(candidate))
    diversification = _component(candidate.diversification_score, _heuristic_diversification(candidate))
    non_redundancy = _component(candidate.non_redundancy_score, _heuristic_non_redundancy(candidate, existing))
    complexity = _component(candidate.complexity_penalty, _heuristic_complexity_penalty(candidate))
    overfit = _component(candidate.overfit_risk_penalty, _heuristic_overfit_penalty(candidate))
    total = round(novelty + economic + diversification + non_redundancy - complexity - overfit, 6)
    return DiscoveryScore(
        novelty_score=novelty,
        economic_logic_score=economic,
        diversification_score=diversification,
        non_redundancy_score=non_redundancy,
        complexity_penalty=complexity,
        overfit_risk_penalty=overfit,
        discovery_score=total,
    )


def _remove_exact_duplicates(
    scored: list[tuple[CreditFunnelCandidate, DiscoveryScore]],
) -> tuple[list[tuple[CreditFunnelCandidate, DiscoveryScore]], list[RejectedCandidate]]:
    survivors: dict[str, tuple[CreditFunnelCandidate, DiscoveryScore]] = {}
    rejected: list[RejectedCandidate] = []
    for candidate, score in scored:
        fingerprint = _fingerprint(candidate)
        current = survivors.get(fingerprint)
        if current is None:
            survivors[fingerprint] = (candidate, score)
            continue
        kept_candidate, kept_score = current
        if _is_better(candidate, score, kept_candidate, kept_score):
            rejected.append(
                _reject(
                    kept_candidate,
                    kept_score,
                    reason="DUPLICATE_CANDIDATE_LOWER_SCORE",
                    stage="CREDIT_FUNNEL_DUPLICATE_FILTER",
                    failed=("duplicate_candidate",),
                    similar_to=candidate.candidate_id,
                    similarity=1.0,
                )
            )
            survivors[fingerprint] = (candidate, score)
        else:
            rejected.append(
                _reject(
                    candidate,
                    score,
                    reason="DUPLICATE_CANDIDATE_LOWER_SCORE",
                    stage="CREDIT_FUNNEL_DUPLICATE_FILTER",
                    failed=("duplicate_candidate",),
                    similar_to=kept_candidate.candidate_id,
                    similarity=1.0,
                )
            )
    return list(survivors.values()), rejected


def _remove_highly_similar(
    scored: list[tuple[CreditFunnelCandidate, DiscoveryScore]],
    *,
    threshold: float,
) -> tuple[list[tuple[CreditFunnelCandidate, DiscoveryScore]], list[RejectedCandidate]]:
    ordered = sorted(scored, key=lambda item: (-item[1].discovery_score, item[0].candidate_id))
    survivors: list[tuple[CreditFunnelCandidate, DiscoveryScore]] = []
    rejected: list[RejectedCandidate] = []
    for candidate, score in ordered:
        closest = _closest_survivor(candidate, survivors)
        if closest and closest[2] >= threshold:
            kept, _kept_score, similarity = closest
            rejected.append(
                _reject(
                    candidate,
                    score,
                    reason="HIGHLY_SIMILAR_TO_HIGHER_RANKED_CANDIDATE",
                    stage="CREDIT_FUNNEL_SIMILARITY_FILTER",
                    failed=("non_redundancy_score", "similarity_threshold"),
                    similar_to=kept.candidate_id,
                    similarity=similarity,
                )
            )
        else:
            survivors.append((candidate, score))
    return survivors, rejected


def _coerce_candidate(row: CreditFunnelCandidate | Mapping[str, Any]) -> CreditFunnelCandidate:
    if isinstance(row, CreditFunnelCandidate):
        return row
    if not isinstance(row, Mapping):
        raise TypeError("candidate must be a CreditFunnelCandidate or mapping")
    candidate_id = str(row.get("candidate_id") or row.get("id") or "").strip()
    if not candidate_id:
        raise ValueError("candidate_id is required")
    return CreditFunnelCandidate(
        candidate_id=candidate_id,
        title=str(row.get("title") or row.get("name") or ""),
        thesis=str(row.get("thesis") or row.get("description") or row.get("logic") or ""),
        mechanism=str(row.get("mechanism") or row.get("strategy_type") or ""),
        universe=_tuple_text(row.get("universe") or row.get("symbols") or ()),
        tags=_tuple_text(row.get("tags") or row.get("features") or ()),
        novelty_score=_maybe_float(row.get("novelty_score")),
        economic_logic_score=_maybe_float(row.get("economic_logic_score")),
        diversification_score=_maybe_float(row.get("diversification_score")),
        non_redundancy_score=_maybe_float(row.get("non_redundancy_score")),
        complexity_penalty=_maybe_float(row.get("complexity_penalty")),
        overfit_risk_penalty=_maybe_float(row.get("overfit_risk_penalty")),
        metadata=dict(row.get("metadata") or {}),
    )


def _coerce_ranked(row: RankedCandidate | Mapping[str, Any]) -> RankedCandidate:
    if isinstance(row, RankedCandidate):
        return row
    if not isinstance(row, Mapping):
        raise TypeError("ranked candidate must be a RankedCandidate or mapping")
    candidate = _coerce_candidate(row.get("candidate", row))
    score = _coerce_score(row.get("score") or row)
    return RankedCandidate(
        candidate=candidate,
        score=score,
        rank=int(row.get("rank") or 0),
        status=str(row.get("status") or ""),
        selection_reason=str(row.get("selection_reason") or ""),
    )


def _coerce_rejected(row: RejectedCandidate | Mapping[str, Any]) -> RejectedCandidate:
    if isinstance(row, RejectedCandidate):
        return row
    if not isinstance(row, Mapping):
        raise TypeError("rejected candidate must be a RejectedCandidate or mapping")
    return RejectedCandidate(
        candidate=_coerce_candidate(row.get("candidate", row)),
        score=_coerce_score(row.get("score") or row.get("discovery_score") or row),
        rejection_reason=str(row.get("rejection_reason") or "UNKNOWN_REJECTION"),
        rejection_stage=str(row.get("rejection_stage") or "UNKNOWN_STAGE"),
        failed_criteria=tuple(str(item) for item in row.get("failed_criteria", ())),
        similar_to_candidate_id=row.get("similar_to_candidate_id"),
        similarity_score=_maybe_float(row.get("similarity_score")),
    )


def _coerce_score(row: Mapping[str, Any]) -> DiscoveryScore:
    return DiscoveryScore(
        novelty_score=float(row["novelty_score"]),
        economic_logic_score=float(row["economic_logic_score"]),
        diversification_score=float(row["diversification_score"]),
        non_redundancy_score=float(row["non_redundancy_score"]),
        complexity_penalty=float(row["complexity_penalty"]),
        overfit_risk_penalty=float(row["overfit_risk_penalty"]),
        discovery_score=float(row["discovery_score"]),
    )


def _tuple_text(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(token for token in (part.strip() for part in value.split(",")) if token)
    return tuple(str(part).strip() for part in value if str(part).strip())


def _maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _component(value: float | None, fallback: float) -> float:
    return round(max(0.0, min(1.0, fallback if value is None else value)), 6)


def _heuristic_novelty(candidate: CreditFunnelCandidate, existing: list[CreditFunnelCandidate]) -> float:
    if not existing:
        return 0.7
    max_similarity = max((_similarity(candidate, row) for row in existing), default=0.0)
    return 1.0 - max_similarity


def _heuristic_economic_logic(candidate: CreditFunnelCandidate) -> float:
    text = f"{candidate.title} {candidate.thesis}".lower()
    terms = ("spread", "valuation", "earnings", "rates", "credit", "liquidity", "momentum", "risk", "cash flow", "macro")
    hits = sum(1 for term in terms if term in text)
    return min(1.0, 0.35 + hits * 0.12)


def _heuristic_diversification(candidate: CreditFunnelCandidate) -> float:
    breadth = len(set(candidate.universe)) + len(set(candidate.tags)) + (1 if candidate.mechanism else 0)
    return min(1.0, 0.25 + breadth * 0.1)


def _heuristic_non_redundancy(candidate: CreditFunnelCandidate, existing: list[CreditFunnelCandidate]) -> float:
    if not existing:
        return 0.75
    return 1.0 - max((_similarity(candidate, row) for row in existing), default=0.0)


def _heuristic_complexity_penalty(candidate: CreditFunnelCandidate) -> float:
    tags = {tag.lower() for tag in candidate.tags}
    text = candidate.thesis.lower()
    knobs = len(re.findall(r"\b\d+(?:\.\d+)?\b", text))
    complexity_terms = {"optimized", "ensemble", "multi-factor", "multifactor", "parameter", "threshold"}
    return min(1.0, 0.08 * knobs + 0.12 * len(tags & complexity_terms))


def _heuristic_overfit_penalty(candidate: CreditFunnelCandidate) -> float:
    text = f"{candidate.title} {candidate.thesis} {' '.join(candidate.tags)}".lower()
    terms = ("optimized", "best", "curve", "in-sample", "lookback", "threshold", "tuned", "perfect")
    return min(1.0, sum(0.12 for term in terms if term in text))


def _fingerprint(candidate: CreditFunnelCandidate) -> str:
    return "|".join(
        (
            _normalize(candidate.title),
            _normalize(candidate.thesis),
            _normalize(candidate.mechanism),
            ",".join(sorted(_normalize(item) for item in candidate.universe)),
        )
    )


def _similarity(left: CreditFunnelCandidate, right: CreditFunnelCandidate) -> float:
    text_overlap = _overlap(_tokens(f"{left.title} {left.thesis}"), _tokens(f"{right.title} {right.thesis}"))
    mechanism_overlap = 1.0 if _normalize(left.mechanism) and _normalize(left.mechanism) == _normalize(right.mechanism) else 0.0
    universe_overlap = _overlap(set(map(_normalize, left.universe)), set(map(_normalize, right.universe)))
    tag_overlap = _overlap(set(map(_normalize, left.tags)), set(map(_normalize, right.tags)))
    return round(0.55 * text_overlap + 0.20 * mechanism_overlap + 0.15 * universe_overlap + 0.10 * tag_overlap, 6)


def _closest_survivor(
    candidate: CreditFunnelCandidate,
    survivors: list[tuple[CreditFunnelCandidate, DiscoveryScore]],
) -> tuple[CreditFunnelCandidate, DiscoveryScore, float] | None:
    if not survivors:
        return None
    scored = [(kept, score, _similarity(candidate, kept)) for kept, score in survivors]
    return max(scored, key=lambda row: (row[2], row[1].discovery_score, row[0].candidate_id))


def _tokens(text: str) -> set[str]:
    return {token for token in re.split(r"\W+", text.lower()) if token and token not in STOPWORDS}


def _normalize(text: str) -> str:
    return " ".join(sorted(_tokens(text)))


def _overlap(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _is_better(
    candidate: CreditFunnelCandidate,
    score: DiscoveryScore,
    kept_candidate: CreditFunnelCandidate,
    kept_score: DiscoveryScore,
) -> bool:
    return (score.discovery_score, kept_candidate.candidate_id) > (kept_score.discovery_score, candidate.candidate_id)


def _selection_reason(score: DiscoveryScore) -> str:
    return (
        f"discovery_score={score.discovery_score:.3f}; "
        "candidate survived exact duplicate and high-similarity filters."
    )


def _reject(
    candidate: CreditFunnelCandidate,
    score: DiscoveryScore,
    *,
    reason: str,
    stage: str,
    failed: tuple[str, ...],
    similar_to: str | None = None,
    similarity: float | None = None,
) -> RejectedCandidate:
    return RejectedCandidate(
        candidate=candidate,
        score=score,
        rejection_reason=reason,
        rejection_stage=stage,
        failed_criteria=failed,
        similar_to_candidate_id=similar_to,
        similarity_score=similarity,
    )


def _reject_ranked(
    row: RankedCandidate,
    *,
    reason: str,
    stage: str,
    failed: tuple[str, ...],
) -> RejectedCandidate:
    return _reject(row.candidate, row.score, reason=reason, stage=stage, failed=failed)
