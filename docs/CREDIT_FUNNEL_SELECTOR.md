# Credit Funnel Selector

## Purpose

`src/credit_funnel_selector.py` reduces Portfolio123 credit consumption by filtering generated candidates before paid testing.

The workflow is:

1. Generate many candidates upstream.
2. Score each candidate internally.
3. Remove exact duplicates.
4. Remove highly similar lower-ranked candidates.
5. Rank remaining candidates.
6. Advance only the top candidates to testing.

This is a pre-testing research filter only. It does not create trade advice, paper positions, broker orders, allocation decisions, or runtime readiness claims. Runtime readiness remains governed by the verified runtime graph and runtime truth kernel.

## Discovery Score

Each candidate receives:

```text
discovery_score =
  novelty_score
  + economic_logic_score
  + diversification_score
  + non_redundancy_score
  - complexity_penalty
  - overfit_risk_penalty
```

All components are clamped to `[0.0, 1.0]`. Callers may provide explicit component scores. If a component is omitted, the selector uses deterministic internal heuristics from candidate text, mechanism, universe, tags, and existing-candidate similarity.

## Public API

- `rank_candidates()` scores, removes duplicates, removes highly similar candidates, and returns ranked survivors plus rejected candidates.
- `select_candidates_for_testing()` applies a score floor and top-N credit budget to the ranked survivors.
- `explain_selection()` explains why a candidate advanced to testing.
- `explain_rejection()` explains the rejection stage, reason, failed criteria, and similarity link when present.

## Candidate Graveyard

Every rejected candidate is represented in `CreditFunnelResult.candidate_graveyard`.

Graveyard records include candidate ID, title, rejection stage, rejection reason, failed criteria, discovery-score components, similarity link, lessons learned, and reopening conditions. Rejected candidates are preserved as research memory rather than silently discarded.

Rejection stages include:

- `CREDIT_FUNNEL_DUPLICATE_FILTER`
- `CREDIT_FUNNEL_SIMILARITY_FILTER`
- `CREDIT_FUNNEL_SCORE_FLOOR`
- `CREDIT_FUNNEL_CAPACITY_CUTOFF`

## Authority Boundary

Selection means `SELECTED_FOR_TESTING` only. It is not a trading recommendation, paper-trading authorization, broker instruction, capital allocation, or production promotion.
