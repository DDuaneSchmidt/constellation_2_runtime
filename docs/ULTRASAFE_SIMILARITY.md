# UltraSafe Similarity Engine

`src.ultrasafe_similarity` scores whether a candidate is genuinely different from UltraSafe. It is a differentiation screen, not a search for a better UltraSafe replacement and not trade advice.

## Output

The engine returns these component scores on a 0.0 to 1.0 scale:

- `factor_family_overlap`
- `rule_overlap`
- `concept_overlap`
- `sector_similarity`
- `market_cap_similarity`
- `return_behavior_similarity`
- `drawdown_similarity`

The final `ultrasafe_similarity_score` is the equal-weight average of those seven measures.

## Scale

- `0.00-0.30`: highly differentiated
- `0.31-0.60`: partially differentiated
- `0.61-1.00`: likely redundant

## Explainability

Every result includes per-component explanations. Set-based dimensions list shared and candidate-only terms. Market-cap output reports the candidate-to-UltraSafe size ratio. Return and drawdown outputs report the aligned observation count used for behavior comparison.

Missing market cap, return behavior, or drawdown inputs score as `0.0` for that component so sparse evidence does not fabricate similarity to UltraSafe.
