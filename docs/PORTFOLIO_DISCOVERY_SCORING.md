# Portfolio Discovery Scoring

`src.portfolio_discovery_scoring` ranks challenger candidates by portfolio diversification value, not by return alone.

The scoring API is research-only. It does not create trade advice, paper-trade readiness, broker authority, or capital allocation authority. Consumers must keep querying verified runtime truth for any readiness or actionability state.

## Inputs

`challenger_score()` requires these numeric candidate fields:

- `performance`: normalized return or performance quality, where higher is better.
- `drawdown`: maximum drawdown as fraction or percent. Lower absolute drawdown is better.
- `stability`: behavior consistency, where higher is better.
- `diversification`: portfolio diversification contribution, where higher is better.
- `correlation_to_ultrasafe`: correlation versus UltraSafe. Lower absolute correlation is better.
- `ultrasafe_similarity_score`: structural similarity to UltraSafe. Lower is better.
- `overfit_risk`: overfit risk estimate. Lower is better.

Missing or non-numeric required metrics raise `ValueError`; the scorer fails closed instead of inventing missing truth.

## Scoring

The default weighted score rewards:

- strong performance
- low drawdown
- high stability
- high diversification
- low correlation to UltraSafe

It penalizes:

- high UltraSafe similarity
- high overfit risk
- unstable behavior
- high UltraSafe correlation
- excessive drawdown

`rank_challengers()` sorts candidates by final score descending and attaches rank-aware explanations. A high-return candidate can rank below a lower-return candidate when drawdown, instability, similarity, correlation, or overfit risk weakens its portfolio value.

## Output

Each score result includes:

- `score`
- `rank`
- normalized `components`
- `weighted_components`
- threshold `penalties`
- plain-language `explanation`

The explanation states the rank, strongest components, weakest components, and active penalties so operators can see why the candidate ranked where it did.
