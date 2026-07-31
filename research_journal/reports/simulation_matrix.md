# Candidate Validation Simulation Matrix

Date: 2026-06-05

Status: Analysis only

Scope: translates direct replay coverage levels into expected Atlas research value. This matrix does not implement data acquisition, create integrations, change replay, change qualification, create candidates, promote candidates, approve paper-forward activity, recommend trades, allocate capital, define position sizing, authorize broker execution, or change governance state.

## Matrix

| Coverage Level | Modeled Symbol Coverage | Assumed Covered Symbols | Candidates Partially Validated | Candidates Fully Validated | Expected False Positives Removed | Expected Replay Support Improvement | Expected Paper-Forward Risk Reduction | Expected Research Hours Saved | Expected Uncertainty Reduction |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Current 6.67% | 1/15 | SPY | 2 | 0 | 0.0 | 0.0% | 0.0% | 0.0 | 0.0% |
| 25% | 4/15 | SPY; DIA; QQQ; TSLA | 6 | 3 | 1.3 | 34.0% | 36.8% | 10.3 | 33.6% |
| 50% | 8/15 | SPY; DIA; QQQ; TSLA; META; BAC; MSFT; TLT | 8 | 3 | 1.5 | 47.5% | 45.8% | 12.1 | 45.7% |
| 75% | 12/15 | SPY; DIA; QQQ; TSLA; META; BAC; MSFT; TLT; USO; AMZN; NFLX; DBC | 8 | 6 | 2.3 | 74.0% | 62.3% | 16.9 | 72.6% |
| 90% | 14/15 | SPY; DIA; QQQ; TSLA; META; BAC; MSFT; TLT; USO; AMZN; NFLX; DBC; AAPL; JPM | 8 | 7 | 2.5 | 94.8% | 70.2% | 18.6 | 94.4% |
| 100% | 15/15 | SPY; DIA; QQQ; TSLA; META; BAC; MSFT; TLT; USO; AMZN; NFLX; DBC; AAPL; JPM; GOOGL | 8 | 8 | 2.8 | 100.0% | 75.0% | 20.2 | 100.0% |

## Interpretation

- Current coverage is local-symbol coverage only: SPY exists, two candidates have partial universe coverage, but direct replay produced zero post-filter samples. This is not meaningful validation feedback.
- The 25% level is the first meaningful feedback threshold because adding the DIA/QQQ cluster plus one high-overlap equity symbol turns 6 of 8 candidates into partially direct-testable cases and 3 of 8 into fully covered direct-replay cases.
- The 50% level broadens partial validation across the full 8-candidate queue, but it does not materially increase fully validated candidates unless the added symbols complete whole candidate universes.
- The 75% level is the best operating target because it moves most candidate evidence out of proxy dependence: 6 of 8 candidates become fully covered, replay support improvement reaches 74.0%, and uncertainty reduction reaches 72.6%.
- The 90% and 100% levels improve completeness, but most research value has already arrived by 75%.

## Best ROI Threshold

Best first-dollar ROI threshold: 25% unique-symbol coverage.

Reason: the DIA/QQQ pair is unusually concentrated. It completes the missing universe for three candidates, including the top two validation-queue priorities, with a small number of added symbols.

Best operating ROI threshold: 75% unique-symbol coverage.

Reason: 75% coverage produces the best balance between breadth and completeness. It fully covers 6 of 8 candidates, reduces proxy dependence across the whole queue, and avoids the diminishing returns of chasing final low-leverage completion symbols first.

## Meaningful Feedback Answer

Atlas begins receiving meaningful validation feedback at 25% unique-symbol coverage, provided that coverage includes DIA and QQQ.

Current 6.67% coverage is not meaningful feedback because it has local SPY data but no complete candidate validation and no useful direct replay sample output. A generic 25% symbol set would also be weaker; the meaningful threshold depends on selecting the high-impact DIA/QQQ cluster.

## Authority Boundary

This matrix is a research-planning estimate only. It does not authorize data acquisition, production integration, replay changes, qualification changes, governance changes, candidate promotion, paper-forward placement, trade recommendations, broker execution, capital allocation, or position sizing.
