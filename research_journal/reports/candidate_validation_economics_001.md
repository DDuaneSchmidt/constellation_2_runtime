# Candidate Validation Economics 001

Status: Research estimate only

Scope: estimates the expected research value of acquiring missing candidate-specific data. This report does not acquire data, implement pipelines, modify candidates, approve paper-forward work, change replay or qualification state, place paper trades, provide trade advice, authorize broker execution, authorize live trading, allocate capital, or define position sizing.

## Objective

Estimate the expected value of acquiring missing data for the current candidate validation set.

Current model:

- candidates: 8
- direct replays: 2
- complete validations: 0
- current coverage: 25%
- current blocker: candidate-specific symbol/universe data is incomplete; evidence still depends materially on proxy data

The economic question is not whether the candidates are attractive. The question is whether acquiring missing data would make the research process more efficient by increasing validation throughput, improving evidence quality, reducing false positives, improving replay interpretability, and lowering paper-forward observation risk.

## Authority Boundary

- This is not a trading recommendation.
- This is not a capital allocation recommendation.
- This is not a paper placement instruction.
- This does not promote, demote, qualify, or disqualify any candidate.
- This does not change replay, qualification, governance, or paper-forward artifacts.
- Any future operational decision must come from explicit human review and verified runtime truth, not from this estimate.

## Current Evidence Problem

The current candidate set has enough proxy evidence to justify research interest, but not enough direct evidence to complete validation.

Observed constraints:

- all 8 candidates require direct symbol or universe data
- only 2 direct replays have run
- 0 candidates have complete validation
- proxy evidence can inflate replay support when candidate-specific behavior differs from SPY or index-proxy behavior
- paper-forward observation plans require 30 samples, but incomplete direct data weakens the interpretation of those samples

Main research risk: Atlas may spend human review and paper-forward observation attention on candidates whose apparent support is mostly proxy structure rather than candidate-specific behavior.

## Modeling Assumptions

Coverage means the share of the 8 candidate set with enough candidate-specific data to run a direct replay and compare it against proxy evidence.

```text
25% coverage = 2 of 8 candidates
50% coverage = 4 of 8 candidates
75% coverage = 6 of 8 candidates
90% coverage = 7-8 of 8 candidates
```

This model assumes:

- direct data produces a replay-quality comparison, not automatic validation
- complete validation requires direct replay plus sufficient lineage, regime interpretation, and paper-forward or equivalent resolved evidence
- some proxy-supported candidates will weaken when direct data is available
- false-positive reduction is valuable even when it reduces the candidate count
- research value is highest when data changes a decision or prevents low-quality paper-forward observation

## Scenario Summary

| Coverage | Direct replays available | Complete validations expected | Validation throughput | Candidate evidence quality | False positive reduction | Expected replay support improvement | Expected paper-forward risk reduction |
| ---: | ---: | ---: | --- | --- | --- | --- | --- |
| 25% | 2 | 0 | LOW | LOW-MEDIUM | LOW | LOW | LOW |
| 50% | 4 | 1-2 | MEDIUM | MEDIUM | MEDIUM | MEDIUM | MEDIUM |
| 75% | 6 | 2-4 | HIGH | MEDIUM-HIGH | HIGH | MEDIUM-HIGH | HIGH |
| 90% | 7-8 | 4-5 | HIGH | HIGH | HIGH | HIGH | HIGH |

## Metric Estimates

| Coverage | validation_throughput | candidate_evidence_quality | false_positive_reduction | expected_replay_support_improvement | expected_paper_forward_risk_reduction |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 25% | 0.25 | 0.35 | 0.10 | 0.15 | 0.15 |
| 50% | 0.50 | 0.55 | 0.30 | 0.35 | 0.35 |
| 75% | 0.75 | 0.72 | 0.50 | 0.55 | 0.60 |
| 90% | 0.90 | 0.82 | 0.65 | 0.70 | 0.75 |

Scale: `0.00` means no material research value; `1.00` means strong validation utility across the candidate set. These are planning estimates, not measured results.

## Scenario Details

### 25% Coverage

State:

- 2 direct replays
- 6 candidates still mainly proxy-dependent
- 0 complete validations

Expected result:

- Validation throughput remains low because most candidates cannot be directly compared against their attributed symbols or universe.
- Candidate evidence quality improves only for the two covered candidates.
- False-positive reduction is limited because proxy-supported candidates still dominate the queue.
- Replay support improvement is mostly diagnostic: Atlas can learn whether the proxy method is directionally reliable for a small subset.
- Paper-forward risk remains high because most candidates can still enter review with unresolved data mismatch.

Economic interpretation: 25% coverage is enough to detect whether missing data matters, but not enough to make the candidate set efficient.

### 50% Coverage

State:

- 4 direct replays
- roughly half the candidate set can be compared against proxy evidence
- 1-2 complete validations become plausible if lineage and regime checks pass

Expected result:

- Validation throughput becomes useful because the system can start sorting direct-supported candidates from proxy-only candidates.
- Candidate evidence quality moves from proxy-heavy to mixed evidence.
- False-positive reduction becomes material: weak proxy artifacts should start falling out.
- Replay support improvement becomes measurable: direct replay can confirm, weaken, or invert proxy replay.
- Paper-forward risk reduction becomes meaningful because human reviewers can prioritize direct-supported candidates.

Economic interpretation: 50% coverage is the first economically useful threshold. It creates enough contrast to estimate proxy error and reduce review waste.

### 75% Coverage

State:

- 6 direct replays
- most campaign candidates have candidate-specific evidence
- 2-4 complete validations become plausible

Expected result:

- Validation throughput becomes high because most candidates can be processed with the same evidence standard.
- Candidate evidence quality improves sharply: proxy dependence becomes an exception rather than the default.
- False-positive reduction is high because mechanism, regime, and symbol-specific failures are easier to detect.
- Expected replay support improvement is medium-high: support quality improves even if some candidates are rejected.
- Paper-forward risk reduction is high because observation plans can be tied to direct replay behavior instead of proxy behavior.

Economic interpretation: 75% coverage is likely the highest marginal value target. It gives Atlas enough direct evidence to cleanly rank, reject, or narrow most candidates without requiring perfect data coverage.

### 90% Coverage

State:

- 7-8 direct replays
- nearly all candidates can be directly evaluated
- 4-5 complete validations become plausible if paper-forward outcome evidence or equivalent resolved validation can mature

Expected result:

- Validation throughput remains high, but marginal throughput gains over 75% are smaller.
- Candidate evidence quality is high because proxy-only exceptions are rare.
- False-positive reduction is high because most proxy artifacts can be checked.
- Expected replay support improvement is high: direct replay support becomes the default evidence standard.
- Paper-forward risk reduction is high because candidates can be filtered before consuming observation capacity.

Economic interpretation: 90% coverage is the quality target, not the first economic target. It is valuable, but the marginal gain from 75% to 90% is smaller than the gain from 25% to 75%.

## Expected Value Curve

| Step | Incremental data acquired | Marginal value | Why |
| --- | ---: | --- | --- |
| 25% -> 50% | +2 candidates | HIGH | Creates enough direct/proxy contrast to estimate proxy error. |
| 50% -> 75% | +2 candidates | HIGH | Moves most of the queue to direct evidence and enables practical triage. |
| 75% -> 90% | +1-2 candidates | MEDIUM | Improves completeness, but most decision value already exists. |

The expected value is front-loaded through 75% coverage. The strongest research-economics target is therefore not perfect coverage; it is enough direct coverage to reduce proxy dependence and expose false positives across most mechanisms.

## Candidate Evidence Quality

Current evidence quality is constrained by:

- proxy replay dependence
- incomplete direct symbol/universe data
- incomplete validation outcomes
- potential intraday/daily mismatch
- unresolved regime specificity

Expected evidence quality by coverage:

- 25%: evidence can identify whether the proxy issue exists but cannot clean the queue.
- 50%: evidence can split candidates into direct-supported, direct-weakened, and still-proxy-only buckets.
- 75%: evidence can support a clean human-review queue and reject many weak proxy artifacts.
- 90%: evidence can support near-complete candidate validation triage.

## False Positive Reduction

Expected false-positive reduction comes from three mechanisms:

1. proxy false positives: candidate looks good on SPY proxy but weakens on attributed symbols
2. regime false positives: candidate support disappears when direct data is assigned to the right regime
3. mechanism false positives: candidate is actually duplicate or generic index behavior rather than mechanism-specific behavior

Estimated reduction:

- 25% coverage: 10%
- 50% coverage: 30%
- 75% coverage: 50%
- 90% coverage: 65%

These percentages estimate reduction in research false positives, not investment outcomes.

## Replay Support Improvement

Direct data can improve replay support in two different ways:

- positive improvement: direct replay confirms proxy support and increases confidence in mechanism-specific behavior
- negative improvement: direct replay weakens proxy support and improves research quality by rejecting a false positive

Both are valuable. The metric is not "more candidates pass"; it is "replay evidence becomes more truthful."

Expected replay support improvement:

- 25% coverage: LOW, mostly calibration
- 50% coverage: MEDIUM, enough to classify half the queue
- 75% coverage: MEDIUM-HIGH, enough to triage most candidates
- 90% coverage: HIGH, direct replay becomes the default standard

## Paper-Forward Risk Reduction

Missing data increases paper-forward risk because paper-forward observation can consume human attention before Atlas knows whether the setup is candidate-specific or proxy-derived.

Direct coverage reduces risk by:

- blocking weak proxy candidates before observation
- improving observation rules with direct replay behavior
- clarifying mechanism and regime boundaries
- reducing duplicate or generic-market samples
- making invalidation rules more concrete

Expected paper-forward risk reduction:

- 25% coverage: LOW
- 50% coverage: MEDIUM
- 75% coverage: HIGH
- 90% coverage: HIGH

## Recommendation

Recommendation: target 75% coverage before treating the current candidate set as a clean validation queue.

Rationale:

- 25% is current state and is insufficient for complete validations.
- 50% is the first useful decision threshold.
- 75% likely captures most research value by converting the majority of candidates from proxy-dependent to directly testable.
- 90% is desirable for completeness, but its marginal value is lower unless the remaining candidates are strategically important or cover unique mechanisms/regimes.

Expected value of acquiring missing data: HIGH through 75% coverage, MEDIUM from 75% to 90%.

This recommendation is research-process guidance only. It does not authorize data acquisition, candidate promotion, paper-forward placement, trading, broker execution, capital allocation, or production integration.

## Measurement Upgrade

To replace this estimate with measured economics, future reports should record:

- data acquisition cost per symbol or universe
- direct replay pass/fail result by candidate
- proxy replay versus direct replay delta
- candidate evidence quality before and after data acquisition
- number of candidates rejected due to direct data
- number of candidates confirmed by direct data
- paper-forward observations avoided
- human review hours saved
- complete validations created

Until those measurements exist, the model should remain an estimate.
