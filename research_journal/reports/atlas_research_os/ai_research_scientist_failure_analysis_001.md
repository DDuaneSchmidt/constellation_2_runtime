# AI Research Scientist Failure Analysis 001

Date: 2026-06-05
Status: Design failure analysis only

Scope: analyzes how an AI Research Scientist layer can fail. This document does not implement code, alter Atlas tooling, create candidates, change replay state, change qualification state, change governance, change ranking, change paper-forward state, recommend trades, allocate capital, size positions, or authorize broker execution.

Review posture: assume most AI-generated theories are plausible nonsense until proven otherwise.

## Failure Mode Summary

| Failure mode | Mechanism | Damage | Detection | Required mitigation |
| --- | --- | --- | --- | --- |
| Narrative overfit | AI explains random noise as mechanism | more false hypotheses and wasted replay | negative controls and repeated-failure tracking | require null explanation and falsification route |
| Post-hoc causality | AI backfills a causal story after observing outcome | false belief updates and mechanism laundering | compare against pre-registered experiment criteria | require timestamped proposal before evidence update |
| Proxy laundering | AI treats proxy evidence as direct target evidence | candidate confidence inflated | lineage and universe-scope checks | preserve proxy labels and forbid promotion |
| Duplicate rediscovery | AI restates retired or failed ideas | repeated research debt | memory duplicate checks | block or require reopening justification |
| Authority leakage | AI emits approval-like language | accidental promotion or paper-forward implication | forbidden-output classifier and human review | quarantine artifact |
| Competing-explanation collapse | AI lists alternatives but only argues for favorite | weak falsification design | explanation balance audit | require testable alternatives |
| Experiment theater | AI proposes experiments that cannot falsify | more prose, no learning | experiment quality scoring | require pass/fail and minimum sample criteria |
| Belief inflation | AI updates confidence from weak evidence | fake progress | evidence-weighted update policy | cap generated-only updates |
| Memory contamination | AI writes unsupported lessons as knowledge | long-lived false beliefs | generated-only label audit | write proposals only |
| Evaluation gaming | AI improves metrics by avoiding hard cases | misleading quality gains | fixed evaluation sets and negative controls | evaluate coverage and exclusions |

## Detailed Failure Modes

### 1. Narrative Overfit

Issue: The AI notices an anomaly and creates a mechanism story that fits observed data but has no predictive or falsifiable content.

Affected artifacts:

- `AnomalyNarrative`
- `MechanismProposal`
- `BeliefUpdateProposal`

Expected symptom:

- high novelty
- low replay support
- low paper-forward survival
- repeated failure under new wording

Mitigation:

- Every anomaly must include a null/noise explanation.
- Every mechanism must list what evidence would falsify it.
- Negative controls must be included in evaluation.
- Proposals without decisive experiments stay backlog-only or are retired.

### 2. Post-Hoc Causality

Issue: The AI observes an outcome, then invents a mechanism that would have predicted it.

Damage:

- makes historical replay look more meaningful than it is
- contaminates belief updates
- creates false confidence in mechanism quality

Mitigation:

- Mechanism proposals must be timestamped before the evidence they claim to explain is used for validation.
- Belief updates must reference prior proposal ids and content hashes.
- Any proposal created after the outcome must be marked `POST_HOC_GENERATED_ONLY` or equivalent future reason code.

### 3. Proxy Laundering

Issue: The AI treats SPY proxy, daily proxy, or single-symbol direct replay as if it proves a candidate-specific universe, intraday rule, or event mechanism.

Known Atlas relevance:

- direct validation shows daily proxy warnings
- zero-sample candidates often have daily data but not intraday rule validation
- universe-level candidates can show one-symbol replay output

Mitigation:

- Every artifact must carry evidence scope: symbol, universe, timeframe, regime, and proxy/direct status.
- AI must explicitly state when evidence is proxy-only.
- AI may propose experiments to replace proxy evidence, but cannot upgrade proxy evidence itself.

### 4. Duplicate Rediscovery

Issue: The AI generates a mechanism that is already present, weakened, falsified, retired, or blocked under another wording.

Damage:

- increases research debt
- makes repeated failures look like independent discoveries
- hides negative knowledge

Mitigation:

- Mandatory memory lookup before `MechanismProposal` acceptance.
- Duplicate score and retired-knowledge refs must be included.
- Reopened ideas require material-difference fields.
- No proposal can enter prioritized backlog without duplicate and retirement checks.

### 5. Authority Leakage

Issue: The AI emits or implies forbidden outputs:

```text
TradeRecommendation
CapitalRecommendation
PositionSizing
PortfolioAllocation
CandidatePromotion
ReplayOverride
QualificationOverride
GovernanceOverride
```

Damage:

- confuses generated research with runtime truth
- risks accidental downstream consumption
- violates Atlas and Aegis authority boundaries

Mitigation:

- Hard forbidden-output scan.
- Artifact quarantine on first violation.
- No free-form output can be consumed by candidate or paper systems.
- Allowed artifact schemas must have no fields that can encode trade, capital, sizing, allocation, promotion, replay override, qualification override, or governance override.

### 6. Competing-Explanation Collapse

Issue: The AI includes alternatives superficially but writes the design so the favored explanation is always privileged.

Damage:

- false rigor
- weak experiment design
- hidden confirmation bias

Mitigation:

- Each `CompetingExplanation` must have support criteria and weakening criteria.
- At least one experiment must be capable of supporting a competing explanation over the favored mechanism.
- Review should score explanation balance, not just explanation count.

### 7. Experiment Theater

Issue: The AI proposes experiments that are plausible but not executable, not falsifiable, not bounded, or not linked to data availability.

Damage:

- backlog pollution
- operator time waste
- no improvement in replay support rate

Mitigation:

- Experiment proposals require data manifest, fixed window, pass/fail criteria, leakage controls, minimum sample requirement, and expected information gain.
- Experiments without decisive failure criteria are marked narrative-only and cannot be prioritized.

### 8. Belief Inflation

Issue: The AI updates beliefs upward from generated text, weak historical support, or ambiguous observations.

Damage:

- confidence grows faster than evidence maturity
- later candidate-quality metrics become contaminated

Mitigation:

- Belief updates are proposals only.
- Generated-only evidence cannot increase empirical support.
- Contradictions and uncertainty must be carried forward.
- Evidence maturity and belief confidence remain separate fields.

### 9. Memory Contamination

Issue: AI outputs become memory records that future workers treat as knowledge.

Damage:

- plausible nonsense becomes institutional memory
- generated claims get laundered into learning nodes

Mitigation:

- AI writes only proposal artifacts.
- Memory integration requires label validation, lineage validation, duplicate checks, and lifecycle-controller approval.
- Generated-only records remain generated in memory and cannot support candidate promotion.

### 10. Evaluation Gaming

Issue: The AI appears to improve metrics by selecting easier observations, avoiding low-quality anomalies, reducing coverage, or producing fewer but safer proposals.

Damage:

- fake improvement in hypothesis survival
- lower learning coverage
- hidden failure displacement

Mitigation:

- Evaluate on fixed observation sets.
- Track exclusions and abstentions.
- Compare to simple baseline triage.
- Require no deterioration in coverage, evidence maturity, or forbidden-artifact rate.

## Specific Atlas Risk Areas

### Zero-Sample Replay Cases

The AI may invent stories for zero-sample direct replays instead of recognizing replay attrition.

Required behavior:

- propose attrition diagnostics
- distinguish missing data, missing intraday data, trigger translation failure, and regime-filter zeroing
- avoid converting zero samples into mechanism disproof unless the test was valid

### Confirmed Candidate Cases

The AI may overgeneralize one confirmed direct-data result into broad mechanism confidence.

Required behavior:

- preserve sample size, symbol scope, proxy/direct comparison, and weakening evidence
- require per-symbol and timeframe validation before universe-level claims

### Event-Reaction Cases

The AI may treat price movement as event evidence without event timestamps.

Required behavior:

- keep event-reaction candidates blocked from stronger claims until event metadata exists
- propose event-window diagnostics rather than causal stories

## Failure Impact On Core Metrics

| Core requirement | How AI can hurt it | Required guardrail |
| --- | --- | --- |
| hypothesis quality | creates more plausible but weak hypotheses | falsification score and duplicate check |
| replay support rate | proposes experiments lacking data | data manifest and replay feasibility gate |
| candidate conversion | increases generated candidate pressure | no candidate output or promotion field |
| backtest support | overfits to historical artifacts | fixed replay sets and leakage checks |
| paper-forward survival | pushes weak ideas into observation | no paper-forward state writes |
| failure reduction | repeats retired ideas | memory and retirement checks |

## Red Lines

The layer should be rejected or disabled if any of the following occur in evaluation:

- forbidden artifact output is emitted
- generated-only text is consumed as supported evidence
- candidate promotion language appears
- replay or qualification status is overwritten
- duplicate/reopened checks are bypassed
- paper-forward state changes because of AI output
- failure rate increases while proposal volume rises
- improvement is not measurable against baseline

## Failure Analysis Conclusion

The largest risk is not that the AI makes mistakes. Mistakes are expected. The largest risk is that the system gives elegant mistakes memory, authority, or candidate influence. The design is acceptable only if the AI is constrained to adversarial, generated-only, falsification-oriented proposals with measurable downstream evaluation.
