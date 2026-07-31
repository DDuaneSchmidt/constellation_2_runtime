# Atlas Hypothesis Generator Architecture Review 001

Objective: design the strongest long-term Atlas hypothesis-generation architecture before implementation begins.

Scope: design review only. This report does not implement Atlas hypothesis generation, create code, create schemas, modify Atlas V1, modify AEGIS runtime truth, create sleeves, create candidates, create paper positions, create validation artifacts, create trading logic, or create capital allocation logic.

Context:
- Atlas V1 is conditionally frozen as a read-only research preparation layer.
- Atlas V1 improves evidence retrieval, synthesis, failure reuse, coverage awareness, contrarian review, and evidence flow monitoring.
- Atlas V1 does not generate hypotheses.
- The current North Star remains distributed mature validation evidence.
- Hypothesis generation is not the current primary bottleneck.

Classification options:
- `APPROVED`
- `APPROVED_WITH_CHANGES`
- `REJECTED`

## Executive Summary

The candidate pipeline is directionally useful but should not be approved as the primary long-term architecture.

Candidate pipeline:

```text
Observation
-> Research Question
-> Mechanism Candidate
-> Hypothesis Family
-> Hypothesis
-> Experiment Design
-> Evidence Flow
-> Knowledge
-> Meta Learning
```

This sequence is too linear and starts too close to episodic surface evidence. It risks turning Atlas into an idea generator that produces more hypothesis objects before Atlas can judge whether a research area deserves attention, whether the idea source is reliable, whether the mechanism has prior evidence, or whether the proposed experiment is likely to reduce important uncertainty.

The stronger architecture should organize around uncertainty reduction and learning yield, with hypotheses as one intermediate artifact rather than the root object. Atlas should learn where good ideas come from, not merely generate more ideas.

Recommended architecture:

```text
Research Domain
-> Search Space
-> Source / Observation / Claim Intake
-> Research Question
-> Mechanism Model
-> Hypothesis Family
-> Hypothesis Draft
-> Experiment Design
-> Evidence Flow
-> Knowledge Update
-> Source and Generator Attribution
-> Meta-Learning
```

The central object should be the `Search Space`, governed by uncertainty, mechanism plausibility, evidence maturity, prior failures, and expected learning yield. The most important internal spine should be `Mechanism Model -> Hypothesis Family -> Hypothesis Draft`, because mechanism-level learning generalizes across instruments, markets, expressions, and source types.

Final classification: `APPROVED_WITH_CHANGES`.

## Architecture Candidates

### Candidate A: Hypothesis-First

```text
Hypothesis
-> Experiment Design
-> Evidence Flow
-> Knowledge
```

This is the smallest architecture, but it is not strong enough. It is useful for turning one idea into one test, but it cannot reliably explain where good ideas came from, which search spaces are being neglected, which mechanisms are over-mined, or which source types create low-quality work.

Verdict: reject as the organizing architecture. Keep hypothesis objects as outputs, not roots.

### Candidate B: Observation-First

```text
Observation
-> Question
-> Mechanism
-> Hypothesis
-> Experiment
```

This matches the current candidate design and is useful when Atlas starts from market behavior, an anomaly, or a recurring operational observation. It preserves empirical humility by starting with something noticed.

Its weakness is source dependence. Observations are not all equal. Some are raw market facts, some are interpretations, some are claims from external sources, and some are human intuitions. A pure observation-first architecture can overfit to whatever enters the inbox.

Verdict: useful intake path, not the whole architecture.

### Candidate C: Claim-First

```text
External Claim
-> Claim Decomposition
-> Mechanism
-> Hypothesis Draft
-> Falsification Plan
```

This is strong for YouTube, blogs, books, papers, and human ideas because most external sources arrive as claims, not clean observations. It supports provenance, source reliability measurement, and missing-detail detection.

Its weakness is that external claims can dominate attention. A claim-first system may become a strategy-debunking machine rather than a research-generation system.

Verdict: required intake lane, not the top-level architecture.

### Candidate D: Question-First

```text
Research Question
-> Mechanism
-> Hypothesis
-> Experiment
```

This is disciplined and testable. It is stronger than hypothesis-first because it separates the uncertainty being reduced from the concrete test being proposed.

Its weakness is that questions need context. Without search-space state, Atlas cannot tell whether a question is high-yield, exhausted, premature, redundant, or dependent on unavailable evidence.

Verdict: strong internal step, insufficient root object.

### Candidate E: Mechanism-First

```text
Mechanism
-> Domains
-> Expressions
-> Hypothesis Families
-> Hypotheses
```

This is the best intellectual spine. Mechanisms carry reusable learning across instruments and expressions. For example, volatility risk premium can be studied through options expressions, volatility products, index behavior, regime filters, and risk-transfer conditions. Trend persistence can be studied through equities, futures, sector rotation, and breakout or pullback expressions.

Its weakness is that mechanism-first research can become abstract and self-confirming if not tied to search-space uncertainty, prior failures, and experiment economics.

Verdict: approve as the core spine, but not as the top-level root.

### Candidate F: Search-Space and Learning-Yield First

```text
Research Domain
-> Search Space
-> Uncertainty Map
-> Source / Observation / Claim Intake
-> Question
-> Mechanism
-> Hypothesis Family
-> Hypothesis Draft
-> Experiment Design
-> Evidence Flow
-> Knowledge
-> Meta-Learning
```

This is the strongest long-term architecture. It treats hypotheses as testable instruments for reducing uncertainty inside a managed search space. It can learn which source types, mechanisms, question forms, human contributors, and evidence paths produce durable knowledge.

Verdict: recommended.

## Strengths

The recommended architecture supports Atlas's long-term goal better than a hypothesis-first design.

Strengths:
- It preserves the current North Star by optimizing for evidence maturity, not idea volume.
- It separates source intake from research truth.
- It supports multiple origins: observations, claims, papers, books, videos, human ideas, failed validations, and coverage gaps.
- It makes mechanisms reusable across markets and instrument domains.
- It prevents options research from becoming a silo by making options one domain of expression for broader mechanisms.
- It creates a place to measure learning yield: which ideas reduced uncertainty, which created noise, and which repeatedly failed.
- It allows external claims to enter as attributed, unvalidated source material instead of as internal conclusions.
- It keeps experiment design downstream from mechanisms and questions, preventing premature test creation.
- It gives Atlas a way to identify neglected but high-value search spaces.

The most important strength is attribution. Atlas should not only know whether a hypothesis worked. It should know where the idea came from, what transformed it, which prior knowledge affected it, what evidence was required, and whether the process that produced it deserves more attention.

## Weaknesses

The recommended architecture is heavier than a simple generator.

Weaknesses:
- It requires more conceptual discipline before implementation.
- It introduces more object boundaries than a simple hypothesis draft assistant.
- Search-space state can become bureaucratic if maintained before there is enough evidence.
- Mechanism labels can become vague if they are not tied to falsifiable questions and actual evidence.
- Learning-yield measurement can create false precision if Atlas has too few completed research cycles.
- External claim intake can flood the system unless it is aggressively gated.
- Meta-learning can become self-referential if it grades process quality without mature outcome evidence.

These weaknesses are acceptable for long-term architecture, but they argue against immediate implementation. Atlas should not build this until V1 evidence preparation is stable, packet quality is improved, and enough research cycles exist to make meta-learning meaningful.

## Renaissance Review

A Renaissance-style research organization would likely approve the shift away from isolated hypothesis generation and toward mechanism-centered, evidence-attributed search spaces. It would value the ability to track source quality, prior failure reuse, uncertainty reduction, and mechanism generalization.

It would approve:
- Mechanism-first reasoning as the reusable scientific layer.
- Search-space management as the portfolio of questions, not the portfolio of trades.
- Strict separation between idea generation, experiment design, validation, and capital use.
- Source attribution for every hypothesis draft.
- Measurement of idea provenance and research process quality.
- Rejection of creator credibility, narrative confidence, or backtest screenshots as evidence.
- Learning from negative evidence and failed ideas.
- Using external material as raw claim intake, not as authority.

It would reject:
- Hypothesis volume as a success metric.
- A standalone options silo disconnected from broader mechanisms.
- Treating external claims as validated inputs.
- Converting generated hypotheses into candidates, sleeves, paper positions, trades, or allocation logic.
- Mechanism labels that do not lead to falsifiable tests.
- Research questions selected because they are interesting rather than uncertainty-reducing.
- Meta-learning based on too few mature outcomes.
- Any architecture that lets Atlas bypass deterministic validation or runtime truth boundaries.

This review does not assume Renaissance replication is the objective. The lesson is narrower: a mature research organization would treat hypothesis generation as part of a controlled research production function, not as a creative content engine.

## Options Integration

Options should not be a separate research silo. They should be represented as an instrument domain and expression layer under mechanisms.

Recommended structure:

```text
Mechanism
-> Instrument Domain
-> Expression
-> Hypothesis Family
-> Hypothesis Draft
-> Experiment Design
```

Examples:

```text
Mechanism: Volatility Risk Premium
Instrument Domain: Options
Expression: Iron Condor
Hypothesis Family: Short premium performs better under bounded realized-volatility regimes than under expanding-volatility regimes.
Hypothesis Draft: For liquid index ETFs, defined-risk short premium structures entered when implied volatility exceeds trailing realized volatility by a threshold produce better risk-adjusted outcomes than a no-edge baseline after transaction costs, subject to drawdown and tail-event controls.
```

```text
Mechanism: Trend Persistence
Instrument Domain: Equities
Expression: Breakout Retest
Hypothesis Family: Post-breakout retests preserve continuation edge when volume, relative strength, and regime filters support persistence.
Hypothesis Draft: Large-cap equities that retest prior breakout levels after relative-strength confirmation produce better forward return distribution than matched non-breakout controls over the same horizon.
```

```text
Mechanism: Event Uncertainty Compression
Instrument Domain: Options
Expression: Post-earnings implied-volatility crush
Hypothesis Family: Implied uncertainty decays after scheduled events faster than realized movement compensates under specific liquidity and regime conditions.
Hypothesis Draft: For liquid earnings names with elevated pre-event implied volatility, post-event defined-risk structures exhibit better expectancy only when event move, spread width, and liquidity controls pass predefined thresholds.
```

The architecture should force options ideas to identify their underlying mechanism. "Iron Condor" is not a research mechanism. It is an expression. "Volatility risk premium", "event uncertainty compression", "skew mispricing", "term-structure dislocation", and "liquidity provision under bounded movement" are candidate mechanisms.

This prevents the common error of validating an instrument wrapper instead of the underlying reason it might work.

## External Claim Integration

External inputs should enter through a claim-intake layer, not directly as hypotheses.

Supported source types:
- YouTube claims.
- Blog claims.
- Books.
- Academic papers.
- Human ideas.
- Internal observations.
- Prior failures.
- Coverage gaps.

The intake architecture should normalize all sources into attributed research material:

```text
Source
-> Extracted Claim or Observation
-> Missing Detail Map
-> Claimed Mechanism
-> Implied Research Question
-> Generated-Only Hypothesis Draft
-> Required Evidence
-> Falsification Path
```

YouTube and blog claims should require transcript or text evidence. Titles, thumbnails, charts, comments, and creator reputation should not be sufficient. Missing entry, exit, risk, market, timeframe, sample, cost, or baseline details should be marked as missing rather than inferred.

Books should be treated as higher-context but still unvalidated source material. Atlas should preserve chapter, section, page, concept, claimed mechanism, and applicable domain. A book's coherence is not evidence that a market hypothesis works.

Academic papers should enter with stronger metadata: paper question, dataset, sample period, method, assumptions, result, limitations, replication requirements, and applicability gap. Papers can provide evidence, but Atlas must still distinguish published evidence from AEGIS-validated evidence.

Human ideas should be first-class but attributed. Atlas should preserve originator, prompt context, intended mechanism, confidence, missing assumptions, and expected learning value. Human confidence should be recorded as context, not evidence.

All external claims should default to `GENERATED_ONLY` until separately tested. They should not create candidates, sleeves, paper positions, validation artifacts, trading advice, or allocation evidence.

## Long-Term Meta-Learning Design

The long-term goal is to learn where good ideas come from.

Atlas should track the idea-production process at the level of provenance and transformation:

```text
Idea Source
-> Intake Type
-> Mechanism Mapping
-> Question Quality
-> Hypothesis Draft Quality
-> Experiment Design Quality
-> Evidence Acquisition Cost
-> Uncertainty Reduction
-> Knowledge Update
-> Future Search-Space Impact
```

Meta-learning should answer:
- Which source types generate hypotheses that survive first falsification?
- Which mechanisms produce transferable knowledge across expressions?
- Which human contributors or review processes produce high learning yield?
- Which external channels produce repeated missing-detail or false-positive claims?
- Which question forms reduce uncertainty fastest?
- Which experiment designs produce interpretable evidence?
- Which search spaces are under-sampled, exhausted, or prematurely abandoned?
- Which prior failures prevented wasted research?

Success metrics should avoid idea volume.

Preferred metrics:
- Uncertainty reduction per research cycle.
- Mature evidence produced per search space.
- Negative knowledge reused before repeated failure.
- Hypotheses retired quickly due to strong falsification.
- Mechanism-level knowledge transferred across domains.
- External source precision after attribution and testing.
- Experiment designs that produce interpretable outcomes.
- Search-space coverage improvement.

Bad metrics:
- Number of hypotheses generated.
- Number of external claims extracted.
- Number of strategy expressions listed.
- Number of backtests proposed.
- Number of options structures cataloged.
- Creator popularity or source confidence.

Meta-learning should be delayed until Atlas has enough completed research cycles. Before that, Atlas can record provenance and process fields, but it should not over-rank sources or mechanisms.

## Recommended Architecture

Recommended long-term architecture:

```text
Research Domain
-> Search Space
-> Uncertainty Map
-> Source / Observation / Claim Intake
-> Research Question
-> Mechanism Model
-> Instrument Domain
-> Expression
-> Hypothesis Family
-> Hypothesis Draft
-> Experiment Design
-> Evidence Flow
-> Knowledge Update
-> Meta-Learning
```

Root object: `Search Space`.

Core spine:

```text
Mechanism Model
-> Instrument Domain
-> Expression
-> Hypothesis Family
-> Hypothesis Draft
```

Primary optimization target: uncertainty reduction and learning yield.

Hypothesis role: testable draft artifact used to reduce uncertainty, not the root of the system.

External source role: attributed claim or observation intake, not evidence authority.

Options role: instrument domain and expression layer, not a separate silo.

Meta-learning role: determine which sources, mechanisms, questions, transformations, and experiment designs produce durable knowledge.

Implementation posture: defer. The architecture is approved for long-term direction, but no implementation should begin while Atlas V1 is conditionally frozen and evidence-flow maturity remains the current bottleneck.

Minimum acceptance bar before future implementation:
- Atlas V1 remains read-only and stable.
- V2 packet quality, source filtering, lexical blind-spot reduction, and coverage confidence are addressed or intentionally deferred with evidence.
- Hypothesis generation is explicitly bounded as `GENERATED_ONLY`.
- No generated output can mutate Research Journal objects, runtime truth, sleeves, candidates, paper positions, validation artifacts, trading logic, or allocation logic.
- The first implementation is an intake-and-drafting assistant, not an autonomous research strategist.
- Every generated hypothesis draft cites source provenance, mechanism mapping, required evidence, missing assumptions, and falsification criteria.

## Deferred Ideas

Deferred:
- Autonomous search-space creation.
- Autonomous search-space retirement.
- Research Economist behavior.
- Chief Scientist behavior.
- Capital-aware hypothesis ranking.
- Trade-expression recommendation.
- Candidate creation.
- Sleeve creation.
- Paper-position creation.
- Runtime validation authority.
- Strategy backtesting automation as part of hypothesis generation.
- Options strategy catalog as a standalone silo.
- Source credibility scoring before enough tested source history exists.
- Meta-learning leaderboards for people, channels, books, or papers.
- Automatic merging or splitting of mechanisms.
- Automatically promoting external claims into AEGIS research objects.

These ideas may become useful later, but they are not justified before Atlas can show mature evidence flow, stable preparation quality, and enough completed research cycles to evaluate idea provenance.

## Final Classification

Final classification: `APPROVED_WITH_CHANGES`.

Approve the long-term ambition, but change the architecture from hypothesis-first or observation-first to search-space and learning-yield first.

The recommended design is differentiated from a Renaissance-style copy. It uses the useful discipline of mature research organizations, but the Atlas objective is stronger and more specific: build a learning system that discovers which sources, mechanisms, questions, and experiment designs create durable knowledge.

