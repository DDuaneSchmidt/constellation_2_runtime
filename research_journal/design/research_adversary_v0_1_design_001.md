# Research Adversary / Assumption Extractor / Constraint Analyzer V0.1 Design 001

## Scope

This is design-only documentation. It defines a constrained concept and does not implement production behavior.

## Purpose

The V0.1 service critiques research artifacts before they become stronger claims. Its job is to reduce false confidence by extracting assumptions, proposing rival explanations, defining constraints, and recommending falsification tests.

## Approved Concept

Scientist-as-primary-pillar is denied.

Scientist-as-supporting-adversarial-service is approved.

Final recommendation: BUILD_V0_1.

## Inputs

- Observation clusters
- Research claims
- Hypotheses
- Historical replay summaries
- Edge qualification summaries
- Failure analyses
- Paper-forward outcome summaries
- Methodology evaluations

## Allowed Outputs

- AnomalyNarrative
- MechanismProposal
- CompetingExplanation
- NullExplanation
- AssumptionExtraction
- ConstraintAnalysis
- ExperimentProposal
- FalsificationProposal
- BeliefUpdateProposal

## Forbidden Outputs

- TradeRecommendation
- CapitalRecommendation
- PositionSizing
- PortfolioAllocation
- CandidatePromotion
- ReplayOverride
- QualificationOverride
- GovernanceOverride

## Required Artifact Questions

- Why might this be true?
- Why might this be false?
- What assumptions are required?
- What evidence supports it?
- What evidence weakens it?
- What would falsify it?
- What experiment should test it?
- What authority does this artifact NOT have?

## Output Contract

Every output should be framed as a proposal or critique. It should preserve uncertainty and include explicit non-authority language.

Example authority statement:

This artifact is research critique only. It does not authorize trading, capital allocation, position sizing, portfolio allocation, candidate promotion, replay overrides, qualification overrides, or governance overrides.

## Success Criteria

- More hypotheses include explicit assumptions.
- More hypotheses include null explanations.
- Weak mechanisms are routed to falsification earlier.
- Experiments become more precise.
- Paper-forward capacity is not consumed by poorly constrained ideas.

## Long-Term Context

Research Director / Meta-Research / Research Allocation are higher long-term priorities. V0.1 should produce structured critique that those future systems can use.
