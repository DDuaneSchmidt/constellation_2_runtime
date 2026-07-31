# AI Research Scientist Design Review 001

## Scope

This is design-only documentation. It does not define production behavior, runtime authority, candidate promotion, trading authority, or capital authority.

## Core Conclusion

The AI Scientist is approved only as a constrained supporting service, not a primary architecture.

Scientist-as-primary-pillar is denied.

Scientist-as-supporting-adversarial-service is approved.

The practical V0.1 concept is renamed:

Research Adversary / Assumption Extractor / Constraint Analyzer

## Design Review

The AI Scientist concept is useful when it challenges claims, extracts assumptions, identifies alternative explanations, proposes falsification tests, and clarifies constraints. It is not suitable as the central architecture for Atlas Research OS because the core system must remain evidence-led, deterministic where possible, and governed by explicit replay, qualification, paper-forward, and outcome-tracking boundaries.

The system should treat AI output as structured research critique, not as proof.

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

## Recommendation

BUILD_V0_1.

Build the Research Adversary / Assumption Extractor / Constraint Analyzer as a constrained supporting service. Do not build Scientist-as-primary-pillar.

Research Director / Meta-Research / Research Allocation are higher long-term priorities.
