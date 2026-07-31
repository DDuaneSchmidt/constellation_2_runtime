# AI Research Scientist Governance 001

## Scope

This is design-only governance documentation. It does not implement runtime controls, production policy, candidate promotion, trading authority, broker authority, or capital authority.

## Governance Conclusion

Scientist-as-primary-pillar is denied.

Scientist-as-supporting-adversarial-service is approved.

The service is allowed only as:

Research Adversary / Assumption Extractor / Constraint Analyzer

## Authority Boundary

The service may generate research critique and proposed tests. It may not decide, approve, override, promote, allocate, size, or recommend trades.

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

## Governance Rules

- Outputs must be labeled as research-only critique.
- Outputs must preserve uncertainty.
- Outputs must include a null explanation when proposing a mechanism.
- Outputs must include falsification criteria.
- Outputs must reference evidence, not invent validation.
- Outputs must not change replay certification.
- Outputs must not change edge qualification.
- Outputs must not create or promote paper-trade candidates.
- Outputs must not authorize live trading, capital, broker execution, position sizing, or portfolio construction.

## Recommendation

BUILD_V0_1 as a constrained adversarial service only.

Research Director / Meta-Research / Research Allocation are higher long-term priorities than a broad AI Scientist pillar.
