# Claim Batch Processor Principles

Atlas V2 Claim Batch Processor V1 is a research-only compression layer for high-volume supplied claims. It exists to preserve intake evidence and compress repeated strategy descriptions into mechanism families before any cheap experiment eligibility decision.

Allowed inputs:

- Transcript Intake
- External Strategy Claims
- Historical Claims
- Manual Claims

Allowed outputs:

- ClaimBatch
- ClaimBatchRun
- ClaimBatchMetrics
- mechanism-granularity ExternalStrategyMechanism, ExternalStrategyDeduplicationResult, ExternalStrategyContrarianTheory, ExternalStrategyCheapExperimentHandoff, and MechanismRegistry records

The processor may classify, dedupe, summarize, audit, and route to cheap experiment eligibility. It may not create experiments per raw claim. It emits cheap experiment eligibility at compressed mechanism granularity only.
