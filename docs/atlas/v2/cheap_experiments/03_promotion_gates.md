# Promotion Gates

Promotion gates protect expensive attention.

TIER_0_DEDUPE and TIER_1_SANITY may run at high volume. TIER_2_LIGHTWEIGHT_VALIDATION may run at moderate volume. TIER_3_ROBUST_VALIDATION and TIER_4_MATURITY_TRACKING require PromotionGateDecision records before promotion.

PromotionGateDecision must record:

- experiment_id
- from_tier and to_tier
- decision
- reason
- expected_incremental_learning
- required_evidence
- forbidden_authority_acknowledged

TIER_4_MATURITY_TRACKING also requires an evidence maturity rationale. Approval means only that more learning effort is justified. It does not validate a claim or authorize action.
