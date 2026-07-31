# Evaluation Tiers

Atlas V2 cheap experiments use explicit tiers.

TIER_0_DEDUPE:
High-volume duplicate and repetition checks. This tier prevents attention waste and may run broadly.

TIER_1_SANITY:
High-volume plausibility checks with minimal cost. This tier can create outcome-linked ExperienceEvents when a prediction and outcome exist.

TIER_2_LIGHTWEIGHT_VALIDATION:
Moderate-volume lightweight checks. This tier can gather stronger learning evidence but still has no validation authority.

TIER_3_ROBUST_VALIDATION:
Low-volume robust evaluation. Entry requires a PromotionGateDecision.

TIER_4_MATURITY_TRACKING:
Longer-lived maturity tracking. Entry requires a PromotionGateDecision and an evidence maturity rationale.

Tiering controls attention cost. It does not grant trading, candidate, sleeve, paper-position, allocation, recommendation, autonomous execution, broker execution, or validation authority.
