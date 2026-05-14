# Research To Lite Promotion

The promotion boundary protects Aegis Lite from research churn.

Promotion must require evidence, not enthusiasm. A research idea becomes eligible for Lite implementation only when:

- It has at least one `research_evidence_packet.v1`.
- The research status is `VALIDATED_RESEARCH`.
- The promotion status is `APPROVED_FOR_LITE_IMPLEMENTATION`.
- `approved_by_human=true`.
- Validation, regime evidence, expectancy evidence, failure modes, governance compatibility, risk contract, stop logic, manual execution compatibility, and operator clarity have been reviewed.

Even then, the promotion artifact does not mutate runtime behavior automatically. It is governance evidence for a future source change, test, build, and activation.

Research Lab may recommend promotion, but it cannot directly place a sleeve into Aegis Lite. Lite may only consume sleeves present in `promoted_sleeve_library.v1`, and that library only accepts sleeves with `promotion_status=promoted` plus explicit promotion evidence.

Draft, rejected, archived, or under-review research is not eligible for Lite implementation.
