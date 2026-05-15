# Research Lifecycle Transitions

Canonical loop:

`research_inbox_item.v1` -> `research_hypothesis.v1` -> `research_program.v1` -> `research_task_queue.v1` -> offline executor -> `research_evidence_packet.v1` -> `research_result_ledger.v1` -> `research_conclusion.v1` -> `research_failure_archetype.v1` -> `research_to_lite_promotion.v1` -> `promoted_sleeve_library.v1` -> Lite feedback -> Research follow-up.

Fail-closed transition rules:
- `IDEA` cannot become `VALIDATED_RESEARCH` without evidence refs.
- `VALIDATED_RESEARCH` cannot become `PROMOTION_CANDIDATE` without result ledger support.
- `PROMOTION_CANDIDATE` cannot become `APPROVED_FOR_LITE` without a promotion artifact and human approval.
- `REJECTED`, `ARCHIVED`, and invalidated hypotheses cannot be promoted.
- `APPROVED_FOR_LITE` cannot be reached directly from task or result output.
- Confidence changes must be explicit and bounded to one step.

Unsupported transitions fail closed.
