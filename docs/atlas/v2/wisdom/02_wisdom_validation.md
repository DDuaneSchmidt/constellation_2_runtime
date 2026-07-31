# Atlas Wisdom Validation

Validation asks one question: did this statement change future behavior?

If behavior_changed is false, validation fails. The statement may be useful, but it is an observation, not wisdom.

Required validation links:

- wisdom_id links WisdomValidation to CandidateWisdom.
- prediction_affected identifies the future prediction shaped by the wisdom.
- decision_affected identifies the future decision shaped by the wisdom.
- behavior_changed must be true.
- validation_result records whether the change improved, degraded, or contested the wisdom.

Validation failures:

- No behavior change.
- Missing originating ExperienceEvent.
- Missing supporting Outcome.
- Retired wisdom reused as active guidance.
- CONTESTED wisdom treated as settled.
- Confidence raised without new supporting outcomes.

Validated success condition:

Experience -> Candidate Wisdom -> Behavior Change -> Improved Future Decision

The improved decision must be explicit in WisdomEvent.actual_impact or subsequent linked decision evidence.
