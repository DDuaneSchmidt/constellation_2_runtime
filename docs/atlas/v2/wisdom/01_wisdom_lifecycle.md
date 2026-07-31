# Atlas Wisdom Lifecycle

Wisdom formation begins only after experience exists. Atlas must not create wisdom directly from possibility, novelty, or interest.

Lifecycle states:

- EMERGING: one or more ExperienceEvent records suggest a behavior-changing rule.
- SUPPORTED: linked outcomes and validations show behavior changed as expected.
- STRONG: repeated supporting outcomes and future decision improvements show durable value.
- CONTESTED: contradicting evidence exists and the wisdom must remain challengeable.
- RETIRED: contradicting evidence, decay, or replacement wisdom makes future use unsafe or obsolete.

Formation requirements:

1. ExperienceEvent links a decision, prediction, and outcome.
2. CandidateWisdom records the reusable statement, originating_experience_ids, supporting_outcome_ids, support counts, confidence, status, and wisdom_score.
3. WisdomValidation proves the statement affected a future prediction or decision and behavior_changed is true.
4. WisdomEvent records previous_behavior, new_behavior, expected_impact, and actual_impact.
5. Future decisions may use only non-retired wisdom and must preserve traceability back to experience and outcomes.

Append-only rule:

Wisdom status changes append new records through transition_history. Retired wisdom is not deleted. It remains historical evidence explaining why future behavior changed again.
