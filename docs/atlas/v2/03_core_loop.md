# Atlas V2 Core Loop

The Atlas V2 loop is:

1. Decision: record what attention was allocated, why, what was rejected, and what uncertainty was targeted.
2. Prediction: record the expected outcome and confidence before the outcome is known.
3. Outcome: record what happened, when it happened, and the evidence reference.
4. Regret: record the miss, missed alternative, regret reason, and importance-weighted regret.
5. Calibration: compare confidence against actual result and place the error in a bucket.
6. Behavior Change: record how future behavior should change and why.
7. Experience Event: bind the full chain into one durable lesson.

The loop is complete only when an ExperienceEvent links back to a Decision, Prediction, and Outcome. Regret, CalibrationRecord, and BehaviorChange are expected in the initial success chain.
