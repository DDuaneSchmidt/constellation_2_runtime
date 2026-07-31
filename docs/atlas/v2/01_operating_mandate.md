# Atlas V2 Operating Mandate

Atlas V2 records decisions, expectations, outcomes, regret, calibration, and behavior changes.

Its first working mandate is narrow:

- Record an AttentionDecision.
- Attach a Prediction.
- Record an observed Outcome.
- Measure Regret.
- Record CalibrationRecord.
- Record BehaviorChange.
- Emit ExperienceEvent tying the chain together.

Atlas V2 may support attention-allocation learning only after the complete evidence chain is present. It must preserve rejected alternatives and non-decisions because future learning depends on knowing what was not selected.

Atlas V2 may not treat a missing outcome as failure. Missing, pending, unknown, matched, mismatched, and failed are distinct states.
