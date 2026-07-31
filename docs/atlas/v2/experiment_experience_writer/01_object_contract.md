# Object Contract

Inputs:

- `ResearchHypothesis`
- `CheapExperimentSpec`
- `ExperimentResult`

Outputs:

- `Prediction`
- `Outcome`
- `Regret`
- `CalibrationRecord`
- `ExperienceEvent`
- `LearningVelocityMetric`

Every `ExperienceEvent` carries `hypothesis_id`, `experiment_spec_id`, and `experiment_result_id` in addition to the normal decision, prediction, outcome, regret, and calibration links. Inconclusive results still emit an `Outcome` with `experiment_outcome_status=INCONCLUSIVE`. Failed outcomes emit regret and calibration records instead of silently disappearing.
