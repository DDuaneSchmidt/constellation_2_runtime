# Atlas V2 Experiment Experience Writer V1 Principles

The writer converts an explicitly supplied `ExperimentResult` into Atlas V2 learning records. It writes append-only evidence only and does not execute experiments, fetch market data, validate investment claims, create candidates, create sleeves, create paper positions, allocate capital, or recommend actions.

Every write creates `Prediction`, `Outcome`, `Regret`, `CalibrationRecord`, `ExperienceEvent`, and `LearningVelocityMetric` records. The writer may create the local `AttentionDecision` needed by the Atlas experience loop so the emitted `ExperienceEvent` remains compatible with existing link audits.
