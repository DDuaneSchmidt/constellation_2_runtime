# Object Contract

The generator emits four append-only objects:

- `CheapExperimentSpec`
- `ExperimentDataRequirement`
- `ExperimentEvaluationPlan`
- `ExperimentGenerationRun`

Every `CheapExperimentSpec` includes entry, exit, stop, target, baseline, required data, evaluation metric, falsification threshold, authority acknowledgement, and spec-only status fields.

`ExperimentEvaluationPlan` repeats the baseline, metric, and falsification threshold and records `execution_allowed=false` and `result_recording_allowed=false`.

`ExperimentGenerationRun` records the generated spec IDs and the allowed and forbidden tier boundaries for audit.
