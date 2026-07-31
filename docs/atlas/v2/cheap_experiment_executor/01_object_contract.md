# Object Contract

Cheap Experiment Executor V1 emits exactly these Atlas V2 object types:

- `ExperimentResult`
- `ExperimentOutcomeSummary`
- `ExperimentExecutionRun`

Every `ExperimentResult` links to one `CheapExperimentSpec`, repeats the baseline condition, records the baseline comparison, records the evaluation metric and falsification threshold, and records both `outcome` and `falsification_result`.

`ExperimentOutcomeSummary` records aggregate pass, fail, inconclusive, falsified, not-falsified, and baseline-comparison coverage counts.

`ExperimentExecutionRun` records the input spec IDs, result IDs, allowed data modes, and explicit forbidden-authority acknowledgement.
