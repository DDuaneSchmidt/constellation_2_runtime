# Forbidden Boundaries

Cheap Experiment Executor V1 may only use fixture, mock historical, or historical-readonly data modes. It must reject live-market modes and operator-supplied non-read-only data requirements.

It must not emit `CheapExperiment`, `Prediction`, `Outcome`, candidate, sleeve, paper-position, allocation, recommendation, validation, broker, order, or trade artifacts.

Executor results are not validation authority and are not trade advice. No consumer may infer readiness, recommendation, or allocation from these records. Consumers must query verified truth and the append-only result records.
