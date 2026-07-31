# Mechanism Mapping

Transcript Intake V1 does not classify mechanisms itself beyond segment type. It passes candidate text into the existing Atlas V2 external strategy claim extractor.

The downstream extractor creates:

- `ExternalStrategyClaim`
- `ExternalStrategyMechanism`
- `ExternalStrategyDeduplicationResult`

Known mechanisms map to existing Atlas V2 mechanism families. Unknown or unclear mechanisms remain `UNKNOWN`; they are not upgraded by inference beyond the supplied text.
