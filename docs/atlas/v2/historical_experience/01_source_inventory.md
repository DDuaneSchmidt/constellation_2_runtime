# Source Inventory

Supported source types are:

- `FAILURE`: a historical expected-versus-actual miss with enough provenance to preserve the originating artifact.
- `OBSERVATION`: a historical observation with an implied attention decision and outcome.
- `KNOWLEDGE`: a historical knowledge artifact that can be expressed as expected outcome, actual outcome, confidence, regret, and quality.
- `DECISION`: a historical decision record with an outcome.
- `CONTRADICTION`: a historical contradiction where later evidence conflicts with an earlier expectation.
- `RESEARCH_OUTCOME`: a historical research outcome review with an expectation, realized result, and preserved provenance.

Inventory is an explicit input list of historical artifact references. A source is conversion-eligible only when existing evidence supplies source artifact, historical date, decision summary, expected outcome, actual outcome, confidence, regret score, quality score, conversion reason, and provenance reference. Missing or unknown expectation or outcome evidence is preserved as `INCOMPLETE_PROVENANCE`; the factory does not fabricate `Prediction` or `Outcome` records.
