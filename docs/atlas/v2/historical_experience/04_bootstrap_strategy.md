# Bootstrap Strategy

Bootstrap starts with a small curated batch of high-provenance historical failures and contradictions, then adds observations, knowledge artifacts, and decisions as explicit expected/actual evidence becomes available.

Recommended sequence:

1. Provide an explicit input batch of historical records with source artifact references.
2. Convert historical failures with clear expected and actual outcomes.
3. Preserve observations or knowledge entries with missing or unknown expected/actual fields as `INCOMPLETE_PROVENANCE`.
4. Convert contradictions that expose calibration error.
5. Run Atlas V2 audits and the Learning Estimator over the resulting ledger.
6. Review input count, converted count, incomplete/rejected count, event count, quality distribution, provenance coverage, and authority audit status.

Do not use bootstrap conversion to create discovery, hypotheses, candidates, sleeves, paper positions, recommendations, validation authority, or allocation surfaces.
