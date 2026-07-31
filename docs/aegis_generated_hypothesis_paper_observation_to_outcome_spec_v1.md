# Aegis Generated Hypothesis Paper Observation-to-Outcome Spec V1

Artifact family: `aegis_generated_hypothesis_paper_observation_to_outcome_v1`.

Output path: `truth/reports/aegis_generated_hypothesis_paper_observation_to_outcome_v1/<TARGET_DAY>/generated_hypothesis_paper_observation_to_outcome.v1.json`.

The `oil_shock` row and summary expose generated hypothesis id, sleeve id, candidate contract id, paper observation ids, ledger source, readiness status, outcome status/id, outcome row creation flag, exit price presence/source, close condition status/reason, holding-period status, mark-data status, furthest stage, blocker code/reason, missing fields, required inputs, owner, and David action requirement.

Success requires a non-OPEN authoritative outcome registry row. Not-ready states preserve `furthest_stage_reached: PAPER_OBSERVATION_FLOW` and use the blocker produced from auto-closure readiness evidence. The proof never fabricates exit prices, close timestamps, outcomes, validation samples, or research quality metrics.
