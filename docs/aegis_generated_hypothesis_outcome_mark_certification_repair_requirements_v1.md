# AEGIS Generated Hypothesis Outcome Mark Certification Repair Requirements v1

Package 021 repairs or proves the generated-hypothesis outcome mark certification path for the Oil Shock paper observation on `TARGET_DAY=2026-06-03`.

The package may certify and route a current mark only when authoritative target-day price evidence exists with source path, source hash, timestamp, current freshness, and target-day session/finality. It must not certify stale, prior-day, provisional, or missing marks.

Required output: `truth/reports/aegis_generated_hypothesis_outcome_mark_certification_repair_v1/<TARGET_DAY>/generated_hypothesis_outcome_mark_certification_repair.v1.json`.

The artifact reports required mark symbol/date/type, authoritative price evidence, before/after certification and routing state, evidence-lineage coverage, candidate diagnostics presence, Package 020 status after repair, exact blocker fields, ownership, and safety flags.
