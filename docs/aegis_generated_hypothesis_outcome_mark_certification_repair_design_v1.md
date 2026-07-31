# AEGIS Generated Hypothesis Outcome Mark Certification Repair Design v1

Package 021 reads the paper position ledger, market-data artifact, paper outcome auto-closure, evidence-lineage integrity report, candidate diagnostics, and Package 020 proof.

If the required symbol has a certifiable target-day market row, the builder rebuilds/enriches the paper ledger through existing deterministic mark fields, reruns paper outcome auto-closure, reruns evidence-lineage measurement, and reruns Package 020. If the source row is missing or stale, it leaves marks unmodified and emits an exact fail-closed blocker.

Candidate generation diagnostics are repaired through the authoritative diagnostics builder when absent, so portal smoke no longer fails due to a missing diagnostics path.

Audit sequencing runs Package 021 after Package 019 maturity monitoring and before Package 020 eligibility-day proof.
