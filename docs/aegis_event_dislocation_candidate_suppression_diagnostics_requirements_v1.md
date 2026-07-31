# AEGIS Event Dislocation Candidate Suppression Diagnostics Requirements v1

Package T06 explains why `C2_EVENT_DISLOCATION_V1` has raw signals but zero valid candidate contracts for a target day.

The diagnostic must:

- read existing truth evidence only;
- identify raw signal ids and source paths;
- validate signal schema and lineage;
- determine whether candidate generation was invoked;
- distinguish candidate construction failure, contract rejection, duplicate/cooldown/exposure suppression, confidence/quality/data gate failure, policy gaps, and unknown deterministic blockers;
- write `truth/reports/aegis_event_dislocation_candidate_suppression_diagnostics_v1/<TARGET_DAY>/event_dislocation_candidate_suppression_diagnostics.v1.json`;
- report owner and whether David action is required.

The diagnostic must not change strategy logic, thresholds, scoring, risk policy, allocation, research quality, broker/live trading, or fabricate signals/candidates.
