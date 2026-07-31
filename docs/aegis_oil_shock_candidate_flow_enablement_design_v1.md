# Aegis Oil Shock Candidate Flow Enablement Design v1

The enablement artifact is a read-only classifier over existing Oil Shock artifacts. It does not run broker systems, create candidates, create paper observations, mutate allocation, or bypass candidate lifecycle gates.

## Design

1. Confirm the deterministic Oil Shock producer command exists.
2. Inspect the latest producer artifact and candidate-flow artifact for the target day.
3. Extract required, available, and missing evidence fields plus required, available, and missing market symbols.
4. Classify the blocker:
   - missing producer/tool or missing producer artifact -> `PRODUCER_MISSING`
   - valid raw intent/candidate evidence -> `CANDIDATE_FLOW_STARTED`
   - producer reports no qualifying setup -> `WAITING_FOR_MARKET_CONDITIONS` with blocker `NO_MARKET_SETUP`
   - required system market-data symbols missing -> `SYSTEM_DATA_PIPELINE_REQUIRED`
   - incomplete policy -> `POLICY_INCOMPLETE`
   - incomplete construction contract -> `CANDIDATE_CONSTRUCTION_INCOMPLETE`
5. Surface the row through the existing generated-hypothesis/Oil Shock UI section using backend artifact fields only.

## June 2 Expected Outcome
For 2026-06-02, the producer exists and runs. The blocker is system market-data evidence for `USO`, so the expected classification is `SYSTEM_DATA_PIPELINE_REQUIRED`, with `david_action_required=false` and no candidate fabrication.
