# paper_submit_smoke_test_v1.contract.md

Contract owner: `ops/tools/run_paper_submit_smoke_test_v1.py`

Purpose:
- provide an explicit TEST-ONLY PAPER submit/evidence smoke path
- prove paper broker plumbing end-to-end without altering the normal production orchestrator

Required governed inputs:
- policy registry:
  `governance/02_REGISTRIES/C2_PAPER_SUBMIT_SMOKE_TEST_POLICY_V1.json`
- operator request:
  `constellation_2/operator_inputs/paper_submit_smoke_test_v1/<DAY_UTC>/paper_submit_smoke_test_request.v1.json`

Hard invariants:
- environment MUST equal `PAPER`
- sleeve_id MUST equal `PRIMARY`
- request MUST be explicit and day-scoped
- broker transmit MUST require explicit operator enablement
- non-PAPER invocation MUST fail closed
- request and authorization artifacts MUST include explicit smoke-test markers
- the normal orchestrator, normal allocation flow, and normal authorization flow MUST remain unchanged
- no fake broker artifacts may be written

Execution flow:
1. read governed smoke policy
2. read day-scoped smoke request
3. build one deterministic execution intent:
   - engine_id = `C2_INTENT_SIMULATOR_V1`
   - symbol = `SPY`
   - side = `BUY`
   - quantity_shares = `1`
4. write canonical smoke authorization artifact:
   `truth/engine_activity_v1/authorization_v1/<DAY_UTC>/<INTENT_HASH>.authorization.v1.json`
5. materialize execution package dependencies through `execution_build_authority_v1`
6. hand off through the existing execution kernel and submit boundary
7. emit a smoke-test report under:
   `truth_sleeves/PRIMARY/PAPER/reports/paper_submit_smoke_test_v1/<DAY_UTC>/paper_submit_smoke_test.v1.json`

Audit markers:
- request notes MUST contain `PAPER_SUBMIT_SMOKE_TEST_V1`
- authorization reason_codes MUST contain `PAPER_SUBMIT_SMOKE_TEST_AUTHORIZED`
- execution intent actor_source MUST equal `paper_submit_smoke_test_v1`
- source_artifact_refs MUST include both the smoke request path and the smoke policy path

What this contract does not allow:
- LIVE invocation
- reuse as a production entry path
- bypass of submit boundary, broker adapter, or evidence writer
- synthetic or handwritten broker submission artifacts
