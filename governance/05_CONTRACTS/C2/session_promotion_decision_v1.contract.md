# session_promotion_decision_v1

`session_promotion_decision_v1` is the canonical startup promotion-gate artifact between `pre_open_bundle_v1` and `active_session_v1/current.json`.

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/session_promotion_decision_v1/<DAY>/session_promotion_decision.v1.json`

Canonical producer:
- `ops/tools/run_session_authority_v1.py`
- code owner: `constellation_2/common/session_promotion_gate_v1.py`

Rules:
- it must consume canonical `pre_open_bundle_v1` and binding `target_day_admission_v1`
- it must not re-derive raw producer readiness outside those bound upstream artifacts
- `promotion_state` must be one of:
  - `PROMOTED`
  - `BLOCKED`
  - `FAILED_VALIDATION`
- `active_session_v1/current.json` is the only startup current-state artifact promoted by this gate in this pass
- `target_day_admission_v1/<DAY>.json` remains a day-scoped binding input, not a promoted current-state artifact
- producer-local current surfaces such as `ib_api_handshake/latest_pointer.v1.json` and PRIMARY/PAPER `run_pointer_v2/canonical_authority_head.v1.json` remain pre-open prerequisite inputs, not session-promotion outputs
- if `pre_open_bundle_v1.materialization_state != COMPLETE`, promotion must be `BLOCKED`
- if `target_day_admission_v1.admission_status != ADMIT`, promotion must be `BLOCKED`
- if required bundle or admission references are unreadable or day-mismatched, promotion must be `FAILED_VALIDATION`
- on `BLOCKED` or `FAILED_VALIDATION`, current active-day rollover must remain withheld
- canonical morning operator surfaces must stop before day-open or orchestrator progression when this artifact is not `promotion_state=PROMOTED`
- when this artifact is the first startup block after pre-open completion, the operator-facing morning summary should identify `session_promotion_decision_v1` as the canonical stop surface and include its path
- when this artifact is the earliest failing owned prerequisite in the morning readiness projection, the operator-facing summary should identify `ops/tools/run_session_authority_v1.py` as the fixing owner and instruct operators to rerun the canonical entrypoint after that prerequisite is corrected
- promotion must record:
  - `target_day`
  - prior current-state reference
  - exact `pre_open_bundle_ref`
  - exact `target_day_admission_ref`
  - `promotion_state`
  - `candidate_artifacts[]`
  - `promoted_artifacts[]`
  - `blocked_reason_codes[]`
  - `owner_tool`
