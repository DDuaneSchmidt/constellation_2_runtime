# session_current_v1

`session_current_v1` is the planned reducer-owned canonical current for active session truth after explicit cutover.

Status in this pass:
- `UNIMPLEMENTED`
- cutover boundary required before activation

Planned reducer owner:
- `truth_kernel_session_current_reducer_v1`

Purpose:
- converge active day, admission, promotion, and operator session state into one reducer-owned current

Allowed inputs after implementation:
- semantic surface `session.active_session_current`
- semantic surface `session.session_promotion_decision`
- semantic surface `session.market_calendar_day`
- semantic surface `operator.control_plane_operator_status`
- additional governed session/lifecycle surfaces only if explicitly admitted by governance

Schema:
- reducer-owned current with:
  - reducer identity
  - activation state
  - active day binding
  - admission/promotion binding refs
  - operator-facing summary refs
  - replay input lineage

Fail-closed rules:
- missing active session truth, missing admission/promotion binding, or contradictory session inputs MUST fail closed

Write prohibition:
- no non-reducer writer may publish `session_current_v1/current.json`

Dual-truth prohibition:
- `active_session_v1/current.json` remains the only live active-day authority in this phase
- `session_authority_status_v1/current.json` remains a derived operator surface in this phase
- `session_current_v1` MUST NOT be activated until an explicit governed cutover boundary retires or bridges those overlapping live surfaces
