# exit_execution_request_v1.contract.md

Contract owner:
- `constellation_2/common/exit_execution_request_v1.py`

Purpose:
- convert canonical exit decisions into canonical execution-plane handoff truth
- ensure no exit order or protective amendment is requested before decision truth exists

Canonical path:
- `truth/positions_v1/exit_execution_request_v1/<DAY_UTC>/<POSITION_ID>/exit_execution_request.v1.json`

Writer:
- `ops/tools/run_exit_execution_request_v1.py`

Required semantics:
- this surface MUST be derived from a canonical `exit_decision_v1` artifact
- only execution-eligible decisions may produce this artifact
- the artifact MUST preserve origin and risk-basis labeling for downstream execution and reconciliation
