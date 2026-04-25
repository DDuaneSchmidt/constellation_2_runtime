# exit_decision_v1.contract.md

Contract owner:
- `constellation_2/common/exit_decision_engine_v1.py`

Purpose:
- provide one canonical decision-only surface for open-position exit management
- separate decision truth from broker execution
- preserve deterministic rule precedence across stop updates, partials, and full closures

Canonical path:
- `truth/positions_v1/exit_decision_v1/<DAY_UTC>/<POSITION_ID>/exit_decision.v1.json`

Writer:
- `ops/tools/run_exit_decision_engine_v1.py`

Required semantics:
- every artifact MUST use one explicit management state from the governed state machine
- every artifact MUST emit one explicit `decision_action`
- full-exit triggers MUST outrank profit-taking and stop-tightening logic
- imported positions MUST respect their normalized risk basis and remain analytically quarantined by default

State machine:
- `OPEN_UNPROTECTED`
- `OPEN_INITIAL_RISK`
- `OPEN_RISK_REDUCED`
- `OPEN_PARTIALS_TAKEN`
- `OPEN_TRAILING`
- `EXIT_PENDING`
- `EXIT_SUBMITTED`
- `EXIT_FILLED`
- `EXIT_CANCELLED`
- `EXIT_BLOCKED`
