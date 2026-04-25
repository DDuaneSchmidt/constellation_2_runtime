# trade_result_v1.contract.md

Contract owner:
- `constellation_2/common/trade_result_closure_v1.py`

Purpose:
- provide one canonical closed-trade truth surface after a position is fully closed
- preserve native-versus-synthetic risk labeling in realized trade results
- separate operational closure from analytics eligibility

Canonical path:
- `truth/positions_v1/trade_result_v1/<DAY_UTC>/<POSITION_ID>/trade_result.v1.json`

Writer:
- `ops/tools/run_trade_result_closure_v1.py`

Required semantics:
- every artifact MUST include origin, risk basis, entry, stop, exit, and realized R
- imported positions MUST keep explicit synthetic-risk labeling when applicable
- analytics eligibility MUST remain excluded by default for imported positions unless explicitly promoted later
