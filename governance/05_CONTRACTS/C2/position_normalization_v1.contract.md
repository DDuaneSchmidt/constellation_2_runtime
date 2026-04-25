# position_normalization_v1.contract.md

Contract owner:
- `constellation_2/common/position_normalization_v1.py`

Purpose:
- provide one canonical runtime truth surface for open-position normalization before exit management
- distinguish `NATIVE` positions from `IMPORTED` positions explicitly
- preserve explicit risk basis so imported positions cannot silently masquerade as native positions

Canonical path:
- `truth/positions_v1/normalization_v1/<DAY_UTC>/<POSITION_ID>/position_normalization.v1.json`

Writer:
- `ops/tools/run_position_normalization_v1.py`

Required semantics:
- every artifact MUST classify `origin` as `NATIVE` or `IMPORTED`
- every artifact MUST classify `risk_basis` as `R_NATIVE`, `R_SYNTHETIC`, or `UNMANAGED`
- imported positions MUST remain excluded from scoring by default unless explicitly promoted later
- imported positions MUST NOT enter exit management unless `normalization_status = NORMALIZED`
- synthetic risk MUST remain explicitly labeled through `risk_basis` and `synthetic_risk_labeled`

Fail-closed rules:
- missing imported reference-entry evidence MUST block normalization
- missing imported synthetic-stop evidence MUST block normalization
- missing imported synthetic 1R evidence MUST block normalization
- native positions with incomplete initial-risk evidence MUST not be silently treated as `R_NATIVE`
