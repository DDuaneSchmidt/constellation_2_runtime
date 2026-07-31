# Mechanism Registry

The mechanism registry aggregates supplied claims by mechanism family.

`MechanismRegistry` fields:

- `mechanism_id`
- `mechanism_family`
- `claim_count`
- `source_count`
- `contrarian_count`
- `cheap_experiment_count`
- `last_updated`

Opening-range aliases are clustered into `OPENING_RANGE`, including:

- ORB Strategy
- Sneaky Pivot
- Opening Range Breakout
- Opening Range Retest

The registry counts mechanism reuse. It does not authorize trading, validation, recommendation, allocation, sleeve construction, candidate creation, or paper position creation.
