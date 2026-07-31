# Atlas V2 State Machine

Atlas V2 state transitions are evidence records, not silent mutations.

Each transition requires:

- `transitioned_at`
- `from_status`
- `to_status`
- `reason`
- `triggering_object`

Minimum status vocabulary:

- `recorded`
- `pending`
- `evaluated`
- `matched`
- `mismatched`
- `unknown`
- `failed`
- `accepted`
- `rejected`
- `superseded`

Unknown is used when the system lacks enough outcome evidence. Failed is used only when evidence supports failure. No transition may collapse unknown into failed.
