# Aegis Workflow State Resolver Design v1

## Architecture

The Phase 2 resolver is a deterministic rule artifact emitted beside workflow state. The workflow builder loads proposal, evidence, shadow, promotion, approval, paper setup, outcomes, validation, sufficiency, and research allocation artifacts, then applies resolver rules in precedence order.

## State Aging

Each workflow row records:

- `state_entered_at_utc`
- `time_in_state_days`
- `stale_state_warning`
- `stale_state_reason`
- `previous_state_hash`
- `current_state_hash`

State age is computed from deterministic artifact timestamps and `TARGET_DAY`, not wall-clock time.

## Replay Verification

`aegis_hypothesis_workflow_replay_verification_v1` rebuilds workflow state and the David action queue from the same inputs. It verifies state counts, per-hypothesis states, action queue signatures, and content hash stability excluding deterministic timestamp fields.

## Safety Boundary

The resolver can only classify research workflow state. It cannot create broker orders, recommend trades, allocate real capital, enable live trading, or manage real-world positions.
