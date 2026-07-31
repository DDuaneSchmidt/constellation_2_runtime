# Aegis Oil Shock Candidate Construction Design v1

## Design

The construction artifact is a read-only audit/bridge between generated-hypothesis approval evidence and the existing Oil Shock producer. It does not create candidates or paper observations.

The producer consumes the construction result before market setup evaluation:

1. If construction is `POLICY_INCOMPLETE`, fail closed with missing construction fields.
2. If construction is ready, evaluate the existing Oil Shock event rules.
3. If no event qualifies, emit `NO_MARKET_SETUP`.
4. If an event qualifies, emit a raw signal through the existing intent path only; candidate contracts and downstream lifecycle gates remain authoritative.

## Safety

This design is research-only. It does not change strategy economics, candidate contracts, entry-price certification, paper lifecycle, outcome validation, broker behavior, trading behavior, or safety gates.


## Paper Setup Bridge Integration

Oil Shock candidate construction consumes `aegis_generated_hypothesis_paper_setup_bridge_v1`. When the bridge reports `PAPER_SETUP_BRIDGE_READY`, construction may use its governed `sleeve_id`, `risk_policy_id`, `exit_policy_id`, `approval_event_hash`, and paper setup status. When the bridge is blocked, construction remains fail-closed and must not create raw signals or candidates.

## Signal-to-Candidate Proof

The signal-to-candidate proof runs after generated-hypothesis producers and after the candidate-contract pipeline has been regenerated. It detects stale downstream contract artifacts when a valid Oil Shock signal is present in the signal evidence graph but missing from candidate contracts.
