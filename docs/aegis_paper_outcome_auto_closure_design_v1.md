# Aegis Paper Outcome Auto-Closure Design v1

Date: 2026-06-01

## Design

The auto-closure layer is a read-only derived evidence builder. It consumes the event-sourced paper position ledger and deterministic exit recommendations, emits a separate paper outcome auto-closure artifact, and allows the outcome registry to classify eligible rows as closed research outcomes.

It does not mutate the paper position ledger, submit broker orders, produce trade advice, or close any real-world position.

## Modules

- `ops/aegis/paper_outcome_auto_closure_v1.py`: builds paper-only auto-closure rows and the David manual review queue.
- `ops/tools/build_aegis_paper_outcome_auto_closure_v1.py`: CLI wrapper.
- `ops/aegis/outcome_registry_v1.py`: consumes auto-closure rows as governed closure evidence.
- `ops/aegis/validation_sample_generator_v1.py`: existing sample logic includes eligible closed outcomes.
- Command Center operator envelope and renderer: display counts and manual review queue visibility.

## Deterministic Construction

For every open paper position, the builder joins by `position_id` and `candidate_id` to the exit recommendation artifact. The deterministic sort key is sleeve, symbol, position id, and recommendation id.

For a non-HOLD recommendation, closure is allowed only if the position contains certified mark metadata:

- `mark_certification_status = CERTIFIED`
- `mark_price` or `current_certified_mark`
- `mark_timestamp_utc`
- `mark_source_path`
- `mark_source_hash`

The deterministic realized return formula is `LONG_EQUITY_RETURN_V1`: `(exit_mark - entry_mark) / entry_mark` for long/buy observations. The output records the formula version and a rerun stability key. Reruns over unchanged source artifacts must produce the same realized return.

## Manual Review Boundary

The David manual review queue is separate from auto-closure. It says that a paper observation closed by rule and may be reviewed manually only if David has a corresponding real-world position outside Aegis. It must explicitly state that the queue row is not trade advice and not broker execution.

## Failure Modes

- Missing exit recommendation.
- HOLD recommendation.
- Missing entry mark.
- Missing exit mark.
- Exit mark not certified.
- Missing exit mark timestamp.
- Missing exit source artifact or hash.
- Policy authority missing or defaulted when explicit authority is required.
- Runtime safety flags unexpectedly allow broker, live, advice, or autonomous execution.
- Realized return is not rerun-stable.

## Audit Behavior

The builder is deterministic after timestamp normalization. Outcome validation consumes the auto-closure artifact before building validation samples. Audit must remain READY, and safety flags must remain disabled.
