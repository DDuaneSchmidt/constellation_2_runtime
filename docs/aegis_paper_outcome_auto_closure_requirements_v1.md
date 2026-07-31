# Aegis Paper Outcome Auto-Closure Requirements v1

Date: 2026-06-01

## Purpose

Automatically close paper-research outcomes when deterministic exit evidence exists, so research validation can accumulate reproducible closed samples without waiting for a human to record a paper-only outcome.

## Scope

This is paper-research outcome closure only. It does not implement or enable trade advice, broker execution, broker submit/transmit, live trading, autonomous execution, automatic real-world trading, or automatic real-world position management.

## Required Flow

Paper Position -> Exit Recommendation -> AUTO_CLOSED_PAPER_OUTCOME -> Outcome Registry Closed Outcome -> Validation Sample -> David Manual Action Queue item, if relevant.

## Eligibility Requirements

A paper observation may auto-close only when:

- the paper position is open
- exit evaluation was performed
- exit recommendation is deterministic and not HOLD
- entry mark exists
- exit reference mark is available and certified
- exit mark has source artifact, source hash, and timestamp
- policy authority is explicit and not mismatched
- realized return is deterministic and rerun-stable
- all trading, broker, live, autonomous, and advice safety flags remain disabled
- closure is explicitly research-only

## Block Conditions

A paper observation must not auto-close when:

- recommendation is HOLD
- entry mark is missing
- exit mark is missing
- exit mark is not certified
- market evidence is stale or incomplete
- return calculation is non-deterministic
- policy authority is invalid, missing, or mismatched
- any record implies broker action, trade advice, live trading, or autonomous execution

## Lifecycle States

- `AUTO_CLOSED_PAPER_OUTCOME`
- `AUTO_CLOSURE_BLOCKED`
- `AUTO_CLOSURE_NOT_ELIGIBLE`

## Manual Review Queue

A separate David manual review queue must list paper exits that may warrant external manual review. This queue is not a trade recommendation and not broker execution. It exists only so David can recognize paper exits that may correspond to real-world positions outside Aegis.

## Acceptance

For TARGET_DAY=2026-06-01, current expected behavior is:

- 55 positions evaluated
- 53 HOLD positions remain open
- BKSY auto-closes as a paper take-profit outcome
- ARM auto-closes as a paper stop-loss outcome
- closed outcomes increase by 2
- validation samples include eligible closed outcomes
- manual review queue includes BKSY and ARM
- no broker execution, autonomous execution, live trading, or trade advice is enabled
- audit remains READY with zero audit blockers
