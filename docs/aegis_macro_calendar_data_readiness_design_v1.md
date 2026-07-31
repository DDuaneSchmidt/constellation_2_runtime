# Aegis Macro Calendar Data Readiness Design v1

## Design

The readiness builder is a deterministic research-only report. It reads one governed macro calendar source artifact, validates the data contract, and writes a day-scoped readiness result.

The artifact separates two questions:

1. Does Aegis have a governed macro event calendar source?
2. Is that source complete enough for Macro Calendar to re-enter automatic shadow validation?

It does not answer whether the hypothesis is valid, profitable, investable, or ready for capital review.

## Source Contract

The source artifact must be explicit. Aegis does not scrape, synthesize, or infer macro event values. `actual`, `consensus`, and `prior` must be present in the source row. `affected_assets` must be a non-empty list or delimited string. `release_datetime` and `source_timestamp` must be parseable timestamps.

## Downstream Rule

When readiness is not `READY`, Macro Calendar remains `NEEDS_DATA` and the David action queue keeps `PROVIDE_DATA_SOURCE`.

When readiness is `READY`, workflow state may move Macro Calendar from `NEEDS_DATA` to `READY_FOR_SHADOW_TRIAL` with `next_action: WAIT_FOR_AUTOMATIC_PROCESSING`. Research quality and follow-through stop reporting the macro event calendar as a missing data source. Shadow validation is still responsible for its own pass/fail result on the next run.

## UI

Command Center and Research render a dedicated `Macro Calendar Data Readiness` section from the artifact. If `NEEDS_SOURCE`, the UI shows:

`Macro Calendar needs a governed macro event calendar source.`

with the buttons:

`Connect Source`, `Upload Dataset`, `Mark Not Available`, `Defer`

Buttons are visibility-only unless backed by existing operator action event routing. They do not execute trades, allocate capital, or mutate broker systems.

## Safety

The builder emits safety fields confirming no broker execution, no live trading, no trade advice, no real capital, no autonomous execution, no order management, and no safety gate changes.
