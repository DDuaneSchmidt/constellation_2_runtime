# AEGIS Missing Market Data Requirement Resolver Design V1

## Design

The resolver scopes itself from T02 rows with `dormant_reason_code == MISSING_MARKET_DATA`. It does not discover new sleeves and does not execute producers.

For each scoped sleeve it derives market requirements from sleeve input contracts, candidate readiness rows, sleeve evaluation active symbols, producer missing input paths, and producer reason codes such as `MISSING_BAR_FOR_DAY`. It then joins these requirements to runtime truth market artifacts.

## Classification

Classification is item-first and sleeve rollup second. Item diagnostics determine whether a configured source exists, whether data has been ingested, whether the data is fresh, whether it is routed to the sleeve, and whether required fields or day bars are complete. The sleeve `resolution_type` is the highest-priority unresolved item category.

## Safety

The builder writes only its own report artifact. T02 integration is a downstream evidence link only and does not change dormant reason classification.
