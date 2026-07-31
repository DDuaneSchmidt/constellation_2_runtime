# AEGIS Missing Market Data Requirement Resolver Spec V1

## Artifact

Family: `aegis_missing_market_data_requirement_resolver_v1`

Path: `truth/reports/aegis_missing_market_data_requirement_resolver_v1/<TARGET_DAY>/missing_market_data_requirement_resolver.v1.json`

## Inputs

The resolver reads T02 dormant sleeve diagnostics, sleeve input contracts, candidate generation diagnostics, sleeve evaluation rollups, market data inputs/readiness/coverage/demand, provider attempts/capabilities, and the canonical market data snapshot manifest.

## Row Contract

Each row contains `sleeve_id`, `sleeve_name`, `hypothesis_id`, `blocker_from_t02`, itemized `required_market_data_items`, required fields, expected source/source type, source existence, ingestion, freshness, routing, quality, missing/stale/unsupported items, `resolution_status`, `resolution_type`, `owner`, `david_action_required`, and `next_action`.

## Owner Rules

`DAVID` is used only when an external source, file, vendor, credential, subscription, or API access must be provided. `AEGIS_SYSTEM` is used for code, routing, manifest, or ingestion repairs. `NONE` is used when no action is required.
