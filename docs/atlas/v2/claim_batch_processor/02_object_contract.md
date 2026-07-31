# Object Contract

ClaimBatch records the input boundary, source types, claim IDs, and pipeline version.

ClaimBatchRun records the completed pipeline steps, emitted record counts, and the compressed record IDs created by the run.

ClaimBatchMetrics records intake count, unique claim count, unique mechanism count, duplicate ratio, mechanism distribution, contrarian coverage, and cheap experiment eligibility coverage.

ClaimBatchMetrics.cheap_experiment_candidates contains mechanism IDs, not trade candidates.
