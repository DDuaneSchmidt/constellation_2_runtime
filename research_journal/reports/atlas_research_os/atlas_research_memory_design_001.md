# Atlas Research Memory Design 001

Objective: design the Atlas Research Memory Index / Knowledge Graph so Atlas can preserve institutional memory about prior mechanisms, claims, hypotheses, experiments, failures, regimes, and reopen decisions.

Scope: design only. This report does not implement Atlas Research Memory, add a vector database, add external infrastructure, modify Atlas tools, modify runtime truth, modify Aegis behavior, create candidates, allocate capital, create paper positions, create trading artifacts, or alter production code.

Runtime truth note: this design treats the verified runtime graph and runtime truth kernel as the source of runtime readiness. Research memory is a read-only research aid until separately implemented, tested, declared, and verified. No consumer may invent truth from memory records.

## 1. Memory Objects

Atlas Research Memory should model institutional knowledge as typed, evidence-linked memory objects. Each object should carry enough source lineage to support reuse without pretending that memory itself is validation.

### Mechanism

A `Mechanism` is the abstract market behavior being studied, independent of one ticker or one artifact.

Required fields:
- `mechanism_id`
- `mechanism_tag`
- `name`
- `description`
- `canonical_terms`
- `related_terms`
- `supported_regimes`
- `known_failure_patterns`
- `source_artifacts`
- `created_at`
- `updated_at`

Purpose:
- Group related ideas across tickers, timeframes, sources, and wording.
- Prevent Atlas from treating a symbol-specific restatement as a new idea when the mechanism has already been weak, falsified, retired, or regime-limited.

### ClaimCluster

A `ClaimCluster` groups semantically similar claims before they become hypotheses.

Required fields:
- `claim_cluster_id`
- `canonical_claim`
- `claim_variants`
- `mechanism_tags`
- `scope`
- `source_artifacts`
- `evidence_status`
- `generated_only`
- `mock_only`
- `dedupe_keys`
- `created_at`
- `updated_at`

Purpose:
- Track repeated claims with different wording.
- Preserve labels such as `GENERATED_ONLY` so generated claims cannot be laundered into validated memory.

### HypothesisCluster

A `HypothesisCluster` groups related hypotheses that test the same core claim or mechanism.

Required fields:
- `hypothesis_cluster_id`
- `canonical_hypothesis`
- `hypothesis_variants`
- `mechanism_tags`
- `timeframes`
- `instrument_scope`
- `regime_context`
- `linked_claim_clusters`
- `linked_experiment_clusters`
- `status`
- `source_artifacts`
- `created_at`
- `updated_at`

Purpose:
- Detect when a new hypothesis is only a timeframe, ticker, or wording variant of prior work.
- Support reopening only when the proposed new context differs materially from the retired or failed context.

### ExperimentCluster

An `ExperimentCluster` groups equivalent or near-equivalent experiment designs.

Required fields:
- `experiment_cluster_id`
- `canonical_design`
- `design_variants`
- `mechanism_tags`
- `instrument_scope`
- `timeframe`
- `regime_context`
- `entry_conditions`
- `exit_conditions`
- `measurement_window`
- `success_metric`
- `failure_metric`
- `source_artifacts`
- `result_summary`
- `created_at`
- `updated_at`

Purpose:
- Prevent repeated execution of duplicate cheap experiments.
- Make failed setups visible before new backlog items are created.

### FailurePattern

A `FailurePattern` records repeated or important failure modes.

Required fields are defined in section 4:
- `failure_id`
- `failure_type`
- `source_artifacts`
- `mechanism`
- `regime_context`
- `reason`
- `evidence_level`
- `repetition_count`
- `last_seen_at`
- `retirement_status`

Purpose:
- Preserve negative knowledge as first-class memory.
- Answer "what should not be repeated?" without forcing every future researcher to rediscover the same weakness.

### RegimeContext

A `RegimeContext` captures the market/session context in which a claim, experiment, or failure occurred.

Required fields:
- `regime_context_id`
- `labels`
- `description`
- `evidence_source`
- `time_bounds`
- `instrument_scope`
- `confidence`

Purpose:
- Distinguish broadly falsified ideas from regime-specific failures.
- Support reopening when the original test context was too narrow or a materially different regime is now in scope.

### EvidenceTrail

An `EvidenceTrail` is the lineage bundle for a memory object.

Required fields:
- `evidence_trail_id`
- `source_artifacts`
- `source_types`
- `claim_refs`
- `hypothesis_refs`
- `experiment_refs`
- `failure_refs`
- `runtime_truth_refs`
- `label_integrity`
- `created_at`
- `updated_at`

Purpose:
- Keep every memory record traceable.
- Separate generated, mock, observed, tested, falsified, retired, and reopened knowledge.

### LearningNode

A `LearningNode` is a distilled lesson derived from evidence-linked claims, experiments, and failures.

Required fields:
- `learning_node_id`
- `lesson`
- `mechanism_tags`
- `regime_context`
- `supporting_evidence_trails`
- `contradicting_evidence_trails`
- `confidence`
- `allowed_uses`
- `prohibited_uses`
- `created_at`
- `updated_at`

Purpose:
- Give Atlas concise reusable lessons without allowing unsupported authority.
- Express what the system learned and where the learning is limited.

### RetiredKnowledge

`RetiredKnowledge` records knowledge that should not influence candidate generation, backlog creation, or positive prioritization unless reopened.

Required fields:
- `retired_knowledge_id`
- `retired_object_refs`
- `retirement_reason`
- `retirement_scope`
- `mechanism_tags`
- `regime_context`
- `evidence_level`
- `retired_at`
- `reopen_conditions`
- `prohibited_uses`

Purpose:
- Prevent repeated weak ideas from returning under new wording.
- Make retirement scoped and reversible only through explicit reopening.

### ReopenedKnowledge

`ReopenedKnowledge` records an explicit decision to reconsider previously retired or stale knowledge.

Required fields:
- `reopened_knowledge_id`
- `retired_knowledge_ref`
- `reopen_reason`
- `new_regime_context`
- `new_evidence_trail`
- `material_difference`
- `allowed_scope`
- `opened_at`
- `review_due_at`
- `status`

Purpose:
- Allow disciplined reconsideration when new evidence, changed regime, improved data, or better experiment design justifies it.
- Preserve the fact that the idea was previously retired.

## 2. Semantic Deduplication

Atlas v0.1 should use simple repo-native deduplication before any embedding or vector database dependency. The goal is not perfect semantic search. The goal is to catch obvious repeated ideas and route ambiguous cases to human review.

### Canonicalization

Each memory object should maintain deterministic dedupe keys:
- `normalized_text_key`: lowercased text with punctuation stripped, stopwords reduced, whitespace normalized, and known aliases expanded.
- `mechanism_key`: sorted mechanism tags plus normalized mechanism phrase.
- `instrument_scope_key`: ticker, asset class, sector, or `CROSS_INSTRUMENT` scope.
- `timeframe_key`: normalized session, intraday window, holding period, and bar interval.
- `regime_key`: sorted regime labels.
- `design_key`: normalized entry conditions, exit conditions, measurement window, and success metric.
- `source_key`: stable source artifact paths and record IDs.

Repo-native v0.1 implementation concept:
- JSON indexes stored in the repo.
- Deterministic normalization rules stored as plain configuration.
- Exact and near-exact matching on normalized keys.
- Token overlap or Jaccard-style similarity for ambiguous text matches.
- Manual `needs_review` flag when a match is plausible but not deterministic.

### Same Claim, Different Wording

Detection rules:
- Compare `normalized_text_key` and high-overlap token sets.
- Expand aliases such as "opening drive", "opening range move", and "first-session breakout" into canonical mechanism vocabulary.
- Match claims when the causal direction, condition, and outcome are equivalent even if phrasing differs.

Output:
- Link to existing `ClaimCluster`.
- Add new wording to `claim_variants`.
- Preserve source label, including `GENERATED_ONLY` or `mock_only`.

### Same Mechanism, Different Ticker

Detection rules:
- Compare `mechanism_key` first.
- Treat ticker changes as scope changes, not new mechanisms.
- Link symbol-specific examples under the same `Mechanism` unless the instrument class or liquidity regime materially changes the behavior.

Output:
- Update `instrument_scope` and `source_artifacts`.
- Flag for review if the new ticker has a materially different regime, liquidity profile, or event exposure.

### Same Hypothesis, Different Timeframe

Detection rules:
- Compare canonical hypothesis text, mechanism tags, entry/exit logic, and success metric.
- Treat timeframe differences as variants when the mechanism and test logic are equivalent.
- Treat timeframe differences as new scope only when the measurement window changes the causal claim.

Output:
- Link to existing `HypothesisCluster`.
- Add the new timeframe to `timeframes`.
- Require explicit `material_difference` if the existing cluster is retired.

### Repeated Failed Setup

Detection rules:
- Compare `design_key`, `mechanism_key`, `regime_key`, and failure reason.
- Match even if the proposed source is new when the entry conditions, exit conditions, and expected edge are effectively the same.

Output:
- Link to `FailurePattern`.
- Increase `repetition_count` only after evidence confirms the setup was repeated.
- Block positive influence if the failure is retired and not reopened.

### Duplicate Experiment Design

Detection rules:
- Compare normalized experiment design components:
  - entry conditions
  - exit conditions
  - instrument scope
  - timeframe
  - measurement window
  - success metric
  - required data
- Flag near-duplicates when only ticker, sample date, or wording changed.

Output:
- Link to existing `ExperimentCluster`.
- Recommend reuse of prior result or a scoped retest justification.

### Stale Idea Reappearing

Detection rules:
- Compare new claims and hypotheses against `retired_knowledge.json`, `failure_patterns.json`, and old `ClaimCluster` records.
- Check `last_seen_at`, `retirement_status`, and `reopen_conditions`.
- Flag stale ideas when no new evidence, regime shift, data improvement, or design improvement is supplied.

Output:
- Return `STALE_PRIOR_IDEA`.
- Attach the prior failure or retirement reason.
- Require `ReopenedKnowledge` before the idea can affect candidate generation or positive backlog priority.

## 3. Mechanism Grouping

Atlas v0.1 should support these mechanism tags:
- `BREAKOUT`
- `MEAN_REVERSION`
- `OPENING_RANGE`
- `SESSION_TIMING`
- `VWAP_OR_AVERAGE_RECLAIM`
- `VOLATILITY_EXPANSION`
- `LIQUIDITY_SWEEP`
- `TREND_CONTINUATION`
- `REVERSAL`
- `EVENT_REACTION`

Mechanism tag rules:
- Tags are non-exclusive. A hypothesis can combine `OPENING_RANGE`, `VOLATILITY_EXPANSION`, and `TREND_CONTINUATION`.
- Tags describe the mechanism, not validation status.
- Tags must not imply tradability, readiness, or allocation.
- Extension is allowed through a future mechanism registry update with definitions, aliases, examples, and dedupe rules.

Recommended tag fields:
- `tag`
- `definition`
- `aliases`
- `included_patterns`
- `excluded_patterns`
- `common_failure_modes`
- `compatible_regime_labels`
- `created_at`
- `updated_at`

## 4. Failure Memory

Failures should be stored as durable negative knowledge, not as incidental notes. Atlas should preserve enough structure to identify repeat failure patterns while keeping evidence labels intact.

Required failure fields:
- `failure_id`
- `failure_type`
- `source_artifacts`
- `mechanism`
- `regime_context`
- `reason`
- `evidence_level`
- `repetition_count`
- `last_seen_at`
- `retirement_status`

Recommended optional fields:
- `failed_claim_refs`
- `failed_hypothesis_refs`
- `failed_experiment_refs`
- `dedupe_keys`
- `first_seen_at`
- `severity`
- `reopen_conditions`
- `prohibited_uses`
- `notes`

Failure type examples:
- `WEAK_EDGE`
- `DUPLICATE_IDEA`
- `REGIME_MISMATCH`
- `INSUFFICIENT_EVIDENCE`
- `MOCK_ONLY_EVIDENCE`
- `GENERATED_ONLY_EVIDENCE`
- `DATA_GAP`
- `NOISY_SIGNAL`
- `OVERFIT_SETUP`
- `UNSUPPORTED_CAUSAL_CLAIM`
- `REPEATED_FAILED_SETUP`

Evidence levels:
- `GENERATED_ONLY`
- `MOCK_ONLY`
- `OBSERVED`
- `TESTED`
- `FALSIFIED`
- `VALIDATED_LIMITED_SCOPE`
- `RETIRED`

Retirement status:
- `ACTIVE_WARNING`
- `RETIRED_DO_NOT_REPEAT`
- `REOPENED_LIMITED_SCOPE`
- `SUPERSEDED`

Rules:
- `mock_only` failures can warn about test quality but cannot become validated memory.
- `generated_only` failures can warn about idea quality but must remain labeled.
- A failure with `RETIRED_DO_NOT_REPEAT` blocks positive priority influence unless a matching `ReopenedKnowledge` record exists.

## 5. Regime Context

Minimum regime labels:
- `trend`
- `chop`
- `high_volatility`
- `low_volatility`
- `opening_session`
- `midday_session`
- `closing_session`
- `news_event`
- `low_liquidity`
- `high_liquidity`

Regime usage rules:
- Labels are descriptive context, not truth claims about readiness.
- A memory object may carry multiple labels.
- Regime labels should be evidence-linked where possible.
- Unknown or weakly supported regime should be recorded as `confidence: LOW`, not inferred as fact.
- Reopening a retired idea requires explaining why the new regime differs from the failed or retired context.

Recommended regime confidence:
- `LOW`: weak inference, incomplete context, or source-only assertion.
- `MEDIUM`: supported by observable context or repeated source artifacts.
- `HIGH`: supported by direct evidence artifacts and consistent classification rules.

## 6. Memory Influence Rules

Memory may influence research workflow only inside bounded, non-authoritative surfaces.

### Allowed Influence

Priority scoring:
- Memory may reduce priority for duplicated, stale, retired, or repeatedly failed ideas.
- Memory may increase research priority for unresolved high-value claims only when evidence labels permit it.
- Memory must expose the evidence trail behind any scoring influence.

Duplicate detection:
- Memory may link new claims, hypotheses, experiments, and failures to existing clusters.
- Ambiguous matches should return `needs_review` rather than silently merging.

Backlog creation:
- Memory may warn that a backlog item duplicates a retired or failed idea.
- Memory may suggest a scoped retest only when a material difference is documented.
- Memory must not create trading candidates.

Lifecycle transitions:
- Memory may provide supporting context for research lifecycle review.
- Memory must not independently promote, validate, retire, or reopen runtime objects.
- Any lifecycle transition must remain governed by the relevant runtime truth, journal, and workflow rules.

Hypothesis reopening:
- Memory may identify retired knowledge that deserves review when new evidence, new data, changed regime, or improved experiment design is present.
- Reopening requires a `ReopenedKnowledge` record with explicit `material_difference`.
- Reopened knowledge remains limited to its approved scope.

### Hard Rules

- Retired knowledge cannot influence candidate generation unless reopened.
- Generated-only knowledge must remain labeled.
- Mock-only learning cannot become validated memory.
- Memory cannot validate a hypothesis, claim, candidate, sleeve, paper position, or allocation.
- Memory cannot infer runtime readiness from code.
- Memory cannot override verified runtime graph, runtime truth kernel, or governed workflow state.
- Memory cannot convert stale or failed ideas into positive backlog pressure without a reviewable reopen record.

## 7. Storage Recommendation

For v0.1, use repo-native JSON files:

```text
reports/atlas_v2_research_os/memory/
  memory_index.json
  mechanism_clusters.json
  failure_patterns.json
  retired_knowledge.json
```

Recommended responsibilities:
- `memory_index.json`: top-level object index, object refs, dedupe keys, evidence labels, and update timestamps.
- `mechanism_clusters.json`: mechanism definitions, aliases, supported tags, related clusters, and common failure modes.
- `failure_patterns.json`: durable failure records and repeated setup warnings.
- `retired_knowledge.json`: retired object refs, retirement scope, reopen conditions, and prohibited uses.

Recommended v0.1 design properties:
- Deterministic JSON serialization.
- Stable IDs.
- Source artifact paths instead of embedded source copies.
- Label fields for `GENERATED_ONLY`, `mock_only`, tested, falsified, retired, and reopened state.
- Manual review flags for ambiguous dedupe decisions.
- No external service dependency.
- No vector database requirement.

Recommended index shape:

```json
{
  "schema_version": "atlas_research_memory.v0.1",
  "updated_at": "ISO-8601",
  "objects": [],
  "dedupe_keys": {},
  "source_artifact_index": {},
  "mechanism_index": {},
  "regime_index": {},
  "review_queue": []
}
```

## 8. Future Upgrade Path

Atlas should upgrade storage only when repo-native JSON becomes the bottleneck.

### SQLite

Consider SQLite when:
- JSON read/write conflicts become common.
- Queries need joins across claims, hypotheses, experiments, failures, regimes, and evidence trails.
- Review queues need transactional updates.
- Memory object count grows enough that full JSON scans become noisy or slow.

SQLite would still be repo-local and should be the first storage upgrade before external services.

### Embeddings

Consider embeddings when:
- Deterministic alias and token-overlap matching misses too many paraphrases.
- Claim wording diversity becomes high enough to create repeated human review overhead.
- Atlas needs semantic recall across long natural-language lessons.

Embedding results must remain advisory. They should produce candidate matches for review, not authoritative merges.

### Graph Database

Consider a graph database when:
- Multi-hop relationship queries become central to research workflow.
- The system needs frequent traversal across mechanisms, regimes, failures, evidence trails, retired knowledge, and reopen decisions.
- Repo-local graph exports are no longer adequate for analysis.

A graph database should not be introduced until the relationship model has stabilized in JSON or SQLite.

### Vector Store

Consider a vector store when:
- Embedding volume exceeds what can be handled with local files or SQLite-backed vectors.
- Recall over large unstructured evidence text becomes a primary workflow.
- The cost of missed duplicates exceeds the operational complexity of an external store.

A vector store must not be required for v0.1. If introduced later, it must preserve source lineage, evidence labels, retirement status, and runtime-truth boundaries.

## Acceptance Check

This design creates one report only. It does not implement memory storage, create external infrastructure, modify production code, create candidate artifacts, create capital artifacts, create trading artifacts, or alter Aegis runtime behavior.
