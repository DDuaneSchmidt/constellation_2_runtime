# Taxonomy Recall Results 001

Date: 2026-06-05
Status: GENERATED_ONLY
Mode: evaluation-only, offline-only

## Purpose

Measure whether Atlas Failure Taxonomy improves Research Adversary recall against the frozen historical failure evaluation set.

This report does not create candidates, run replay, change qualification, change governance, allocate capital, recommend trades, execute through a broker, or write memory automatically.

## Inputs

- `research_journal/evaluation_sets/research_adversary/historical_failure_eval_set_001.json`
- `reports/atlas_v2_research_os/research_adversary_validation/failure_recall_study_001.json`
- `research_journal/reports/assumption_recall_study_001.md`
- `research_journal/reports/atlas_failure_taxonomy_validation_001.md`
- `reports/atlas_v2_research_os/research_adversary_corpus/corpus_index.json`

## Method

Baseline run: replayed the frozen Research Adversary baseline labels without taxonomy fields.

Taxonomy-enhanced run: applied the Atlas Failure Taxonomy mapping from the frozen taxonomy validation report to the same 19 historical failures. HIGH-confidence mappings receive direct-hit credit. MEDIUM-confidence mappings receive partial-hit credit because they require human confirmation or have category ambiguity.

Scoring formula:

```text
weighted_recall = (direct_hits + 0.5 * partial_hits) / total_cases
```

## Results

| Metric | Baseline | Taxonomy-Enhanced | Delta | Judgment |
| --- | ---: | ---: | ---: | --- |
| failure recall | 23.7% | 86.8% | +63.1 pp | PASS |
| direct hits | 0 | 14 | +14 | PASS |
| partial hits | 9 | 5 | -4 | PASS |
| misses | 10 | 0 | -10 | PASS |
| assumption recall | 39.1% | 81.2% | +42.1 pp | PASS |
| constraint recall | 13.2% | 86.8% | +73.6 pp | PASS |
| false positives | 0 | 0 | +0 | PASS |
| authority violations | 0 | 0 | 0 | PASS |
| output status | GENERATED_ONLY | GENERATED_ONLY | unchanged | PASS |

## Case Results

| Failure | Baseline Recall | Taxonomy Recall | Taxonomy Category | Confidence |
| --- | --- | --- | --- | --- |
| `FAIL_0004` | MISS | DIRECT | `EVIDENCE_MATURITY_CONFUSION` | HIGH |
| `FAIL_0005` | MISS | DIRECT | `AUTHORITY_READINESS_CONFUSION` | HIGH |
| `FAIL_0006` | PARTIAL | DIRECT | `OUTCOME_MATURITY_GAP` | HIGH |
| `FAIL_0007` | MISS | DIRECT | `CANDIDATE_VOLUME_QUALITY_CONFUSION` | HIGH |
| `FAIL_0008` | PARTIAL | DIRECT | `FIXTURE_BASELINE_GENERALIZATION` | HIGH |
| `FAIL_0009` | PARTIAL | PARTIAL | `FLOW_CONVERSION_MISDIAGNOSIS` | MEDIUM |
| `FAIL_0010` | MISS | PARTIAL | `AUTHORITY_READINESS_CONFUSION` | MEDIUM |
| `FAIL_0011` | MISS | DIRECT | `RUNTIME_DEPENDENCY_READINESS` | HIGH |
| `FAIL_0012` | PARTIAL | PARTIAL | `DATA_READINESS_GAP` | MEDIUM |
| `FAIL_0013` | MISS | PARTIAL | `AUDIT_UNDERSTANDING_CONFUSION` | MEDIUM |
| `FAIL_0014` | PARTIAL | DIRECT | `FLOW_CONVERSION_MISDIAGNOSIS` | HIGH |
| `FAIL_0015` | MISS | DIRECT | `WARNING_INTEGRITY_RECURRENCE` | HIGH |
| `FAIL_0016` | PARTIAL | DIRECT | `FIXTURE_BASELINE_GENERALIZATION` | HIGH |
| `failure-demo-opening-range` | PARTIAL | PARTIAL | `REGIME_MISMATCH` | MEDIUM |
| `failure-2026-06-04-worker-no-compatible-connected-worker-c5b07de5e574` | PARTIAL | DIRECT | `WORKER_COMPATIBILITY_GAP` | HIGH |
| `failure-2026-06-05-autonomous_research-artifactstoreerror-64d655a35e43` | MISS | DIRECT | `ARTIFACT_LINEAGE_GAP` | HIGH |
| `failure-2026-06-05-autonomous_research-artifactstoreerror-c54059bb338d` | PARTIAL | DIRECT | `ARTIFACT_LINEAGE_GAP` | HIGH |
| `failure-2026-06-05-autonomous_research-safety-gate-failed-794b177f0415` | MISS | DIRECT | `SAFETY_CERTIFICATION_GATE_GAP` | HIGH |
| `failure-2026-06-05-certification-certification-block-0d8cd9475db6` | MISS | DIRECT | `SAFETY_CERTIFICATION_GATE_GAP` | HIGH |

## False Positives And Ambiguity

False positives are counted only when the taxonomy produces a non-material or incorrect objection. No incorrect category objections were found in this frozen mapping.

Ambiguity is still material:

- ambiguous taxonomy cases: 5
- multi-category cases needing reviewer confirmation: 6

These are not counted as false positives, but they are the main reason the decision is EXPAND with refinement requirements rather than unrestricted expansion.

## Decision

Decision output: EXPAND

Rationale: taxonomy-enhanced recall materially improves over the 23.7% failure-recall baseline, assumption recall improves over 39.1%, constraint recall improves, false positives remain 0 under the frozen mapping, authority violations remain 0, and generated outputs remain GENERATED_ONLY.

Required refinement before broader use:

- add precedence rules for ambiguous categories
- separate authority readiness from evidence maturity when both appear
- separate data readiness gaps from artifact lineage gaps
- keep secondary categories as human-review hints, not decisions
- keep all outputs GENERATED_ONLY

## Authority Boundary

- no candidate authority
- no replay authority
- no qualification authority
- no governance authority
- no capital authority
- no trade recommendation authority
- no broker execution authority
- no position sizing authority
- no automatic memory writes

This evaluation does not modify original failure records, adversary corpus artifacts, candidate state, replay state, qualification state, governance state, paper-forward state, or memory state.
