# Taxonomy Integration Evaluation 001

Date: 2026-06-05
Status: GENERATED_ONLY
Mode: evaluation-only, offline only

## Purpose

Pilot Atlas Failure Taxonomy annotations inside Research Adversary generation to determine whether taxonomy lookup and pattern matching improve review quality without expanding authority.

## Scope

Allowed surface:

- ResearchAdversaryReview optional schema fields
- Review generation template annotations
- Evaluation readout fields
- Tests

Forbidden authority remains unchanged:

- replay authority: not authorized
- qualification authority: not authorized
- candidate authority: not authorized
- governance authority: not authorized
- capital authority: not authorized

All outputs remain GENERATED_ONLY. Taxonomy categories may explain suspected failure modes, but they cannot approve, reject, qualify, promote, place, size, allocate, or override anything.

## Added Evaluation Fields

- suspected_taxonomy_categories: optional generated-only category labels such as REGIME_DEPENDENCY, PROXY_DEPENDENCY, WARNING_RECURRENCE, CERTIFICATION_BLOCKER, DATA_COVERAGE_GAP
- taxonomy_confidence: optional bounded score from 0.0 to 1.0 for category suspicion only
- related_failure_patterns: optional generated-only references to historical failure patterns
- taxonomy_reasoning: optional generated-only explanation for why the taxonomy labels were attached

## Measurement Method

Before condition: Research Adversary review generated without taxonomy lookup fields.

After condition: Research Adversary review generated with taxonomy categories, confidence, related patterns, and taxonomy reasoning, while using the same input artifact and the same authority boundary.

Failure recall formula:

`matched_historical_failure_modes / known_relevant_historical_failure_modes`

Assumption recall formula:

`identified_material_assumptions / known_material_assumptions`

Constraint recall formula:

`identified_material_constraints / known_material_constraints`

## Before And After Targets

| Metric | Before Baseline | After Minimum Useful Result | Required Lift |
| --- | ---: | ---: | ---: |
| failure recall | baseline adversary score | baseline x 1.237 | +23.7% relative |
| assumption recall | baseline adversary score | baseline x 1.391 | +39.1% relative |
| constraint recall | baseline adversary score | baseline x 1.200 | +20.0% relative |

The pilot should be considered useful only if failure recall and assumption recall clear the requested improvement thresholds without reducing constraint recall or increasing reviewer burden enough to offset the gain.

## Expected Failure Categories Under Test

- REGIME_DEPENDENCY
- PROXY_DEPENDENCY
- WARNING_RECURRENCE
- CERTIFICATION_BLOCKER
- DATA_COVERAGE_GAP
- DUPLICATE_CLUSTER
- RECURRING_ASSUMPTION
- RECURRING_BLOCKER

## Evaluation Decision Rule

Advance taxonomy integration only if the after condition shows:

- failure recall improves by at least 23.7% relative to the before condition
- assumption recall improves by at least 39.1% relative to the before condition
- constraint recall does not regress materially
- generated taxonomy notes do not introduce forbidden authority language
- authority boundaries are byte-for-byte equivalent except for generated-only taxonomy explanation fields
- all outputs remain GENERATED_ONLY

## Current Pilot Finding

This change establishes the evaluation surface and tests. It does not claim production improvement yet. The next valid measurement requires scoring a fixed historical review set twice: once with taxonomy fields disabled and once with taxonomy fields enabled.

## Authority Verification

No live trading authority is added.
No broker execution authority is added.
No capital authority is added.
No automatic paper placement authority is added.
No replay, qualification, candidate, or governance authority is added.
No position sizing is added.
