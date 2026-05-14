# sleeve_edge_overlap_review.v1

`sleeve_edge_overlap_review.v1` is the deterministic Aegis Lite review artifact that evaluates all trade candidates across sleeves before the EOD manual trade report is finalized.

## Principle

Aegis Lite does not reject trades merely because multiple sleeves fired. The question is how many distinct edges fired.

Multiple sleeve candidates may all appear in the EOD report when they represent distinct edges. Duplicate, correlated, or concentrated candidates are surfaced through governance recommendations rather than hidden sleeve-count limits.

## Review Dimensions

The review classifies candidates by:

- Same edge family.
- Same directional exposure.
- Same risk factor.
- Same regime dependency.
- Same macro sensitivity.
- Same correlated symbol exposure.
- Same duplicate thesis.
- Same volatility or liquidity dependency.

## Candidate Fields

Each candidate-level note includes:

- `candidate_id`
- `sleeve_id`
- `symbol`
- `direction`
- `edge_family`
- `thesis_id`
- `shared_risk_tags`
- `correlated_symbols`
- `overlap_group_id`
- `duplicate_thesis_flag`
- `concentration_warning_flag`
- `governance_recommendation`

## Governance Recommendations

Allowed recommendations are:

- `approve`
- `approve_reduced_size`
- `prefer_best_candidate_in_group`
- `block_due_to_duplicate_edge`
- `block_due_to_concentration`
- `manual_review_required`

Missing stop, risk, sizing, entry reference, symbol, direction, or instrument type forces `manual_review_required`. Duplicate thesis groups are marked `prefer_best_candidate_in_group`. Concentrated same-symbol exposure is marked for reduced size or review.

## Artifact Location

The canonical path is:

`reports/sleeve_edge_overlap_review_v1/<DAY>/<RUN_ID>/sleeve_edge_overlap_review.v1.json`

The artifact is advisory/governance evidence for manual reporting. It does not submit orders or grant broker authority.
