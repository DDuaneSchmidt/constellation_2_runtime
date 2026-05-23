# Governance Proposal: ONE_PRIMARY_PER_REGIME_BUCKET_RANKED_SELECTION_V1

Status: PROPOSED_ONLY  
Runtime behavior changed: NO  
Selection behavior changed: NO  
Trading behavior changed: NO  
Broker/order/allocation authority changed: NO  
Generated: 2026-05-20

## Current Policy Authority

No standalone governance contract file was found that fully defines `ONE_PRIMARY_PER_REGIME_BUCKET` as an independent policy contract.

Current effective authority is embedded in:

- `ops/tools/run_portfolio_activation_gate_v1.py`
  - `_apply_one_primary_per_bucket(rows)`
  - emitted artifact policy: `policy.policy_id = portfolio_activation_gate_v1`
  - emitted artifact rule list includes `ONE_PRIMARY_PER_REGIME_BUCKET`
- `ops/aegis/portfolio_gate_candidate_report_v1.py`
  - describes the effective limit as one `ALLOW` per `regime_bucket`
  - reports `ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED` alternatives as watchlist-only
- `docs/candidate_centric_phase1_observability.md`
  - states candidate visibility is diagnostic only and does not alter arbitration, submit, authorization, kill-switch, or trading behavior

Current implementation summary:

1. `portfolio_activation_gate_v1` first computes per-candidate gate decisions.
2. Candidates initially marked `ALLOW` are grouped by `regime_bucket`.
3. If more than one candidate is allowed in the bucket, the implementation sorts by embedded sleeve priority.
4. The first row after that priority ordering remains `ALLOW`.
5. All remaining rows in that bucket are changed to `SUPPRESS` with reason code `ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED`.
6. Candidate score is not consumed by this suppression step.

## Current Diagnostic Finding

Source diagnostic artifact:

`/home/node/constellation_2_runtime/constellation_2/runtime/truth/reports/regime_bucket_candidate_ranking_v1/2026-05-20/regime_bucket_candidate_ranking.v1.json`

Observed state:

- selected_candidate_id: `c2_trend_eq_amt_2026-05-20_v1`
- selected_candidate_rank: `1`
- top_ranked_candidate_id: `c2_trend_eq_amt_2026-05-20_v1`
- selected was top-ranked: `YES`
- order_dependency_detected: `YES`
- suppressed candidates tied at comparable score `6.3333`

Interpretation:

The current AMT selection would not change under the proposed ranked-selection policy because AMT is already rank 1 in the diagnostic. However, the current mechanism is still order-dependent when candidates tie or when input order influences which same-priority candidate survives the bucket cap.

## Proposed Policy Option

Policy id:

`ONE_PRIMARY_PER_REGIME_BUCKET_RANKED_SELECTION_V1`

Objective:

For each `regime_bucket`, select the highest-ranked candidate from the full pre-suppression comparable candidate set before suppressing alternatives. Suppressed alternatives remain watchlist-only and must not be converted into trades by this policy.

This proposal does not grant execution authority. It only changes the candidate selected by the portfolio activation gate if adopted in a later governed implementation.

## Proposed Ranking Inputs

All candidates in the same `regime_bucket` that would otherwise be eligible before `ONE_PRIMARY_PER_REGIME_BUCKET` suppression must receive comparable pre-suppression ranking metrics.

Required inputs:

- comparable pre-suppression score
- confidence or signal-strength component
- liquidity metric
- market-data freshness status/session
- lifecycle eligibility
- concentration/overlap metrics
- deterministic tie-break fields
- source artifact references for all metrics

Input requirements:

- Missing required ranking inputs must fail closed for the ranked-selection policy.
- Missing optional ranking metrics must produce explicit diagnostics and deterministic fallback behavior.
- Stale or missing required data must not be treated as valid.
- No fabricated values are allowed.
- No raw candidates may become execution packages because of this policy.

## Proposed Deterministic Tie-Breakers

Tie-break order:

1. Higher comparable pre-suppression score.
2. Fresher validated market data.
3. Higher approved liquidity metric.
4. Lower concentration overlap.
5. Higher lifecycle eligibility quality, where fully eligible beats stale/partial lifecycle state.
6. Lower execution-readiness penalty.
7. Stable symbol ordering as final tie-breaker only.

Stable symbol ordering must be the last resort, not the primary selector.

## Expected Behavior

For each `regime_bucket`:

1. Build the pre-suppression candidate set.
2. Exclude candidates that are not lifecycle eligible or lack required evidence.
3. Score all remaining candidates on comparable ranking inputs.
4. Sort candidates by the deterministic tie-breaker chain.
5. Select the top-ranked candidate as the only `ALLOW` for that bucket.
6. Suppress the rest with reason code:
   `ONE_PRIMARY_PER_REGIME_BUCKET_RANKED_SELECTION_SUPPRESSED`
7. Preserve existing watchlist visibility for suppressed alternatives.
8. Emit ranking evidence showing:
   - selected candidate
   - selected rank
   - top ranked candidate
   - suppressed ranks/scores
   - source artifacts
   - tie-breakers applied

Expected current-day result for 2026-05-20:

- Current AMT selection would change: `NO`
- Reason: AMT is already top-ranked in `regime_bucket = TREND`.

## Expected Benefits

- Removes accidental dependence on input artifact order for candidate selection.
- Makes one-primary-per-bucket selection explainable with comparable metrics.
- Preserves the one-primary risk-control constraint.
- Improves auditability of suppressed alternatives.
- Makes future broad/dynamic universe runs safer because symbol ordering is no longer the primary selector.
- Gives UI and diagnostics clear evidence for why a candidate was selected over alternatives.

## Risks

- If liquidity/freshness inputs are unavailable or inconsistent, ranked selection may block more often until data contracts are complete.
- A newly top-ranked candidate could differ from the historically selected candidate, changing paper/ad-hoc behavior once implemented.
- Existing tests that assume sleeve-priority selection may need to be updated or split into legacy-vs-ranked policy tests.
- Comparable scoring must avoid circular dependency with post-gate portfolio scoring.
- Tie-breaker implementation must be deterministic across machines and Python versions.
- The proposal could be misread as execution authorization; implementation must preserve submit-boundary authority.

## Required Tests Before Activation

Old-vs-new behavior tests:

1. Legacy policy fixture where two candidates share a bucket and lower-scored first row is selected by input order.
   - Old behavior: first/priority row remains `ALLOW`.
   - New behavior: higher-ranked candidate remains `ALLOW`.

2. Same-score fixture with different freshness.
   - New behavior selects fresher validated market data.

3. Same-score and same-freshness fixture with different liquidity.
   - New behavior selects higher liquidity.

4. Same-score/freshness/liquidity fixture with different overlap.
   - New behavior selects lower concentration overlap.

5. Full tie fixture.
   - New behavior uses stable symbol ordering only as final tie-breaker.

6. Lifecycle-ineligible candidate fixture.
   - New behavior does not select lifecycle-ineligible candidate even with higher score.

7. Missing ranking inputs fixture.
   - New behavior fails closed or emits explicit blocker, not silent fallback.

8. Current AMT regression fixture.
   - New behavior keeps `c2_trend_eq_amt_2026-05-20_v1` selected.

9. Watchlist projection fixture.
   - Suppressed candidates include pre-suppression score, bucket rank, and suppression reason.

10. Safety fixture.
   - No broker connection, no order, no allocation, no paper submit, no execution package, no submit-boundary weakening.

## Implementation Constraints If Approved Later

- Add explicit policy switch or versioned policy id before changing runtime behavior.
- Keep `ONE_PRIMARY_PER_REGIME_BUCKET` legacy behavior available for replay/backtest comparison.
- Do not change `submit_boundary_paper_v4.py`.
- Do not alter paper submit eligibility.
- Do not create trades automatically.
- Do not mutate sleeves.
- Do not broaden broker or live-trading authority.
- Emit a migration report comparing legacy selected candidate vs ranked selected candidate for every affected bucket.

## Recommendation

Approve implementation planning for `ONE_PRIMARY_PER_REGIME_BUCKET_RANKED_SELECTION_V1`, but require a separate activation packet before runtime selection behavior changes.

The ranked-selection policy is directionally preferable because it preserves the risk-control intent while replacing input-order survival with auditable candidate quality metrics. Activation should wait until liquidity, freshness, lifecycle, and tie-breaker evidence are fully covered by tests.
