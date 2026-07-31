# Research Adversary Candidate Retrospective 001

Objective: determine whether Research Adversary would have exposed risks earlier for existing candidates and paper-forward observation items.

Scope: retrospective review only. This report does not modify production systems, candidate state, paper-forward state, qualification status, replay status, ranking, promotion, demotion, paper placement, broker execution, live trading, position sizing, or capital allocation.

Inputs reviewed:
- `reports/atlas_v2_research_os/paper_forward_observation/latest.json`
- `reports/atlas_v2_research_os/paper_forward_campaign/latest.json`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/candidate_review/latest.json`
- `reports/atlas_v2_research_os/paper_trade_candidates/latest.json`

Authority boundary:
- This is a human-readable research retrospective.
- It is not a trading recommendation.
- It does not approve, reject, promote, demote, qualify, disqualify, or paper-place any candidate.
- It does not change replay, qualification, or paper-forward artifacts.
- All usefulness estimates are retrospective process judgments only.

## Executive Finding

Research Adversary would likely have helped most on candidate families where replay support looked acceptable but the supporting evidence was proxy-dependent, regime-weak, or under-specified. It would not have replaced replay, ranking, or paper-forward review. Its value would have been earlier surfacing of assumptions, missing constraints, and falsification tests before paper-forward queue capacity was consumed.

Estimated overall usefulness: HIGH for process discipline, MEDIUM for direct paper-forward survival improvement.

Why:
- Final ranking reported 600 evaluated candidates, with 58 ready for paper-forward observation, 490 rejected for now, 44 too fragile, 6 needing data improvement, and 2 too proxy-dependent.
- The biggest remaining risk in final ranking was that selected candidates still depended on local SPY daily proxy evidence until candidate-specific data is supplied.
- Candidate review listed common risks: historical replay is not paper-forward evidence, mechanism clusters can duplicate the same structural idea, weak or unknown regime labels reduce interpretability, paper-forward observation may fail to reproduce replay metrics, and 88 candidates were below the edge threshold.
- Paper-forward observation plans exist for 12 candidates, but no realized paper-forward outcome was available in the reviewed plan artifact.

## Reviewed Candidates

### 1. `ptc_f492d1bebb6bd47f`

- Original hypothesis: MEAN_REVERSION in TRENDING regime can produce repeatable paper-forward observation evidence after replay support.
- Known risks: replay evidence is not paper-forward evidence; selected candidate remains exposed to proxy evidence limitations; mean reversion can fail in persistent directional moves.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: the TRENDING label is stable enough to separate this candidate from generic mean reversion; replay recurrence conditions can be reconstructed in forward observation; proxy evidence is representative of candidate-specific behavior.
- Constraints it should have flagged: no trade placement authority; no capital or sizing authority; minimum 30 forward samples; candidate-specific data should replace proxy evidence before confidence increases.
- Falsification tests it should have proposed: compare forward samples against replay recurrence rules; reject if mean reversion only works after ex post trend labeling; track failure rate during strong continuation days; require candidate-specific data reconstruction for every sample.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have forced the team to separate true mean reversion edge from proxy-supported trend-regime labeling earlier.
- Would this have improved paper-forward survival? Probably. It would have narrowed valid observation windows and reduced false positives.

### 2. `ptc_3d46c2d50e5fe58b`

- Original hypothesis: BREAKOUT in TRENDING regime can survive paper-forward observation after replay support.
- Known risks: breakout candidates are vulnerable to failed follow-through, false breaks, and daily/intraday mismatch; replay support may not imply forward survival.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: breakout confirmation is defined before observation; forward data can identify the same trigger without hindsight; trend context is not just a post-breakout label.
- Constraints it should have flagged: proxy dependency; no automatic paper placement; no candidate promotion; observation-only evidence until human review.
- Falsification tests it should have proposed: predeclare breakout trigger and confirmation window; measure false-break rate; test whether outcomes degrade when entry is delayed to observable confirmation; compare trend and non-trend contexts.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have made failed-break accounting explicit before queue admission.
- Would this have improved paper-forward survival? Probably. It would have reduced ambiguous breakout observations.

### 3. `ptc_7f054744c90194c8`

- Original hypothesis: VWAP_OR_AVERAGE_RECLAIM can produce durable forward evidence, despite UNKNOWN regime labeling.
- Known risks: unknown regime reduces interpretability; average reclaim signals can duplicate mean reversion, liquidity sweep, or opening range behavior; replay may overfit common intraday structure.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: VWAP or average reclaim is the causal mechanism, not a proxy for broader risk-on behavior; UNKNOWN regime is acceptable for observation; trigger can be observed without subjective chart reading.
- Constraints it should have flagged: regime label weakness; mechanism overlap; daily proxy evidence cannot validate intraday reclaim behavior by itself.
- Falsification tests it should have proposed: require a precise reclaim definition; compare against mean reversion and opening range baselines; mark invalid if regime remains UNKNOWN after observation; test whether reclaim edge disappears after excluding broad index trend days.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have highlighted mechanism duplication and regime ambiguity before forward observation.
- Would this have improved paper-forward survival? Possibly. Survival improves only if the reclaim definition becomes stricter.

### 4. `ptc_fc801836c012a96d`

- Original hypothesis: EVENT_REACTION behavior can produce repeatable paper-forward observations, despite UNKNOWN regime labeling.
- Known risks: event classification may be incomplete; macro or catalyst context may be missing; unknown regime makes replay interpretation weak.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: event type, timing, and market expectation are available before observation; observed move is event reaction rather than normal volatility; event data source is current and reconstructable.
- Constraints it should have flagged: event source lineage; macro calendar or catalyst data dependency; no authority expansion if event evidence is missing.
- Falsification tests it should have proposed: require pre-event timestamped catalyst metadata; compare event windows with matched non-event windows; invalidate samples where event surprise cannot be reconstructed; separate scheduled and unscheduled events.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have exposed data-source dependency before paper-forward queue use.
- Would this have improved paper-forward survival? Probably, because event candidates fail quickly when event lineage is weak.

### 5. `ptc_b57c4cfe8ed94fff`

- Original hypothesis: SESSION_TIMING behavior can produce repeatable observation evidence in UNKNOWN regime.
- Known risks: session effects can be calendar artifacts; replay may capture broad liquidity cycles rather than candidate-specific edge; UNKNOWN regime weakens diagnosis.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: session window is predeclared; effect survives day-of-week, volatility, and market open/close controls; forward samples are not duplicated by opening range or breakout candidates.
- Constraints it should have flagged: time-zone/session boundary consistency; holiday and half-day handling; mechanism duplication with opening range.
- Falsification tests it should have proposed: stratify by day type; compare against randomized same-day windows; invalidate samples with ambiguous session boundaries; test post-cost and delayed-observation robustness in research-only form.
- Estimated usefulness: MEDIUM.
- Would this have saved research time? Yes, but mostly by requiring controls rather than rejecting the item.
- Would this have improved paper-forward survival? Possibly. It would reduce calendar-artifact samples.

### 6. `ptc_ffad13e2dbe09619`

- Original hypothesis: REVERSAL behavior can survive paper-forward observation in UNKNOWN regime.
- Known risks: reversal and mean reversion overlap; reversals can be regime-sensitive and fail during trend continuation; unknown regime weakens interpretation.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: reversal trigger is distinct from mean reversion; invalidation is known before observation; regime misclassification will not dominate outcomes.
- Constraints it should have flagged: need for predeclared reversal level; need for separate trend/chop analysis; no candidate promotion from replay alone.
- Falsification tests it should have proposed: track reversal failure during high momentum; compare against mean reversion baseline; require predeclared exhaustion evidence; invalidate if success depends on after-the-fact swing-point selection.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have attacked overlap and hindsight risk early.
- Would this have improved paper-forward survival? Probably, by excluding trend-continuation traps.

### 7. `ptc_9112fdddb80509f1`

- Original hypothesis: TREND_CONTINUATION behavior can produce repeatable forward evidence in UNKNOWN regime.
- Known risks: continuation can be a generic market beta effect; unknown regime makes persistence assumptions weak; proxy evidence may inflate support.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: trend continuation is distinct from breakout; continuation trigger is observable before outcome; signal is not just SPY proxy direction.
- Constraints it should have flagged: candidate-specific data requirement; separate breakout and continuation samples; no trade recommendation from observation readiness.
- Falsification tests it should have proposed: test continuation after excluding breakout initiation bars; require candidate-specific trend measure; compare against passive trend benchmark; invalidate if only index proxy explains returns.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have forced benchmark comparison earlier.
- Would this have improved paper-forward survival? Possibly. It would prevent generic trend beta from masquerading as candidate edge.

### 8. `ptc_b97beffb99f141ae`

- Original hypothesis: OPENING_RANGE behavior can survive paper-forward observation in UNKNOWN regime.
- Known risks: overlap with breakout and session timing; opening range definitions can be parameter-sensitive; daily proxy evidence is weak for intraday opening structure.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: opening range duration and trigger are fixed; the effect survives alternate range lengths; liquidity conditions are comparable across samples.
- Constraints it should have flagged: intraday data dependency; parameter sensitivity; holiday and event-day handling.
- Falsification tests it should have proposed: test 5, 15, and 30 minute range sensitivity; compare against session timing and breakout baselines; invalidate if only one parameterization works; track failed range breaks separately.
- Estimated usefulness: MEDIUM.
- Would this have saved research time? Yes, by making parameter sensitivity explicit.
- Would this have improved paper-forward survival? Possibly, if brittle range definitions were excluded.

### 9. `ptc_695a47fe74f8e36d`

- Original hypothesis: MEAN_REVERSION behavior can survive forward observation in UNKNOWN regime.
- Known risks: same mechanism as the top mean-reversion candidate but weaker regime definition; proxy dependency; potential duplication.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: this is not a duplicate of `ptc_f492d1bebb6bd47f`; unknown regime is acceptable; reversal horizon is predeclared.
- Constraints it should have flagged: duplicate mechanism concentration; unknown regime; need for sample independence.
- Falsification tests it should have proposed: compare against the TRENDING mean-reversion candidate; require non-overlapping sample attribution; invalidate if outcomes cluster on the same market days; test separate chop and trend subsets.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have identified duplication risk early.
- Would this have improved paper-forward survival? Possibly. It would reduce redundant samples rather than directly improving hit rate.

### 10. `ptc_78687792cbb592e8`

- Original hypothesis: LIQUIDITY_SWEEP behavior can produce repeatable paper-forward evidence in UNKNOWN regime.
- Known risks: liquidity sweeps can be subjective; event labeling may be intraday-data dependent; proxy evidence is not enough to validate microstructure behavior.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: sweep definition is measurable; liquidity reference level is known before observation; observed reversal or continuation is not just volatility expansion.
- Constraints it should have flagged: intraday data and level-definition dependency; high risk of subjective labeling; need for audit trail per sample.
- Falsification tests it should have proposed: predeclare sweep threshold and lookback; compare against volatility expansion samples; invalidate if reference liquidity level is selected after the move; require screenshot-free, data-reconstructable evidence.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have pushed this candidate toward stricter evidence capture before observation.
- Would this have improved paper-forward survival? Probably. Poorly defined sweep samples would be filtered out.

### 11. `ptc_0614c2bf7a7c39b0`

- Original hypothesis: VOLATILITY_EXPANSION behavior can survive paper-forward observation in UNKNOWN regime.
- Known risks: volatility expansion is often a condition rather than an edge; may duplicate breakout, event reaction, or liquidity sweep; outcome direction may be under-specified.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: expansion has a directional or outcome rule, not just increased variance; volatility threshold is fixed; expansion regime does not merely identify noisy periods.
- Constraints it should have flagged: directionality ambiguity; overlap with other mechanisms; need for volatility baseline and sample stratification.
- Falsification tests it should have proposed: compare directional and non-directional outcomes; invalidate if expansion increases variance without expectancy; test against breakout and event-reaction subsets; require predeclared volatility threshold.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have challenged whether the candidate is an edge or only a filter.
- Would this have improved paper-forward survival? Probably, if non-directional expansion samples were excluded.

### 12. `ptc_2c34d7d9d627b7e7`

- Original hypothesis: BREAKOUT behavior can survive paper-forward observation in UNKNOWN regime.
- Known risks: weaker than the TRENDING breakout candidate because regime is UNKNOWN; false-break risk; duplicate exposure to breakout cluster.
- Paper-forward outcome if available: no realized outcome available; observation plan exists with minimum sample size 30 and human review required.
- Assumptions Research Adversary should have identified: this is not redundant with `ptc_3d46c2d50e5fe58b`; breakout works outside known trend context; forward trigger is reconstructable.
- Constraints it should have flagged: regime ambiguity; duplicate mechanism concentration; proxy evidence limitation.
- Falsification tests it should have proposed: compare with TRENDING breakout samples; invalidate if success appears only after trend relabeling; track false-break rate; require non-overlapping sample days with other breakout candidates.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have identified duplicate and regime risks.
- Would this have improved paper-forward survival? Possibly. It would tighten observation eligibility.

### 13. `ptc_6fe5fd4fe1dba60c`

- Original hypothesis: MEAN_REVERSION candidate from hypothesis `rhy-ff4819dea493ef54` had positive historical replay and might warrant paper testing consideration.
- Known risks: candidate was disqualified because edge score was below 0.7; replay sample size was 10; regime context was UNKNOWN; paper trade eligible was false.
- Paper-forward outcome if available: not paper-forward eligible in reviewed artifact; recommendation was to continue research and not paper test yet.
- Assumptions Research Adversary should have identified: positive replay with 10 samples is not enough; high profit factor may be unstable with small samples; UNKNOWN regime weakens interpretability.
- Constraints it should have flagged: edge score threshold; human review required; no paper placement authorization; no qualification override.
- Falsification tests it should have proposed: require at least 30 independent samples; test sensitivity to one or two losing samples; rerun with candidate-specific data; demand regime assignment before further review.
- Estimated usefulness: HIGH.
- Would this have saved research time? Yes. It would have redirected effort away from positive-replay narrative toward threshold and sample-size limits.
- Would this have improved paper-forward survival? Yes, indirectly, by keeping below-threshold candidates out of the paper-forward queue.

## Cross-Candidate Risks Research Adversary Should Have Surfaced

- Proxy dependency: final ranking stated all selected candidates still depended on SPY daily proxy evidence until candidate-specific data is supplied.
- Regime weakness: many paper-forward observation candidates carry UNKNOWN regime labels, limiting interpretability.
- Mechanism duplication: mean reversion, reversal, VWAP or average reclaim, opening range, breakout, trend continuation, liquidity sweep, and volatility expansion can overlap on the same market days.
- Intraday versus daily mismatch: several mechanisms are intraday by nature but the reviewed ranking notes proxy limitations.
- Sample insufficiency: paper-forward plans require 30 samples, while at least one disqualified candidate had only 10 replay samples.
- Replay-to-forward gap: historical replay support does not prove paper-forward survival.
- Authority boundary risk: none of the reviewed artifacts authorize trade recommendations, live trading, broker execution, capital allocation, automatic paper placement, or production promotion.

## Retrospective Answer

Would Research Adversary have exposed risks earlier? Yes.

Most likely early warnings:
- "This candidate is replay-supported but still proxy-dependent."
- "This candidate has an UNKNOWN or weak regime label."
- "This mechanism overlaps with another candidate and may double-count the same phenomenon."
- "The observation rule is not precise enough to falsify."
- "The minimum sample size is not yet available."
- "The candidate is not eligible for paper-forward action even if the narrative sounds strong."

Would it have saved research time? Yes, mainly by reducing time spent interpreting replay-positive candidates whose main weakness was already visible from assumptions and constraints.

Would it have improved paper-forward survival? Likely in a narrow sense. It would not make weak candidates stronger, but it would make paper-forward admission stricter, reduce ambiguous samples, and force better falsification design before observation work begins.

## Recommended Use In Future Reviews

Research Adversary should run before any candidate enters a human paper-forward review queue. It should produce only advisory, generated-only critique until a human reviewer accepts or rejects its claims.

Required pre-queue adversary checks:
- source lineage and proxy dependency
- regime specificity
- mechanism duplication
- predeclared observation trigger
- predeclared invalidation rule
- minimum sample size and independence
- data availability and reconstructability
- explicit authority boundary

This recommendation is process-only. It does not change any existing candidate, paper-forward item, replay result, qualification status, or governance artifact.
