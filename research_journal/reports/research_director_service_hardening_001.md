# Research Director Service Hardening 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: evaluation-only hardening review of Research Director. No authority, no workflow changes, no production integration, no replay changes, no qualification changes, no candidate changes, no governance changes, no memory writes, no paper-placement authority, no trading recommendation, no capital authority, no broker execution, and no position sizing.

## Question

What evidence is still required before Research Director could advance beyond `EVALUATION_SERVICE`?

Short answer: Research Director should not advance beyond `EVALUATION_SERVICE` yet.

It has enough evidence to run as a shadow/evaluation service because it correctly ranked the first-order and later exposed bottleneck sequence. It does not yet have enough prospective, multi-cycle, false-positive, missed-bottleneck, and uncertainty-calibration evidence to influence live workflow order or autonomous research execution.

## Inputs Reviewed

- `research_journal/reports/research_director_prediction_accuracy_001.md`
- `research_journal/reports/research_director_evaluation_001.md`
- `research_journal/reports/research_director_bottleneck_selection_test_001.md`
- `research_journal/reports/research_director_retrospective_001.md`
- `research_journal/reports/research_debt_trend_001.md`
- `research_journal/reports/meta_research_feasibility_001.md`
- `research_journal/reports/replay_yield_discrepancy_001.md`
- `research_journal/reports/atlas_research_debt_inventory_001.md`

## Current Service Level

Recommended current state: `EVALUATION_SERVICE`.

Allowed:

- Generate offline rankings.
- Explain priority, uncertainty, and research-debt order.
- Compare predictions with realized bottlenecks.
- Produce shadow recommendations for human review.
- Measure false prioritizations, missed bottlenecks, and uncertainty calibration.

Not allowed:

- Change workflow order.
- Select autonomous work.
- Modify backlog state.
- Change candidate, replay, qualification, governance, memory, or paper-forward state.
- Override human prioritization.
- Promote or demote candidates.
- Recommend trades, capital action, broker execution, or position sizing.

## Prediction Accuracy Review

Prior measured result: `PASS_WITH_LIMITATIONS`.

| Dimension | Evidence | Judgment |
| --- | --- | --- |
| Broad bottleneck detection | `4/4` broad bottlenecks identified in prediction accuracy review. | Strong enough for shadow evaluation. |
| Precise prediction accuracy | `2.5/4`, or `62.5%`, precise prediction accuracy. | Not strong enough for workflow authority. |
| First bottleneck selection | Data Coverage ranked #1 before less direct work; confirmed by 6.67% coverage and 8/8 insufficient-data state. | Strong. |
| Later bottleneck sequencing | Replay Attrition and Vocabulary Mismatch were ranked correctly after coverage repair exposed them. | Useful, but partially retrospective. |
| Research hours saved | Estimated `19-40` hours saved. | Promising but still estimate-based. |
| Counterfactual delay | Estimated `3-6` research/validation cycles avoided. | Useful, but not prospectively validated. |

Assessment: Research Director is accurate enough to continue as an evaluator. It is not yet accurate enough to control sequencing because its strongest result is first-order bottleneck selection, while second- and third-order diagnoses required root-cause evidence produced later.

## False Prioritizations

Observed false-prioritization risk is moderate.

| Candidate False Prioritization | Evidence | Risk | Hardening Need |
| --- | --- | --- | --- |
| Failure Taxonomy over Data Coverage | Failure Taxonomy ranked close to Data Coverage in earlier bottleneck tests, but does not directly unblock missing data. | Medium | Require hard-blocker override: data/replay blockers outrank descriptive layers when validation cannot run. |
| Research Adversary over direct validation repair | Research Adversary is high value, but generated-only critique cannot add data, replay samples, or vocabulary compatibility. | Medium | Require evidence of current review burden or failure-recall gain before ranking adversary above direct blockers. |
| Search Space Mapping during blocked validation | Search-space work has long-term value but would add candidate surface while evidence production is blocked. | Low-medium | Penalize new generation while unvalidated/proxy/replay debt is high. |
| Research Economics as action substitute | Economics supports prioritization but does not itself repair the bottleneck. | Low-medium | Separate diagnostic work from bottleneck-removal work. |
| Global replay health over candidate replay collapse | Global replay yield appeared healthy while candidate-specific evidence remained weak. | Medium-high | Force candidate-level evidence checks before declaring replay-related debt reduced. |

False prioritization conclusion: no catastrophic false prioritization was observed in the reviewed sequence, but the service is vulnerable to attractive analysis layers outranking hard evidence-production blockers unless hard-blocker precedence rules are explicit.

## Missed Bottlenecks

Observed missed-bottleneck risk is still material.

| Bottleneck | Status | Evidence | Hardening Need |
| --- | --- | --- | --- |
| Replay Attrition | Partially missed early. | Broadly predicted as replay/backtest weakness, but exact trigger/regime attrition was diagnosed only later. | Require prospective attrition-ledger prediction before root-cause report exists. |
| Regime Vocabulary Mismatch | Partially missed early. | Exact `CHOP` versus daily proxy vocabulary mismatch was not predeclared. | Require vocabulary compatibility checks in the uncertainty model. |
| Proxy Dependence Plateau | Known but not cleared. | Research debt trend shows proxy dependence remains `600`. | Research Director must flag unchanged high-mass debt after local wins. |
| Qualification Gaps | Known but not actively decomposed by Research Director. | `490/600` candidates below final threshold; `372/600` replay gaps remain. | Require qualification blocker attribution ranking. |
| Unresolved Findings | Known but not resolved. | `150` unresolved adversary findings remain. | Require review-burden and false-positive measurement. |
| Closed Outcome Scarcity | Known feasibility limit. | Meta-research feasibility found only `2` paper-forward outcomes. | Prevent outcome-success claims until closed outcome volume improves. |

Missed-bottleneck conclusion: Research Director needs a richer prospective diagnostic vocabulary before advancing. It can identify broad areas, but exact second-order blockers still depend heavily on human/root-cause reports.

## Uncertainty Ranking Quality

Current uncertainty ranking from `research_director_evaluation_001.md`:

1. Replay Attrition: `0.892`
2. Vocabulary Mismatch: `0.861`
3. Data Coverage: `0.840`
4. Failure Taxonomy: `0.579`
5. Research Adversary: `0.538`
6. Research Economics: `0.483`
7. Search Space Mapping: `0.472`
8. More Hypothesis Generation: `0.299`

Assessment: `PASS_WITH_LIMITATIONS`.

Strengths:

- Correctly places Replay Attrition above Data Coverage once daily coverage is substantially repaired.
- Correctly treats Vocabulary Mismatch as high uncertainty because exact label compatibility changes interpretation of zero-sample candidates.
- Correctly deprioritizes more hypothesis generation while validation is blocked.

Limitations:

- The uncertainty ranking is retrospective and source-aware; it has not been scored before the root-cause reports exist.
- It does not yet show calibration: high uncertainty should predict where the next root-cause report will change conclusions.
- It does not separate uncertainty from actionability strongly enough. A high-uncertainty issue can still be lower action priority if the remediation path is unclear or expensive.
- It does not yet include confidence intervals or evidence freshness penalties.
- It has not been tested against false uncertainty alarms, where Research Director sends reviewers toward issues that do not change conclusions.

Required hardening: uncertainty rankings must be measured prospectively against realized conclusion changes, not only against narrative plausibility.

## Evidence Required Before Advancing Beyond EVALUATION_SERVICE

Research Director should remain `EVALUATION_SERVICE` until all required evidence below exists.

| Required Evidence | Minimum Bar | Why It Matters |
| --- | --- | --- |
| Prospective shadow cycles | At least `5` independent research/validation cycles scored before root-cause outcomes are known. | Current evidence is mostly same-day retrospective. |
| Precise bottleneck prediction | At least `75%` precise prediction accuracy, not just broad category detection. | Current precise accuracy is `62.5%`. |
| False prioritization rate | Measured false-priority rate below `20%` for top-3 recommendations. | Need proof the service does not route attention to attractive but non-blocking work. |
| Missed bottleneck rate | No missed critical bottleneck in `5` consecutive shadow cycles. | A missed hard blocker can waste validation cycles. |
| Uncertainty calibration | Top-3 uncertainty items predict at least `60%` of subsequent conclusion-changing root-cause discoveries. | Uncertainty ranking must be predictive, not descriptive. |
| Debt movement attribution | Recommendations must be linked to measurable debt movement, not just completed reports. | Research debt trend shows only data/validation debt moved; proxy/replay/qualification debt did not. |
| Candidate-level evidence guard | Global metrics must be reconciled with candidate-level rows before ranking replay health. | Replay yield discrepancy shows global health can mask candidate collapse. |
| Taxonomy precedence rules | Ambiguous and multi-category failures need precedence rules and human scoring. | Meta-research feasibility shows taxonomy coverage is broad but ambiguity remains. |
| Review burden measurement | Human time saved must be measured, not estimated, for at least one cycle. | Current `19-40` hours saved is plausible but estimate-based. |
| Outcome boundary guard | Service must explicitly block outcome-success claims until closed outcomes are sufficient. | Current paper-forward outcomes count is only `2`. |

## Hardening Rules

Before any future service tier, Research Director needs these evaluation-only rules:

1. Hard-blocker precedence: if validation cannot run or evidence cannot be interpreted, direct evidence-production blockers outrank analysis layers.
2. Post-win plateau detection: after a debt class improves, Research Director must identify which high-mass debts did not move.
3. Candidate-level reconciliation: global replay or aggregate health cannot be used without candidate-level and qualification-level checks.
4. Broad-vs-precise scoring: each prediction must be scored separately for broad category detection and precise causal diagnosis.
5. Retrospective penalty: rankings generated after a root-cause report must be labeled as source-aware retrospective rankings.
6. Uncertainty-action split: high uncertainty does not automatically mean highest priority; actionability and blocker-removal must be scored separately.
7. False-priority ledger: every top-3 recommendation must later be marked true positive, false positive, partial, or unresolved.
8. Miss ledger: every realized bottleneck must be checked against prior rankings to identify missed or late detections.

## Advancement Decision

Current decision: `DO_NOT_ADVANCE_BEYOND_EVALUATION_SERVICE`.

Reason:

Research Director is useful as a measured shadow service, but it has not yet demonstrated enough prospective precision, false-priority control, missed-bottleneck control, or uncertainty calibration. Its current value is evaluation, explanation, and retrospective measurement, not workflow control.

Permitted next step:

- Continue as `EVALUATION_SERVICE`.
- Run prospective shadow scoring over future cycles.
- Produce prediction ledgers and calibration reports.
- Keep every output non-authoritative and generated-only.

Rejected next step:

- Do not advance to authoritative routing, autonomous work selection, workflow ordering, candidate gating, replay changes, qualification influence, governance influence, or memory-writing service.

## Conclusion

Research Director has passed the first hardening gate: it can identify and explain high-value bottlenecks in an evaluation-only setting. It has not passed the service-advancement gate. The missing evidence is prospective calibration: top recommendations must predict realized bottlenecks, uncertainty rankings must predict conclusion-changing discoveries, and false prioritizations/misses must stay below explicit thresholds across multiple cycles.

Until that evidence exists, Research Director should remain `EVALUATION_SERVICE` with no authority and no workflow changes.

## Authority Boundary

This report is evaluation-only. It does not modify production, workflow order, backlog state, candidate state, replay state, qualification state, governance state, paper-forward state, memory state, trading state, broker execution, capital allocation, or position sizing.
