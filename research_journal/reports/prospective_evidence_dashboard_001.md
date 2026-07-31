# Prospective Evidence Dashboard 001

Date: 2026-06-06

Status: `MEASUREMENT_DASHBOARD_ONLY`

## Purpose

Combine prospective evidence measurements into one report-only dashboard for Research Adversary, Research Director, candidate validation, and paper-forward outcome tracking.

This dashboard does not implement software, change workflows, change prioritization, modify candidates, modify replay, modify validation, modify qualification, change governance, write memory automatically, authorize paper-forward actions, recommend trades, allocate capital, authorize broker execution, construct portfolios, or size positions.

## Runtime Truth Context

Latest verified runtime graph reviewed:

- `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-06/verified_runtime_graph.v1.json`
- active mode: `HUMAN_REVIEWED_PAPER_MODE`
- active mode readiness status: `BLOCKED`
- runtime truth classification linked in graph: `PARTIAL_CONTEXT`
- research journal validation capability: `ALLOWED` for `READ_ONLY` and `AUDIT`

Interpretation: this dashboard is a journal measurement surface only. It does not assert runtime readiness or operational authority.

## Status Legend

| Status | Meaning |
| --- | --- |
| `GREEN` | Metric has enough evidence to meet the success threshold or is on track with a valid denominator. |
| `YELLOW` | Metric is measurable but incomplete, underpowered, pending future scoring, or not yet at threshold. |
| `RED` | Metric is unmeasured, blocked, lacks required human or future evidence, or is below a hold threshold. |

## Executive Dashboard

| Lane | Current status | Primary reason |
| --- | --- | --- |
| Research Adversary | `RED` | Human review rows exist, but completed human scores are `0`; averages must remain `N/A`. |
| Research Director | `YELLOW` | One prospective shadow cycle exists, but Cycle 002 must wait for later evidence before scoring. |
| Candidates | `YELLOW` | Focused direct validation has early positive signal, but 6 of 8 remain insufficient-data and 0 are weakened. |
| Paper-forward | `YELLOW` | Outcomes exist, but denominator is only 2 and is below the 10 linked-outcome threshold. |

## Research Adversary

Source:

- `research_journal/reports/research_adversary_human_review_scores_001.csv`
- `research_journal/reports/prospective_evidence_operating_plan_001.md`

| Metric | Current value | Denominator | Status | Notes |
| --- | ---: | ---: | --- | --- |
| Human reviews completed | 0 | 20 pending rows | `RED` | Rows are `PENDING_HUMAN_REVIEW` / `UNSCORED_HUMAN_REQUIRED`. |
| Average usefulness | `N/A` | 0 scored rows | `RED` | Do not treat blank score fields as zero. |
| Average correctness | `N/A` | 0 scored rows | `RED` | Requires human-filled score rows. |
| Average novelty | `N/A` | 0 scored rows | `RED` | Requires human-filled score rows. |
| Average time saved | `N/A` | 0 scored rows | `RED` | Requires human-filled score rows. |

Measurement rule: compute averages only from completed human-scored rows with reviewer, reviewed timestamp, required score fields, and valid score source. Pending rows remain pending evidence, not zero scores.

## Research Director

Sources:

- `research_journal/reports/research_director_shadow_cycle_002.md`
- `research_journal/reports/research_director_prediction_accuracy_001.md`
- `research_journal/reports/prospective_evidence_operating_plan_001.md`

| Metric | Current value | Denominator | Status | Notes |
| --- | ---: | ---: | --- | --- |
| Shadow predictions | 5 bottlenecks, 5 experiments, 8 debt items | 1 prospective cycle | `YELLOW` | Cycle 002 is recorded as `PROSPECTIVE_TRIAL`. |
| Prediction accuracy | `PENDING` | 1 prospective cycle | `YELLOW` | Cycle 002 must be scored only against future evidence created after the report. |
| Bottleneck accuracy | `PENDING` | 1 prospective cycle | `YELLOW` | Current retrospective baseline was `100%` broad detection and `50%` precise detection, but it is not a prospective score. |

Prospective scoring labels: `EXACT`, `PARTIAL`, `MISS`, `FALSE`, `PENDING`.

Accuracy rule: report `EXACT` separately, and compute useful alignment as `EXACT + PARTIAL` only after later evidence exists.

## Candidates

Sources:

- `research_journal/reports/candidate_quality_scoreboard_002.md`
- `research_journal/reports/candidate_survival_scorecard_001.md`
- `research_journal/reports/prospective_evidence_operating_plan_001.md`

| Metric | Current value | Denominator | Status | Notes |
| --- | ---: | ---: | --- | --- |
| Confirmed | 2 | 8 focused direct-validation candidates | `YELLOW` | Positive signal, but small sample. |
| Weakened | 0 | 8 focused direct-validation candidates | `YELLOW` | No evidence-complete direct weakened cases yet. |
| Blocked / insufficient data | 6 | 8 focused direct-validation candidates | `RED` | Main focused validation blocker remains `INSUFFICIENT_DATA`. |

Candidate interpretation:

- confirmation rate: `25.0%`
- weakened rate: `0.0%`
- insufficient-data rate: `75.0%`

Measurement rule: preserve direct, proxy, bridge, intraday, event, and insufficient-data labels. Bridge-only or proxy-only evidence must not be counted as direct confirmation.

## Paper-Forward

Sources:

- `research_journal/reports/candidate_survival_scorecard_001.md`
- `research_journal/reports/candidate_quality_scoreboard_002.md`
- `research_journal/reports/prospective_evidence_operating_plan_001.md`

| Metric | Current value | Denominator | Status | Notes |
| --- | ---: | ---: | --- | --- |
| Outcomes | 2 | 10 minimum linked outcomes for survival-quality readout | `YELLOW` | Current outcome evidence is underpowered. |
| Survival rate | 50.0% | 2 recorded outcomes | `YELLOW` | 1 survived and 1 weakened; do not generalize to 58 paper-forward-ready candidates. |

Paper-forward interpretation:

- paper-forward-ready candidates: 58
- paper-forward observation plans: 12
- recorded outcomes: 2
- survived: 1
- weakened: 1
- falsified: 0
- needs more data: 0

Measurement rule: survival-quality claims require at least 10 linked paper-forward outcomes with explicit current candidate-id linkage.

## Next Dashboard Update

Update this dashboard when any of the following prospective evidence changes:

- a human reviewer completes Research Adversary scores;
- a later research cycle provides evidence to score Research Director Cycle 002;
- focused direct validation changes confirmed, weakened, or insufficient-data counts;
- paper-forward outcomes increase, especially linked outcomes tied to current candidate ids.

## Authority Boundary

This dashboard is measurement only. It is not an authority expansion, runtime readiness claim, trade recommendation, candidate promotion or rejection, paper placement instruction, capital allocation instruction, broker instruction, or workflow-control surface.
