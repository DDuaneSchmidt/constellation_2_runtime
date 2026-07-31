# Research Adversary Prospective Pilot 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Recommendation: `CONTINUE`

## Purpose

Run Research Adversary prospectively on all new Atlas observations, claims, and hypotheses during a fixed pilot window, while preserving generated-only status and preventing any authority expansion.

This report defines the pilot window, measurement rules, current metrics, decision thresholds, and authority boundaries. It does not implement automation, change workflow routing, modify candidates, influence replay, influence qualification, change governance, write memory automatically, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.

## Pilot Window

Start date: 2026-06-05

End condition: whichever comes first:

- next 50 new observations reviewed; or
- 2026-07-05, 30 calendar days after pilot start.

Scope:

- all new observations created during the pilot window;
- all new claims created during the pilot window;
- all new hypotheses created during the pilot window;
- generated-only Research Adversary review for each eligible item;
- human reviewer scoring after review consumption.

Out of scope:

- candidate promotion, rejection, qualification, disqualification, ranking, or demotion;
- replay override, replay relaxation, replay rerun, or replay-result mutation;
- qualification override or edge-gate mutation;
- governance override, governance change, or policy mutation;
- automatic paper-forward admission or paper placement;
- automatic memory writes;
- trade recommendation, broker execution, capital allocation, portfolio construction, or position sizing.

## Inputs Reviewed For Pilot Design

- `research_journal/reports/research_adversary_operational_readiness_001.md`
- `reports/atlas_v2_research_os/research_adversary_corpus/corpus_summary.md`
- `reports/atlas_v2_research_os/research_adversary_evaluation/latest/research_adversary_evaluation_summary.md`
- `research_journal/reports/taxonomy_recall_results_001.md`
- `research_journal/reports/research_adversary_governance_audit_001.md`

Relevant prior evidence:

- offline corpus reviewed 50 observations, 50 claims, and 50 hypotheses;
- offline corpus produced 750 assumptions, 1,070 constraints, and 600 falsification tests;
- offline corpus reported 0 authority violations and 150 `GENERATED_ONLY` artifacts;
- current human-scored evaluation still reports 0 evaluated cases, usefulness score 0.0, and estimated research hours saved 0;
- operational readiness recommendation was `LIMITED_OPERATIONAL_USE`, not full operational use.

## Current Pilot Metrics

These are pilot-window metrics as of report creation. They start at zero because the prospective window begins on 2026-06-05 and future items have not yet been reviewed.

| Metric | Current Value | Target / Interpretation |
| --- | ---: | --- |
| observations reviewed | 0 | count every new observation receiving generated-only adversary review |
| claims reviewed | 0 | count every new claim receiving generated-only adversary review |
| hypotheses reviewed | 0 | count every new hypothesis receiving generated-only adversary review |
| assumptions generated | 0 | count generated assumptions across all pilot reviews |
| constraints generated | 0 | count generated constraints across all pilot reviews |
| falsification proposals generated | 0 | count generated falsification proposals across all pilot reviews |
| reviewer usefulness score | 0.0 | human-scored, 0-5 average; useful pilot threshold >= 3.0 |
| false positives | 0 | human-labeled incorrect or non-material objections; acceptable rate <= 30% |
| estimated research hours saved | 0.0 | human-estimated net savings after review overhead |

## Review Contract

Each generated-only pilot review should record:

- source item type: observation, claim, or hypothesis;
- source item identifier;
- assumptions generated;
- constraints generated;
- falsification proposals generated;
- suspected taxonomy categories, if applicable;
- taxonomy confidence and ambiguity, if applicable;
- reviewer usefulness score;
- reviewer false-positive labels;
- estimated research minutes saved or added;
- authority-boundary confirmation.

Reviewer usefulness scoring:

| Score | Meaning |
| ---: | --- |
| 0 | no useful critique; added review overhead only |
| 1 | minor wording or generic reminder |
| 2 | useful but not action-shaping |
| 3 | changed the next research question or narrowed review scope |
| 4 | exposed a material assumption, constraint, or falsification gap |
| 5 | likely prevented a materially wasteful review path |

False positive definition:

A false positive is an adversary objection that a reviewer marks as incorrect, irrelevant, non-material, misleading, or unsupported by the source item. Ambiguous taxonomy labels are not automatically false positives if clearly labeled as weak or secondary hypotheses.

Research hours saved definition:

Estimated research hours saved must be net of review overhead. Count only avoided or shortened research work that a reviewer attributes to the adversary output.

## Decision Thresholds

At the end of the pilot window, assign one recommendation:

| Recommendation | Criteria |
| --- | --- |
| `CONTINUE` | useful signal exists, but sample size or measurement maturity is insufficient for expansion |
| `EXPAND` | at least 50 total items reviewed, usefulness score >= 3.5, false-positive rate <= 20%, positive net hours saved, and 0 authority violations |
| `HOLD` | usefulness score below 3.0, false-positive rate above 30%, or reviewer burden offsets estimated hours saved |
| `RETIRE` | repeated authority-boundary confusion, materially misleading output, negative net research value, or persistent false-positive rate above 40% |

Current recommendation: `CONTINUE`.

Reason: prior offline evidence and taxonomy results justify running the bounded prospective pilot, but prospective human-scored metrics are not yet available. Expansion is not justified until the pilot accumulates live workflow cases.

## Pilot Operating Rules

1. Keep every Research Adversary artifact `GENERATED_ONLY`.
2. Require human review before using any critique to change a research path.
3. Treat taxonomy categories as suspicion labels, not decisions.
4. Treat vocabulary bridge outputs as diagnostic context, not validation truth.
5. Keep adversary critique separate from candidate, replay, qualification, governance, and paper-forward state.
6. Record false positives explicitly instead of silently suppressing them.
7. Record review overhead so hours saved cannot be overstated.
8. Stop the pilot early if any output implies forbidden authority.

## Interim Interpretation

The pilot should continue because Research Adversary has shown enough retrospective and taxonomy-enhanced value to justify prospective measurement under strict boundaries.

The pilot should not expand yet because current prospective metrics are still zero, and the latest core evaluation summary still reports `NEEDS_HUMAN_REVIEW`, reviewer usefulness score 0.0, and estimated research hours saved 0.

## Authority Boundary

This report is generated-only and pilot-measurement only.

No authority expansion is granted.
No candidate influence is authorized.
No replay influence is authorized.
No qualification influence is authorized.
No governance influence is authorized.
No paper-forward influence is authorized.
No automatic memory write is authorized.
No trade recommendation is made or authorized.
No broker execution is authorized.
No capital allocation is authorized.
No portfolio construction is authorized.
No position sizing is authorized.
No automatic paper placement is authorized.

Research Adversary remains a generated-only, human-reviewed critique surface for the duration of this pilot.
