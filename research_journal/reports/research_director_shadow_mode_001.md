# Research Director Shadow Mode 001

Date: 2026-06-05

Status: SHADOW_MODE_RECORD

Authority: No authority. No workflow changes. No prioritization changes. This report records only what Research Director would have prioritized.

## Scope

Run Research Director in shadow mode and record its non-authoritative selections for new research cycles.

This report does not:

- change research priorities
- route work
- modify candidates
- modify replay
- modify validation
- modify qualification
- write accepted memory
- alter governance
- recommend trades
- allocate capital
- authorize broker execution
- change paper or live workflows

## Shadow Run Source

Shadow run source:

- `constellation_2/common/atlas_v2_research_os/research_director_evaluation.py`
- `research_journal/reports/research_director_evaluation_001.md`

Run mode:

- generated-only
- evaluation-only
- read-only inputs
- no production integration
- no authority

Shadow run timestamp:

- `2026-06-06T00:04:06Z`

The timestamp is UTC. The local report date is 2026-06-05.

## Shadow Cycle Ledger

### SHADOW_CYCLE_001

Cycle basis: latest available Research Director evaluation and current Atlas research reports.

| Field | Shadow Selection | Score | Basis |
| --- | --- | ---: | --- |
| top bottleneck | Data Coverage | 0.9445 priority score | Direct validation history showed low initial direct replay coverage and validation blockage from insufficient data. |
| top uncertainty | Replay Attrition | 0.8925 uncertainty score | Candidates with coverage still lost usable samples during trigger, regime, daily-vs-intraday, or proxy replay filtering. |
| top experiment | Vocabulary bridge diagnostic: `CHOP -> RANGE_BOUND` | derived | The current evaluator does not emit a native experiment ranking. The highest-leverage bounded experiment is derived from the top uncertainty plus the vocabulary mismatch diagnosis: test whether explicit `CHOP -> RANGE_BOUND` bridging recovers samples without granting validation authority. |
| top research debt item | Data Coverage | 0.9055 research debt score | Missing or incomplete direct data coverage creates recurring validation drag and blocks downstream interpretation. |

Full priority order from the shadow run:

1. Data Coverage
2. Replay Attrition
3. Vocabulary Mismatch
4. Failure Taxonomy
5. Research Adversary
6. Research Economics
7. Search Space Mapping
8. More Hypothesis Generation

Full uncertainty order from the shadow run:

1. Replay Attrition
2. Vocabulary Mismatch
3. Data Coverage
4. Failure Taxonomy
5. Research Adversary
6. Research Economics
7. Search Space Mapping
8. More Hypothesis Generation

Full research-debt order from the shadow run:

1. Data Coverage
2. Replay Attrition
3. Vocabulary Mismatch
4. Failure Taxonomy
5. Research Adversary
6. Research Economics
7. Search Space Mapping
8. More Hypothesis Generation

## Actual Actions Taken During Initial Shadow Window

The initial shadow window contains report-only work. No workflow or priority changes were made.

Actual actions visible in the research journal:

- regime vocabulary inventory
- CHOP semantics audit
- vocabulary bridge effectiveness measurement
- Research Director operational readiness audit
- this shadow-mode record

These actions primarily address `Vocabulary Mismatch` and `Replay Attrition`. They do not directly complete the top bottleneck/debt item, `Data Coverage`.

## Comparison Against Actual Actions

| Shadow Selection | Actual Action Alignment | Classification | Notes |
| --- | --- | --- | --- |
| Data Coverage as top bottleneck | No direct data-coverage action in this initial report-only window | PENDING / NOT_ACTED | Not treated as a false priority yet because the user explicitly requested no workflow changes and the window contains analysis reports only. |
| Replay Attrition as top uncertainty | Vocabulary and bridge reports directly analyze why replay samples are lost | USEFUL | The CHOP and bridge audits explain sample attrition caused by exact regime matching. |
| Vocabulary bridge diagnostic as top experiment | Bridge effectiveness report measured `CHOP -> RANGE_BOUND` sample recovery | USEFUL | The diagnostic recovered 396 additional samples in simulation, made 3 candidates evaluable, and confirmed 2 candidates in bridge-enabled simulation while preserving authority boundaries. |
| Data Coverage as top research debt item | No direct debt-reduction action in this initial report-only window | PENDING / NOT_ACTED | Data Coverage remains the top recurring debt item for future shadow comparison. |

## Metrics

### Prediction Accuracy

Prospective prediction accuracy: PENDING.

Reason:

The shadow pilot has just started. There is not yet a completed future research cycle where shadow predictions can be scored against independently chosen actual actions and outcomes.

Initial retrospective alignment:

- top bottleneck: not yet acted
- top uncertainty: aligned
- top experiment: aligned by derived diagnostic
- top research debt item: not yet acted

Initial alignment count:

- aligned: 2
- pending/not acted: 2
- contradicted: 0

Initial accuracy should not be treated as final because the pilot has not accumulated prospective cycles.

### Useful Prioritizations

Useful prioritizations observed in the initial window: 2.

1. Replay Attrition as the top uncertainty was useful because the actual reports focused on why samples disappeared after trigger/regime/proxy filtering.
2. The derived vocabulary bridge diagnostic was useful because the bridge effectiveness report quantified sample recovery, candidate evaluability, confirmations, remaining blockers, false-positive risk, and bridge confidence.

### Missed Prioritizations

Missed or unacted prioritizations in the initial window: 2 pending.

1. Data Coverage as top bottleneck was not directly acted in this report-only window.
2. Data Coverage as top research debt item was not directly reduced in this report-only window.

These are classified as pending rather than misses because the operator requested no workflow changes and no prioritization changes.

### Research Hours Saved Estimate

Measured hours saved: 0.

Reason:

No prospective cycle has completed with a measured no-Director baseline, reviewer time ledger, or operator-scored usefulness result.

Initial shadow estimate: 1-3 hours saved.

Basis:

- Research Director surfaced the same replay/vocabulary lane that the actual report-only work investigated.
- The shadow ledger reduces retrospective ambiguity by recording the would-have-prioritized items before future action comparison.
- The estimate is deliberately narrow and should be replaced by measured reviewer time after future cycles.

Do not combine this initial shadow estimate with the broader 19-40 hour historical Research Director estimate from the prediction accuracy audit. That earlier number measured retrospective Director usefulness, not this shadow pilot.

## Pilot Scoring Rules For Future Cycles

For every new research cycle, append one row with:

- top bottleneck
- top uncertainty
- top experiment
- top research debt item
- actual action taken
- outcome after action
- prediction classification
- estimated or measured hours saved

Prediction classification:

- EXACT: shadow selection matched actual high-value work and outcome.
- PARTIAL: shadow selection matched the right area but not the precise action.
- MISS: actual high-value work was outside the shadow selection.
- FALSE: shadow selection would have consumed effort without useful learning.
- PENDING: not enough outcome evidence yet.

Useful prioritization:

- Count only EXACT or PARTIAL items that reduced uncertainty, removed a blocker, clarified failure cause, reduced research debt, or prevented wasted work.

Missed prioritization:

- Count actual high-value work that was not present in the shadow top selections.

Research hours saved:

- Record measured reviewer/operator time when available.
- Otherwise estimate conservatively and label as estimate.
- Keep saved hours separate from analysis overhead.

## Current Pilot Status

Pilot status: ACTIVE_SHADOW_ONLY

Current evidence supports continuing shadow recording.

No authority should be added.

No workflow should be changed.

No actual priority should be changed by this report.

## Final Output

Research Director shadow mode cycle 001 recorded.

Current top selections:

- top bottleneck: Data Coverage
- top uncertainty: Replay Attrition
- top experiment: Vocabulary bridge diagnostic, `CHOP -> RANGE_BOUND`
- top research debt item: Data Coverage

Current metrics:

- Prediction Accuracy: PENDING_PROSPECTIVE, initial alignment 2 aligned / 2 pending / 0 contradicted
- Useful Prioritizations: 2
- Missed Prioritizations: 0 confirmed, 2 pending/not acted
- Research Hours Saved Estimate: 1-3 hours, unmeasured
