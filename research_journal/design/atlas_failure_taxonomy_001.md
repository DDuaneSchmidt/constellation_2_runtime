# Atlas Failure Taxonomy 001

Status: design and evaluation reference only.

This taxonomy formalizes recurring Atlas-specific failure patterns so Research Adversary can be evaluated against concrete failure classes instead of only generic research critique. It does not integrate with production systems, alter replay, alter qualification, change candidate state, update governance, write memory automatically, authorize paper placement, authorize trading, authorize capital, or promote any candidate.

## Reviewed Sources

- `research_journal/failures/FAIL_0004.yaml` through `FAIL_0016.yaml`
- `reports/atlas_v2_research_os/memory/failure_patterns.json`
- `reports/atlas_v2_research_os/failures/failure_index.json`
- `research_journal/reports/research_adversary_candidate_retrospective_001.md`
- `reports/atlas_v2_research_os/paper_forward_outcomes/latest_summary.md`
- `reports/atlas_v2_research_os/research_adversary_validation/failure_recall_study_001.md`
- `research_journal/reports/assumption_recall_study_001.md`

## FailureCategory Format

Each category uses:

- `id`
- `name`
- `description`
- `typical symptoms`
- `detection signals`
- `related failures`
- `suggested falsification questions`

## Categories

### REGIME_DEPENDENCY

- name: Regime Dependency
- description: A hypothesis, observation, or memory appears valid only under a regime that is unknown, mislabeled, unstable, or not reconstructable.
- typical symptoms:
  - UNKNOWN or conflicting regime labels.
  - Support disappears when trend, chop, volatility, or event context is separated.
  - Generated-only memory is reused without checking regime compatibility.
- detection signals:
  - `regime_context` is UNKNOWN, generated, or missing.
  - Mechanism success is concentrated in one market state.
  - Review language depends on after-the-fact regime assignment.
- related failures:
  - `failure-demo-opening-range`
  - `FAIL_0016`
- suggested falsification questions:
  - Does the claim still hold when replay samples are split by predeclared regime?
  - Can the regime be identified before the observation outcome is known?
  - Does support vanish outside the favored regime?

### PROXY_DEPENDENCY

- name: Proxy Dependency
- description: Evidence depends on a proxy instrument, broad index, daily fixture, or indirect artifact that may not represent the candidate-specific behavior being claimed.
- typical symptoms:
  - Candidate-specific data is absent or replaced by broad market evidence.
  - Daily proxy evidence is used for intraday mechanism claims.
  - SPY or index behavior explains the result better than the candidate mechanism.
- detection signals:
  - Source artifact ids refer to proxy fixtures rather than candidate samples.
  - Candidate review mentions proxy limitations.
  - Expectancy improves only under broad market proxy filters.
- related failures:
  - `FAIL_0008`
  - `FAIL_0016`
- suggested falsification questions:
  - Does the result survive candidate-specific data reconstruction?
  - Does a naive proxy baseline explain the same return profile?
  - Is the asserted mechanism observable in the candidate artifact itself?

### RUNTIME_DEPENDENCY

- name: Runtime Dependency
- description: An architecture or workflow decision assumes supporting runtime truth, artifact availability, or legacy compatibility that has not actually been retired or proven.
- typical symptoms:
  - Architecture says a layer is obsolete while runtime still consumes it.
  - Source artifacts referenced by execution paths cannot be loaded.
  - Lineage passes but runtime execution fails closed.
- detection signals:
  - `ArtifactStoreError`
  - Runtime truth graph still references legacy artifacts.
  - Execution path has missing `source_artifact_ids`.
- related failures:
  - `FAIL_0011`
  - `failure-2026-06-05-autonomous_research-artifactstoreerror-64d655a35e43`
  - `failure-2026-06-05-autonomous_research-artifactstoreerror-c54059bb338d`
- suggested falsification questions:
  - Does verified runtime truth still consume the dependency?
  - Can every source artifact be loaded before work is selected?
  - Does the workflow fail closed when a required artifact is absent?

### DATA_QUALITY

- name: Data Quality
- description: A claim depends on missing, immature, partial, stale, or non-reconstructable data rather than observed evidence.
- typical symptoms:
  - Macro, event, sleeve, or intraday data is missing.
  - Sample evidence is underpowered or unresolved.
  - Data availability is inferred from workflow progress.
- detection signals:
  - Missing required data.
  - Insufficient sample size.
  - Open observations exceed closed outcomes.
  - Event or macro metadata cannot be reconstructed.
- related failures:
  - `FAIL_0006`
  - `FAIL_0009`
  - `FAIL_0012`
- suggested falsification questions:
  - Is the required data present, timestamped, and reconstructable?
  - Are there enough resolved samples to support the claim?
  - Would the conclusion change if unresolved or partial samples were excluded?

### WORKER_COMPATIBILITY

- name: Worker Compatibility
- description: A ready backlog item or research action is selected even though no compatible connected worker can execute it.
- typical symptoms:
  - Ready queue contains items that cannot run.
  - Worker registry has no connected worker for the required capability.
  - Execution fails before research evidence is generated.
- detection signals:
  - `NO_COMPATIBLE_CONNECTED_WORKER`
  - Required worker capability absent from registry.
  - Backlog status and worker availability disagree.
- related failures:
  - `failure-2026-06-04-worker-no-compatible-connected-worker-c5b07de5e574`
- suggested falsification questions:
  - Is there a connected worker advertising the required capability?
  - Does the backlog item declare requirements that match available workers?
  - Can a dry-run execute without selecting an incompatible item?

### CERTIFICATION_BLOCKER

- name: Certification Blocker
- description: A workflow treats partial safety, lineage, or governance progress as sufficient even though certification still blocks execution.
- typical symptoms:
  - Blocker count drops but certification is still not passed.
  - Lineage and governance pass while certification fails.
  - Research readiness is confused with execution readiness.
- detection signals:
  - `CERTIFICATION_BLOCK`
  - `SAFETY_GATE_FAILED`
  - Certification status is blocked despite fewer blockers.
- related failures:
  - `failure-2026-06-05-autonomous_research-safety-gate-failed-794b177f0415`
  - `failure-2026-06-05-certification-certification-block-0d8cd9475db6`
- suggested falsification questions:
  - Has certification fully passed, rather than merely improved?
  - Which blocker remains unresolved?
  - Does the workflow stay blocked when certification has not passed?

### WARNING_RECURRENCE

- name: Warning Recurrence
- description: Non-blocking warnings are treated as one-day operational noise even when repeated warnings predict later blocked readiness.
- typical symptoms:
  - Daily integrity passes with repeated warnings.
  - Warning categories recur across several available days.
  - Warnings are not converted into research follow-up.
- detection signals:
  - Same warning category recurs for multiple days.
  - Warning state later escalates to blocked graph readiness.
  - Report says clean while warning list remains non-empty.
- related failures:
  - `FAIL_0013`
  - `FAIL_0015`
- suggested falsification questions:
  - Did this warning clear or recur across the next review window?
  - Does the same category appear in multiple integrity reports?
  - Would treating warnings as evidence gaps have changed the review?

### DUPLICATE_CLUSTER

- name: Duplicate Cluster
- description: Multiple candidates, claims, or observation clusters count the same structural idea, sample days, or mechanism exposure as independent evidence.
- typical symptoms:
  - Many candidates share the same mechanism and source window.
  - Near-duplicate claims inflate throughput or support.
  - Paper-forward plans consume capacity on overlapping samples.
- detection signals:
  - Same mechanism and regime repeated across candidate ids.
  - Deduplication or cluster-splitting reports flag overlap.
  - Recommended reviews contain redundant candidate families.
- related failures:
  - `FAIL_0007`
  - `FAIL_0014`
- suggested falsification questions:
  - Are these candidates independent or the same exposure restated?
  - Do sample days overlap across the candidate cluster?
  - Does the evidence remain after one representative per cluster is kept?

### INSUFFICIENT_EVIDENCE

- name: Insufficient Evidence
- description: Architecture, documentation, replay, or generated claims are treated as stronger than the evidence level permits.
- typical symptoms:
  - Architecture maturity outruns empirical validation.
  - Positive replay is used without sufficient sample size.
  - Research artifacts exist but remain underpowered.
- detection signals:
  - Evidence level is GENERATED_ONLY or HISTORICAL_REPLAY only.
  - Small sample positive replay.
  - No closed paper-forward outcomes.
  - Capital or readiness language appears before evidence is mature.
- related failures:
  - `FAIL_0004`
  - `FAIL_0006`
  - `FAIL_0010`
- suggested falsification questions:
  - What independent evidence supports the claim beyond architecture or generation?
  - How many closed samples exist?
  - Would the conclusion survive if replay-only evidence were labeled as non-forward evidence?

### MECHANISM_MISMATCH

- name: Mechanism Mismatch
- description: The named mechanism does not explain the observed behavior as well as an alternate mechanism, baseline, or broader market condition.
- typical symptoms:
  - Technical indicator support is broad but not relationship-specific.
  - Volatility expansion, breakout, opening range, and liquidity sweep overlap.
  - Mechanism works only under naive baseline recovery.
- detection signals:
  - `mechanism_tags` are broad or interchangeable.
  - Baseline-recoverable performance.
  - Support lacks relationship-specific evidence.
- related failures:
  - `FAIL_0008`
  - `FAIL_0016`
- suggested falsification questions:
  - What competing mechanism explains the same samples?
  - Does the effect survive a naive baseline comparison?
  - Can the relationship-specific mechanism be observed before outcome?

### QUALIFICATION_MISMATCH

- name: Qualification Mismatch
- description: A candidate's narrative, replay score, or workflow status is mistaken for qualification, eligibility, or review readiness.
- typical symptoms:
  - Positive replay exists but edge score remains below threshold.
  - Paper workflow progress is read as readiness progress.
  - Candidate volume is confused with quality evidence.
- detection signals:
  - `paper_trade_eligible` is false despite positive narrative.
  - Edge score below threshold.
  - Qualification or certification reason contradicts readiness language.
- related failures:
  - `FAIL_0005`
  - `FAIL_0007`
  - `FAIL_0010`
- suggested falsification questions:
  - What exact qualification threshold has been met?
  - Does the candidate remain ineligible despite supporting narrative?
  - Which gate is blocking the candidate and why?

### OBSERVATION_DRIFT

- name: Observation Drift
- description: Observed forward samples, open positions, or accumulated observations drift away from the original hypothesis, trigger, or evidence question.
- typical symptoms:
  - Open observations accumulate faster than resolved outcomes.
  - Paper-forward samples do not match replay recurrence rules.
  - Observation plans lack predeclared invalidation rules.
- detection signals:
  - Sample size grows while wins/losses remain unresolved.
  - `hypothesis_supported` and `hypothesis_weakened` both lack clear cause.
  - Minimum sample size or trigger definition is absent.
- related failures:
  - `FAIL_0006`
- suggested falsification questions:
  - Do forward observations match the original trigger and mechanism?
  - Are closed outcomes reviewed before strengthening the claim?
  - What invalidation rule prevents drift from becoming anecdote?

### SELECTION_BIAS

- name: Selection Bias
- description: The research process overweights selected, surviving, convenient, or high-volume artifacts and underweights blocked, dormant, or rejected artifacts.
- typical symptoms:
  - Candidate count is treated as progress.
  - Low-output sleeves are judged without checking valid no-signal states.
  - Only supported replay examples are emphasized.
- detection signals:
  - Rejected, dormant, or blocked items omitted from summary.
  - Throughput metrics are cited without quality metrics.
  - Surviving clusters share source or mechanism concentration.
- related failures:
  - `FAIL_0007`
  - `FAIL_0014`
- suggested falsification questions:
  - What did the rejected or dormant artifacts show?
  - Does the conclusion hold when blocked and no-signal sleeves are included?
  - Are volume metrics separated from quality metrics?

### CONTEXT_OMISSION

- name: Context Omission
- description: A review omits required market, macro, event, calendar, source, or governance context needed to interpret an artifact.
- typical symptoms:
  - Event reaction lacks event metadata.
  - Macro readiness is inferred from workflow behavior.
  - Session or calendar boundary is not specified.
- detection signals:
  - `market_context`, event source, or regime fields are empty.
  - Claim uses broad context language without source lineage.
  - Governance or authority context is missing from the artifact.
- related failures:
  - `FAIL_0012`
  - `FAIL_0016`
- suggested falsification questions:
  - Which context fields are required to reproduce the claim?
  - Would the conclusion change under different event, macro, or calendar context?
  - Is the missing context an evidence gap rather than a workflow gap?

## Crosswalk To FAIL_0004 Through FAIL_0016

| Failure | Primary taxonomy category | Secondary categories |
| --- | --- | --- |
| `FAIL_0004` | INSUFFICIENT_EVIDENCE | DATA_QUALITY |
| `FAIL_0005` | QUALIFICATION_MISMATCH | CERTIFICATION_BLOCKER |
| `FAIL_0006` | OBSERVATION_DRIFT | DATA_QUALITY, INSUFFICIENT_EVIDENCE |
| `FAIL_0007` | SELECTION_BIAS | DUPLICATE_CLUSTER, QUALIFICATION_MISMATCH |
| `FAIL_0008` | MECHANISM_MISMATCH | PROXY_DEPENDENCY |
| `FAIL_0009` | DATA_QUALITY | WORKER_COMPATIBILITY |
| `FAIL_0010` | INSUFFICIENT_EVIDENCE | QUALIFICATION_MISMATCH |
| `FAIL_0011` | RUNTIME_DEPENDENCY | CONTEXT_OMISSION |
| `FAIL_0012` | CONTEXT_OMISSION | DATA_QUALITY |
| `FAIL_0013` | WARNING_RECURRENCE | INSUFFICIENT_EVIDENCE |
| `FAIL_0014` | SELECTION_BIAS | DUPLICATE_CLUSTER |
| `FAIL_0015` | WARNING_RECURRENCE | CERTIFICATION_BLOCKER |
| `FAIL_0016` | MECHANISM_MISMATCH | REGIME_DEPENDENCY, PROXY_DEPENDENCY |

## Research Adversary Use

This taxonomy should be used to evaluate whether Research Adversary V0.1 can recover Atlas-specific failure classes:

- Did it name the failure category directly?
- Did it ask falsification questions that would expose the category?
- Did it confuse a generic warning with the concrete Atlas failure?
- Did it avoid authority-expanding language while surfacing the risk?

Research Adversary may generate only critique artifacts. The taxonomy does not permit Research Adversary to override replay, qualification, certification, governance, candidate review, paper-forward observation, or human review.

## Authority Boundary

This taxonomy has no authority to:

- recommend trades
- allocate capital
- size positions
- construct portfolios
- promote candidates
- approve candidates
- override replay
- override qualification
- override governance
- bypass paper-forward testing
- write memory automatically
- integrate with production pipelines

