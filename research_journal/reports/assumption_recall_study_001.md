# Assumption Recall Study 001

Date: 2026-06-05
Status: Evaluation study only

Scope: determines whether Research Adversary assumptions match hidden assumptions later identified in Atlas research failures and candidate retrospective review. This study does not modify original failure records, adversary corpus artifacts, candidate state, replay state, qualification state, governance state, paper-forward state, broker execution, live trading, position sizing, or capital allocation.

## Inputs Reviewed

- Historical failures: `research_journal/evaluation_sets/research_adversary/historical_failure_eval_set_001.json`
- Candidate retrospective report: `research_journal/reports/research_adversary_candidate_retrospective_001.md`
- Adversary corpus: `reports/atlas_v2_research_os/research_adversary_corpus/`

The adversary corpus contains 150 generated-only reviews: 50 observations, 50 claims, and 50 hypotheses. The corpus summary reports 750 assumptions extracted, 1,070 constraints extracted, 600 falsification tests proposed, and 0 authority violations.

## Scoring Method

ASSUMPTION_RECALL values:

- `FOUND`: the adversary assumption surface directly identified the later hidden assumption.
- `PARTIAL`: the adversary identified a related general assumption, but missed important Atlas-specific detail.
- `MISSED`: the adversary did not identify the material hidden assumption.

Because the corpus review schema stores assumptions mostly as adversarial questions, risk flags, invalidation tests, and missing-evidence statements, this study treats those fields as the adversary assumption surface.

Weighted recall formula used for `assumption_recall_rate`:

```text
(FOUND + 0.5 * PARTIAL) / labeled_hidden_assumptions
```

Useful assumption formula used for `useful_assumption_rate`:

```text
(FOUND + PARTIAL) / labeled_hidden_assumptions
```

## Metrics

- Labeled hidden assumptions reviewed: 32
- Historical failure assumptions reviewed: 19
- Candidate retrospective assumptions reviewed: 13
- Adversary corpus reviews inspected: 150
- Corpus assumptions extracted: 750
- `assumptions_per_review`: 5.00
- `ASSUMPTION_RECALL` counts: FOUND 1, PARTIAL 23, MISSED 8
- `assumption_recall_rate`: 39.1%
- Exact found rate: 3.1%
- `useful_assumption_rate`: 75.0%

Interpretation: the adversary is directionally useful as a generic critique layer, but it is not yet strong enough at recovering concrete hidden Atlas assumptions. Most useful matches are partial, not exact.

## Historical Failure Scores

| Case | ASSUMPTION_RECALL | Later Hidden Assumption | Study Note |
| --- | --- | --- | --- |
| `FAIL_0004` | PARTIAL | Architecture maturity implied comparable evidence maturity. | Corpus catches missing evidence and unproven repeatability, but not architecture-versus-evidence maturity directly. |
| `FAIL_0005` | PARTIAL | Paper tracking progress implied authority readiness. | Corpus catches no promotion, replay, qualification, or paper-placement authority, but not paper workflow readiness as a specific false premise. |
| `FAIL_0006` | PARTIAL | Accumulated observations and open paper positions quickly become resolved evidence. | Corpus catches no independent evidence and generated-only status, but not the closed-outcome maturation assumption. |
| `FAIL_0007` | MISSED | Candidate or raw signal volume indicated candidate quality. | Corpus does not materially challenge volume-as-quality or throughput-as-progress. |
| `FAIL_0008` | PARTIAL | Broad technical indicator support would survive relationship-specific review. | Corpus challenges mechanism separability and data-selection effects, but not relationship-specific support or baseline recoverability. |
| `FAIL_0009` | MISSED | Active sleeve status implied candidate flow capability. | Corpus does not identify sleeve dormancy, no-signal states, or missing sleeve data as hidden assumptions. |
| `FAIL_0010` | PARTIAL | Research documentation implied readiness for capital review. | Corpus strongly enforces no capital or trading authority, but does not identify underpowered evidence as the capital-review blocker. |
| `FAIL_0011` | MISSED | Desired architecture state was enough to retire legacy runtime dependencies. | Corpus does not identify runtime truth consumers or legacy dependency assumptions. |
| `FAIL_0012` | PARTIAL | Workflow expansion could compensate for missing macro readiness evidence. | Corpus catches missing evidence and authority boundaries, but not macro-readiness data maturity specifically. |
| `FAIL_0013` | MISSED | Clean audit state meant research understanding was mature. | Corpus does not distinguish auditability from understanding maturity or recurring warnings. |
| `FAIL_0014` | MISSED | Low sleeve output meant poor sleeve quality. | Corpus does not perform causal separation among valid no-signal, data need, and conversion bottleneck states. |
| `FAIL_0015` | MISSED | Non-blocking warnings could be treated as one-day operational noise. | Corpus does not check warning recurrence or escalation. |
| `FAIL_0016` | PARTIAL | Standalone indicator evidence could support durable strategy claims without mechanism and regime specificity. | Corpus catches mechanism and regime context, but not out-of-fixture generalization or naive baseline separation. |
| `failure-demo-opening-range` | FOUND | Generated-only opening-range memory could be reused without testing regime compatibility. | Corpus directly checks generated-only status and whether regime context is unknown or conflicts with the source. |
| `failure-2026-06-04-worker-no-compatible-connected-worker-c5b07de5e574` | PARTIAL | A ready backlog item could be executed by the available worker set. | Corpus mentions execution feasibility generally, but not connected-worker compatibility. |
| `failure-2026-06-05-autonomous_research-artifactstoreerror-64d655a35e43` | PARTIAL | Referenced source artifacts existed and could be loaded. | Corpus checks source reconstruction and source boundaries, but not artifact-store existence. |
| `failure-2026-06-05-autonomous_research-artifactstoreerror-c54059bb338d` | PARTIAL | Backlog selection implied all input artifacts were available. | Corpus catches source availability generically, but not selected-input artifact loading. |
| `failure-2026-06-05-autonomous_research-safety-gate-failed-794b177f0415` | MISSED | Passing lineage and governance was enough despite certification failure. | Corpus does not identify partial safety-gate pass as insufficient. |
| `failure-2026-06-05-certification-certification-block-0d8cd9475db6` | MISSED | A smaller blocker count meant certification was close enough for execution. | Corpus does not inspect certification blocker counts or warning status. |

Historical subtotal: FOUND 1, PARTIAL 10, MISSED 8. Weighted recall: 31.6%. Useful assumption rate: 57.9%.

## Candidate Retrospective Scores

| Candidate | ASSUMPTION_RECALL | Later Hidden Assumption | Study Note |
| --- | --- | --- | --- |
| `ptc_f492d1bebb6bd47f` | PARTIAL | Trending regime stability, replay reconstruction, and proxy representativeness. | Corpus catches regime and reconstruction, but misses proxy and candidate-specific behavior. |
| `ptc_3d46c2d50e5fe58b` | PARTIAL | Breakout confirmation must be predefined and trend context must not be post-breakout labeling. | Corpus catches chronology and regime conflict, but not breakout confirmation precision. |
| `ptc_7f054744c90194c8` | PARTIAL | VWAP reclaim must be causal, not proxy risk-on behavior; UNKNOWN regime must be acceptable; trigger must avoid subjective chart reading. | Corpus catches mechanism and regime ambiguity, but misses proxy and subjective-trigger risks. |
| `ptc_fc801836c012a96d` | PARTIAL | Event type, timing, expectation, and source reconstructability must be known before observation. | Corpus catches source and chronology generally, but not event metadata lineage. |
| `ptc_b57c4cfe8ed94fff` | PARTIAL | Session window must be predefined and survive calendar, volatility, and day-type controls. | Corpus catches chronology and duplication, but not calendar or session-boundary controls. |
| `ptc_ffad13e2dbe09619` | PARTIAL | Reversal trigger must be distinct from mean reversion and invalidation must be known before observation. | Corpus catches mechanism separation, but not reversal-specific overlap. |
| `ptc_9112fdddb80509f1` | PARTIAL | Trend continuation must be distinct from breakout and not merely SPY proxy direction. | Corpus catches mechanism and regime separation, but misses SPY proxy dependence. |
| `ptc_b97beffb99f141ae` | PARTIAL | Opening range duration and trigger must be fixed and robust to alternate range lengths. | Corpus catches regime conflict and reconstruction, but not opening-range parameter sensitivity. |
| `ptc_695a47fe74f8e36d` | PARTIAL | Candidate must not duplicate the stronger mean-reversion candidate; unknown regime and reversal horizon must be predeclared. | Corpus catches duplication and regime ambiguity, but not candidate-pair redundancy. |
| `ptc_78687792cbb592e8` | PARTIAL | Liquidity sweep definition and reference level must be measurable before observation. | Corpus catches source reconstruction, but not liquidity-level subjectivity. |
| `ptc_0614c2bf7a7c39b0` | PARTIAL | Volatility expansion must specify directionality and not only variance increase. | Corpus catches mechanism usefulness generally, but not directionality ambiguity. |
| `ptc_2c34d7d9d627b7e7` | PARTIAL | Breakout must work outside known trend context and not duplicate the stronger breakout candidate. | Corpus catches regime and duplication generally, but not cluster-specific redundancy. |
| `ptc_6fe5fd4fe1dba60c` | PARTIAL | Positive replay with 10 samples was not enough; high profit factor could be unstable; UNKNOWN regime weakened interpretation. | Corpus catches replay and regime limitations, but not edge threshold or small-sample profit-factor instability. |

Candidate subtotal: FOUND 0, PARTIAL 13, MISSED 0. Weighted recall: 50.0%. Useful assumption rate: 100.0%.

## What The Adversary Finds Reliably

The corpus repeatedly identifies these assumption classes:

- source artifact may mix observation, claim, hypothesis, and candidate language
- mechanism must be separable from broad market regime or data-selection effects
- regime context may be unknown or conflict with the source
- duplicated observations or survivorship bias may explain the pattern
- generated-only output is not independent holdout evidence
- replay, qualification, promotion, and paper-placement evidence are not produced by the adversary review
- chronology, independence, and execution feasibility are not verified by offline template review

These are useful assumptions. They are broad, repeatable, and safe.

## What The Adversary Misses

The corpus does not reliably identify these later-important hidden assumptions:

- proxy evidence is representative of candidate-specific behavior
- SPY daily proxy evidence can support intraday or candidate-specific mechanisms
- small replay sample sizes can make high profit factor unstable
- candidate volume is a quality signal
- sleeve activity implies sleeve candidate-flow capability
- low sleeve output means poor sleeve quality
- runtime truth no longer consumes legacy artifacts
- warnings can be ignored if not currently blocking
- reduced certification blocker count is close enough to certification pass
- worker compatibility and artifact-store availability are prerequisites for autonomous research execution
- event metadata, macro readiness, and source lineage are available before observation
- mechanism clusters can double-count the same market days

These misses explain why exact assumption recall is low even though useful broad critique is common.

## Study Judgment

Research Adversary V0.1 has useful generic assumption coverage, especially around regime, source boundaries, generated-only status, and evidence separation. It does not yet recover the concrete hidden assumptions that later explain many Atlas failures.

The strongest current use is pre-review discipline: it can force humans to ask better questions before paper-forward review. The weakest current use is precise diagnosis: it cannot yet be trusted to catch proxy dependence, runtime dependency, certification, worker compatibility, or candidate-cluster assumptions without human labels or richer artifact context.

## Recommendation

Hold authority steady. Do not expand authority or integrate into candidate, replay, qualification, governance, or paper-forward state.

Next measurement work should add labeled assumption fixtures for:

- proxy dependency
- candidate-specific data availability
- sample-size sufficiency
- mechanism duplication across candidate clusters
- runtime truth dependency
- worker compatibility
- artifact-store lineage
- certification blocker interpretation
- warning recurrence

Research Adversary should advance only after exact assumption recall improves, not merely because useful partial critique is common.
