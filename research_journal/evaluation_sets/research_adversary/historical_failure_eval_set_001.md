# Research Adversary Historical Failure Eval Set 001

Date: 2026-06-05
Status: Evaluation data only

Scope: curated historical failure recall set for Research Adversary evaluation. This set does not alter original failure records, create candidates, change replay state, change qualification state, change governance state, or change paper-forward state.

Authority boundary: this evaluation set is for measuring Research Adversary recall and usefulness only. It does not authorize candidate promotion, trade recommendations, capital recommendations, position sizing, live trading, broker execution, replay override, qualification override, or governance override.

## Sources

- `research_journal/failures/FAIL_0004.yaml` through `research_journal/failures/FAIL_0016.yaml`
- `reports/atlas_v2_research_os/memory/failure_patterns.json`
- `reports/atlas_v2_research_os/failures/failure_registry.jsonl`
- `reports/atlas_v2_research_os/failures/latest.json`

The source records were read only. This file and the JSON companion are derived evaluation data.

## Evaluation Use

For each case, a Research Adversary review should be scored on whether it detects the known failure mode, known constraint, hidden assumption, and expected detection terms.

Suggested recall labels:

- `EXACT`: detects the specific failure and constraint
- `PARTIAL`: detects a related risk but misses important specificity
- `MISS`: does not detect the known failure
- `FALSE`: invents unsupported evidence or misstates the source

## Cases

| Failure ID | Severity | Known Failure Mode | Expected Detection Terms |
| --- | --- | --- | --- |
| `FAIL_0004` | HIGH | Architecture maturity was mistaken for evidence maturity. | architecture versus evidence; empirical validation lag; evidence maturity; runtime readiness is not validation |
| `FAIL_0005` | CRITICAL | Paper workflow progress was mistaken for runtime readiness progress. | paper progress is not readiness; live readiness gate; authority remains blocked; separate paper lifecycle from runtime authority |
| `FAIL_0006` | HIGH | Observation accumulation was mistaken for meaningful outcome evidence. | open observations are not outcomes; outcome maturation; closed sample requirement; resolved evidence |
| `FAIL_0007` | HIGH | Candidate volume was treated as a proxy for candidate quality. | volume is not quality; certification blocker; portfolio gate; evidence quality before throughput |
| `FAIL_0008` | HIGH | Standalone technical-indicator claims failed fixture review. | false positive; relationship-specific support; baseline recoverability; fixture review |
| `FAIL_0009` | MEDIUM | Active sleeves did not produce broadly distributed candidate flow. | sleeve concentration; dormant sleeve; missing required data; no signal condition |
| `FAIL_0010` | CRITICAL | Research artifacts existed, but no hypotheses were ready for capital review. | underpowered evidence; blocked evidence; attention allocation not capital review; capital review not authorized |
| `FAIL_0011` | HIGH | Legacy compatibility layers could not be retired by architecture decision alone. | runtime dependency; verified runtime truth; legacy artifact still consumed; deprecation readiness |
| `FAIL_0012` | CRITICAL | Macro readiness gaps could not be repaired by expanding workflow behavior. | missing data; immature data; research-only; do not fabricate authority |
| `FAIL_0013` | HIGH | A clean audit state was mistaken for mature research understanding. | auditability is not understanding; warnings matter; runtime readiness blocked; maturity not proven |
| `FAIL_0014` | MEDIUM | Low sleeve candidate production was misdiagnosed as poor sleeve quality. | misdiagnosed sleeve quality; valid no-signal; data need; conversion rejection |
| `FAIL_0015` | HIGH | Recurring integrity warnings were treated as operational noise. | recurring warnings; non-blocking does not mean clean; candidate integrity follow-through; graph readiness blocked |
| `FAIL_0016` | HIGH | Standalone technical-indicator evidence did not support durable Technical Strategy Factory claims. | out-of-fixture generalization; naive baseline; relationship-specific; mechanism and regime context |
| `failure-demo-opening-range` | MEDIUM | Opening-range memory carried a regime mismatch warning. | regime mismatch; generated-only; active warning; not validation |
| `failure-2026-06-04-worker-no-compatible-connected-worker-c5b07de5e574` | ERROR | Selected backlog item had no compatible connected worker. | no compatible worker; worker capability mismatch; backlog item not executable; connected worker requirement |
| `failure-2026-06-05-autonomous_research-artifactstoreerror-64d655a35e43` | ERROR | Priority or certification path referenced a missing artifact. | missing artifact; artifact lineage; artifact store; priority influence |
| `failure-2026-06-05-autonomous_research-artifactstoreerror-c54059bb338d` | ERROR | Input artifact loading failed for a selected human-review failure-analysis item. | input artifact missing; backlog input unavailable; artifact-demo-midday-chop; load input artifacts |
| `failure-2026-06-05-autonomous_research-safety-gate-failed-794b177f0415` | ERROR | Autonomous research failed closed after certification failure despite lineage and governance passing. | certification failed; safety gate failed; failed closed; partial gate pass is insufficient |
| `failure-2026-06-05-certification-certification-block-0d8cd9475db6` | CERTIFICATION_BLOCK | Research OS certification remained blocked even after blocker count dropped. | certification block; blocker count; warnings still present; not certified |

## Detailed Review Targets

### `FAIL_0004`

- Known constraint: architecture readiness and empirical validation mature on different timelines.
- Hidden assumption: mature architecture implies comparable maturity in outcome evidence, sleeve evidence, and factory claims.
- Discovery: journal failure record found evidence maturity lagged architecture readiness.

### `FAIL_0005`

- Known constraint: paper lifecycle artifacts and live readiness gates answer different governance questions.
- Hidden assumption: more paper tracking artifacts mean authority readiness has improved.
- Discovery: journal failure record found paper tracking progressed while runtime authority remained blocked.

### `FAIL_0006`

- Known constraint: outcome maturation requires elapsed time and closed samples.
- Hidden assumption: accumulated observations and open paper positions quickly convert into resolved evidence.
- Discovery: journal failure record found observations and open paper positions accumulated faster than closed outcomes.

### `FAIL_0007`

- Known constraint: certification, portfolio gates, and evidence quality limit useful candidate flow.
- Hidden assumption: a larger raw signal or candidate count indicates better research progress.
- Discovery: journal failure record found volume ignored governance and validation blockers.

### `FAIL_0008`

- Known constraint: broad support and baseline recoverability do not prove relationship-specific signal quality.
- Hidden assumption: a technical indicator pattern that appears broadly supported will survive relationship-specific review.
- Discovery: journal failure record found factory evidence was blocked, false-positive-prone, or insufficient.

### `FAIL_0009`

- Known constraint: some sleeves had no signals or lacked required data.
- Hidden assumption: active sleeve status implies a sleeve can produce candidate flow.
- Discovery: journal failure record found candidate production concentrated in a subset of sleeves while others were dormant or blocked.

### `FAIL_0010`

- Known constraint: evidence remained underpowered or blocked despite richer artifacts.
- Hidden assumption: enough research documentation implies readiness for capital review.
- Discovery: journal failure record found governance still had no hypotheses ready for capital review.

### `FAIL_0011`

- Known constraint: runtime truth still consumed legacy artifacts.
- Hidden assumption: desired architecture state is enough to retire legacy runtime dependencies.
- Discovery: journal failure record found deprecation readiness depended on runtime dependencies.

### `FAIL_0012`

- Known constraint: missing or immature data cannot be repaired by inventing authority.
- Hidden assumption: workflow expansion can compensate for absent macro readiness evidence.
- Discovery: journal failure record found governance kept macro readiness research-only and blocked fabrication or autonomous sleeve creation.

### `FAIL_0013`

- Known constraint: auditability and understanding maturity are related but not identical.
- Hidden assumption: passing or mostly clean audits means the research conclusions are understood.
- Discovery: journal failure record found daily research integrity still passed with warnings and runtime readiness remained blocked.

### `FAIL_0014`

- Known constraint: non-production can reflect healthy no-candidate states, valid no-signal states, data needs, or conversion bottlenecks.
- Hidden assumption: low output from a sleeve means the sleeve itself is low quality.
- Discovery: journal failure record found reviewed artifacts did not support poor sleeve quality as the primary cause.

### `FAIL_0015`

- Known constraint: non-blocking warning status can still represent persistent candidate-integrity and sleeve-health issues.
- Hidden assumption: a warning that does not block today can be ignored as a cleared or one-day condition.
- Discovery: journal failure record found warnings recurred for three consecutive available days and escalated into failed integrity audit when graph readiness became blocked.

### `FAIL_0016`

- Known constraint: claims lacked relationship-specific support, out-of-fixture generalization, or separation from naive baselines.
- Hidden assumption: standalone indicator evidence can support durable strategy claims without mechanism and regime specificity.
- Discovery: journal failure record found standalone and broad discovery claims remained unsupported, false-positive-prone, baseline-recoverable, or non-generalizing.

### `failure-demo-opening-range`

- Source claim ID: `q-demo-memory-opening-range`
- Known constraint: opening-range evidence was generated-only and not validation.
- Hidden assumption: a repeated opening-range memory can be reused without testing regime compatibility.
- Discovery: Atlas v2 failure memory recorded `REGIME_MISMATCH` with retirement status `ACTIVE_WARNING`.

### `failure-2026-06-04-worker-no-compatible-connected-worker-c5b07de5e574`

- Source claim ID: `q-demo-memory-opening-range`
- Known constraint: research item execution requires a compatible connected worker before useful processing can occur.
- Hidden assumption: a ready backlog item with an artifact can be executed by the available worker set.
- Discovery: Atlas v2 failure registry recorded `NO_COMPATIBLE_CONNECTED_WORKER`.

### `failure-2026-06-05-autonomous_research-artifactstoreerror-64d655a35e43`

- Source claim ID: `q-throughput-smoke-claim-20260605`
- Known constraint: artifact lineage must resolve before priority influence or certification can run.
- Hidden assumption: referenced source artifacts exist and can be loaded from the artifact store.
- Discovery: Atlas v2 failure registry stack trace recorded missing artifact `artifact-demo-opening-range`.

### `failure-2026-06-05-autonomous_research-artifactstoreerror-c54059bb338d`

- Known constraint: selected backlog work cannot proceed when declared input artifacts are absent.
- Hidden assumption: backlog selection implies all input artifacts are available.
- Discovery: Atlas v2 failure registry stack trace recorded missing artifact `artifact-demo-midday-chop`.

### `failure-2026-06-05-autonomous_research-safety-gate-failed-794b177f0415`

- Source claim ID: `ros-grc-0fa5779043aa8a0a-a8953ab172864aa1`
- Known constraint: all required safety gates must pass; lineage and governance pass status do not compensate for certification failure.
- Hidden assumption: passing lineage and governance is enough for autonomous research execution to proceed.
- Discovery: latest Atlas v2 failure report recorded `SAFETY_GATE_FAILED` with `certification_status` `FAIL`, `governance_status` `PASS`, and `lineage_status` `PASS`.

### `failure-2026-06-05-certification-certification-block-0d8cd9475db6`

- Known constraint: reduced blocker count is not equivalent to certification pass.
- Hidden assumption: a smaller blocker count means the certification state is close enough for execution.
- Discovery: Atlas v2 failure registry recorded `CERTIFICATION_BLOCK` with `blocker_count` 3 and `warning_count` 1.

## Non-Goals

- Do not alter original failure records.
- Do not create candidates.
- Do not change replay, qualification, governance, or paper-forward state.
- Do not treat evaluation recall as authority expansion.
