# Atlas Research Operating System Roadmap 001

Date: 2026-06-05
Status: DESIGN_ONLY
Mode: evaluation-only, offline-only

## Purpose

Define the next Atlas generation as a staged Research Operating System.

The roadmap sequences the research stack from artifact creation through critique, failure reuse, prioritization, debt control, research allocation, and meta-research. The goal is not more autonomous authority. The goal is better evidence production, faster uncertainty reduction, less repeated research waste, and clearer separation between generated research artifacts and verified runtime truth.

This document does not implement Atlas behavior, change Aegis behavior, modify manifests, alter verified runtime truth, create candidates, run replay, change qualification, change governance, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.

## Roadmap Summary

| Phase | Capability | Current status | Roadmap posture |
| ---: | --- | --- | --- |
| 1 | Observation / Claim / Hypothesis | Partially operating; evidence volume exceeds resolved validation | Stabilize intake, lineage, and validation routing |
| 2 | Research Adversary | Designed and generated-only; useful in retrospective review | Measure human usefulness and false positives |
| 3 | Failure Taxonomy | Strong evaluation lift; still generated-only | Formalize controlled category use and ambiguity rules |
| 4 | Research Director | Evaluation-only ranking passed target bottleneck order | Keep offline until policy and measurement gates mature |
| 5 | Research Debt | Inventory and dashboard exist; debt is material | Make debt reduction a first-class research objective |
| 6 | Research Allocation | Future design only | Allocate research effort, not capital |
| 7 | Meta-Research | Future design only | Measure whether the research process itself improves |

## Phase 1: Observation / Claim / Hypothesis

Purpose:

Convert raw observations into structured claims and hypotheses that can be reviewed, challenged, attributed, and validated. This phase is the intake layer for the Research OS.

Status:

- Partially operating.
- Observation, claim, and hypothesis artifacts exist.
- Claim-to-hypothesis flow can create READY validation backlog items.
- The phase currently creates more research surface than the downstream validation system can resolve.

Evidence:

- Claim/hypothesis funnel artifacts show hypothesis-validation backlog creation from claim artifacts.
- Research debt inventory reports unresolved hypotheses and a large generated candidate surface.
- Atlas usage review found that retrieval and synthesis can reconnect observations, knowledge, failures, and reports around recurring topics.
- Direct validation and replay reports show that downstream validation is still the limiting layer, not idea generation.

Remaining gaps:

- Intake can outrun validation throughput.
- Generated hypotheses require stronger dedupe, lineage, and validation routing.
- Observation clusters can be over-merged, hiding symbol, timeframe, and source-type differences.
- Hypothesis volume should not be treated as research progress unless validation, rejection, or debt reduction follows.
- Phase 1 needs explicit stop rules for weak, duplicate, proxy-only, or unactionable hypotheses.

## Phase 2: Research Adversary

Purpose:

Challenge claims and hypotheses before they consume validation capacity. The adversary extracts assumptions, constraints, competing explanations, null explanations, and falsification tests.

Status:

- Designed and generated-only.
- Retrospective review indicates high process value for surfacing risks earlier.
- Evaluation framework and metrics exist, but human-scored usefulness remains incomplete.

Evidence:

- Research Adversary design documents define bounded generated-only critique.
- Candidate retrospective estimated HIGH usefulness for many proxy-dependent, regime-weak, or under-specified candidate families.
- Adversary evaluation materials define recall, assumption, constraint, falsification quality, reviewer usefulness, hallucinated evidence, duplication, and authority-compliance metrics.
- Research debt inventory reports a large unresolved adversary corpus: many generated assumptions, constraints, and falsification tests without enough human evaluation.

Remaining gaps:

- Generated critiques must be scored by humans on a bounded sample.
- False-positive and review-overhead rates need measurement.
- Useful adversary findings need a path into review memory without automatic authority or automatic state changes.
- Falsification tests need quality control so they become runnable validation designs rather than generic warnings.
- Adversary output must remain advisory and cannot veto, approve, qualify, promote, or trade.

## Phase 3: Failure Taxonomy

Purpose:

Turn repeated failures into reusable negative knowledge. The taxonomy should help Atlas recognize known failure patterns earlier, improve adversary recall, and reduce repeated investigation of already-understood traps.

Status:

- Strong evaluation signal.
- Generated-only taxonomy integration is useful but not production-authoritative.
- Expansion needs controlled precedence, tie-breaking, confidence, and UNKNOWN rules.

Evidence:

- Taxonomy recall evaluation improved failure recall from 23.7% to 86.8%.
- Assumption recall improved from 39.1% to 81.2%.
- Constraint recall improved from 13.2% to 86.8%.
- The frozen evaluation reported zero false positives and zero authority violations.
- Taxonomy expansion plan identifies 5 ambiguous cases and 6 multi-category cases needing reviewer confirmation.

Remaining gaps:

- Ambiguous taxonomy cases need precedence and split rules before broader use.
- Secondary categories must remain reviewer hints, not decisions.
- Failure memory encoding remains incomplete relative to the number of known historical failures.
- Taxonomy labels must not imply validation status, candidate rejection, governance status, or readiness.
- The taxonomy needs measured impact on future research hours saved, not only retrospective recall.

## Phase 4: Research Director

Purpose:

Rank research work by information gain, bottleneck removal, evidence strength, uncertainty reduction, research cost, and debt reduction. The Research Director should choose what research work deserves attention next.

Status:

- Evaluation-only.
- A generated Research Director ranking test passed the target bottleneck order.
- Retrospective evidence suggests it would have prioritized high-leverage infrastructure earlier.

Evidence:

- Research Director evaluation ranked `Data Coverage`, `Replay Attrition`, and `Vocabulary Mismatch` as the top three workstreams.
- That ranking matched the observed bottleneck sequence after direct data coverage improved.
- Research Director retrospective judged Failure Taxonomy and Data Coverage as very high priority, with Candidate Attribution and Research Adversary also high priority.
- Scoring models and evaluation framework documents exist for priority, uncertainty, and research debt ranking.

Remaining gaps:

- Current output is generated-only and offline.
- Ranking needs repeated out-of-sample tests against future bottleneck outcomes.
- The Director must not route around verified truth, human review, replay gates, or governance.
- It needs explicit cost models and stop conditions before directing real research effort.
- It should prioritize validation unblockers over more hypothesis generation while validation remains constrained.

## Phase 5: Research Debt

Purpose:

Make unresolved research obligations visible and actionable. Research debt includes unvalidated candidates, missing data, proxy dependency, unresolved hypotheses, duplicated clusters, stale observations, unscored adversary findings, and uncoded failure knowledge.

Status:

- Inventory and dashboard exist.
- Debt is material and currently constrains Atlas learning rate.
- Research debt is not yet a first-class operating gate.

Evidence:

- Research debt inventory identified CRITICAL debt in unvalidated candidates, missing market data, and proxy-dependent ranked candidate surfaces.
- The same inventory identified HIGH debt in unresolved hypotheses, duplicate clusters, replay coverage gaps, qualification gaps, stale observations, unscored adversary findings, and failure patterns not yet encoded.
- Research Director evaluation ranked Data Coverage, Replay Attrition, and Vocabulary Mismatch as top debt-reduction workstreams.
- Direct validation and counterfactual reports show that vocabulary and replay-state debt can materially change validation throughput.

Remaining gaps:

- Debt needs ownership, aging, severity updates, and closure evidence.
- Research debt should connect to allocation decisions and stop conditions.
- Debt reduction should be measured by validation unblock, hours saved, uncertainty reduction, and avoided duplicate work.
- Some debt is generated by Atlas itself; the OS needs controls to avoid producing more artifacts than it can resolve.
- Research debt must not be hidden behind generic `INSUFFICIENT_DATA` labels when a more precise blocker exists.

## Phase 6: Research Allocation

Purpose:

Allocate scarce research effort, review bandwidth, compute time, and validation priority. This is research allocation only, not capital allocation.

Status:

- Future design only.
- Not implemented.
- No authority expansion.

Evidence:

- Research Allocation Future Design defines allocation units such as mechanism family, hypothesis cluster, replay task, uncertainty repair, retirement review, failure-pattern review, and search-space compression task.
- Research economics design defines research cost, research yield, research ROI, and research hours saved as measurement concepts.
- Research debt and Research Director evaluations show why effort should be directed toward bottleneck removal and debt reduction before additional generation.

Remaining gaps:

- Allocation scores are not yet measured against realized research outcomes.
- Research cost and research yield need empirical calibration.
- Stop conditions need to be attached to allocated work before work begins.
- Allocation decisions need human review, explicit budgets, and evidence-linked rationale.
- Research allocation must never become capital allocation, trade advice, paper-placement authority, or production promotion.

## Phase 7: Meta-Research

Purpose:

Study the research process itself. Meta-Research evaluates whether Atlas is improving at producing reliable knowledge, reducing false positives, retiring weak ideas, compressing search space, and spending research effort efficiently.

Status:

- Future design only.
- Not implemented.
- Dependent on earlier phases producing enough decision history and outcome data.

Evidence:

- Meta-Research Director Future Design defines the role as measuring process quality, cost, failure modes, learning rate, gate quality, false positive sources, false negative risks, and research labor waste.
- Research economics design supplies candidate metrics such as information gain per research hour, research hours saved, and research ROI.
- Existing roadmap reconciliation warns that more architecture is not the repair while outcome maturity and evidence accumulation remain binding constraints.

Remaining gaps:

- Meta-research needs a larger history of research decisions, allocations, validations, failures, and retirements.
- Process metrics must distinguish better research from weaker gates.
- False-positive elimination must be balanced against missed-useful-idea risk.
- Policy recommendations need governance review before any implementation.
- Meta-Research should not become Chief Scientist authority or autonomous strategic control.

## Sequencing Principles

1. Stabilize evidence intake before expanding generation.
2. Challenge claims before spending validation capacity.
3. Reuse known failures before inventing new review language.
4. Direct research toward bottlenecks before adding search space.
5. Reduce research debt before increasing research volume.
6. Allocate research effort only with explicit cost, expected information gain, and stop conditions.
7. Evaluate the research process only after enough decisions and outcomes exist to measure.

## Next Generation Definition

The next Atlas generation should be considered successful only if it improves research throughput without weakening authority boundaries.

Primary success measures:

- fewer repeated failure patterns
- higher failure recall before validation spend
- faster identification of data, replay, vocabulary, intraday, and event blockers
- lower unresolved research debt
- more explicit rejections and retirements
- higher information gain per research hour
- cleaner separation between generated research artifacts and verified runtime truth

Non-goals:

- no autonomous trading authority
- no capital allocation
- no broker execution
- no position sizing
- no automatic paper placement
- no candidate promotion
- no replay, qualification, governance, or runtime-truth override

## Recommended Roadmap Posture

Near-term:

- harden Phase 1 lineage and validation routing
- measure Phase 2 adversary usefulness on bounded human-scored samples
- refine Phase 3 taxonomy ambiguity and precedence rules
- use Phase 4 Director only as offline prioritization evidence
- make Phase 5 research debt visible in every major review

Mid-term:

- connect debt reduction to Director scoring
- measure research hours saved from taxonomy and adversary use
- require stop conditions on major research tasks
- start calibration for research cost and yield

Long-term:

- introduce Phase 6 research allocation only after cost/yield measures are credible
- introduce Phase 7 meta-research only after Atlas has enough decision history to evaluate process changes

## Authority Boundary

This roadmap is design only. It does not implement new Atlas behavior, modify Aegis behavior, modify manifests, alter verified runtime truth, promote candidates, override replay, override qualification, override governance, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.

Any future implementation must follow Aegis runtime truth rules, update affected manifests and tests, and pass audit before being treated as runtime behavior.
