# Atlas Research OS Design 001: Evidence Levels

Date: 2026-06-04
Status: Design only

Scope: defines evidence labels for future Atlas Research OS artifacts. This document does not implement workers, runtime modules, candidate promotion, paper setup, sleeve mutation, trade advice, broker execution, or capital authorization.

Runtime posture: the latest verified runtime graph for 2026-06-04 is `BLOCKED`, active mode is `HUMAN_REVIEWED_PAPER_MODE`, and Research OS capabilities are linked to the runtime truth kernel. Consumers must query verified truth and must not infer readiness from this design.

Hard rule: `OPERATOR_APPROVED` does not mean capital approved.

## Principles

- Evidence level labels describe the maturity of a claim, hypothesis, experiment, learning node, or research artifact.
- Evidence labels are not authorization labels.
- Evidence labels must be carried forward without laundering. A generated claim that later appears in a report remains generated unless separately promoted by evidence.
- Candidate generation may consume evidence labels only through future approved interfaces. It must not treat labels as proof of candidate validity.
- Capital authorization is out of scope for Research OS evidence levels. Capital approval requires a separate verified capital authority path.

## Level Definitions

| Level | Definition | Allowed uses | Forbidden uses | Promotion requirements | Demotion or quarantine rules | Candidate generation relationship | Capital authorization relationship |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `GENERATED_ONLY` | A claim, hypothesis, design, or note produced by a model, script, operator prompt, or research ideation process without empirical support. | Backlog seeding, duplicate detection, cheap experiment design, hypothesis clustering, novelty review. | Treating as fact, using as validation evidence, ranking as proven edge, promoting to candidate, allocating capital. | Promote only after source lineage is recorded, duplicate checks pass, and either mock, historical, or external evidence is attached. | Demote or quarantine if source prompt is missing, lineage is unverifiable, generated text asserts unsupported truth, or duplicate laundering is detected. | May inspire candidate research only; cannot create or promote a candidate. | No capital authority. |
| `MOCK_ONLY` | Evidence produced from fixtures, synthetic data, mocked services, deterministic examples, or non-market test harnesses. | Contract testing, schema validation, workflow rehearsal, failure-mode discovery, interface verification. | Claiming market validity, estimating edge, promoting candidates, proving live readiness, capital allocation. | Promote only by replacing mock dependency with historical replay, paper-forward observation, or external validation and preserving mock lineage. | Demote/quarantine if mock data is confused with real data, fixture scope is omitted, or outputs enter production truth as observed evidence. | May validate candidate-factory plumbing only; cannot validate candidate quality. | No capital authority. |
| `HISTORICAL_REPLAY` | Evidence from replaying historical data with declared source, time bounds, regime labels, lookahead controls, and reproducible measurement. | Testing mechanism plausibility, falsifying weak ideas, estimating historical failure patterns, improving experiment design. | Claiming forward performance, bypassing paper observation, treating backtest as external validation, capital authorization. | Promote to paper-forward only after replay passes lineage, leakage, duplicate, and regime-coverage checks and a forward observation plan is approved. | Demote/quarantine for data leakage, stale or missing source, unbounded parameter search, regime mislabeling, or unverifiable replay. | May inform candidate-quality scoring in future measurement, but cannot directly promote. | No capital authority. |
| `PAPER_FORWARD_OBSERVATION` | Evidence from forward paper observation or shadow operation with real-time or time-ordered inputs, no broker execution, and no real capital. | Measuring operational feasibility, observing candidate-factory behavior, survival tracking, failure attribution, evidence maturity improvement. | Claiming live trading readiness, broker execution, real capital use, or final validation without closure and review. | Promote only after observation window closes, marks are certified, outcomes are validated, failure patterns are reviewed, and lineage is complete. | Demote/quarantine when paper position lineage breaks, mark data is missing, close conditions are unmet, or paper artifacts are confused with real trades. | May be a measured input to future candidate impact analytics; cannot by itself promote to real trading. | No capital authority. |
| `EXTERNALLY_VALIDATED` | Evidence corroborated by an independent source, dataset, benchmark, external publication, third-party audit, or independently reproduced result with lineage. | Cross-checking internal learning, reducing model/system bias, supporting stronger hypothesis confidence, informing research prioritization. | Treating external evidence as runtime readiness, bypassing internal verification, importing unvetted claims into candidate/capital paths. | Promote to operator review only after source reliability, reproducibility, scope match, and conflict checks pass. | Demote/quarantine if the external source is stale, retracted, unverifiable, mismatched to the tested regime, or contradicted by stronger internal evidence. | May strengthen research inputs to candidate evaluation if a future approved interface exists. | No capital authority. |
| `OPERATOR_APPROVED` | A human operator has approved a bounded research disposition, such as continuing, retiring, reopening, escalating for paper research, or accepting a learning record. | Research lifecycle transitions, backlog reprioritization, learning acceptance, reopening retired knowledge, operator-reviewed paper research routing. | Capital approval, live trade advice, broker execution, sleeve mutation, bypassing runtime truth, declaring candidate promotion complete. | Promotion beyond operator-approved research requires separate verified systems, explicit policy authority, and evidence required by the target domain. | Demote/quarantine if approval scope is missing, approval is used outside its stated scope, lineage is incomplete, or the approval is contradicted by runtime truth. | May authorize research handling of a candidate-related question; does not create, promote, or approve a candidate for capital. | Explicitly no capital authority. |

## Promotion Discipline

Promotion is monotonic only when evidence quality genuinely improves. A higher label must include:

- source artifact ids and hashes
- parent evidence levels
- reviewer or worker identity
- scope and regime covered
- uncertainty carried forward
- known contradictions
- explicit allowed and forbidden uses

Promotion cannot occur by renaming, summarizing, or copying evidence into a higher-status artifact.

## Demotion And Quarantine

Demotion applies when evidence becomes weaker but remains usable with a lower label. Quarantine applies when evidence cannot be safely consumed.

Quarantine triggers:

- missing lineage
- fabricated or unverifiable source
- label mismatch
- mock or generated evidence presented as observed evidence
- stale runtime dependency
- source retraction or material contradiction
- candidate, sleeve, trade, or capital leakage risk

Quarantined evidence is read-only for audit and cannot support candidate generation, promotion, capital authorization, or positive research scoring until reopened by an explicit repair process.
