# Research To Lite Architecture ADR

## Status

Accepted as architecture clarification.

This ADR documents the intended long-term relationship between the Research layer and Aegis Lite. It does not introduce new schemas, services, runtime hooks, agents, execution paths, promotion authority, or orchestration.

## Purpose

Research is an offline advisory layer built on top of the existing Aegis/OCP replay, evidence, taxonomy, and governance foundations.

Research may capture ideas, formalize hypotheses, define deterministic experiments, validate or reject evidence, and prepare review material for humans. Aegis Lite remains the deterministic operating layer for supervised manual paper-trading decisions. Research artifacts are work records and evidence only; they are not runtime instructions.

The purpose of this document is to clarify the lifecycle and source-of-truth boundaries among overlapping research concepts already present in the repository:

- `research_hypothesis.v1`
- `hypothesis_registry.v1` legacy compatibility
- `ai_hypothesis_batch.v1` / AI hypothesis intake
- `edge_hypothesis.v1` idea buckets
- `research_proposal_ledger.v1`
- `research_lab_index.v1`
- `research_evidence_packet.v1`
- `ai_backtest_experiment_spec.v1`
- `ai_experiment_review_packet.v1`
- `edge_taxonomy.v1`

## Non-Goals

This ADR explicitly rejects:

- Autonomous trading.
- Runtime hooks from Research into Aegis Lite.
- Live execution.
- Paper execution.
- Broker submit.
- Transmit automation.
- Fill lifecycle automation.
- AI promotion authority.
- AI risk sizing authority.
- AI quarantine authority.
- AI enforcement authority.
- Automatic allocation changes.
- Orchestration sprawl.
- Hidden conversational state as an architecture input.
- Agent teams or multi-agent operational workflows.
- Any design where a research artifact mutates runtime behavior.

## Canonical Lifecycle

The intended lifecycle is:

1. `research_inbox_item.v1`
2. `research_hypothesis.v1`
3. `research_program.v1`
4. `research_task_queue.v1`
5. deterministic offline executor
6. `research_evidence_packet.v1`
7. `research_result_ledger.v1`
8. `research_conclusion.v1`
9. `research_failure_archetype.v1`
10. `research_to_lite_promotion.v1`
11. `promoted_sleeve_library.v1`
12. Aegis Lite feedback back into Research follow-up

The lifecycle is deliberately conservative. Research conclusions and failure archetypes are durable memory only. They do not mean the idea is approved for Aegis Lite, manual trading, paper trading, or automation.

Operational promotion into Aegis Lite is a separate governance boundary. It requires human approval, implementation work, tests, release activation, and inclusion in the promoted sleeve library. It is not part of this research lifecycle.

## Artifact Role Map

### `research_proposal_ledger.v1`

Role: lightweight idea/proposal capture.

This artifact is appropriate for early strategy proposals that are not yet formal hypotheses. It can hold a proposed thesis, signal idea, expected market behavior, required data, risks, creator, timestamp, and proposal-only status.

It should remain advisory and non-operational. Its authority is proposal capture, not validation or promotion.

### `edge_hypothesis.v1` Idea Buckets

Role: AI-originated or imported idea storage by status bucket.

The `ideas/proposed`, `ideas/validated`, `ideas/rejected`, and `ideas/archived` buckets are useful for durable AI research intake and retention. They preserve accepted and rejected ideas with fields such as hypothesis text, expected mechanism, instruments, data requirements, test design, success criteria, rejection criteria, and risk notes.

These buckets should not be treated as Aegis Lite inputs. `validated` means valid as research intake, not valid for trading.

### `ai_hypothesis_batch.v1`

Role: structured AI hypothesis intake format.

This is an input contract for AI-generated hypotheses. It exists to make AI output explicit, reviewable, rejectable, and testable. It is not a runtime contract and does not grant authority to create orders, alter gates, change sizing, or promote sleeves.

### `research_hypothesis.v1`

Role: authoritative hypothesis object for new Research Lab work.

Once an idea has been triaged into a clear research hypothesis, this artifact is the source of truth for the formal hypothesis. It owns the hypothesis identifier, title, summary, market thesis, edge family, behavioral state, expected regime, expected direction, holding period, instruments, rationale, expected behavior, failure conditions, invalidation conditions, related sleeves, related research refs, confidence, status, source, and notes.

It is the right boundary between loose idea capture and governed research work.

### `hypothesis_registry.v1`

Role: legacy compatibility only.

Existing registry/test-plan/experiment-result paths may be read or one-way adapted into `research_hypothesis.v1`, but they are not the source of truth for new Research Lab work.

### `edge_taxonomy.v1`

Role: taxonomy and naming control.

This artifact controls vocabulary for market theses, edge families, edge clusters, trade expressions, deprecated terms, naming rules, and duplicate-term warnings.

It does not capture ideas and does not validate evidence. Its purpose is to reduce concept drift and prevent duplicated naming from becoming accidental architecture.

### `ai_backtest_experiment_spec.v1`

Role: experiment planning.

This artifact describes a proposed deterministic experiment for a formal hypothesis or idea. It should define inputs, universe, windows, costs, slippage assumptions, walk-forward or out-of-sample requirements, metrics, rejection criteria, and reproducibility expectations.

It is a plan only. It must not trigger backtest execution without a separate explicit human action.

### `research_evidence_packet.v1`

Role: evidence summary.

This artifact summarizes completed deterministic research evidence: source data, replay window, instruments, regimes, methodology, metrics, expectancy, drawdown, MAE/MFE, failure modes, limitations, reproducibility notes, and lineage.

It is evidence, not promotion. It may inform a review packet or human decision, but it cannot directly authorize Aegis Lite behavior.

### `ai_experiment_review_packet.v1`

Role: review packet.

This artifact packages experiment intent, deterministic validation results, evidence, limitations, risks, known failure modes, and reviewer-facing questions. It is the primary object a human should inspect before deciding whether to archive, reject, revise, or continue offline research.

It is not an execution packet and not a promotion packet.

### `research_lab_index.v1`

Role: index and navigation.

This artifact helps humans find research items, current statuses, latest evidence, related sleeves, related edge families, archived state, and operator notes.

It is not authoritative for the underlying hypothesis, experiment, evidence, or decision content. It indexes those artifacts.

## Source Of Truth By Stage

| Lifecycle stage | Authoritative artifact |
| --- | --- |
| Raw idea | `research_proposal_ledger.v1` or `edge_hypothesis.v1` in `ideas/proposed` |
| Triaged idea | `edge_hypothesis.v1` bucket status or proposal ledger status |
| Formal hypothesis | `research_hypothesis.v1` |
| Experiment spec | `ai_backtest_experiment_spec.v1` |
| Deterministic validation | Deterministic replay/backtest output referenced by the evidence packet |
| Evidence | `research_evidence_packet.v1` |
| Review packet | `ai_experiment_review_packet.v1` |
| Navigation | `research_lab_index.v1` |
| Taxonomy | `edge_taxonomy.v1` |
| Human decision | Explicit human review record or review packet decision field, when present |

If two artifacts contain overlapping status fields, the stage-specific source of truth above wins. Index status is navigational. AI intake status is intake status. `research_hypothesis.v1` status is formal research status for new work. Legacy registry lifecycle state is compatibility metadata only. Evidence packet status is evidence status.

## Allowed Transitions

Allowed transitions are one-way and advisory:

- Raw idea may become triaged idea.
- Triaged idea may become formal hypothesis.
- Formal hypothesis may receive an experiment spec.
- Experiment spec may be selected by a human for deterministic validation.
- Deterministic validation may produce evidence.
- Evidence may be summarized in a review packet.
- Review packet may receive a human decision.
- Human decision may archive, reject, or accept the item for continued offline research only.
- Taxonomy review may annotate, cluster, rename, or deprecate terms.
- Index updates may point to the current artifact locations and statuses.

Each transition should preserve lineage back to its source artifact. No transition should erase rejected ideas or failed evidence.

## Forbidden Transitions

The following transitions are forbidden:

- Research artifact -> trade.
- Review packet -> execution.
- Evidence packet -> promotion.
- AI output -> runtime mutation.
- Idea -> order.
- Hypothesis -> allocation.
- Experiment spec -> backtest execution without separate human action.
- Experiment spec -> Aegis Lite candidate.
- Edge hypothesis -> Aegis Lite queue.
- Research Lab status -> broker submit.
- Research Lab status -> transmit automation.
- Research Lab artifact -> risk sizing change.
- Research Lab artifact -> quarantine/enforcement action.
- Research Lab artifact -> promoted sleeve library mutation.

## Weaknesses And Current Design Risks

The current design has useful pieces, but the boundaries are easy to confuse:

- Multiple artifacts can look like idea capture: `research_proposal_ledger.v1`, `edge_hypothesis.v1`, AI hypothesis intake, and `hypothesis_registry.v1`.
- Some artifacts include overlapping status fields with different meanings.
- `validated` can be misread as operational validation when it may only mean schema or intake validation.
- `research_lab_index.v1` can look authoritative even though it is an index.
- Evidence packets can be mistaken for promotion packets.
- Review packets can be mistaken for execution packets.
- AI experiment specs can be mistaken for runnable commands.
- Adding orchestration too early would create an unnecessary control plane around artifacts that should remain offline and human-directed.
- A parallel `research_idea_register.v1` could duplicate existing proposal and edge-hypothesis storage unless tightly scoped.

## Recommended Simplification

Do not add a broad parallel `research_idea_register.v1` yet.

Preserve the existing artifacts and clarify their lifecycle roles:

- Use `research_inbox_item.v1` for raw idea/proposal capture.
- Treat `research_proposal_ledger.v1` and `edge_hypothesis.v1` as legacy/advisory inputs that may be one-way adapted or referenced.
- Use `research_hypothesis.v1` once an idea is formal enough to become a governed hypothesis.
- Use `research_program.v1` to organize related hypotheses.
- Use `hypothesis_registry.v1` only as legacy compatibility or one-way adapter input.
- Use `ai_backtest_experiment_spec.v1` only for experiment planning.
- Use deterministic validation output and `research_evidence_packet.v1` for evidence.
- Use `research_result_ledger.v1` for immutable task outcomes.
- Use `research_conclusion.v1` for immutable conclusions and supersession.
- Use `research_failure_archetype.v1` for advisory recurring failure memory.
- Use `ai_experiment_review_packet.v1` for human review.
- Use `research_lab_index.v1` only for navigation.
- Use `edge_taxonomy.v1` only for vocabulary and clustering control.

`research_inbox_item.v1` is now the minimal pre-hypothesis lifecycle root. Do not add another idea register unless real usage proves this artifact is insufficient.

## Stop Rule

No new research schemas, components, daemons, agents, queues, or orchestration should be added until real usage exposes a concrete gap that cannot be handled by the completed loop above.

When a gap appears, the first response should be documentation and field clarification. New implementation should be the last response, not the first.

## Safety Boundary

Research may produce artifacts and evidence only.

Aegis Lite runtime remains deterministic, isolated, and governed by its own operational spine. Research artifacts do not become Lite candidates unless a separate human-approved implementation and promotion process adds an eligible sleeve to the promoted sleeve library.

Humans decide any operational action. AI may suggest, summarize, test offline, and prepare review material. AI does not promote, size, enforce, quarantine, submit, transmit, or execute.
