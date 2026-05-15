# Aegis Research Lab

Aegis Research Lab is the offline, exploratory layer for hypothesis discovery. It is separate from Aegis Lite.

## Purpose

Research Lab exists to explore sleeves, edge ideas, regime signals, stop logic, sizing ideas, overlap behavior, governance ideas, and behavioral state hypotheses before they are eligible for operational use.

## Separation From Runtime

Research Lab artifacts are not runtime inputs. They do not authorize trading, broker submit, transmit automation, fill lifecycle processing, or Aegis Lite behavior changes.

Aegis Lite may only consume ideas that have crossed the governed promotion boundary. Research may discover ideas. Aegis Lite may only consume validated and promoted ideas.

For new Research Lab work, `research_hypothesis.v1` is the canonical hypothesis object. `hypothesis_registry.v1`, `hypothesis_test_plan.v1`, and `experiment_result.v1` are legacy compatibility paths and should not be used as the source of truth unless they are one-way adapted into `research_hypothesis.v1`.

## Allowed Activities

- Offline replay and simulation.
- Exploratory analysis.
- Hypothesis comparison.
- Regime and edge discovery.
- Stop/risk research.
- Governance compatibility review.
- Manual execution clarity review.
- Failure-mode documentation.

## Prohibited Activities

- Broker submit.
- Transmit automation.
- Fill lifecycle automation.
- Live market-session execution requirements.
- Automatic runtime mutation.
- Treating experimental artifacts as operational evidence.

## Failure Tolerance

Research Lab is allowed to fail, reject ideas, archive ideas, and keep drafts. Failure is evidence. Rejected and draft research cannot cross into Aegis Lite implementation.

## Research Outputs

Canonical Research Lab outputs include:

- `research_inbox_item.v1`
- `research_hypothesis.v1`
- `research_program.v1`
- `research_result_ledger.v1`
- `research_conclusion.v1`
- `research_failure_archetype.v1`
- `research_knowledge_graph.v1`
- `research_evidence_packet.v1`
- `research_to_lite_promotion.v1`
- `research_lab_index.v1`
- `edge_taxonomy.v1`
- `hypothesis_registry.v1`
- `hypothesis_test_plan.v1`
- `research_task_queue.v1`
- `research_experiment_result.v1`
- `research_lab_awareness_report.v1`
- `hypothesis_progress_report.v1`

The stronger Research Lab hypothesis architecture is:

`research_inbox_item.v1` -> `research_hypothesis.v1` -> `research_program.v1` -> `research_task_queue.v1` -> `research_evidence_packet.v1` -> `research_result_ledger.v1` -> `research_conclusion.v1` -> `research_failure_archetype.v1` -> `research_knowledge_graph.v1` -> `research_to_lite_promotion.v1` -> `promoted_sleeve_library.v1` -> Aegis Lite feedback -> Research follow-up.

The completed loop captures raw ideas, formalizes hypotheses, organizes them into programs, executes deterministic offline tasks, preserves evidence/results/conclusions/failure memory, and imports Lite feedback as research learning. The knowledge graph remains a generated, non-authoritative index only.

Every research action must come from an explicit trigger: manual hypothesis registration, sleeve failure review, EOD anomaly review, stale promising hypothesis retest, promotion candidate review, or duplicate-cluster review.

Research Lab does not stop at one exploratory test. Each hypothesis must progress through the governed test plan before it can be rejected, validated, promoted, or retired.

Inbox items do not execute. Programs do not authorize. Hypotheses do not execute themselves. `research_task_queue.v1` controls offline work. `research_result_ledger.v1` preserves every outcome, including failed, contradicted, and invalidated hypotheses. `research_conclusion.v1` is immutable; changes are represented by supersession.

## Closed-Loop Safety Boundary

Research Lab can register hypotheses, detect overlap, enqueue tasks, run offline tests, write experiment results, recommend follow-up work, and recommend promotion review. It can also learn from outcome-ledger rows by creating new offline research tasks.

Research Lab cannot create trades, submit orders, enable transmit, manage fills, mutate Aegis Lite runtime behavior, or make exploratory artifacts operational. Research artifacts are evidence and work records only.

Research Lab commands require an explicit `--truth_root`; there is no implicit production/runtime root. The writers place artifacts only under that root's `research_lab/` subtree.

For new work, a hypothesis advances through `research_task_queue.v1`, `research_evidence_packet.v1`, `research_result_ledger.v1`, and `apply_research_result_to_hypothesis_v1`. Legacy `hypothesis_test_plan.v1` paths remain compatibility-only unless adapted into the canonical model.

Promotion requires a compatible `research_hypothesis.v1`, evidence packet refs, result ledger refs, positive friction-adjusted evidence, out-of-sample support where applicable, regime notes, failure-mode notes, edge-overlap review, implementation-readiness notes, lineage fields, reason codes, reproducibility notes, and human/operator approval. A single promising experiment is not enough.

Lifecycle transitions are fail-closed. `IDEA` cannot become `VALIDATED_RESEARCH` without evidence; `PROMOTION_CANDIDATE` requires result ledger support; `APPROVED_FOR_LITE` requires a separate promotion artifact and human approval. Rejected, archived, or invalidated hypotheses cannot promote.

No research artifact can directly create trades because Aegis Lite only evaluates sleeves from `promoted_sleeve_library.v1`. `manual_trade_packet.v1` candidates are actionable only when their sleeve and source hypothesis match that promoted library and complete entry, stop, risk, sizing, symbol, side, and instrument fields are present.
