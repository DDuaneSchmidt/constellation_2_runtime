# Atlas Research OS Design Review Summary 001

Date: 2026-06-04
Status: Design only

Scope: summarizes Level 1 design clarifications for evidence levels, information gain, knowledge retirement, candidate impact measurement, and worker interfaces. This review does not implement production code, runtime modules, candidate promotion, sleeve mutation, paper setup, trade advice, broker execution, or capital authorization.

## Runtime Baseline

The required pre-change `npm run aegis:audit` was run before editing. It exited with code 2 at the strict verified-runtime-graph step.

Latest verified graph read:

- path: `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-04/verified_runtime_graph.v1.json`
- `graph_status`: `BLOCKED`
- `active_mode`: `HUMAN_REVIEWED_PAPER_MODE`
- `active_mode_readiness_status`: `BLOCKED`
- runtime truth classification observed during audit: `PARTIAL_CONTEXT`
- highest readiness layer observed during audit: `BLOCKED`
- trade advice, broker execution, live trading, autonomous execution, and real capital allocation were false in the audit outputs reviewed

These documents therefore specify future design only. They must not be read as runtime readiness.

## Design Decisions

- Evidence levels are maturity labels, not authority labels.
- `OPERATOR_APPROVED` means a bounded research disposition was approved. It does not mean capital approved.
- Information gain is an advisory research-priority score, gated by lineage, safety, authority, and runtime truth.
- Retirement is scoped and reversible only through explicit reopening with material new evidence or regime context.
- Quarantine is audit-only and blocks positive consumption until repaired.
- Candidate impact measurement is retrospective and observational. It measures whether learning improves the candidate factory but does not create or promote candidates.
- Worker interfaces are fail-closed, idempotent, lineage-first, and forbidden from candidate, sleeve, trade, broker, or capital mutation.

## Unresolved Questions

- Exact weights for the information gain formula.
- Minimum sample sizes for paper-forward observation and hypothesis survival metrics.
- Whether `OPERATOR_APPROVED` should be split into narrower labels such as `OPERATOR_APPROVED_RESEARCH` and `OPERATOR_APPROVED_PAPER_RESEARCH`.
- Exact stale thresholds by asset class, regime type, and data source.
- How future Atlas memory should represent partially falsified claims across regimes.
- Which future governance artifact owns evidence-level promotion policy.

## Recommended Build Sequence

1. Define schemas for evidence labels, lifecycle states, worker metadata, and lineage bundles.
2. Add read-only validators for evidence-level integrity and forbidden-use fields.
3. Implement memory and retirement checks before any worker execution.
4. Implement information-gain scoring in audit-only mode.
5. Implement worker dry-run interfaces with no artifact mutation beyond research reports.
6. Add candidate impact measurement reports after candidate-factory source lineage is stable.
7. Only then consider orchestrated worker execution, still behind verified graph and runtime truth gates.

## Implementation Risks

- Label laundering from generated or mock evidence into higher maturity labels.
- Treating operator research approval as paper, candidate, or capital authority.
- Overstating causality in candidate impact metrics.
- Duplicate research tasks returning under new wording.
- Stale source data causing false support or false falsification.
- Worker partial writes becoming consumable without lineage validation.
- Future automation bypassing verified runtime graph checks.

## Required Tests For Future Codex Sessions

- Evidence label promotion and demotion tests.
- Generated/mock evidence laundering prevention tests.
- Quarantine blocks consumption tests.
- Retirement and reopening scope tests.
- Duplicate task penalty and retired-knowledge duplicate tests.
- Information-gain formula normalization tests.
- Runtime truth blocked means worker ineligible tests.
- Worker idempotency and duplicate-run tests.
- Worker lineage missing or mismatched means quarantine tests.
- Candidate impact metrics do not create or promote candidates tests.
- Operator-approved research state does not imply capital approval tests.
- Forbidden artifact path and semantic type tests for candidate, sleeve, trade, broker, and capital artifacts.
