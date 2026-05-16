# Aegis ChatGPT Control Plane Audit

Date: 2026-05-15

Scope: `npm run aegis:chatgpt:packet` and `npm run aegis:chatgpt:show`.

This is an audit only. No runtime artifacts were generated and no production or broker paths were activated.

## Executive Finding

The current `aegis:chatgpt:packet` / `aegis:chatgpt:show` commands do not fully represent the post-pivot Aegis Lite ecosystem.

They still provide useful closed-world source-integrity framing, git/source currentness, runtime-root evidence, legacy paper-readiness diagnostics, and a Markdown packet wrapper. However, the packet is still centered on pre-pivot PAPER readiness, submit-boundary, broker supply, day-run ledger, latest broker attempt, and legacy authority surfaces.

The post-pivot operator state is only partially represented, and important Aegis Lite / Research Lab / Operator Inbox / Event Monitoring / Sleeve Performance / AI Feedback state is either missing, indirect, or buried behind older readiness vocabulary.

## Command Wiring

Current npm wiring:

- `aegis:chatgpt:packet` runs `python3 ops/tools/aegis_chatgpt_packet.py` ([package.json](/home/node/constellation/package.json:21)).
- `aegis:chatgpt:show` runs `python3 ops/tools/aegis_chatgpt_show.py` ([package.json](/home/node/constellation/package.json:22)).
- A newer control-packet command pair also exists, but it is separate from the audited legacy commands ([package.json](/home/node/constellation/package.json:23)).

## What The Existing Commands Expose

`aegis:chatgpt:packet` writes a Markdown packet to the legacy runtime export path and archive path ([ops/tools/aegis_chatgpt_packet.py](/home/node/constellation/ops/tools/aegis_chatgpt_packet.py:3104)).

It exposes:

- repo path, runtime mode, runtime truth root, production promoted commit, git branch/commit/dirty state, source reproducibility, and canonical repo protection ([ops/tools/aegis_chatgpt_packet.py](/home/node/constellation/ops/tools/aegis_chatgpt_packet.py:3030)).
- closed-world handoff rules telling ChatGPT to ignore prior memory and use only packet evidence ([ops/tools/aegis_chatgpt_packet.py](/home/node/constellation/ops/tools/aegis_chatgpt_packet.py:3059)).
- current functionality and active component sections, then operating contract, paper status, latest attempt, file changes, and evidence sections ([ops/tools/aegis_chatgpt_packet.py](/home/node/constellation/ops/tools/aegis_chatgpt_packet.py:3083)).
- current calendar day runtime status, latest trading day evidence status, day-run ledger, requirement graph, market data supply, broker supply, capital supply, risk budget supply, authorization supply, unified truth kernel, and Aegis paper-trading status ([ops/tools/aegis_chatgpt_packet.py](/home/node/constellation/ops/tools/aegis_chatgpt_packet.py:2429)).
- latest paper-trade attempt using broker submission records, order IDs, perm IDs, fill ledger, execution stream records, and submit mode ([ops/tools/aegis_chatgpt_packet.py](/home/node/constellation/ops/tools/aegis_chatgpt_packet.py:2688)).

`aegis:chatgpt:show` only selects and prints the latest or freshest archived Markdown packet. It does not build a new read model or reconcile post-pivot state ([ops/tools/aegis_chatgpt_show.py](/home/node/constellation/ops/tools/aegis_chatgpt_show.py:15)).

## Coverage Matrix

| Area | Current legacy command coverage | Assessment |
| --- | --- | --- |
| Aegis Lite | Indirect. It may mention UI/control-plane and paper status, but does not present Lite as canonical manual execution spine. | Partial |
| Event Monitoring | Not a first-class section. Event rules, monitor status, ledger, tactical packets, validity gate, and alert gate are not exposed as canonical state. | Missing |
| Research Lab | Not first-class. Legacy packet is oriented toward runtime readiness, not hypotheses, task queue, evidence/result ledger, promotions, or dataset gaps. | Missing/partial |
| Operator Inbox | Not represented. | Missing |
| Sleeve Performance | Some performance-intelligence section exists, but not the post-pivot sleeve performance lifecycle status with missing receipts/outcomes and research feedback. | Partial |
| AI Feedback Engine | Not represented as Evidence Gate / AI Feedback / Research Task Gate state. | Missing |
| Dataset gaps | Legacy market-data supply appears, but Research dataset bindings for price, volatility, breadth, macro calendar, regimes, and sleeve outcomes are not presented as current Research blockers. | Partial |
| Operator status | Legacy operator next actions exist, but no single post-pivot control-plane summary of last EOD, current packets, missing receipts/outcomes, event status, open tasks, and next action. | Partial |
| Runtime truth classification | Runtime mode/root is exposed, but fixed `REAL_RUNTIME` / `DEMO_ONLY` / `DRY_RUN_ONLY` / `PARTIAL_CONTEXT` / `ADVISORY_ONLY` / `BLOCKED` classification is not native to the legacy Markdown packet. | Missing |
| Readiness state | Paper-trading readiness is exposed, but readiness is framed around old PAPER/day-run/submit surfaces rather than Lite manual-paper readiness. | Obsolete framing |

## Obsolete Assumptions

The legacy packet still assumes or emphasizes:

- `paper_session_authority`, `paper_trading_day_authority`, `submit_boundary_status`, and `can_submit_paper_orders` as central readiness concepts.
- broker supply, broker observed accounts, broker net liquidation, and broker execution readiness as core packet surfaces.
- latest broker submission record, order IDs, perm IDs, execution stream, and fill ledger as latest attempt evidence.
- `Aegis Paper-Trading Status` as the main readiness section instead of `Aegis Lite Manual Execution Status`.
- day-run ledger as final readiness authority, which is not the cleanest post-pivot operator-facing authority for Aegis Lite manual EOD packets.

These are not necessarily unsafe if treated as legacy diagnostics, but they are misleading as the primary ChatGPT/operator context after the Lite pivot.

## Missing Control-Plane Visibility

The current legacy commands do not give ChatGPT one fixed-schema view of:

- latest Aegis Lite EOD run, canonical 15:50 ET timer, broker mode, manual packets, execution queue, blocked packets, and promoted sleeve status.
- Event Monitoring enabled/running status, event rules registry version, event ledger, actionable/advisory/blocked packet separation, validity gate, alert gate, and alert transport truth.
- Research Lab hypothesis/task/promotion state.
- Operator Inbox open/stale/high-priority items.
- Sleeve Performance missing receipt/outcome counts, return rows, slippage/stop issues, and Research feedback recommendations.
- AI Feedback evidence gate, deterministic fallback, findings, and human review requirement.
- Research dataset gap status.
- explicit `Do Not Claim` list for unproven/deferred items.
- explicit `trade_advice_allowed`, `manual_trade_capture_allowed`, and `reason_if_blocked`.

## Duplicated / Hidden Information

Some state is duplicated or hidden across older packet sections:

- readiness appears in current calendar status, latest trading day evidence, day-run ledger, unified truth kernel, and paper-trading status.
- broker/submit state appears in broker supply, execution mode, submit boundary, latest attempt, and paper-trading status.
- operator next actions appear across multiple supply sections, but there is no single post-pivot “next operator action” authority.
- runtime truth root is visible, but source artifact coverage and missing/stale source list are not fixed-schema fields.

This makes the packet harder for ChatGPT to parse conservatively. It also increases the chance that a legacy broker-oriented blocker is mistaken for current Lite manual-operating state.

## Recommendation

Do not extend the legacy Markdown packet further as the primary post-pivot control plane. It is large, pre-pivot, and coupled to legacy PAPER/broker readiness semantics.

Recommended direction:

1. Keep `aegis:chatgpt:packet` / `aegis:chatgpt:show` as legacy closed-world Markdown exports until dependent tools are migrated.
2. Promote the fixed-schema post-pivot control packet to the primary operator/ChatGPT surface.
3. Add or standardize a unified command name:

```bash
npm run aegis:chatgpt:status
```

This should either call the existing fixed-schema control packet builder/show flow or become a thin wrapper that:

- builds `aegis_chatgpt_control_packet.v1`
- prints the human-readable summary
- clearly reports `trade_advice_allowed`, `manual_trade_capture_allowed`, and `reason_if_blocked`
- never consults broker/IB state as a runtime requirement
- fails closed when required Lite/Event/Research artifacts are missing or stale

The existing newer command pair, `aegis:chatgpt:control-packet` and `aegis:chatgpt:control-show`, is the right shape for the post-pivot model. The remaining improvement is naming and operator discoverability, not another architecture layer.

## Proposed Status Model

`aegis:chatgpt:status` should expose these top-level sections:

- Aegis Lite status
- Event Monitoring status
- Research Lab status
- Operator Inbox status
- Sleeve Performance status
- AI Feedback status
- Dataset gaps
- Readiness state
- Current actionable items
- Blocked items
- Next operator actions
- Do Not Claim
- Safety assertions

It should use fixed runtime truth classifications:

- `REAL_RUNTIME`
- `DEMO_ONLY`
- `DRY_RUN_ONLY`
- `PARTIAL_CONTEXT`
- `ADVISORY_ONLY`
- `BLOCKED`

It should include the explicit safety gates:

- `trade_advice_allowed`
- `manual_trade_capture_allowed`
- `reason_if_blocked`

## Bottom Line

The current audited commands are not wrong as legacy diagnostics, but they are no longer the best operator-facing ChatGPT context for Aegis after the Lite pivot.

They should not be treated as the canonical source for manual trade-capture guidance. A unified post-pivot `aegis:chatgpt:status` command should become the primary control-plane summary, backed by the fixed-schema control packet rather than the legacy Markdown paper-readiness packet.
