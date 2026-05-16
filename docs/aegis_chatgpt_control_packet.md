# Aegis ChatGPT Control Packet v1

## Purpose

`aegis_chatgpt_control_packet.v1` is the fixed-schema context packet that David can give to ChatGPT after the Aegis Lite pivot. It summarizes the current operator-facing state from runtime artifacts only.

Core rule:

- No control packet means no trade advice.
- Partial context means advisory-only discussion.
- Demo or dry-run context is never actionable.

## Artifact

The builder writes:

`reports/aegis_chatgpt_control_packet_v1/<YYYY-MM-DD>/aegis_chatgpt_control_packet.v1.json`

The packet includes fixed sections for Aegis Lite, Event Monitoring, Research Lab, Operator Inbox, Sleeve Performance, AI Feedback, dataset gaps, actionable items, blocked items, next operator actions, Do Not Claim, and safety assertions.

## Runtime Truth

Allowed `runtime_truth_classification` values:

- `REAL_RUNTIME`
- `DEMO_ONLY`
- `DRY_RUN_ONLY`
- `PARTIAL_CONTEXT`
- `ADVISORY_ONLY`
- `BLOCKED`

Manual trade-capture guidance requires `REAL_RUNTIME`. `DEMO_ONLY`, `DRY_RUN_ONLY`, and `PARTIAL_CONTEXT` always block trade advice.

## Safety Gate

The packet includes:

- `trade_advice_allowed`
- `manual_trade_capture_allowed`
- `reason_if_blocked`

Manual trade capture is allowed only when the packet proves a current real runtime actionable item with symbol, side, sizing, entry, stop, risk, and validity window, with `broker_mode=MANUAL_ONLY` and no autonomous execution.

## Commands

Legacy Markdown packet commands remain available:

```bash
npm run aegis:chatgpt:packet
npm run aegis:chatgpt:show
```

The v1 control-packet commands are:

```bash
npm run aegis:chatgpt:control-packet
npm run aegis:chatgpt:control-show
```

For explicit safe roots:

```bash
python3 ops/tools/build_aegis_chatgpt_control_packet_v1.py --truth_root /tmp/aegis_truth --day 2026-05-15
python3 ops/tools/show_aegis_chatgpt_control_packet_v1.py --truth_root /tmp/aegis_truth --day 2026-05-15
```

## Do Not Infer

ChatGPT must not infer missing runtime state. If the packet lists missing or stale sources, ChatGPT may explain blockers and next steps, but must not provide manual trade-capture guidance.

The packet explicitly lists unproven or deferred claims such as live email/SMS transport, missing promoted runtime candidates, incomplete dataset binding, deterministic AI feedback fallback, and event monitor schedule/status gaps.

## Safety Boundaries

The control packet is read-only. It does not:

- create trades
- submit broker orders
- start IB/TWS
- mutate Aegis Lite runtime
- promote Research Lab artifacts
- alter sleeve logic
- bypass Event/EOD validity gates
