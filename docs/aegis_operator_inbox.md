# Aegis Operator Inbox

Date: 2026-05-15

`operator_inbox.v1` is a lightweight capture surface for future Aegis ideas, reminders, observations, possible sleeve improvements, dataset gaps, UI improvements, operational fixes, performance-review notes, and future automation ideas.

Core rule: capture is easy; promotion is strict.

## What It Is

The Operator Inbox is pre-research. It is for quick capture only.

An inbox item can record:
- raw idea title and description,
- category,
- source,
- priority,
- tags,
- related artifact refs,
- notes,
- lineage to a later Research idea or hypothesis.

## What It Cannot Do

Raw inbox items cannot:
- create trades,
- create sleeves,
- create Research Lab tasks,
- affect Aegis Lite,
- affect production state,
- submit broker orders,
- enable IB transmit,
- auto-promote themselves.

## Item, Idea, Hypothesis, Task, Sleeve

`operator_inbox.v1` is a loose capture item.

`research_inbox_item.v1` is the Research Lab idea/register layer used after an operator explicitly decides the item belongs in Research.

`research_hypothesis.v1` is a formal hypothesis with lifecycle status, edge family, regime, failure conditions, and invalidation conditions.

`research_task_queue.v1` controls offline research work. Inbox items and hypotheses do not execute themselves.

`promoted_sleeve_library.v1` is the governed Lite boundary. Only human-approved, implementation-approved promoted sleeves can feed Aegis Lite candidates.

## Commands

Capture:

```bash
python3 ops/tools/aegis_operator_inbox_capture_v1.py --truth_root <path> --title "..." --description "..." --category RESEARCH_IDEA
```

List open items:

```bash
python3 ops/tools/aegis_operator_inbox_review_v1.py --truth_root <path> --action list
```

Mark reviewed:

```bash
python3 ops/tools/aegis_operator_inbox_review_v1.py --truth_root <path> --action mark-reviewed --inbox_item_id <id>
```

Archive or reject:

```bash
python3 ops/tools/aegis_operator_inbox_review_v1.py --truth_root <path> --action archive --inbox_item_id <id>
python3 ops/tools/aegis_operator_inbox_review_v1.py --truth_root <path> --action reject --inbox_item_id <id>
```

Promote to Research idea:

```bash
python3 ops/tools/aegis_operator_inbox_review_v1.py --truth_root <path> --action promote-to-idea --inbox_item_id <id>
```

This writes a `research_inbox_item.v1` artifact and updates the Operator Inbox item lineage. It does not create a Research task.

Promote to hypothesis lineage:

```bash
python3 ops/tools/aegis_operator_inbox_review_v1.py --truth_root <path> --action promote-to-hypothesis --inbox_item_id <id> --hypothesis_id <existing-hypothesis-id>
```

The hypothesis must be created through the existing Research Lab intake path first. The inbox command records lineage only.

Weekly review report:

```bash
python3 ops/tools/aegis_operator_inbox_review_v1.py --truth_root <path> --action report
```

The report writes `operator_inbox_review_report.v1` and shows:
- captured items,
- reviewed items,
- promoted items,
- archived/rejected items,
- stale captured items,
- high-priority open items,
- lineage gaps.

## Promotion Rules

Allowed transitions:

- `CAPTURED -> REVIEWED`
- `CAPTURED -> ARCHIVED`
- `CAPTURED -> REJECTED`
- `REVIEWED -> PROMOTED_TO_IDEA`
- `REVIEWED -> PROMOTED_TO_HYPOTHESIS`
- `REVIEWED -> ARCHIVED`
- `REVIEWED -> REJECTED`
- `PROMOTED_TO_IDEA -> ARCHIVED`
- `PROMOTED_TO_HYPOTHESIS -> ARCHIVED`

Promotion to Research idea preserves:

`operator_inbox_id -> research_inbox_item.v1`

Promotion to hypothesis preserves:

`operator_inbox_id -> hypothesis_id`

Later Research and Lite governance may extend lineage:

`operator_inbox_id -> research_idea_id -> hypothesis_id -> sleeve_id`

## Weekly Workflow

1. List open inbox items.
2. Mark items reviewed.
3. Archive or reject stale/noisy items.
4. Promote only clear Research ideas into `research_inbox_item.v1`.
5. Create formal hypotheses only through Research Lab intake.
6. Generate the review report and inspect lineage gaps.

The Operator Inbox is deliberately small. If an item needs tests, evidence, or promotion, it must leave the inbox through an explicit governed path.
