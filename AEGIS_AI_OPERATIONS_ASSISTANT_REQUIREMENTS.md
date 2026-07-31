# AEGIS_AI_OPERATIONS_ASSISTANT_REQUIREMENTS.md

## Purpose

Transform the current "Ask Aegis" panel from a predefined operational FAQ into a real grounded AI operations assistant.

The assistant must answer operator questions using existing Aegis artifacts, audits, runtime graphs, review briefs, sleeve diagnostics, research artifacts, and operational state.

The assistant exists to explain Aegis.

The assistant does not control Aegis.

---

# Core Principle

Current implementation:

```text
Button
-> Fixed Question
-> Fixed Answer
```

Target implementation:

```text
Operator Question
-> Context Assembly
-> Evidence Selection
-> AI Reasoning
-> Grounded Answer
```

The operator may ask arbitrary questions.

The assistant must answer from evidence.

---

# Allowed Questions

Examples:

```text
Why is this candidate here?

Why is QQQ awaiting review?

Why is Defensive Tail silent?

What changed today?

Why is the mode blocked?

What should be fixed first?

Why is this hypothesis not qualified?

What happened after the 9:50 run?

Why is this position improving?

Why is this sleeve underperforming?

What should I pay attention to today?
```

The system must not require predefined buttons.

Buttons may remain as suggested prompts only.

---

# Architecture

Required flow:

```text
Question
    -> Intent Classification
    -> Context Builder
    -> Evidence Selection
    -> AI Assistant
    -> Grounded Response
```

---

# Context Builder

Create:

```text
aegis_ai_operations_context_v1
```

Purpose:

Build deterministic context from Aegis artifacts.

The context builder selects evidence.

The AI does not search the entire system directly.

---

# Context Sources

Allowed sources:

* verified runtime graph
* audit handoff
* control packet
* hydrate packet
* runtime truth
* operator cockpit
* queue audit
* candidate readiness
* position reviews
* sleeve analytics
* sleeve evaluation
* research review
* hypothesis qualification
* paper validation
* performance artifacts
* market regime artifacts

The context must record all sources used.

---

# Required Metadata

Every answer must include:

* context_id
* context_hash
* generated_at
* source_artifacts
* source_artifact_hashes
* model_name
* model_version
* prompt_version

---

# Grounding Rules

Every answer must be evidence-backed.

The assistant may:

* explain
* summarize
* compare
* diagnose
* prioritize
* interpret

The assistant may not:

* fabricate facts
* invent artifacts
* invent positions
* invent candidates
* invent hypotheses

---

# Unsupported Claims

Required field:

```text
unsupported_claims
```

If evidence is missing:

```text
I do not have enough evidence to answer.
```

must be returned.

---

# Safety Rules

The assistant may not:

* create trades
* submit orders
* override qualification
* override risk
* override sleeves
* override candidate generation
* override execution

The assistant explains.

It does not decide.

---

# Operator Actions

Assistant may recommend:

```text
Investigate
Monitor
Review
Refresh
Run Diagnostic
```

Assistant may not recommend:

```text
Buy
Sell
Exit
Increase size
Reduce size
Execute trade
```

---

# Question Categories

Support:

## Operational

```text
What happened?
What changed?
What is blocked?
What should be fixed?
```

## Position

```text
Why is this position here?
Why is it improving?
Why is it weakening?
```

## Candidate

```text
Why was this candidate created?
Why is it awaiting review?
```

## Sleeve

```text
Why is this sleeve silent?
Why is it underperforming?
```

## Research

```text
Why is this hypothesis collecting evidence?
Why is it not qualified?
```

## Performance

```text
Why did performance change?
```

---

# Explain This

Every major entity should expose:

```text
Ask about this position
Ask about this candidate
Ask about this sleeve
Ask about this hypothesis
Ask about this blocker
```

The selected object becomes context.

---

# UI Requirements

Replace static FAQ behavior.

Provide:

```text
Ask Aegis
```

with:

* question input
* suggested prompts
* response area
* evidence sources
* confidence
* supporting artifacts

Buttons become shortcuts.

Buttons are not the primary mechanism.

---

# Evidence Panel

Every answer must expose:

```text
Sources Used
```

collapsed by default.

The operator should be able to inspect evidence.

---

# Confidence

Every answer includes:

```text
HIGH
MEDIUM
LOW
```

confidence.

Confidence is determined from:

* evidence coverage
* artifact quality
* data freshness

Not model confidence.

---

# Determinism

AI answers may vary in wording.

Evidence selection must be deterministic.

Same question + same context must produce:

* same evidence set
* same sources
* same confidence

---

# Auditability

Store:

```text
question
context_hash
sources
answer
confidence
timestamp
```

Create:

```text
aegis_ai_operations_response_v1
```

---

# Self Check

Add:

```bash
npm run aegis:ai-operations-self-check
```

Fail if:

* answer lacks evidence
* unsupported claims exist
* source references missing
* forbidden trade language appears
* confidence missing
* context hash missing

---

# UX Goals

Operator should be able to ask:

```text
What should I fix first?
```

and receive a useful grounded answer in less than 10 seconds.

The operator should not need to know:

* artifact names
* npm commands
* runtime internals

to understand the answer.

---

# Governance Rule

Ask Aegis is an AI operations assistant.

It explains the system.

It does not operate the system.

Update:

```text
aegis/modules/operator_portal/aegis.module.yaml
```

to reference this document as the authority for the AI Operations Assistant.
## Operator UI Architecture Authority

This document is subordinate to `AEGIS_OPERATOR_UI_ARCHITECTURE_REQUIREMENTS.md` for rendering architecture. Pages must consume the Operator Surface Contract through shared templates before rendering page-specific content.

