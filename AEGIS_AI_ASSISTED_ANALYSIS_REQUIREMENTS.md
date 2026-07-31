# Aegis AI-Assisted Analysis Requirements

## Core Architecture

```text
Deterministic Evidence Context
    ↓
AI Brief Generation
    ↓
Audited Brief Artifact
    ↓
UI
```

AI-assisted analysis is read-only. Aegis may use AI to explain, summarize, compare, diagnose, and narrate verified evidence, but it must not invent truth or act on analysis.

Every AI analysis product must have two artifacts:

```text
aegis_<domain>_review_context_v1
aegis_<domain>_review_brief_v1
```

The context artifact is deterministic and auditable. The brief artifact is generated from the context artifact and is constrained by explicit safety rules.

## Required Context Artifact Rules

Every context artifact must include:

* context_id
* domain
* target_id
* generated_at
* as_of
* source_artifacts
* source_artifact_hashes
* input_context_hash
* data_quality_status
* included_evidence
* excluded_evidence
* null_reasons
* allowed_ai_scope

The AI may only use evidence present in the context artifact.

## Required Brief Artifact Rules

Every AI brief must include:

* brief_id
* context_id
* input_context_hash
* prompt_version
* model_name
* model_version
* temperature
* schema_version
* generated_at
* conclusion
* supporting_evidence
* contradicting_evidence
* risks
* monitoring_points
* unsupported_claims
* source_references
* data_quality_status

If `unsupported_claims` is non-empty, status must be `PARTIAL` or `BLOCKED`.

## Safety Rules

AI may:

* explain
* summarize
* compare
* diagnose
* narrate

AI may not:

* recommend trade entry
* recommend trade exit
* size positions
* modify risk
* modify candidates
* modify sleeve rules
* override qualification
* submit broker orders
* enable live trading

Forbidden execution language includes:

* buy
* sell
* exit now
* increase size
* reduce size
* execute

Use safer language such as:

* Monitoring point
* Risk to watch
* Evidence weakened
* Evidence strengthened

## Phase 1: Position Review Brief

Phase 1 implements only:

```text
aegis_position_review_context_v1
aegis_position_review_brief_v1
```

Commands:

```bash
npm run aegis:position-review-context
npm run aegis:position-review-brief
npm run aegis:position-review-self-check
```

Context source inputs:

* paper position ledger
* entry receipts
* exit receipts if present
* sleeve attribution
* market marks
* signal evidence
* candidate lineage
* paper P&L report
* sleeve analytics

For every open paper position, Aegis must build a deterministic context.

The brief should answer:

* What was the entry thesis?
* What has changed since entry?
* What evidence still supports the position?
* What evidence contradicts it?
* What risks should be monitored?
* What data quality issues affect the review?

The UI must read audited brief artifacts. The browser must not generate AI text directly.

## Phase 1 Self-Check Requirements

The self-check must fail if:

* brief exists without matching context
* context hash mismatch
* source artifact hash missing
* unsupported claims are present but status is `CANONICAL`
* brief uses forbidden trade advice language
* UI renders AI text not backed by a brief artifact
* browser generates AI content directly

## Future Products Not Included In Phase 1

Do not implement these until explicitly requested:

* Candidate Review
* Regime Analysis
* Sleeve Review
* Validation Analysis
* Portfolio Narrative

## Safety Invariants

AI-assisted analysis must not enable:

* trade advice
* broker execution
* live trading
* autonomous live trading
* execution recommendations

This workflow is explanatory and diagnostic only.
