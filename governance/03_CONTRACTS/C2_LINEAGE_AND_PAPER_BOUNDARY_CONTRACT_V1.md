---
id: C2_LINEAGE_AND_PAPER_BOUNDARY_CONTRACT_V1
title: "C2 Lineage + Paper Boundary Contract v1"
owner: "Constellation"
status: "ACTIVE"
scope: ["paper_trading", "execution_evidence", "lineage", "positions", "monitoring"]
last_reviewed_utc: "2026-02-17T00:00:00Z"
---

# Purpose

This contract defines the minimum audit-grade lineage and broker-truth requirements for Constellation 2.0 PAPER trading.

Hostile-review posture:
- No silent failures.
- No synthetic execution evidence accepted in PAPER readiness claims.
- Deterministic, immutable truth artifacts.
- Fail-closed enforcement at submission boundary and monitors.

# Definitions

- **Intent**: an engine-authored deterministic exposure intent artifact.
- **Order Plan**: a deterministic transformation of intent into a broker-ready plan.
- **Submission**: the boundary where the system sends an order to IB PAPER and records evidence.
- **Broker Raw Log**: append-only JSONL log of raw IB API events.
- **Broker Day Manifest**: immutable seal that validates and hashes the broker raw log for the day.

# Required Lineage Fields

Every artifact from Order Plan onward MUST include:

- engine_id (string, non-empty)
- source_intent_id (string, non-empty)
- intent_sha256 (string, sha256 over canonical JSON of the intent payload)
- producer.module (string, non-empty)

Fail-closed: missing any required field MUST stop the pipeline before broker submission.

# PAPER Broker Truth Requirements

PAPER mode claims require:

1) A broker raw log exists:
   constellation_2/runtime/truth/execution_evidence_v1/broker_events/<DAY>/broker_event_log.v1.jsonl

2) A broker day manifest exists and validates the raw log:
   .../broker_event_day_manifest.v1.json

3) Submission-level execution evidence MUST be derived from broker raw events, not synthetic placeholders.
   Synthetic statuses (prefix "SYNTH") are forbidden in PAPER readiness.

Fail-closed: if submissions exist for a day and broker raw or manifest is missing, the day is NOT PAPER READY and the pipeline must fail.

# Atomic Pointer Requirements

Any pointer file (e.g., latest.json, effective pointers) MUST:

- be written using atomic replace in the same directory
- reference an existing immutable target
- include or imply a hash match to the target payload

Fail-closed: pointer mismatch triggers monitoring alert and blocks reporting.

# Monitoring Requirements (Hard)

Monitoring MUST detect and alert on:

- Missing intents by engine/day (expected schedule vs actual)
- Submissions with no broker events beyond SLA
- Any "SYNTH_*" execution status in PAPER
- Reconciliation failures tied to submission_id
- Any schema or lineage validation failure

# Audit Evidence

Minimum evidence set for a day with activity:

- intents_v1 snapshots per engine/day
- order plan(s) with required lineage
- broker_submission_record v3 (or higher) with required lineage
- broker_event_log JSONL + broker_event_day_manifest
- execution_event_record v2 (or higher) linked to broker raw via manifest sha256
- positions snapshot with engine attribution
- reconciliation report referencing submission_ids
