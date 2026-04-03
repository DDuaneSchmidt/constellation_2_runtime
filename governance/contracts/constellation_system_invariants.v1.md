# Constellation System Invariants v1

## Purpose

This document records only those invariants that are already proven by repository architecture, registries, contracts, or runtime truth structure.

## Invariants

### INV-001 — Canonical truth root
- **Statement:** Authoritative immutable runtime output must live under `constellation_2/runtime/truth/`.
- **Rationale:** Governance index and manifest define the canonical truth root.
- **Proof basis:** `governance/00_INDEX.md`, `governance/00_MANIFEST.yaml`
- **Enforcement point if proven:** governance authority model and truth-root-preflight surfaces
- **Violation impact:** Canonical truth becomes ambiguous; audit integrity fails.

### INV-002 — Sleeve truth partitioning
- **Statement:** Sleeve-scoped truth must live under `truth_sleeves/<sleeve_id>/<mode>` where sleeve partitioning is in use.
- **Rationale:** Sleeve registry defines canonical sleeve truth partition pattern.
- **Proof basis:** `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`
- **Enforcement point if proven:** sleeve registry and runtime truth_sleeves paths
- **Violation impact:** Sleeve ownership becomes ambiguous and sleeve auditability fails.

### INV-003 — Registered engine IDs only
- **Statement:** Every `engine_id` must be one of the identifiers listed in the governed engine registry.
- **Rationale:** The engine registry explicitly states any other engine ID is invalid and must fail closed.
- **Proof basis:** `governance/02_REGISTRIES/C2_ENGINE_IDS_V1.md`
- **Enforcement point if proven:** validation-time engine ID checks
- **Violation impact:** Invalid engine attribution and non-governed execution lineage.

### INV-004 — Accounting spine authority
- **Statement:** The active accounting spine is `accounting_v2`.
- **Rationale:** Spine authority registry marks accounting v2 active and exclusive.
- **Proof basis:** `governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json`
- **Enforcement point if proven:** spine authority governance and consumers of accounting truth
- **Violation impact:** Split-brain accounting truth.

### INV-005 — Monitoring spine authority
- **Statement:** The active monitoring spine is `monitoring_v1`.
- **Rationale:** Spine authority registry marks monitoring v1 active and exclusive.
- **Proof basis:** `governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json`
- **Enforcement point if proven:** spine authority governance and consumers of monitoring truth
- **Violation impact:** Split-brain monitoring truth.

### INV-006 — Position lifecycle authority
- **Statement:** The authoritative position lifecycle root is `constellation_2/runtime/truth/position_lifecycle_v2`.
- **Rationale:** Truth authority registry declares v2 authoritative and v1 non-authoritative.
- **Proof basis:** `governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json`
- **Enforcement point if proven:** truth authority selection by readers and gates
- **Violation impact:** Position lifecycle reconstruction becomes non-authoritative.

### INV-007 — Positions snapshot authority
- **Statement:** The authoritative positions snapshot root is `constellation_2/runtime/truth/positions_v1/snapshots`.
- **Rationale:** Truth authority registry declares this authoritative for positions snapshots.
- **Proof basis:** `governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json`
- **Enforcement point if proven:** truth authority selection by readers and gates
- **Violation impact:** Position state may be read from deprecated or quarantined roots.

### INV-008 — Execution broker-events authority
- **Statement:** The authoritative broker-events execution evidence root is `constellation_2/runtime/truth/execution_evidence_v1/broker_events`.
- **Rationale:** Truth authority registry declares this authoritative and names v2 broker_events non-authoritative for this family.
- **Proof basis:** `governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json`
- **Enforcement point if proven:** execution evidence readers and reconcilers
- **Violation impact:** Broker event lineage can be reconstructed from the wrong source.

### INV-009 — Surface version exclusivity
- **Statement:** For exclusive surfaces, only the active declared version may be treated as authoritative from the declared enforcement date onward.
- **Rationale:** Truth surface authority registry declares active versions and exclusivity.
- **Proof basis:** `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- **Enforcement point if proven:** report readers, gates, diagnostics consumers
- **Violation impact:** Split-brain report interpretation.

### INV-010 — Operator daily gate authority
- **Statement:** `operator_daily_gate_v3` is the active authoritative operator daily gate surface from 2026-02-20 onward.
- **Rationale:** Surface authority registry declares operator_daily_gate active version v3 and exclusive.
- **Proof basis:** `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- **Enforcement point if proven:** operator readiness and daily verdict consumers
- **Violation impact:** Daily operational readiness may be evaluated against obsolete surfaces.

### INV-011 — Replay integrity authority
- **Statement:** `replay_integrity_v2` is the active authoritative replay integrity surface from 2026-02-20 onward.
- **Rationale:** Surface authority registry declares replay_integrity active version v2 and exclusive.
- **Proof basis:** `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- **Enforcement point if proven:** replay integrity readers and gates
- **Violation impact:** Replay audit conclusions may be based on obsolete evidence.

### INV-012 — Systemic risk gate authority
- **Statement:** `systemic_risk_gate_v3` is the active authoritative systemic risk gate surface from 2026-02-20 onward.
- **Rationale:** Surface authority registry declares systemic_risk_gate active version v3 and exclusive.
- **Proof basis:** `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- **Enforcement point if proven:** risk gating consumers
- **Violation impact:** Risk posture may be read from obsolete versions.

### INV-013 — Intent lineage into preflight
- **Statement:** Every proven phase C preflight decision or veto must correspond to an intent-scoped artifact set or intent-hash-scoped record.
- **Rationale:** Phase C runtime artifacts are emitted in intent-scoped names and directories.
- **Proof basis:** `constellation_2/runtime/truth/phaseC_preflight_v1`
- **Enforcement point if proven:** phase C writers and downstream lineage readers
- **Violation impact:** Intent-to-preflight traceability breaks.

### INV-014 — Order plan lineage
- **Statement:** Every proven equity order plan must belong to exactly one phase C preflight artifact set.
- **Rationale:** Order plans are embedded inside specific preflight artifact directories.
- **Proof basis:** `constellation_2/runtime/truth/phaseC_preflight_v1/.../equity_order_plan.v2.json`
- **Enforcement point if proven:** phase C materialization
- **Violation impact:** Order-plan governance lineage becomes ambiguous.

### INV-015 — Submission evidence lineage
- **Statement:** Every governed submission must appear in execution evidence submissions and/or submission index surfaces.
- **Rationale:** Submission truth surfaces exist specifically for governed submission evidence.
- **Proof basis:** `constellation_2/runtime/truth/execution_evidence_v1/submissions`, `constellation_2/runtime/truth/execution_evidence_v1/submission_index`
- **Enforcement point if proven:** execution evidence writers
- **Violation impact:** Submission traceability fails.

### INV-016 — Broker event lineage
- **Statement:** Broker event evidence must be captured under the authoritative broker-events truth family or be treated as non-authoritative.
- **Rationale:** Broker event family has an explicit authority root.
- **Proof basis:** `governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json`
- **Enforcement point if proven:** broker event observers and reconciliation readers
- **Violation impact:** Execution reconstruction may consume non-authoritative events.

### INV-017 — Fill ledger lineage
- **Statement:** Every authoritative fill ledger record must derive from execution evidence surfaces and remain inside canonical truth.
- **Rationale:** Fill ledger is a governed execution evidence output surface under canonical truth.
- **Proof basis:** `constellation_2/runtime/truth/fill_ledger_v1`
- **Enforcement point if proven:** fill ledger writer
- **Violation impact:** Fill attribution and reconciliation integrity fail.

### INV-018 — Fail-closed on unknown authority
- **Statement:** When multiple versions exist and no active authority is proven for the question at hand, the system or AI consumer must fail closed or declare the result UNKNOWN.
- **Rationale:** Governance and authority registries require explicit active authority selection.
- **Proof basis:** authority registries and governance authority model
- **Enforcement point if proven:** AI reasoning, gates, audits
- **Violation impact:** Silent acceptance of ambiguous truth surfaces.

Generated UTC: 2026-03-08T03:56:21Z
