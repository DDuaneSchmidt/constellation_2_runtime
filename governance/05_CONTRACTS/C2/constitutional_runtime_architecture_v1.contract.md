# constitutional_runtime_architecture_v1

Purpose:
- define the permanent governing runtime model for active Constellation artifact production and consumption
- prevent hidden dependency, root-ownership, shadow-truth, and ambiguous-writer failures on the canonical runtime path

Scope:
- active C2 paper-day runtime
- governed artifact contracts
- dependency law
- lineage and finality semantics
- admission and enforcement boundaries
- audit and read-model restrictions

Non-goals:
- no rewrite of the existing seven-function runtime
- no workflow-engine introduction
- no advisory, UI, or AI feature expansion
- no weakening of fail-closed behavior

## Runtime tier model

The runtime is governed as six constitutional tiers:

1. Source Authorities
   - own source facts and append-only evidence
2. Canonical State Compilers
   - produce canonical derived state from governed inputs only
3. Policy Interpreters
   - bind immutable policy snapshots to governed state
4. Decision Producers
   - produce proposed action or allocation artifacts from frozen governed inputs
5. Admission / Enforcement Boundaries
   - decide allow, block, degrade, or withhold execution on explicit governed inputs only
6. Audit + Read Models
   - read finalized governed artifacts only and must never regain authority

## Artifact authority law

- every governed artifact family MUST have one authoritative writer
- every governed artifact family MUST declare one authoritative domain and root
- every governed artifact family MUST declare its legal consumers
- every governed artifact family MUST declare required upstream dependencies
- no consumer may rely on undeclared dependencies
- no silent fallback between canonical truth and execution truth is allowed

## Artifact taxonomy

Allowed governed artifact classes:
- `source_fact`
- `compiled_state`
- `policy_snapshot`
- `policy_interpretation`
- `decision_proposal`
- `admission_result`
- `execution_result`
- `outcome_record`
- `override_record`
- `exception_record`
- `read_model`

## Dependency law

- dependency flow is downward-only by governed class
- same-tier dependencies are allowed only when explicitly declared and not upward
- lower-tier artifacts must never depend on higher-tier artifacts
- hidden dependency observation is a constitutional failure
- a governed build or boundary must fail closed if an observed dependency is undeclared

## Root/domain ownership

Canonical domains for the active route:
- `canonical_portfolio`
  - canonical truth root only
- `execution`
  - execution truth root only unless a declared mirror exists
- `economic`
  - canonical build plus execution package when explicitly declared
- `authority`
  - readiness, session, gate, and admission artifacts

If an artifact exists in multiple roots:
- one root is authoritative
- one declared bridge may mirror it
- consumers must not improvise fallback across roots

## Lifecycle and finality law

Allowed finality states:
- `provisional`
- `finalized`
- `corrected`
- `superseded`
- `archived`

Rules:
- governed artifacts must carry explicit finality state through constitutional lineage when adopted
- `corrected` requires an explicit reference to the corrected artifact
- `superseded` requires an explicit reference to the superseded artifact
- read models must not declare governed finality for themselves

## Frozen decision input law

- decision-producing artifacts must freeze the exact governed input bundle used for the decision
- frozen input bundles must be deterministic and hash-stable
- policy snapshot refs, when used, must be immutable refs and not mutable constants

## Read-model restriction

- a read model is projection or composition only
- a read model must not register as the authoritative writer of canonical or execution truth
- a read model must not emit governed authority fields that imply canonical writer ownership
- a read model may read finalized governed artifacts and may expose provenance refs only

## Audit lineage law

- governed artifacts adopted into the constitutional layer must carry:
  - artifact type and version
  - authority id
  - producer id
  - generated and effective time
  - finality state
  - governed input refs
  - policy snapshot refs when applicable
  - code version and run id when available
- audit artifacts are append-only

## Fail-closed law

- invalid writer ownership must block
- invalid dependency direction must block
- missing required dependency must block unless the contract explicitly allows owned materialization
- read-model authority claims must fail validation
- missing lineage on adopted governed artifacts must block schema validation

## Adoption guidance

- constitutional runtime validation is introduced first as a validation and metadata layer
- existing runtime producers may adopt progressively
- active-route adoption in this pass is limited to:
  - economic state build
  - execution build
  - UI projection guardrails

## Invariants

- one writer per governed artifact family
- explicit root ownership for every governed artifact family
- declared dependency DAG only
- immutable policy refs where policy is bound
- frozen decision input bundle for adopted decision producers
- explicit finality and correction semantics
- append-only audit lineage
- no shadow computation in UI or read models
