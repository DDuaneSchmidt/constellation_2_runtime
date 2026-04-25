# control_plane_boundary_migration_quarantine_v1

This contract governs forbidden and quarantined control-plane surfaces after Bundle 4 v2.

Forbidden live authority roots:
- `/home/node/constellation/constellation_2/runtime/truth`
- `/home/node/constellation/constellation_2/runtime/truth_sleeves`
- `/home/node/constellation_2_runtime`

Forbidden live authority behavior:
- no live validator may accept authority inputs from forbidden roots
- no live control-plane writer may target forbidden roots
- no live current-state surface may resolve outside the canonical truth roots

Quarantined legacy/diagnostic surfaces:
- `reports/startup_proof_validation_v1/<DAY>/startup_proof_validation.v1.json`
- `session_authority_status_v1/current.json`
- `session_authority_alert_v1/current.json`

Quarantine rule:
- quarantined surfaces may remain readable for diagnostics and operator summaries
- quarantined surfaces MUST be labeled derived/non-authoritative
- quarantined surfaces MUST NOT satisfy an authoritative family prerequisite

Validator enforcement:
- boundary validator MUST fail closed on forbidden roots
- family validators MUST fail closed when an authoritative ref resolves to a quarantined derived surface

Transition rule:
- there is no delayed transition allowance for control-plane authority inputs in this bundle
- forbidden-root or derived-surface authority use is an immediate fail-closed condition
