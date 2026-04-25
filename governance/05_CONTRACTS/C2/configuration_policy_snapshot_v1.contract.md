# configuration_policy_snapshot_v1

`configuration_policy_snapshot_v1` is the immutable policy-snapshot surface for runtime configuration activation.

Canonical output:
- `/home/node/constellation_runtime_data/truth/configuration_policy_snapshot_v1/<POLICY_SNAPSHOT_ID>/configuration_policy_snapshot.v1.json`

Canonical writer owner:
- `configuration_activation_authority_v1`

Rules:
- it MUST record the exact governed configuration source documents by path and sha256
- it MUST record the exact governance utility refs used to validate downstream artifact conventions
- it MUST be immutable once written
- it MUST carry `constitutional_dependency_declaration` and `constitutional_lineage`
- it MUST fail closed when no governed source-document set can be proven
- downstream validation and compilation may consume only this snapshot, not ad hoc repo reads
