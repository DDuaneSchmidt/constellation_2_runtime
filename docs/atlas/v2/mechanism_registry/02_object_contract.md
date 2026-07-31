# Object Contract

MechanismRegistryEntry persists one durable entry per mechanism family.

MechanismCluster records representative claim IDs and compressed claim count for a family. Duplicate claims increase claim_count without increasing mechanism family count.

MechanismLineage records root family lineage only in V1. It does not generate variants.

MechanismMetrics appends count and coverage metrics. Count updates are appended as new metrics records, not overwrites.
