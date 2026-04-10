# runtime_path_authority_v1

`runtime_path_authority_v1` is the shared resolver and classifier for Constellation runtime paths.

It exists to eliminate silent path ambiguity between:
- canonical runtime truth roots
- canonical runtime sleeve truth roots
- authoritative repo-local mirror roots
- active release roots

Rules:
- policy-critical and write-critical runners must resolve through this layer
- explicit canonical runtime paths win
- silent fallback to repo-local or release-local truth paths is forbidden for policy-critical reads and writes
- authoritative repo-local truth paths remain readable only for governed advisory or mirror workflows

This surface is implemented as a governed contract plus shared library and tests.
No day-scoped report schema is used because this is resolver authority, not runtime evidence.
