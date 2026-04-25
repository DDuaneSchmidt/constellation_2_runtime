# control_plane_trust_semantic_preservation_bundle6_v1

This contract governs Bundle 6 semantic preservation.

Bundle 6 MUST preserve:
- Bundle 5 transition semantics
- Bundle 4 certification and forbidden-root boundary semantics
- Bundle 3 configuration activation semantics

Bundle 6 is a trust-surface bundle only:
- it MUST NOT rewrite control-plane stage ordering
- it MUST NOT weaken certification requirements
- it MUST NOT weaken forbidden-root rejection
- it MUST NOT promote derived or advisory trust surfaces into authority

