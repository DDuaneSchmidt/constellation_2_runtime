# AEGIS Event Dislocation Candidate Suppression Diagnostics Design v1

T06 is a read-only evidence reducer.

It builds a signal inventory from the signal evidence graph, falling back to candidate-generation rejection evidence when needed. It then reads candidate manifest rows and candidate contract validation evidence to determine the furthest proven stage.

Classification is deterministic and ordered:

1. signal schema, lineage, confidence, and quality validation;
2. candidate builder invocation;
3. candidate construction policy presence;
4. duplicate, cooldown, exposure, data-quality, and risk suppression;
5. candidate construction failure;
6. candidate contract rejection;
7. unknown deterministic blocker.

For June 2, 2026 evidence, the expected blocker is contract rejection after candidate conversion: raw Event Dislocation signals exist, candidate attempts exist, and candidate contract validation rejects them because candidate contract fields such as direction, instrument type, and governance status are unfulfilled.
