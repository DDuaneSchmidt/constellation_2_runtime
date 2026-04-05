# official_recommendation_set_v1

Defines the governed recommendation input consumed by Execution Layer V1.

Rules:
- input artifact only
- no recommendation generation in this phase
- must validate against `governance/04_DATA/SCHEMAS/C2/ADVISOR_EXECUTION/official_recommendation_set.v1.schema.json`
- compiler must fail closed if recommendation mapping is missing or ambiguous

Mapping rule:
- each generated action must resolve to exactly one active recommendation by `(domain_id, action_type)`
