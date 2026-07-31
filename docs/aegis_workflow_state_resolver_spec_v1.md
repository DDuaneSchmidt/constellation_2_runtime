# Aegis Workflow State Resolver Spec v1

## Artifact

`reports/aegis_workflow_state_resolver_v1/{day}/workflow_state_resolver.v1.json`

## Schema

- `schema_id`: `aegis_workflow_state_resolver_v1`
- `day_utc`
- `computed_at_utc`
- `rules`: ordered list of resolver rules
- `stale_artifact_policy`
- `determinism_policy`
- `safety_statement`
- `safety`
- `content_hash`

## Rule Record

Each rule includes:

- `rule_id`
- `precedence`
- `result_state`
- `condition`
- `overrides`
- `next_action`
- `reason_code`

## Consumer Contract

`aegis_hypothesis_workflow_state_v1` records the selected `resolver_rule_id` for generated hypotheses and includes state aging fields. The Research UI reads workflow state and action queue artifacts only; it does not run resolver rules.
