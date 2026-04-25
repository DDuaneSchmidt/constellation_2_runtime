# configuration_review_diff_v1

`configuration_review_diff_v1` is the deterministic review surface comparing a candidate compiled configuration with the currently active compiled configuration.

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/configuration_review_diff_v1/<REVIEW_DIFF_ID>/configuration_review_diff.v1.json`

Canonical writer owner:
- `configuration_activation_authority_v1`

Rules:
- it MUST bind one exact candidate `compiled_active_config_v1`
- if `configuration_state_v1/current.json` is absent, `review_kind` MUST be `FIRST_ACTIVATION`
- if `configuration_state_v1/current.json` is present, `review_kind` MUST be `SUPERSEDING_ACTIVATION`
- superseding review MUST bind the prior `configuration_state_v1/current.json` and prior active `compiled_active_config.v1`
- activation MUST block if the required review diff artifact is missing or unreadable
