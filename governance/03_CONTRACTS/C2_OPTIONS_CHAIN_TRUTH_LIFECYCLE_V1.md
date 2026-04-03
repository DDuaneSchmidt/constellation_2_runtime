# C2 Options Chain Truth Lifecycle V1

Status: ACTIVE

Required ordering:
1. A6C_OPTIONS_RAW_CHAIN_ACQUIRE_V1
2. A6D_OPTIONS_CHAIN_TRUTH_PROMOTION_V1
3. A6E_PHASEC_IDENTITY_MATERIALIZER_MIXED_V1
4. A6F_EXPOSURE_TO_OPTIONS_INTENT_ADAPTER_V1
5. A7A_GOVERNED_SUBMIT_V5

Identity surface:
- constellation_2/runtime/truth/phaseC_preflight_v1/<day>/<intent_hash>/

For authorized SHORT_VOL_DEFINED intents with authorized_quantity > 0:
- same-day options chain snapshot must exist
- same-day freshness certificate must exist
- governed exposure->options adapter path must produce `options_intent.v2` from `exposure_intent.v1`
- same-day options identity dir must exist
- submit must fail closed if any required surface is missing

Prohibitions:
- no alternate truth roots
- no sample promotion into truth
- no quarantine surfaces
- no operator/manual substitute chain truth
