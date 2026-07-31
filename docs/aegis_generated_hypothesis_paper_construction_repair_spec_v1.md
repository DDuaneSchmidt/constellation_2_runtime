# Aegis Generated Hypothesis Paper Construction Repair Spec V1

## Ownership

`stop_price` is owned by governed risk policy for the candidate sleeve. The deterministic derivation is:

- Source: `C2_RISK_POLICY_REGISTRY_V1.{sleeve_id}.stop_loss_bps_default`
- Long formula: `entry_reference_price * (1 - stop_loss_bps / 10000)`
- Short formula: `entry_reference_price * (1 + stop_loss_bps / 10000)`
- Rounding: currency cents, half-up

Candidate contracts must carry `stop_price`, `stop_loss_bps`, `stop_price_source`, and `stop_price_status` before paper construction. If the governed source is absent, candidate contract validation fails closed with an explicit stop/risk-policy blocker.

## Runtime Artifact

`aegis_generated_hypothesis_paper_construction_repair_v1` is a proof artifact over existing authoritative artifacts. It recognizes paper observation only when the authoritative paper position ledger contains the Oil Shock candidate position.
