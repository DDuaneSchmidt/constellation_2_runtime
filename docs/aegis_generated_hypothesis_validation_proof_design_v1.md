# Aegis Generated Hypothesis Validation Proof Design v1

## Design

`aegis_generated_hypothesis_validation_proof_v1` is a deterministic read model over existing generated-hypothesis and research validation artifacts.

The proof starts from `aegis_generated_hypothesis_throughput_v1.generated_hypotheses[]`, overlays owner/action context from `aegis_data_action_routing_v1`, and uses the authoritative Oil Shock candidate-flow artifact for Oil Shock candidate-flow state and stop reason.

Candidate, observation, outcome, and validation sample states are counted only from existing artifacts. A missing count is never treated as success. No row may be marked as validation-producing unless existing validation sample evidence is bound to that generated hypothesis.

## Stop Logic

Macro Calendar stops at data readiness / shadow validation when routed to `DAVID`. Oil Shock stops at candidate flow when its authoritative candidate-flow artifact reports no candidate flow and missing market/source data.

## UI

Command Center renders a compact section below Generated Hypothesis Progress showing the furthest stage reached, whether any generated hypothesis produced validation samples, Oil Shock stop stage and next step, and Macro Calendar stop stage and David action.

## Safety

The producer writes only the proof artifact. It does not call trading systems, broker APIs, candidate producers, paper lifecycle commands, outcome writers, allocation writers, or validation sample writers.
