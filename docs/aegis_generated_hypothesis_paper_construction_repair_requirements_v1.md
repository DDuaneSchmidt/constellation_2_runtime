# Aegis Generated Hypothesis Paper Construction Repair Requirements V1

Package 017 repairs the deterministic generated-hypothesis path from candidate contract to paper observation. It must not generate outcomes, validation samples, research quality metrics, broker actions, live trades, trade advice, real-capital allocation, autonomous execution, or manual operator overrides.

## Required Runtime Proof

The `aegis_generated_hypothesis_paper_construction_repair_v1` artifact must report the Oil Shock generated hypothesis id, candidate contract id, stop-price presence/source/status, paper construction status, authoritative paper ledger insertion, paper observation id, furthest stage reached, remaining blocker, blocker reason, owner, and David action requirement.

A successful run ends at paper observation with `remaining_blocker: NONE`, `paper_position_ledger_written: true`, and `david_action_required: false`.
