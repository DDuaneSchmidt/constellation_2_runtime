# critical_path_obligation_pipeline_v1

This contract governs the Bundle 9 bounded obligation pipeline architecture for critical runtime paths.

Core law:
- every Bundle 9 critical path MUST execute through exactly five ordered phases:
  - `resolve_inputs`
  - `evaluate`
  - `persist_outputs`
  - `project_emit`
  - `measure_phase_boundaries`
- no module outside those phases may improvise semantics
- evaluators are the only legal place for Bundle 9 semantic execution on the touched paths

`resolve_inputs` allowed responsibilities:
- canonical root resolution
- argument normalization
- required artifact/ref discovery
- day/session/sleeve/environment/account resolution
- explicit run-identity reservation only when a governed output path requires it before evaluation
- exact-ref replay input loading

`resolve_inputs` forbidden responsibilities:
- semantic evaluation
- rendering decisions
- explanation logic
- fallback truth inference outside governed replay escalation

`evaluate` allowed responsibilities:
- invoke existing semantic kernels and current authoritative stage logic
- run bounded command/stage execution already owned by the current critical path
- return structured semantic results for later persistence and projection

`evaluate` forbidden responsibilities:
- root resolution
- presentation rendering
- probe-driven behavior changes
- policy invention outside existing certified kernels

`persist_outputs` allowed responsibilities:
- write ratified artifacts, pointers, manifests, verdicts, and deterministic pipeline proof outputs
- enforce fail-closed persistence ordering
- publish current-state surfaces only where already contractually allowed

`persist_outputs` forbidden responsibilities:
- semantic branching
- explanation generation
- replay-scope broadening

`project_emit` allowed responsibilities:
- render deterministic JSON/CLI output from already-governed results
- surface authority labels, blocked reasons, proof refs, and timing summaries

`project_emit` forbidden responsibilities:
- semantic recomputation
- trust/actionability decisions
- truth derivation

`measure_phase_boundaries` allowed responsibilities:
- capture elapsed time at phase boundaries only
- compare measured timing to the ratified performance envelope
- emit deterministic timing/result objects

`measure_phase_boundaries` forbidden responsibilities:
- retries
- fallbacks
- semantic interpretation
- behavior changes

Replay law:
- replay/recompute mode legality is governed separately by `bundle9_replay_forensic_escalation_v1`

Semantic-preservation law:
- Bundle 9 MUST preserve Bundles 3–8 semantics and may only change obligation boundaries, measurement, replay labeling, and proof visibility
