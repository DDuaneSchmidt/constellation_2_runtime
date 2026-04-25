# bundle9_decomposition_map_v1

This contract governs the first-wave Bundle 9 decomposition map.

First-wave targets:
- `ops/tools/run_c2_paper_day_orchestrator_v2.py`
- `constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_v1.py`

Deferred targets:
- `constellation_2/common/session_authority_monitor_v1.py`
- `constellation_2/common/subsystem_authority_v1.py`

Why deferred:
- they remain large and important, but the first-wave Bundle 9 scope is bounded to one execution hot path and one operator collector hot path
- expanding further would risk semantic drift across Bundles 3–8 in a single pass

Decomposition map:

## `ops/tools/run_c2_paper_day_orchestrator_v2.py`
- current mixed obligations:
  - argument/root/session resolution
  - run-identity reservation
  - stage evaluation and command execution
  - manifest/verdict/pointer persistence
  - replay labeling
  - return-code projection
- required Bundle 9 splits:
  - resolver: root/day/account/ledger/mode/session/identity reservation
  - evaluator: stage orchestration, classification, status derivation
  - persistence adapter: attempt manifest, compat publishers, verdict, pointer append, proof sidecar
  - projection adapter: deterministic CLI result and exit-code mapping
  - probe: phase timings and budget comparison
- responsibilities that must remain centralized:
  - current stage order and status derivation
  - governed submit and manifest publication sequencing
- responsibilities that must not move:
  - Bundle 5/7 semantics and stage meaning
  - canonical truth-root and sleeve-account enforcement

## `constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_v1.py`
- current mixed obligations:
  - attempt selection
  - certified artifact discovery
  - projection evaluation
  - server-side diffing
  - surface rendering payload assembly
- required Bundle 9 splits:
  - resolver: day/attempt/truth/config normalization
  - evaluator: current collector computation over certified inputs
  - persistence adapter: deterministic pipeline proof JSON only
  - projection adapter: current payload plus proof block
  - probe: phase timings and budget comparison
- responsibilities that must remain centralized:
  - current collector payload shape
  - existing operational-readiness and advisor-visibility semantics
- responsibilities that must not move:
  - Bundle 6 trust-plane semantics
  - Bundle 7 release/readiness semantics

Semantic-preservation expectation:
- same certified inputs MUST yield the same authoritative outputs, refs, authority labels, and blocked/fail behavior before and after Bundle 9 decomposition
