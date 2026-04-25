# Global Context Authority V1

## Purpose
Global Context Authority is the canonical upstream context compiler for fresh PAPER entry. It does not own business truth. It consumes the sealed day-activation package, validates context coherence, emits global_context_build.v1.json, and seals global_context_package.v1.json only when closure is complete.

## Inputs
- day_activation_package_v1

## Outputs
- truth/reports/global_context_build_v1/<DAY>/<context_hash>/global_context_build.v1.json
- truth_sleeves/<SLEEVE>/<MODE>/global_context_package_v1/<DAY>/<context_hash>/global_context_package.v1.json

## Ownership Boundary
Global Context Authority may evaluate and seal already-governed upstream context. It may not invent or override the underlying day-activation package or its raw truth-owner artifacts.

## Seal Rules
- no package unless closure is COMPLETE
- package must record dependency refs and hashes
- same day/sleeve/mode/account/operation plus same upstream truths must reproduce the same package hash

## Downstream Consumption
- Economic State Authority consumes global_context_package_v1
- Execution Build Authority consumes global_context_package_v1
- Submit Boundary does not reopen raw upstream authority nodes in the primary path
