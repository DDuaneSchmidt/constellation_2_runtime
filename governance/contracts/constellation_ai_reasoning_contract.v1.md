# Constellation AI Reasoning Contract v1

## Purpose

This contract defines how any AI agent must reason about the Constellation repository and its runtime truth surfaces.

## Scope

This contract applies to AI analysis of:
- repository architecture
- runtime truth
- paper-trading readiness
- lifecycle completeness
- diagnostics
- capability claims
- sleeve-scoped and system-scoped truth surfaces

## Required Inputs

An AI agent must load these inputs in this order before making architecture or readiness claims:

1. `constellation_2/runtime/truth/system_snapshot/constellation_system_snapshot.v1.json`
2. `constellation_2/runtime/truth/system_snapshot/constellation_ai_architecture_index.v1.json`
3. `constellation_2/runtime/truth/system_snapshot/constellation_capability_manifest.v1.json`

The AI agent must also consult proven governance authorities when needed:
- `governance/00_INDEX.md`
- `governance/00_MANIFEST.yaml`
- `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`
- `governance/02_REGISTRIES/C2_ENGINE_IDS_V1.md`
- `governance/02_REGISTRIES/C2_TRUTH_AUTHORITY_REGISTRY_V1.json`
- `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- `governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json`

## Reasoning Procedure

1. Prove repo root, governance root, and canonical truth root.
2. Load the system snapshot first.
3. Load the AI architecture index second.
4. Load the capability manifest before any capability analysis.
5. Verify every referenced path before making a path-based claim.
6. Classify every major conclusion as one of:
   - PROVEN_ACTIVE
   - PROVEN_PRESENT
   - PARTIALLY_PROVEN
   - UNKNOWN
   - DEPRECATED only where explicit repo evidence proves deprecation
7. For readiness analysis, use the execution lifecycle encoded in the system snapshot.
8. Where multiple versioned surfaces exist, prefer the active authority declared in proven registries.
9. Where no active authority is proven, state the ambiguity explicitly.

## Evidence Standard

Valid proof types are limited to:
- path exists
- schema exists
- contract exists
- artifact stream exists
- script references component
- report references component
- runtime artifact sample exists

Naming alone is not sufficient proof of behavior.

## Allowed Inference Boundary

Allowed inference is limited to:
- restating behavior that is directly encoded by a proven contract, registry, schema, path, or runtime artifact sample
- connecting adjacent lifecycle surfaces only when both sides are independently proven
- using declared active versions from proven registries to select preferred surfaces

Disallowed inference includes:
- inventing sleeves, engines, paths, contracts, or artifact streams
- assuming active behavior solely from source code presence
- treating quarantined or non-authoritative roots as canonical
- promoting present-but-unproven behavior into proven capability

## Unknown Handling

If evidence is insufficient, the AI agent must:
- mark the field or conclusion `UNKNOWN`, or
- omit the field when omission is cleaner and does not hide uncertainty

The AI agent must not use placeholders, examples, or speculative filler in authoritative outputs.

## Fail-Closed Rules

The AI agent must fail closed when:
- repo root is unproven
- canonical truth root is unproven
- a claim depends on an unverified path
- a capability exceeds proven evidence
- readiness analysis requires lifecycle steps not proven in runtime truth or governance authority
- an authoritative version cannot be determined from proven registries and the ambiguity is material

## Operating Constraints

The AI agent must treat:
- `constellation_2/runtime/truth/` as canonical system truth
- `constellation_2/runtime/truth_sleeves/<sleeve_id>/<mode>/` as canonical sleeve partition truth where proven
- quarantined, archived, deprecated, or non-authoritative roots as non-canonical unless an explicit authority contract states otherwise

Generated UTC: 2026-03-08T03:56:21Z
