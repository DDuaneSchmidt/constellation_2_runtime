# AI Hypothesis Output Prompt V1

> Source-only note: This top-level docs file is for human authoring workflow and is not required in release artifacts at runtime.

## Input Material
1. Read `research_ai_packet.md` for the target day.
2. Use only evidence and constraints in that packet.

## Output Contract
- Output only JSON matching `ai_hypothesis_batch.v1`.
- Do not output prose, markdown, or explanations.
- Prefer provider features equivalent to Structured Outputs / JSON Schema-constrained output when available.
- Regardless of provider capability, output must validate against the schema.

## Required JSON Shape
- `schema_version` must equal `ai_hypothesis_batch.v1`.
- `source_packet_path` must reference the source packet file.
- `generated_utc` must be UTC timestamp text.
- `hypotheses` must be an array of hypothesis objects.

Each hypothesis must include:
- `idea_id` (`EDGE-YYYY-NNNN`)
- `hypothesis`
- `expected_edge_mechanism`
- `market`
- `edge_type`
- `instruments`
- `features_required`
- `data_required`
- `test_design` with in-sample and out-of-sample periods
- `success_criteria`
- `rejection_criteria`
- `risk_notes`
- `forbidden_if`

## Safety Constraints
- No live trade recommendations.
- No paper promotion recommendations.
- No requests to override gates.
- No requests to modify risk or capital settings.
- No requests to modify Aegis Core.

## Research Constraints
- Every hypothesis must be sandbox-testable.
- Every hypothesis must include explicit success criteria.
- Every hypothesis must include explicit rejection criteria.
- Every hypothesis must include test design with cost/slippage and walk-forward requirements.
- Every idea must be rejectable from its own criteria.
