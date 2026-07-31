# Aegis Sleeve Throughput Diagnostics Requirements V1

## Goal

Produce a deterministic, read-only audit that explains per sleeve where flow stops across:

Hypothesis -> Signal -> Candidate -> Candidate Contract -> Paper Observation -> Outcome -> Validation Sample.

## Requirements

- Emit `aegis_sleeve_throughput_diagnostics_v1` under `truth/reports/aegis_sleeve_throughput_diagnostics_v1/<TARGET_DAY>/`.
- Include every known sleeve from research mapping rules, research portfolio sleeve implementations, and generated-hypothesis throughput rows.
- Count raw signals, candidates, valid candidate contracts, candidate rejections, paper observations, outcomes, and validation samples.
- Classify each sleeve as `FLOWING`, `DORMANT`, `BLOCKED`, or `UNDERPRODUCING`.
- For every non-flowing sleeve, emit the furthest stage reached, blocker stage, blocker code, blocker reason, owner, and whether David action is required.
- Produce portfolio totals and rankings for most evidence, most candidates, most blocked, and least active.
- Answer why only a minority of sleeves are producing candidates using deterministic evidence from the counted rows.

## Non-Goals

- No sleeve mutation.
- No candidate creation or repair.
- No quality, allocation, paper, outcome, validation, or broker mutation.
- No fabricated evidence.
