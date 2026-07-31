# Validation Vocabulary Bridge v0.1

Date: 2026-06-05
Status: DESIGN_ONLY
Mode: GENERATED_ONLY

## Purpose

Define a design-only vocabulary bridge between candidate-facing regime labels and validation-facing regime labels.

Architecture:

```text
Candidate Regime
  -> Vocabulary Bridge
  -> Validation Regime
```

The bridge is a compatibility description only. It does not run replay, alter replay filters, qualify candidates, promote candidates, authorize governance changes, or create validation authority.

## Hard Boundaries

- `GENERATED_ONLY`
- No replay authority.
- No qualification authority.
- No candidate authority.
- No governance authority.
- No paper placement authority.
- No trade advice.
- No broker execution.
- No capital allocation.
- No position sizing.

Consumers must treat bridge output as compatibility metadata, not truth about market behavior.

## Validation Regime Vocabulary

Current daily validation regime labels:

- `HIGH_VOLATILITY`
- `TRENDING`
- `RANGE_BOUND`
- `LOW_VOLATILITY`
- `UNKNOWN`

The current validation classifier does not emit `CHOP`, `TREND`, `BREAKOUT`, `MEAN_REVERSION`, `REVERSAL`, or `EVENT_REACTION`.

## Mapping Types

| mapping_type | Meaning |
| --- | --- |
| `EXACT_MATCH` | Candidate regime and validation regime use the same label with compatible semantics. |
| `PARTIAL_MATCH` | Candidate regime can be approximated by a validation regime, but semantic loss or ambiguity remains. |
| `UNMAPPABLE` | Input is not a valid candidate regime or has no safe validation-regime target. |
| `UNKNOWN` | Insufficient evidence to map safely; must not pass silently. |

## Output Model

Each bridge row has this shape:

```json
{
  "candidate_regime": "CHOP",
  "validation_regime": "RANGE_BOUND",
  "mapping_type": "PARTIAL_MATCH",
  "confidence": 0.75,
  "reasoning": "CHOP is closest to range-bound daily behavior, but the validation classifier does not emit CHOP and daily bars cannot prove intraday chop."
}
```

Confidence is a design confidence between `0.0` and `1.0`. It is not a statistical probability and must not be used as a replay weight without a separate governed design.

## Bridge Table

| candidate_regime | target validation regimes | mapping_type | confidence | rationale | ambiguity |
| --- | --- | --- | ---: | --- | --- |
| `TRENDING` | `TRENDING` | `EXACT_MATCH` | 0.95 | Candidate and validation vocabularies both contain `TRENDING`. | Still depends on the validation classifier's daily-bar trend definition; triggered samples may also occur in non-trending daily regimes. |
| `TREND` | `TRENDING` | `PARTIAL_MATCH` | 0.70 | `TREND` is a common shorthand for `TRENDING`. | Alias may refer to trend direction, trend strength, or trend-continuation mechanism rather than regime. |
| `CHOP` | `RANGE_BOUND` | `PARTIAL_MATCH` | 0.75 | `RANGE_BOUND` is the closest daily validation label for non-trending, bounded price action. | `CHOP` can imply noisy intraday two-way movement, while `RANGE_BOUND` is daily close-to-SMA proximity. |
| `CHOP` | `LOW_VOLATILITY` | `PARTIAL_MATCH` | 0.45 | Some chop regimes are low-volatility and mean-reverting. | Chop can also be high-volatility whipsaw; low volatility is not sufficient evidence of chop. |
| `CHOP` | `UNKNOWN` | `UNKNOWN` | 0.25 | `UNKNOWN` may preserve samples where the classifier cannot identify trend, range, or volatility regime. | `UNKNOWN` is not positive evidence of chop; using it as a pass-through risks overfitting and semantic dilution. |
| `RANGE_BOUND` | `RANGE_BOUND` | `EXACT_MATCH` | 0.90 | Candidate label directly matches validation label. | Exact label match still inherits the classifier's daily-bar definition. |
| `LOW_VOLATILITY` | `LOW_VOLATILITY` | `EXACT_MATCH` | 0.90 | Candidate label directly matches validation label. | Low volatility does not imply range-bound or mean-reverting behavior. |
| `HIGH_VOLATILITY` | `HIGH_VOLATILITY` | `EXACT_MATCH` | 0.90 | Candidate label directly matches validation label. | High volatility can include trend, event, reversal, or whipsaw states. |
| `UNKNOWN` | `UNKNOWN` | `UNKNOWN` | 0.50 | Candidate explicitly declares no reliable regime. | Should disable strict regime filtering only if the validation plan separately permits unconstrained-regime testing. |
| `BREAKOUT` | none | `UNMAPPABLE` | 0.95 | `BREAKOUT` is a mechanism label, not a regime label. | If a future candidate uses breakout as a regime, it needs a new candidate-regime definition before mapping. |
| `MEAN_REVERSION` | none | `UNMAPPABLE` | 0.95 | `MEAN_REVERSION` is a mechanism label, not a regime label. | Mean reversion can occur inside multiple regimes and cannot safely map to one validation regime. |
| `REVERSAL` | none | `UNMAPPABLE` | 0.95 | `REVERSAL` is a mechanism label, not a regime label. | Reversal describes transition behavior, not a stable regime. |
| `EVENT_REACTION` | none | `UNMAPPABLE` | 0.95 | `EVENT_REACTION` is a mechanism label requiring event metadata and timing, not a regime. | Event reaction can happen in any regime; mapping it as a regime would erase the event requirement. |
| empty or missing | none | `UNKNOWN` | 0.20 | No candidate regime was declared. | Missing regime may mean unconstrained, omitted, or malformed metadata; bridge cannot infer intent. |
| any unrecognized label | none | `UNKNOWN` | 0.20 | Label is outside the known candidate-regime vocabulary. | Could be a valid future regime, a typo, a mechanism, or free text. |

## Output Rows

Canonical generated-only rows:

```json
[
  {
    "candidate_regime": "TRENDING",
    "validation_regime": "TRENDING",
    "mapping_type": "EXACT_MATCH",
    "confidence": 0.95,
    "reasoning": "Candidate and validation vocabularies both contain TRENDING; compatibility is label-exact, subject to the validation classifier's daily-bar trend definition."
  },
  {
    "candidate_regime": "TREND",
    "validation_regime": "TRENDING",
    "mapping_type": "PARTIAL_MATCH",
    "confidence": 0.70,
    "reasoning": "TREND is a plausible alias for TRENDING but may also describe mechanism direction or strength."
  },
  {
    "candidate_regime": "CHOP",
    "validation_regime": "RANGE_BOUND",
    "mapping_type": "PARTIAL_MATCH",
    "confidence": 0.75,
    "reasoning": "RANGE_BOUND is the closest daily validation label for bounded, non-trending price action, but it does not prove intraday chop."
  },
  {
    "candidate_regime": "CHOP",
    "validation_regime": "LOW_VOLATILITY",
    "mapping_type": "PARTIAL_MATCH",
    "confidence": 0.45,
    "reasoning": "LOW_VOLATILITY can overlap with quiet chop but misses high-volatility whipsaw chop."
  },
  {
    "candidate_regime": "CHOP",
    "validation_regime": "UNKNOWN",
    "mapping_type": "UNKNOWN",
    "confidence": 0.25,
    "reasoning": "UNKNOWN may preserve classifier-ambiguous samples, but it is not positive evidence of CHOP."
  },
  {
    "candidate_regime": "RANGE_BOUND",
    "validation_regime": "RANGE_BOUND",
    "mapping_type": "EXACT_MATCH",
    "confidence": 0.90,
    "reasoning": "Candidate label directly matches a validation label."
  },
  {
    "candidate_regime": "LOW_VOLATILITY",
    "validation_regime": "LOW_VOLATILITY",
    "mapping_type": "EXACT_MATCH",
    "confidence": 0.90,
    "reasoning": "Candidate label directly matches a validation label."
  },
  {
    "candidate_regime": "HIGH_VOLATILITY",
    "validation_regime": "HIGH_VOLATILITY",
    "mapping_type": "EXACT_MATCH",
    "confidence": 0.90,
    "reasoning": "Candidate label directly matches a validation label."
  },
  {
    "candidate_regime": "UNKNOWN",
    "validation_regime": "UNKNOWN",
    "mapping_type": "UNKNOWN",
    "confidence": 0.50,
    "reasoning": "Candidate declares no reliable regime; this must remain explicit and cannot silently grant regime compatibility."
  },
  {
    "candidate_regime": "BREAKOUT",
    "validation_regime": null,
    "mapping_type": "UNMAPPABLE",
    "confidence": 0.95,
    "reasoning": "BREAKOUT is a mechanism label, not a regime label."
  },
  {
    "candidate_regime": "MEAN_REVERSION",
    "validation_regime": null,
    "mapping_type": "UNMAPPABLE",
    "confidence": 0.95,
    "reasoning": "MEAN_REVERSION is a mechanism label, not a regime label."
  },
  {
    "candidate_regime": "REVERSAL",
    "validation_regime": null,
    "mapping_type": "UNMAPPABLE",
    "confidence": 0.95,
    "reasoning": "REVERSAL is a mechanism label, not a regime label."
  },
  {
    "candidate_regime": "EVENT_REACTION",
    "validation_regime": null,
    "mapping_type": "UNMAPPABLE",
    "confidence": 0.95,
    "reasoning": "EVENT_REACTION is a mechanism label and requires event metadata rather than regime translation."
  }
]
```

## Bridge Decision Rules

1. Preserve original candidate regime in every output.
2. Emit one row per candidate-regime to validation-regime target.
3. Use `EXACT_MATCH` only when the same label exists in both vocabularies and semantics are compatible.
4. Use `PARTIAL_MATCH` when a target is useful for diagnostics or approximation but loses meaning.
5. Use `UNMAPPABLE` when the input is a mechanism, action, event type, or otherwise not a regime.
6. Use `UNKNOWN` when evidence is insufficient, input is missing, or the bridge cannot safely decide.
7. Never turn a bridge row into a replay pass/fail decision without a separate governed implementation.
8. Never collapse multiple partial targets into a single hidden pass condition.

## Benefits

- Makes vocabulary mismatch explicit before samples are silently filtered.
- Separates candidate language from validation language.
- Distinguishes data insufficiency from semantic incompatibility.
- Gives reviewers a stable object to audit.
- Enables safer future diagnostics for `CHOP` without changing replay behavior.
- Supports generated-only planning for future validation vocabulary work.

## Risks

- Partial matches can be overread as proof.
- A bridge can become stale as candidate or validation vocabularies change.
- Confidence scores may be mistaken for statistical evidence.
- Multiple partial mappings can inflate apparent support if combined naively.
- Mapping `UNKNOWN` can become a loophole if treated as permissive.
- Misfiled mechanism labels may hide upstream candidate-metadata defects.

## Failure Modes

- `CHOP -> RANGE_BOUND` is treated as exact validation rather than approximation.
- `CHOP -> LOW_VOLATILITY` misses high-volatility whipsaw behavior.
- `UNKNOWN` is used as a pass-through instead of a review state.
- Mechanism labels such as `BREAKOUT` or `EVENT_REACTION` are accepted as regimes.
- A candidate receives replay or qualification authority because a bridge row exists.
- Bridge output is consumed without preserving the original candidate regime.
- Confidence values are tuned after outcomes are known.
- Bridge rows are changed without versioning or audit.

## Retirement Criteria

Retire or replace this bridge when any of the following is true:

- Candidate artifacts and validation artifacts share a single governed regime vocabulary.
- The validation classifier emits `CHOP` with a tested definition.
- A governed intraday validation regime model supersedes daily approximation for `CHOP`.
- Event-reaction candidates move to an event-aware validation path where regime mapping is no longer the primary blocker.
- Bridge mappings repeatedly cause reviewer confusion or false readiness claims.
- A future implementation introduces a versioned, tested, manifest-backed vocabulary contract.

## Acceptance Criteria For Future Implementation

This document does not authorize implementation. If implementation is later requested, minimum acceptance criteria should include:

- schema for bridge rows
- tests for `EXACT_MATCH`, `PARTIAL_MATCH`, `UNMAPPABLE`, and `UNKNOWN`
- explicit preservation of original candidate regime
- explicit report of mapped validation regimes
- no replay change without separate approval
- no qualification change without separate approval
- Aegis manifest updates if Aegis behavior, evidence, commands, policies, or UI surfaces change
- full Aegis audit after changes

## Authority Boundary

This document is design-only and `GENERATED_ONLY`. It does not implement a bridge, modify replay, modify validation, modify governance, qualify candidates, promote candidates, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, or alter safety gates.

