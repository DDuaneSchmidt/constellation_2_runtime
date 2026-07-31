# Validation Vocabulary Alignment 001

Date: 2026-06-05
Status: DESIGN_ONLY
Mode: evaluation-only, offline-only

## Purpose

Rank possible approaches for aligning candidate vocabulary with direct validation vocabulary.

The immediate design problem is that candidate-facing labels and validation-facing labels may describe related concepts with different names or granularities. When validation requires exact label agreement, otherwise testable candidates can be blocked before replay samples are formed.

This document ranks alignment approaches by:

- validation unblock potential
- complexity
- authority risk

It does not implement vocabulary changes, change replay behavior, promote candidates, override qualification, override governance, recommend trades, allocate capital, size positions, execute through a broker, or authorize paper trading.

## Ranking Summary

| Rank | Approach | Validation unblock potential | Complexity | Authority risk | Overall assessment |
| ---: | --- | --- | --- | --- | --- |
| 1 | Translation Layer | High | Low to medium | Low | Best first design candidate. It directly addresses vocabulary mismatch while preserving validator authority. |
| 2 | Unknown/Unmappable Classification | Medium to high | Low | Very low | Best safety companion. It prevents false certainty and creates auditable failure categories. |
| 3 | Regime Crosswalk | High | Medium | Medium | Useful for regime-specific blockage, but easier to overinterpret as semantic truth. |
| 4 | Candidate-Specific Validation Modes | Medium | Medium to high | Medium to high | Can unblock targeted candidates, but risks special-case validation semantics. |
| 5 | Intraday Validation Path | Medium to high | High | Medium | Valuable for some mechanisms, but it expands data and replay surface beyond vocabulary alignment. |

## Evaluation Criteria

Validation unblock potential asks how directly the approach can turn a currently blocked validation path into an auditable replay path without weakening evidence requirements.

Complexity includes implementation complexity, manifest/test burden for any future implementation, reviewer burden, maintenance cost, and the chance of hidden coupling between candidate generation and validation.

Authority risk asks whether the approach could be misread as readiness, qualification, replay override, governance override, or permission to take trading action.

Lower authority risk is preferred even when unblock potential is similar.

## 1. Translation Layer

Design:

Introduce an explicit vocabulary translation layer between candidate artifacts and validation artifacts. Candidate terms remain candidate terms. Validator terms remain validator terms. The translation layer only maps between them for validation compatibility.

Example shape:

| Candidate vocabulary | Validation vocabulary | Mapping status |
| --- | --- | --- |
| `CHOP` | `RANGE_BOUND` | mapped |
| `TRENDING` | `TRENDING` | exact |
| `UNKNOWN` | `UNKNOWN` | exact |

Why it ranks first:

- directly targets the observed vocabulary mismatch
- keeps the validator from inventing meaning internally
- can be versioned, audited, and tested as a narrow boundary object
- does not require new replay mechanics
- supports explicit unmappable outcomes

Risks:

- overly broad mappings can hide real semantic differences
- stale mappings can become false compatibility
- reviewers may treat translated labels as stronger than they are

Controls:

- every mapping needs source and rationale
- mappings should be directional, not assumed symmetric
- translated values should be reported separately from original values
- unmapped values must not silently pass

Overall:

Best first approach because it has high unblock potential with relatively low complexity and low authority risk.

## 2. Unknown/Unmappable Classification

Design:

Add a controlled classification for cases where candidate vocabulary cannot be mapped to validation vocabulary. The result should distinguish real data insufficiency from vocabulary incompatibility.

Possible labels:

- `VOCABULARY_UNMAPPABLE`
- `REGIME_UNMAPPABLE`
- `TIMEFRAME_UNMAPPABLE`
- `MECHANISM_UNMAPPABLE`
- `UNKNOWN_ALIGNMENT`

Why it ranks second:

- prevents misleading `INSUFFICIENT_DATA` outcomes when data exists but vocabulary does not align
- lowers false certainty
- improves reviewer triage
- pairs naturally with a translation layer

Risks:

- does not by itself create replay samples
- can become a catch-all category if source evidence is weak
- may increase report surface without resolving validation

Controls:

- require original value, target vocabulary, attempted mapping, and failure reason
- distinguish unmappable from missing data
- keep unknown as a valid final result, not a defect to paper over

Overall:

Best safety companion to the translation layer. It has lower direct unblock potential than translation, but the lowest authority risk.

## 3. Regime Crosswalk

Design:

Create a dedicated crosswalk for regime terms only. This is narrower than a full translation layer and focuses on labels such as `CHOP`, `RANGE_BOUND`, `TRENDING`, `LOW_VOLATILITY`, and `HIGH_VOLATILITY`.

Why it ranks third:

- regime mismatch appears to be a major validation blocker
- narrow scope makes review easier than a general vocabulary system
- can produce immediate diagnostic value

Risks:

- regime terms are semantically loaded
- a crosswalk can be mistaken for proof that two regime classifiers are equivalent
- partial overlaps are difficult to encode as exact mappings

Controls:

- mark mappings as exact, compatible, partial, or unmappable
- require evidence for any non-exact compatibility
- report candidate regime, validation regime, and crosswalk result side by side
- avoid using crosswalk success as candidate readiness

Overall:

Strong targeted design option. It ranks below the general translation layer because it may solve only the current regime symptom while leaving other vocabulary mismatches unresolved.

## 4. Candidate-Specific Validation Modes

Design:

Allow validation to choose a mode based on candidate type, mechanism, or declared validation needs. A candidate could request a regime-tolerant, event-window, symbol-universe, or mechanism-specific validation mode.

Why it ranks fourth:

- can fit validation semantics more closely to candidate intent
- useful when a single validator path is too rigid
- may unblock candidates whose evidence requirements differ by mechanism

Risks:

- special-case modes can weaken comparability across candidates
- candidates may appear to choose favorable validation rules
- mode selection can become an implicit qualification decision
- significantly higher test and governance burden for future implementation

Controls:

- mode eligibility must be declared before validation
- mode selection must come from verified plan fields, not from desired outcome
- reports must show why the mode was selected
- no mode may bypass evidence, sample, replay, or authority requirements

Overall:

Potentially useful after vocabulary alignment is stable. It is not the best first move because it changes validation semantics rather than only aligning terms.

## 5. Intraday Validation Path

Design:

Add or define a validation path that can use intraday data for candidates whose mechanism depends on event windows, opening range behavior, close-to-close limitations, or same-day sequence effects.

Why it ranks fifth:

- can unblock candidates that daily bars cannot fairly test
- improves mechanism fidelity for intraday hypotheses
- may separate true vocabulary mismatch from timeframe mismatch

Risks:

- high data availability and quality burden
- higher leakage and timestamp-discipline risk
- substantially broader implementation surface
- can distract from the narrower vocabulary alignment issue

Controls:

- require explicit intraday need in the validation plan
- require timestamp, session, and data-source discipline
- keep intraday validation separate from vocabulary translation
- classify missing intraday data separately from failed mechanism support

Overall:

Important future design path, but not the first answer to vocabulary alignment. It should follow after unmappable classification and translation/crosswalk design clarify which candidates are blocked by labels versus data granularity.

## Recommended Sequencing

1. Design the translation layer contract.
2. Design unknown/unmappable classifications as mandatory outputs.
3. Add a regime crosswalk as the first concrete vocabulary family inside the translation layer.
4. Defer candidate-specific validation modes until alignment outcomes are measurable.
5. Treat intraday validation as a separate data-path design, not as the first vocabulary fix.

## Decision Rationale

The safest high-leverage design is a small, explicit boundary between candidate vocabulary and validator vocabulary. The validator should not silently infer that `CHOP` means `RANGE_BOUND`, but it also should not classify a mapped vocabulary mismatch as ordinary data insufficiency.

The key design principle is:

```text
Original vocabulary remains source truth.
Translated vocabulary is validation compatibility metadata.
Unmappable vocabulary is an auditable outcome.
No vocabulary alignment result grants readiness or authority.
```

## Authority Boundary

This document is design only. It does not implement changes, modify Aegis behavior, modify manifests, alter verified runtime truth, promote candidates, override replay, override qualification, override governance, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.

Any future implementation must follow the Aegis runtime truth rules, update affected manifests and tests, and pass audit before being treated as runtime behavior.
