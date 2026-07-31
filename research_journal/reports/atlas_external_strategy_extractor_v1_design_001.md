# Atlas External Strategy Extractor V1 Design 001

Objective: design a read-only Atlas extractor that converts external trading-strategy claims from YouTube, video, or blog sources into bounded `GENERATED_ONLY` hypothesis drafts.

Scope: design only. This report does not implement Atlas External Strategy Extractor V1, add architecture, add schemas, modify Atlas V1 tools, change retrieval, change synthesis, change governance, modify runtime truth, modify sleeves, modify candidates, create paper positions, create trade advice, or alter allocation logic.

## Design Classification

Classification: `V2_BUILD_LATER`.

Reason: Atlas V2 Backlog Review 001 prioritized packet quality, source noise filtering, lexical blind-spot reduction, and coverage confidence before broader external-source expansion. External strategy extraction is plausible as a later bounded drafting aid, but current evidence does not justify implementing it before V2's evidence-preparation quality work.

## Purpose

Atlas External Strategy Extractor V1 should help a human researcher convert an external creator's stated strategy claim into a structured research draft.

It should answer:
- What did the external source claim?
- What indicators, rules, market, and timeframe were explicitly stated?
- What would AEGIS need to test before treating the claim as evidence?
- Which prior failures should warn against over-trusting the claim?

It must not answer:
- Should this be traded?
- Is the creator correct?
- Is the strategy validated?
- Is this ready for a sleeve, paper position, capital review, or allocation?

## Inputs

Required input:
- YouTube, video, or blog URL.

First implementation supported input:
- Transcript or captions text.

Optional metadata:
- Video title.
- Video description.
- Source author or channel name.
- Publication date.

Explicitly out of V1 extraction scope:
- Screenshot or chart interpretation.
- Audio transcription generation.
- Video frame parsing.
- Comment-section mining.
- Backtest execution.
- Market-data lookup.
- Runtime artifact mutation.

If transcript or captions are unavailable, the extractor must return:

`TRANSCRIPT_REQUIRED`

No partial strategy extraction should be attempted from title, description, screenshots, thumbnails, or creator reputation alone.

## Outputs

The extractor should produce a deterministic, citation-bound Markdown or JSON-like brief with these fields:

- `source_url`
- `source_title`
- `source_description`
- `transcript_available`
- `status`
- `strategy_claim`
- `indicators_mentioned`
- `entry_rule`
- `exit_rule`
- `risk_rule`
- `market_timeframe`
- `implied_hypothesis`
- `required_data`
- `falsification_test`
- `related_prior_failures`
- `unsupported_or_missing_fields`
- `safety_boundary`

Allowed statuses:
- `EXTERNAL_CLAIM_EXTRACTED`
- `TRANSCRIPT_REQUIRED`

Required draft status:
- `GENERATED_ONLY`

The output must always state that the extracted claim is an unvalidated external claim and not an AEGIS finding.

## Extraction Rules

Use transcript evidence only.

Every extracted field must be traceable to transcript text or explicitly marked `NOT_STATED`.

Do not infer missing rules from common trading practice. For example:
- If the transcript says "buy when RSI is oversold" but gives no threshold, `entry_rule` should include the phrase and mark threshold `NOT_STATED`.
- If the transcript discusses an entry but not exits, `exit_rule` must be `NOT_STATED`.
- If the transcript says "risk small" without position sizing, stop, max loss, or invalidation detail, `risk_rule` should be `INSUFFICIENT_DETAIL`.
- If the transcript implies a market but never names one, `market_timeframe` should be `NOT_STATED`.

No creator credibility assumption is allowed.

No performance claim should be treated as evidence unless the transcript explicitly states the test, sample, period, market, method, and result. Even then, the status remains `GENERATED_ONLY`.

## Field Semantics

`strategy_claim`

Plain-language summary of the external claim. It should preserve uncertainty and attribution, for example: "The creator claims that a moving-average pullback setup can identify continuation entries in large-cap equities."

`indicators_mentioned`

Only indicators, signals, patterns, or data inputs explicitly named in the transcript. Examples: RSI, MACD, moving average, VWAP, earnings surprise, VIX, relative strength, volume breakout.

`entry_rule`

The entry condition claimed by the source. If incomplete, split into stated and missing parts.

`exit_rule`

The exit, profit-taking, stop, invalidation, time stop, or close condition claimed by the source. If absent, mark `NOT_STATED`.

`risk_rule`

Any stop, sizing, risk budget, loss limit, invalidation rule, or portfolio constraint explicitly stated. If the source only says "manage risk" or "use a stop" without specifics, mark `INSUFFICIENT_DETAIL`.

`market_timeframe`

The market, instrument class, symbol set, trading session, bar interval, or holding period explicitly stated. If the source mixes examples without a declared target market, mark that ambiguity.

`implied_hypothesis`

A falsifiable `GENERATED_ONLY` research draft. It should be phrased as a testable relationship, not a conclusion. Example: "For [market/timeframe], entries satisfying [stated condition] produce better forward risk-adjusted outcomes than a baseline entry over [test horizon]."

`required_data`

Minimum data required to test the claim. Include prices, volumes, indicators, corporate events, macro events, timestamps, survivorship-bias controls, benchmark/baseline data, transaction cost assumptions, and outcome labels when relevant.

`falsification_test`

A specific test that could disprove the implied hypothesis. It should include baseline comparison, out-of-sample or time-split testing when possible, and failure criteria such as no improvement over baseline, instability across regimes, or excess sensitivity to parameter choices.

`related_prior_failures`

Relevant Research Journal failures or cautions to check before using the draft. Default candidates:
- `FAIL_0004`: architecture maturity does not imply evidence maturity.
- `FAIL_0007`: candidate or signal volume does not imply candidate quality.
- `FAIL_0010`: capital review remained premature while evidence was underpowered.
- `FAIL_0016`: standalone technical-indicator claims remained unsupported, false-positive-prone, baseline-recoverable, or non-generalizing.

The extractor should include only failures that are relevant to the extracted claim and explain why they matter.

## Hard Boundaries

The extractor must not:
- Give trade advice.
- Validate the source claim.
- Claim the creator is correct.
- Create a sleeve.
- Create a candidate.
- Create a paper position.
- Create capital relevance.
- Recommend allocation.
- Recommend position size.
- Recommend entry or exit action.
- Modify governance.
- Modify runtime truth.
- Modify Research Journal objects.
- Modify validation samples.
- Modify candidate rules.
- Modify sleeve logic.
- Treat generated drafts as evidence.

All outputs must include:

`This is a GENERATED_ONLY external-claim draft. It is not trade advice, not validation, not a candidate, not a sleeve, not a paper position, and not capital-allocation evidence.`

## Workflow

1. Accept URL and optional metadata.
2. Check transcript or captions presence.
3. If transcript is missing, return `TRANSCRIPT_REQUIRED` and stop.
4. Extract only explicitly stated strategy details from transcript text.
5. Mark missing or ambiguous fields as `NOT_STATED` or `INSUFFICIENT_DETAIL`.
6. Draft a falsifiable `GENERATED_ONLY` hypothesis.
7. List required data and falsification test.
8. Attach related prior failures.
9. Emit a bounded source-cited brief.

## Output Template

```text
# Atlas External Strategy Extractor V1

Status: EXTERNAL_CLAIM_EXTRACTED
Draft status: GENERATED_ONLY

Source URL: <url>
Source title: <title or NOT_PROVIDED>
Transcript available: true

## Strategy Claim
<attributed transcript-only claim>

## Indicators Mentioned
- <indicator or NOT_STATED>

## Entry Rule
<rule or NOT_STATED>

## Exit Rule
<rule or NOT_STATED>

## Risk Rule
<rule, INSUFFICIENT_DETAIL, or NOT_STATED>

## Market / Timeframe
<market/timeframe or NOT_STATED>

## Implied Hypothesis
GENERATED_ONLY: <falsifiable hypothesis>

## Required Data
- <data requirement>

## Falsification Test
<test that could disprove the hypothesis>

## Related Prior Failures
- <FAIL_ID>: <why relevant>

## Unsupported Or Missing Fields
- <field>: <reason>

## Safety Boundary
This is a GENERATED_ONLY external-claim draft. It is not trade advice, not validation, not a candidate, not a sleeve, not a paper position, and not capital-allocation evidence.
```

Transcript-missing template:

```text
# Atlas External Strategy Extractor V1

Status: TRANSCRIPT_REQUIRED
Draft status: GENERATED_ONLY

Source URL: <url>
Source title: <title or NOT_PROVIDED>
Transcript available: false

No strategy extraction was performed because transcript or captions text was unavailable.

Safety boundary: no trade advice, no validation claim, no sleeve creation, no candidate creation, no paper position, no capital relevance, and no assumption that the creator is correct.
```

## Acceptance Criteria

Atlas External Strategy Extractor V1 is acceptable only if:
- It returns `TRANSCRIPT_REQUIRED` when transcript text is missing.
- It uses transcript text as the only source of strategy rules.
- It marks missing fields rather than inferring them.
- It outputs `GENERATED_ONLY` on every successful extraction.
- It includes prior failure cautions when relevant.
- It includes a falsification test and required data.
- It emits no trade advice, validation, sleeve, candidate, paper-position, or capital-readiness claim.
- It is deterministic for identical inputs.

## Non-Goals

V1 does not:
- Download YouTube videos.
- Generate transcripts from audio.
- Interpret screenshots or charts.
- Backtest the claim.
- Score creator quality.
- Score strategy quality.
- Search for supporting market evidence.
- Compare against live market conditions.
- Generate candidates.
- Generate paper trades.
- Modify any AEGIS or Atlas artifact.

## Risks

False authority risk:
- External strategy wording can sound actionable. The extractor must keep attribution and generated-only status visible.

Missing-rule risk:
- Many creator claims omit exits, stops, sample definitions, and market universes. Missing fields must be explicit.

Technical-indicator overreach risk:
- Prior Research Journal evidence found broad standalone technical-indicator claims weak. `FAIL_0016` should be attached when the source relies on broad indicator claims.

Capital-pressure risk:
- A clean extracted claim can look more mature than it is. `FAIL_0010` should be attached when a user tries to connect extracted claims to capital relevance.

Transcript-quality risk:
- Captions may be inaccurate. The extractor should include a transcript-quality note when transcript provenance is unclear.

## Final Recommendation

Do not implement Atlas External Strategy Extractor V1 until higher-priority Atlas V2 packet-quality work is complete.

When implemented, V1 should be transcript-only and `GENERATED_ONLY`. Its value is converting external claims into falsifiable research drafts with explicit missing evidence, required data, and prior failure cautions. It must never convert external claims into trade advice, validation, sleeves, candidates, paper positions, or capital relevance.
