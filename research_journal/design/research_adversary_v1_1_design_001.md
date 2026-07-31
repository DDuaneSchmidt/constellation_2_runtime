# Research Adversary V1.1 Design 001

## Purpose

Research Adversary V1.1 is an Atlas-specific generated-only critique layer. Its job is to challenge Atlas research artifacts before they advance through review by surfacing known failure modes, recurring blockers, hidden assumptions, weak constraints, and duplicate idea clusters.

It is not a decision engine. It does not approve, reject, promote, override, allocate, qualify, or govern any artifact. It produces structured critique and review prompts only.

## Scope

Research Adversary V1.1 reviews Atlas research objects such as mechanism proposals, generated hypotheses, observation clusters, paper-forward candidate packets, failure reports, and research journal evidence.

The output remains `GENERATED_ONLY` and must be treated as advisory research critique requiring human review.

## New Capabilities

### Atlas Failure Taxonomy Lookup

The adversary should map reviewed artifacts to Atlas failure taxonomy entries.

Expected behavior:
- Identify relevant known failure categories.
- Link each category to the reviewed mechanism, assumption, constraint, or evidence gap.
- Avoid inventing taxonomy entries when no match exists.
- Mark uncertain matches as low confidence.

### Failure Pattern Matching

The adversary should compare the reviewed artifact against prior Atlas failure patterns.

Expected behavior:
- Detect similarities to prior failed mechanisms, hypotheses, or paper-forward packets.
- Explain why the prior pattern may apply.
- Distinguish direct matches from weak analogies.
- Preserve source lineage to the prior failure pattern.

### Recurring Blocker Detection

The adversary should identify blockers that have repeatedly appeared in Atlas workflows.

Expected behavior:
- Flag recurring blockers such as missing source data, weak replay evidence, unresolved regime constraints, duplicate candidate logic, or incomplete paper-forward readiness.
- Report blocker frequency where available.
- Separate current blockers from historical blockers that may no longer apply.

### Recurring Assumption Detection

The adversary should detect assumptions that recur across Atlas research and often remain under-tested.

Expected behavior:
- Extract explicit and implicit assumptions.
- Match assumptions to prior recurring assumption clusters.
- Identify assumptions requiring falsification before further review.
- Avoid treating assumptions as evidence.

### Duplicate-Cluster Detection

The adversary should compare the reviewed artifact against existing Atlas mechanism, claim, hypothesis, and observation clusters.

Expected behavior:
- Identify possible duplicate or near-duplicate clusters.
- Explain overlap by mechanism, regime, source lineage, and claim text.
- Distinguish duplicate research from legitimate independent confirmation.
- Require human review before any duplicate disposition is applied.

## Explicitly Forbidden Authority

Research Adversary V1.1 must not have:

- Replay authority.
- Qualification authority.
- Candidate authority.
- Governance authority.
- Capital authority.

It must not:
- Override replay results.
- Override edge qualification.
- Create or promote candidates.
- Approve governance status.
- Allocate capital.
- Recommend trades.
- Authorize live trading, broker execution, paper placement, or position sizing.

## Evidence Status

All Research Adversary V1.1 outputs must remain:

`GENERATED_ONLY`

Generated-only critique can inform human review, but it cannot be treated as validation, certification, replay evidence, qualification evidence, or governance approval.

## Expected Improvements

### Failure Recall

Expected improvement: increase the share of relevant known Atlas failure modes surfaced during review.

Target effect:
- More failure modes identified before paper-forward review.
- Fewer repeated failures advancing without explicit acknowledgement.

### Assumption Recall

Expected improvement: increase the share of material assumptions extracted from mechanism proposals and hypothesis packets.

Target effect:
- Fewer hidden assumptions entering replay or paper-forward evaluation.
- More assumptions converted into falsification tests.

### Constraint Recall

Expected improvement: increase detection of material constraints, especially regime, data, source-lineage, and applicability constraints.

Target effect:
- Clearer boundaries for when a hypothesis should not be trusted.
- Fewer overbroad claims reaching candidate review.

### Research Hours Saved

Expected improvement: reduce reviewer time spent rediscovering known blockers, duplicate clusters, recurring assumptions, and prior failure patterns.

Target effect:
- Faster triage.
- More reviewer time spent on judgment rather than retrieval.
- Lower rework from missed historical context.

### Paper-Forward Survival

Expected improvement: improve the survival rate of paper-forward items by filtering or redesigning weak ideas earlier.

Target effect:
- Fewer paper-forward items fail for already-known reasons.
- More surviving items have explicit risk, assumption, and falsification context.

## Advancement Rule

Research Adversary V1.1 should advance only if measured usefulness exceeds review burden. It should be retired or redesigned if it increases reviewer workload, creates excessive false positives, misses recurring Atlas-specific failures, or produces any forbidden authority behavior.
