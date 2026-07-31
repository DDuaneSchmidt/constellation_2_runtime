# Aegis Research Screen Product Specification

## Purpose

The Research screen answers:

```text
What is Aegis investigating, what has it learned, what is blocked, and what happens next?
```

Research is the operator surface for investigations, hypotheses, experiment status, findings, research blockers, and next research steps.

Research is not:

* candidate workflow
* position workflow
* performance reporting
* system diagnostics
* trade execution
* trade advice

## Primary Operator Questions

### What investigations exist?

Required data:

* investigation or hypothesis count
* active hypotheses
* completed hypotheses
* blocked hypotheses
* archived/dismissed hypotheses if relevant
* source day and generated timestamp

Source system:

* research console
* research doctor
* hypothesis registry / research hypothesis artifacts
* research review brief artifact
* hypothesis qualification artifact

Update frequency:

* after research scheduler/doctor runs
* after qualification/review brief generation
* after validation sample update
* after operator research decision

Empty state:

```text
No active research investigations are recorded today.
```

Blocked state:

```text
Research status is unavailable because hypothesis or research run evidence is missing.
```

Degraded state:

```text
Research is visible, but some findings or evidence freshness details are incomplete.
```

User action state:

* only if a research decision is explicitly required, such as review a brief, dismiss, monitor, archive, or request more research.
* no trading or broker action appears.

### What is currently being researched?

Required data:

* active research/hypothesis rows
* autonomous state
* evidence collection state
* sample progress where applicable
* last attempted run
* next expected research step

Source system:

* research doctor
* research validation samples
* hypothesis qualification
* paper-testing sleeve report as validation context

Update frequency:

* after autonomous research scheduler/doctor
* after research test execution
* after evidence sample accumulation

Empty state:

```text
No research is currently active.
```

Blocked state:

```text
Research cannot progress because required evidence or runner support is missing.
```

Degraded state:

```text
Research is progressing, but next timing or sample status is incomplete.
```

User action state:

* none when research is collecting evidence automatically.
* operator action appears only when the next step is truly a user decision.

### What hypotheses are active?

Required data:

* hypothesis id hidden in details
* operator-facing title
* state label
* affected symbols
* confidence or qualification state
* evidence progress
* blocker reason if blocked
* next action

Source system:

* hypothesis registry / research hypothesis
* research doctor
* research validation samples
* hypothesis qualification
* research review brief

Update frequency:

* after research lifecycle transitions
* after validation sample updates
* after brief/qualification generation

Empty state:

```text
No active hypotheses are recorded.
```

Blocked state:

```text
Hypothesis state cannot be trusted because required research evidence is missing or stale.
```

Degraded state:

```text
Hypotheses are listed, but some evidence or next-step details are incomplete.
```

User action state:

* review brief
* monitor
* dismiss
* archive
* request more research

### What has been learned?

Required data:

* research findings
* conclusion
* confidence
* key evidence
* counter-evidence
* risks/caveats
* decision needed

Source system:

* research review brief
* research result ledger
* research conclusion
* hypothesis qualification

Update frequency:

* after research brief generation
* after result ledger update
* after qualification evaluation

Empty state:

```text
No research findings are ready yet.
```

Blocked state:

```text
Findings cannot be shown because the review brief or required structured fields are missing.
```

Degraded state:

```text
Findings are available, but confidence or evidence coverage is limited.
```

User action state:

* review, monitor, dismiss, archive, or request more research.

### What is blocked?

Required data:

* blocked hypothesis count
* blocker reason
* missing evidence or source
* whether waiting on time/data/system/operator
* repair or next research action

Source system:

* research doctor
* hypothesis qualification
* research validation self-check
* research review brief diagnostics
* System Health summary as repair owner

Update frequency:

* after research doctor/self-check
* after evidence generation
* after System Health refresh

Empty state:

```text
No research blockers are reported.
```

Blocked state:

```text
Research status cannot be evaluated.
```

Degraded state:

```text
Blockers are summarized, but some repair ownership is incomplete.
```

User action state:

* only if blocker explicitly requires operator decision.
* system/data blockers point to System Health, not manual research action.

### What happens next?

Required data:

* next research run
* next expected sample
* expected completion condition
* next qualification/review step
* operator expectation

Source system:

* research doctor
* research validation samples
* hypothesis qualification
* scheduler/automation state

Update frequency:

* after research doctor
* after sample accumulation
* after scheduler update

Empty state:

```text
No next research step is scheduled.
```

Blocked state:

```text
Next research step cannot be determined from current evidence.
```

Degraded state:

```text
Next research step is known, but timing is approximate.
```

User action state:

* none if Aegis is waiting for evidence or the next sample.
* user action only if a decision is required.

### Do I need to do anything?

Required data:

* operator action required count
* action type
* reason
* next step
* allowed actions

Source system:

* research doctor
* research review brief
* hypothesis qualification
* operator decision ledger if present

Update frequency:

* after research state transition
* after brief generation
* after operator decision

Empty state:

```text
No research action is required.
```

Blocked state:

```text
Research action state is unavailable.
```

Degraded state:

```text
Research action state is visible, but some detail is incomplete.
```

User action state:

* show only research decisions.
* never show candidate capture, broker, trading, or position actions.

## Proposed Screen Sections

### Section 1: Research Summary

Purpose: give the first visible answer to what research exists and whether action is required.

Operator question answered: What research exists, and do I need to do anything?

Required data:

* active investigations
* collecting evidence count
* findings ready count
* blocked count
* operator action count
* one-line next step

Value score: 10

Classification: NEW

Justification: current Research screens are split across link hub, Lab, and Review. The rebuilt screen needs one first answer.

### Section 2: Active Research

Purpose: show hypotheses or investigations currently researching, waiting for samples, or running tests.

Operator question answered: What is currently being researched?

Required data:

* title
* state label
* sample progress
* next expected sample/run
* operator action required flag

Value score: 9

Classification: IMPROVE

Justification: current Research Lab cards have useful data but need clearer hierarchy and less internal language.

### Section 3: Findings Ready

Purpose: show findings that have operator-readable conclusions.

Operator question answered: What has been learned?

Required data:

* title
* conclusion
* confidence
* why it matters
* allowed research actions

Value score: 8

Classification: IMPROVE

Justification: Research Review brief cards provide value, but artifact headers and raw labels should move behind details.

### Section 4: Research Blockers

Purpose: show only research blockers that affect progress.

Operator question answered: What is blocked?

Required data:

* blocker reason
* affected hypothesis
* waiting on data/time/system/operator
* next step

Value score: 8

Classification: NEW

Justification: current blocked states are scattered and can read as generic waiting/manual review.

### Section 5: Next Research Step

Purpose: answer what happens next and when.

Operator question answered: What happens next?

Required data:

* next run/sample/qualification step
* expected completion condition
* operator expectation

Value score: 9

Classification: NEW

Justification: prior audits identified missing next expected observation and unclear autonomous progress.

### Section 6: Collapsed Evidence

Purpose: allow verification without making raw artifacts the main experience.

Operator question answered: Why should I trust this research state?

Required data:

* source artifacts
* generated_at/as_of
* source day
* evidence freshness

Value score: 6

Classification: IMPROVE

Justification: evidence improves trust, but raw schema/paths should stay collapsed.

## State Model

### NORMAL

Visible message:

```text
Research is active. Aegis is monitoring investigations and collecting evidence.
```

Operator expectation: monitor; review only if a finding is ready.

Components shown: Research Summary, Active Research, Findings Ready if present, Next Research Step, collapsed Evidence.

Components hidden: broker/trading actions, candidate capture, position/P&L details.

### NO_RESEARCH

Visible message:

```text
No active research investigations are recorded today.
```

Operator expectation: no research action required.

Components shown: Research Summary, empty Active Research, Next Research Step if known.

Components hidden: findings, blocker table unless evidence explains unavailable state.

### RESEARCH_RUNNING

Visible message:

```text
Research is running or collecting evidence.
```

Operator expectation: no user action unless explicitly stated.

Components shown: Active Research, sample/run progress, next expected observation.

Components hidden: manual review prompts unless a decision is actually required.

### RESEARCH_BLOCKED

Visible message:

```text
Research cannot progress because required evidence, runner support, or source data is unavailable.
```

Operator expectation: use System Health or follow the named research next step.

Components shown: Research Blockers, impact, next step, collapsed Evidence.

Components hidden: findings as current truth when required evidence is missing.

### RESEARCH_COMPLETE

Visible message:

```text
Research findings are ready.
```

Operator expectation: review finding, monitor, dismiss, archive, or request more research.

Components shown: Findings Ready, conclusion, confidence, evidence summary, allowed research actions.

Components hidden: candidate/position/trade actions.

### NEEDS_USER_ACTION

Visible message:

```text
Research needs an operator decision.
```

Operator expectation: take a research-only decision.

Components shown: Research Summary, Action Required, relevant finding/hypothesis.

Components hidden: system-only blockers from the action queue unless they truly require user action.

## Screenshot Acceptance

A non-engineer must be able to answer:

* What research exists?
* What is currently active?
* What has been learned?
* What is blocked?
* What happens next?
* Do I need to do anything?

Acceptance fails if the screen primarily shows:

* route/link hub cards
* raw artifact names
* internal status labels as the first answer
* candidate capture content
* open positions or P&L details
* performance dashboards
* generic “Recommendation Ready” without brief content or validation state
* “Manual review required” when Aegis should continue autonomous validation

## Components That Must Never Appear On Research

* open positions table
* holdings/exposure cards
* candidate review/capture controls
* candidate qualification tables as primary content
* portfolio P&L or sleeve performance scorecards
* raw System Health diagnostics as primary content
* broker submit/transmit/live trading controls
* trade advice language

## Top 10 Most Important Elements On Research

1. Research state summary.
2. Operator action required: yes/no.
3. Active investigations count.
4. Active hypothesis cards with plain state labels.
5. Evidence collection progress.
6. Findings ready count and brief previews.
7. Research blocker summary.
8. Next research step and timing/condition.
9. Confidence/evidence quality summary for findings.
10. Collapsed source evidence.
