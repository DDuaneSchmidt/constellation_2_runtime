# Aegis Future Enhancements v1

This document captures future-state Aegis enhancements that are not approved implementation work. Items here do not change runtime behavior, trading logic, broker execution, live trading, autonomous execution, canonical artifacts, or safety gates.

## Future Enhancement: Portfolio Capital Allocation Engine

Change Control record: `ACC-20260531-003`

Status: `FUTURE_ENHANCEMENT`

### Purpose

Determine when a validated sleeve or hypothesis should receive more, less, or no real portfolio capital.

This is separate from Research Capital Allocation. Research Capital Allocation decides where Aegis should spend research effort. Portfolio Capital Allocation would decide how much real brokerage capital, if any, should be assigned to a validated sleeve after sufficient evidence exists.

### Non-Goals

- Do not implement this now.
- Do not connect to IB.
- Do not automate live trading.
- Do not imply Aegis is ready for real capital.
- Do not allow research allocation recommendations to become capital allocation recommendations.
- Do not change trading logic, broker execution, live trading, autonomous execution, sleeve logic, candidate generation, canonical artifacts, or safety gates.

### Prerequisites

- Validated sleeve or hypothesis evidence.
- Sufficient closed outcomes.
- Sufficient validation samples.
- Statistical sufficiency proof.
- Paper-trading track record.
- Drawdown and failure-mode history.
- Oak Harvest compatibility review.
- Manual approval and manual execution operating model.

Required future-state flow:

```text
Hypothesis
-> Validation
-> Paper Track Record
-> Capital Eligibility
-> Portfolio Allocation Recommendation
-> Human Approval
-> Manual Execution
```

### Required Safety Controls

- Capital eligibility gate.
- Sleeve and hypothesis validation threshold.
- Maximum allocation limits.
- Drawdown limits.
- Confidence-adjusted sizing.
- Diversification constraints.
- Correlation checks.
- Live pilot limits.
- Kill switch.
- Manual approval requirement.
- Oak Harvest compatibility note.
- Explicit no-autonomous-IB-execution policy.

### Relationship To Research Capital Allocation

Research Capital Allocation is research-attention-only. It may prioritize candidate-generation attention, research queue priority, compute effort, review attention, paper-testing bandwidth, or hypothesis development effort.

Portfolio Capital Allocation must not consume Research Capital Allocation output as permission to allocate real money. A future Portfolio Capital Allocation Engine would require its own capital eligibility gate and evidence-backed paper track record before producing any read-only recommendation.

### Future Success Criteria

- The engine remains read-only.
- Capital recommendations require human approval.
- Execution remains manual.
- Recommendations are backed by validation, closed outcomes, statistical sufficiency, and paper-trading evidence.
- Allocation sizing respects maximum allocation, drawdown, diversification, correlation, and pilot limits.
- Oak Harvest compatibility is explicit.
- Kill switch behavior is defined and tested.
- No autonomous IB execution path exists.

### Explicit Out-Of-Scope Items

- Broker execution.
- IB connection.
- Live trading automation.
- Autonomous execution.
- Safety-gate relaxation.
- Canonical artifact rule changes.
- Real capital readiness claims.
- Research allocation promotion into capital allocation.
