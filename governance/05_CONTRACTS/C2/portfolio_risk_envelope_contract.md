# portfolio_risk_envelope_contract.md

Ownership:
- Portfolio Governance is the sole owner of the portfolio-wide effective risk envelope.

Inputs:
- governed capital authority policy manifest
- effective capital risk envelope output
- governed sleeve registry

Outputs:
- one `portfolio_governance_snapshot.v1` record per governed day

Invariants:
- portfolio-wide `risk_budget_total` and `risk_budget_effective` must be derived from governed upstream facts only
- drawdown scaling, correlation compression, hard stop state, and reserve buffer must be recorded explicitly
- the snapshot must be immutable and replayable for its day and snapshot identity

Failure model:
- fail closed if any required governed input is missing, malformed, or cannot be hashed deterministically
- do not emit a partially populated snapshot

Must-not-change rules:
- this contract does not change live allocation logic
- this contract does not grant or spend budget
- this contract does not replace canonical runtime ledger truth

Audit requirements:
- record policy hash and sleeve registry hash
- retain input manifest references sufficient for replay
- preserve deterministic snapshot identity
