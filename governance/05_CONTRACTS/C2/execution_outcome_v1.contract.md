# execution_outcome_v1

`execution_outcome_v1` is the explicit Execution Plane artifact.

It summarizes what actually ran for a given day and release:
- overall exit code
- stage pass/fail outcomes
- non-fatal items
- self-heal actions
- deferred statuses
- clean-run classification

It must not redefine readiness or policy.
It consumes already-produced evidence and execution-stage results and reports operational outcome only.
