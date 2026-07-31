# Aegis Research Daily Scorecard Design v1

## Architecture
The scorecard is a deterministic read model layered above existing Aegis truth artifacts. It does not create new research state. The flow is:

Existing authoritative artifacts -> Research Daily Scorecard -> Command Center read model -> UI renderer.

## Data Access
The builder reads day-scoped JSON artifacts from the runtime truth root. It also reads the prior available day when present to compute deltas. If no prior comparable artifact exists, deltas default to today's total counts so the result remains useful after the first run.

## Delta Strategy
Delta metrics are calculated from stable count fields in existing artifacts. The scorecard avoids comparing generated timestamps and avoids deriving decisions from UI state. Generated hypothesis advancement is detected by comparing generated-hypothesis throughput rows for the target day against the prior available throughput artifact.

## Command Center Integration
The operator cockpit API attaches `research_daily_scorecard` and its source path to the payload. The browser renders the scorecard above existing daily cards and uses backend fields directly for status, message, metrics, action count, and bottleneck.

## Safety Boundary
The builder is read-only and produces a JSON report. It does not call candidate producers, create candidates, create paper observations, close outcomes, alter validation samples, mutate allocation weights, change David action queues, or change any broker/trading safety gate.

## Test Strategy
Unit tests seed minimal authoritative artifacts to prove artifact production, status precedence, validation deltas, generated-hypothesis advancement, David action surfacing, audit blocker behavior, and safety-gate invariants. UI tests assert the Command Center renders `TODAY'S RESEARCH RESULT` above detailed cards and uses scorecard text from the backend.


## Operator Decision Dashboard Integration
For Operator Decision Dashboard v1, the scorecard is the primary source for the top Command Center answer. The Command Center must render `daily_progress_status`, `primary_message`, `latest_run_time`, `run_status`, daily deltas, David action summary, generated hypothesis progress, validation progress, and primary bottleneck directly from `aegis_research_daily_scorecard_v1`.

The scorecard remains a read-only artifact. It does not become a workflow authority for candidate generation, paper lifecycle, validation, allocation mutation, retirement, broker behavior, trading behavior, or safety gates.

## Oil Shock Blocker Source Precedence
Oil Shock candidate-flow blockers are not inferred by the scorecard. The builder reads `aegis_oil_shock_candidate_flow_v1` as an authoritative source and overlays its Oil Shock blocker fields onto `generated_hypothesis_progress`. This overlay is source mapping only: it does not run or alter the Oil Shock producer, candidate generation, paper lifecycle, broker behavior, trading behavior, or safety gates.

When Macro Calendar has `NEEDS_DATA` and Oil Shock has a candidate-flow blocker, the scorecard keeps both rows separate. Macro Calendar may show missing macro event calendar from the operator action/data-source artifacts; Oil Shock shows the exact blocker from `aegis_oil_shock_candidate_flow_v1`, such as `PRODUCER_MISSING`, without generic conversion to `MISSING_DATA`.


## Operator Decision Dashboard Section Order
The Command Center operator decision dashboard order is TODAY'S RESEARCH RESULT, DAVID ACTIONS, GENERATED HYPOTHESIS PROGRESS, VALIDATION PROGRESS, CURRENT BOTTLENECK, then detailed research metrics below the fold.
