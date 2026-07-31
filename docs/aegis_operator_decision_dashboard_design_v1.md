# Aegis Operator Decision Dashboard Design v1

## Design Intent
The Command Center top is a decision surface. It answers operator questions first and preserves raw operational metrics as supporting detail below the fold. The top should be compact, scannable, and deterministic.

## Layout
The layout is a vertical operator-decision stack under the PAPER MODE header:

1. TODAY'S RESEARCH RESULT: status, sentence, run context, daily deltas.
2. DAVID ACTIONS: exact action card or exact no-action statement.
3. GENERATED HYPOTHESIS PROGRESS: compact rows for generated hypotheses.
4. VALIDATION PROGRESS: sample/outcome/sufficiency summary.
5. CURRENT BOTTLENECK: one plain-English bottleneck.
6. Detailed research metrics and diagnostics.

## Rendering Model
The browser is a dumb renderer. It receives artifacts from the backend payload and performs only formatting: escaping text, showing counts, applying status tones, and preserving source labels. It does not compute hard gates, workflow states, generated-hypothesis advancement, Oil Shock blocker text, allocation recommendations, or David action requirements.

## Visual Treatment
The five operator sections use compact panels with clear section labels. Status and action copy are emphasized over raw counts. Buttons are rendered as non-executing operator controls unless an existing backend action route is explicitly wired elsewhere.

## Button Design
Exact David action buttons appear only when the backend artifact provides button labels. The no-action state appears as text, not a disabled fake button.

## Generated Hypothesis Design
Each generated hypothesis is a dense row: name, state, throughput, blocker, next step, and David action yes/no. Oil Shock blocker text is copied from `aegis_oil_shock_candidate_flow_v1`.

## Validation Design
Validation progress focuses on learning: included samples, closed outcomes, closest sufficiency target, samples needed, and underpowered count. It avoids implying capital readiness.

## Bottleneck Design
The bottleneck panel shows one sentence and supporting source status. It must be concrete enough that David understands the next constraint without inspecting raw cards.

## Demotion Design
Raw candidate generation, paper observations, pipeline strip, paper promotion, run summary, safety, and diagnostics remain available after the decision sections. They are not the first explanation of whether the run was useful.

## Safety Boundary
No UI element in this package performs broker execution, live trading, trade advice, manual capture, real-capital allocation, autonomous execution, order management, candidate creation, paper observation creation, or automatic retirement.

## Oil Shock Truth Rendering
The generated-hypothesis progress row for Oil Shock is a direct rendering of `aegis_oil_shock_candidate_flow_v1` blocker fields. The browser may format the row, but it cannot reinterpret candidate-flow blockers from throughput or workflow state.

The bottleneck and today-result sections must avoid generic data-issue language for Oil Shock when the Oil Shock artifact reports `PRODUCER_MISSING`. Producer-missing and data-missing are separate operational truths and should be displayed as separate rows when both exist across generated hypotheses.
