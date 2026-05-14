# Aegis Research Lab

Aegis Research Lab is the offline, exploratory layer for hypothesis discovery. It is separate from Aegis Lite.

## Purpose

Research Lab exists to explore sleeves, edge ideas, regime signals, stop logic, sizing ideas, overlap behavior, governance ideas, and behavioral state hypotheses before they are eligible for operational use.

## Separation From Runtime

Research Lab artifacts are not runtime inputs. They do not authorize trading, broker submit, transmit automation, fill lifecycle processing, or Aegis Lite behavior changes.

Aegis Lite may only consume ideas that have crossed the governed promotion boundary. Research may discover ideas. Aegis Lite may only consume validated and promoted ideas.

## Allowed Activities

- Offline replay and simulation.
- Exploratory analysis.
- Hypothesis comparison.
- Regime and edge discovery.
- Stop/risk research.
- Governance compatibility review.
- Manual execution clarity review.
- Failure-mode documentation.

## Prohibited Activities

- Broker submit.
- Transmit automation.
- Fill lifecycle automation.
- Live market-session execution requirements.
- Automatic runtime mutation.
- Treating experimental artifacts as operational evidence.

## Failure Tolerance

Research Lab is allowed to fail, reject ideas, archive ideas, and keep drafts. Failure is evidence. Rejected and draft research cannot cross into Aegis Lite implementation.

## Research Outputs

Canonical Research Lab outputs include:

- `research_evidence_packet.v1`
- `research_to_lite_promotion.v1`
- `research_lab_index.v1`
- `edge_taxonomy.v1`
