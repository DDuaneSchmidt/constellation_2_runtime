# AEGIS Generated Hypothesis Outcome Eligibility Day Proof Design v1

## Design

Package 020 is a targeted eligibility-day proof layered on top of the existing deterministic outcome pipeline. It consumes the paper ledger, auto-closure, exit recommendation, outcome registry, Package 018, Package 019, and generated-hypothesis validation proof artifacts.

The builder computes calendar age and trading-day age from the paper observation entry date and target day. Auto-promoted research paper observations default to a one-trading-day minimum holding period unless an authoritative position or closure row supplies a more specific minimum.

Outcome creation is detected only from authoritative outcome evidence. The artifact does not call any outcome-mutating API and does not insert rows.

## Sequencing

For `TARGET_DAY=2026-06-03`, audit should run candidate/paper lifecycle evidence, paper outcome auto-closure, outcome validation, Package 018, Package 019, then Package 020 before generated-hypothesis validation proof and portal reporting.

## Safety

The artifact records explicit safety flags showing that it is research-only and read-only. It does not fabricate exit prices, close timestamps, validation samples, research quality rows, or allocation recommendations.
