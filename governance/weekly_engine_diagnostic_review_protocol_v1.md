# Constellation Weekly Engine Diagnostic Review Protocol
version: 1
status: active
owner: research
type: diagnostic_protocol

Purpose

This protocol defines the deterministic weekly diagnostic review
performed on Constellation trading engines.

The purpose of the review is to:

- analyze engine performance
- diagnose intent pipeline bottlenecks
- evaluate parameter efficiency
- identify deterministic improvements
- propose sandbox experiments
- maintain portfolio health

This protocol produces research diagnostics only.

It does NOT modify trading behavior.

The protocol must be executed using the rubric below.

--------------------------------------------------

Operating Mode

Mode: Deterministic Research Auditor

Responsibilities

Analyze trading system evidence.

Diagnose engine behavior.

Identify deterministic improvements.

Recommend parameter adjustments.

Propose controlled experiments.

Constraints

No AI-driven trading.

No probabilistic decision systems.

No adaptive systems.

All modifications must be deterministic.

All conclusions must reference evidence.

All low sample sizes must be flagged.

This review is research only and does not control trading.

--------------------------------------------------

Section 0 — Dataset Integrity Verification

Before analysis begins the dataset must be verified.

Artifacts required:

runtime/truth
intents
orders
fills
portfolio_state
nav_series
engine parameters
sleeve allocations

Checks required:

missing artifacts
corrupt json
duplicate fills
missing timestamps
inconsistent account ids
nav gaps

If issues exist:

Flag:

DATA_INTEGRITY_WARNING

Severity levels:

low
moderate
critical

Diagnostics may proceed but limitations must be noted.

--------------------------------------------------

Section 1 — System Overview

Compute system-level metrics.

Metrics:

total trades executed
total intents emitted
intent-to-fill ratio
realized pnl
unrealized pnl
weekly return
cumulative return
portfolio nav change
max drawdown this week

Assess:

system stability
changes from prior week
abnormal behavior

--------------------------------------------------

Section 2 — Engine Activity Analysis

For each engine:

Calculate:

candidate opportunities
intents emitted
orders submitted
fills completed
win rate
average win
average loss
expectancy
profit factor
largest win
largest loss
total pnl
max drawdown
average hold time
symbol concentration
time-of-day distribution

Sample size thresholds:

trades < 5
insufficient evidence

trades 5–20
early evidence

trades 20–50
moderate evidence

trades > 50
strong evidence

Grade engines:

A strong positive expectancy

B positive but low sample size

C neutral / inconclusive

D negative expectancy

F clearly harmful

Confidence levels:

low sample size
moderate evidence
strong evidence

--------------------------------------------------

Section 3 — Intent Pipeline Diagnostics

Evaluate pipeline stages:

candidate opportunities
filtered signals
intents
orders
fills

Metrics:

rejection rate per gate
intent suppression rate
order rejection rate

Latency diagnostics:

signal to intent latency
intent to order latency
order to fill latency

Diagnosis questions:

Which filters suppress most signals?

Are profitable setups filtered?

Are engines over-filtered?

Highlight top three bottlenecks.

--------------------------------------------------

Section 4 — Parameter Efficiency Review

Review parameters including:

volatility filters
time-of-day restrictions
universe filters
stop logic
target logic
confirmation requirements
liquidity thresholds

Assess:

impact on signal frequency
impact on trade outcomes
over-constraining parameters
parameters allowing poor trades

--------------------------------------------------

Section 5 — Risk and Capital Efficiency

Evaluate:

nav utilization
capital allocation per sleeve
unused capital
exposure concentration
drawdown containment
trade sizing behavior

Additional metrics:

return on deployed capital
largest symbol exposure
sector concentration
engine correlation (if data available)

Diagnosis questions:

are sleeves using capital efficiently

are sleeves consuming risk without returns

is diversification adequate

--------------------------------------------------

Section 6 — Trade Outcome Diagnostics

Analyze trade distributions.

Metrics:

R multiple distribution
median R
hold time distribution
win/loss clustering
symbol specific results
time-of-day patterns

Diagnosis patterns:

early session losses
regime dependent outcomes
premature stop losses
targets too tight
targets too wide

--------------------------------------------------

Section 7 — Regime Analysis

If market context data exists evaluate:

trend vs range days
high volatility days
low volatility days
gap days
macro event days

Compare engine expectancy by regime.

--------------------------------------------------

Section 8 — Engine Health Table

Summarize engines:

engine
version
grade
trades
expectancy
drawdown
confidence level

--------------------------------------------------

Section 9 — Parameter Adjustment Recommendations

Each recommendation must include:

engine
parameter
current rule
proposed adjustment
expected impact
supporting evidence
risk
priority

All adjustments must be deterministic.

--------------------------------------------------

Section 10 — Sandbox Experiment Proposals

Maximum five experiments.

Each must include:

experiment id
parent engine
exact rule change
expected effect
risk
sample size warning
priority

Experiment lifecycle:

proposed
active
evaluating
accepted
rejected
archived

--------------------------------------------------

Section 11 — Critical System Risks

Identify:

over-filtering
intent scarcity
capital underutilization
persistent drawdown
unstable engine behavior

--------------------------------------------------

Section 12 — Weekly Summary

Provide:

top three engine improvement opportunities

top three parameter adjustments

top three sandbox experiments

key risks to monitor next week

--------------------------------------------------

Section 13 — Engine Evolution Roadmap

Categorize engines:

expand
refine
pause
retire

Decisions must reference evidence and sample size.

--------------------------------------------------

Constellation Weekly Grade

Assign a system grade:

A excellent system health
B stable with minor issues
C functional but needs improvement
D unstable performance
F structural failure
