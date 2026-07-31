# Intraday Candidate Priority 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: priority order for the top three `HIGH_VALUE_INTRADAY` candidates. No downloads, no implementation, no ingestion, no replay changes, no validation changes, no candidate changes, no qualification changes, no governance changes, and no trading authority.

## Inputs

- `research_journal/reports/intraday_event_evidence_readiness_pack_001.md`
- `reports/atlas_v2_research_os/candidate_validation_queue_001.md`

## Priority Order

| Priority | candidate_id | expected validation gain | expected candidate impact | data volume | implementation complexity | evidence quality improvement |
| ---: | --- | --- | --- | ---: | --- | --- |
| 1 | `ptc_backtest_final_469607b8340421b7` | HIGH | HIGH: top campaign and robust candidate; tests SPY-proxy survival across DIA/QQQ/SPY ETF universe | 45,000-51,000 30m bars | MEDIUM | HIGH: replaces daily proxy/zero-sample filter result with attributed 30m universe evidence |
| 2 | `ptc_backtest_final_854ad10b904e1ae9` | HIGH | HIGH: priority 3 queue candidate; uses same DIA/QQQ intraday cluster while testing a different mechanism/regime pair | 30,000-34,000 30m bars | MEDIUM | HIGH: adds rule-specific 30m mean-reversion evidence and mechanism diversity at lower data volume |
| 3 | `ptc_backtest_final_3a4ac24107c77136` | HIGH | HIGH: priority 2 queue candidate; tests same ETF breakout cluster at 5m timeframe sensitivity | 270,000-300,000 5m bars | HIGH | HIGH: replaces daily proxy/zero-sample filter result with precise 5m trigger reconstruction |

## Boundary

This report is priority order only. It does not download market data, implement ingestion, run replay, alter validation, promote candidates, qualify candidates, modify governance, create paper observations, recommend trades, allocate capital, authorize broker execution, or size positions.
