# Intraday Event Evidence Readiness Pack 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: evidence requirements for candidates that cannot be fully validated with daily data alone. No implementation, no downloads, no replay changes, no candidate changes, no qualification changes, no governance changes, no paper-forward changes, no trading authority, no broker authority, no capital allocation, and no position sizing.

## Inputs

- `research_journal/reports/intraday_dependency_inventory_001.md`
- `research_journal/reports/event_metadata_requirements_001.md`
- `research_journal/reports/insufficient_data_root_cause_analysis_001.md`
- `reports/atlas_v2_research_os/candidate_validation_queue_001.md`

## Classification Model

| Output | Meaning |
| --- | --- |
| `HIGH_VALUE_INTRADAY` | Intraday bars are the main missing evidence needed for rule-specific validation, and the queue or root-cause reports indicate high or medium-high validation gain. |
| `EVENT_REQUIRED` | Event metadata is mandatory in addition to intraday bars. Daily data cannot validate the event-timing claim. |
| `DAILY_SUFFICIENT` | Daily data is sufficient for the candidate's current validation question. |
| `DEFER` | Intraday is still required for rule-specific validation, but current evidence indicates daily prerequisites, broad coverage burden, or validation sequencing gaps should be resolved before intraday/event work is treated as ready. |

No affected candidate in this pack is classified `DAILY_SUFFICIENT` for rule-specific validation.

## Readiness Summary

| Output | Candidate Count | Candidates |
| --- | ---: | --- |
| `HIGH_VALUE_INTRADAY` | 4 | `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_854ad10b904e1ae9`, `ptc_backtest_final_b23c6756bfb3263a` |
| `EVENT_REQUIRED` | 1 | `ptc_backtest_final_d5931b24bd391113` |
| `DAILY_SUFFICIENT` | 0 | none |
| `DEFER` | 3 | `ptc_backtest_final_624fdd85668e2c08`, `ptc_backtest_final_4df2e8e80685a054`, `ptc_backtest_final_7d839944a8a4070a` |

## Candidate Evidence Requirements

| candidate_id | output | required timeframe | required symbols | event metadata required | estimated bars required | expected validation gain | implementation complexity | authority risk |
| --- | --- | --- | --- | --- | ---: | --- | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | `HIGH_VALUE_INTRADAY` | `30m` | DIA, QQQ, SPY | no | 45,000-51,000 | HIGH | MEDIUM | LOW if evidence-only; HIGH if used for promotion or replay override |
| `ptc_backtest_final_3a4ac24107c77136` | `HIGH_VALUE_INTRADAY` | `5m` | DIA, QQQ, SPY | no | 270,000-300,000 | HIGH | HIGH | LOW if evidence-only; HIGH if used for promotion or replay override |
| `ptc_backtest_final_854ad10b904e1ae9` | `HIGH_VALUE_INTRADAY` | `30m` | DIA, QQQ | no | 30,000-34,000 | HIGH | MEDIUM | LOW if evidence-only; HIGH if used for promotion or replay override |
| `ptc_backtest_final_624fdd85668e2c08` | `DEFER` | `30m` | DBC, TLT, USO | no | 45,000-51,000 | MEDIUM-HIGH | MEDIUM | LOW if evidence-only; HIGH if used before daily coverage completion |
| `ptc_backtest_final_d5931b24bd391113` | `EVENT_REQUIRED` | `30m` | AMZN, BAC, META, MSFT, NFLX, TSLA | yes | 90,000-102,000 plus event records | MEDIUM | HIGH | MEDIUM for evidence design; HIGH if event metadata is treated as validation authority |
| `ptc_backtest_final_4df2e8e80685a054` | `DEFER` | `1h` | AAPL, AMZN, BAC, JPM, META, MSFT, TSLA | no | 56,000-63,000 | MEDIUM | MEDIUM-HIGH | LOW if evidence-only; HIGH if partial-universe evidence is treated as full validation |
| `ptc_backtest_final_7d839944a8a4070a` | `DEFER` | `15m` | TLT, USO | no | 60,000-68,000 | HIGH | MEDIUM | LOW if evidence-only; HIGH if daily gaps are skipped |
| `ptc_backtest_final_b23c6756bfb3263a` | `HIGH_VALUE_INTRADAY` | `30m` | BAC, GOOGL, META, MSFT, NFLX, TSLA | no | 90,000-102,000 | MEDIUM-HIGH | HIGH | LOW if evidence-only; HIGH if broad universe aggregation is bypassed |

Estimated bars use the five-year row-count ranges from the intraday dependency inventory:

- `1h`: about `8,000-9,000` rows per symbol
- `30m`: about `15,000-17,000` rows per symbol
- `15m`: about `30,000-34,000` rows per symbol
- `5m`: about `90,000-100,000` rows per symbol

## Detailed Pack

### `ptc_backtest_final_469607b8340421b7`

| Field | Value |
| --- | --- |
| output | `HIGH_VALUE_INTRADAY` |
| mechanism / regime | `BREAKOUT` / `CHOP` |
| required timeframe | `30m` |
| required symbols | DIA, QQQ, SPY |
| event metadata required | no |
| estimated bars required | 45,000-51,000 |
| data source options | local CSV under `data/historical/{SYMBOL}_30m.csv`; fallback `data/cache/{SYMBOL}_30m.csv`; provider-normalized 30m OHLCV with lineage |
| minimum viable dataset | 30m OHLCV for DIA, QQQ, and SPY over the tested range; timezone-normalized timestamps; session labels; no duplicate bars; explicit adjustment mode |
| validation logic requirements | Test the attributed 30m breakout rule directly across DIA, QQQ, and SPY; preserve predeclared CHOP/regime handling; report post-filter sample size by symbol and aggregate; separate daily proxy output from intraday output |
| expected validation gain | HIGH: priority 1 queue candidate; top campaign and robust rank; directly tests SPY-proxy survival across ETF universe |
| implementation complexity | MEDIUM: three ETF symbols and 30m bars; no event metadata |
| authority risk | Evidence-only risk is LOW. Risk becomes HIGH if intraday evidence is used to promote, qualify, override replay, or change governance. |

### `ptc_backtest_final_3a4ac24107c77136`

| Field | Value |
| --- | --- |
| output | `HIGH_VALUE_INTRADAY` |
| mechanism / regime | `BREAKOUT` / `CHOP` |
| required timeframe | `5m` |
| required symbols | DIA, QQQ, SPY |
| event metadata required | no |
| estimated bars required | 270,000-300,000 |
| data source options | local CSV under `data/historical/{SYMBOL}_5m.csv`; fallback `data/cache/{SYMBOL}_5m.csv`; provider-normalized 5m OHLCV with lineage |
| minimum viable dataset | 5m OHLCV for DIA, QQQ, and SPY over the tested range; timezone-normalized timestamps; session labels; no duplicate bars; explicit adjustment mode |
| validation logic requirements | Test the attributed 5m breakout rule directly; reconstruct trigger timing without daily-bar proxy; report post-filter sample size by symbol; preserve CHOP/regime preconditions |
| expected validation gain | HIGH: priority 2 queue candidate; same high-leverage DIA/QQQ/SPY cluster as priority 1 but tests timeframe sensitivity |
| implementation complexity | HIGH: 5m data volume is largest in the current queue |
| authority risk | Evidence-only risk is LOW. Risk becomes HIGH if intraday evidence is used to promote, qualify, override replay, or change governance. |

### `ptc_backtest_final_854ad10b904e1ae9`

| Field | Value |
| --- | --- |
| output | `HIGH_VALUE_INTRADAY` |
| mechanism / regime | `MEAN_REVERSION` / `TRENDING` |
| required timeframe | `30m` |
| required symbols | DIA, QQQ |
| event metadata required | no |
| estimated bars required | 30,000-34,000 |
| data source options | local CSV under `data/historical/{SYMBOL}_30m.csv`; fallback `data/cache/{SYMBOL}_30m.csv`; provider-normalized 30m OHLCV with lineage |
| minimum viable dataset | 30m OHLCV for DIA and QQQ over the tested range; timezone-normalized timestamps; session labels; no duplicate bars; explicit adjustment mode |
| validation logic requirements | Test mean-reversion trigger and TRENDING regime constraints on the attributed 30m ETF universe; report symbol-level and aggregate samples; distinguish rule-specific intraday validation from daily coverage |
| expected validation gain | HIGH: priority 3 queue candidate; uses same DIA/QQQ acquisition cluster as priorities 1-2 while testing a different mechanism/regime pair |
| implementation complexity | MEDIUM: two ETF symbols at 30m; no event metadata |
| authority risk | Evidence-only risk is LOW. Risk becomes HIGH if used to alter candidate, replay, qualification, or governance state. |

### `ptc_backtest_final_624fdd85668e2c08`

| Field | Value |
| --- | --- |
| output | `DEFER` |
| mechanism / regime | `MEAN_REVERSION` / `TRENDING` |
| required timeframe | `30m` |
| required symbols | DBC, TLT, USO |
| event metadata required | no |
| estimated bars required | 45,000-51,000 |
| data source options | local CSV under `data/historical/{SYMBOL}_30m.csv`; fallback `data/cache/{SYMBOL}_30m.csv`; provider-normalized 30m OHLCV with lineage |
| minimum viable dataset | Daily adjusted files for DBC, TLT, and USO for first-pass coverage; 30m OHLCV for rule-specific validation; timezone-normalized timestamps; session labels; explicit adjustment mode |
| validation logic requirements | After first-pass daily coverage exists, test 30m mean-reversion logic across DBC, TLT, and USO; report sample counts by symbol; separate coverage unlock from intraday mechanism validation |
| expected validation gain | MEDIUM-HIGH: priority 5 queue candidate; non-equity ETF universe; shares TLT/USO with priority 4 |
| implementation complexity | MEDIUM: compact symbol set but current report records no daily coverage for the three symbols |
| authority risk | Evidence-only risk is LOW. Risk becomes HIGH if intraday validation is treated as ready before daily coverage and universe coverage are complete. |

### `ptc_backtest_final_d5931b24bd391113`

| Field | Value |
| --- | --- |
| output | `EVENT_REQUIRED` |
| mechanism / regime | `EVENT_REACTION` / `CHOP` |
| required timeframe | `30m` |
| required symbols | AMZN, BAC, META, MSFT, NFLX, TSLA |
| event metadata required | yes |
| estimated bars required | 90,000-102,000 plus timestamped event records |
| data source options | local CSV under `data/historical/{SYMBOL}_30m.csv`; fallback `data/cache/{SYMBOL}_30m.csv`; provider-normalized 30m OHLCV with lineage; timestamped earnings/news/event metadata with source hashes |
| minimum viable dataset | 30m OHLCV for all six symbols over the tested range; event records with event timestamp UTC and ET, session bucket, source publication time, first eligible 30m bar, event type, source lineage, and metadata completeness status |
| validation logic requirements | Bind events to first eligible 30m bar; define pre-event baseline and post-event reaction windows; exclude date-only events; deduplicate catalysts; report eligible/excluded event counts by symbol and event type; compare event windows against non-event controls |
| expected validation gain | MEDIUM: priority 7 queue candidate; full daily coverage exists in later root-cause state, but daily proxy produced zero usable post-filter samples and cannot validate event timing |
| implementation complexity | HIGH: requires both 30m market data and timestamped event metadata plus event-aware validation logic |
| authority risk | MEDIUM for evidence design because metadata interpretation can alter event eligibility. HIGH if metadata or intraday alignment is used as replay, qualification, promotion, or governance authority. |

### `ptc_backtest_final_4df2e8e80685a054`

| Field | Value |
| --- | --- |
| output | `DEFER` |
| mechanism / regime | `REVERSAL` / `TRENDING` |
| required timeframe | `1h` |
| required symbols | AAPL, AMZN, BAC, JPM, META, MSFT, TSLA |
| event metadata required | no |
| estimated bars required | 56,000-63,000 |
| data source options | local CSV under `data/historical/{SYMBOL}_1h.csv`; fallback `data/cache/{SYMBOL}_1h.csv`; provider-normalized 1h OHLCV with lineage |
| minimum viable dataset | Daily adjusted files for AAPL and JPM to complete first-pass universe coverage; 1h OHLCV for all seven symbols for rule-specific reversal validation; timezone-normalized timestamps; session labels; explicit adjustment mode |
| validation logic requirements | Separate partial BAC-supported daily replay from full-universe validation; test 1h reversal trigger across all attributed symbols; report sample size and performance by symbol and aggregate |
| expected validation gain | MEDIUM: priority 8 queue candidate; broadest symbol requirement and lower robust rank, but mechanism diversity exists |
| implementation complexity | MEDIUM-HIGH: seven-symbol 1h universe and daily prerequisites for AAPL/JPM |
| authority risk | Evidence-only risk is LOW. Risk becomes HIGH if partial-universe evidence is treated as full validation. |

### `ptc_backtest_final_7d839944a8a4070a`

| Field | Value |
| --- | --- |
| output | `DEFER` |
| mechanism / regime | `BREAKOUT` / `CHOP` |
| required timeframe | `15m` |
| required symbols | TLT, USO |
| event metadata required | no |
| estimated bars required | 60,000-68,000 |
| data source options | local CSV under `data/historical/{SYMBOL}_15m.csv`; fallback `data/cache/{SYMBOL}_15m.csv`; provider-normalized 15m OHLCV with lineage |
| minimum viable dataset | Daily adjusted files for TLT and USO for first-pass coverage; 15m OHLCV for rule-specific breakout validation; timezone-normalized timestamps; session labels; no duplicate bars; explicit adjustment mode |
| validation logic requirements | After daily coverage exists, test 15m breakout logic across TLT and USO; report post-filter sample size by symbol; preserve CHOP/regime handling |
| expected validation gain | HIGH: priority 4 queue candidate and compact symbol set, but current root-cause state records no daily coverage for the required symbols |
| implementation complexity | MEDIUM: 15m data volume for two ETF symbols plus daily prerequisites |
| authority risk | Evidence-only risk is LOW. Risk becomes HIGH if daily gaps are skipped or intraday data is treated as candidate authority. |

### `ptc_backtest_final_b23c6756bfb3263a`

| Field | Value |
| --- | --- |
| output | `HIGH_VALUE_INTRADAY` |
| mechanism / regime | `BREAKOUT` / `CHOP` |
| required timeframe | `30m` |
| required symbols | BAC, GOOGL, META, MSFT, NFLX, TSLA |
| event metadata required | no |
| estimated bars required | 90,000-102,000 |
| data source options | local CSV under `data/historical/{SYMBOL}_30m.csv`; fallback `data/cache/{SYMBOL}_30m.csv`; provider-normalized 30m OHLCV with lineage |
| minimum viable dataset | 30m OHLCV for BAC, GOOGL, META, MSFT, NFLX, and TSLA over the tested range; timezone-normalized timestamps; session labels; no duplicate bars; explicit adjustment mode |
| validation logic requirements | Test 30m breakout logic across the full single-stock universe; report symbol-level contribution and aggregate sample size; separate daily proxy results from intraday output; preserve CHOP/regime preconditions |
| expected validation gain | MEDIUM-HIGH: priority 6 queue candidate; full daily coverage exists after GOOGL unlock, but daily proxy replay still produced zero usable post-filter samples |
| implementation complexity | HIGH: six single-stock symbols at 30m plus universe aggregation requirements |
| authority risk | Evidence-only risk is LOW. Risk becomes HIGH if broad universe aggregation limits are bypassed or if intraday evidence is used for promotion or qualification. |

## Data Source Options

Allowed planning options only:

| Option | Description | Current Action |
| --- | --- | --- |
| Local normalized CSV | `data/historical/{SYMBOL}_{TIMEFRAME}.csv` | no files created |
| Local cache CSV | `data/cache/{SYMBOL}_{TIMEFRAME}.csv` | no files created |
| Provider-normalized OHLCV | external provider bars normalized before local validation use | no downloads requested or performed |
| Event metadata catalog | timestamped earnings/news/event records with source lineage | no metadata ingestion implemented |

Every option requires source lineage, timestamp normalization, session labels, duplicate-bar checks, OHLCV invariants, explicit adjustment mode, and bar-count reasonableness checks before validation use.

## Validation Logic Requirements Across Pack

- Daily proxy results must remain separate from intraday validation results.
- Intraday rules must be tested at the attributed candidate timeframe.
- Candidate-level outputs must report symbol-level sample counts and aggregate sample counts.
- `CHOP` and other regime labels must be predeclared or bar-aligned before filtering.
- Zero post-filter samples must be reported as insufficient validation evidence, not as candidate failure.
- Event-reaction validation requires event-to-bar alignment and non-event control windows.
- Intraday evidence has no replay, qualification, candidate, governance, paper-forward, trading, broker, capital, or position-sizing authority.

## Output Notes

- `HIGH_VALUE_INTRADAY` does not mean validated. It means intraday evidence is the main high-value missing evidence class.
- `EVENT_REQUIRED` does not mean event metadata exists. It means event metadata is mandatory before validation can become meaningful.
- `DEFER` does not mean daily data is sufficient. It means current evidence sequencing or breadth makes intraday/event readiness lower than the high-value intraday group.
- `DAILY_SUFFICIENT` is unused because every affected candidate has a rule-specific intraday requirement in the reviewed inputs.

## Authority Boundary

This pack is a planning and evidence-readiness artifact only. It does not implement data ingestion, download market data, create event metadata, run replay, change validation, qualify candidates, promote candidates, create paper observations, modify governance, recommend trades, allocate capital, authorize broker execution, size positions, or alter safety gates.
