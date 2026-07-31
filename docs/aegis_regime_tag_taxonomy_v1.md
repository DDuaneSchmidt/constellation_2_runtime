# Aegis Regime Tag Taxonomy v1

Purpose: define stable initial regime tags so future validation can answer which hypotheses work under which market regimes.

This is taxonomy definition only. It does not implement regime detection and does not drive automated candidate generation, validation state changes, broker execution, or allocation decisions.

## Assignment Status

- `ACTIVE`: deterministic assignment can be implemented with existing or standard market data.
- `NEEDS_DATA`: useful tag but requires a clean input source or calibration.
- `FUTURE`: useful concept, but not enough current evidence plumbing for deterministic assignment.

## Market Direction Tags

| tag_id | Description | Deterministic assignment rule if available | Data required | Fallback behavior | Validation use | Status |
|---|---|---|---|---|---|---:|
| `RISK_ON` | Broad preference for risky assets over defensive assets. | SPY/QQQ above moving average and HYG outperforming IEF/TLT over lookback. | Equity index, credit ETF, Treasury ETF returns. | `UNKNOWN_REGIME` if any required input missing. | Segment trend, mean reversion, vol-premium outcomes. | `ACTIVE` |
| `RISK_OFF` | Defensive assets outperform and/or equities break trend. | SPY below moving average or HYG underperforms Treasuries with equity drawdown. | Equity index, credit ETF, Treasury ETF returns. | `UNKNOWN_REGIME`. | Test defensive convexity and reduce false positives in risk-on sleeves. | `ACTIVE` |
| `RANGE_BOUND` | Directional trend is weak and realized range is contained. | Absolute index return below threshold and ADX/trend proxy below threshold. | Index returns, high/low or ADX proxy. | Do not assign. | Separate mean-reversion friendly periods. | `NEEDS_DATA` |
| `TRENDING_UP` | Upward price persistence. | Index above medium moving average and positive medium-horizon return. | Index prices. | Do not assign. | Validate momentum sleeves. | `ACTIVE` |
| `TRENDING_DOWN` | Downward price persistence. | Index below medium moving average and negative medium-horizon return. | Index prices. | Do not assign. | Validate defensive and short/avoidance logic. | `ACTIVE` |

## Volatility Tags

| tag_id | Description | Deterministic assignment rule if available | Data required | Fallback behavior | Validation use | Status |
|---|---|---|---|---|---|---:|
| `LOW_VOL` | Realized/implied volatility below normal band. | VIX or realized vol percentile below 25th percentile. | VIX and/or realized vol. | `VOL_UNKNOWN`. | Identify low-premium and crowded carry regimes. | `NEEDS_DATA` |
| `NORMAL_VOL` | Volatility within normal band. | Vol percentile between 25th and 75th percentile. | VIX and/or realized vol. | `VOL_UNKNOWN`. | Baseline validation segment. | `NEEDS_DATA` |
| `HIGH_VOL` | Elevated volatility. | Vol percentile above 75th percentile. | VIX and/or realized vol. | `VOL_UNKNOWN`. | Test defensive convexity and vol income filters. | `NEEDS_DATA` |
| `VOL_EXPANDING` | Volatility rising. | Current vol above short moving average and positive vol change. | VIX or realized vol time series. | Do not assign. | Separate entry quality for defensive and vol-premium sleeves. | `NEEDS_DATA` |
| `VOL_COMPRESSING` | Volatility falling. | Current vol below short moving average and negative vol change. | VIX or realized vol time series. | Do not assign. | Test vol-premium exits and trend stability. | `NEEDS_DATA` |

## Rates / Macro Tags

| tag_id | Description | Deterministic assignment rule if available | Data required | Fallback behavior | Validation use | Status |
|---|---|---|---|---|---|---:|
| `RATES_RISING` | Treasury yields rising / bond prices falling. | TLT/IEF negative return over lookback or yield series rising. | TLT/IEF prices or yield data. | Do not assign. | Segment cross-asset trend and defensive sleeves. | `ACTIVE` |
| `RATES_FALLING` | Treasury yields falling / bond prices rising. | TLT/IEF positive return over lookback or yield series falling. | TLT/IEF prices or yield data. | Do not assign. | Segment risk-off and cross-asset outcomes. | `ACTIVE` |
| `DOLLAR_STRENGTH` | USD strengthening against broad basket. | UUP positive return over lookback or DXY rising. | UUP or DXY. | Do not assign. | Segment commodity/cross-asset trend outcomes. | `ACTIVE` |
| `DOLLAR_WEAKNESS` | USD weakening against broad basket. | UUP negative return over lookback or DXY falling. | UUP or DXY. | Do not assign. | Segment commodity/cross-asset trend outcomes. | `ACTIVE` |

## Market Structure Tags

| tag_id | Description | Deterministic assignment rule if available | Data required | Fallback behavior | Validation use | Status |
|---|---|---|---|---|---|---:|
| `BROAD_PARTICIPATION` | Many assets/sectors confirm the move. | Advance/decline breadth or majority sector ETFs above moving average. | Breadth or sector ETF data. | `STRUCTURE_UNKNOWN`. | Improve confidence in trend hypotheses. | `NEEDS_DATA` |
| `NARROW_LEADERSHIP` | Few assets drive index returns. | Index positive while breadth/sector participation below threshold. | Breadth or sector ETF data. | `STRUCTURE_UNKNOWN`. | Detect fragile trend signals and false positives. | `NEEDS_DATA` |
| `SECTOR_ROTATION` | Leadership shifts across sectors. | Sector relative-strength rank turnover above threshold. | Sector ETF returns. | Do not assign. | Segment event and trend sleeve outcomes. | `NEEDS_DATA` |
| `CROSS_ASSET_CONFIRMATION` | Equities, credit, rates, commodities agree with risk regime. | Multiple asset-class proxies align with same direction/risk state. | SPY/QQQ, HYG, TLT/IEF, GLD/DBC/UUP. | Do not assign. | Validate cross-asset trend signal quality. | `ACTIVE` |
| `CROSS_ASSET_DIVERGENCE` | Asset classes disagree. | Equity trend conflicts with credit/rates/commodity confirmation. | SPY/QQQ, HYG, TLT/IEF, GLD/DBC/UUP. | Do not assign. | Identify higher false-positive regimes. | `ACTIVE` |

## Event Context Tags

| tag_id | Description | Deterministic assignment rule if available | Data required | Fallback behavior | Validation use | Status |
|---|---|---|---|---|---|---:|
| `EARNINGS_SEASON` | Market is in heavy earnings-reporting window. | Calendar rule by quarter or earnings calendar density. | Earnings calendar. | Do not assign. | Segment event dislocation and single-name signals. | `FUTURE` |
| `FED_WEEK` | Scheduled FOMC/Fed decision week. | Economic calendar has FOMC event in current week. | Economic calendar. | Do not assign. | Segment rates, vol, and cross-asset hypotheses. | `FUTURE` |
| `CPI_WEEK` | CPI release week. | Economic calendar has CPI event in current week. | Economic calendar. | Do not assign. | Segment macro-sensitive outcomes. | `FUTURE` |
| `OPTIONS_EXPIRATION_WEEK` | Monthly options expiration context. | Third Friday week or options calendar. | Calendar. | Do not assign. | Segment volatility and mean-reversion outcomes. | `ACTIVE` |

## Validation Contract Recommendation

Future validation samples should allow multiple regime tags per sample:

- `market_direction_tags`
- `volatility_tags`
- `rates_macro_tags`
- `market_structure_tags`
- `event_context_tags`
- `regime_assignment_status`
- `regime_source_artifacts`

Missing regime inputs should never block sample creation. They should set `regime_assignment_status: PARTIAL` or `MISSING` and use fallback tags such as `UNKNOWN_REGIME`, `VOL_UNKNOWN`, or `STRUCTURE_UNKNOWN`.
