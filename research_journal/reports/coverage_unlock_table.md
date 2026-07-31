# Coverage Unlock Table

| Phase | Symbols Added | New Full Candidates | Full Candidates | Unique Coverage | Candidate-Symbol Coverage | Pair Coverage Gain | Validation Block Rate | Expected Information Gain |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Baseline | `SPY existing` | 0 | 0/8 | 6.67% | 6.25% | 0.00% | 100.00% | Baseline only; direct validation remains blocked because SPY-only proxy coverage produced zero post-filter samples. |
| Phase 1 | `DIA,QQQ` | 3 | 3/8 | 20.00% | 25.00% | 18.75% | 62.50% | HIGH: unlocks the top DIA/QQQ/SPY ETF cluster, including the top two campaign-ranked breakout candidates and one mean-reversion comparator. |
| Phase 2 | `BAC,META,MSFT,TSLA` | 0 | 3/8 | 46.67% | 62.50% | 37.50% | 62.50% | MEDIUM-HIGH: adds the highest-overlap single-stock core and tests whether equity candidates survive beyond ETF proxy behavior, but most broad equity universes remain incomplete. |
| Phase 3 | `AMZN,NFLX` | 1 | 4/8 | 60.00% | 75.00% | 12.50% | 50.00% | MEDIUM: reaches 75% candidate-symbol coverage and reduces single-stock proxy dependence; still leaves full-candidate gaps for GOOGL, AAPL/JPM, and rates/commodity ETFs. |
| Phase 4 | `AAPL,DBC,GOOGL,JPM,TLT,USO` | 4 | 8/8 | 100.00% | 100.00% | 25.00% | 0.00% | HIGH COMPLETION VALUE: closes remaining current-universe gaps, unlocks rates/commodity ETF candidates and brings full-candidate coverage above 75%. |

Authority: planning table only; no downloads, replay changes, qualification changes, candidate changes, or production changes.