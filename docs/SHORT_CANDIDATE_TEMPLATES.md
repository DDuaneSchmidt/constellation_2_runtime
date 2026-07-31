# Short Candidate Templates

Status: DRAFT_ONLY

These are draft short-side Portfolio123 templates for research review only. They make no API calls, do not query Portfolio123, do not check live borrow, and do not create trade advice, candidates, paper positions, orders, or allocation recommendations.

Common universe constraints:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`

Common borrow-risk notes:

- Avoid microcaps by requiring MktCap > 1000 and AvgDailyTot(60) > 2000000.
- Avoid obvious borrow nightmares by excluding low-price, low-liquidity, OTC, ADR, and financial-sector edge cases.
- Before any non-draft use, add live borrow, fee, hard-to-borrow, corporate-action, and locate checks outside this generator.

## Template 1: Negative Revisions

Status: DRAFT_ONLY

Template ID: `short_negative_revisions_v1`

Thesis: Short candidates where analyst estimate pressure is broad enough to suggest deteriorating forward fundamentals.

Portfolio123 draft rules:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`
- `CurFYEPSMean < CurFYEPSMean4WkAgo`
- `NextFYEPSMean < NextFYEPSMean4WkAgo`
- `CurFYEPSMean < CurFYEPSMean13WkAgo`
- `EPSActual(0, QTR) < EPSEstimate(0, QTR)`

Rank formula draft: `Lower is worse: FRank("CurFYEPSMean / CurFYEPSMean13WkAgo", #All, #DESC) + FRank("NextFYEPSMean / NextFYEPSMean13WkAgo", #All, #DESC)`

Entry review notes:

- Confirm revisions are not a one-time accounting reset.
- Prefer names with negative revisions across both current-year and next-year estimates.

## Template 2: Negative FCF

Status: DRAFT_ONLY

Template ID: `short_negative_fcf_v1`

Thesis: Short candidates where reported earnings or valuation support is undermined by negative free cash flow.

Portfolio123 draft rules:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`
- `FCF(0, TTM) < 0`
- `OperCashFl(0, TTM) < CapEx(0, TTM)`
- `Sales(0, TTM) > 0`
- `DebtTotQ > CashPSQ * SharesQ`

Rank formula draft: `Lower is worse: FRank("FCF(0, TTM) / Sales(0, TTM)", #All, #ASC)`

Entry review notes:

- Separate temporary working-capital drag from structurally negative cash generation.
- Require a clear path from cash burn to balance-sheet or valuation pressure.

## Template 3: Negative ROA

Status: DRAFT_ONLY

Template ID: `short_negative_roa_v1`

Thesis: Short candidates with poor asset productivity and weak profitability despite sufficient liquidity for testing.

Portfolio123 draft rules:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`
- `ROA%TTM < 0`
- `ROE%TTM < 0`
- `OpMgn%TTM < 0`
- `Sales(0, TTM) > 0`

Rank formula draft: `Lower is worse: FRank("ROA%TTM", #All, #ASC) + FRank("OpMgn%TTM", #All, #ASC)`

Entry review notes:

- Avoid early-stage biotech and single-event loss cases unless separately governed.
- Prefer recurring operating losses over non-cash one-time charges.

## Template 4: Weak Momentum

Status: DRAFT_ONLY

Template ID: `short_weak_momentum_v1`

Thesis: Short candidates where price action confirms deteriorating market perception.

Portfolio123 draft rules:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`
- `Close(0) < SMA(50, 0)`
- `SMA(50, 0) < SMA(200, 0)`
- `Pr52W%Chg < 0`
- `RelStrength(26) < 40`

Rank formula draft: `Lower is worse: FRank("RelStrength(26)", #All, #ASC) + FRank("Pr52W%Chg", #All, #ASC)`

Entry review notes:

- Avoid crowded gap-down exhaustion immediately after capitulation events.
- Prefer weak momentum confirmed by fundamental deterioration.

## Template 5: Revision Collapse + Poor Quality

Status: DRAFT_ONLY

Template ID: `short_revision_collapse_poor_quality_v1`

Thesis: Short candidates combining estimate cuts with low-quality accounting and poor profitability.

Portfolio123 draft rules:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`
- `CurFYEPSMean < CurFYEPSMean13WkAgo * 0.9`
- `ROA%TTM < 3`
- `AccrualRatioTTM > 0.05`
- `FCF(0, TTM) < NetIncBXor(0, TTM)`

Rank formula draft: `Lower is worse: FRank("CurFYEPSMean / CurFYEPSMean13WkAgo", #All, #ASC) + FRank("ROA%TTM", #All, #ASC)`

Entry review notes:

- Use this as a higher-conviction overlay, not a standalone borrow decision.
- Check whether estimate collapse has already been fully repriced.

## Template 6: Accruals / Cash Flow Mismatch

Status: DRAFT_ONLY

Template ID: `short_accruals_cashflow_mismatch_v1`

Thesis: Short candidates where accounting earnings quality appears weaker than headline profits imply.

Portfolio123 draft rules:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`
- `NetIncBXor(0, TTM) > 0`
- `FCF(0, TTM) < 0`
- `AccrualRatioTTM > 0.08`
- `OperCashFl(0, TTM) < NetIncBXor(0, TTM)`

Rank formula draft: `Higher is worse: FRank("AccrualRatioTTM", #All, #DESC)`

Entry review notes:

- Inspect receivables, inventory, and capitalized-cost drivers before testing.
- Avoid penalizing seasonal working-capital timing without persistence.

## Template 7: Margin Degradation

Status: DRAFT_ONLY

Template ID: `short_margin_degradation_v1`

Thesis: Short candidates where operating economics are deteriorating before consensus fully catches down.

Portfolio123 draft rules:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`
- `OpMgn%TTM < OpMgn%PYQ`
- `GrossMgn%TTM < GrossMgn%PYQ`
- `Sales(0, TTM) > Sales(4, TTM)`
- `CurFYEPSMean < CurFYEPSMean4WkAgo`

Rank formula draft: `Lower is worse: FRank("OpMgn%TTM - OpMgn%PYQ", #All, #ASC)`

Entry review notes:

- Prefer margin deterioration with continued sales growth, which can reveal poor operating leverage.
- Review commodity and FX exposures before assuming structural margin damage.

## Template 8: Expensive Growth Breakdown

Status: DRAFT_ONLY

Template ID: `short_expensive_growth_breakdown_v1`

Thesis: Short candidates with premium valuation, slowing growth, and weakening price confirmation.

Portfolio123 draft rules:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`
- `PEExclXorTTM > 35`
- `SalesGr%TTM < SalesGr%PYQ`
- `EPS%ChgTTM < EPS%ChgPYQ`
- `Close(0) < SMA(100, 0)`

Rank formula draft: `Higher valuation and weaker growth is worse: FRank("PEExclXorTTM", #All, #DESC) + FRank("SalesGr%TTM - SalesGr%PYQ", #All, #ASC)`

Entry review notes:

- Avoid shorting high-quality compounders solely on valuation.
- Prefer premium multiples where growth deceleration is already visible in revisions or margins.

## Template 9: Levered Earnings Decay

Status: DRAFT_ONLY

Template ID: `short_levered_earnings_decay_v1`

Thesis: Short candidates where debt load and falling earnings may increase refinancing and equity dilution risk.

Portfolio123 draft rules:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`
- `DebtTotQ / EBITDA(0, TTM) > 3`
- `IntCovTTM < 3`
- `CurFYEPSMean < CurFYEPSMean13WkAgo`
- `FCF(0, TTM) < 0`

Rank formula draft: `Worse leverage and coverage rank: FRank("DebtTotQ / EBITDA(0, TTM)", #All, #DESC) + FRank("IntCovTTM", #All, #ASC)`

Entry review notes:

- Exclude financials and pass any capital-structure edge case through separate review.
- Confirm debt metrics are meaningful for the issuer's sector.

## Template 10: Weak Relative Strength + Negative Estimates

Status: DRAFT_ONLY

Template ID: `short_weak_relative_strength_negative_estimates_v1`

Thesis: Short candidates where weak relative price behavior aligns with negative estimate pressure.

Portfolio123 draft rules:

- `DRAFT_ONLY = true`
- `Close(0) > 5`
- `AvgDailyTot(60) > 2000000`
- `MktCap > 1000`
- `Universe($ADR) = false`
- `Universe($OTC) = false`
- `Universe($Financials) = false`
- `RelStrength(13) < 35`
- `RelStrength(26) < 45`
- `CurFYEPSMean < CurFYEPSMean4WkAgo`
- `NextFYEPSMean < NextFYEPSMean4WkAgo`

Rank formula draft: `Lower is worse: FRank("RelStrength(13)", #All, #ASC) + FRank("CurFYEPSMean / CurFYEPSMean4WkAgo", #All, #ASC)`

Entry review notes:

- Use as a liquid short watchlist screen, not a signal to trade.
- Prefer names with both price and estimate deterioration rather than one isolated weak input.
