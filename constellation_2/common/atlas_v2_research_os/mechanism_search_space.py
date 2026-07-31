from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .mechanism_search_models import MechanismFamily, SourceSearchConfig


@dataclass(frozen=True)
class MechanismTemplate:
    mechanism: str
    condition_sets: list[list[str]]
    regimes: list[str]
    timeframes: list[str]
    entry_rule: str
    exit_rule: str
    invalidation_rule: str
    required_observations: list[str]
    base_complexity: float


MECHANISM_TEMPLATES = [
    MechanismTemplate(
        mechanism=MechanismFamily.MEAN_REVERSION.value,
        condition_sets=[
            ["extended move from rolling mean", "momentum deceleration"],
            ["lower band pierce", "close back inside band"],
            ["intraday overextension", "volume normalization"],
        ],
        regimes=["CHOP", "LOW_VOL", "VOL_CONTRACTION"],
        timeframes=["5m", "15m", "1d"],
        entry_rule="Observe reclaim of the reference mean after an extension.",
        exit_rule="Observe return to reference mean or loss of deceleration.",
        invalidation_rule="Invalidate when price extends again with rising participation.",
        required_observations=["rolling_mean", "distance_from_mean", "momentum_slope", "volume"],
        base_complexity=0.34,
    ),
    MechanismTemplate(
        mechanism=MechanismFamily.BREAKOUT.value,
        condition_sets=[
            ["range compression", "close above range boundary"],
            ["multi-session resistance test", "participation expansion"],
            ["higher lows", "prior high acceptance"],
        ],
        regimes=["VOL_CONTRACTION", "TRENDING", "BULL"],
        timeframes=["15m", "1h", "1d"],
        entry_rule="Observe acceptance beyond the prior range boundary.",
        exit_rule="Observe failure to hold accepted breakout area or measured move completion.",
        invalidation_rule="Invalidate when price closes back inside the prior range.",
        required_observations=["range_high", "range_low", "close_location", "volume"],
        base_complexity=0.36,
    ),
    MechanismTemplate(
        mechanism=MechanismFamily.OPENING_RANGE.value,
        condition_sets=[
            ["first interval range defined", "range break with confirmation"],
            ["opening drive", "pullback holds range midpoint"],
            ["gap context", "opening range acceptance"],
        ],
        regimes=["HIGH_VOL", "VOL_EXPANSION", "RISK_ON"],
        timeframes=["1m", "5m", "15m"],
        entry_rule="Observe opening range boundary acceptance after the range is established.",
        exit_rule="Observe loss of opening range midpoint or end of observation window.",
        invalidation_rule="Invalidate when opening range boundary break immediately fails.",
        required_observations=["opening_range_high", "opening_range_low", "gap", "volume"],
        base_complexity=0.38,
    ),
    MechanismTemplate(
        mechanism=MechanismFamily.VWAP_OR_AVERAGE_RECLAIM.value,
        condition_sets=[
            ["below session average", "reclaim with hold"],
            ["failed sell pressure", "average reclaim"],
            ["prior rejection", "second reclaim attempt"],
        ],
        regimes=["TRENDING", "CHOP", "RISK_ON"],
        timeframes=["5m", "15m", "1h"],
        entry_rule="Observe reclaim and hold of VWAP or selected average.",
        exit_rule="Observe loss of VWAP or selected average after reclaim.",
        invalidation_rule="Invalidate when reclaim fails without acceptance.",
        required_observations=["vwap", "moving_average", "price_acceptance", "volume"],
        base_complexity=0.37,
    ),
    MechanismTemplate(
        mechanism=MechanismFamily.SESSION_TIMING.value,
        condition_sets=[
            ["morning impulse", "midday compression"],
            ["late-session imbalance", "directional continuation"],
            ["lunch fade", "afternoon reclaim"],
        ],
        regimes=["RISK_ON", "CHOP", "RISK_OFF"],
        timeframes=["5m", "15m", "session"],
        entry_rule="Observe time-window-specific behavior matching the session pattern.",
        exit_rule="Observe session window expiry or loss of time-window pattern.",
        invalidation_rule="Invalidate when the expected session behavior does not appear in window.",
        required_observations=["session_time", "range_progression", "volume_curve"],
        base_complexity=0.41,
    ),
    MechanismTemplate(
        mechanism=MechanismFamily.VOLATILITY_EXPANSION.value,
        condition_sets=[
            ["volatility contraction", "range expansion"],
            ["inside range cluster", "expansion close"],
            ["low realized volatility", "participation expansion"],
        ],
        regimes=["VOL_CONTRACTION", "VOL_EXPANSION", "HIGH_VOL"],
        timeframes=["15m", "1h", "1d"],
        entry_rule="Observe range expansion after measured volatility compression.",
        exit_rule="Observe expansion exhaustion or return inside compression area.",
        invalidation_rule="Invalidate when expansion lacks follow-through or participation.",
        required_observations=["realized_volatility", "true_range", "volume", "range_context"],
        base_complexity=0.43,
    ),
    MechanismTemplate(
        mechanism=MechanismFamily.LIQUIDITY_SWEEP.value,
        condition_sets=[
            ["prior swing level sweep", "close back through level"],
            ["stop-zone breach", "fast rejection"],
            ["equal high or low sweep", "acceptance failure"],
        ],
        regimes=["CHOP", "HIGH_VOL", "BEAR"],
        timeframes=["1m", "5m", "15m"],
        entry_rule="Observe sweep of known liquidity level followed by rejection.",
        exit_rule="Observe return to opposing range area or loss of rejection structure.",
        invalidation_rule="Invalidate when price accepts beyond swept level.",
        required_observations=["swing_levels", "wick_rejection", "close_location", "volume"],
        base_complexity=0.46,
    ),
    MechanismTemplate(
        mechanism=MechanismFamily.TREND_CONTINUATION.value,
        condition_sets=[
            ["higher high", "higher low continuation"],
            ["trend pullback", "moving average hold"],
            ["impulse leg", "shallow retracement"],
        ],
        regimes=["TRENDING", "BULL", "RISK_ON"],
        timeframes=["15m", "1h", "1d"],
        entry_rule="Observe pullback stabilization in the direction of the existing trend.",
        exit_rule="Observe trend structure break or measured continuation completion.",
        invalidation_rule="Invalidate when trend structure breaks against the expected direction.",
        required_observations=["trend_structure", "retracement_depth", "moving_average", "volume"],
        base_complexity=0.39,
    ),
    MechanismTemplate(
        mechanism=MechanismFamily.REVERSAL.value,
        condition_sets=[
            ["failed breakdown", "higher low"],
            ["failed breakout", "lower high"],
            ["exhaustion move", "structure reversal"],
        ],
        regimes=["BEAR", "RISK_OFF", "CHOP"],
        timeframes=["5m", "15m", "1h"],
        entry_rule="Observe failed continuation followed by early opposite structure.",
        exit_rule="Observe reversal target area or failed opposite structure.",
        invalidation_rule="Invalidate when original direction resumes with acceptance.",
        required_observations=["failed_break", "market_structure", "momentum_slope", "volume"],
        base_complexity=0.44,
    ),
    MechanismTemplate(
        mechanism=MechanismFamily.EVENT_REACTION.value,
        condition_sets=[
            ["scheduled event", "first reaction fades"],
            ["surprise move", "second-order acceptance"],
            ["news impulse", "volatility normalization"],
        ],
        regimes=["VOL_EXPANSION", "RISK_OFF", "HIGH_VOL"],
        timeframes=["5m", "15m", "1d"],
        entry_rule="Observe post-event reaction pattern after initial volatility settles.",
        exit_rule="Observe reaction exhaustion or event-window expiry.",
        invalidation_rule="Invalidate when event data remains unavailable or reaction pattern changes.",
        required_observations=["event_time", "initial_reaction", "volatility", "volume"],
        base_complexity=0.49,
    ),
]


def iter_search_space() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    max_condition_sets = max(len(template.condition_sets) for template in MECHANISM_TEMPLATES)
    max_regimes = max(len(template.regimes) for template in MECHANISM_TEMPLATES)
    max_timeframes = max(len(template.timeframes) for template in MECHANISM_TEMPLATES)
    for condition_index in range(max_condition_sets):
        for regime_index in range(max_regimes):
            for timeframe_index in range(max_timeframes):
                for template in MECHANISM_TEMPLATES:
                    if condition_index >= len(template.condition_sets):
                        continue
                    if regime_index >= len(template.regimes):
                        continue
                    if timeframe_index >= len(template.timeframes):
                        continue
                    conditions = template.condition_sets[condition_index]
                    regime = template.regimes[regime_index]
                    timeframe = template.timeframes[timeframe_index]
                    rows.append(
                        {
                            "mechanism": template.mechanism,
                            "conditions": list(conditions),
                            "regime": regime,
                            "timeframe": timeframe,
                            "entry_observation_rule": template.entry_rule,
                            "exit_observation_rule": template.exit_rule,
                            "invalidation_rule": template.invalidation_rule,
                            "complexity_score": _complexity(template.base_complexity, condition_index, timeframe),
                            "source_search_config": SourceSearchConfig(
                                search_family=template.mechanism,
                                evidence_scope="TECHNICAL_MECHANISM_OBSERVATION",
                                required_observations=list(template.required_observations),
                                metadata={"regime": regime, "timeframe": timeframe},
                            ).to_dict(),
                        }
                    )
    return rows


def _complexity(base: float, condition_index: int, timeframe: str) -> float:
    timeframe_add = {"1m": 0.06, "5m": 0.05, "15m": 0.04, "1h": 0.03, "1d": 0.02, "session": 0.04}.get(timeframe, 0.03)
    return round(min(1.0, base + condition_index * 0.04 + timeframe_add), 3)
