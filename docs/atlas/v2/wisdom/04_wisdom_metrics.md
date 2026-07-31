# Atlas Wisdom Metrics

Canonical score: wisdom_score.

Components, each normalized from 0 to 1:

- importance: expected cost of ignoring the wisdom.
- behavior_change_frequency: how often the wisdom changes future behavior when relevant.
- future_decision_impact: observed improvement in future decisions.
- calibration_impact: effect on prediction calibration.
- adaptation_impact: effect on adapting behavior when reality changes.

Default scoring model:

```text
total =
  importance * 0.25 +
  behavior_change_frequency * 0.25 +
  future_decision_impact * 0.20 +
  calibration_impact * 0.15 +
  adaptation_impact * 0.15
```

Interpretation:

- Below 0.40: weak observation unless behavior impact is newly emerging.
- 0.40 to 0.64: EMERGING or SUPPORTED depending on validation.
- 0.65 to 0.84: SUPPORTED or STRONG with repeated behavior impact.
- 0.85 and above: STRONG only when contradiction remains low and future decision impact is proven.

Score is not authority. Contradicting evidence can contest or retire high-scoring wisdom.
