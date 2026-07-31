# Independence Requirements

A trustworthy learning signal requires expected labels to be generated before or independently from actual outcomes. Actual labels should come from outcome measurements, calibration results, or behavior changes that are not derived from the same expected-label source.

For historical conversions, quality scores may support record inclusion, but they must not serve as both expected learning value and actual learning value when interpreting predictive-learning metrics.

Reports that are `HIGHLY_SHARED` or `CIRCULAR` should block interpretation of correlation as predictive learning strength.
