# Expected Vs Actual Learning

LearningEstimate records capture expected_learning_value before evaluation. LearningEstimateEvaluation records compare that expectation to observed actual_learning_value.

The required prediction error is:

```text
learning_prediction_error = actual_learning_value - expected_learning_value
```

The required importance-weighted error is:

```text
importance_weighted_learning_error = importance_score * learning_prediction_error
```

Positive error means the source produced more learning than expected. Negative error means Atlas overestimated the learning value. This is calibration evidence only; it does not promote the source into validation or trading authority.
