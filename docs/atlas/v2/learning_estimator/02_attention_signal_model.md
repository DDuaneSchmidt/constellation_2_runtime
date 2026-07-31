# Attention Signal Model

AttentionSignal records turn expected learning, importance, regret, and attention cost into an attention_priority_score.

V1 computes priority from four bounded inputs:

```text
attention_priority_score =
  expected_learning_value * 0.40
+ importance_score * 0.25
+ expected_regret_if_ignored * 0.25
- attention_cost_estimate * 0.10
```

Allowed recommended_attention_action values are IGNORE, REJECT, WATCH, CHEAP_TEST, PROMOTE_TO_TIER_2, and REQUIRES_GATE_REVIEW.

These actions route attention inside the read-only research workflow. They do not create candidates, sleeves, paper positions, trades, capital allocations, recommendations, or validation authority.
