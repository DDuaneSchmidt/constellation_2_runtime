# Learning Velocity Metrics

Learning velocity measures how quickly Atlas converts predictions into outcomes, calibration, regret measurement, and behavior change.

Core metrics:

- prediction_outcome_cycles: completed prediction to outcome cycles.
- cheap_experiments_completed: cheap experiments with recorded outcomes.
- actual_learning_total: summed realized learning value.
- average_learning_per_cycle: actual_learning_total divided by prediction_outcome_cycles.
- rejection_count: experiments rejected or stopped.
- promotion_count: experiments promoted through a gate.
- promotion_rate: promotion_count divided by cheap_experiments_completed.
- importance_weighted_regret_total: regret weighted by importance.

Learning velocity is useful only when every cycle remains traceable to recorded outcomes. High volume without outcomes is not progress.
