# Forbidden Boundaries

The writer is a learning-record adapter only. It must not add trading authority, broker execution, autonomous execution, live-market access, sleeves, candidates, paper positions, allocation, recommendations, or validation authority.

Inputs containing prohibited authority fields are rejected before any records are written. Emitted records are append-only and suitable for later `LearningEstimator` reads, not for direct execution or recommendation.
