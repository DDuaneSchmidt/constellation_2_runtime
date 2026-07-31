# Atlas V2 Autonomous Research Loop Runner V1

The runner composes existing Atlas V2 safe research components into one bounded loop:

Claim -> Mechanism -> ResearchHypothesis -> CheapExperimentSpec -> mock/historical result evidence -> ExperienceEvent -> LearningEstimator -> LabelIntegrityReport.

It is an evidence and learning-record surface only. It does not create trades, broker orders, sleeves, candidates, paper positions, allocations, recommendations, validation decisions, or live-market access.

The default maximum loop count is 10. Runs must use `mock` or `historical` mode. Any prohibited authority field stops the loop and records a stopped run summary.
