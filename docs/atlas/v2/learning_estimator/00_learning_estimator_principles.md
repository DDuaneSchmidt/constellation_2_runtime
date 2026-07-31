# Atlas V2 Learning Estimator Principles

The Atlas V2 Learning Value Estimator V1 is read-only and audit-only. It measures whether Atlas attention was pointed at records that produced learning, regret reduction, and behavior change.

The estimator reads existing Atlas V2 records only: CheapExperiment, ExperienceEvent, AttentionDecision, and LearningVelocityMetric. It may append LearningEstimate, LearningEstimateEvaluation, AttentionSignal, and EstimatorPerformanceReport records.

No estimator output is validation authority. No output may create or approve a candidate, sleeve, paper position, broker action, trade, recommendation, autonomous action, or capital allocation.

Core invariant: consumers do not invent truth. Estimator records compare observed Atlas V2 learning records and remain subordinate to the verified runtime graph and runtime truth kernel.
