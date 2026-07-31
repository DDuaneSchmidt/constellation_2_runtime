# Label Provenance

Every expected label and actual label needs field-level provenance. The audit records the fields used for expected label construction, the fields used for actual label construction, and any shared fields.

Expected label examples include `LearningEstimate.expected_learning_value` and source decision fields. Actual label examples include `LearningEstimateEvaluation.actual_learning_value` and source outcome fields.

If both expected and actual labels trace back to `HistoricalExperienceRecord.experience_quality_score`, the labels are not independent even when intermediate object fields have different names.
