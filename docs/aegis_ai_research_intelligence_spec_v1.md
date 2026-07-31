# Aegis AI Research Intelligence Spec v1

## Artifacts
- `aegis_ai_research_critic_v1`: per-hypothesis critique rows.
- `aegis_ai_root_cause_analysis_v1`: per-hypothesis root-cause rows for weak or stalled hypotheses.
- `aegis_ai_hypothesis_repair_advisor_v1`: advisory repair rows for REDESIGN or PAUSE cases.
- `aegis_ai_evidence_synthesis_v1`: evidence summaries for hypotheses with outcomes or samples, and sparse-evidence limitations.
- `aegis_ai_duplicate_detection_v1`: pairwise overlap rows and per-hypothesis duplicate summaries.
- `aegis_ai_research_intelligence_summary_v1`: compact UI read model combining all dimensions.

## Recommendation Types
Allowed `ai_recommendation_type` values are `WATCH`, `INVESTIGATE`, `REPAIR_SUGGESTED`, `DATA_NEEDED`, `POSSIBLE_DUPLICATE`, `EVIDENCE_TOO_SPARSE`, and `NO_AI_CONCERN`.

## Determinism
This MVP is deterministic and local. It is an AI-style advisory layer with explicit non-authority flags, not a source of truth. For the same target day and input artifacts, rows, hashes, and recommendation types must be stable except generated timestamps.

## Row Contracts
Critic rows include economic plausibility critique, testability critique, sample-frequency warning, data-quality warning, regime-dependency warning, false-discovery warning, confidence level, reason codes, source artifact paths, and source hashes.

Root-cause rows include likely causes, supporting artifacts, confidence level, suggested investigation, David-action likelihood, source paths, and source hashes.

Repair rows include repair options and advisory-only reasons for REDESIGN or PAUSE hypotheses.

Evidence rows include working/failing synthesis, winning/losing condition summaries, sample limitations, sparse-evidence flag, confidence level, source paths, and source hashes.

Duplicate rows include duplicate score, overlap reason, shared instruments, shared factor exposure, shared signal behavior, and merge/reject suggestion.

Summary rows include hypothesis id, display name, summaries from all five dimensions, AI confidence, AI recommendation type, and safety statement.
