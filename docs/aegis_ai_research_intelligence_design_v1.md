# Aegis AI Research Intelligence Design v1

## Architecture
Deterministic research artifacts feed a narrow advisory builder. The builder produces five dimension artifacts and a UI summary artifact. No artifact is consumed by trading, broker, allocation mutation, retirement, paper observation creation, or deterministic gate mutation paths.

## Data Flow
Existing deterministic artifacts -> advisory dimension builders -> AI research intelligence summary -> Research page advisory section.

## Authority Boundary
The layer can explain, critique, diagnose, and suggest. It cannot approve, block, trade, retire, allocate, create paper observations, or change workflow state. Deterministic Aegis artifacts retain authority.

## Implementation Notes
The MVP uses deterministic heuristics over canonical artifacts. It intentionally avoids calling external AI services so validation and replay remain stable. Future model-backed analysis may be added only behind the same artifact contracts, source-hash binding, and advisory-only flags.

## UI Design
The Research page adds one compact section labeled `AI research analysis — advisory only.` It shows per-hypothesis concerns, root-cause highlights, repair suggestions, duplicate warnings, evidence synthesis, and confidence. It renders the summary artifact only and must not infer authoritative state.

## Auditability
Each output records input artifact hashes, source artifact paths, computed time, deterministic rerun id, policy version, content hash, and safety flags.
