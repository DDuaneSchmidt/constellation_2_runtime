# Aegis Research Validation Engine V1 Rework Plan

## Objective

Move `ACC-20260530-008 Research Validation Engine` from an implemented prototype to a validated governance boundary by closing the hostile audit P0/P1 findings.

Target hostile audit score after rework:

```text
>= 85/100
```

## Affected Files and Components

Expected implementation files:

- `ops/aegis/research_lab/research_validation_engine_v1.py`
- `ops/tools/build_aegis_research_validation_engine_v1.py`
- `ops/tools/run_aegis_research_validation_engine_self_check_v1.py`
- candidate generation/readiness boundary modules that ingest research-derived inputs
- Research UI read model in `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`
- Research UI rendering in `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`
- domain client in `constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js`
- operator portal manifest `aegis/modules/operator_portal/aegis.module.yaml`
- tests under `constellation_2/common/tests/`
- Research UI tests under `constellation_2/phaseL/ui/tests/`
- Change Control register `aegis/change_control/aegis_change_control_register_v1.json`

Expected artifact families:

- `aegis_research_hypothesis_registry_v1`
- `aegis_research_validation_protocol_v1`
- `aegis_research_validation_run_v1`
- `aegis_research_validation_result_v1`
- `aegis_research_promotion_gate_v1`
- `aegis_research_outcome_feedback_v1`
- candidate artifacts carrying research validation enforcement fields when research-derived

## Architectural Options

### Option A: Keep current engine and strengthen self-check only

Description:

Add more validation checks around existing artifacts without changing evaluator architecture or candidate boundaries.

Pros:

- Smallest implementation.
- Low migration impact.

Cons:

- Does not close candidate-generation bypass.
- Generic evaluator remains able to skip protocol-specific requirements.
- Hostile audit P0s likely remain.

Verdict:

Rejected.

### Option B: Add protocol-specific evaluators and candidate gate enforcement in-place

Description:

Keep the V1 artifact families, but harden them with protocol-specific evaluators, strict artifact binding, immutable reproducibility fields, bias-control statuses, and candidate-boundary enforcement.

Pros:

- Closes P0s without introducing a V2 artifact family.
- Preserves existing Research UI/API integration.
- Keeps migration controlled and auditable.
- Fits the current Change Control item.

Cons:

- Requires careful integration with candidate boundaries.
- Requires more extensive tests.

Verdict:

Recommended.

### Option C: Create Research Validation Engine V2

Description:

Create new V2 artifact families and leave V1 as a prototype.

Pros:

- Clean schema break.
- Avoids compatibility complexity.

Cons:

- Larger migration.
- More architecture churn.
- V1 remains misleading unless deprecated everywhere.

Verdict:

Not recommended for this rework.

## Recommended Option

Use **Option B: in-place V1 hardening**.

Rationale:

The problem is not that the V1 artifact names are wrong. The problem is that V1 lacks hard enforcement and protocol-specific evaluation. An in-place hardening pass gives the smallest auditable path to validation while preserving current UI and artifact consumers.

## Implementation Phases

### Phase 1: Candidate promotion enforcement

Tasks:

1. Inventory every candidate-generation path that can consume research or hypothesis lineage.
2. Add a research validation boundary function:

   ```text
   resolve_research_promotion_eligibility(hypothesis_id, hypothesis_version, day)
   ```

3. Require candidate producers to reject research-derived inputs unless promotion gate status is eligible.
4. Add candidate artifact fields for research validation enforcement status.
5. Add tests proving non-supported hypotheses cannot appear as research-derived actionable candidate inputs.

Output:

- Candidate generation bypass risk closed.

### Phase 2: Protocol-specific evaluator split

Tasks:

1. Replace final generic status emission with protocol-specific evaluators:
   - `evaluate_event_window_return_v1`,
   - `evaluate_mean_reversion_forward_return_v1`,
   - `evaluate_post_signal_survival_v1`.
2. Keep shared metric helpers only for common calculations.
3. Require each evaluator to declare:
   - required metrics,
   - required sources,
   - blocking checks,
   - support thresholds,
   - disproof thresholds.
4. Fail closed if protocol ID has no evaluator.

Output:

- Generic evaluator can no longer mark hypotheses `SUPPORTED`.

### Phase 3: Artifact binding and source ownership

Tasks:

1. Add artifact binding status to validation runs.
2. Validate hypothesis-specific source artifacts match `hypothesis_id`.
3. Require shared artifacts to declare allowed shared-source semantics.
4. Reject wrong-hypothesis samples.
5. Add source hash checks and fail on missing hashes.

Output:

- Validation samples cannot be silently reused across unrelated hypotheses.

### Phase 4: Immutable reproducibility

Tasks:

1. Add protocol hash to run rows.
2. Add evaluator hash or immutable evaluator reference.
3. Add repository commit or explicit unavailable blocker.
4. Add input context hash and source artifact hash map.
5. Add configuration hash.
6. Make `SUPPORTED` impossible when immutable references are missing.

Output:

- Validation runs can be reproduced or fail closed.

### Phase 5: Bias-control enforcement

Tasks:

1. Implement deterministic bias-control checks:
   - lookahead,
   - survivorship,
   - selection bias,
   - duplicate events,
   - stale data.
2. Store per-control status and evidence.
3. Make blocking bias-control failures prevent `SUPPORTED`.
4. Add tests for future-dated forward samples, duplicate events, stale data, and non-predeclared universes.

Output:

- Bias controls are enforced, not just documented.

### Phase 6: Defensible sample-size policy

Tasks:

1. Replace bare `minimum_sample_size` with `sample_size_policy`.
2. Add per-protocol rationale.
3. Add distinct date and symbol constraints where applicable.
4. Record sample-size status and reason in results.
5. Add tests proving sample count alone is insufficient if distinct-date or distinct-symbol requirements fail.

Output:

- Sample-size rules become auditable and harder to game.

### Phase 7: UI and operator explanation

Tasks:

1. Update Research UI to show:
   - validation status,
   - promotion eligibility,
   - required metric status,
   - sample-size status,
   - bias-control status,
   - artifact binding status.
2. Keep all trading/broker/autonomous actions absent.
3. Capture a post-load screenshot that proves why hypotheses are or are not eligible.

Output:

- Operator can understand eligibility without reading raw artifacts.

### Phase 8: Change Control and hostile re-audit

Tasks:

1. Add implementation records to `ACC-20260530-008`.
2. Add validation records with:
   - tests,
   - artifacts,
   - API evidence,
   - browser screenshot,
   - safety proof.
3. Run hostile re-audit.
4. Move to `VALIDATING` only after implementation evidence exists.
5. Move to `VALIDATED` only after hostile audit score is at least `85/100` and no P0 remains.

## Migration Impact

Expected low-to-medium migration impact:

- Artifact family names remain stable.
- Existing Research UI can continue reading the same endpoint with added fields.
- Existing current-day V1 artifacts may need regeneration.
- Candidate producers must classify whether inputs are research-derived.
- Research-derived candidate paths will fail closed until promotion gate evidence is present.

No migration should alter:

- sleeve thresholds,
- live/broker/autonomous policy,
- candidate scoring logic outside research eligibility enforcement,
- canonical trading artifacts.

## Testing Strategy

Required unit tests:

- non-supported research-derived candidate input is rejected,
- missing promotion gate rejects candidate input,
- stale or wrong-version promotion gate rejects candidate input,
- `EVENT_WINDOW_RETURN_V1` cannot support without benchmark-relative return,
- `MEAN_REVERSION_FORWARD_RETURN_V1` cannot support with future-dated forward close,
- `POST_SIGNAL_SURVIVAL_V1` cannot support without post-signal timestamp ordering,
- wrong-hypothesis sample artifact is rejected,
- missing source hash blocks support,
- missing code commit or immutable reference blocks support,
- blocking bias-control failure blocks support,
- sample count alone does not pass if distinct-date or distinct-symbol requirements fail,
- `SUPPORTED` remains candidate-review-only and never tradeable,
- safety gates remain disabled.

Required integration tests:

- research validation engine builds all artifacts,
- self-check fails on each hostile malformed fixture,
- candidate generation/readiness boundary rejects unsupported research lineage,
- Research UI shows eligibility reason and no trading action,
- Change Control validation passes.

Required browser tests:

- Research page shows validation status,
- Research page shows why zero hypotheses are eligible when under-sampled,
- Research page does not show candidate approval, broker, or trading actions,
- Research evidence details remain read-only.

## Validation Strategy

Minimum validation commands after rework:

```bash
TARGET_DAY=2026-05-30 npm run aegis:research-validation-engine
TARGET_DAY=2026-05-30 npm run aegis:research-validation-engine-self-check
pytest constellation_2/common/tests/test_aegis_research_validation_engine_v1.py
pytest relevant candidate-boundary tests
pytest relevant research UI tests
npm run aegis:change-control-validate
TARGET_DAY=2026-05-30 npm run aegis:portal-smoke
TARGET_DAY=2026-05-30 npm run aegis:audit
```

Hostile re-audit must include malformed fixtures for:

- generic support without required benchmark metric,
- wrong-hypothesis sample source,
- future-dated sample,
- duplicate event,
- stale data,
- missing immutable code reference,
- candidate bypass attempt.

## Hostile Audit Success Criteria

The rework is successful only if:

1. Candidate generation requires promotion eligibility for research-derived inputs.
2. Protocol-specific evaluators produce final statuses.
3. Protocol-required metrics are enforced.
4. Artifact ownership and hypothesis binding are validated.
5. Reproducibility includes immutable version references.
6. Bias controls are enforced with blocking statuses.
7. Sample-size policy is defensible and auditable.
8. No current or malformed fixture can produce false `SUPPORTED`.
9. Research UI clearly explains promotion eligibility and blocking reasons.
10. Safety gates remain unchanged and disabled.
11. Hostile audit score is at least `85/100`.

## Stop Conditions

Stop implementation and keep `ACC-20260530-008` at `IMPLEMENTED` if:

- candidate promotion enforcement requires changing candidate generation logic beyond research-derived eligibility gating,
- any safety gate would need to change,
- any protocol-specific evaluator cannot prove required data binding,
- any `SUPPORTED` result cannot include immutable reproducibility references.
