# paper_day_readiness_runbook_v1

## Scope

Authoritative operator runbook for the validated paper-day readiness proof path and the single explicit micro-live boundary proof path in `/home/node/constellation`.

This runbook remains valid for proof-only readiness exercises.

For live governed PAPER execution authority, root and profile ownership are governed separately by:

- `governance/05_CONTRACTS/C2/execution_root_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_profile_authority_v1.contract.md`

If this runbook conflicts with those authority contracts, the authority contracts win.

## Validated Paper-Day Command

```bash
cd /home/node/constellation
python3 ops/tools/run_paper_day_readiness_proof_v1.py
```

## Preflight Checks

- Run from `/home/node/constellation`.
- Leave `C2_ENABLE_BROKER_TRANSMIT` unset for the paper-day proof command.
- Confirm the repo-owned Phase C fixture exists at `/home/node/constellation/_smoketest_phasec_2026_04_02/`.
- Confirm `/tmp/constellation_2_foundation` is writable.
- Confirm the governed submit path will stay fail-closed unless both `C2_ENABLE_BROKER_TRANSMIT=YES` and submit-boundary `--dry_run NO` are explicitly present.

## Default Live-Boundary Safety

- Live transmit remains disabled by default.
- Setting `C2_ENABLE_BROKER_TRANSMIT=YES` alone does not transmit while submit-boundary `--dry_run YES` remains in force.
- Any live submit path must prove both `C2_ENABLE_BROKER_TRANSMIT=YES` and submit-boundary `--dry_run NO`.
- No silent fallback from broker transmit to dry-run is allowed.

## Preflight Proof

1. Run the canonical proof command:

```bash
cd /home/node/constellation
python3 ops/tools/run_paper_day_readiness_proof_v1.py
```

2. Confirm the command prints governed artifact paths for:
   - `broker_submission_record`
   - `execution_truth_gap`
   - `authorization_verdict`
   - `runtime_trace_bundle`
   - `replay_manifest`
   - `authority_registry`
   - `publication_gate_result`
   - `promotion_gate_result`

## Broker Transmit Disabled Proof

Use the governed proof output as the fail-closed truth root and prove that broker transmit is still blocked by default:

```bash
cd /home/node/constellation
unset C2_ENABLE_BROKER_TRANSMIT
export C2_TRUTH_ROOT=/tmp/constellation_2_foundation/final_readiness_proof_v1/truth_sleeves/PRIMARY/PAPER
python3 constellation_2/phaseD/tools/c2_submit_paper_v5.py \
  --eval_time_utc 2026-04-02T14:30:00Z \
  --phasec_out_dir /home/node/constellation/_smoketest_phasec_2026_04_02 \
  --risk_budget /home/node/constellation/constellation_2/phaseD/inputs/sample_risk_budget.v1.json \
  --ib_host 127.0.0.1 \
  --ib_port 4002 \
  --ib_client_id 7 \
  --ib_account DUO847203 \
  --dry_run NO
```

The required result is an immediate fail-closed abort containing `FAIL_CLOSED` and `C2_ENABLE_BROKER_TRANSMIT=YES`.

## Explicit Enablement Proof

Prove that transmit is still safe when `C2_ENABLE_BROKER_TRANSMIT=YES` is set but submit-boundary `--dry_run YES` remains in force:

```bash
cd /home/node/constellation
export C2_ENABLE_BROKER_TRANSMIT=YES
export C2_TRUTH_ROOT=/tmp/constellation_2_foundation/final_readiness_proof_v1/truth_sleeves/PRIMARY/PAPER
python3 constellation_2/phaseD/tools/c2_submit_paper_v5.py \
  --eval_time_utc 2026-04-02T14:30:00Z \
  --phasec_out_dir /home/node/constellation/_smoketest_phasec_2026_04_02 \
  --risk_budget /home/node/constellation/constellation_2/phaseD/inputs/sample_risk_budget.v1.json \
  --ib_host 127.0.0.1 \
  --ib_port 4002 \
  --ib_client_id 7 \
  --ib_account DUO847203 \
  --dry_run YES
```

The required result is artifact-only dry-run output with no broker connection and no live submission.

## Micro-Live Checklist

1. Complete the preflight proof and broker transmit disabled proof first.
2. Use one governed symbol only and a micro-size quantity only from an already produced Phase C identity set. Do not hand-edit artifacts.
3. Verify the submit truth root is authoritative for the intended day:
   - `canonical_authority_head.v1.json`
   - `authorization_gate_verdict.v1.json`
   - `trade_submit_readiness_c2_v1/status.json`
4. Verify the kill switch is inactive and entries are allowed.
5. If the operator path is orchestrator-controlled, set `C2_GOVERNED_SUBMIT_DRY_RUN=NO` explicitly so the orchestrator passes submit-boundary `--dry_run NO`.
6. Set `C2_ENABLE_BROKER_TRANSMIT=YES` explicitly.
7. Execute exactly one single-symbol micro-size live path through the existing governed submit boundary.
8. Unset `C2_ENABLE_BROKER_TRANSMIT` immediately after the micro-live proof path completes or aborts.

## Single-Symbol Micro-Size Live Path

```bash
cd /home/node/constellation
export C2_ENABLE_BROKER_TRANSMIT=YES
export C2_GOVERNED_SUBMIT_DRY_RUN=NO
export C2_TRUTH_ROOT=<authoritative_truth_root_for_day>
python3 constellation_2/phaseD/tools/c2_submit_paper_v5.py \
  --eval_time_utc <EVAL_TIME_UTC_Z> \
  --phasec_out_dir <repo_owned_single_symbol_phasec_dir> \
  --risk_budget /home/node/constellation/constellation_2/phaseD/inputs/sample_risk_budget.v1.json \
  --ib_host 127.0.0.1 \
  --ib_port 4002 \
  --ib_client_id 7 \
  --ib_account <governed_DU_account> \
  --dry_run NO
unset C2_ENABLE_BROKER_TRANSMIT
unset C2_GOVERNED_SUBMIT_DRY_RUN
```

## Post-Trade Replay / Trace / Authority Verification

After the single-symbol micro-live path, verify:

- `<truth_root>/execution_evidence_v1/submissions/<DAY>/<submission_id>/broker_submission_record.v2.json`
- `<truth_root>/reports/execution_completion_gap_report_v1/<DAY>/execution_completion_gap_report.v1.json`
- `<truth_root>/reports/authorization_gate_verdict_v1/<DAY>/authorization_gate_verdict.v1.json`
- `<truth_root>/reports/runtime_trace_bundle_v1/<DAY>/<submission_id>.runtime_trace_bundle.v1.json`
- `<replay_truth_root>/reports/replay_manifest_v1/<DAY>/<submission_id>.replay_manifest.v1.json`
- `/tmp/constellation_2_foundation/advisor_runtime/PAPER/reports/authority_registry_v1/<DAY>/authority_registry.v1.json`
- `/tmp/constellation_2_foundation/advisor_runtime/PAPER/publication_gate_result_v1/<DAY>/publication_gate_result.v1.json`
- `/tmp/constellation_2_foundation/advisor_runtime/PAPER/promotion_gate_result_v1/<DAY>/promotion_gate_result.v1.json`

The micro-live proof is complete only when replay, trace, and authority artifacts are present and read cleanly.

## Pre-Live Abort Conditions

Abort before enabling broker transmit if any of the following are true:

- `canonical_authority_head.v1.json` is missing, stale, non-authoritative, or for the wrong day.
- `authorization_gate_verdict.v1.json` is not `PASS` or `BOOTSTRAP_PASS`.
- `trade_submit_readiness_c2_v1/status.json` is missing, not `OK`, not `PAPER`, or bound to the wrong account.
- `global_kill_switch_state.v1.json` is active or `allow_entries` is not `true`.
- the identity set is not single-symbol and micro-size.
- the operator cannot prove the exact submit-boundary `--dry_run NO` path that will be invoked.
- `C2_ENABLE_BROKER_TRANSMIT` would need to remain set beyond the controlled micro-live window.

## Immediate Abort Conditions

Abort immediately and unset `C2_ENABLE_BROKER_TRANSMIT` if any of the following occur:

- `FAIL_CLOSED: broker transmit disabled by default`
- missing `broker_submission_record.v2.json`
- missing runtime trace bundle artifact references or sha256 values
- non-OK replay manifest
- missing authority registry, publication gate result, or promotion gate result
- any account mismatch, gate mismatch, or unexpected symbol/size mismatch

## Expected Paper-Day Success Artifacts

The validated readiness proof writes its evidence bundle under `/tmp/constellation_2_foundation/final_readiness_proof_v1/`.

Required evidence paths:

- `truth_sleeves/PRIMARY/PAPER/execution_evidence_v1/submissions/2026-04-02/<submission_id>/broker_submission_record.v2.json`
- `truth_sleeves/PRIMARY/PAPER/reports/execution_completion_gap_report_v1/2026-04-02/execution_completion_gap_report.v1.json`
- `truth_sleeves/PRIMARY/PAPER/reports/authorization_gate_verdict_v1/2026-04-02/authorization_gate_verdict.v1.json`
- `truth_sleeves/PRIMARY/PAPER/reports/runtime_trace_bundle_v1/2026-04-02/<submission_id>.runtime_trace_bundle.v1.json`
- `replay_truth/reports/replay_manifest_v1/2026-04-02/<submission_id>.replay_manifest.v1.json`
- `/tmp/constellation_2_foundation/advisor_runtime/PAPER/reports/authority_registry_v1/2026-04-02/authority_registry.v1.json`
- `/tmp/constellation_2_foundation/advisor_runtime/PAPER/publication_gate_result_v1/2026-04-02/publication_gate_result.v1.json`
- `/tmp/constellation_2_foundation/advisor_runtime/PAPER/promotion_gate_result_v1/2026-04-02/promotion_gate_result.v1.json`

## What Must Not Vary

- The validated paper-day command.
- The proof day `2026-04-02`.
- The repo-owned Phase C identity fixture used by the proof.
- The requirement that broker transmission remains explicitly opt-in and fail-closed by default.
