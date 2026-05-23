# Aegis Noon Preflight Rehearsal

The noon preflight rehearsal runs ahead of the canonical 09:50 UTC and 14:50 UTC Aegis Lite sleeve runs. Its purpose is fail-only operator notification: David should only be interrupted when the preflight fails, requires operator action, or predicts that the 09:50 UTC and 14:50 UTC sleeve run is likely to fail.

The systemd source timer is `ops/systemd/user/aegis-noon-preflight-rehearsal-v1.timer`:

- `OnCalendar=Mon..Fri *-*-* 12:00:00 America/New_York`
- service: `aegis-noon-preflight-rehearsal-v1.service`
- command: `run_aegis_noon_preflight_rehearsal_v1`

The timer only runs automatically when enabled by the operator or deployment process. This repo change does not enable production timers.

## Artifacts

The runner writes only the noon preflight family:

- `reports/aegis_noon_preflight_rehearsal_v1/<day>/<run_id>/preflight_run_manifest.v1.json`
- `reports/aegis_noon_preflight_rehearsal_v1/<day>/<run_id>/preflight_status.v1.json`
- `reports/aegis_noon_preflight_rehearsal_v1/<day>/<run_id>/diagnostic_dry_run.v1.json`
- `reports/aegis_noon_preflight_rehearsal_v1/<day>/<run_id>/preflight_alert_ledger.v1.json` only when an alert candidate exists

It may refresh `aegis_chatgpt_control_packet.v1` only when the computed packet is not `REAL_RUNTIME`. If the computed packet would be `REAL_RUNTIME`, the refresh is skipped and no `REAL_RUNTIME` packet is created by preflight.

## Results

Valid result states are:

- `PASS`
- `WARN`
- `FAIL`
- `SKIPPED_NON_TRADING_DAY`

`PASS`, no trade candidates, normal advisory-only state, and non-trading days do not alert.

`READY_PARTIAL` is expected when candidate generation ran for sleeves whose required inputs are available while a separate sleeve remains blocked by a real missing input. The canonical example is `C2_VOL_INCOME_DEFINED_RISK_V1`, which requires canonical `VIX`.

Noon preflight consumes the same canonical market-symbol resolver as market data refresh, candidate diagnostics, sleeve readiness, and fallback provider lookup. For VIX, aliases `VIX`, `^VIX`, `$VIX`, `vix`, and `VIXCLS` normalize to canonical `VIX`. Missing VIX is reported as `VIX`, not as a provider alias.

If VIX cannot be sourced from local cache or the configured fallback provider, noon preflight keeps the VIX-dependent sleeve blocked and writes a stable missing-symbol explanation:

- the blocked sleeve requires `market.volatility.VIX`
- `VIX` was missing from local cache canonical and alias paths
- fallback provider lookup failed
- the run remains `READY_PARTIAL` when other sleeves can proceed to operator review
- no execution is authorized

This is fail-closed behavior. It is not a bypass and it does not create synthetic VIX data.

## Alert Rules

An alert candidate is created only for:

- `FAIL`
- `WARN` requiring operator action
- `PARTIAL_CONTEXT_AFTER_REFRESH`
- missing or stale required inputs
- promoted sleeve library missing or ineffective
- event rules registry missing or empty
- market context snapshot missing or stale
- sizing engine validation failure
- EOD pipeline contract validation failure

The alert subject is:

`[Aegis Preflight FAIL] action needed before 09:50 UTC and 14:50 UTC sleeve run`

The body includes result, failed validations, blockers, exact artifact paths, recommended operator action, whether canonical EOD is at risk, and confirmation that no trades or broker automation occurred.

## Transport Truth

Live email transport is not proven. Until a future explicit proof exists, preflight writes `preflight_alert_ledger.v1` with:

- `delivery_status=DRY_RUN_MESSAGE_BODY_ONLY`
- `email_transport_proven=false`
- `operator_status=alert not live`

The runner does not claim live email delivery.

## Safety Boundaries

The preflight runner does not:

- submit broker orders
- use IB automation
- provide trade advice
- create `REAL_RUNTIME` control packets
- mutate canonical EOD report or EOD manifest families
- activate production

The canonical 09:50 UTC and 14:50 UTC sleeve runs remains the official daily decision run.
