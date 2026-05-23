# Aegis EOD Provider Credentials Runbook

## Credential File

Store US_EQUITIES_EOD provider credentials outside the git repo at:

`/home/node/constellation_runtime_data/config/private/aegis_eod_provider.env`

Required keys:

```bash
TIINGO_API_KEY=...
ALPHA_VANTAGE_API_KEY=...
AEGIS_MARKET_DATA_PROVIDER_PRIMARY=TIINGO
AEGIS_MARKET_DATA_PROVIDER_FALLBACK=ALPHA_VANTAGE
```

Permissions must stay private to the runtime user:

```bash
chmod 700 /home/node/constellation_runtime_data/config/private
chmod 600 /home/node/constellation_runtime_data/config/private/aegis_eod_provider.env
chown node:node /home/node/constellation_runtime_data/config/private/aegis_eod_provider.env
```

Do not commit this file or copy credentials into repo files, logs, screenshots, tickets, or artifacts.

## Systemd Wiring

Managed Aegis market-data services load the private file with:

```ini
EnvironmentFile=/home/node/constellation_runtime_data/config/private/aegis_eod_provider.env
```

After edits, reload user units:

```bash
systemctl --user daemon-reload
```


## Single Final EOD Pipeline

`aegis-market-data-final-eod-v1.service` starts with the certification-grade EOD path:

```bash
npm run aegis:eod-tiingo-certify -- --day YYYY-MM-DD
```

That command performs the governed flow: provider fetch when needed, canonical `final_eod_market_data.v1.json`, validation, US_EQUITIES_EOD domain certification, then downstream projections.

The default is artifact-first idempotency. If the same-day canonical artifact is already valid, covers the full governed universe, and has valid OHLCV rows, provider calls are skipped and domain certification is rerun from the artifact. The result should show `provider_fetch_status=SKIPPED_VALID_ARTIFACT_EXISTS`.

Use provider fetch only when the artifact is missing, stale, incomplete, invalid, or when explicitly forcing a refresh:

```bash
npm run aegis:eod-tiingo-certify -- --day YYYY-MM-DD --force-provider-refresh
```

To disable artifact reuse for validation work:

```bash
npm run aegis:eod-tiingo-certify -- --day YYYY-MM-DD --no-reuse-valid-artifact
```

## Validate Provider Status

```bash
npm run aegis:eod-provider-status -- --day YYYY-MM-DD
```

Expected credential state is `CONFIGURED` for `TIINGO_API_KEY` and `ALPHA_VANTAGE_API_KEY`. The command must not print key values.

## Run Managed EOD Certification

For normal timer operation, no `TARGET_DAY` override is required. For a dated validation run:

```bash
systemctl --user set-environment TARGET_DAY=YYYY-MM-DD
systemctl --user restart aegis-market-data-final-eod-v1.service
systemctl --user status aegis-market-data-final-eod-v1.service --no-pager
systemctl --user unset-environment TARGET_DAY
```

Then confirm US_EQUITIES_EOD is certified:

```bash
jq '.domains[] | select(.domain_id=="US_EQUITIES_EOD") | {domain_id,certification_status,completeness_score}' \
  /home/node/constellation_runtime_data/truth/reports/domain_certification_v1/YYYY-MM-DD/domain_certification.v1.json
```

## Rotate Keys

1. Edit the private EnvironmentFile locally without printing values.
2. Keep permissions at `600`.
3. Run `systemctl --user daemon-reload`.
4. Restart `aegis-market-data-final-eod-v1.service` for a validation day.
5. Run provider status and domain certification checks.

## Leak Check

Do not use commands that print environment values. `systemctl --user show aegis-market-data-final-eod-v1.service --property=Environment` should show only non-secret inline unit environment, not EnvironmentFile contents.

To check generated report roots for accidental secret values, use a local script that reads secrets from process environment and prints only matching file paths/counts, never the secret values.
