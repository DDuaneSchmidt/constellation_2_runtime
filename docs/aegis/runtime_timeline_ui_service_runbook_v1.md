# Aegis Runtime Timeline UI Service Runbook v1

Purpose: keep the Aegis operator UI on `127.0.0.1:8787` managed, restartable, and easy to validate after Runtime Timeline changes.

## Service Ownership

The primary operator UI is managed by the user systemd unit:

```bash
systemctl --user status constellation-phasel-ui.service --no-pager
```

Expected process:

```bash
/usr/bin/python3 -B -m constellation_2.phaseL.ui.server.run_ops_dashboard_v1 --host 127.0.0.1 --port 8787
```

The service unit is installed at:

```text
/home/node/.config/systemd/user/constellation-phasel-ui.service
```

The repository copy is:

```text
ops/systemd/user/constellation-phasel-ui.service
```

## Restart UI

Preferred restart:

```bash
systemctl --user restart constellation-phasel-ui.service
```

Project wrapper:

```bash
npm run aegis:ui:restart
```

After restart, confirm the main PID changed and the service is active:

```bash
systemctl --user status constellation-phasel-ui.service --no-pager
ps -ef | rg 'run_ops_dashboard_v1.*8787'
```

## View Logs

Recent logs:

```bash
journalctl --user -u constellation-phasel-ui.service -n 100 --no-pager
```

Follow logs:

```bash
journalctl --user -u constellation-phasel-ui.service -f
```

## Validate Runtime Timeline

Route check:

```bash
curl -s -o /tmp/aegis_runtime_timeline_8787.html -w '%{http_code}\n' \
  http://127.0.0.1:8787/aegis-runtime-timeline
```

Expected: `200`.

Navigation check:

```bash
chromium --headless --disable-gpu --no-sandbox --virtual-time-budget=5000 \
  --dump-dom http://127.0.0.1:8787/aegis-runtime-timeline \
  | rg 'Runtime Timeline|Market Session Timeline|Scheduled Jobs|Current Pipeline State'
```

API projection check:

```bash
curl -s 'http://127.0.0.1:8787/api/aegis/operator/state-snapshot/latest?day=2026-05-21' \
  | jq '{ok, day:.data.day_utc, timeline_schema:.data.runtime_timeline_projection.schema_id, scheduled_jobs:(.data.runtime_timeline_projection.scheduled_jobs|length), current_stage:.data.runtime_timeline_projection.current_pipeline_state.current_stage}'
```

Expected:

```json
{
  "ok": true,
  "timeline_schema": "aegis_runtime_timeline_projection"
}
```

## Safety Boundary

The Runtime Timeline workspace is observability-only. Restarting the UI must not change:

- scoring logic
- certification logic
- governance artifacts
- broker submit/transmit
- autonomous execution
- trade advice

