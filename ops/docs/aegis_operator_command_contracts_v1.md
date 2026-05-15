# Aegis Operator Command Contracts v1

This guide is the operator contract for release, launcher, and scheduled Aegis commands. It is intentionally strict: use the supported command form exactly, and treat missing/stale pointer evidence as a blocker.

## Command Contract Table

| Command | Correct invocation form | Invalid invocation forms | Source or active release | Pointer files read | Mutates files | Safe validation example |
| --- | --- | --- | --- | --- | --- | --- |
| Current-release launcher | `/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh <tool_without_py> [args...]` or `/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh npm <npm args...>` | `run_current_release_tool_v1.sh python3 ops/tools/tool.py`; any tool containing `/`, `..`, `.py`, or empty tool name | Active release | `/home/node/constellation_runtime_data/truth/releases/current_release.v1.json` unless `AEGIS_CURRENT_RELEASE_MANIFEST` overrides it | None by itself; invoked tool may mutate | `/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh explain_aegis_no_paper_trade_v1 --no-journal` |
| No-trade explainer | `/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh explain_aegis_no_paper_trade_v1` | `/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh python3 ops/tools/explain_aegis_no_paper_trade_v1.py` | Active release | Current-release manifest; release manifest; runtime truth artifacts; optional user journal | None | `/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh explain_aegis_no_paper_trade_v1 --no-journal` |
| NPM through launcher | `/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh npm run aegis:kernel:test` | `npm run ...` from arbitrary working directory when active-release behavior is required | Active release | Current-release manifest | Depends on npm script; `aegis:kernel:test` is test-only | `/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh npm run aegis:kernel:test` |
| Build release | `python3 ops/tools/build_constellation_release_v1.py` | Running from non-canonical root; dirty canonical tree; using runtime-generated source inputs | Source | Git HEAD and source tree; no current-release pointer | Creates `/home/node/constellation_releases/<release_id>` | `python3 ops/tools/build_constellation_release_v1.py` |
| Activate release | `python3 ops/tools/activate_constellation_release_v1.py --release_id <release_id>` | `--latest` without verifying intended release; activation with manifest hash unchecked by operator | Source tool activates release | `/home/node/constellation_releases/<release_id>/release_manifest.v1.json`; `/home/node/constellation_active`; release current/control-plane surfaces | Updates `/home/node/constellation_active`; atomically updates `/home/node/constellation_runtime_data/truth/releases/current_release.v1.json`; writes activation receipt and runtime contract/release-current surfaces | Verify manifest hash first, then run activation command |
| Apply patch bundle | `python3 ops/tools/apply_codex_patch_bundle_v1.py --task_id <task_id> --commit_message "<message>"` | Applying with dirty canonical tree; stale `base_commit`; patch touching runtime paths | Source | `/home/node/constellation_patch_inbox/<task_id>/manifest.json`; `changes.patch`; Git HEAD | Unprotects/protects canonical repo, applies patch, runs manifest tests, commits | `python3 ops/tools/verify_patch_bundle_base_commit_v1.py --task_id <task_id>` |
| Paper-ready timer service | systemd only: `aegis-paper-ready-kernel-v1.timer` invokes `run_current_release_tool_v1.sh run_aegis_paper_ready_kernel_v1 --target-day @today_utc@ --environment PAPER --scheduled-run true` | Manual paper-ready runs unless explicitly requested; direct source invocation for scheduled authority | Active release | Current-release manifest; runtime truth artifacts used by the kernel | Writes scheduled paper-ready/readiness artifacts | `systemctl --user status aegis-paper-ready-kernel-v1.timer --no-pager` |
| Post-trade measurement timer service | systemd only: `aegis-post-trade-measurement-v1.timer` invokes `run_current_release_tool_v1.sh run_sleeve_economic_truth_pipeline_v1 --target-day @today_utc@ --truth-root /home/node/constellation_runtime_data/truth --execution-root /home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER --scheduled-run true` | Direct source invocation for scheduled authority when active-release behavior is required | Active release | Current-release manifest; truth and PAPER sleeve roots from service args | Writes post-trade/economic-truth measurement artifacts | `systemctl --user status aegis-post-trade-measurement-v1.timer --no-pager` |

## IB Gateway Ownership

For the Aegis Lite pivot, no IB Gateway unit is part of the default runtime. Aegis Lite is `MANUAL_ONLY`, `ib_automation_status=DEFERRED`, and `broker_required_for_runtime=false`.

The user unit `c2-ib-gateway.service` is deprecated and must remain non-owning: it must not launch IB Gateway, kill IBC/Gateway processes, reconnect, or compete for port `4002`. In the active user runtime it should be disabled and may be a marker-only unit.

The separate `ib-gateway.service` may remain available for explicit operator-started manual paper entry or later reconciliation, but it must be disabled by default and must not be started by Lite EOD, event awareness, Research Lab, sleeve performance reporting, timers, or hidden reconnect logic.

Trading readiness for the old broker-enabled PAPER path was derived from broker/account evidence through `ib_broker_event_probe_v1`, `broker_supply_v1`, `runtime_resilience_authority_v1`, and downstream submit-boundary artifacts. Those paths are legacy/deferred for Lite and are not current Lite readiness authority.

## NPM Contracts Used By Aegis

Run active-release npm scripts through the current-release launcher when validating deployed behavior:

```bash
/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh npm run aegis:kernel:test
```

Source `npm run` is acceptable only when validating source before build or bundle intake. Active-release validation must use the launcher so `cwd` and `PYTHONPATH` are the resolved release.

Submit-capable scripts such as `aegis:paper:submit`, `aegis:paper:auto`, and direct paper-ready commands must not be run unless the operator explicitly requests that action and the submit boundary authorizes PAPER behavior.

## Pointer Rules

After activation, these must agree:

- `/home/node/constellation_active` symlink target
- `/home/node/constellation_runtime_data/truth/releases/current_release.v1.json`
- The release manifest under the resolved release path

Activation must advance `/home/node/constellation_active` and `current_release.v1.json` together. If the symlink advances but `current_release.v1.json` remains stale, the launcher will run the old release. Treat that as a blocker and repair only the current-release pointer after verifying the activated release manifest hash.

## Timer Rules

Timer `ExecStart` lines must pass a bare tool name to the launcher. The bare name maps to `ops/tools/<tool>.py` inside the active release.

Required active-release timer tools:

- `ops/tools/run_aegis_paper_ready_kernel_v1.py`
- `ops/tools/run_sleeve_economic_truth_pipeline_v1.py`

Do not use `python3 ops/tools/...` with the launcher. That shape is not the launcher contract.
