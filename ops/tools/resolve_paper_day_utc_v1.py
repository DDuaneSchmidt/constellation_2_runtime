#!/usr/bin/env python3
"""
resolve_paper_day_utc_v1.py

Deterministic PAPER day-key resolver for systemd.

Hard constraint (fail-closed):
- The paper-day orchestrator pipeline refuses FUTURE day keys.
- Therefore this resolver MUST return only today_utc (UTC), never a future day.

Policy:
- Compute today_utc (UTC).
- Ensure the Engine Model Registry Gate v1 for today_utc is PASS.
  - If artifact exists and is PASS -> OK.
  - If artifact exists and is not PASS -> FAIL (poisoned day; explicit operator action required).
  - If artifact missing -> run the gate strictly for today_utc; require PASS.

Outputs (shell-safe lines):
  DAY_UTC=YYYY-MM-DD
  PRODUCED_UTC=YYYY-MM-DDT00:00:00Z
  PRODUCER_GIT_SHA=<git sha>

Exit:
- 0 on success
- 2 on fail-closed
"""

from __future__ import annotations

import datetime as _dt
import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

ENGINE_GATE_ROOT = (TRUTH / "reports" / "engine_model_registry_gate_v1").resolve()
ENGINE_GATE_TOOL = (REPO_ROOT / "ops" / "tools" / "run_engine_model_registry_gate_v1.py").resolve()


def _today_utc_day() -> str:
    return _dt.datetime.now(_dt.timezone.utc).date().isoformat()


def _git_head_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), text=True).strip()
    if not out:
        raise SystemExit("FATAL: GIT_HEAD_SHA_EMPTY")
    return out


def _read_json_obj(path: Path) -> dict:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: cannot read json: {path}: {e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: json not object: {path}")
    return obj


def _run_engine_gate(day_utc: str, current_sha: str) -> None:
    p = subprocess.run(
        ["python3", str(ENGINE_GATE_TOOL), "--day_utc", day_utc, "--current_git_sha", current_sha],
        cwd=str(REPO_ROOT),
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
    )
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def _require_engine_gate_pass(day_utc: str, current_sha: str) -> None:
    gate_path = (ENGINE_GATE_ROOT / day_utc / "engine_model_registry_gate.v1.json").resolve()

    if gate_path.exists():
        obj = _read_json_obj(gate_path)
        st = str(obj.get("status") or "").strip().upper()
        if st == "PASS":
            return
        # Poisoned: immutable fail artifact exists for today.
        raise SystemExit(
            "FAIL: TODAY_ENGINE_MODEL_REGISTRY_GATE_NOT_PASS "
            f"day_utc={day_utc} status={st} gate_path={str(gate_path)} "
            "ACTION_REQUIRED: explicitly quarantine/rollover day key under governed procedure."
        )

    # Missing: run it for today (immutable write).
    _run_engine_gate(day_utc, current_sha)

    if not gate_path.exists():
        raise SystemExit(f"FAIL: engine registry gate did not produce artifact: {gate_path}")

    obj2 = _read_json_obj(gate_path)
    st2 = str(obj2.get("status") or "").strip().upper()
    if st2 != "PASS":
        raise SystemExit(
            "FAIL: TODAY_ENGINE_MODEL_REGISTRY_GATE_NOT_PASS "
            f"day_utc={day_utc} status={st2} gate_path={str(gate_path)} "
            "ACTION_REQUIRED: explicitly quarantine/rollover day key under governed procedure."
        )


def main() -> int:
    if not REPO_ROOT.exists() or not REPO_ROOT.is_dir():
        print(f"FAIL: repo root missing: {REPO_ROOT}", file=sys.stderr)
        return 2
    if not TRUTH.exists() or not TRUTH.is_dir():
        print(f"FAIL: truth root missing: {TRUTH}", file=sys.stderr)
        return 2
    if not ENGINE_GATE_TOOL.exists():
        print(f"FAIL: missing engine gate tool: {ENGINE_GATE_TOOL}", file=sys.stderr)
        return 2

    day = _today_utc_day()
    sha = _git_head_sha()

    _require_engine_gate_pass(day, sha)

    print(f"DAY_UTC={day}")
    print(f"PRODUCED_UTC={day}T00:00:00Z")
    print(f"PRODUCER_GIT_SHA={sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
