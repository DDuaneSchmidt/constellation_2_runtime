from __future__ import annotations

import json
import os
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_aegis_bod_prepare_v1 as bod
import ops.tools.run_aegis_paper_ready_v1 as paper_ready


DAY = "2026-04-29"


def _ctx(tmp_path: Path) -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(
        day_utc=DAY,
        environment="PAPER",
        truth_root=truth,
        execution_root=execution,
        runtime_root=runtime,
        operator_input_root=operator,
        ib_account="DUO847203",
    )


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _seed_other_pass_inputs(ctx: bod.BodContext) -> None:
    _write(ctx.truth_root / "reports" / "market_data_authority_v1" / DAY / "market_data_authority.v1.json", {"status": "PASS", "market_data_state": "READY"})
    _write(ctx.truth_root / "reports" / "strategy_decision_authority_v1" / DAY / "strategy_decision_authority.v1.json", {"status": "PASS", "strategy_decision_state": "INTENT_CREATED"})
    _write(ctx.truth_root / "reports" / "risk_sizing_authority_v1" / DAY / "risk_sizing_authority.v1.json", {"status": "PASS", "risk_sizing_state": "SIZED"})
    _write(ctx.truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json", {"status": "PASS", "boundary_status": "AUTHORIZED", "submission_authorized": True})
    _write(ctx.truth_root / "reports" / "execution_mode_authority_v1" / DAY / "execution_mode_authority.v1.json", {"status": "PASS", "mode_state": "DRY_RUN_LOCKED"})


def test_stale_lifecycle_blocks_as_stale_not_pre_open_not_ready(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _seed_other_pass_inputs(ctx)
    lifecycle = _write(ctx.truth_root / "reports" / "aegis_day_lifecycle_v1" / DAY / "aegis_day_lifecycle.v1.json", {"day_utc": DAY, "state": "PRE_OPEN_BLOCKED"})
    pre_open = _write(ctx.truth_root / "reports" / "aegis_pre_open_verify_v1" / DAY / "aegis_pre_open_verify.v1.json", {"day_utc": DAY, "status": "PRE_OPEN_READY"})
    os.utime(lifecycle, (1000, 1000))
    os.utime(pre_open, (2000, 2000))

    payload = paper_ready.evaluate_paper_ready_v1(ctx)

    assert payload["canonical_blocker"] == "STALE_ARTIFACT"


def test_current_pre_open_failure_remains_pre_open_not_ready(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _seed_other_pass_inputs(ctx)
    _write(ctx.truth_root / "reports" / "aegis_pre_open_verify_v1" / DAY / "aegis_pre_open_verify.v1.json", {"day_utc": DAY, "status": "PRE_OPEN_BLOCKED"})
    _write(ctx.truth_root / "reports" / "aegis_day_lifecycle_v1" / DAY / "aegis_day_lifecycle.v1.json", {"day_utc": DAY, "state": "PRE_OPEN_BLOCKED"})

    payload = paper_ready.evaluate_paper_ready_v1(ctx)

    assert payload["canonical_blocker"] == "PRE_OPEN_NOT_READY"


def test_missing_lifecycle_is_missing_evidence_not_stale(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _seed_other_pass_inputs(ctx)
    _write(ctx.truth_root / "reports" / "aegis_pre_open_verify_v1" / DAY / "aegis_pre_open_verify.v1.json", {"day_utc": DAY, "status": "PRE_OPEN_READY"})

    payload = paper_ready.evaluate_paper_ready_v1(ctx)

    assert payload["canonical_blocker"] == "MISSING_EVIDENCE"
