from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_aegis_live_intelligence_v1 as live  # noqa: E402
import ops.tools.run_aegis_operator_projection_v1 as projection  # noqa: E402
from ops.tools import run_aegis_bod_prepare_v1 as bod  # noqa: E402


def _ctx(tmp_path: Path) -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(
        day_utc="2026-04-29",
        environment="PAPER",
        truth_root=truth,
        execution_root=execution,
        runtime_root=runtime,
        operator_input_root=operator,
        ib_account="DU123456",
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def test_operator_projection_source_reproducibility_action(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write(
        ctx.truth_root / "reports" / "aegis_day_run_v1" / ctx.day_utc / "day_run.v1.json",
        {
            "day_utc": ctx.day_utc,
            "final_status": "NOT_READY",
            "canonical_phase": "SOURCE_INTEGRITY",
            "canonical_blocker": "SOURCE_REPRODUCIBILITY_BLOCKED",
            "root_cause_chain": [{"blocker_detail": "git tree is dirty"}],
            "downstream_consequences": [],
        },
    )

    payload = projection._projection_for(ctx, json.loads((ctx.truth_root / "reports" / "aegis_day_run_v1" / ctx.day_utc / "day_run.v1.json").read_text()), {})

    assert payload["first_blocker"] == "SOURCE_REPRODUCIBILITY_BLOCKED"
    assert payload["owner"] == "source_reproducibility_authority"
    assert "Clean/protect" in payload["next_valid_actions"][0]


def test_operator_projection_market_data_points_to_supply_artifacts(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    payload = projection._projection_for(
        ctx,
        {"final_status": "NOT_READY", "canonical_phase": "MARKET_DATA_BOD_PREP", "canonical_blocker": "MARKET_DATA_BLOCKED"},
        {},
    )

    assert payload["owner"] == "market_data_authority"
    assert any("market_data_supply_v1" in path for path in payload["artifact_paths"])


def test_operator_projection_no_eligible_structure_points_to_authorization(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    payload = projection._projection_for(
        ctx,
        {"final_status": "NOT_READY", "canonical_phase": "AUTHORIZATION_FINAL", "canonical_blocker": "NO_ELIGIBLE_OPTION_STRUCTURE"},
        {},
    )

    assert payload["owner"] == "authorization_supply"
    assert any("authorization_supply_v1" in path for path in payload["artifact_paths"])


def test_live_intelligence_cannot_recommend_when_day_not_ready(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write(
        ctx.truth_root / "reports" / "aegis_day_run_v1" / ctx.day_utc / "day_run.v1.json",
        {"day_utc": ctx.day_utc, "final_status": "NOT_READY", "canonical_blocker": "SOURCE_REPRODUCIBILITY_BLOCKED"},
    )

    payload = live.build_live_intelligence_v1(ctx)

    assert payload["status"] == "NOT_READY"
    assert payload["advisory_only"] is True
    assert payload["opportunity_radar"]["state"] == "UNKNOWN"
    assert payload["opportunity_radar"]["actionable_recommendations"] == []
    assert payload["submit_boundary_effect"] == "NONE"
