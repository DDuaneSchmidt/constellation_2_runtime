from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import constellation_2.common.governed_evaluation_control_v1 as control


def _scope_policy() -> dict:
    action_map = {
        action_state: {
            "headroom_multiplier_bp": 10000,
            "control_state": "allow",
            "reason_codes": [],
        }
        for action_state in control.ALLOWED_ACTION_STATES
    }
    return {
        "action_map": action_map,
        "missing_or_invalid_action_artifact": {
            "headroom_multiplier_bp": 0,
            "control_state": "fail_safe_block_new_risk",
            "reason_codes": ["MISSING_ACTION_ARTIFACT"],
        },
        "not_adopted_passthrough": {
            "headroom_multiplier_bp": 10000,
            "control_state": "not_adopted_passthrough",
            "reason_codes": ["SLEEVE_CONTROL_NOT_YET_ADOPTED"],
        },
    }


def test_resolve_runtime_control_normalizes_sleeve_truth_root_paths(tmp_path: Path, monkeypatch) -> None:
    calls: list[dict] = []

    def _fake_resolve_action_decision(**kwargs):
        calls.append(kwargs)
        return {
            "scope_id": kwargs["scope_id"],
            "scope_kind": kwargs["scope_kind"],
            "headroom_multiplier_bp": 10000,
        }

    monkeypatch.setattr(
        control,
        "_runtime_policy",
        lambda: {
            "portfolio_scope": _scope_policy(),
            "sleeve_scope": _scope_policy(),
            "scorecard_control_input_forbidden": True,
        },
    )
    monkeypatch.setattr(
        control,
        "resolve_adopted_governed_sleeve_bindings_v1",
        lambda: [
            {
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "execution_sleeve_id": "PRIMARY",
                "mode": "PAPER",
            }
        ],
    )
    monkeypatch.setattr(control, "_resolve_action_decision", _fake_resolve_action_decision)

    truth_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)

    result = control.resolve_capital_authority_runtime_control_v1(
        truth_root=truth_root,
        day_utc="2026-04-14",
        sleeve_ids=["C2_TREND_EQ_PRIMARY"],
    )

    assert result["portfolio_headroom_multiplier_bp"] == 10000
    assert result["sleeve_headroom_multiplier_bp_by_sleeve"] == {"C2_TREND_EQ_PRIMARY": 10000}

    portfolio_call = next(item for item in calls if item["scope_kind"] == "portfolio")
    sleeve_call = next(item for item in calls if item["scope_kind"] == "sleeve")

    assert portfolio_call["path"] == (
        tmp_path
        / "truth"
        / "reports"
        / "portfolio_governance_action_state_v1"
        / "2026-04-14"
        / "portfolio_governance_action_state.v1.json"
    ).resolve()
    assert sleeve_call["path"] == (
        tmp_path
        / "truth_sleeves"
        / "PRIMARY"
        / "PAPER"
        / "reports"
        / "sleeve_governance_action_state_v1"
        / "2026-04-14"
        / "C2_TREND_EQ_PRIMARY"
        / "sleeve_governance_action_state.v1.json"
    ).resolve()
    assert "truth_sleeves/PRIMARY/truth_sleeves" not in str(sleeve_call["path"])
