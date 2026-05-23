from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.data_remediation_v1 import PLAYBOOK_MARK_PROVIDER_DATA_NEEDED, _blocker_priority_v1, _playbook_for_blocker, rebuild_data_projections_v1, run_data_remediation_v1
from ops.aegis.operator_command_v1 import build_operator_state_snapshot_v1
from ops.aegis.run_context_v1 import RunContext, child_run_context_v1, step_allowed_v1
from ops.tools import refresh_aegis_market_data_v1 as refresh_cmd
from ops.tools import write_aegis_candidate_generation_diagnostics_v1 as diagnostics_cmd

DAY = "2026-05-18"


def _context(**overrides) -> RunContext:
    data = {
        "run_id": "test-run",
        "command_name": "test",
        "started_at": f"{DAY}T00:00:00Z",
        "allow_market_data_refresh": True,
        "allow_self_heal": True,
        "allow_projection_rebuild": True,
        "parent_run_id": "",
        "recursion_depth": 0,
        "visited_steps": (),
        "max_recursion_depth": 2,
    }
    data.update(overrides)
    return RunContext(**data)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    _write_json(repo / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json", {"sleeves": []})
    _write_json(repo / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json", {"engines": []})
    _write_json(repo / "ops/config/aegis_runtime_universe.json", {"runtime_universe_mode": "sleeve_required_only", "production_scan_dataset_id": ""})
    return repo


def test_run_context_blocks_repeated_refresh_and_excess_depth() -> None:
    parent = _context(visited_steps=("market_data_refresh",))
    allowed, reason = step_allowed_v1(parent, "market_data_refresh")
    deep = _context(recursion_depth=3, max_recursion_depth=2)
    deep_allowed, deep_reason = step_allowed_v1(deep, "self_heal")

    assert allowed is False
    assert reason == "STEP_ALREADY_VISITED"
    assert deep_allowed is False
    assert deep_reason == "MAX_RECURSION_DEPTH_EXCEEDED"


def test_candidate_diagnostics_passes_self_heal_context_that_disallows_refresh(monkeypatch, tmp_path: Path) -> None:
    seen = {}

    def fake_remediation(*, truth_root, repo_root, day_utc, run_context, **_kwargs):
        seen["context"] = run_context
        return {"blocker_count": 0, "attempts": [], "governance": {"broker_execution_allowed": False}}

    def fake_build(**_kwargs):
        return {
            "candidate_generation_status": "RAN",
            "operator_interpretation": "NORMAL_NO_SIGNAL",
            "total_sleeves_expected": 0,
            "total_sleeves_run": 0,
            "total_raw_signals": 0,
            "total_candidates_generated": 0,
            "total_candidates_rejected": 0,
        }

    monkeypatch.setattr(diagnostics_cmd, "run_data_remediation_v1", fake_remediation)
    monkeypatch.setattr(diagnostics_cmd, "build_candidate_generation_diagnostics_v1", fake_build)
    monkeypatch.setattr(diagnostics_cmd, "write_candidate_generation_diagnostics_v1", lambda **_kwargs: {"json": str(tmp_path / "diag.json")})

    assert diagnostics_cmd.main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert seen["context"].command_name == "self-heal-data"
    assert seen["context"].allow_market_data_refresh is False
    assert seen["context"].allow_projection_rebuild is True


def test_refresh_market_data_guard_writes_failed_artifact_without_provider_fetch(monkeypatch, tmp_path: Path) -> None:
    calls = {"fetch": 0}

    def forbidden_fetch(**_kwargs):
        calls["fetch"] += 1
        raise AssertionError("provider fetch must not run when refresh recursion guard blocks")

    monkeypatch.setenv("AEGIS_RUN_CONTEXT_JSON", json.dumps(_context(visited_steps=("market_data_refresh",)).to_dict()))
    monkeypatch.setenv("AEGIS_RUNTIME_UNIVERSE_MODE", "sleeve_required_only")
    monkeypatch.setattr(refresh_cmd, "fetch_market_data_v1", forbidden_fetch)
    monkeypatch.setattr(refresh_cmd, "build_symbol_map_v1", lambda **_kwargs: {"required_symbols": ["VIX"], "runtime_universe_mode": "sleeve_required_only"})

    assert refresh_cmd.main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    payload = json.loads((tmp_path / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json").read_text(encoding="utf-8"))
    assert calls["fetch"] == 0
    assert payload["status"] == "FAILED"
    assert payload["failure_reason"] == "STEP_ALREADY_VISITED"
    assert payload["timeout_diagnostic"]["diagnostic_type"] == "market_data_refresh_skipped"


def test_self_heal_prioritizes_vix_required_blocker_before_scan_symbols() -> None:
    blockers = [
        {"blocker_id": "scan", "blocker_type": "missing_scan_symbol", "affected_symbol": "SPY"},
        {"blocker_id": "required", "blocker_type": "missing_required_symbol", "affected_symbol": "VIX"},
        {"blocker_id": "other", "blocker_type": "missing_required_symbol", "affected_symbol": "QQQ"},
    ]

    ordered = sorted(blockers, key=_blocker_priority_v1)

    assert [row["blocker_id"] for row in ordered] == ["required", "other", "scan"]


def test_missing_scan_symbol_uses_fail_closed_escalation_not_provider_fetch() -> None:
    blocker = {"blocker_type": "missing_scan_symbol", "affected_symbol": "VTI"}

    assert _playbook_for_blocker("missing_scan_symbol") == PLAYBOOK_MARK_PROVIDER_DATA_NEEDED


def test_self_heal_projection_rebuild_is_read_only_and_never_calls_refresh_or_diagnostics(monkeypatch, tmp_path: Path) -> None:
    commands = []

    def fake_run(command, **_kwargs):
        commands.append(" ".join(command))
        return SimpleNamespace(returncode=0, stdout="{}", stderr="")

    monkeypatch.setattr("ops.aegis.data_remediation_v1.subprocess.run", fake_run)
    result = rebuild_data_projections_v1(truth_root=tmp_path / "truth", repo_root=ROOT, day_utc=DAY, run_context=_context())

    assert result["ok"] is True
    assert result["read_only_projection_rebuild"] is True
    assert commands
    assert not any("refresh_aegis_market_data_v1.py" in command for command in commands)
    assert not any("write_aegis_candidate_generation_diagnostics_v1.py" in command for command in commands)


def test_operator_state_snapshot_builder_is_read_only(monkeypatch) -> None:
    def forbidden_run(*_args, **_kwargs):
        raise AssertionError("projection builder must not spawn runtime commands")

    monkeypatch.setattr("subprocess.run", forbidden_run)
    snapshot = build_operator_state_snapshot_v1({
        "runtime": {"runtime_truth_classification": "REAL_RUNTIME", "highest_readiness_layer": "READY"},
        "opportunities": {"market_data_summary": {"missing_symbols": [], "stale_symbols": []}, "sleeve_run_summary": []},
        "top_candidates": [],
        "candidate_decisions_corrections": {},
        "source_paths": {},
    })

    assert snapshot["validation_result"]["status"] == "PASS"
    assert snapshot["broker_execution_allowed"] is False


def test_self_heal_reuses_existing_final_event_per_session(tmp_path: Path, monkeypatch) -> None:
    from constellation_2.common.tests.test_aegis_data_remediation_v1 import _cache_vix, _config, _market_report, _readiness, _repo as fixture_repo
    from ops.aegis.data_remediation_v1 import read_remediation_events_v1

    root = tmp_path / "truth"
    repo = fixture_repo(tmp_path)
    _config(tmp_path, monkeypatch)
    _market_report(root, status="MISSING")
    _readiness(root)
    _cache_vix(root, session=DAY)

    first = run_data_remediation_v1(truth_root=root, repo_root=repo, day_utc=DAY, run_context=_context())
    first_events = read_remediation_events_v1(truth_root=root, day_utc=DAY)
    second = run_data_remediation_v1(truth_root=root, repo_root=repo, day_utc=DAY, run_context=_context())
    second_events = read_remediation_events_v1(truth_root=root, day_utc=DAY)

    assert first["attempt_count"] >= 1
    assert len(second_events) == len(first_events)
    assert second["attempt_count"] >= 1
    assert second["blocker_count"] == 0
    assert second["governance"]["broker_execution_allowed"] is False
