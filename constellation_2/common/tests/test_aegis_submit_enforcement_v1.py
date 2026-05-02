from __future__ import annotations

import json
from pathlib import Path

import pytest

import ops.tools.aegis_submit_enforcement_v1 as enforcement


DAY = "2026-04-29"
COMMIT = "a" * 40


def _write(path: Path, payload: dict | str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(truth_root: Path, family: str, filename: str) -> Path:
    return truth_root / "reports" / family / DAY / filename


def _packet(runtime_root: Path, commit: str = COMMIT, runtime_mode: str = "PRODUCTION") -> None:
    _write(
        runtime_root / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md",
        "\n".join(
            [
                "# Aegis Packet",
                "- generated_at_utc: 2026-04-29T14:00:00Z",
                f"- runtime_mode: {runtime_mode}",
                f"- git_commit: {commit}",
                "- git_dirty_status: CLEAN",
            ]
        ),
    )


@pytest.fixture(autouse=True)
def _stable_git(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(enforcement, "_git_commit", lambda: COMMIT)
    monkeypatch.setattr(enforcement, "_git_dirty_status", lambda: "CLEAN")


def _seed_ready(tmp_path: Path, runtime_mode: str = "PRODUCTION") -> tuple[Path, Path, Path]:
    truth = tmp_path / ("candidate_truth" if runtime_mode == "CANDIDATE" else "production_truth")
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    if runtime_mode == "PRODUCTION":
        _write(
            truth / "governance" / "production_version.v1.json",
            {
                "schema_id": "production_version",
                "schema_version": "production_version.v1",
                "promoted_commit": COMMIT,
                "promoted_at_utc": "2026-04-29T13:00:00Z",
                "promoted_by": "test",
                "promotion_id": "test-promotion",
                "rollback_commit": "b" * 40,
                "status": "ACTIVE",
            },
        )
    _write(_report(truth, "aegis_day_run_v1", "day_run.v1.json"), {"final_status": "PAPER_READY", "canonical_blocker": ""})
    _write(
        _report(truth, "submit_boundary_status_v1", "submit_boundary_status.v1.json"),
        {"status": "PASS", "submit_allowed": True, "submission_authorized": True},
    )
    _write(
        _report(truth, "action_validity_v1", "action_validity.v1.json"),
        {"action_rules": [{"action_id": "submit_paper_order", "status": "ALLOWED"}]},
    )
    _write(
        _report(truth, "truth_freshness_v1", "truth_freshness.v1.json"),
        {
            "freshness_records": [
                {"artifact_type": "aegis_day_run_v1", "freshness_status": "FRESH"},
                {"artifact_type": "submit_boundary_status_v1", "freshness_status": "FRESH"},
                {"artifact_type": "action_validity_v1", "freshness_status": "FRESH"},
            ]
        },
    )
    _write(
        execution / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        {"state": "INACTIVE", "active": False},
    )
    _packet(runtime, runtime_mode=runtime_mode)
    return truth, execution, runtime


def _evaluate(truth: Path, execution: Path, runtime: Path, runtime_mode: str = "PRODUCTION") -> dict:
    return enforcement.evaluate_submit_enforcement_v1(
        truth_root=truth,
        execution_root=execution,
        runtime_root=runtime,
        day_utc=DAY,
        action_id="submit_paper_order",
        runtime_mode=runtime_mode,
    )


def test_submit_enforcement_allows_only_fully_ready_state(tmp_path: Path) -> None:
    truth, execution, runtime = _seed_ready(tmp_path)
    result = _evaluate(truth, execution, runtime)
    assert result["ok"] is True
    assert result["status"] == "PASS"
    assert result["runtime_mode"] == "PRODUCTION"


def test_candidate_runtime_and_unpromoted_commit_hard_block_submit(tmp_path: Path) -> None:
    truth, execution, runtime = _seed_ready(tmp_path / "candidate", runtime_mode="CANDIDATE")
    result = _evaluate(truth, execution, runtime, runtime_mode="CANDIDATE")
    assert result["ok"] is False
    assert result["canonical_blocker"] == "CANDIDATE_RUNTIME_SUBMIT_DISABLED"

    truth, execution, runtime = _seed_ready(tmp_path / "unpromoted")
    _write(
        truth / "governance" / "production_version.v1.json",
        {"schema_version": "production_version.v1", "promoted_commit": "b" * 40, "status": "ACTIVE"},
    )
    result = _evaluate(truth, execution, runtime)
    assert any(row["code"] == "UNPROMOTED_PRODUCTION_COMMIT" for row in result["blockers"])

    truth, execution, runtime = _seed_ready(tmp_path / "missing_version")
    (truth / "governance" / "production_version.v1.json").unlink()
    result = _evaluate(truth, execution, runtime)
    assert any(row["code"] == "PRODUCTION_VERSION_MISSING" for row in result["blockers"])


def test_blocked_ledger_kill_switch_forbidden_action_packet_and_freshness_all_hard_block(tmp_path: Path) -> None:
    truth, execution, runtime = _seed_ready(tmp_path)
    _write(_report(truth, "aegis_day_run_v1", "day_run.v1.json"), {"final_status": "NOT_READY", "canonical_blocker": "C2_KILL_SWITCH_ACTIVE"})
    assert _evaluate(truth, execution, runtime)["canonical_blocker"] == "DAY_RUN_LEDGER_NOT_READY"

    truth, execution, runtime = _seed_ready(tmp_path / "kill")
    _write(execution / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json", {"state": "ACTIVE", "active": True})
    assert any(row["code"] == "KILL_SWITCH_NOT_INACTIVE" for row in _evaluate(truth, execution, runtime)["blockers"])

    truth, execution, runtime = _seed_ready(tmp_path / "action")
    _write(_report(truth, "action_validity_v1", "action_validity.v1.json"), {"action_rules": [{"action_id": "submit_paper_order", "status": "FORBIDDEN"}]})
    assert any(row["code"] == "ACTION_VALIDITY_FORBIDS_SUBMIT" for row in _evaluate(truth, execution, runtime)["blockers"])

    truth, execution, runtime = _seed_ready(tmp_path / "packet")
    _packet(runtime, commit="b" * 40)
    assert any(row["code"] == "AEGIS_PACKET_STALE" for row in _evaluate(truth, execution, runtime)["blockers"])

    truth, execution, runtime = _seed_ready(tmp_path / "packet_mode")
    _packet(runtime, runtime_mode="CANDIDATE")
    assert any(row["code"] == "AEGIS_PACKET_STALE" for row in _evaluate(truth, execution, runtime)["blockers"])

    truth, execution, runtime = _seed_ready(tmp_path / "fresh")
    _write(
        _report(truth, "truth_freshness_v1", "truth_freshness.v1.json"),
        {
            "freshness_records": [
                {"artifact_type": "aegis_day_run_v1", "freshness_status": "FRESH"},
                {"artifact_type": "submit_boundary_status_v1", "freshness_status": "EXPIRED"},
                {"artifact_type": "action_validity_v1", "freshness_status": "FRESH"},
            ]
        },
    )
    assert any(row["code"] == "REQUIRED_AUTHORITY_NOT_FRESH" for row in _evaluate(truth, execution, runtime)["blockers"])


def test_submit_entrypoints_use_shared_enforcement_gate() -> None:
    repo = enforcement.REPO_ROOT
    required = [
        repo / "ops" / "tools" / "run_aegis_paper_submit_v1.py",
        repo / "ops" / "tools" / "run_aegis_paper_auto_v1.py",
        repo / "constellation_2" / "phaseD" / "lib" / "submit_boundary_paper_v4.py",
        repo / "constellation_2" / "phaseD" / "tools" / "c2_submit_paper_v5.py",
    ]
    for path in required:
        assert "aegis_submit_enforcement_v1" in path.read_text(encoding="utf-8")

    for legacy in ("c2_submit_paper_v1.py", "c2_submit_paper_v2.py", "c2_submit_paper_v3.py", "c2_submit_paper_v4.py"):
        text = (repo / "constellation_2" / "phaseD" / "tools" / legacy).read_text(encoding="utf-8")
        assert "LEGACY_EXECUTION_SUBMISSION_PATH_DISABLED" in text
