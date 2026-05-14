from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_aegis_paper_ready_kernel_v1 as kernel


DAY = "2026-05-12"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _stage() -> kernel.KernelStage:
    return kernel._stage(
        "paper_authority_pointer_refresh",
        "authorization",
        ["python3", "ops/tools/run_pointer_append_v1.py", "--guarded-by", "authorization_gate_verdict_v1"],
        "PAPER_SLEEVE",
        Path("run_pointer_v2/canonical_authority_head.v1.json"),
        ("PASS", "BOOTSTRAP_PASS"),
        "Run governed pointer append/head materialization only after same-day PAPER authorization verdict PASS.",
        "Refresh the same-day PAPER canonical authority head from the governed PASS authorization verdict before capital allocation.",
    )


def _head_path(root: Path) -> Path:
    return root / "run_pointer_v2" / "canonical_authority_head.v1.json"


def _verdict_path(root: Path, day: str = DAY) -> Path:
    return root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json"


def _verdict_payload(*, day: str = DAY, status: str = "PASS") -> dict:
    return {
        "schema_id": "authorization_gate_verdict_v1",
        "schema_version": "v1",
        "day_utc": day,
        "status": status,
        "reason_codes": ["AUTHORIZATION_GATES_PASS"] if status in {"PASS", "BOOTSTRAP_PASS"} else ["AUTHORIZATION_GATE_NOT_PASS"],
    }


def _head_payload(root: Path, *, day: str = DAY, status: str = "PASS") -> dict:
    return {
        "schema_id": "c2_run_pointer_canonical_authority_head",
        "schema_version": "v1",
        "day_utc": day,
        "status": status,
        "authoritative": True,
        "points_to": str(_verdict_path(root, day)),
    }


def _refresh_runner(
    *,
    sleeve_root: Path,
    call_log: list[str],
    append_rc: int = 0,
    heads_rc: int = 0,
    materialize_head: bool = True,
):
    def run(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        script = Path(command[1]).name if len(command) > 1 else ""
        call_log.append(script)
        if script == "run_pointer_attempt_alloc_v1.py":
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=json.dumps({"attempt_id": f"{DAY}__A0001__abcdef0__123456789abc", "attempt_seq": 1}) + "\n",
                stderr="",
            )
        if script == "run_pointer_append_v1.py":
            return subprocess.CompletedProcess(command, append_rc, stdout='{"ok":true}\n' if append_rc == 0 else "", stderr="")
        if script == "run_pointer_heads_materialize_v1.py":
            if materialize_head and heads_rc == 0:
                _write_json(_head_path(sleeve_root), _head_payload(sleeve_root))
            return subprocess.CompletedProcess(command, heads_rc, stdout='{"ok":true}\n' if heads_rc == 0 else "", stderr="")
        raise AssertionError(f"unexpected command: {command}")

    return run


def test_same_day_pass_verdict_triggers_pointer_append_and_head_materialization(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_json(_verdict_path(sleeve_root), _verdict_payload(status="PASS"))
    calls: list[str] = []

    result = kernel._run_paper_authority_pointer_refresh_stage(
        stage=_stage(),
        artifact_path=_head_path(sleeve_root),
        target_day=DAY,
        sleeve_root=sleeve_root,
        runner=_refresh_runner(sleeve_root=sleeve_root, call_log=calls),
        workdir=SOURCE_ROOT,
        release_commit="1" * 40,
    )

    assert result["status"] == "PASS"
    assert result["authority_pointer_refresh"]["action"] == "MATERIALIZED"
    assert calls == [
        "run_pointer_attempt_alloc_v1.py",
        "run_pointer_append_v1.py",
        "run_pointer_heads_materialize_v1.py",
    ]
    head = json.loads(_head_path(sleeve_root).read_text(encoding="utf-8"))
    assert head["day_utc"] == DAY
    assert head["points_to"] == str(_verdict_path(sleeve_root))


def test_strategy_decision_authority_runs_before_authorization_artifacts() -> None:
    stages = kernel._stages(
        target_day=DAY,
        canonical_truth_root=Path("/tmp/canonical"),
        paper_sleeve_root=Path("/tmp/sleeve"),
        environment="PAPER",
        ib_account="DUO847203",
        release_commit="1" * 40,
    )
    stage_ids = [stage.stage_id for stage in stages]

    assert stage_ids.index("strategy_decision_authority") < stage_ids.index("authorization_artifacts")
    assert stage_ids.index("authorization_artifacts") < stage_ids.index("authorization_supply")


def test_strategy_decision_authority_with_active_intents_satisfies_kernel_stage(tmp_path: Path) -> None:
    artifact = tmp_path / "reports" / "strategy_decision_authority_v1" / DAY / "strategy_decision_authority.v1.json"
    _write_json(
        artifact,
        {
            "schema_id": "C2_STRATEGY_DECISION_AUTHORITY_V1",
            "schema_version": 1,
            "day_utc": DAY,
            "status": "FAIL",
            "strategy_decision_state": "SIGNAL_MISSING",
            "intent_count": 2,
        },
    )
    stage = kernel._stage(
        "strategy_decision_authority",
        "authorization",
        ["python3", "ops/tools/run_strategy_decision_authority_v1.py"],
        "PAPER_SLEEVE",
        Path("reports/strategy_decision_authority_v1") / DAY / "strategy_decision_authority.v1.json",
        ("PASS", "READY", "OK"),
        "",
        "",
    )

    result = kernel._validate_stage_artifact(stage=stage, artifact_path=artifact, target_day=DAY)

    assert result["status"] == "PASS"
    assert result["artifact_status"] == "FAIL"
    assert result["intent_count"] == 2


def test_same_day_bootstrap_pass_verdict_can_refresh_with_existing_pointer_rules(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_json(_verdict_path(sleeve_root), _verdict_payload(status="BOOTSTRAP_PASS"))
    calls: list[str] = []

    def run(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        script = Path(command[1]).name if len(command) > 1 else ""
        calls.append(script)
        if script == "run_pointer_attempt_alloc_v1.py":
            return subprocess.CompletedProcess(command, 0, stdout=json.dumps({"attempt_id": "a", "attempt_seq": 1}), stderr="")
        if script == "run_pointer_append_v1.py":
            assert "--status" in command
            assert command[command.index("--status") + 1] == "PASS"
            return subprocess.CompletedProcess(command, 0, stdout='{"ok":true}\n', stderr="")
        if script == "run_pointer_heads_materialize_v1.py":
            _write_json(_head_path(sleeve_root), _head_payload(sleeve_root, status="PASS"))
            return subprocess.CompletedProcess(command, 0, stdout='{"ok":true}\n', stderr="")
        raise AssertionError(f"unexpected command: {command}")

    result = kernel._run_paper_authority_pointer_refresh_stage(
        stage=_stage(),
        artifact_path=_head_path(sleeve_root),
        target_day=DAY,
        sleeve_root=sleeve_root,
        runner=run,
        workdir=SOURCE_ROOT,
        release_commit="1" * 40,
    )

    assert result["status"] == "PASS"
    assert result["authority_head_freshness"]["same_day_authorization_verdict_status"] == "BOOTSTRAP_PASS"
    assert calls == [
        "run_pointer_attempt_alloc_v1.py",
        "run_pointer_append_v1.py",
        "run_pointer_heads_materialize_v1.py",
    ]


def test_same_day_fail_verdict_does_not_refresh_pointer(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_json(_verdict_path(sleeve_root), _verdict_payload(status="FAIL"))
    calls: list[str] = []

    result = kernel._run_paper_authority_pointer_refresh_stage(
        stage=_stage(),
        artifact_path=_head_path(sleeve_root),
        target_day=DAY,
        sleeve_root=sleeve_root,
        runner=_refresh_runner(sleeve_root=sleeve_root, call_log=calls),
        workdir=SOURCE_ROOT,
        release_commit="1" * 40,
    )

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "AUTHORITY_HEAD_NOT_READY_FOR_DAY"
    assert result["failed_field"] == "authorization_gate_verdict_v1.status"
    assert calls == []


def test_missing_verdict_blocks_without_pointer_mutation(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    calls: list[str] = []

    result = kernel._run_paper_authority_pointer_refresh_stage(
        stage=_stage(),
        artifact_path=_head_path(sleeve_root),
        target_day=DAY,
        sleeve_root=sleeve_root,
        runner=_refresh_runner(sleeve_root=sleeve_root, call_log=calls),
        workdir=SOURCE_ROOT,
        release_commit="1" * 40,
    )

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "AUTHORITY_HEAD_NOT_READY_FOR_DAY"
    assert calls == []
    assert not _head_path(sleeve_root).exists()


def test_pointer_append_failure_blocks_before_head_materialization(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_json(_verdict_path(sleeve_root), _verdict_payload(status="PASS"))
    calls: list[str] = []

    result = kernel._run_paper_authority_pointer_refresh_stage(
        stage=_stage(),
        artifact_path=_head_path(sleeve_root),
        target_day=DAY,
        sleeve_root=sleeve_root,
        runner=_refresh_runner(sleeve_root=sleeve_root, call_log=calls, append_rc=2),
        workdir=SOURCE_ROOT,
        release_commit="1" * 40,
    )

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "AUTHORITY_POINTER_REFRESH_FAILED"
    assert result["failed_field"] == "run_pointer_append_v1.returncode"
    assert calls == ["run_pointer_attempt_alloc_v1.py", "run_pointer_append_v1.py"]


def test_head_materialization_failure_blocks(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    _write_json(_verdict_path(sleeve_root), _verdict_payload(status="PASS"))
    calls: list[str] = []

    result = kernel._run_paper_authority_pointer_refresh_stage(
        stage=_stage(),
        artifact_path=_head_path(sleeve_root),
        target_day=DAY,
        sleeve_root=sleeve_root,
        runner=_refresh_runner(sleeve_root=sleeve_root, call_log=calls, heads_rc=2),
        workdir=SOURCE_ROOT,
        release_commit="1" * 40,
    )

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "AUTHORITY_POINTER_REFRESH_FAILED"
    assert result["failed_field"] == "run_pointer_heads_materialize_v1.returncode"


def test_stale_prior_head_remains_blocked_if_refresh_does_not_update_it(tmp_path: Path) -> None:
    sleeve_root = tmp_path / "truth_sleeves/PRIMARY/PAPER"
    stale_day = "2026-05-04"
    _write_json(_verdict_path(sleeve_root), _verdict_payload(status="PASS"))
    _write_json(_verdict_path(sleeve_root, stale_day), _verdict_payload(day=stale_day, status="PASS"))
    _write_json(_head_path(sleeve_root), _head_payload(sleeve_root, day=stale_day))
    calls: list[str] = []

    result = kernel._run_paper_authority_pointer_refresh_stage(
        stage=_stage(),
        artifact_path=_head_path(sleeve_root),
        target_day=DAY,
        sleeve_root=sleeve_root,
        runner=_refresh_runner(sleeve_root=sleeve_root, call_log=calls, materialize_head=False),
        workdir=SOURCE_ROOT,
        release_commit="1" * 40,
    )

    assert result["status"] == "BLOCKED"
    assert result["first_blocker"] == "AUTHORITY_HEAD_DAY_MISMATCH"
    assert result["authority_pointer_refresh"]["action"] == "MATERIALIZED"
    assert json.loads(_head_path(sleeve_root).read_text(encoding="utf-8"))["day_utc"] == stale_day
