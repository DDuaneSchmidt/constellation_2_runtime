from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from unittest import mock

import ops.tools.run_c2_paper_day_orchestrator_v2 as orchestrator_v2


REPO_ROOT = Path("/home/node/constellation").resolve()
TREND_RUNNER = (
    REPO_ROOT / "constellation_2" / "phaseI" / "trend_eq_primary" / "run" / "run_trend_eq_primary_intents_day_v1.py"
).resolve()


def _index_of_pair(cmd: list[str], flag: str, value: str) -> int:
    for i in range(len(cmd) - 1):
        if cmd[i] == flag and cmd[i + 1] == value:
            return i
    return -1


def test_structural_pre_activity_producers_pass_truth_root_to_market_data_engines() -> None:
    captured: list[tuple[str, list[str]]] = []

    def fake_run_cmd(stage_id: str, cmd: list[str], env: dict[str, str]) -> int:
        captured.append((stage_id, list(cmd)))
        return 0

    active = [
        {
            "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
            "runner_path": "constellation_2.phaseI.vol_income_defined_risk.run.run_vol_income_defined_risk_intents_day_v1",
            "engine_runner_path": "constellation_2/phaseI/vol_income_defined_risk/run/run_vol_income_defined_risk_intents_day_v1.py",
            "engine_runner_sha256": "a" * 64,
        },
        {
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "runner_path": "constellation_2.phaseI.trend_eq_primary.run.run_trend_eq_primary_intents_day_v1",
            "engine_runner_path": "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py",
            "engine_runner_sha256": "b" * 64,
        },
        {
            "engine_id": "C2_MEAN_REVERSION_EQ_V1",
            "runner_path": "constellation_2.phaseI.mean_reversion.run.run_mean_reversion_intents_day_v1",
            "engine_runner_path": "constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py",
            "engine_runner_sha256": "c" * 64,
        },
    ]

    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td).resolve()
        env = {"C2_TRUTH_ROOT": str(truth_root)}
        with (
            mock.patch.object(orchestrator_v2, "_load_engine_registry", return_value={"engines": []}),
            mock.patch.object(orchestrator_v2, "_active_engines_sorted", return_value=active),
            mock.patch.object(
                orchestrator_v2,
                "_build_allowed_symbols_map_fail_closed",
                return_value={row["engine_id"]: None for row in active},
            ),
            mock.patch.object(orchestrator_v2, "_sha256_file", return_value="d" * 64),
            mock.patch.object(orchestrator_v2, "_resolve_structural_activity_mode", return_value="OFF"),
            mock.patch.object(orchestrator_v2, "_run_cmd", side_effect=fake_run_cmd),
            mock.patch.object(orchestrator_v2, "_emit_missing_engine_heartbeat", return_value=None),
        ):
            orchestrator_v2._run_structural_pre_activity_producers(
                truth_root=truth_root,
                day="2026-04-09",
                mode="PAPER",
                symbol="SPY",
                produced_utc="2026-04-09T00:00:00Z",
                git_sha="e" * 40,
                env=env,
            )

    stage_map = {stage_id: cmd for stage_id, cmd in captured}
    expected_root = str(truth_root)
    assert _index_of_pair(stage_map["A0A_VOL_INCOME_DEFINED_RISK_INTENTS_DAY_V1_SPY"], "--truth_root", expected_root) > 0
    assert _index_of_pair(stage_map["A0B_TREND_EQ_PRIMARY_INTENTS_DAY_V1_SPY"], "--truth_root", expected_root) > 0
    assert _index_of_pair(stage_map["A0C_MEAN_REVERSION_INTENTS_DAY_V1_SPY"], "--truth_root", expected_root) > 0


def test_trend_engine_explicit_truth_root_wins_and_missing_manifest_still_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td).resolve()
        result = subprocess.run(
            [
                "python3",
                str(TREND_RUNNER),
                "--day_utc",
                "2026-04-09",
                "--mode",
                "PAPER",
                "--truth_root",
                str(truth_root),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            cwd=str(REPO_ROOT),
        )

    expected_manifest = str((truth_root / "market_data_snapshot_v1" / "dataset_manifest.json").resolve())
    assert result.returncode != 0
    assert f"MARKET_DATA_MANIFEST_MISSING: {expected_manifest}" in result.stderr


def test_post_submit_canonical_execution_propagation_stage_uses_sleeve_source_root(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_execution_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"

    class ExecRoots:
        execution_root_path = sleeve_execution_truth

    with mock.patch.object(orchestrator_v2, "resolve_governed_paper_execution_roots", return_value=ExecRoots()):
        stages = orchestrator_v2._build_stage_defs(
            truth=canonical_truth,
            day="2026-04-13",
            input_day="2026-04-13",
            ib_account="DUO847203",
            git_sha="a" * 40,
            attempt_id="2026-04-13__attempt",
        )

    by_id = {stage.stage_id: stage for stage in stages}
    assert "B0A_EXECUTION_EVIDENCE_TRUTH_V1" in by_id
    cmd = by_id["B0A_EXECUTION_EVIDENCE_TRUTH_V1"].cmd
    assert _index_of_pair(cmd, "--truth_root", str(canonical_truth)) > 0
    assert _index_of_pair(cmd, "--source_truth_root", str(sleeve_execution_truth)) > 0


def test_execution_reconciliation_stage_receives_explicit_canonical_truth_root(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"

    class ExecRoots:
        execution_root_path = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"

    with mock.patch.object(orchestrator_v2, "resolve_governed_paper_execution_roots", return_value=ExecRoots()):
        stages = orchestrator_v2._build_stage_defs(
            truth=canonical_truth,
            day="2026-04-13",
            input_day="2026-04-13",
            ib_account="DUO847203",
            git_sha="b" * 40,
            attempt_id="2026-04-13__attempt",
        )

    by_id = {stage.stage_id: stage for stage in stages}
    assert "B2_EXECUTION_RECONCILIATION_V1" in by_id
    cmd = by_id["B2_EXECUTION_RECONCILIATION_V1"].cmd
    assert _index_of_pair(cmd, "--truth_root", str(canonical_truth)) > 0


def test_submission_lifecycle_refresh_stage_is_ordered_between_stream_snapshot_and_fill_ledger(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"

    class ExecRoots:
        execution_root_path = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"

    with mock.patch.object(orchestrator_v2, "resolve_governed_paper_execution_roots", return_value=ExecRoots()):
        stages = orchestrator_v2._build_stage_defs(
            truth=canonical_truth,
            day="2026-04-15",
            input_day="2026-04-15",
            ib_account="DUO847203",
            git_sha="c" * 40,
            attempt_id="2026-04-15__attempt",
        )

    stage_ids = [stage.stage_id for stage in stages]
    assert stage_ids.index("B0_EXECUTION_STREAM_SNAPSHOT_V1") < stage_ids.index("B0AC_SUBMISSION_LIFECYCLE_REFRESH_V1")
    assert stage_ids.index("B0AC_SUBMISSION_LIFECYCLE_REFRESH_V1") < stage_ids.index("B1_FILL_LEDGER_V1")


def test_submission_lifecycle_refresh_stage_invokes_refresh_tool_for_each_submission(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-15"
    submissions_day = truth_root / "execution_evidence_v1" / "submissions" / day
    (submissions_day / "sub_a").mkdir(parents=True)
    (submissions_day / "sub_b").mkdir(parents=True)
    (submissions_day / "sub_bad_plan_version").mkdir(parents=True)
    (submissions_day / "sub_skip_no_broker_record").mkdir(parents=True)
    (submissions_day / "__tombstones__").mkdir(parents=True)
    (submissions_day / "sub_a" / "broker_submission_record.v2.json").write_text("{}\n", encoding="utf-8")
    (submissions_day / "sub_b" / "broker_submission_record.v2.json").write_text("{}\n", encoding="utf-8")
    (submissions_day / "sub_bad_plan_version" / "broker_submission_record.v2.json").write_text("{}\n", encoding="utf-8")
    (submissions_day / "sub_a" / "equity_order_plan.v2.json").write_text(
        json.dumps({"schema_id": "equity_order_plan", "schema_version": "v2"}) + "\n",
        encoding="utf-8",
    )
    (submissions_day / "sub_b" / "order_plan.v1.json").write_text(
        json.dumps({"schema_id": "order_plan", "schema_version": "v1"}) + "\n",
        encoding="utf-8",
    )
    # Malformed shape (unsupported schema version) must be excluded from lifecycle refresh candidates.
    (submissions_day / "sub_bad_plan_version" / "equity_order_plan.v1.json").write_text(
        json.dumps({"schema_id": "equity_order_plan", "schema_version": "v3"}) + "\n",
        encoding="utf-8",
    )

    captured: list[tuple[str, list[str]]] = []

    def fake_run_cmd(stage_id: str, cmd: list[str], env: dict[str, str]) -> int:
        captured.append((stage_id, list(cmd)))
        return 0

    with mock.patch.object(orchestrator_v2, "_run_cmd", side_effect=fake_run_cmd):
        executed, rc, reason_codes, outputs_present = orchestrator_v2._run_submission_lifecycle_refresh_stage(
            truth_root=truth_root,
            day=day,
            env={},
        )

    assert executed is True
    assert rc == 0
    assert reason_codes == [
        "SUBMISSION_LIFECYCLE_REFRESH_PENDING_NO_STREAM:sub_a",
        "SUBMISSION_LIFECYCLE_REFRESH_PENDING_NO_STREAM:sub_b",
    ]
    assert outputs_present == [
        str((submissions_day / "sub_a" / "broker_submission_record.v2.json").resolve()),
        str((submissions_day / "sub_b" / "broker_submission_record.v2.json").resolve()),
    ]
    assert captured == [
        (
            "B0AC_SUBMISSION_LIFECYCLE_REFRESH_V1_sub_a",
            [
                "python3",
                "ops/tools/run_submission_lifecycle_refresh_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth_root),
                "--submission_id",
                "sub_a",
            ],
        ),
        (
            "B0AC_SUBMISSION_LIFECYCLE_REFRESH_V1_sub_b",
            [
                "python3",
                "ops/tools/run_submission_lifecycle_refresh_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                str(truth_root),
                "--submission_id",
                "sub_b",
            ],
        ),
    ]


def test_execution_stream_snapshot_skip_safe_requires_stream_coverage_for_all_authoritative_submissions(
    tmp_path: Path,
) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-23"
    submissions_day = truth_root / "execution_evidence_v1" / "submissions" / day
    stream_day = truth_root / "execution_stream_v1" / day
    (submissions_day / "sub_a").mkdir(parents=True)
    (submissions_day / "sub_b").mkdir(parents=True)
    stream_day.mkdir(parents=True)

    for sub in ("sub_a", "sub_b"):
        (submissions_day / sub / "broker_submission_record.v2.json").write_text("{}\n", encoding="utf-8")
        (submissions_day / sub / "equity_order_plan.v1.json").write_text(
            json.dumps({"schema_id": "equity_order_plan", "schema_version": "v1"}) + "\n",
            encoding="utf-8",
        )

    (stream_day / "event-sub-a.execution_event_stream_record.v1.json").write_text(
        json.dumps({"submission_id": "sub_a"}) + "\n",
        encoding="utf-8",
    )

    assert orchestrator_v2._execution_stream_snapshot_skip_safe(truth_root, day) is False


def test_execution_stream_snapshot_skip_safe_true_when_every_authoritative_submission_has_stream_record(
    tmp_path: Path,
) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-23"
    submissions_day = truth_root / "execution_evidence_v1" / "submissions" / day
    stream_day = truth_root / "execution_stream_v1" / day
    (submissions_day / "sub_a").mkdir(parents=True)
    (submissions_day / "sub_b").mkdir(parents=True)
    stream_day.mkdir(parents=True)

    for sub in ("sub_a", "sub_b"):
        (submissions_day / sub / "broker_submission_record.v2.json").write_text("{}\n", encoding="utf-8")
        (submissions_day / sub / "equity_order_plan.v2.json").write_text(
            json.dumps({"schema_id": "equity_order_plan", "schema_version": "v2"}) + "\n",
            encoding="utf-8",
        )
        (stream_day / f"event-{sub}.execution_event_stream_record.v1.json").write_text(
            json.dumps({"submission_id": sub}) + "\n",
            encoding="utf-8",
        )

    assert orchestrator_v2._execution_stream_snapshot_skip_safe(truth_root, day) is True
