from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.diagnostic_foundation_v1 import (
    build_batch1_docs,
    select_attempt_for_day,
    write_activity_flow_diagnostics,
    write_batch1_diagnostics,
    write_runtime_regression_analytics,
)


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _stage(stage_id: str, status: str, *, required: bool, blocking: bool, executed: bool = True, reason_codes=None):
    return {
        "stage_id": stage_id,
        "classification": {
            "required_for_mode": {"PAPER": True, "LIVE": False},
            "required_if_activity": False,
            "blocking": blocking,
            "effective_required": required,
            "effective_blocking": blocking,
        },
        "executed": executed,
        "rc": 0 if status in ("OK", "SKIP") else 1,
        "status": status,
        "reason_codes": reason_codes or [],
        "outputs_present": [],
    }


def _write_day_fixture(truth_root: Path, day: str, *, attempt_id: str, verdict_status: str, intents: int, submitted: int, filled: int, rejected: int, vetoed: int) -> None:
    attempt_dir = truth_root / "reports" / "orchestrator_run_verdict_v2" / day / attempt_id
    pointer_idx = truth_root / "reports" / "orchestrator_run_verdict_v2" / day / "canonical_pointer_index.v1.jsonl"
    attempt_manifest_path = attempt_dir / "orchestrator_attempt_manifest.v2.json"
    verdict_path = attempt_dir / "orchestrator_run_verdict.v2.json"

    if verdict_status == "FAIL":
        auth_stage = _stage("A6B_AUTHORIZATION_ARTIFACTS_DAY_V1", "FAIL", required=True, blocking=False, reason_codes=["AUTHORIZATION_FAILED"])
        recon_stage = _stage("B2_EXECUTION_RECONCILIATION_V1", "SKIP", required=False, blocking=False, executed=False, reason_codes=["SKIP_NOT_REQUIRED_NO_ACTIVITY"])
    else:
        auth_stage = _stage("A6B_AUTHORIZATION_ARTIFACTS_DAY_V1", "OK", required=True, blocking=False)
        recon_stage = _stage("B2_EXECUTION_RECONCILIATION_V1", "OK", required=False, blocking=False)

    stages = [
        _stage("A0_ENFORCE_SINGLE_ACCOUNT_TOPOLOGY", "OK", required=True, blocking=False, executed=False, reason_codes=["SLEEVE_ACCOUNT_BINDING_ENFORCED"]),
        auth_stage,
        recon_stage,
    ]

    attempt_manifest = {
        "schema_id": "C2_ORCHESTRATOR_ATTEMPT_MANIFEST_V2",
        "day_utc": day,
        "input_day_utc": day,
        "mode": "PAPER",
        "symbol": "SPY",
        "ib_account": "DUO847203",
        "attempt_id": attempt_id,
        "attempt_seq": 1,
        "produced_utc": f"{day}T00:00:00Z",
        "producer": {"repo": "constellation_2_runtime", "module": "fixture", "git_sha": "fixture"},
        "activity": {
            "activity": intents > 0 or submitted > 0 or filled > 0,
            "intents_json_count": intents,
            "exec_submission_json_count": submitted,
            "fill_ledger_json_count": filled,
            "paths": {},
        },
        "effective_activity": intents > 0 or submitted > 0 or filled > 0,
        "session": {"session_state": "TRADING_SESSION"},
        "stages": stages,
        "outputs": [],
    }
    verdict = {
        "schema_id": "C2_ORCHESTRATOR_RUN_VERDICT_V2",
        "day_utc": day,
        "input_day_utc": day,
        "mode": "PAPER",
        "symbol": "SPY",
        "ib_account": "DUO847203",
        "attempt_id": attempt_id,
        "attempt_seq": 1,
        "produced_utc": f"{day}T00:00:00Z",
        "status": verdict_status,
        "safety_breaches": [],
        "reason_codes": ["NO_ACTIVITY_DAY"] if intents == 0 else [],
        "stages": stages,
        "replay": {"derived_from_attempt_manifest": True, "hashes": {"attempt_manifest_sha256": "fixture"}},
        "producer": {"repo": "constellation_2_runtime", "module": "fixture", "git_sha": "fixture"},
    }
    pointer_entry = {
        "schema_id": "C2_ORCHESTRATOR_RUN_VERDICT_V2_POINTER_INDEX_V1",
        "pointer_seq": 1,
        "day_utc": day,
        "mode": "PAPER",
        "attempt_id": attempt_id,
        "attempt_seq": 1,
        "status": verdict_status,
        "authoritative": verdict_status == "PASS",
        "producer_git_sha": "fixture",
        "produced_utc": f"{day}T00:00:00Z",
        "points_to": str(verdict_path),
        "attempt_manifest_path": str(attempt_manifest_path),
    }
    intents_rollup = {
        "schema_id": "intents_day_rollup.v1",
        "day_utc": day,
        "produced_utc": f"{day}T00:00:00Z",
        "producer": {"component": "fixture", "version": "v1", "git_sha": "fixture"},
        "inputs": {"market_data_snapshot_hashes": [], "market_calendar_hash": "", "engine_config_hashes": []},
        "engines": [
            {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "intent_type": "exposure_intent.v1", "intent_hashes": [], "intent_count": intents}
        ],
        "rollup_sha256": "fixture"
    }

    _write_json(attempt_manifest_path, attempt_manifest)
    _write_json(verdict_path, verdict)
    pointer_idx.parent.mkdir(parents=True, exist_ok=True)
    pointer_idx.write_text(json.dumps(pointer_entry, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    _write_json(truth_root / "intents_v1" / "day_rollup" / day / "intents_day_rollup.v1.json", intents_rollup)

    auth_root = truth_root / "engine_activity_v1" / "authorization_v1" / day
    for idx in range(rejected):
        _write_json(
            auth_root / f"rej{idx}.authorization.v1.json",
            {"intent_hash": f"rej{idx}", "engine_id": "C2_TREND_EQ_PRIMARY_V1", "status": "REJECTED"},
        )
    for idx in range(max(intents - rejected, 0)):
        _write_json(
            auth_root / f"auth{idx}.authorization.v1.json",
            {"intent_hash": f"auth{idx}", "engine_id": "C2_TREND_EQ_PRIMARY_V1", "status": "AUTHORIZED"},
        )

    veto_root = truth_root / "phaseC_preflight_v1" / day / "intent_group"
    for idx in range(vetoed):
        _write_json(veto_root / f"veto{idx}.veto_record.v1.json", {"reason_code": "VETO"})

    submission_root = truth_root / "execution_evidence_v1" / "submissions" / day
    for idx in range(submitted):
        bucket = submission_root / f"intent_{idx}"
        _write_json(bucket / "broker_submission_record.v2.json", {"schema_id": "broker_submission_record.v2", "status": "SUBMITTED"})
        if idx < filled:
            _write_json(bucket / "execution_event_record.v1.json", {"schema_id": "execution_event_record.v1", "status": "FILL"})


def test_select_attempt_for_day(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_day_fixture(truth_root, "2026-04-02", attempt_id="2026-04-02__A0001", verdict_status="PASS", intents=3, submitted=2, filled=1, rejected=1, vetoed=0)
    selection = select_attempt_for_day(truth_root, "2026-04-02")
    assert selection.pointer_entry["attempt_id"] == "2026-04-02__A0001"
    assert selection.verdict["status"] == "PASS"


def test_write_activity_flow_diagnostics(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_day_fixture(truth_root, "2026-04-02", attempt_id="2026-04-02__A0001", verdict_status="PASS", intents=3, submitted=2, filled=1, rejected=1, vetoed=0)
    wr = write_activity_flow_diagnostics(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-02",
        produced_utc="2026-04-02T00:00:00Z",
    )
    assert wr.action == "WROTE"
    doc = json.loads((truth_root / "reports" / "activity_flow_diagnostics_v1" / "2026-04-02" / "activity_flow_diagnostics.v1.json").read_text(encoding="utf-8"))
    assert doc["counts"]["intents"] == 3
    assert doc["counts"]["submitted"] == 2
    assert doc["counts"]["filled"] == 1
    assert doc["rejection_breakdown"][0]["count"] == 1


def test_write_batch1_diagnostics(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_day_fixture(truth_root, "2026-04-03", attempt_id="2026-04-03__A0001", verdict_status="FAIL", intents=0, submitted=0, filled=0, rejected=0, vetoed=0)
    writes = write_batch1_diagnostics(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert writes["root_cause"].action == "WROTE"
    docs = build_batch1_docs(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert docs["root_cause"]["deterministic_classification"] == "INVARIANT_VIOLATION"
    assert docs["intent_absence"]["zero_intent_classification"] == "UPSTREAM_BLOCKED_NO_INTENT_PATH"


def test_write_runtime_regression_analytics(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_day_fixture(truth_root, "2026-04-02", attempt_id="2026-04-02__A0001", verdict_status="PASS", intents=3, submitted=2, filled=1, rejected=1, vetoed=0)
    _write_day_fixture(truth_root, "2026-04-03", attempt_id="2026-04-03__A0001", verdict_status="FAIL", intents=0, submitted=0, filled=0, rejected=0, vetoed=0)
    write_activity_flow_diagnostics(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-02", produced_utc="2026-04-02T00:00:00Z")
    write_activity_flow_diagnostics(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03", produced_utc="2026-04-03T00:00:00Z")
    wr = write_runtime_regression_analytics(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert wr.action == "WROTE"
    doc = json.loads((truth_root / "reports" / "runtime_regression_analytics_v1" / "2026-04-03" / "runtime_regression_analytics.v1.json").read_text(encoding="utf-8"))
    assert doc["comparison_mode"] == "PRIOR_DAY"
    assert doc["comparison_status"] == "PRIOR_DAY_AVAILABLE"
    assert any(row["metric_id"] == "intents" and row["absolute_change"] == -3 for row in doc["compared_metrics"])


def test_runtime_regression_unavailable_without_prior_day(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_day_fixture(truth_root, "2026-04-03", attempt_id="2026-04-03__A0001", verdict_status="PASS", intents=1, submitted=1, filled=1, rejected=0, vetoed=0)
    write_activity_flow_diagnostics(repo_root=REPO_ROOT, truth_root=truth_root, day_utc="2026-04-03", produced_utc="2026-04-03T00:00:00Z")
    wr = write_runtime_regression_analytics(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-03",
        produced_utc="2026-04-03T00:00:00Z",
    )
    assert wr.action == "WROTE"
    doc = json.loads((truth_root / "reports" / "runtime_regression_analytics_v1" / "2026-04-03" / "runtime_regression_analytics.v1.json").read_text(encoding="utf-8"))
    assert doc["comparison_status"] == "PRIOR_DAY_UNAVAILABLE"
