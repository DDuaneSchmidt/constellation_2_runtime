from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.execution_outcome_v1 import derive_execution_outcome_payload
from constellation_2.common.next_day_readiness_probe_v1 import derive_next_day_readiness_probe_payload
from constellation_2.common.runtime_path_authority_v1 import (
    RuntimePathAuthorityV1,
    classify_runtime_path_v1,
    resolve_decision_truth_root_v1,
)
import constellation_2.common.runtime_path_authority_v1 as runtime_path_module


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _trade_submit_status(*, day_utc: str, truth_root: Path, environment: str, ib_account: str) -> dict:
    return {
        "schema_id": "trade_submit_readiness_c2",
        "schema_version": "v1",
        "day_utc": day_utc,
        "as_of_utc": f"{day_utc}T00:00:00Z",
        "expires_utc": f"{day_utc}T23:59:59Z",
        "ok": True,
        "state": "OK",
        "environment": environment,
        "ib_account": ib_account,
        "reasons": [],
        "input_manifest": [],
        "producer": {
            "repo": "constellation",
            "module": "ops/tools/run_trade_submit_readiness_c2_v1.py",
            "git_sha": "a" * 40,
        },
        "provenance": {
            "truth_root": str(truth_root),
            "registry_sha256": "a" * 64,
            "sleeve_registry_sha256": "b" * 64,
        },
        "session_authority_attestation": {
            "decision_artifact_path": str(truth_root / "reports" / "day_authority_decision_v1" / day_utc / "day_authority_decision.v1.json"),
            "decision_artifact_sha256": "0" * 64,
            "policy_version": "validation_result_only",
            "evaluator_version": "validation_result_only",
            "venue": "C2",
            "session_date": day_utc,
            "decision_status": "UNKNOWN",
            "session_class": None,
            "stage_id": "PRE_ORCHESTRATION_PREFLIGHT",
            "policy_action": "SKIP",
            "stage_execution_status": "OK",
            "reason_codes": [],
        },
        "run_state_authority_attestation": {
            "authority_family": "day_authority_decision_v1",
            "authority_artifact_path": str(truth_root / "reports" / "day_authority_decision_v1" / day_utc / "day_authority_decision.v1.json"),
            "authority_artifact_sha256": "0" * 64,
            "policy_version": "validation_result_only",
            "evaluator_version": "validation_result_only",
            "decision_status": "UNKNOWN",
            "classification_field": "decision_state",
            "classification_value": "UNKNOWN",
            "cycle_snapshot_family": "paper_policy_verdict_v1",
            "cycle_snapshot_artifact_path": str(truth_root / "reports" / "paper_policy_verdict_v1" / day_utc / "paper_policy_verdict.v1.json"),
            "cycle_snapshot_artifact_sha256": "c" * 64,
            "cycle_id": f"{day_utc}:OK",
            "cycle_coherence_status": "COHERENT",
            "stage_id": "TRADE_SUBMIT_READINESS",
            "stage_execution_status": "OK",
            "reason_codes": [],
            "upstream_authority_refs": [],
        },
    }


def test_runtime_path_authority_resolves_canonical_and_rejects_release_local(monkeypatch, tmp_path: Path) -> None:
    authority = RuntimePathAuthorityV1(
        authoritative_repo_root=(tmp_path / "repo").resolve(),
        canonical_runtime_truth_root=(tmp_path / "runtime_data" / "truth").resolve(),
        canonical_runtime_truth_sleeves_root=(tmp_path / "runtime_data" / "truth_sleeves").resolve(),
        authoritative_repo_truth_root=(tmp_path / "repo" / "constellation_2" / "runtime" / "truth").resolve(),
        authoritative_repo_truth_sleeves_root=(tmp_path / "repo" / "constellation_2" / "runtime" / "truth_sleeves").resolve(),
        active_release_root=(tmp_path / "release").resolve(),
    )
    for path in (
        authority.canonical_runtime_truth_root,
        authority.canonical_runtime_truth_sleeves_root,
        authority.authoritative_repo_truth_root,
        authority.authoritative_repo_truth_sleeves_root,
        authority.active_release_root,
    ):
        path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(runtime_path_module, "load_runtime_path_authority_v1", lambda repo_root=None: authority)

    resolved = resolve_decision_truth_root_v1(str(authority.canonical_runtime_truth_root), repo_root=SOURCE_ROOT)
    assert resolved == authority.canonical_runtime_truth_root
    classified = classify_runtime_path_v1(authority.canonical_runtime_truth_sleeves_root / "PRIMARY", authority=authority)
    assert classified["path_class"] == "CANONICAL_RUNTIME_TRUTH_SLEEVES_SUBPATH"

    try:
        resolve_decision_truth_root_v1(str(authority.active_release_root / "constellation_2" / "runtime" / "truth"), repo_root=SOURCE_ROOT)
    except ValueError as exc:
        assert "RUNTIME_PATH_AUTHORITY_POLICY_TRUTH_ROOT_FORBIDDEN" in str(exc)
    else:
        raise AssertionError("expected release-local policy root rejection")


def test_execution_outcome_classifies_clean_self_heal_deferred_and_fail() -> None:
    clean = derive_execution_outcome_payload(
        truth_root=SOURCE_ROOT,
        context={
            "day_utc": "2026-04-09",
            "release_id": "r1",
            "git_sha": "a" * 40,
            "entrypoint": "entry.sh",
            "overall_exit_code": 0,
            "generated_at_utc": "2026-04-09T00:00:00Z",
            "runs": {"stage_a": {"cmd": ["x"], "returncode": 0, "stdout": "OK", "stderr": ""}},
            "source_artifacts": [],
        },
    )
    assert clean["execution_status"] == "PASS"
    assert clean["clean_run_status"] == "CLEAN"

    self_heal = derive_execution_outcome_payload(
        truth_root=SOURCE_ROOT,
        context={
            "day_utc": "2026-04-09",
            "release_id": "r1",
            "git_sha": "a" * 40,
            "entrypoint": "entry.sh",
            "overall_exit_code": 0,
            "generated_at_utc": "2026-04-09T00:00:00Z",
            "runs": {"stage_a": {"cmd": ["x"], "returncode": 0, "stdout": "WARN: REFRESHED_STALE_X", "stderr": ""}},
            "source_artifacts": [],
        },
    )
    assert self_heal["execution_status"] == "PASS_WITH_SELF_HEAL"

    deferred = derive_execution_outcome_payload(
        truth_root=SOURCE_ROOT,
        context={
            "day_utc": "2026-04-09",
            "release_id": "r1",
            "git_sha": "a" * 40,
            "entrypoint": "entry.sh",
            "overall_exit_code": 0,
            "generated_at_utc": "2026-04-09T00:00:00Z",
            "runs": {"stage_a": {"cmd": ["x"], "returncode": 0, "stdout": "status=DEFERRED_EXISTING_JOURNAL_IDENTITY_MISMATCH", "stderr": ""}},
            "source_artifacts": [],
        },
    )
    assert deferred["execution_status"] == "PASS_WITH_DEFERRED"

    failed = derive_execution_outcome_payload(
        truth_root=SOURCE_ROOT,
        context={
            "day_utc": "2026-04-09",
            "release_id": "r1",
            "git_sha": "a" * 40,
            "entrypoint": "entry.sh",
            "overall_exit_code": 1,
            "generated_at_utc": "2026-04-09T00:00:00Z",
            "runs": {"stage_a": {"cmd": ["x"], "returncode": 1, "stdout": "", "stderr": "FAIL: bad"}},
            "source_artifacts": [],
        },
    )
    assert failed["execution_status"] == "FAIL"


def test_next_day_readiness_probe_ready_blocked_unknown(monkeypatch, tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    release_id = "20260410T000000Z__abc"
    git_sha = "a" * 40
    monkeypatch.setattr(
        "constellation_2.common.next_day_readiness_probe_v1.resolve_release_provenance",
        lambda: {"release_id": release_id, "git_sha": git_sha},
    )
    monkeypatch.setattr(
        "constellation_2.common.next_day_readiness_probe_v1.resolve_runtime_path_authority_snapshot_v1",
        lambda repo_root=None: {
            "authoritative_repo_root": str(tmp_path / "repo"),
            "canonical_runtime_truth_root": str(truth_root),
            "canonical_runtime_truth_sleeves_root": str(tmp_path / "truth_sleeves"),
            "authoritative_repo_truth_root": str(tmp_path / "repo" / "constellation_2" / "runtime" / "truth"),
            "authoritative_repo_truth_sleeves_root": str(tmp_path / "repo" / "constellation_2" / "runtime" / "truth_sleeves"),
            "active_release_root": str(tmp_path / "release"),
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.next_day_readiness_probe_v1.resolve_decision_truth_root_v1",
        lambda truth_root, repo_root=None: Path(truth_root).resolve(),
    )

    current_day = "2026-04-09"
    next_day = "2026-04-10"
    env = "PAPER"
    acct = "DUO847203"

    _write_json(
        truth_root / "reports" / "paper_policy_verdict_v1" / current_day / "paper_policy_verdict.v1.json",
        {"schema_id": "paper_policy_verdict", "schema_version": "v1", "day_utc": current_day, "overall_status": "PASS", "paper_allowed": True, "blocking_items": [], "advisory_items": [], "production_only_open_items": [], "capability_state_ref": {"artifact_path": str(truth_root / "reports" / "capability_state_v1" / current_day / "capability_state.v1.json"), "artifact_sha256": "d" * 64}, "source_artifacts": [], "release_id": release_id, "git_sha": git_sha, "generated_at_utc": f"{current_day}T00:00:00Z", "environment": env, "ib_account": acct, "sleeve_id": "PRIMARY"},
    )
    _write_json(
        truth_root / "reports" / "production_policy_verdict_v1" / current_day / "production_policy_verdict.v1.json",
        {"schema_id": "production_policy_verdict", "schema_version": "v1", "day_utc": current_day, "overall_status": "PASS", "production_allowed": True, "blocking_items": [], "gate_stack_status": "PASS", "gate_stack_reason_codes": [], "capability_state_ref": {"artifact_path": str(truth_root / "reports" / "capability_state_v1" / current_day / "capability_state.v1.json"), "artifact_sha256": "d" * 64}, "gate_stack_ref": {"artifact_path": str(tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER" / "reports" / "gate_stack_verdict_v1" / current_day / "gate_stack_verdict.v1.json"), "artifact_sha256": "e" * 64}, "source_artifacts": [], "release_id": release_id, "git_sha": git_sha, "generated_at_utc": f"{current_day}T00:00:00Z", "environment": env, "ib_account": acct, "sleeve_id": "PRIMARY"},
    )
    _write_json(
        truth_root / "trade_submit_readiness_c2_v1" / "_history" / env / acct / current_day / "status.json",
        _trade_submit_status(day_utc=current_day, truth_root=truth_root, environment=env, ib_account=acct),
    )
    unknown = derive_next_day_readiness_probe_payload(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        target_day_utc=next_day,
        environment=env,
        ib_account=acct,
    )
    assert unknown["probe_status"] == "UNKNOWN"

    _write_json(
        truth_root / "reports" / "paper_policy_verdict_v1" / next_day / "paper_policy_verdict.v1.json",
        {"schema_id": "paper_policy_verdict", "schema_version": "v1", "day_utc": next_day, "overall_status": "PASS", "paper_allowed": True, "blocking_items": [], "advisory_items": [], "production_only_open_items": [], "capability_state_ref": {"artifact_path": str(truth_root / "reports" / "capability_state_v1" / next_day / "capability_state.v1.json"), "artifact_sha256": "d" * 64}, "source_artifacts": [], "release_id": release_id, "git_sha": git_sha, "generated_at_utc": f"{next_day}T00:00:00Z", "environment": env, "ib_account": acct, "sleeve_id": "PRIMARY"},
    )
    _write_json(
        truth_root / "trade_submit_readiness_c2_v1" / "_history" / env / acct / next_day / "status.json",
        _trade_submit_status(day_utc=next_day, truth_root=truth_root, environment=env, ib_account=acct),
    )
    ready = derive_next_day_readiness_probe_payload(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        target_day_utc=next_day,
        environment=env,
        ib_account=acct,
    )
    assert ready["probe_status"] == "READY"

    _write_json(
        truth_root / "reports" / "paper_policy_verdict_v1" / next_day / "paper_policy_verdict.v1.json",
        {"schema_id": "paper_policy_verdict", "schema_version": "v1", "day_utc": next_day, "overall_status": "FAIL", "paper_allowed": False, "blocking_items": [{"capability_id": "x", "status": "FAIL", "paper_role": "BLOCKING", "production_role": "BLOCKING", "reason_codes": [], "rationale": "x"}], "advisory_items": [], "production_only_open_items": [], "capability_state_ref": {"artifact_path": str(truth_root / "reports" / "capability_state_v1" / next_day / "capability_state.v1.json"), "artifact_sha256": "d" * 64}, "source_artifacts": [], "release_id": release_id, "git_sha": git_sha, "generated_at_utc": f"{next_day}T00:00:00Z", "environment": env, "ib_account": acct, "sleeve_id": "PRIMARY"},
    )
    blocked = derive_next_day_readiness_probe_payload(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        target_day_utc=next_day,
        environment=env,
        ib_account=acct,
    )
    assert blocked["probe_status"] == "BLOCKED"


def test_next_day_readiness_probe_returns_unknown_when_reference_day_not_materialized(monkeypatch, tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    monkeypatch.setattr(
        "constellation_2.common.next_day_readiness_probe_v1.resolve_release_provenance",
        lambda: {"release_id": "20260410T000000Z__abc", "git_sha": "a" * 40},
    )
    monkeypatch.setattr(
        "constellation_2.common.next_day_readiness_probe_v1.resolve_runtime_path_authority_snapshot_v1",
        lambda repo_root=None: {
            "authoritative_repo_root": str(tmp_path / "repo"),
            "canonical_runtime_truth_root": str(truth_root),
            "canonical_runtime_truth_sleeves_root": str(tmp_path / "truth_sleeves"),
            "authoritative_repo_truth_root": str(tmp_path / "repo" / "constellation_2" / "runtime" / "truth"),
            "authoritative_repo_truth_sleeves_root": str(tmp_path / "repo" / "constellation_2" / "runtime" / "truth_sleeves"),
            "active_release_root": str(tmp_path / "release"),
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.next_day_readiness_probe_v1.resolve_decision_truth_root_v1",
        lambda truth_root, repo_root=None: Path(truth_root).resolve(),
    )

    payload = derive_next_day_readiness_probe_payload(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        target_day_utc="2026-04-11",
        environment="PAPER",
        ib_account="DUO847203",
    )

    assert payload["probe_status"] == "UNKNOWN"
    assert payload["confidence"] == "LOW"
    assert payload["predicted_blocking_items"][0]["reason"] == "CURRENT_DAY_BASELINE_NOT_MATERIALIZED"
