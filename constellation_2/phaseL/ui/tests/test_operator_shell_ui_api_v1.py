from __future__ import annotations

from types import SimpleNamespace

from constellation_2.phaseL.ui_api.kernel_operator_shell_v1 import (
    build_kernel_status_rail_view,
    build_operator_work_queue_view,
    build_workspace_view,
    dispatch_kernel_command,
)


def _workspace(workspace_id: str, code: str, reason_codes: list[str] | None = None) -> dict:
    return {
        "ok": True,
        "workspace_id": workspace_id,
        "status": {
            "code": code,
            "label": code.title(),
            "semantic": "healthy" if code == "ready" else ("blocked" if code == "blocked" else "warning"),
            "reason_codes": reason_codes or [],
        },
    }


def test_kernel_status_rail_is_built_from_workspace_authority_states(monkeypatch) -> None:
    monkeypatch.setattr(
        "constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_control_workspace_view",
        lambda: _workspace("control", "ready"),
    )
    monkeypatch.setattr(
        "constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_state_workspace_view",
        lambda: _workspace("state", "blocked", ["SNAPSHOT_BLOCKED"]),
    )
    monkeypatch.setattr(
        "constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_advisory_workspace_view",
        lambda: _workspace("advisory", "missing", ["ADVISORY_CHAIN_MISSING"]),
    )
    monkeypatch.setattr(
        "constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_submission_workspace_view",
        lambda: _workspace("submission", "in_progress"),
    )
    monkeypatch.setattr(
        "constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_lifecycle_workspace_view",
        lambda: _workspace("lifecycle", "terminal"),
    )

    payload = build_kernel_status_rail_view()
    assert payload["ok"] is True
    kernels = {row["kernel_id"]: row for row in payload["kernels"]}
    assert kernels["control"]["status"]["code"] == "ready"
    assert kernels["state"]["status"]["reason_codes"] == ["SNAPSHOT_BLOCKED"]
    assert kernels["advisory"]["href"] == "/advisory"


def test_operator_work_queue_uses_authoritative_status_only(monkeypatch) -> None:
    monkeypatch.setattr(
        "constellation_2.phaseL.ui_api.kernel_operator_shell_v1.build_kernel_status_rail_view",
        lambda: {
            "ok": True,
            "kernels": [
                {"kernel_id": "control", "label": "Control", "href": "/control", "status": {"code": "ready", "label": "Ready", "semantic": "healthy", "reason_codes": []}},
                {"kernel_id": "state", "label": "State", "href": "/state", "status": {"code": "blocked", "label": "Blocked", "semantic": "blocked", "reason_codes": ["SNAPSHOT_INVALID"]}},
                {"kernel_id": "submission", "label": "Submission", "href": "/submission", "status": {"code": "stale", "label": "Stale", "semantic": "warning", "reason_codes": ["SUBMISSION_STALE"]}},
            ],
        },
    )

    payload = build_operator_work_queue_view()
    assert payload["ok"] is True
    assert [item["kernel_id"] for item in payload["work_items"]] == ["state", "submission"]
    assert "summary_metrics" not in payload


def test_submission_command_requires_current_explicit_execution_intent(monkeypatch) -> None:
    current_intent = SimpleNamespace(execution_intent_id="intent-current")
    current_record = SimpleNamespace(submission_record_id="record-current")
    monkeypatch.setattr(
        "constellation_2.phaseL.ui_api.kernel_operator_shell_v1._current_execution_intent_for_submit",
        lambda: (current_intent, current_record),
    )

    blocked = dispatch_kernel_command(
        "/api/commands/submission/submit",
        {"execution_intent_id": "intent-stale"},
    )
    assert blocked["outcome"] == "blocked"
    assert "SUPERSEDED_OR_NONCURRENT_EXECUTION_INTENT" in blocked["reason_codes"]


def test_submission_command_returns_envelope_and_artifact_refs(monkeypatch, tmp_path) -> None:
    current_intent = SimpleNamespace(
        execution_intent_id="intent-current",
        account_id="DU123",
    )
    current_record = SimpleNamespace(
        submission_record_id="record-current",
        day_utc="2026-04-13",
        submission_id="submission-current",
    )
    monkeypatch.setattr(
        "constellation_2.phaseL.ui_api.kernel_operator_shell_v1._current_execution_intent_for_submit",
        lambda: (current_intent, current_record),
    )
    monkeypatch.setattr(
        "constellation_2.phaseL.ui_api.kernel_operator_shell_v1.run_execution_kernel_v1",
        lambda **_: {
            "execution_run_envelope": SimpleNamespace(run_outcome="submit", reason_codes=("SUBMITTED",), run_id="env-submit"),
            "execution_run_envelope_path": "/tmp/env-submit.json",
            "submission_record": SimpleNamespace(submission_record_id="record-current", day_utc="2026-04-13", submission_id="submission-current"),
            "execution_state_record": None,
            "submission_decision_path": "/tmp/decision.json",
        },
    )

    budget_path = tmp_path / "risk_budget.json"
    budget_path.write_text("{}", encoding="utf-8")

    payload = dispatch_kernel_command(
        "/api/commands/submission/submit",
        {
            "execution_intent_id": "intent-current",
            "risk_budget_path": str(budget_path),
            "ib_host": "127.0.0.1",
            "ib_port": "7497",
            "ib_client_id": "1",
            "dry_run": True,
        },
    )
    assert payload["outcome"] == "submit"
    assert payload["envelope_ref"]["path"] == "/tmp/env-submit.json"
    assert payload["authority_artifact_ref"]["artifact_type"] == "SubmissionRecord"


def test_lifecycle_refresh_command_requires_current_submission_record(monkeypatch) -> None:
    monkeypatch.setattr(
        "constellation_2.phaseL.ui_api.kernel_operator_shell_v1._latest_submission_record",
        lambda: (None, None),
    )
    payload = dispatch_kernel_command(
        "/api/commands/lifecycle/refresh",
        {"submission_record_id": "missing"},
    )
    assert payload["outcome"] == "blocked"
    assert "SUBMISSION_RECORD_MISSING" in payload["reason_codes"]


def test_real_workspace_queries_fail_closed_to_payloads() -> None:
    for workspace_id in ["control", "state", "advisory", "submission", "lifecycle"]:
        payload = build_workspace_view(workspace_id)
        assert isinstance(payload, dict)
        assert payload["workspace_id"] == workspace_id
        assert "status" in payload
        assert "authority_artifacts" in payload
        assert "lineage_chain" in payload
