from __future__ import annotations
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from constellation_2.phaseL.ui_api.reliability_ledger_v1 import (
    assess_reliability_readiness_v1,
    create_reliability_fix_attempt_v1,
    create_reliability_issue_v1,
    create_reliability_issue_work_order_v1,
    create_reliability_issue_verification_v1,
    create_reliability_observation_v1,
    create_reliability_verification_v1,
    create_reliability_work_order_from_issue_v1,
    create_reliability_work_order_v1,
    draft_reliability_issue_v1,
    get_reliability_fix_attempt_v1,
    get_reliability_issue_v1,
    get_reliability_work_order_v1,
    link_reliability_issue_observation_v1,
    list_reliability_fix_attempts_v1,
    update_reliability_issue_v1,
    list_reliability_next_actions_v1,
    list_reliability_work_orders_v1,
)


ROOT = Path(__file__).resolve().parents[4]


def _set_db_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    db_path = (tmp_path / "reliability_ledger.sqlite3").resolve()
    monkeypatch.setenv("C2_RELIABILITY_LEDGER_DB_PATH", str(db_path))
    return db_path


def _observation_payload(summary: str = "test observation") -> dict[str, object]:
    return {
        "source": "operator_test",
        "environment": "PAPER",
        "run_id": "run-1",
        "component": "execution",
        "event_type": "test_event",
        "severity_hint": "high",
        "summary": summary,
        "structured_payload": {"summary": summary},
    }


def _issue_payload(*, canonical_key: str, severity: str = "medium", category: str = "infrastructure", status: str = "open") -> dict[str, object]:
    return {
        "title": f"Issue for {canonical_key}",
        "type": "bug",
        "category": category,
        "severity": severity,
        "status": status,
        "canonical_key": canonical_key,
        "impact_summary": "Impact summary",
        "expected_behavior": "Expected behavior",
        "actual_behavior": "Actual behavior",
        "created_by": "operator",
        "codex_status": "not_needed",
    }


def test_reliability_routes_and_endpoints_are_wired() -> None:
    server = (ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    pages = pages_source_v1(ROOT)
    domain_client = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js").read_text(encoding="utf-8")
    api_client = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/api_client/index.js").read_text(encoding="utf-8")
    main_js = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")

    for route in [
        'path: "/reliability"',
        'path: "/reliability/issues"',
        'path: "/reliability/issues/detail"',
        'path: "/reliability/observations"',
        'path: "/reliability/work-orders"',
        'path: "/reliability/work-orders/detail"',
        'path: "/reliability/verifications"',
        'path: "/reliability/ai"',
        'case "reliability_dashboard":',
        'case "reliability_issues":',
        'case "reliability_issue_detail":',
        'case "reliability_observations":',
        'case "reliability_work_orders":',
        'case "reliability_work_order_detail":',
        'case "reliability_verifications":',
        'case "reliability_ai":',
    ]:
        assert route in pages

    for alias in [
        '"/reliability/readiness": "/reliability"',
        '"/reliability/ai-draft": "/reliability/ai"',
    ]:
        assert alias in pages

    for endpoint in [
        '"/reliability/readiness"',
        '"/reliability/ai"',
        '"/reliability/work-orders"',
        '"/reliability/verifications"',
        '"/api/reliability/observations"',
        '"/api/reliability/issues"',
        '"/api/reliability/work-orders"',
        '"/api/reliability/fix-attempts"',
        '"/api/reliability/verifications"',
        '"/api/reliability/next-actions"',
        '"record-fix-attempt"',
        '"create-work-order"',
        '"verify"',
        '"/api/reliability/ai/draft-issue"',
        '"/api/reliability/readiness/assess"',
        '"/api/reliability/readiness/latest"',
        "def do_PATCH",
    ]:
        assert endpoint in server

    for client_call in [
        'query("/api/reliability/observations"',
        'postJson("/api/reliability/observations"',
        'query("/api/reliability/issues"',
        'postJson("/api/reliability/issues"',
        'patchJson(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}`',
        'query("/api/reliability/work-orders"',
        'postJson("/api/reliability/work-orders"',
        'query("/api/reliability/fix-attempts"',
        'postJson("/api/reliability/fix-attempts"',
        'query("/api/reliability/verifications"',
        'postJson("/api/reliability/verifications"',
        'query("/api/reliability/next-actions")',
        'postJson(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}/create-work-order`',
        'postJson(`/api/reliability/work-orders/${encodeURIComponent(String(workOrderId || ""))}/record-fix-attempt`',
        'postJson(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}/verify`',
        'postJson("/api/reliability/ai/draft-issue"',
        'postJson("/api/reliability/readiness/assess"',
        'query("/api/reliability/readiness/latest")',
    ]:
        assert client_call in domain_client

    assert "export async function patchJson" in api_client
    assert "executeReliabilityWorkflow" in main_js
    assert '.reliability-action-form' in main_js


def test_reliability_ui_readability_contract_for_work_orders_and_verifications() -> None:
    pages = pages_source_v1(ROOT)
    main_js = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")

    work_orders_section = pages.split("async function renderReliabilityWorkOrdersPage()")[1].split("async function renderReliabilityWorkOrderDetailPage()")[0]
    work_order_detail_section = pages.split("async function renderReliabilityWorkOrderDetailPage()")[1].split("async function renderReliabilityVerificationsPage()")[0]
    verifications_section = pages.split("async function renderReliabilityVerificationsPage()")[1].split("async function renderReliabilityAiDraftPage(state)")[0]
    observations_section = pages.split("async function renderReliabilityObservationsPage()")[1].split("async function renderReliabilityWorkOrdersPage()")[0]

    for helper in [
        "function shortIssueId",
        "function shortWorkOrderId",
        "function shortVerificationId",
        "function shortObservationId",
        "function shortFixAttemptId",
        "function truncatedCell",
        "function ellipsisText",
        "function workOrderFixLabel",
        "function isSleeveLiveReadinessContext",
        "function isSystemSmokeTestWork",
        "function workOrderNextStepLabel",
        "function nextActionForWorkOrder",
        "function nextActionForVerification",
    ]:
        assert helper in pages

    assert "My Open Reliability Work" in work_orders_section
    assert "What Needs Attention?" in work_orders_section
    assert "Sleeve LIVE Readiness Work" in work_orders_section
    assert "Fix sleeve grading for LIVE readiness" in work_orders_section
    assert "Show system smoke tests" in work_orders_section
    assert "Search issues or work orders..." in work_orders_section
    assert "System smoke test" in work_orders_section
    assert "nonSmokePriority" in work_orders_section
    assert "primaryEntry = nonSmokePriority[0] || prioritizedAll[0] || null" in work_orders_section
    assert "shortWorkOrderId" in work_orders_section
    assert "shortIssueId" in work_orders_section
    assert "<th>Problem</th>" in work_orders_section
    assert "Why it matters" in work_orders_section
    assert "workOrderNextStepLabel" in work_orders_section
    assert "Open Prompt" in work_orders_section
    assert "Verify" in work_orders_section
    assert "Review" in work_orders_section

    for legacy_column in ['label: "issue_id"', 'label: "work_order_id"', 'label: "fix_attempt_id"']:
        assert legacy_column not in verifications_section
    assert "nextActionForVerification" in verifications_section
    assert "shortIssueId" in verifications_section
    assert "shortWorkOrderId" in verifications_section
    assert "data-route" in verifications_section
    assert "What Needs Attention?" in verifications_section

    assert "shortObservationId" in observations_section
    assert "truncatedCell" in observations_section
    assert "Copy Codex Prompt" in work_order_detail_section
    assert "Run with Codex" in work_order_detail_section
    assert "Paste this prompt into Codex. After Codex finishes, return here and record the fix attempt." in work_order_detail_section
    assert "Record Fix Attempt" in work_order_detail_section
    assert "[data-copy-source]" in main_js
    assert "[data-scroll-target]" in main_js


def test_creating_observation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    observation = create_reliability_observation_v1(_observation_payload("observation created"))
    assert observation["id"].startswith("reliability_observation:")
    assert observation["summary"] == "observation created"
    assert observation["structured_payload_hash"]


def test_creating_issue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    created = create_reliability_issue_v1(_issue_payload(canonical_key="issue-create-key"))
    issue = created["issue"]
    assert issue["id"].startswith("reliability_issue:")
    assert issue["status"] == "open"
    assert issue["occurrence_count"] == 1


def test_linking_observation_to_issue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    observation = create_reliability_observation_v1(_observation_payload("link evidence"))
    created = create_reliability_issue_v1(_issue_payload(canonical_key="link-key"))
    issue_id = created["issue"]["id"]
    linked = link_reliability_issue_observation_v1(
        issue_id,
        {"observation_id": observation["id"], "link_type": "primary_evidence"},
    )
    assert linked["ok"] is True
    detail = get_reliability_issue_v1(issue_id)
    linked_ids = [row["observation_id"] for row in detail["linked_observations"]]
    assert observation["id"] in linked_ids


def test_repeated_observation_increments_recurrence_for_matching_canonical_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_db_path(monkeypatch, tmp_path)
    first_observation = create_reliability_observation_v1(_observation_payload("first recurrence"))
    created = create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="recurrence-key"),
            "observation_ids": [first_observation["id"]],
        }
    )
    issue_id = created["issue"]["id"]
    second_observation = create_reliability_observation_v1(_observation_payload("second recurrence"))
    reused = create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="recurrence-key"),
            "observation_ids": [second_observation["id"]],
        }
    )
    assert reused["issue"]["id"] == issue_id
    assert reused["issue"]["canonical_match_reused"] is True
    assert reused["issue"]["occurrence_count"] == 2


def test_readiness_not_ready_with_open_critical_issue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    create_reliability_issue_v1(_issue_payload(canonical_key="critical-key", severity="critical"))
    result = assess_reliability_readiness_v1({})
    assert result["assessment"]["status"] == "not_ready"


def test_readiness_not_ready_with_open_high_execution_issue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    create_reliability_issue_v1(
        _issue_payload(
            canonical_key="high-execution-key",
            severity="high",
            category="execution",
        )
    )
    result = assess_reliability_readiness_v1({})
    assert result["assessment"]["status"] == "not_ready"


def test_readiness_not_ready_with_open_readiness_blocker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="readiness-blocker-key", severity="medium"),
            "type": "readiness_blocker",
            "readiness_blocker": True,
        }
    )
    result = assess_reliability_readiness_v1({})
    assert result["assessment"]["status"] == "not_ready"


def test_readiness_ready_when_only_closed_verified_issues_remain(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    verified_at = (datetime.now(UTC) - timedelta(days=8)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    created = create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="old-verified-blocker", severity="high", category="execution", status="open"),
            "readiness_blocker": True,
            "last_seen_at": verified_at,
            "first_seen_at": verified_at,
        }
    )
    issue_id = created["issue"]["id"]
    work_order = create_reliability_work_order_v1(
        {
            "issue_id": issue_id,
            "objective": "Fix blocker and verify",
            "affected_component": "execution",
            "expected_behavior": "Expected behavior",
            "actual_behavior": "Actual behavior",
            "constraints": ["advisory only"],
            "forbidden_changes": ["no trading logic changes"],
            "likely_files": ["constellation_2/phaseL/ui_api/reliability_ledger_v1.py"],
            "required_tests": ["pytest reliability tests"],
            "verification_criteria": ["passed verification evidence"],
        }
    )
    create_reliability_fix_attempt_v1(
        {
            "work_order_id": work_order["work_order"]["id"],
            "status": "tests_passed",
            "started_at": verified_at,
            "completed_at": verified_at,
            "files_changed": ["constellation_2/phaseL/ui_api/reliability_ledger_v1.py"],
            "tests_run": ["pytest reliability"],
            "test_results": {"status": "passed"},
        }
    )
    create_reliability_issue_verification_v1(
        issue_id,
        {
            "method": "paper_trading_replay",
            "status": "passed",
            "verified_at": verified_at,
            "evidence": {"report": "clean replay"},
            "notes": "Verified in advisory workflow",
        },
    )
    result = assess_reliability_readiness_v1({})
    assert result["assessment"]["status"] == "ready"


def test_fix_submitted_does_not_equal_verified(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="fix-submitted-key"),
            "status": "fix_submitted",
            "resolved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }
    )
    result = assess_reliability_readiness_v1({})
    assert result["assessment"]["status"] == "not_ready"
    failing_rules = [row for row in result["rule_results"] if row["rule_id"] == "R8_FIX_SUBMITTED_REQUIRES_VERIFICATION"]
    assert failing_rules and failing_rules[0]["passed"] is False


def test_verified_issue_can_be_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    created = create_reliability_issue_v1(_issue_payload(canonical_key="verify-then-close-key"))
    issue_id = created["issue"]["id"]

    verified = update_reliability_issue_v1(
        issue_id,
        {"status": "verified", "verification_method": "manual check + test"},
    )
    assert verified["issue"]["status"] == "verified"
    assert verified["issue"]["verified_at"]

    closed = update_reliability_issue_v1(issue_id, {"status": "closed"})
    assert closed["issue"]["status"] == "closed"


def test_ai_draft_endpoint_returns_structured_issue_fields(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    payload = draft_reliability_issue_v1(
        {
            "operator_message": "Paper trading incident caused wrong order correctness path in execution.",
            "environment": "PAPER",
            "component": "execution",
        }
    )
    draft = payload["draft_issue"]
    for key in [
        "title",
        "type",
        "category",
        "severity",
        "canonical_key",
        "expected_behavior",
        "actual_behavior",
        "impact_summary",
        "readiness_blocker",
        "confidence",
        "suggested_codex_task",
        "linked_observation_ids",
    ]:
        assert key in draft


def _work_order_payload(issue_id: str) -> dict[str, object]:
    return {
        "issue_id": issue_id,
        "objective": "Repair reliability failure",
        "affected_component": "execution",
        "expected_behavior": "Expected behavior",
        "actual_behavior": "Actual behavior",
        "constraints": ["advisory only", "no trading logic changes"],
        "forbidden_changes": ["strategy logic", "execution logic", "capital sizing/routing"],
        "likely_files": ["constellation_2/phaseL/ui_api/reliability_ledger_v1.py"],
        "required_tests": ["pytest constellation_2/phaseL/ui/tests/test_reliability_ledger_v1.py"],
        "verification_criteria": ["verification record status=passed"],
        "assigned_agent": "codex",
    }


def test_create_work_order_for_issue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="work-order-create"))["issue"]
    created = create_reliability_work_order_v1(_work_order_payload(issue["id"]))
    assert created["work_order"]["id"].startswith("reliability_work_order:")
    assert created["work_order"]["issue_id"] == issue["id"]
    assert created["issue"]["status"] in {"triaged", "fix_proposed"}


def test_list_work_orders_and_issue_can_have_multiple(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="work-order-multi"))["issue"]
    create_reliability_work_order_v1(_work_order_payload(issue["id"]))
    create_reliability_work_order_v1(
        {
            **_work_order_payload(issue["id"]),
            "objective": "Second attempt objective",
            "status": "queued",
        }
    )
    listed = list_reliability_work_orders_v1({"issue_id": issue["id"]})
    assert listed["total_count"] == 2


def test_issue_scoped_work_order_create_accepts_short_issue_id_and_lists_immediately(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="work-order-short-issue-id"))["issue"]
    short_issue_id = f"ISSUE-{issue['id'].split(':', 1)[1][:6]}"
    created = create_reliability_issue_work_order_v1(short_issue_id, {"objective": "Create from short issue id"})
    assert created["work_order"]["issue_id"] == issue["id"]
    listed = list_reliability_work_orders_v1({"issue_id": short_issue_id})
    listed_ids = [row["id"] for row in listed["work_orders"]]
    assert created["work_order"]["id"] in listed_ids


def test_creating_work_order_does_not_close_issue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="work-order-not-close"))["issue"]
    create_reliability_work_order_v1(_work_order_payload(issue["id"]))
    refreshed = get_reliability_issue_v1(issue["id"])["issue"]
    assert refreshed["status"] != "closed"


def test_create_fix_attempt_for_work_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="fix-attempt-create"))["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    created = create_reliability_fix_attempt_v1(
        {
            "work_order_id": work_order["id"],
            "status": "started",
            "files_changed": ["a.py", "b.py"],
            "tests_run": ["pytest -k reliability"],
            "test_results": {"started": True},
        }
    )
    assert created["fix_attempt"]["id"].startswith("reliability_fix_attempt:")
    assert created["work_order"]["status"] == "in_progress"
    assert created["issue"]["status"] == "fix_in_progress"


def test_tests_passed_does_not_verify_issue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="tests-passed-not-verified"))["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    create_reliability_fix_attempt_v1(
        {
            "work_order_id": work_order["id"],
            "status": "tests_passed",
            "files_changed": ["fix.py"],
            "tests_run": ["pytest"],
            "test_results": {"passed": True},
        }
    )
    refreshed = get_reliability_issue_v1(issue["id"])["issue"]
    assert refreshed["status"] == "verification_pending"
    assert refreshed["status"] != "verified"


def test_tests_failed_keeps_issue_not_ready(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="tests-failed-not-ready", severity="medium"),
            "type": "readiness_blocker",
            "readiness_blocker": True,
        }
    )["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    create_reliability_fix_attempt_v1(
        {
            "work_order_id": work_order["id"],
            "status": "tests_failed",
            "files_changed": ["fix.py"],
            "tests_run": ["pytest"],
            "test_results": {"failed": True},
        }
    )
    result = assess_reliability_readiness_v1({})
    assert result["assessment"]["status"] == "not_ready"


def test_multiple_fix_attempts_are_preserved(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="multi-fix-attempt"))["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    create_reliability_fix_attempt_v1({"work_order_id": work_order["id"], "status": "started"})
    create_reliability_fix_attempt_v1({"work_order_id": work_order["id"], "status": "tests_failed"})
    listed = list_reliability_fix_attempts_v1({"work_order_id": work_order["id"]})
    assert listed["total_count"] == 2


def test_passed_verification_moves_issue_verified(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="verification-pass"))["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    attempt = create_reliability_fix_attempt_v1({"work_order_id": work_order["id"], "status": "tests_passed"})["fix_attempt"]
    created = create_reliability_issue_verification_v1(
        issue["id"],
        {
            "work_order_id": work_order["id"],
            "fix_attempt_id": attempt["id"],
            "method": "manual_review",
            "status": "passed",
            "evidence": {"checklist": "done"},
        },
    )
    assert created["issue"]["status"] == "verified"
    assert created["issue"]["verified_at"]


def test_failed_verification_does_not_close_issue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="verification-failed"))["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    attempt = create_reliability_fix_attempt_v1({"work_order_id": work_order["id"], "status": "tests_passed"})["fix_attempt"]
    created = create_reliability_verification_v1(
        {
            "issue_id": issue["id"],
            "work_order_id": work_order["id"],
            "fix_attempt_id": attempt["id"],
            "method": "automated_test",
            "status": "failed",
            "evidence": {"suite": "regression"},
        }
    )
    assert created["issue"]["status"] in {"open", "fix_in_progress"}
    with pytest.raises(ValueError):
        update_reliability_issue_v1(issue["id"], {"status": "closed"})


def test_closed_allows_duplicate_or_wont_fix_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="duplicate-close"))["issue"]
    duplicate = update_reliability_issue_v1(issue["id"], {"status": "duplicate"})
    closed = update_reliability_issue_v1(issue["id"], {"status": "closed"})
    assert duplicate["issue"]["status"] == "duplicate"
    assert closed["issue"]["status"] == "closed"


def test_verification_can_reference_fix_attempt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="verification-ref-fix"))["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    attempt = create_reliability_fix_attempt_v1({"work_order_id": work_order["id"], "status": "patch_submitted"})["fix_attempt"]
    verification = create_reliability_verification_v1(
        {
            "issue_id": issue["id"],
            "work_order_id": work_order["id"],
            "fix_attempt_id": attempt["id"],
            "method": "manual_review",
            "status": "pending",
            "evidence": {"note": "awaiting replay"},
        }
    )["verification"]
    assert verification["fix_attempt_id"] == attempt["id"]
    fetched = get_reliability_fix_attempt_v1(attempt["id"])
    assert fetched["fix_attempt"]["id"] == attempt["id"]


def test_readiness_blocker_with_work_order_but_no_fix_attempt_is_not_ready(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="blocker-wo-no-fix"),
            "type": "readiness_blocker",
            "readiness_blocker": True,
        }
    )["issue"]
    create_reliability_work_order_v1(_work_order_payload(issue["id"]))
    result = assess_reliability_readiness_v1({})
    assert result["assessment"]["status"] == "not_ready"
    assert any(rule["rule_id"] == "R6_BLOCKER_FIX_ATTEMPT_REQUIRED" and not rule["passed"] for rule in result["rule_results"])


def test_readiness_blocker_with_fix_attempt_but_no_verification_is_not_ready(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="blocker-fix-no-verify"),
            "type": "readiness_blocker",
            "readiness_blocker": True,
        }
    )["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    create_reliability_fix_attempt_v1({"work_order_id": work_order["id"], "status": "tests_passed"})
    result = assess_reliability_readiness_v1({})
    assert result["assessment"]["status"] == "not_ready"
    assert any(rule["rule_id"] == "R7_BLOCKER_VERIFICATION_REQUIRED" and not rule["passed"] for rule in result["rule_results"])


def test_blocker_with_passed_verification_can_be_ready_or_conditionally_ready(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    old_time = (datetime.now(UTC) - timedelta(days=8)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    issue = create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="blocker-verified-ready"),
            "type": "readiness_blocker",
            "readiness_blocker": True,
            "first_seen_at": old_time,
            "last_seen_at": old_time,
        }
    )["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    attempt = create_reliability_fix_attempt_v1(
        {
            "work_order_id": work_order["id"],
            "status": "tests_passed",
            "started_at": old_time,
            "completed_at": old_time,
        }
    )["fix_attempt"]
    create_reliability_issue_verification_v1(
        issue["id"],
        {
            "work_order_id": work_order["id"],
            "fix_attempt_id": attempt["id"],
            "method": "observation_window",
            "status": "passed",
            "verified_at": old_time,
            "evidence": {"window_days": 8},
        },
    )
    result = assess_reliability_readiness_v1({})
    assert result["assessment"]["status"] in {"ready", "conditionally_ready"}


def test_failed_verification_after_latest_fix_keeps_not_ready(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    old_time = (datetime.now(UTC) - timedelta(minutes=30)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    issue = create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="failed-after-fix"),
            "type": "readiness_blocker",
            "readiness_blocker": True,
        }
    )["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    attempt = create_reliability_fix_attempt_v1(
        {
            "work_order_id": work_order["id"],
            "status": "tests_passed",
            "started_at": old_time,
            "completed_at": old_time,
        }
    )["fix_attempt"]
    create_reliability_issue_verification_v1(
        issue["id"],
        {
            "work_order_id": work_order["id"],
            "fix_attempt_id": attempt["id"],
            "method": "manual_review",
            "status": "failed",
            "evidence": {"reason": "regression remains"},
        },
    )
    result = assess_reliability_readiness_v1({})
    assert result["assessment"]["status"] == "not_ready"
    assert any(rule["rule_id"] == "R10_FAILED_VERIFICATION_AFTER_LATEST_FIX" and not rule["passed"] for rule in result["rule_results"])


def test_next_actions_include_open_blocker_without_work_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(
        {
            **_issue_payload(canonical_key="next-action-blocker-no-wo"),
            "type": "readiness_blocker",
            "readiness_blocker": True,
        }
    )["issue"]
    actions = list_reliability_next_actions_v1()["actions"]
    assert any(item["action_type"] == "create_work_order" and item["issue_id"] == issue["id"] for item in actions)


def test_next_actions_include_queued_work_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="next-action-queued"))["issue"]
    work_order = create_reliability_work_order_v1({**_work_order_payload(issue["id"]), "status": "queued"})["work_order"]
    actions = list_reliability_next_actions_v1()["actions"]
    assert any(item["action_type"] == "start_fix_attempt" and item["work_order_id"] == work_order["id"] for item in actions)


def test_next_actions_include_fix_attempt_awaiting_verification(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="next-action-awaiting-verification"))["issue"]
    work_order = create_reliability_work_order_v1(_work_order_payload(issue["id"]))["work_order"]
    attempt = create_reliability_fix_attempt_v1({"work_order_id": work_order["id"], "status": "tests_passed"})["fix_attempt"]
    actions = list_reliability_next_actions_v1()["actions"]
    assert any(item["action_type"] == "record_verification" and item["fix_attempt_id"] == attempt["id"] for item in actions)


def test_next_actions_include_verified_issue_ready_to_close(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="next-action-verified-close"))["issue"]
    create_reliability_issue_verification_v1(
        issue["id"],
        {
            "method": "manual_review",
            "status": "passed",
            "evidence": {"note": "verified"},
        },
    )
    actions = list_reliability_next_actions_v1()["actions"]
    assert any(item["action_type"] == "close_verified_issue" and item["issue_id"] == issue["id"] for item in actions)


def test_create_work_order_from_issue_generates_codex_contract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_db_path(monkeypatch, tmp_path)
    issue = create_reliability_issue_v1(_issue_payload(canonical_key="create-wo-from-issue"))["issue"]
    created = create_reliability_work_order_from_issue_v1(
        issue["id"],
        {"assigned_agent": "codex", "operator_instruction": "Generate safe patch plan."},
    )
    work_order = created["work_order"]
    detail = get_reliability_work_order_v1(work_order["id"])
    assert work_order["ai_generated"] is True
    assert "Forbidden Changes" in detail["codex_ready_prompt"]
    assert detail["work_order"]["issue_id"] == issue["id"]
