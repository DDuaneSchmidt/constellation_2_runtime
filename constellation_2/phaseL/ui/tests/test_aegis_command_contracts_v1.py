from __future__ import annotations

import json
import re
from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from ops.aegis.domain_certification_v1 import build_domain_certification_report_v1
from ops.aegis.repair_center_projection_v1 import build_repair_center_projection_v1
from ops.aegis.operator_action_command_contracts_v1 import (
    API_COMMAND,
    command_audit_path_v1,
    command_registry_v1,
    execute_aegis_command_v1,
    validate_command_registry_v1,
)

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
CLIENT = ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
COMMAND_MODULE = ROOT / "ops/aegis/operator_action_command_contracts_v1.py"


def test_command_registry_has_required_contract_fields() -> None:
    result = validate_command_registry_v1()
    assert result["ok"], result["errors"]
    commands = result["registry"]["commands_by_id"]
    for command_id in [
        "START_RESEARCH",
        "VIEW_FINDINGS",
        "VIEW_WAITING_REASON",
        "VIEW_BLOCKER",
        "REPAIR_DOMAIN",
        "VALIDATE_SOURCE",
        "UPLOAD_SOURCE",
        "RECHECK_DOMAIN",
        "VIEW_REPAIR_JOB",
        "VIEW_QUEUE",
        "VIEW_RECOMMENDATION",
        "MARK_CAPTURE_COMPLETE",
    ]:
        assert command_id in commands
        command = commands[command_id]
        assert command["browser_acceptance_test_required"] is True
        assert command["safety_classification"]["broker_submit_transmit_allowed"] is False
        assert command["safety_classification"]["autonomous_execution_allowed"] is False
        assert command["safety_classification"]["trade_advice_allowed"] is False
        if command["action_type"] == API_COMMAND:
            assert command["endpoint"] == "/api/aegis/commands/execute"


def test_rendered_command_ids_exist_in_registry() -> None:
    source = "\n".join(path.read_text(encoding="utf-8") for path in [PAGES, MAIN])
    rendered_ids = set(re.findall(r'data-aegis-command-id=\\?"([^"$\\]+)\\?"', source))
    rendered_ids.update(re.findall(r'command_id["\']?\s*[:=]\s*["\']([A-Z_]+)["\']', source))
    rendered_ids = {item for item in rendered_ids if item and not item.startswith("${")}
    registry_ids = set(command_registry_v1()["commands_by_id"])
    assert rendered_ids
    assert rendered_ids <= registry_ids


def test_backend_command_router_contract_is_registered() -> None:
    server = SERVER.read_text(encoding="utf-8")
    client = CLIENT.read_text(encoding="utf-8")
    main = MAIN.read_text(encoding="utf-8")

    assert '"/api/aegis/commands/execute"' in server
    assert '"/api/aegis/commands/registry"' in server
    assert "execute_aegis_command_v1" in server
    assert 'postJson("/api/aegis/commands/execute"' in client
    assert "runAegisCommandElement" in main
    assert "data-aegis-command-id" in main


def test_repair_domain_source_setup_uses_real_command_response_and_audit(tmp_path: Path) -> None:
    day = "2026-05-21"
    result = execute_aegis_command_v1(
        {
            "command_id": "REPAIR_DOMAIN",
            "target_type": "domain_certification",
            "target_id": "MACRO_CALENDAR",
            "payload": {"domain_id": "MACRO_CALENDAR"},
        },
        truth_root=tmp_path,
        day_utc=day,
    )

    assert result["ok"] is True
    assert result["result_status"] == "SOURCE_SETUP_REQUIRED"
    assert "AEGIS_MACRO_CALENDAR_SOURCE_FILE" in result["user_message"] or "Source setup required" in result["user_message"]
    assert result["audit_id"]
    panel = result["command_result"]
    assert panel["status_label"] == "Source setup required"
    assert panel["missing_source_name"] == "macro_calendar_v1 source artifact or configured API"
    assert "macro_calendar.v1.json" in panel["expected_source_path"]
    assert panel["copy_required_path"] == panel["expected_source_path"]
    assert panel["audit_id"] == result["audit_id"]
    assert command_audit_path_v1(truth_root=tmp_path, day_utc=day).exists()
    audit_text = command_audit_path_v1(truth_root=tmp_path, day_utc=day).read_text(encoding="utf-8")
    assert "REPAIR_DOMAIN" in audit_text
    assert "command_result" in audit_text
    assert "broker_execution_allowed" in audit_text


def test_repair_domain_eod_queues_real_repair_job_when_runner_available(tmp_path: Path) -> None:
    day = "2026-05-21"

    def runner(request: dict) -> dict:
        return {"job_id": f"repair-job:{request['domain_id']}", "status": "QUEUED"}

    result = execute_aegis_command_v1(
        {
            "command_id": "REPAIR_DOMAIN",
            "target_type": "domain_certification",
            "target_id": "US_EQUITIES_EOD",
            "payload": {"domain_id": "US_EQUITIES_EOD"},
        },
        truth_root=tmp_path,
        day_utc=day,
        repair_job_runner=runner,
    )

    assert result["ok"] is True
    assert result["result_status"] == "QUEUED"
    assert result["job_id"] == "repair-job:US_EQUITIES_EOD"
    assert result["command_result"]["status_label"] == "Repair job queued"
    assert result["command_result"]["job_id"] == "repair-job:US_EQUITIES_EOD"
    assert "Repair job queued" in result["user_message"]


def test_repair_domain_failure_returns_inline_command_result(tmp_path: Path) -> None:
    day = "2026-05-21"
    result = execute_aegis_command_v1(
        {
            "command_id": "REPAIR_DOMAIN",
            "target_type": "domain_certification",
            "target_id": "US_EQUITIES_EOD",
            "payload": {"domain_id": "US_EQUITIES_EOD"},
        },
        truth_root=tmp_path,
        day_utc=day,
    )

    assert result["ok"] is False
    assert result["result_status"] == "REPAIR_JOB_UNAVAILABLE"
    assert result["command_result"]["status_label"] == "Failed"
    assert "Automatic repair" in result["command_result"]["failure_reason"]
    assert result["command_result"]["audit_id"] == result["audit_id"]


def test_latest_repair_command_result_persists_in_domain_projection(tmp_path: Path) -> None:
    day = "2026-05-21"
    result = execute_aegis_command_v1(
        {
            "command_id": "REPAIR_DOMAIN",
            "target_type": "domain_certification",
            "target_id": "MACRO_CALENDAR",
            "payload": {"domain_id": "MACRO_CALENDAR"},
        },
        truth_root=tmp_path,
        day_utc=day,
    )

    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    macro_plan = next(plan for plan in report["domain_repair_actions"] if plan["domain_id"] == "MACRO_CALENDAR")
    latest = macro_plan["latest_command_result"]
    assert latest["status_label"] == "Source setup required"
    assert latest["audit_id"] == result["audit_id"]
    assert "macro_calendar.v1.json" in latest["expected_source_path"]


def test_ok_command_responses_include_visible_command_result(tmp_path: Path) -> None:
    result = execute_aegis_command_v1(
        {
            "command_id": "VIEW_FINDINGS",
            "target_type": "hypothesis",
            "target_id": "rh-visible-result-test",
            "payload": {},
        },
        truth_root=tmp_path,
        day_utc="2026-05-21",
    )

    assert result["ok"] is True
    assert result["command_result"]["plain_english_result"]
    assert result["command_result"]["next_required_step"]
    assert result["command_result"]["audit_id"] == result["audit_id"]


def test_start_research_command_creates_user_initiated_audit_and_run(tmp_path: Path) -> None:
    day = "2026-05-21"
    store = tmp_path / "research_store"
    result = execute_aegis_command_v1(
        {
            "command_id": "START_RESEARCH",
            "target_type": "hypothesis",
            "target_id": "rh-command-contract-test",
            "payload": {"hypothesis_id": "rh-command-contract-test", "title": "Command contract test"},
        },
        truth_root=tmp_path,
        day_utc=day,
        research_store_root=store,
    )

    assert result["ok"] is True
    assert result["result_status"] == "QUEUED"
    assert result["research_run_id"]
    assert result["research_run_trigger_source"] == "USER_INITIATED"
    assert command_audit_path_v1(truth_root=tmp_path, day_utc=day).exists()


def test_fake_rechecking_and_legacy_domain_repair_affordance_removed() -> None:
    pages = pages_source_v1(ROOT)
    main = MAIN.read_text(encoding="utf-8")
    assert "data-domain-repair-action" not in pages
    assert "data-domain-repair-action" not in main
    assert "Rechecking domain…" not in pages
    assert "Rechecking domain…" not in main
    assert "Running command…" in main
    assert "showCommandResultPanel" in main
    assert "command-result-panel" in main
    assert "SOURCE_SETUP_REQUIRED" in COMMAND_MODULE.read_text(encoding="utf-8")


def test_domain_repair_plans_carry_command_ids(tmp_path: Path) -> None:
    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc="2026-05-21")
    plans = report["domain_repair_actions"]
    assert plans
    assert all(plan["command_id"] == "REPAIR_DOMAIN" for plan in plans)
    assert all(plan["target_type"] == "domain_certification" for plan in plans)


def test_repair_center_projection_maps_repairs_to_lifecycle_items(tmp_path: Path) -> None:
    projection = build_repair_center_projection_v1(truth_root=tmp_path, day_utc="2026-05-21")
    assert projection["schema_id"] == "aegis_repair_center_projection"
    assert projection["repair_items"]
    item = projection["repair_items"][0]
    for key in [
        "repair_id",
        "domain_id",
        "issue_type",
        "reason",
        "affected_sleeves",
        "required_artifact_source",
        "repair_mode",
        "status",
        "next_step",
        "primary_command",
    ]:
        assert key in item
    section_ids = {section["section_id"] for section in projection["sections"]}
    assert {"automatic_repair_available", "source_setup_required", "repair_running", "repair_failed", "repair_completed"} <= section_ids
    assert projection["summary"]["total_open"] == len([row for row in projection["repair_items"] if row["status"] != "COMPLETED"])


def test_repair_center_surfaces_eod_normal_noop_candidate_consumption(tmp_path: Path) -> None:
    day = "2026-05-22"
    run = "noop"
    report_dir = tmp_path / "reports" / "aegis_lite_eod_report_v1" / day / run
    audit_dir = tmp_path / "reports" / "candidate_consumption_audit_v1" / day / run
    audit_path = audit_dir / "candidate_consumption_audit.v1.json"
    report_path = report_dir / "aegis_lite_eod_report.v1.json"
    audit_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps(
            {
                "schema_id": "candidate_consumption_audit",
                "raw_candidate_count": 3,
                "promoted_candidate_count": 0,
                "excluded_candidate_count": 3,
                "consumption_counts": {"EXCLUDED_LOW_SCORE": 1, "EXCLUDED_UNCOVERED_SYMBOL": 2},
            }
        ),
        encoding="utf-8",
    )
    report_path.write_text(
        json.dumps(
            {
                "schema_id": "aegis_lite_eod_report",
                "generated_at_utc": "2026-05-22T22:00:00Z",
                "eod_outcome_status": "NORMAL_NO_OP_NO_PROMOTED_CANDIDATES",
                "candidate_consumption_audit_artifact_path": str(audit_path),
            }
        ),
        encoding="utf-8",
    )

    projection = build_repair_center_projection_v1(truth_root=tmp_path, day_utc=day)
    item = next(row for row in projection["repair_items"] if row["domain_id"] == "AEGIS_LITE_EOD_REPORT")

    assert item["status"] == "COMPLETED"
    assert item["operator_action_required"] is False
    assert item["issue_type"] == "NORMAL_NO_OP_NO_PROMOTED_CANDIDATES"
    assert "Raw candidates: 3" in item["impact"]
    assert "EXCLUDED_UNCOVERED_SYMBOL: 2" in item["result"]["plain_english_result"]


def test_source_setup_commands_return_concrete_guidance(tmp_path: Path) -> None:
    result = execute_aegis_command_v1(
        {
            "command_id": "UPLOAD_SOURCE",
            "target_type": "domain_source",
            "target_id": "MACRO_CALENDAR",
            "payload": {"domain_id": "MACRO_CALENDAR"},
        },
        truth_root=tmp_path,
        day_utc="2026-05-21",
    )
    assert result["ok"] is True
    assert result["result_status"] == "SOURCE_SETUP_REQUIRED"
    assert "macro_calendar.v1.json" in result["command_result"]["expected_source_path"]
    assert result["command_result"]["copy_required_path"]

    validate = execute_aegis_command_v1(
        {
            "command_id": "VALIDATE_SOURCE",
            "target_type": "domain_source",
            "target_id": "MACRO_CALENDAR",
            "payload": {"domain_id": "MACRO_CALENDAR"},
        },
        truth_root=tmp_path,
        day_utc="2026-05-21",
    )
    assert validate["ok"] is False
    assert validate["result_status"] in {"SOURCE_MISSING", "SOURCE_SETUP_REQUIRED"}
    assert "missing" in validate["command_result"]["plain_english_result"].lower()


def test_recheck_domain_command_persists_domain_certification_report(tmp_path: Path) -> None:
    result = execute_aegis_command_v1(
        {
            "command_id": "RECHECK_DOMAIN",
            "target_type": "domain_certification",
            "target_id": "MACRO_CALENDAR",
            "payload": {"domain_id": "MACRO_CALENDAR"},
        },
        truth_root=tmp_path,
        day_utc="2026-05-21",
    )
    assert result["result_status"] in {"RECERTIFICATION_STILL_DELAYED", "COMPLETED"}
    assert result["audit_id"]
    report_path = tmp_path / "reports" / "domain_certification_v1" / "2026-05-21" / "domain_certification.v1.json"
    assert report_path.exists()


def test_runtime_domain_repair_renders_localized_persistent_result_panel() -> None:
    pages = pages_source_v1(ROOT)
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")
    assert "renderRuntimeCommandResultPanel" in pages
    assert "renderRuntimeRepairCenterSummary" in pages
    assert "renderRepairCenterWorkspace" in pages
    assert "data-repair-center-workspace" in pages
    assert "data-runtime-repair-summary" in pages
    assert "Open Repair Center" in pages
    assert "VALIDATE_SOURCE" in COMMAND_MODULE.read_text(encoding="utf-8")
    assert "UPLOAD_SOURCE" in COMMAND_MODULE.read_text(encoding="utf-8")
    assert "RECHECK_DOMAIN" in COMMAND_MODULE.read_text(encoding="utf-8")
    assert "VIEW_REPAIR_JOB" in COMMAND_MODULE.read_text(encoding="utf-8")
    assert "rowRepairPlan.latest_command_result" not in pages
    assert "${renderRuntimeCommandResultPanel(rowRepairPlan.latest_command_result)}" not in pages
    assert "Copy required path" in pages
    assert ".repair-center-card" in css
    assert ".runtime-repair-summary" in css
    assert ".command-result-panel" in css
    assert "grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));" in css
    assert "overflow-wrap: anywhere" in css
    assert "runtime-domain-rechecking" not in css



def test_repair_domain_failed_job_returns_failed_result_panel(tmp_path: Path) -> None:
    day = "2026-05-21"

    def runner(request: dict) -> dict:
        return {"job_id": f"repair-job:{request['domain_id']}", "status": "FAILED", "failure_reason": "FINAL_EOD_VALIDATION_FAILED"}

    result = execute_aegis_command_v1(
        {
            "command_id": "REPAIR_DOMAIN",
            "target_type": "domain_certification",
            "target_id": "US_EQUITIES_EOD",
            "payload": {"domain_id": "US_EQUITIES_EOD"},
        },
        truth_root=tmp_path,
        day_utc=day,
        repair_job_runner=runner,
    )

    assert result["ok"] is False
    assert result["result_status"] == "FAILED"
    assert result["command_result"]["status_label"] == "Failed"
    assert result["command_result"]["failure_reason"] == "FINAL_EOD_VALIDATION_FAILED"


def test_command_audit_artifacts_and_repair_projection_replay_converge(tmp_path: Path) -> None:
    day = "2026-05-21"
    result = execute_aegis_command_v1(
        {
            "command_id": "REPAIR_DOMAIN",
            "target_type": "domain_certification",
            "target_id": "MACRO_CALENDAR",
            "payload": {"domain_id": "MACRO_CALENDAR"},
        },
        truth_root=tmp_path,
        day_utc=day,
    )

    def stable_projection() -> dict:
        report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
        projection = build_repair_center_projection_v1(truth_root=tmp_path, day_utc=day)
        macro_report = next(plan for plan in report["domain_repair_actions"] if plan["domain_id"] == "MACRO_CALENDAR")
        macro_item = next(item for item in projection["repair_items"] if item["domain_id"] == "MACRO_CALENDAR")
        return {
            "domain_status": next(domain for domain in report["domains"] if domain["domain_id"] == "MACRO_CALENDAR")["certification_status"],
            "repair_status": macro_item["status"],
            "repair_mode": macro_item["repair_mode"],
            "latest_result_status": macro_item["result"]["result_status"],
            "latest_audit_id": macro_item["audit_id"],
            "report_latest_audit_id": macro_report["latest_command_result"]["audit_id"],
            "expected_source_path": macro_item["required_source"]["path"],
            "primary_command": macro_item["primary_command"]["command_id"],
        }

    first = stable_projection()
    second = stable_projection()

    assert first == second
    assert first["latest_audit_id"] == result["audit_id"]
    assert first["report_latest_audit_id"] == result["audit_id"]
    assert first["latest_result_status"] == "SOURCE_SETUP_REQUIRED"
