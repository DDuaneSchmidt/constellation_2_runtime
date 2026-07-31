from __future__ import annotations

import json
import re
from types import SimpleNamespace
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
        "QUEUE_CERTIFICATION",
        "CERTIFY_SELECTED_SYMBOLS",
        "VIEW_CERTIFICATION_RESULT",
        "UPLOAD_SOURCE",
        "VIEW_SOURCE_SETUP",
        "DOWNLOAD_SOURCE_TEMPLATE",
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



def test_context_readiness_command_registry_and_execution_use_safe_argv(tmp_path: Path) -> None:
    commands = command_registry_v1()["commands_by_id"]
    command = commands["RUN_CONTEXT_READINESS_COMMAND"]
    assert command["label"] == "Repair Context Readiness"
    assert command["action_type"] == API_COMMAND
    assert command["endpoint"] == "/api/aegis/commands/execute"
    assert command["safety_classification"]["portal_action_id"] == "repair_context_readiness"
    assert command["safety_classification"]["trade_advice_allowed"] is False
    assert command["safety_classification"]["broker_submit_transmit_allowed"] is False
    assert command["safety_classification"]["autonomous_execution_allowed"] is False

    captured: dict = {}

    def runner(argv, **kwargs):
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return SimpleNamespace(returncode=0, stdout="context repaired", stderr="")

    result = execute_aegis_command_v1(
        {
            "command_id": "RUN_CONTEXT_READINESS_COMMAND",
            "target_type": "runtime_context",
            "target_id": "repair_context_readiness",
            "payload": {"action_id": "repair_context_readiness"},
        },
        truth_root=tmp_path,
        day_utc="2026-05-22",
        command_runner=runner,
    )

    assert result["ok"] is True
    assert result["portal_action_id"] == "repair_context_readiness"
    assert result["command_argv"] == ["npm", "run", "aegis:repair-context-readiness"]
    assert captured["argv"] == result["command_argv"]
    assert captured["kwargs"]["shell"] is False
    assert result["safety_policy_summary"]["trade_advice_allowed"] is False
    assert result["safety_policy_summary"]["broker_execution_allowed"] is False
    assert result["safety_policy_summary"]["autonomous_execution_allowed"] is False


def test_context_readiness_command_rejects_unknown_action(tmp_path: Path) -> None:
    result = execute_aegis_command_v1(
        {
            "command_id": "RUN_CONTEXT_READINESS_COMMAND",
            "target_type": "runtime_context",
            "target_id": "unknown_action",
            "payload": {"action_id": "unknown_action"},
        },
        truth_root=tmp_path,
        day_utc="2026-05-22",
        command_runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("runner must not be called")),
    )

    assert result["ok"] is False
    assert result["result_status"] == "REJECTED"
    assert result["trade_advice_allowed"] is False
    assert result["broker_execution_allowed"] is False
    assert result["autonomous_execution_allowed"] is False

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


def test_add_manual_receipt_command_writes_receipt_and_refreshes_projections(tmp_path: Path) -> None:
    day = "2026-05-22"
    mark_path = tmp_path / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json"
    mark_path.parent.mkdir(parents=True, exist_ok=True)
    mark_path.write_text(json.dumps({"schema_id": "final_eod_market_data", "schema_version": "v1", "day_utc": day, "status": "CURRENT", "validation_status": "VALID", "normalized_records": [{"symbol": "DOW", "close": 36.01, "market_session_date": day}], "content_hash": "mark"}), encoding="utf-8")
    ticket_path = tmp_path / "reports" / "captured_ticket_history_v1" / day / "ticket_dow" / "captured_ticket_history.v1.json"
    ticket_path.parent.mkdir(parents=True, exist_ok=True)
    ticket_path.write_text(json.dumps({"schema_id": "captured_ticket_history", "schema_version": "v1", "day_utc": day, "ticket_id": "ticket:dow", "symbol": "DOW", "side": "BUY", "quantity": 166, "fill_price": "36.06", "sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "captured_at_utc": f"{day}T17:16:00Z", "history_hash": "dow-history"}), encoding="utf-8")

    result = execute_aegis_command_v1(
        {
            "command_id": "ADD_MANUAL_RECEIPT",
            "target_type": "paper_trade",
            "target_id": "ticket:dow",
            "payload": {"operator_attestation": True, "account_alias": "paper/manual"},
        },
        truth_root=tmp_path,
        day_utc=day,
    )

    assert result["ok"] is True
    assert result["result_status"] == "RECEIPT_RECORDED"
    assert Path(result["receipt_path"]).exists()
    assert Path(result["manual_execution_receipt_path"]).exists()
    assert result["refreshed_trade"]["evidence_status"] == "MISSING_EXIT"
    assert result["refreshed_exit_review"]["exit_decision"] in {"HOLD", "UPDATE_STOP", "TAKE_PARTIAL", "EXIT_FULL", "BLOCK"}
    assert result["broker_execution_allowed"] is False
    assert result["autonomous_execution_allowed"] is False
    assert result["trade_advice_allowed"] is False
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
    terminal = {"COMPLETED", "SOURCE_CONFIGURED", "NOT_APPLICABLE_FOR_CURRENT_MODE", "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT"}
    assert projection["summary"]["total_open"] == len([row for row in projection["repair_items"] if row["status"] not in terminal])


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


def test_dynamic_certification_queue_command_writes_reviewable_artifact(tmp_path: Path) -> None:
    day = "2026-05-21"
    audit_path = tmp_path / "reports" / "candidate_consumption_audit_v1" / day / "candidate_consumption_audit.v1.json"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps(
            {
                "schema_id": "candidate_consumption_audit",
                "trading_date": day,
                "raw_candidate_count": 1,
                "promoted_candidate_count": 0,
                "excluded_candidate_count": 1,
                "consumption_counts": {"EXCLUDED_UNCOVERED_SYMBOL": 1},
                "candidate_rows": [
                    {
                        "candidate_id": "candidate-iwm",
                        "raw_intent_id": "intent-iwm",
                        "symbol": "IWM",
                        "sleeve_id": "TREND",
                        "score": 0.92,
                        "near_promotion": True,
                        "source_status": "CANDIDATE_CREATED",
                        "consumption_category": "EXCLUDED_UNCOVERED_SYMBOL",
                        "consumption_reason": "outside certified universe",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = execute_aegis_command_v1(
        {
            "command_id": "QUEUE_CERTIFICATION",
            "target_type": "candidate_funnel",
            "target_id": "DYNAMIC_CERTIFICATION_QUEUE",
            "payload": {"day_utc": day},
        },
        truth_root=tmp_path,
        day_utc=day,
    )

    assert result["ok"] is True
    assert result["result_status"] == "PENDING"
    assert result["requested_symbols"] == ["IWM"]
    assert result["queue_path"]
    assert Path(result["queue_path"]).exists()
    assert result["broker_execution_allowed"] is False

    view = execute_aegis_command_v1(
        {
            "command_id": "VIEW_CERTIFICATION_RESULT",
            "target_type": "candidate_funnel",
            "target_id": "DYNAMIC_CERTIFICATION_QUEUE",
            "payload": {"day_utc": day},
        },
        truth_root=tmp_path,
        day_utc=day,
    )
    assert view["ok"] is True
    assert view["requested_symbols"] == ["IWM"]


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


def test_external_source_template_command_writes_guided_json_template(tmp_path: Path) -> None:
    day = "2026-05-22"
    result = execute_aegis_command_v1(
        {
            "command_id": "DOWNLOAD_SOURCE_TEMPLATE",
            "target_type": "domain_source",
            "target_id": "MACRO_CALENDAR",
            "payload": {"domain_id": "MACRO_CALENDAR"},
        },
        truth_root=tmp_path,
        day_utc=day,
    )

    assert result["ok"] is True
    assert result["result_status"] == "TEMPLATE_READY"
    panel = result["command_result"]
    assert panel["status_label"] == "JSON template ready"
    assert "source_template.v1.json" in panel["expected_source_path"]
    template_path = Path(panel["expected_source_path"])
    assert template_path.exists()
    template = json.loads(template_path.read_text(encoding="utf-8"))
    assert template["template_only"] is True
    assert template["broker_submit_transmit_allowed"] is False
    assert template["autonomous_execution_allowed"] is False
    assert "event_name" in template["events"][0]


def test_invalid_external_source_validation_shows_precise_field_errors(tmp_path: Path, monkeypatch) -> None:
    day = "2026-05-22"
    source = tmp_path / "incoming" / "macro.json"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(json.dumps({"events": [{"event_name": "FOMC", "country": "US", "importance": "HIGH", "source": "TEST"}]}), encoding="utf-8")
    monkeypatch.setenv("AEGIS_MACRO_CALENDAR_SOURCE_FILE", str(source))

    result = execute_aegis_command_v1(
        {
            "command_id": "VALIDATE_SOURCE",
            "target_type": "domain_source",
            "target_id": "MACRO_CALENDAR",
            "payload": {"domain_id": "MACRO_CALENDAR"},
        },
        truth_root=tmp_path,
        day_utc=day,
    )

    assert result["ok"] is False
    assert result["result_status"] == "INVALID_SOURCE"
    assert "row_0_missing:time" in result["command_result"]["plain_english_result"]
    assert "row_0_missing:time" in result["command_result"]["failure_reason"]


def test_valid_external_source_validation_recetifies_domain(tmp_path: Path, monkeypatch) -> None:
    day = "2026-05-22"
    source = tmp_path / "incoming" / "macro.json"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(json.dumps({"events": [{"event_name": "FOMC Minutes", "time": f"{day}T18:00:00Z", "country": "US", "importance": "HIGH", "source": "TEST_MACRO_FEED"}]}), encoding="utf-8")
    monkeypatch.setenv("AEGIS_MACRO_CALENDAR_SOURCE_FILE", str(source))

    result = execute_aegis_command_v1(
        {
            "command_id": "VALIDATE_SOURCE",
            "target_type": "domain_source",
            "target_id": "MACRO_CALENDAR",
            "payload": {"domain_id": "MACRO_CALENDAR"},
        },
        truth_root=tmp_path,
        day_utc=day,
    )
    report = build_domain_certification_report_v1(truth_root=tmp_path, day_utc=day)
    domain_status = next(row for row in report["domains"] if row["domain_id"] == "MACRO_CALENDAR")["certification_status"]

    assert result["ok"] is True
    assert result["result_status"] == "VALIDATED"
    assert domain_status == "CERTIFIED"


def test_repair_center_projection_exposes_guided_external_source_setup(tmp_path: Path) -> None:
    day = "2026-05-22"
    projection = build_repair_center_projection_v1(truth_root=tmp_path, day_utc=day)
    expected = {
        "MACRO_CALENDAR": ("AEGIS_MACRO_CALENDAR_SOURCE_FILE", ["event_name", "time", "country", "importance", "source"]),
        "EARNINGS_EVENTS": ("AEGIS_EARNINGS_EVENTS_SOURCE_FILE", ["symbol", "company", "report_date", "report_time", "confirmed_or_estimated", "source"]),
        "CORPORATE_ACTIONS": ("AEGIS_CORPORATE_ACTIONS_SOURCE_FILE", ["symbol", "action_type", "effective_date", "source"]),
    }

    for domain_id, (config_key, fields) in expected.items():
        item = next(row for row in projection["repair_items"] if row["domain_id"] == domain_id)
        workflow = item["source_setup_workflow"]
        labels = [command["label"] for command in item["secondary_commands"]]
        assert item["primary_command"]["command_id"] == "VIEW_SOURCE_SETUP"
        assert workflow["title"] == "External source required — not a system failure"
        assert workflow["required_config_key"] == config_key
        assert workflow["required_json_fields"] == fields
        assert workflow["required_file_path"].endswith(".v1.json")
        assert "Example" not in workflow["why_aegis_needs_it"]
        assert "Download template" in labels
        assert "Configure source path" in labels
        assert "Validate Source" in labels
        assert "Re-run certification" in labels


def test_repair_center_ui_renders_guided_source_setup_controls() -> None:
    pages = pages_source_v1(ROOT)
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")

    assert "renderSourceSetupWorkflow" in pages
    assert "External source required — not a system failure" in pages
    assert "Example valid JSON template" in pages
    assert "VIEW_SOURCE_SETUP" in pages
    assert "DOWNLOAD_SOURCE_TEMPLATE" in COMMAND_MODULE.read_text(encoding="utf-8")
    assert "Configure source path" in (ROOT / "ops/aegis/repair_center_projection_v1.py").read_text(encoding="utf-8")
    assert ".repair-source-setup-panel" in css


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
    assert "View Setup" in pages
    assert "repairSeverity" in pages
    assert "repair-source-setup-actions" in pages
    assert ".repair-center-card" in css
    assert ".repair-compact-facts" in css
    assert ".runtime-repair-summary" in css
    assert ".command-result-panel" in css
    assert "grid-template-columns: repeat(3, minmax(260px, 1fr));" in css
    assert "overflow-wrap: anywhere" in css
    assert "runtime-domain-rechecking" not in css



def test_repair_center_layout_uses_full_operational_width() -> None:
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")

    assert ".dashboard-main-only .page-content > .repair-center-workspace" in css
    assert ".operational-layout .page-content > .repair-center-workspace" in css
    span_start = css.index(".dashboard-main-only .page-content > .repair-center-workspace")
    span_end = css.index("}\n\n.repair-center-workspace", span_start)
    assert "grid-column: 1 / -1;" in css[span_start:span_end]
    workspace_block = css[css.index(".repair-center-workspace {"):css.index(".repair-center-hero,")]
    assert "width: 100%;" in workspace_block
    assert "max-width: none;" in workspace_block

    grid_block = css[css.index(".repair-center-grid {"):css.index(".repair-center-card {")]
    assert "grid-template-columns: repeat(3, minmax(260px, 1fr));" in grid_block
    assert "width: 100%;" in grid_block
    assert "@media (max-width: 1380px)" in css
    assert "grid-template-columns: repeat(2, minmax(260px, 1fr));" in css
    assert "@media (max-width: 900px)" in css
    assert "grid-template-columns: minmax(0, 1fr);" in css[css.index("@media (max-width: 900px)"):]
    assert "overflow-wrap: anywhere;" in css[css.index(".repair-source-path code"):css.index(".repair-progress {")]


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
    assert first["repair_status"] == "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT"
    assert first["latest_result_status"] == "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT"


def test_repair_center_source_missing_is_exact_external_requirement(tmp_path: Path) -> None:
    day = "2026-05-22"
    projection = build_repair_center_projection_v1(truth_root=tmp_path, day_utc=day)
    macro = next(item for item in projection["repair_items"] if item["domain_id"] == "MACRO_CALENDAR")

    assert macro["status"] == "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT"
    assert macro["result"]["result_status"] == "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT"
    assert macro["source_config_key"] == "AEGIS_MACRO_CALENDAR_SOURCE_FILE"
    assert "macro_calendar.v1.json" in macro["required_source"]["path"]
    assert "Required fields" in macro["next_step"]
    assert projection["summary"]["blocked_external_requirements"] >= 1


def test_repair_center_summary_banner_and_external_requirement_language(tmp_path: Path) -> None:
    day = "2026-05-22"
    projection = build_repair_center_projection_v1(truth_root=tmp_path, day_utc=day)
    banner = projection["summary_banner"]
    macro = next(item for item in projection["repair_items"] if item["domain_id"] == "MACRO_CALENDAR")

    assert banner["title"] == "Core system operational"
    assert "external source requirements remain" in banner["message"]
    assert banner["external_source_requirements_remaining"] >= 1
    assert macro["not_system_failure_explanation"] == "Requires external source; not a system failure."
    assert macro["source_options"]["existing_internal_builder"] is True
    assert macro["source_options"]["can_generate_from_existing_data"] is False
    assert "would fabricate coverage" in macro["source_options"]["generation_assessment"]


def test_repair_center_ui_renders_operational_summary_banner() -> None:
    pages = pages_source_v1(ROOT)
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")

    assert "data-repair-center-summary-banner" in pages
    assert "Core system operational" in pages
    assert "External requirements" in pages
    assert "Affected sleeves" in pages
    assert "repairShortStatus" in pages
    assert ".repair-center-status-banner" in css




def test_repair_center_uses_tiered_disclosure_for_source_setup() -> None:
    pages = pages_source_v1(ROOT)
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")

    assert "External source required — not a system failure" in pages
    assert "View Setup" in pages
    assert "Required file path" in pages
    assert "Required JSON fields" in pages
    assert "Example valid JSON template" in pages
    assert "View evidence / provenance" in pages
    assert "repair-section-summary" in pages
    assert "repair-compact-facts" in pages
    card_start = pages.index("function renderRepairCard")
    card_end = pages.index("function renderRepairSection", card_start)
    card_block = pages[card_start:card_end]
    assert "required_artifact_source" not in card_block
    assert "required_source?.path" not in card_block
    assert "repair-source-path" not in card_block
    assert "renderRuntimeCommandResultPanel" not in card_block
    assert ".repair-section-summary" in css
    assert ".repair-source-setup-actions" in css

def test_repair_center_latest_api_route_is_registered() -> None:
    server = SERVER.read_text(encoding="utf-8")

    assert '"/api/aegis/repair-center/latest"' in server
    assert '"/api/aegis/repair-center"' in server
