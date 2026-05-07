from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = REPO_ROOT / "ops/tools/explain_aegis_no_paper_trade_v1.py"
SPEC = importlib.util.spec_from_file_location("explain_aegis_no_paper_trade_v1", TOOL_PATH)
assert SPEC is not None
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

KERNEL_TOOL_PATH = REPO_ROOT / "ops/tools/run_aegis_paper_ready_kernel_v1.py"
KERNEL_SPEC = importlib.util.spec_from_file_location("run_aegis_paper_ready_kernel_v1", KERNEL_TOOL_PATH)
assert KERNEL_SPEC is not None
KERNEL_MODULE = importlib.util.module_from_spec(KERNEL_SPEC)
assert KERNEL_SPEC.loader is not None
sys.modules[KERNEL_SPEC.name] = KERNEL_MODULE
KERNEL_SPEC.loader.exec_module(KERNEL_MODULE)

build_no_trade_explanation_v1 = MODULE.build_no_trade_explanation_v1
render_explanation_v1 = MODULE.render_explanation_v1
kernel_exit_code_for_report = KERNEL_MODULE._process_exit_code_for_report


DAY = "2026-05-05"
RELEASE_ID = "20260505T200256Z__54eca43a7aa0"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _base_runtime(tmp_path: Path) -> tuple[Path, Path]:
    runtime = tmp_path / "runtime"
    release = tmp_path / "release" / RELEASE_ID
    active = tmp_path / "active"
    (release / "ops/tools").mkdir(parents=True)
    (release / "ops/tools/run_aegis_paper_ready_kernel_v1.py").write_text("# tool\n", encoding="utf-8")
    _write(release / "release_manifest.v1.json", {"release_id": RELEASE_ID, "git_sha": "54eca43a7aa0fc64c974d89c02f47046392c83d2"})
    manifest_hash = hashlib.sha256((release / "release_manifest.v1.json").read_bytes()).hexdigest()
    _write(
        runtime / "truth/releases/current_release.v1.json",
        {
            "release_id": RELEASE_ID,
            "release_path": str(release),
            "commit": "54eca43a7aa0fc64c974d89c02f47046392c83d2",
            "release_manifest_hash": manifest_hash,
            "activated_at_utc": "2026-05-05T20:05:58Z",
        },
    )
    active.symlink_to(release)
    return runtime, active


def _journal_success() -> list[str]:
    return [
        "2026-05-05T13:30:00-04:00 node systemd[1]: Starting aegis-paper-ready-kernel-v1.service - Aegis PAPER ready kernel v1...",
        "2026-05-05T13:30:10-04:00 node systemd[1]: Finished aegis-paper-ready-kernel-v1.service - Aegis PAPER ready kernel v1.",
    ]


def _journal_blocked_nonzero() -> list[str]:
    return [
        "2026-05-05T13:30:00-04:00 node systemd[1]: Starting aegis-paper-ready-kernel-v1.service - Aegis PAPER ready kernel v1...",
        "2026-05-05T13:30:10-04:00 node run_current_release_tool_v1.sh[1]: BLOCKED target_day=2026-05-05 first_blocker=OPTIONS_SNAPSHOT_CAPTURE_FAILED submit_allowed=false submission_authorized=false broker_transmit_enabled=null",
        "2026-05-05T13:30:10-04:00 node systemd[1]: aegis-paper-ready-kernel-v1.service: Main process exited, code=exited, status=2/INVALIDARGUMENT",
        "2026-05-05T13:30:10-04:00 node systemd[1]: aegis-paper-ready-kernel-v1.service: Failed with result 'exit-code'.",
        "2026-05-05T13:30:10-04:00 node systemd[1]: Failed to start aegis-paper-ready-kernel-v1.service - Aegis PAPER ready kernel v1.",
    ]


def _journal_runtime_exception() -> list[str]:
    return [
        "2026-05-05T13:30:00-04:00 node systemd[1]: Starting aegis-paper-ready-kernel-v1.service - Aegis PAPER ready kernel v1...",
        "2026-05-05T13:30:01-04:00 node run_current_release_tool_v1.sh[1]: Traceback (most recent call last):",
        "2026-05-05T13:30:01-04:00 node run_current_release_tool_v1.sh[1]: RuntimeError: broker tool crashed",
        "2026-05-05T13:30:01-04:00 node systemd[1]: aegis-paper-ready-kernel-v1.service: Failed with result 'exit-code'.",
        "2026-05-05T13:30:01-04:00 node systemd[1]: Failed to start aegis-paper-ready-kernel-v1.service - Aegis PAPER ready kernel v1.",
    ]


def _write_kernel_report(runtime: Path, *, final_status: str = "BLOCKED", first_blocker: str = "OPTIONS_SNAPSHOT_CAPTURE_FAILED", failed_stage_id: str = "") -> None:
    _write(
        runtime / f"truth/reports/aegis_paper_ready_kernel_v1/{DAY}/paper_ready_kernel.v1.json",
        {
            "schema_version": "aegis_paper_ready_kernel.v1",
            "target_day": DAY,
            "scheduled_run": True,
            "final_status": final_status,
            "first_blocker": first_blocker,
            "failed_stage_id": failed_stage_id,
            "submit_allowed": False,
            "submission_authorized": False,
            "broker_transmit_enabled": None,
            "generated_at_utc": "2026-05-05T17:30:10Z",
        },
    )


def _populate(runtime: Path, fail_at: str) -> None:
    truth = runtime / "truth"
    sleeve = runtime / "truth_sleeves/PRIMARY/PAPER"
    _write(sleeve / f"reports/trading_day_intent_generation_v1/{DAY}/trading_day_intent_generation.v1.json", {"day_utc": DAY, "final_status": "INTENTS_PRESENT", "generated_at_utc": "2026-05-05T13:30:00Z"})
    _write(sleeve / "pointers/selected_intent_pointer.v1.json", {"day_utc": DAY, "status": "SELECTED", "created_at_utc": "2026-05-05T13:30:01Z", "selected_intent": {"intent_id": "i1"}})
    _write(truth / f"reports/trading_day_readiness_authority_v1/{DAY}/trading_day_readiness_authority.v1.json", {"day_utc": DAY, "submit_allowed_by_mode": True, "generated_at_utc": "2026-05-05T13:30:02Z"})
    market = {"day_utc": DAY, "status": "PASS", "canonical_blocker": "", "capture_attempted_by_gate": True, "generated_at_utc": "2026-05-05T13:30:03Z"}
    if fail_at == "MARKET_NOT_OPEN":
        market.update({"status": "PENDING", "canonical_blocker": "MARKET_NOT_OPEN"})
    _write(sleeve / f"reports/market_open_data_gate_v1/{DAY}/market_open_data_gate.v1.json", market)
    if fail_at != "QUOTE_COMPLETE":
        _write(sleeve / f"reports/market_data_supply_v1/{DAY}/market_data_supply.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": "2026-05-05T13:30:04Z"})
    runtime_payload = {"day_utc": DAY, "status": "PASS", "ib_connection_state": "CONNECTED", "account_summary_state": "PRESENT", "reason_codes": [], "generated_at_utc": "2026-05-05T13:30:05Z"}
    if fail_at == "IB_DISCONNECTED":
        runtime_payload.update({"status": "BLOCKED", "canonical_blocker": "IB_DISCONNECTED", "ib_connection_state": "UNKNOWN", "reason_codes": ["IB_DISCONNECTED"]})
    _write(truth / f"reports/runtime_resilience_authority_v1/{DAY}/runtime_resilience_authority.v1.json", runtime_payload)
    safety = {"day_utc": DAY, "status": "PASS", "nav_valid": True, "reason_codes": [], "produced_utc": "2026-05-05T13:30:06Z"}
    if fail_at == "NAV_INVALID":
        safety.update({"status": "BLOCKED", "canonical_blocker": "NAV_INVALID", "nav_valid": False, "reason_codes": ["NAV_INVALID"]})
    _write(truth / f"reports/safety_state_authority_v1/{DAY}/safety_state_authority.v1.json", safety)
    _write(sleeve / f"reports/capital_supply_v1/{DAY}/capital_supply.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": "2026-05-05T13:30:07Z"})
    _write(sleeve / f"reports/risk_budget_supply_v1/{DAY}/risk_budget_supply.v1.json", {"day_utc": DAY, "status": "PASS", "generated_at_utc": "2026-05-05T13:30:08Z"})
    structure = {"day_utc": DAY, "status": "PASS", "canonical_blocker": "", "generated_at_utc": "2026-05-05T13:30:08Z"}
    if fail_at == "STRUCTURE":
        structure.update({"status": "BLOCKED", "canonical_blocker": "NO_ELIGIBLE_OPTION_STRUCTURE"})
    _write(sleeve / f"reports/structure_decision_supply_v1/{DAY}/structure_decision_supply.v1.json", structure)
    auth = {"day_utc": DAY, "status": "PASS", "produced_utc": "2026-05-05T13:30:09Z"}
    if fail_at == "AUTHORIZATION":
        auth = {"day_utc": DAY, "status": "FAIL", "reason_codes": ["AUTHORIZATION_MISSING"], "produced_utc": "2026-05-05T13:30:09Z"}
    _write(sleeve / f"reports/authorization_gate_verdict_v1/{DAY}/authorization_gate_verdict.v1.json", auth)
    if fail_at != "AUTHORIZATION":
        _write(sleeve / f"engine_activity_v1/authorization_v1/{DAY}/auth.json", {"authorized": True})
    kill = {"day_utc": DAY, "state": "INACTIVE", "allow_entries": True, "produced_utc": "2026-05-05T13:30:10Z", "reason_codes": []}
    if fail_at == "KILL_SWITCH":
        kill.update({"state": "ACTIVE", "allow_entries": False, "reason_codes": ["C2_KILL_SWITCH_ACTIVE"]})
    _write(truth / f"risk_v1/kill_switch_v1/{DAY}/global_kill_switch_state.v1.json", kill)
    _write(truth / f"reports/submit_boundary_status_v1/{DAY}/submit_boundary_status.v1.json", {"day_utc": DAY, "status": "READY" if fail_at == "NONE" else "NOT_READY", "canonical_blocker": "" if fail_at == "NONE" else "UPSTREAM_BLOCKED", "submit_allowed": fail_at == "NONE", "submission_authorized": fail_at == "NONE", "broker_transmit_enabled": False, "generated_at_utc": "2026-05-05T13:30:11Z"})


def test_identifies_kernel_launch_failure(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    lines = [
        "2026-05-05T15:45:01-04:00 node systemd[1]: Starting aegis-paper-ready-kernel-v1.service - Aegis PAPER ready kernel v1...",
        "2026-05-05T15:45:01-04:00 node run_current_release_tool_v1.sh[1]: python3: can't open file '/x/run_aegis_paper_ready_kernel_v1.py': [Errno 2] No such file or directory",
        "2026-05-05T15:45:01-04:00 node systemd[1]: Failed to start aegis-paper-ready-kernel-v1.service - Aegis PAPER ready kernel v1.",
    ]
    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=lines)
    assert result.first_blocker == "PAPER_READY_KERNEL_DID_NOT_START"
    assert result.classification == "BUG"


def test_failed_systemd_exit_with_valid_blocked_kernel_artifact_is_completed(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "MARKET_NOT_OPEN")
    _write_kernel_report(runtime)

    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_blocked_nonzero())

    kernel_gate = next(gate for gate in result.ordered_gate_results if gate.state == "KERNEL_STARTED")
    assert kernel_gate.status == "PASS"
    assert kernel_gate.reason_code == ""
    assert result.first_blocker == "MARKET_NOT_OPEN"
    assert result.submit_allowed is False


def test_runtime_exception_without_kernel_artifact_is_kernel_failed(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)

    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_runtime_exception())

    assert result.first_blocker == "PAPER_READY_KERNEL_FAILED"
    assert result.classification == "BUG"


def test_runtime_exception_is_not_masked_by_older_kernel_artifact(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _write_kernel_report(runtime)

    result = build_no_trade_explanation_v1(
        runtime_root=runtime,
        active_link=active,
        day_utc=DAY,
        journal_lines=[
            "2026-05-05T13:31:00-04:00 node systemd[1]: Starting aegis-paper-ready-kernel-v1.service - Aegis PAPER ready kernel v1...",
            "2026-05-05T13:31:01-04:00 node run_current_release_tool_v1.sh[1]: RuntimeError: broker tool crashed",
            "2026-05-05T13:31:01-04:00 node systemd[1]: aegis-paper-ready-kernel-v1.service: Failed with result 'exit-code'.",
        ],
    )

    assert result.first_blocker == "PAPER_READY_KERNEL_FAILED"


def test_kernel_process_exit_zero_for_valid_blocked_report() -> None:
    assert kernel_exit_code_for_report({"final_status": "BLOCKED", "submit_allowed": False}) == 0
    assert kernel_exit_code_for_report({"final_status": "MARKET_NOT_OPEN"}) == 0
    assert kernel_exit_code_for_report({"final_status": "PAPER_READY"}) == 0
    assert kernel_exit_code_for_report({"final_status": "ERROR"}) == 2


def test_identifies_market_not_open_and_prints_paths(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "MARKET_NOT_OPEN")
    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())
    text = render_explanation_v1(result)
    assert result.first_blocker == "MARKET_NOT_OPEN"
    assert "market_open_data_gate.v1.json" in text


def test_kernel_upstream_risk_failure_preferred_over_downstream_missing_market_gate(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "NONE")
    sleeve = runtime / "truth_sleeves/PRIMARY/PAPER"
    (sleeve / f"reports/market_open_data_gate_v1/{DAY}/market_open_data_gate.v1.json").unlink()
    (sleeve / f"reports/market_data_supply_v1/{DAY}/market_data_supply.v1.json").unlink()
    _write(
        sleeve / f"reports/risk_budget_supply_v1/{DAY}/risk_budget_supply.v1.json",
        {"day_utc": DAY, "status": "BLOCKED", "canonical_blocker": "CAPITAL_RISK_ENVELOPE_BLOCKED", "generated_at_utc": "2026-05-05T13:30:08Z"},
    )
    _write_kernel_report(runtime, first_blocker="CAPITAL_RISK_ENVELOPE_BLOCKED", failed_stage_id="risk_budget_supply")

    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())

    assert result.first_blocker == "CAPITAL_RISK_ENVELOPE_BLOCKED"
    risk_gate = next(gate for gate in result.ordered_gate_results if gate.state == "RISK_VALID")
    market_gate = next(gate for gate in result.ordered_gate_results if gate.state == "MARKET_SESSION_VALID")
    assert risk_gate.reason_code == "CAPITAL_RISK_ENVELOPE_BLOCKED"
    assert market_gate.reason_code == "MARKET_GATE_NOT_REACHED"
    assert market_gate.classification == "NOT_REACHED"


def test_market_gate_missing_still_reported_when_kernel_reached_market_gate(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "NONE")
    sleeve = runtime / "truth_sleeves/PRIMARY/PAPER"
    (sleeve / f"reports/market_open_data_gate_v1/{DAY}/market_open_data_gate.v1.json").unlink()
    (sleeve / f"reports/market_data_supply_v1/{DAY}/market_data_supply.v1.json").unlink()
    _write_kernel_report(runtime, first_blocker="MISSING_ARTIFACT", failed_stage_id="market_open_data_gate")

    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())

    assert result.first_blocker == "MARKET_SESSION_INVALID"
    market_gate = next(gate for gate in result.ordered_gate_results if gate.state == "MARKET_SESSION_VALID")
    assert market_gate.classification == "MISSING_EVIDENCE"


def test_kernel_structure_failure_preferred_over_downstream_governance_gates(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "STRUCTURE")
    _write_kernel_report(runtime, first_blocker="NO_ELIGIBLE_OPTION_STRUCTURE", failed_stage_id="structure_decision_supply")

    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())

    assert result.first_blocker == "NO_ELIGIBLE_OPTION_STRUCTURE"
    structure_gate = next(gate for gate in result.ordered_gate_results if gate.state == "STRUCTURE_DECISION_VALID")
    assert structure_gate.reason_code == "NO_ELIGIBLE_OPTION_STRUCTURE"
    assert structure_gate.classification == "EXPECTED_SAFETY"


def test_quote_complete_uses_market_gate_diagnostics_when_supply_skipped(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "STRUCTURE")
    sleeve = runtime / "truth_sleeves/PRIMARY/PAPER"
    _write(
        sleeve / f"reports/market_open_data_gate_v1/{DAY}/market_open_data_gate.v1.json",
        {
            "day_utc": DAY,
            "status": "PASS",
            "canonical_blocker": "",
            "capture_attempted_by_gate": True,
            "generated_at_utc": "2026-05-05T13:30:03Z",
            "diagnostics": {"quote_completeness_result": {"result": "PASS"}},
        },
    )
    _write(sleeve / f"reports/market_data_supply_v1/{DAY}/market_data_supply.v1.json", {"day_utc": DAY, "status": "SKIPPED", "canonical_blocker": "", "generated_at_utc": "2026-05-05T13:30:04Z"})
    _write_kernel_report(runtime, first_blocker="NO_ELIGIBLE_OPTION_STRUCTURE", failed_stage_id="structure_decision_supply")

    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())

    assert result.first_blocker == "NO_ELIGIBLE_OPTION_STRUCTURE"
    quote_gate = next(gate for gate in result.ordered_gate_results if gate.state == "QUOTE_COMPLETE")
    assert quote_gate.status == "PASS"
    assert quote_gate.artifact_path.endswith("market_open_data_gate.v1.json")


def test_quote_complete_still_fails_without_supply_or_market_gate_quote_pass(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "NONE")
    sleeve = runtime / "truth_sleeves/PRIMARY/PAPER"
    _write(sleeve / f"reports/market_data_supply_v1/{DAY}/market_data_supply.v1.json", {"day_utc": DAY, "status": "SKIPPED", "canonical_blocker": "", "generated_at_utc": "2026-05-05T13:30:04Z"})

    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())

    assert result.first_blocker == "QUOTE_COMPLETENESS_FAILED"
    quote_gate = next(gate for gate in result.ordered_gate_results if gate.state == "QUOTE_COMPLETE")
    assert quote_gate.status == "FAIL"


def test_identifies_stale_artifact(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "NONE")
    path = runtime / f"truth/reports/trading_day_readiness_authority_v1/{DAY}/trading_day_readiness_authority.v1.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["day_utc"] = "2026-05-04"
    _write(path, payload)
    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())
    assert result.classification == "STALE_ARTIFACT"
    assert any(item.stale_status == "STALE_DAY_MISMATCH" for item in result.stale_artifacts)


def test_identifies_ib_disconnected(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "IB_DISCONNECTED")
    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())
    assert result.first_blocker == "IB_DISCONNECTED"


def test_identifies_nav_invalid(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "NAV_INVALID")
    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())
    assert result.first_blocker == "NAV_INVALID"


def test_identifies_kill_switch_active(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "KILL_SWITCH")
    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())
    assert result.first_blocker == "C2_KILL_SWITCH_ACTIVE"


def test_identifies_authorization_missing(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "AUTHORIZATION")
    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())
    assert result.first_blocker == "AUTHORIZATION_MISSING"


def test_preserves_first_blocker_ordering(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "IB_DISCONNECTED")
    safety_path = runtime / f"truth/reports/safety_state_authority_v1/{DAY}/safety_state_authority.v1.json"
    payload = json.loads(safety_path.read_text(encoding="utf-8"))
    payload.update({"status": "BLOCKED", "nav_valid": False, "reason_codes": ["NAV_INVALID"]})
    _write(safety_path, payload)
    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())
    assert result.first_blocker == "IB_DISCONNECTED"


def test_never_changes_submit_flags_or_artifact(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "NONE")
    submit_path = runtime / f"truth/reports/submit_boundary_status_v1/{DAY}/submit_boundary_status.v1.json"
    before = submit_path.read_bytes()
    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())
    assert result.submit_allowed is True
    assert result.submission_authorized is True
    assert result.broker_transmit_enabled is False
    assert submit_path.read_bytes() == before
