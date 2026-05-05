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

build_no_trade_explanation_v1 = MODULE.build_no_trade_explanation_v1
render_explanation_v1 = MODULE.render_explanation_v1


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


def test_identifies_market_not_open_and_prints_paths(tmp_path: Path) -> None:
    runtime, active = _base_runtime(tmp_path)
    _populate(runtime, "MARKET_NOT_OPEN")
    result = build_no_trade_explanation_v1(runtime_root=runtime, active_link=active, day_utc=DAY, journal_lines=_journal_success())
    text = render_explanation_v1(result)
    assert result.first_blocker == "MARKET_NOT_OPEN"
    assert "market_open_data_gate.v1.json" in text


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
