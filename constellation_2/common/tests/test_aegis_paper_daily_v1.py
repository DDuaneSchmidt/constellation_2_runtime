from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_aegis_paper_daily_v1 as daily
from constellation_2.common.aegis_day_evidence_ledger_v1 import record_command_v1, utc_now_iso


DAY = "2026-04-27"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _patch_roots(monkeypatch, tmp_path: Path) -> tuple[Path, Path, list[dict]]:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    calls: list[dict] = []
    monkeypatch.setattr(daily, "require_authoritative_repo_runtime_v1", lambda repo: None)
    monkeypatch.setattr(daily, "resolve_decision_truth_root_bridge_v1", lambda *a, **k: truth_root)
    monkeypatch.setattr(daily, "resolve_single_paper_ib_account_from_sleeve_registry", lambda repo: "DUO847203")
    monkeypatch.setattr(
        daily,
        "resolve_sleeve_execution_root_v1",
        lambda **k: type("Root", (), {"execution_root_path": execution_root})(),
    )

    def fake_run(ledger, *, phase, name, cmd, env):
        rc = 0
        if name == "aegis_paper_submit" and getattr(fake_run, "fail_submit", False):
            rc = 2
        if name == "aegis_paper_preflight" and getattr(fake_run, "fail_preflight", False):
            rc = 2
        calls.append({"phase": phase, "name": name, "cmd": cmd, "env": dict(env), "return_code": rc})
        record_command_v1(ledger, phase=phase, name=name, command=cmd, start_utc=utc_now_iso(), end_utc=utc_now_iso(), exit_code=rc)
        return {"name": name, "phase": phase, "return_code": rc, "stdout": "", "stderr": ""}

    monkeypatch.setattr(daily, "_run", fake_run)
    return truth_root, execution_root, calls


def _seed_intent(truth_root: Path, count: int = 1) -> None:
    _write_json(
        truth_root / "reports" / "trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json",
        {"intent_count": count},
    )


def _seed_released_candidate(execution_root: Path) -> None:
    candidate = execution_root / "phaseC_preflight_v1" / DAY / "attempt_A0001" / ("6" * 64)
    _write_json(candidate / "submit_preflight_decision.v1.json", {"decision": "ALLOW"})


def _seed_dry_run_submission(execution_root: Path) -> None:
    submission_id = "7" * 64
    root = execution_root / "execution_evidence_v1" / "submissions" / DAY / submission_id
    _write_json(root / "broker_submission_record.v2.json", {"submission_id": submission_id, "broker_ids": {"order_id": None, "perm_id": None}, "error": {"code": "DRY_RUN_NO_BROKER_ID"}})
    _write_json(root / "broker_submit_attempt_v1.json", {"submission_id": submission_id, "dry_run": True})


def test_dry_run_day_with_intent_completes_success_dry_run(monkeypatch, tmp_path: Path) -> None:
    truth_root, execution_root, _calls = _patch_roots(monkeypatch, tmp_path)
    _seed_intent(truth_root, 1)
    _seed_dry_run_submission(execution_root)
    rc = daily.main(["--day_utc", DAY])
    assert rc == 0
    ledger = json.loads((truth_root / "reports" / "aegis_day_evidence_ledger_v1" / DAY / "aegis_day_evidence_ledger.v1.json").read_text())
    assert ledger["final_daily_outcome"] == "SUCCESS_DRY_RUN"


def test_no_intent_day_completes_no_intent_expected(monkeypatch, tmp_path: Path) -> None:
    truth_root, _execution_root, _calls = _patch_roots(monkeypatch, tmp_path)
    _seed_intent(truth_root, 0)
    rc = daily.main(["--day_utc", DAY])
    assert rc == 0
    summary = json.loads((truth_root / "reports" / "aegis_daily_operator_summary_v1" / DAY / "aegis_daily_operator_summary.v1.json").read_text())
    assert summary["no_silent_day_outcome"] == "NO_INTENT_EXPECTED"


def test_blocking_preflight_completes_blocked_with_reason(monkeypatch, tmp_path: Path) -> None:
    truth_root, _execution_root, _calls = _patch_roots(monkeypatch, tmp_path)
    _seed_intent(truth_root, 1)
    daily._run.fail_preflight = True
    rc = daily.main(["--day_utc", DAY])
    assert rc == 2
    ledger = json.loads((truth_root / "reports" / "aegis_day_evidence_ledger_v1" / DAY / "aegis_day_evidence_ledger.v1.json").read_text())
    assert ledger["final_daily_outcome"] == "BLOCKED_WITH_REASON"


def test_failed_submit_completes_failed_with_owner(monkeypatch, tmp_path: Path) -> None:
    truth_root, execution_root, _calls = _patch_roots(monkeypatch, tmp_path)
    _seed_intent(truth_root, 1)
    ledger = {"commands": [{"phase": "SUBMIT", "name": "aegis_paper_submit", "return_code": 2, "command": ["npm", "run", "aegis:paper:submit"]}]}
    graph = {"blocking_nodes": []}
    outcome, blockers = daily._derive_outcome(truth_root=truth_root, execution_root=execution_root, day_utc=DAY, ledger=ledger, graph=graph)  # noqa: SLF001
    assert outcome == "FAILED_WITH_OWNER"
    assert blockers[0]["owner"] == "aegis_paper_submit"


def test_packet_generated_even_on_blocked_day(monkeypatch, tmp_path: Path) -> None:
    truth_root, _execution_root, calls = _patch_roots(monkeypatch, tmp_path)
    _seed_intent(truth_root, 1)
    daily._run.fail_preflight = True
    daily.main(["--day_utc", DAY])
    names = [call["name"] for call in calls]
    assert "aegis_chatgpt_packet" in names
    assert "aegis_chatgpt_show" in names


def test_broker_transmit_is_never_enabled_implicitly(monkeypatch, tmp_path: Path) -> None:
    truth_root, execution_root, calls = _patch_roots(monkeypatch, tmp_path)
    _seed_intent(truth_root, 1)
    _seed_released_candidate(execution_root)
    daily.main(["--day_utc", DAY, "--mode", "DRY_RUN"])
    submit_calls = [call for call in calls if call["name"] == "aegis_paper_submit"]
    assert submit_calls
    assert submit_calls[0]["env"].get("C2_GOVERNED_SUBMIT_DRY_RUN") == "YES"
