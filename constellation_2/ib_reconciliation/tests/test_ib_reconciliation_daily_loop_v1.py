from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DAILY_LOOP_TOOL = (REPO_ROOT / "ops" / "tools" / "run_ib_reconciliation_daily_loop_v1.py").resolve()
FIXTURE_XML = (Path(__file__).resolve().parent / "fixtures" / "ib_flex_sample.xml").resolve()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _seed_truth_root(day_utc: str, truth_root: Path, *, include_submission: bool, fill_qty: int = 10) -> None:
    if not include_submission:
        return
    subdir = (truth_root / "execution_evidence_v1" / "submissions" / day_utc / "submission-abc").resolve()
    subdir.mkdir(parents=True, exist_ok=True)
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": "submission-abc",
            "submitted_at_utc": f"{day_utc}T14:25:00Z",
            "binding_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": "FILLED",
            "broker_ids": {"order_id": 111, "perm_id": 222},
        },
    )
    _write_json(
        subdir / "equity_order_plan.v2.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "plan_id": "plan-abc",
            "created_at_utc": f"{day_utc}T14:20:00Z",
            "intent_hash": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            "structure": "EQUITY_SPOT",
            "symbol": "AAPL",
            "currency": "USD",
            "action": "BUY",
            "qty_shares": 10,
            "order_terms": {"order_type": "LIMIT", "limit_price": "189.50", "time_in_force": "DAY"},
            "engine_id": "trend_eq",
            "source_intent_id": "intent-abc",
            "intent_sha256": "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
        },
    )
    _write_json(
        truth_root / "fill_ledger_v1" / day_utc / "submission-abc.fill_ledger.v1.json",
        {
            "schema_id": "C2_FILL_LEDGER_V1",
            "schema_version": 1,
            "submission_id": "submission-abc",
            "filled_qty": fill_qty,
            "avg_fill_price_weighted": "189.50",
        },
    )
    _write_json(
        truth_root / "submission_index_v1" / day_utc / "submission_index.v1.json",
        {
            "schema_version": "submission_index.v1",
            "day": day_utc,
            "attempts": [
                {
                    "submission_id": "submission-abc",
                    "attempt_id": "attempt-abc",
                    "broker_order_id": 111,
                    "broker_perm_id": 222,
                }
            ],
        },
    )


def _run_daily_loop(
    *,
    day_utc: str,
    runtime_root: Path,
    truth_root: Path,
    ib_flex_xml: Path | None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["IB_RECONCILIATION_ALLOW_ANY_RUNTIME_ROOT"] = "1"
    env["IB_RECONCILIATION_RUNTIME_ROOT"] = str(runtime_root.resolve())
    env["IB_RECONCILIATION_TRUTH_SURFACE_ROOT"] = str((runtime_root.parent / "truth_reports").resolve())
    env["C2_TRUTH_ROOT"] = str(truth_root.resolve())

    cmd = [sys.executable, str(DAILY_LOOP_TOOL), "--day_utc", day_utc]
    if ib_flex_xml is not None:
        cmd.extend(["--ib_flex_xml", str(ib_flex_xml.resolve())])
    return subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def test_01_daily_loop_missing_report_skips(tmp_path: Path) -> None:
    day = "2026-04-26"
    runtime_root = (tmp_path / "runtime" / "ib_reconciliation").resolve()
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)

    proc = _run_daily_loop(day_utc=day, runtime_root=runtime_root, truth_root=truth_root, ib_flex_xml=None)
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["final_status"] == "SKIPPED_NO_IB_REPORT"

    skip_path = (runtime_root / "reconciliations" / day / "ib_reconciliation_skip.v1.json").resolve()
    assert skip_path.exists()
    skip_payload = json.loads(skip_path.read_text(encoding="utf-8"))
    assert skip_payload["status"] == "SKIPPED_NO_IB_REPORT"


def test_02_daily_loop_fixture_runs_full_chain(tmp_path: Path) -> None:
    day = "2026-04-24"
    runtime_root = (tmp_path / "runtime" / "ib_reconciliation").resolve()
    truth_root = (tmp_path / "truth").resolve()
    _seed_truth_root(day, truth_root, include_submission=True, fill_qty=10)

    proc = _run_daily_loop(day_utc=day, runtime_root=runtime_root, truth_root=truth_root, ib_flex_xml=FIXTURE_XML)
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["final_status"] in {"PASS", "WARN", "FAIL"}
    assert [step["step"] for step in payload["steps"]] == [
        "normalize_ib_flex",
        "extract_aegis_expected",
        "run_reconciliation",
        "build_ai_packet",
    ]
    assert (runtime_root / "reconciliations" / day / "ib_reconciliation.v1.json").exists()
    assert (runtime_root / "ai_reviews" / day / "ib_reconciliation_ai_packet.v1.json").exists()


def test_03_warn_or_fail_creates_alert_candidate(tmp_path: Path) -> None:
    day = "2026-04-24"
    runtime_root = (tmp_path / "runtime" / "ib_reconciliation").resolve()
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    # no submission evidence => expected is empty, fixture IB trade becomes unexpected => FAIL
    proc = _run_daily_loop(day_utc=day, runtime_root=runtime_root, truth_root=truth_root, ib_flex_xml=FIXTURE_XML)
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["final_status"] in {"WARN", "FAIL"}

    alert_path = (runtime_root / "alerts" / day / "ib_reconciliation_alert_candidate.v1.json").resolve()
    assert alert_path.exists()
    alert_payload = json.loads(alert_path.read_text(encoding="utf-8"))
    assert alert_payload["status"] in {"WARN", "FAIL"}
    assert "subject" in alert_payload
    assert "evidence_paths" in alert_payload


def test_04_daily_loop_does_not_write_into_repo(tmp_path: Path) -> None:
    day = "2026-04-26"
    runtime_root = (tmp_path / "runtime" / "ib_reconciliation").resolve()
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    proc = _run_daily_loop(day_utc=day, runtime_root=runtime_root, truth_root=truth_root, ib_flex_xml=None)
    assert proc.returncode == 0
    skip_path = (runtime_root / "reconciliations" / day / "ib_reconciliation_skip.v1.json").resolve()
    assert skip_path.exists()
    assert not str(skip_path).startswith(str(REPO_ROOT.resolve()))


def test_05_daily_loop_does_not_fetch_from_ib() -> None:
    source = DAILY_LOOP_TOOL.read_text(encoding="utf-8").lower()
    forbidden_tokens = ["requests", "urllib", "http://", "https://", "curl ", "flex web service", "ibkr flex web service"]
    assert all(token not in source for token in forbidden_tokens)


def test_06_daily_loop_does_not_touch_submit_or_closure_authority() -> None:
    source = DAILY_LOOP_TOOL.read_text(encoding="utf-8").lower()
    forbidden_tokens = [
        "submit boundary",
        "run_paper_submit",
        "submit_boundary",
        "closure authority",
        "aegis_day_closure_authority",
        "run_aegis_day_closure_authority",
    ]
    assert all(token not in source for token in forbidden_tokens)

