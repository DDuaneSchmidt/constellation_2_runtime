from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_aegis_paper_cancel_v1 as cancel_tool  # noqa: E402
from constellation_2.phaseD.adapters.broker_adapter_v1 import BrokerSubmitResult  # noqa: E402


DAY = "2026-04-29"
SID = "s" * 64


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _seed_submission(
    root: Path,
    *,
    environment: str = "PAPER",
    account: str = "DUO847203",
    order_id: int | None = 123,
    perm_id: int | None = 456,
) -> Path:
    subdir = root / "execution_evidence_v1" / "submissions" / DAY / SID
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": SID,
            "submitted_at_utc": f"{DAY}T15:00:00Z",
            "binding_hash": "a" * 64,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": environment},
            "status": "SUBMITTED",
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
            "error": None,
            "canonical_json_hash": None,
        },
    )
    _write_json(
        subdir / "broker_submit_attempt_v1.json",
        {
            "schema_id": "broker_submit_attempt",
            "schema_version": "v1",
            "day_utc": DAY,
            "environment": environment,
            "submission_id": SID,
            "attempted_at_utc": f"{DAY}T15:00:00Z",
            "attempt_state": "SUBMIT_ATTEMPTED",
            "ib_account": account,
            "dry_run": False,
            "reason_codes": ["REAL_SUBMIT_ATTEMPT"],
            "evidence_artifacts": [],
            "canonical_json_hash": None,
        },
    )
    return subdir


def _ctx(root: Path) -> cancel_tool.CancelContext:
    return cancel_tool.CancelContext(
        day_utc=DAY,
        submission_id=SID,
        execution_root=root,
        canonical_truth_root=root / "canonical_truth",
        produced_utc=f"{DAY}T15:01:00Z",
    )


class _FakeAdapter:
    cancelled_order_ids: list[int] = []

    def __init__(self, _spec) -> None:
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def cancel_order(self, *, order_id: int) -> BrokerSubmitResult:
        self.cancelled_order_ids.append(order_id)
        return BrokerSubmitResult(
            ok=True,
            status="PENDINGCANCEL",
            order_id=order_id,
            perm_id=None,
            error_code=None,
            error_message=None,
            raw={"cancel_order_id": order_id},
        )


def _runner(_cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(_cmd, 0, stdout="OK", stderr="")


def _patch_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cancel_tool,
        "resolve_governed_paper_execution_profile",
        lambda **_kwargs: SimpleNamespace(host="127.0.0.1", port=4002, client_id_orders=7),
    )


def test_paper_cancel_allowed_and_uses_derived_order_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_submission(tmp_path)
    _patch_profile(monkeypatch)
    _FakeAdapter.cancelled_order_ids = []

    result = cancel_tool.run_aegis_paper_cancel_v1(
        _ctx(tmp_path),
        adapter_factory=_FakeAdapter,
        runner=_runner,
    )

    assert result["classification"] == "CANCEL_REQUESTED"
    assert _FakeAdapter.cancelled_order_ids == [123]
    assert result["order_id"] == 123
    assert result["perm_id"] == 456


def test_live_cancel_rejected(tmp_path: Path) -> None:
    _seed_submission(tmp_path, environment="LIVE")

    with pytest.raises(cancel_tool.PaperCancelError, match="CANCEL_ENVIRONMENT_NOT_PAPER"):
        cancel_tool.run_aegis_paper_cancel_v1(_ctx(tmp_path), adapter_factory=_FakeAdapter, runner=_runner)


def test_wrong_account_rejected(tmp_path: Path) -> None:
    _seed_submission(tmp_path, account="DU999999")

    with pytest.raises(cancel_tool.PaperCancelError, match="CANCEL_ACCOUNT_NOT_ALLOWED"):
        cancel_tool.run_aegis_paper_cancel_v1(_ctx(tmp_path), adapter_factory=_FakeAdapter, runner=_runner)


def test_missing_order_id_rejected(tmp_path: Path) -> None:
    _seed_submission(tmp_path, order_id=None)

    with pytest.raises(cancel_tool.PaperCancelError, match="BROKER_ORDER_ID_MISSING"):
        cancel_tool.run_aegis_paper_cancel_v1(_ctx(tmp_path), adapter_factory=_FakeAdapter, runner=_runner)


def test_missing_perm_id_rejected(tmp_path: Path) -> None:
    _seed_submission(tmp_path, perm_id=None)

    with pytest.raises(cancel_tool.PaperCancelError, match="BROKER_PERM_ID_MISSING"):
        cancel_tool.run_aegis_paper_cancel_v1(_ctx(tmp_path), adapter_factory=_FakeAdapter, runner=_runner)


def test_cancel_artifacts_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    subdir = _seed_submission(tmp_path)
    _patch_profile(monkeypatch)

    cancel_tool.run_aegis_paper_cancel_v1(_ctx(tmp_path), adapter_factory=_FakeAdapter, runner=_runner)

    request = json.loads((subdir / "aegis_paper_cancel_request.v1.json").read_text(encoding="utf-8"))
    result = json.loads((subdir / "aegis_paper_cancel_result.v1.json").read_text(encoding="utf-8"))
    assert request["status"] == "CANCEL_REQUESTED"
    assert request["order_id"] == 123
    assert result["classification"] == "CANCEL_REQUESTED"
    assert result["cancel_request_path"].endswith("aegis_paper_cancel_request.v1.json")


def test_filled_before_cancel_is_handled_without_broker_call(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    subdir = _seed_submission(tmp_path)
    _write_json(
        subdir / "broker_order_outcome_v1.json",
        {
            "schema_id": "broker_order_outcome",
            "schema_version": "v1",
            "day_utc": DAY,
            "environment": "PAPER",
            "submission_id": SID,
            "evaluated_at_utc": f"{DAY}T15:02:00Z",
            "outcome_state": "FILLED",
            "status": "FILLED",
            "order_id": 123,
            "perm_id": 456,
            "ib_account": "DUO847203",
            "reason_codes": [],
            "evidence_artifacts": [],
            "error": None,
            "canonical_json_hash": None,
        },
    )
    _patch_profile(monkeypatch)
    _FakeAdapter.cancelled_order_ids = []

    result = cancel_tool.run_aegis_paper_cancel_v1(
        _ctx(tmp_path),
        adapter_factory=_FakeAdapter,
        runner=_runner,
    )

    assert result["classification"] == "FILLED_BEFORE_CANCEL"
    assert _FakeAdapter.cancelled_order_ids == []


def test_no_arbitrary_order_id_input_is_accepted() -> None:
    with pytest.raises(SystemExit) as exc:
        cancel_tool.main(["--day_utc", DAY, "--submission_id", SID, "--order_id", "999"])
    assert exc.value.code == 2
